"""
AI 分析模块：基于评论证据包对 hypothesis 做最终裁决。
"""

from __future__ import annotations

from datetime import date

from rich.progress import track

import meme_detector.researcher.decider as _decider_module
from meme_detector.config import settings
from meme_detector.logging_utils import get_logger
from meme_detector.pipeline_service import update_job_runtime_progress
from meme_detector.researcher.models import ResearchRunResult
from meme_detector.researcher.persistence import (
    list_queued_bundles as _list_queued_bundles,
    load_bundle as _load_bundle,
    persist_research_decision as _persist_research_decision,
)
from meme_detector.researcher.tools import (
    verify_urls,
)

logger = get_logger(__name__)


async def _decide_bundle(*args, **kwargs):
    return await _decider_module.decide_bundle(*args, **kwargs)


# ── 主流程 ──────────────────────────────────────────────────

async def run_research() -> ResearchRunResult:
    """评论证据包裁决主流程。"""
    logger.info("researcher started", extra={"event": "research_started"})
    update_job_runtime_progress(
        "research",
        phase="loading",
        current=0,
        total=0,
        unit="证据包",
        message="正在载入待裁决证据包",
    )

    queued_bundles = _list_queued_bundles()
    result = ResearchRunResult(pending_count=len(queued_bundles))
    update_job_runtime_progress(
        "research",
        phase="loading",
        current=0,
        total=len(queued_bundles),
        unit="证据包",
        message=f"已载入 {len(queued_bundles)} 个待裁决证据包",
    )
    logger.info(
        "research queued bundle summary",
        extra={
            "event": "research_bundle_queue_summary",
            "pending_count": len(queued_bundles),
        },
    )

    if not queued_bundles:
        update_job_runtime_progress(
            "research",
            phase="idle",
            current=0,
            total=0,
            unit="证据包",
            message="没有待裁决证据包",
        )
        logger.info("no pending bundles for research", extra={"event": "research_no_pending_bundles"})
        return result

    logger.info(
        "research bundles loaded",
        extra={"event": "research_bundles_loaded", "bundle_count": len(queued_bundles)},
    )

    logger.info("research bundle adjudication started", extra={"event": "research_bundle_adjudication_started"})
    today = date.today()

    for item in track(queued_bundles, description="分析中..."):
        bundle_id = str(item.get("bundle_id", "")).strip()
        processed_count = result.adjudicated_count
        update_job_runtime_progress(
            "research",
            phase="adjudicating",
            current=processed_count,
            total=len(queued_bundles),
            unit="证据包",
            message=f"正在裁决第 {processed_count + 1}/{len(queued_bundles)} 个证据包：{bundle_id}",
        )
        bundle = _load_bundle(bundle_id)
        if bundle is None:
            result.failed_bundle_ids.append(bundle_id)
            logger.warning(
                "research bundle missing during load",
                extra={"event": "research_bundle_missing", "bundle_id": bundle_id},
            )
            continue
        result.adjudicated_count += 1
        logger.info(
            "research bundle adjudication started",
            extra={
                "event": "research_bundle_started",
                "bundle_id": bundle_id,
                "bvid": bundle.insight.bvid,
                "hypothesis_count": len(bundle.hypotheses),
            },
        )

        try:
            decision = await _decide_bundle(bundle, today=today)
        except Exception:
            result.failed_bundle_ids.append(bundle_id)
            logger.warning(
                "research bundle adjudication failed",
                extra={"event": "research_bundle_failed", "bundle_id": bundle_id},
                exc_info=True,
            )
            update_job_runtime_progress(
                "research",
                phase="adjudicating",
                current=result.adjudicated_count,
                total=len(queued_bundles),
                unit="证据包",
                message=f"证据包裁决失败：{bundle_id}",
            )
            continue

        if decision.record is not None and decision.record.source_urls:
            original_source_count = len(decision.record.source_urls)
            valid_urls = await verify_urls(decision.record.source_urls)
            logger.info(
                "research source urls verified",
                extra={
                    "event": "research_source_urls_verified",
                    "bundle_id": bundle_id,
                    "source_count": original_source_count,
                    "valid_source_count": len(valid_urls),
                },
            )
            decision.record.source_urls = valid_urls
            if original_source_count > 0 and len(valid_urls) < original_source_count / 2:
                decision.record.confidence_score *= 0.8
                logger.info(
                    "research confidence lowered after source verification",
                    extra={
                        "event": "research_confidence_lowered_after_source_verification",
                        "bundle_id": bundle_id,
                        "source_count": original_source_count,
                        "valid_source_count": len(valid_urls),
                    },
                )

        await _persist_research_decision(decision)

        if decision.decision.value in {"accept", "rewrite_title"} and decision.record is not None:
            result.add_accepted_record(decision.record)
            logger.info(
                "research bundle accepted",
                extra={
                    "event": "research_bundle_accepted",
                    "bundle_id": bundle_id,
                    "title": decision.record.title,
                    "confidence_score": decision.record.confidence_score,
                },
            )
        elif decision.decision.value == "reject":
            result.rejected_count += 1
            result.rejected_bundle_ids.append(bundle_id)
            logger.info(
                "research bundle rejected",
                extra={"event": "research_bundle_rejected", "bundle_id": bundle_id},
            )
        else:
            result.failed_bundle_ids.append(bundle_id)
            logger.info(
                "research bundle ended without direct acceptance",
                extra={
                    "event": "research_bundle_non_accepting_decision",
                    "bundle_id": bundle_id,
                    "decision": decision.decision.value,
                },
            )
        update_job_runtime_progress(
            "research",
            phase="adjudicating",
            current=result.adjudicated_count,
            total=len(queued_bundles),
            unit="证据包",
            message=(
                f"已完成 {result.adjudicated_count}/{len(queued_bundles)} 个证据包，"
                f"接受 {result.accepted_count} / 驳回 {result.rejected_count}"
            ),
        )

    logger.info(
        "researcher completed",
        extra={
            "event": "research_completed",
            "accepted_count": result.accepted_count,
            "rejected_count": result.rejected_count,
            "failed_count": len(result.failed_bundle_ids),
        },
    )
    return result
