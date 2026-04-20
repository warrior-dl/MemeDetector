"""
AI 分析模块：基于评论证据包对 hypothesis 做最终裁决。
"""

from __future__ import annotations

import json
from contextlib import closing
from datetime import date

from rich.progress import track

import meme_detector.researcher.decider as _decider_module
from meme_detector.agent_tracing import TraceTimelineBuilder, start_langfuse_trace
from meme_detector.archivist.agent_store import (
    create_agent_conversation,
    finish_agent_conversation,
    replace_agent_trace_events,
)
from meme_detector.archivist.schema import get_conn
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
from meme_detector.run_tracker import get_current_run_id

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
        conversation_id = create_research_conversation(bundle_id=bundle_id)
        trace = TraceTimelineBuilder(
            conversation_id=conversation_id or "",
            run_id=get_current_run_id() or "",
            agent_name="researcher",
            entity_type="bundle",
            entity_id=bundle_id,
        )
        trace.add_step(
            event_type="input",
            stage="prepare",
            title="读取证据包",
            status="success",
            summary=f"已加载 {len(bundle.hypotheses)} 个 hypotheses、{len(bundle.evidences)} 条 evidences",
            output_data={
                "bundle_id": bundle_id,
                "bvid": bundle.insight.bvid,
                "hypothesis_count": len(bundle.hypotheses),
                "evidence_count": len(bundle.evidences),
            },
        )
        with start_langfuse_trace(
            name=f"research:{bundle_id}",
            session_id=get_current_run_id(),
            metadata={
                "agent_name": "researcher",
                "conversation_id": conversation_id or "",
                "bundle_id": bundle_id,
                "bvid": bundle.insight.bvid,
                "hypothesis_count": len(bundle.hypotheses),
            },
        ) as langfuse_trace:
            try:
                decision = await _decide_bundle(bundle, today=today, trace=trace)
            except Exception as exc:
                result.failed_bundle_ids.append(bundle_id)
                trace.add_step(
                    event_type="error",
                    stage="reason",
                    title="证据包裁决失败",
                    status="failed",
                    summary=str(exc),
                    output_data={"error": str(exc)},
                )
                persist_research_conversation(
                    conversation_id=conversation_id,
                    bundle=bundle,
                    status="failed",
                    summary=f"{bundle_id} 裁决失败",
                    output={"bundle_id": bundle_id, "error": str(exc)},
                    trace=trace,
                    langfuse_trace_id=langfuse_trace.trace_id,
                    langfuse_public_url=langfuse_trace.trace_url,
                    error_message=str(exc),
                )
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
                trace.add_step(
                    event_type="tool",
                    stage="verify",
                    title="校验来源 URL",
                    status="success",
                    summary=f"保留 {len(valid_urls)}/{original_source_count} 个可访问来源",
                    input_data={"source_urls": decision.record.source_urls},
                    output_data={"valid_source_urls": valid_urls},
                )
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
            trace.add_step(
                event_type="decision",
                stage="finalize",
                title="生成最终决策",
                status="success",
                summary=decision.reason,
                output_data={
                    "decision": decision.decision.value,
                    "final_title": decision.final_title,
                    "target_record_id": decision.target_record_id,
                    "confidence": decision.confidence,
                },
            )
            trace.add_step(
                event_type="persist",
                stage="finalize",
                title="写入裁决结果",
                status="success",
                summary=f"已写入 decision_{bundle_id}",
                output_data={"decision_id": decision.decision_id},
            )

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

            persist_research_conversation(
                conversation_id=conversation_id,
                bundle=bundle,
                status="success",
                summary=decision.reason,
                output={
                    "bundle_id": bundle_id,
                    "decision": decision.decision.value,
                    "final_title": decision.final_title,
                    "target_record_id": decision.target_record_id,
                    "record": decision.record.model_dump(mode="json") if decision.record else None,
                },
                trace=trace,
                langfuse_trace_id=langfuse_trace.trace_id,
                langfuse_public_url=langfuse_trace.trace_url,
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


def create_research_conversation(*, bundle_id: str) -> str | None:
    run_id = get_current_run_id()
    if not run_id:
        return None
    with closing(get_conn()) as conn:
        return create_agent_conversation(
            conn,
            run_id=run_id,
            agent_name="researcher",
            word=bundle_id,
            entity_type="bundle",
            entity_id=bundle_id,
            langfuse_session_id=run_id,
        )


def persist_research_conversation(
    *,
    conversation_id: str | None,
    bundle,
    status: str,
    summary: str,
    output: dict,
    trace: TraceTimelineBuilder,
    langfuse_trace_id: str = "",
    langfuse_public_url: str = "",
    error_message: str = "",
) -> None:
    if not conversation_id:
        return
    with closing(get_conn()) as conn:
        replace_agent_trace_events(
            conn,
            conversation_id=conversation_id,
            run_id=trace.run_id,
            agent_name=trace.agent_name,
            entity_type=trace.entity_type,
            entity_id=trace.entity_id,
            events=trace.all_steps(),
        )
        finish_agent_conversation(
            conn,
            conversation_id,
            status=status,
            summary=summary,
            messages_json="[]",
            message_count=0,
            output_json=json.dumps(output, ensure_ascii=False, default=str),
            public_timeline_json=json.dumps(trace.public_steps(), ensure_ascii=False, default=str),
            raw_timeline_json=json.dumps(trace.all_steps(), ensure_ascii=False, default=str),
            input_summary_json=json.dumps(
                {
                    "bundle_id": bundle.bundle_id,
                    "bvid": bundle.insight.bvid,
                    "hypothesis_count": len(bundle.hypotheses),
                    "evidence_count": len(bundle.evidences),
                },
                ensure_ascii=False,
                default=str,
            ),
            token_usage_json=json.dumps(trace.token_usage(), ensure_ascii=False, default=str),
            langfuse_trace_id=langfuse_trace_id,
            langfuse_public_url=langfuse_public_url,
            error_message=error_message,
        )
