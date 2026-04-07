"""
AI 分析模块：三步流程对候选词进行梗识别和溯源。

Step 1: DeepSeek 批量快速筛选（低成本）
Step 2: 深度分析 + 工具调用（仅高置信度候选词）
Step 3: 来源 URL 验证（防幻觉）
"""

from __future__ import annotations

from datetime import date

from rich.progress import track

import meme_detector.researcher.deep_analysis as _deep_analysis_module
from meme_detector.config import settings
from meme_detector.logging_utils import get_logger
from meme_detector.miner.video_context import get_bilibili_video_context
from meme_detector.researcher.deep_analysis import (
    build_research_provider as _build_research_provider,
    deep_agent,
)
from meme_detector.researcher.bootstrap import (
    bootstrap_candidates_from_miner as _bootstrap_candidates_from_miner,
)
from meme_detector.researcher.models import ResearchRunResult
from meme_detector.researcher.persistence import (
    accept_candidate as _accept_candidate,
    list_pending_candidates as _list_pending_candidates,
    list_pending_scout_videos as _list_pending_scout_videos,
    reject_candidates as _reject_candidates,
)
from meme_detector.researcher.screening import (
    batch_screen as _batch_screen,
    partition_screen_results as _partition_screen_results,
)
from meme_detector.researcher.tools import (
    verify_urls,
    volcengine_web_search,
    volcengine_web_search_summary,
)
from meme_detector.run_tracker import get_current_run_id

logger = get_logger(__name__)


async def _deep_analyze(*args, **kwargs):
    # 保持 agent 模块作为稳定 monkeypatch 入口。
    _deep_analysis_module.get_bilibili_video_context = get_bilibili_video_context
    _deep_analysis_module.volcengine_web_search_summary = volcengine_web_search_summary
    _deep_analysis_module.volcengine_web_search = volcengine_web_search
    _deep_analysis_module.get_current_run_id = get_current_run_id
    return await _deep_analysis_module.deep_analyze(*args, **kwargs)


# ── 主流程 ──────────────────────────────────────────────────

async def run_research() -> ResearchRunResult:
    """完整的 AI 分析流程。"""
    logger.info("researcher started", extra={"event": "research_started"})

    pending_videos = _list_pending_scout_videos()
    if pending_videos:
        logger.warning(
            "research blocked by pending miner videos",
            extra={
                "event": "research_blocked_by_pending_miner_videos",
                "video_count": len(pending_videos),
            },
        )
        return ResearchRunResult.blocked_by_pending_videos(len(pending_videos))

    bootstrapped_candidates = await _bootstrap_candidates_from_miner()
    candidates = _list_pending_candidates(limit=settings.ai_batch_size)
    result = ResearchRunResult(
        pending_count=len(candidates),
        bootstrapped_count=len(bootstrapped_candidates),
    )

    if not candidates:
        logger.info("no pending candidates for research", extra={"event": "research_no_pending_candidates"})
        return result

    logger.info(
        "research candidates loaded",
        extra={"event": "research_candidates_loaded", "candidate_count": len(candidates)},
    )

    # ── Step 1: 批量快速筛选 ─────────────────────────────────
    logger.info("research step 1 screening started", extra={"event": "research_step_screening_started"})
    screen_results = await _batch_screen(candidates)
    result.screened_count = len(screen_results)

    screen_map = {r.word: r for r in screen_results}
    to_deep, rejected, pending_retry = _partition_screen_results(candidates, screen_results)
    _reject_candidates(rejected)
    result.rejected_words = rejected
    result.rejected_count = len(rejected)
    result.deep_analysis_count = len(to_deep)
    result.screen_failed_words = pending_retry

    logger.info(
        "research screening summary",
        extra={
            "event": "research_screening_summary",
            "accepted_count": len(to_deep),
            "rejected_count": len(rejected),
            "failed_count": len(pending_retry),
        },
    )
    logger.info(
        "research screening completed",
        extra={
            "event": "research_screening_completed",
            "candidate_count": len(candidates),
            "result_count": len(screen_results),
            "accepted_count": len(to_deep),
            "rejected_count": len(rejected),
            "failed_count": len(pending_retry),
        },
    )

    if not to_deep:
        return result

    # ── Step 2 & 3: 深度分析 + URL 验证 ──────────────────────
    logger.info("research step 2 deep analysis started", extra={"event": "research_step_deep_analysis_started"})
    today = date.today()

    for c in track(to_deep, description="分析中..."):
        word = c["word"]
        screen = screen_map.get(word)
        logger.info(
            "research candidate deep analysis started",
            extra={
                "event": "research_candidate_started",
                "word": word,
            },
        )

        record = await _deep_analyze(
            word=word,
            sample_comments=c.get("sample_comments", ""),
            video_refs=c.get("video_refs", []),
            score=c["score"],
            today=today,
        )
        if record is None:
            result.failed_words.append(word)
            logger.warning(
                "research candidate deep analysis returned no record",
                extra={"event": "research_candidate_failed", "word": word},
            )
            continue

        # Step 3: URL 验证
        if record.source_urls:
            original_source_count = len(record.source_urls)
            valid_urls = await verify_urls(record.source_urls)
            logger.info(
                "research source urls verified",
                extra={
                    "event": "research_source_urls_verified",
                    "word": word,
                    "source_count": original_source_count,
                    "valid_source_count": len(valid_urls),
                },
            )
            record.source_urls = valid_urls
            # 有效来源少于预期时，适当降低置信度
            if original_source_count > 0 and len(valid_urls) < original_source_count / 2:
                record.confidence_score *= 0.8

        await _accept_candidate(word, record)
        result.add_accepted_record(record)
        logger.info(
            "research candidate accepted",
            extra={"event": "research_candidate_accepted", "word": word},
        )

    logger.info(
        "researcher completed",
        extra={
            "event": "research_completed",
            "accepted_count": result.accepted_count,
            "rejected_count": result.rejected_count,
            "failed_count": len(result.failed_words),
        },
    )
    return result
