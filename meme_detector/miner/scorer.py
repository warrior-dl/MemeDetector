"""
Miner 编排层：拆分为评论初筛与证据包生成两个阶段。
"""

from __future__ import annotations

from datetime import date

import meme_detector.miner.analysis as _analysis_module
import meme_detector.miner.bundler as _bundler_module
from meme_detector.config import settings
from meme_detector.logging_utils import get_logger
from meme_detector.miner.analysis import AsyncOpenAI
from meme_detector.miner.models import (
    MinerBundlesRunResult,
    MinerInsightsRunResult,
    MinerRunResult,
)
from meme_detector.miner.persistence import (
    list_pending_bundle_insights as _list_pending_bundle_insights,
    list_pending_scout_videos as _list_pending_scout_videos,
    mark_insight_bundle_failed as _mark_insight_bundle_failed,
    mark_insight_bundling as _mark_insight_bundling,
    mark_video_failed as _mark_video_failed,
    mark_video_mined as _mark_video_mined,
    mark_video_processing as _mark_video_processing,
    persist_comment_bundle as _persist_comment_bundle,
    persist_video_insights as _persist_video_insights,
    recover_processing_videos as _recover_processing_videos,
)
from meme_detector.miner.video_context import get_bilibili_video_context
from meme_detector.pipeline_service import update_job_runtime_progress
from meme_detector.researcher.tools import volcengine_web_search, volcengine_web_search_summary
from meme_detector.run_tracker import get_current_run_id

logger = get_logger(__name__)


async def _score_video_comments(video: dict, comments: list[str]) -> list[dict]:
    _analysis_module.AsyncOpenAI = AsyncOpenAI
    _analysis_module.get_bilibili_video_context = get_bilibili_video_context
    _analysis_module.get_current_run_id = get_current_run_id
    return await _analysis_module.score_video_comments(video, comments)


async def _build_bundles(video: dict, insights: list[dict]):
    _bundler_module.AsyncOpenAI = AsyncOpenAI
    _bundler_module.volcengine_web_search_summary = volcengine_web_search_summary
    _bundler_module.volcengine_web_search = volcengine_web_search
    return await _bundler_module.build_bundles_from_insights(video, insights)


def _count_high_value_insights(insights: list[dict]) -> int:
    return sum(
        1
        for item in insights
        if item.get("confidence", 0.0) >= settings.miner_comment_confidence_threshold
        and (item.get("is_meme_candidate") or item.get("is_insider_knowledge"))
    )


async def run_miner_insights(target_date: date | None = None) -> MinerInsightsRunResult:
    """Miner Stage 1：对 Scout 视频做评论初筛。"""
    today = target_date or date.today()
    logger.info(
        "miner insights started",
        extra={"event": "miner_insights_started", "target_date": today.isoformat()},
    )
    recovered_processing_count = _recover_processing_videos()
    if recovered_processing_count:
        logger.warning(
            "miner insights recovered stale processing videos",
            extra={
                "event": "miner_insights_recovered_stale_processing_videos",
                "video_count": recovered_processing_count,
            },
        )

    pending_videos = _list_pending_scout_videos()
    if not pending_videos:
        update_job_runtime_progress(
            "miner_insights",
            phase="idle",
            current=0,
            total=0,
            unit="视频",
            message="没有待做评论初筛的视频",
        )
        return MinerInsightsRunResult(target_date=today.isoformat())

    update_job_runtime_progress(
        "miner_insights",
        phase="preparing",
        current=0,
        total=len(pending_videos),
        unit="视频",
        message=f"已载入 {len(pending_videos)} 个待初筛视频",
    )

    insight_count = 0
    high_value_count = 0
    failed_video_count = 0
    for video_index, video in enumerate(pending_videos, 1):
        bvid = str(video.get("bvid", "")).strip() or "UNKNOWN"
        comments = video.get("comments", [])
        if not isinstance(comments, list):
            comments = []
        comments = [str(comment).strip() for comment in comments if str(comment).strip()]
        update_job_runtime_progress(
            "miner_insights",
            phase="processing",
            current=video_index - 1,
            total=len(pending_videos),
            unit="视频",
            message=f"正在初筛第 {video_index}/{len(pending_videos)} 个视频：{bvid}",
        )
        try:
            _mark_video_processing(video)
            if not comments:
                _mark_video_mined(video)
                continue

            insights = await _score_video_comments(video, comments)
            _persist_video_insights(video, insights)
        except Exception as exc:
            failed_video_count += 1
            _mark_video_failed(video, str(exc))
            logger.exception(
                "miner insights video failed",
                extra={"event": "miner_insights_video_failed", "bvid": bvid},
            )
            update_job_runtime_progress(
                "miner_insights",
                phase="processing",
                current=video_index,
                total=len(pending_videos),
                unit="视频",
                message=f"评论初筛失败，已标记为 failed：{bvid}",
            )
            continue

        current_high_value_count = _count_high_value_insights(insights)
        insight_count += len(insights)
        high_value_count += current_high_value_count
        logger.info(
            "miner insights video persisted",
            extra={
                "event": "miner_insights_video_persisted",
                "bvid": bvid,
                "result_count": len(insights),
                "high_value_count": current_high_value_count,
            },
        )
        update_job_runtime_progress(
            "miner_insights",
            phase="processing",
            current=video_index,
            total=len(pending_videos),
            unit="视频",
            message=(
                f"已完成 {video_index}/{len(pending_videos)} 个视频，"
                f"累计 {insight_count} 条评论线索 / {high_value_count} 条高价值"
            ),
        )

    return MinerInsightsRunResult(
        target_date=today.isoformat(),
        video_count=len(pending_videos),
        insight_count=insight_count,
        high_value_count=high_value_count,
        failed_video_count=failed_video_count,
    )


async def run_miner_bundles(target_date: date | None = None) -> MinerBundlesRunResult:
    """Miner Stage 2：对高价值评论线索生成证据包。"""
    today = target_date or date.today()
    logger.info(
        "miner bundles started",
        extra={"event": "miner_bundles_started", "target_date": today.isoformat()},
    )

    pending_insights = _list_pending_bundle_insights(limit=settings.ai_batch_size * 20)
    if not pending_insights:
        update_job_runtime_progress(
            "miner_bundles",
            phase="idle",
            current=0,
            total=0,
            unit="评论",
            message="没有待生成证据包的高价值评论",
        )
        return MinerBundlesRunResult(target_date=today.isoformat())

    update_job_runtime_progress(
        "miner_bundles",
        phase="preparing",
        current=0,
        total=len(pending_insights),
        unit="评论",
        message=f"已载入 {len(pending_insights)} 条待生成证据包的评论",
    )

    bundled_count = 0
    failed_insight_count = 0
    for insight_index, insight in enumerate(pending_insights, 1):
        insight_id = str(insight.get("insight_id", "")).strip() or "UNKNOWN"
        bvid = str(insight.get("bvid", "")).strip() or "UNKNOWN"
        update_job_runtime_progress(
            "miner_bundles",
            phase="bundling",
            current=insight_index - 1,
            total=len(pending_insights),
            unit="评论",
            message=f"正在生成第 {insight_index}/{len(pending_insights)} 条评论的证据包：{bvid}",
        )
        try:
            _mark_insight_bundling(insight)
            video = {
                "bvid": insight.get("bvid"),
                "collected_date": insight.get("collected_date"),
                "partition": insight.get("partition"),
                "title": insight.get("title"),
                "description": insight.get("description"),
                "url": insight.get("url") or insight.get("video_url"),
                "video_url": insight.get("url") or insight.get("video_url"),
                "tags": insight.get("tags") or [],
            }
            bundles = await _build_bundles(video, [insight])
            if not bundles:
                raise RuntimeError("bundle generation returned no bundles")
            for bundle in bundles:
                _persist_comment_bundle(bundle)
            bundled_count += len(bundles)
        except Exception:
            failed_insight_count += 1
            _mark_insight_bundle_failed(insight)
            logger.exception(
                "miner bundles failed",
                extra={
                    "event": "miner_bundles_failed",
                    "insight_id": insight_id,
                    "bvid": bvid,
                },
            )
            update_job_runtime_progress(
                "miner_bundles",
                phase="bundling",
                current=insight_index,
                total=len(pending_insights),
                unit="评论",
                message=f"证据包生成失败：{insight_id}",
            )
            continue

        update_job_runtime_progress(
            "miner_bundles",
            phase="bundling",
            current=insight_index,
            total=len(pending_insights),
            unit="评论",
            message=(
                f"已完成 {insight_index}/{len(pending_insights)} 条评论，"
                f"累计生成 {bundled_count} 个证据包"
            ),
        )

    return MinerBundlesRunResult(
        target_date=today.isoformat(),
        queued_insight_count=len(pending_insights),
        bundled_count=bundled_count,
        failed_insight_count=failed_insight_count,
    )


async def run_miner(target_date: date | None = None) -> MinerRunResult:
    """组合执行两个 Miner 阶段，供测试或串行手动运行使用。"""
    insights_result = await run_miner_insights(target_date=target_date)
    bundles_result = await run_miner_bundles(target_date=target_date)
    return MinerRunResult(
        target_date=insights_result.target_date,
        video_count=insights_result.video_count,
        insight_count=insights_result.insight_count,
        high_value_count=insights_result.high_value_count,
        bundle_count=bundles_result.bundled_count,
        failed_video_count=insights_result.failed_video_count,
    )
