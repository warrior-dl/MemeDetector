"""
Miner 主流程：预取视频内容并对评论做初步线索打分。
"""

from __future__ import annotations

from datetime import date

import meme_detector.miner.analysis as _analysis_module
from meme_detector.config import settings
from meme_detector.logging_utils import get_logger
from meme_detector.miner.analysis import AsyncOpenAI
from meme_detector.miner.models import MinerRunResult
from meme_detector.miner.persistence import (
    list_pending_scout_videos as _list_pending_scout_videos,
    mark_video_mined as _mark_video_mined,
    persist_video_insights as _persist_video_insights,
)
from meme_detector.miner.video_context import get_bilibili_video_context
from meme_detector.run_tracker import get_current_run_id

logger = get_logger(__name__)

async def _score_video_comments(video: dict, comments: list[str]) -> list[dict]:
    # 保持 scorer 模块作为稳定 monkeypatch 入口。
    _analysis_module.AsyncOpenAI = AsyncOpenAI
    _analysis_module.get_bilibili_video_context = get_bilibili_video_context
    _analysis_module.get_current_run_id = get_current_run_id
    return await _analysis_module.score_video_comments(video, comments)


async def run_miner(target_date: date | None = None) -> MinerRunResult:
    """对待处理的 Scout 原始视频做评论线索打分。"""
    today = target_date or date.today()
    logger.info("miner started", extra={"event": "miner_started", "target_date": today.isoformat()})

    pending_videos = _list_pending_scout_videos()
    if not pending_videos:
        logger.info("no pending scout videos for miner", extra={"event": "miner_no_pending_videos"})
        return MinerRunResult(target_date=today.isoformat())
    logger.info(
        "miner pending videos loaded",
        extra={
            "event": "miner_pending_videos_loaded",
            "target_date": today.isoformat(),
            "video_count": len(pending_videos),
        },
    )

    insight_count = 0
    high_value_count = 0
    for video_index, video in enumerate(pending_videos, 1):
        comments = video.get("comments", [])
        if not isinstance(comments, list):
            comments = []
        comments = [str(comment).strip() for comment in comments if str(comment).strip()]
        if not comments:
            _mark_video_mined(video)
            logger.info(
                "miner skipped video without valid comments",
                extra={
                    "event": "miner_video_skipped_no_comments",
                    "bvid": str(video.get("bvid", "")).strip() or "UNKNOWN",
                    "video_index": video_index,
                    "video_total": len(pending_videos),
                },
            )
            continue

        logger.info(
            "miner processing video",
            extra={
                "event": "miner_video_processing",
                "bvid": str(video.get("bvid", "")).strip() or "UNKNOWN",
                "video_index": video_index,
                "video_total": len(pending_videos),
                "comment_count": len(comments),
            },
        )
        insights = await _score_video_comments(video, comments)
        _persist_video_insights(video, insights)
        current_high_value_count = sum(
            1
            for item in insights
            if item.get("confidence", 0.0) >= settings.miner_comment_confidence_threshold
            and (item.get("is_meme_candidate") or item.get("is_insider_knowledge"))
        )
        insight_count += len(insights)
        high_value_count += current_high_value_count
        logger.info(
            "miner video persisted",
            extra={
                "event": "miner_video_persisted",
                "bvid": str(video.get("bvid", "")).strip() or "UNKNOWN",
                "result_count": len(insights),
                "high_value_count": current_high_value_count,
                "video_index": video_index,
                "video_total": len(pending_videos),
            },
        )

    logger.info(
        "miner completed",
        extra={
            "event": "miner_completed",
            "target_date": today.isoformat(),
            "result_count": insight_count,
            "high_value_count": high_value_count,
            "video_count": len(pending_videos),
        },
    )
    return MinerRunResult(
        target_date=today.isoformat(),
        video_count=len(pending_videos),
        insight_count=insight_count,
        high_value_count=high_value_count,
    )
