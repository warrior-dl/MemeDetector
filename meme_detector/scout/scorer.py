"""
Scout 主流程：采集高热评论和视频元数据，并直接写入原始库。
"""

from __future__ import annotations

from datetime import date

from meme_detector.logging_utils import get_logger
from meme_detector.scout.collector import collect_all_partitions
from meme_detector.scout.models import ScoutRunResult
from meme_detector.scout.persistence import persist_raw_videos

logger = get_logger(__name__)


def _flatten_partition_videos(all_partition_data: dict) -> tuple[list[dict], int]:
    flattened_videos: list[dict] = []
    total_comments = 0
    for partition, video_list in all_partition_data.items():
        for video in video_list:
            comments = [comment.strip() for comment in video.comments if comment.strip()]
            total_comments += len(comments)
            flattened_videos.append(
                {
                    "bvid": video.bvid,
                    "partition": partition or video.partition,
                    "title": video.title,
                    "description": video.description,
                    "url": video.url,
                    "comments": comments,
                    "tags": video.tags,
                    "comment_snapshots": video.comment_snapshots,
                }
            )
    return flattened_videos, total_comments


async def run_scout(target_date: date | None = None) -> ScoutRunResult:
    """
    完整 Scout 流程：
    1. 采集 B站各分区 Top 视频的高赞评论和视频元信息
    2. 将原始视频/评论快照写入 DuckDB

    候选词提取延后到 Researcher 阶段处理。
    """
    today = target_date or date.today()
    logger.info("scout started", extra={"event": "scout_started", "target_date": today.isoformat()})

    all_partition_data = await collect_all_partitions()
    flattened_videos, total_comments = _flatten_partition_videos(all_partition_data)

    total_videos = len(flattened_videos)
    logger.info(
        "scout collection summary",
        extra={
            "event": "scout_collection_summary",
            "target_date": today.isoformat(),
            "video_count": total_videos,
            "comment_count": total_comments,
        },
    )

    persist_raw_videos(flattened_videos, today)

    logger.info(
        "scout completed",
        extra={
            "event": "scout_completed",
            "target_date": today.isoformat(),
            "video_count": total_videos,
            "comment_count": total_comments,
        },
    )
    return ScoutRunResult(
        target_date=today.isoformat(),
        video_count=total_videos,
        comment_count=total_comments,
    )
