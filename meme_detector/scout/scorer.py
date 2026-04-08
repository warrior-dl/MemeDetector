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
    merged_by_bvid: dict[str, dict] = {}
    for partition, video_list in all_partition_data.items():
        for video in video_list:
            bvid = video.bvid.strip()
            if not bvid:
                continue
            merged = merged_by_bvid.setdefault(
                bvid,
                {
                    "bvid": bvid,
                    "partition": partition or video.partition,
                    "title": video.title,
                    "description": video.description,
                    "url": video.url,
                    "comments": [],
                    "tags": [],
                    "comment_snapshots": [],
                },
            )
            merged["comments"] = _merge_unique_strings(
                [*merged["comments"], *video.comments]
            )
            merged["tags"] = _merge_unique_strings(
                [*merged["tags"], *video.tags]
            )
            merged["comment_snapshots"] = _merge_comment_snapshots(
                [*merged["comment_snapshots"], *video.comment_snapshots]
            )

    flattened_videos = list(merged_by_bvid.values())
    total_comments = sum(len(video["comments"]) for video in flattened_videos)
    return flattened_videos, total_comments


def _merge_unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []
    for value in values:
        item = str(value).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        merged.append(item)
    return merged


def _merge_comment_snapshots(values: list[dict]) -> list[dict]:
    seen_keys: set[tuple] = set()
    merged: list[dict] = []
    for value in values:
        if not isinstance(value, dict):
            continue
        rpid = value.get("rpid")
        message = str(value.get("message", "")).strip()
        uname = str(value.get("uname", "")).strip()
        ctime = value.get("ctime")
        key = ("rpid", int(rpid)) if rpid else ("text", message, uname, ctime)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        merged.append(value)
    return merged


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
    raw_video_count = sum(len(video_list) for video_list in all_partition_data.values())
    flattened_videos, total_comments = _flatten_partition_videos(all_partition_data)

    total_videos = len(flattened_videos)
    logger.info(
        "scout collection summary",
        extra={
            "event": "scout_collection_summary",
            "target_date": today.isoformat(),
            "raw_video_count": raw_video_count,
            "video_count": total_videos,
            "comment_count": total_comments,
            "merged_duplicate_video_count": max(raw_video_count - total_videos, 0),
        },
    )

    persist_stats = persist_raw_videos(flattened_videos, today)
    logger.info(
        "scout persistence summary",
        extra={
            "event": "scout_persistence_summary",
            "target_date": today.isoformat(),
            **persist_stats,
        },
    )

    logger.info(
        "scout completed",
        extra={
            "event": "scout_completed",
            "target_date": today.isoformat(),
            "video_count": total_videos,
            "comment_count": total_comments,
            **persist_stats,
        },
    )
    return ScoutRunResult(
        target_date=today.isoformat(),
        video_count=total_videos,
        comment_count=total_comments,
    )
