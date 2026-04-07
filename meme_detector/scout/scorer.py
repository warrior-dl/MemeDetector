"""
Scout 主流程：采集高热评论和视频元数据，并直接写入原始库。
"""

from __future__ import annotations

from datetime import date

from rich.console import Console

from meme_detector.archivist.duckdb_store import get_conn, upsert_scout_raw_videos
from meme_detector.scout.collector import collect_all_partitions

console = Console()


async def run_scout(target_date: date | None = None) -> dict:
    """
    完整 Scout 流程：
    1. 采集 B站各分区 Top 视频的高赞评论和视频元信息
    2. 将原始视频/评论快照写入 DuckDB

    候选词提取延后到 Researcher 阶段处理。
    """
    today = target_date or date.today()
    console.print(f"\n[bold blue]═══ Scout 开始运行 {today} ═══[/bold blue]")

    all_partition_data = await collect_all_partitions()

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

    total_videos = len(flattened_videos)
    console.print(f"\n共采集 {total_videos} 个视频，{total_comments} 条高赞评论")

    conn = get_conn()
    upsert_scout_raw_videos(conn, flattened_videos, today)
    conn.close()

    console.print(
        f"[bold green]Scout 完成，已写入 {total_videos} 个视频快照和 {total_comments} 条评论[/bold green]"
    )
    return {
        "target_date": today.isoformat(),
        "video_count": total_videos,
        "comment_count": total_comments,
    }
