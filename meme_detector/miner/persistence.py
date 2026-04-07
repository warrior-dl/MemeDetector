"""
Miner 持久化服务。
"""

from __future__ import annotations

from contextlib import closing

from meme_detector.archivist.duckdb_store import (
    get_conn,
    get_pending_scout_raw_videos,
    mark_scout_raw_videos_mined,
    upsert_miner_comment_insights,
)


def list_pending_scout_videos() -> list[dict]:
    with closing(get_conn()) as conn:
        return get_pending_scout_raw_videos(conn)


def persist_video_insights(video: dict, insights: list[dict]) -> None:
    with closing(get_conn()) as conn:
        upsert_miner_comment_insights(conn, insights)
        mark_scout_raw_videos_mined(conn, [video])


def mark_video_mined(video: dict) -> None:
    with closing(get_conn()) as conn:
        mark_scout_raw_videos_mined(conn, [video])
