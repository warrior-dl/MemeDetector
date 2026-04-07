"""
Scout 持久化服务。
"""

from __future__ import annotations

from contextlib import closing
from datetime import date

from meme_detector.archivist.duckdb_store import get_conn, upsert_scout_raw_videos


def persist_raw_videos(videos: list[dict], collected_date: date) -> None:
    with closing(get_conn()) as conn:
        upsert_scout_raw_videos(conn, videos, collected_date)
