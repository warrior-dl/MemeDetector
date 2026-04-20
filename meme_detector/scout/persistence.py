"""
Scout 持久化服务。
"""

from __future__ import annotations

import asyncio
from contextlib import closing
from datetime import date

from meme_detector.archivist.schema import get_conn
from meme_detector.archivist.scout_store import upsert_scout_raw_videos


def _persist_raw_videos_sync(videos: list[dict], collected_date: date) -> dict:
    with closing(get_conn()) as conn:
        return upsert_scout_raw_videos(conn, videos, collected_date)


async def persist_raw_videos(videos: list[dict], collected_date: date) -> dict:
    return await asyncio.to_thread(_persist_raw_videos_sync, videos, collected_date)
