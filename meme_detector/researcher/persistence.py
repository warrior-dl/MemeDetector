"""
Research 持久化服务。
"""

from __future__ import annotations

from contextlib import closing

from meme_detector.archivist.duckdb_store import (
    get_conn,
    get_pending_candidates,
    get_pending_scout_raw_videos,
    upsert_meme_record,
    update_candidate_status,
)
from meme_detector.archivist.meili_store import upsert_meme
from meme_detector.researcher.models import MemeRecord


def list_pending_scout_videos() -> list[dict]:
    with closing(get_conn()) as conn:
        return get_pending_scout_raw_videos(conn)


def list_pending_candidates(*, limit: int) -> list[dict]:
    with closing(get_conn()) as conn:
        return get_pending_candidates(conn, limit=limit)


def reject_candidates(words: list[str]) -> None:
    if not words:
        return
    with closing(get_conn()) as conn:
        for word in words:
            update_candidate_status(conn, word, "rejected")


async def accept_candidate(word: str, record: MemeRecord) -> None:
    await upsert_meme(record)
    with closing(get_conn()) as conn:
        upsert_meme_record(conn, record)
        update_candidate_status(conn, word, "accepted")
