"""
Research 持久化服务。
"""

from __future__ import annotations

from contextlib import closing

from meme_detector.archivist.duckdb_store import (
    get_comment_bundle,
    get_conn,
    list_queued_comment_bundles,
    get_pending_scout_raw_videos,
    upsert_research_decision,
)
from meme_detector.pipeline_models import MinerBundle, ResearchDecision


def list_pending_scout_videos() -> list[dict]:
    with closing(get_conn()) as conn:
        return get_pending_scout_raw_videos(conn)


def list_queued_bundles(*, limit: int) -> list[dict]:
    with closing(get_conn()) as conn:
        return list_queued_comment_bundles(conn, limit=limit)


def load_bundle(bundle_id: str) -> MinerBundle | None:
    with closing(get_conn()) as conn:
        return get_comment_bundle(conn, bundle_id=bundle_id)


async def persist_research_decision(decision: ResearchDecision) -> None:
    from meme_detector.archivist.meili_store import upsert_meme

    if decision.record is not None:
        await upsert_meme(decision.record)
    with closing(get_conn()) as conn:
        upsert_research_decision(
            conn,
            decision,
            persist_record=decision.record is not None,
        )
