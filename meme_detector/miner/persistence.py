"""
Miner 持久化服务。
"""

from __future__ import annotations

from contextlib import closing

from meme_detector.archivist.duckdb_store import (
    get_conn,
    get_pending_miner_comment_insights,
    get_pending_scout_raw_videos,
    mark_miner_comment_insights_bundle_failed,
    mark_miner_comment_insights_bundling,
    mark_miner_comment_insights_processed,
    mark_scout_raw_videos_miner_failed,
    mark_scout_raw_videos_miner_processing,
    mark_scout_raw_videos_mined,
    recover_stale_miner_processing_videos,
    upsert_comment_bundle,
    upsert_miner_comment_insights,
)


def list_pending_scout_videos() -> list[dict]:
    with closing(get_conn()) as conn:
        return get_pending_scout_raw_videos(conn)


def recover_processing_videos() -> int:
    with closing(get_conn()) as conn:
        return recover_stale_miner_processing_videos(conn)


def list_pending_bundle_insights(*, limit: int = 200) -> list[dict]:
    with closing(get_conn()) as conn:
        return get_pending_miner_comment_insights(conn, limit=limit)


def mark_video_processing(video: dict) -> None:
    with closing(get_conn()) as conn:
        mark_scout_raw_videos_miner_processing(conn, [video])


def persist_video_insights(video: dict, insights: list[dict]) -> None:
    with closing(get_conn()) as conn:
        upsert_miner_comment_insights(conn, insights)
        mark_scout_raw_videos_mined(conn, [video])


def mark_insight_bundling(insight: dict) -> None:
    with closing(get_conn()) as conn:
        mark_miner_comment_insights_bundling(conn, [insight])


def persist_comment_bundle(bundle) -> None:
    with closing(get_conn()) as conn:
        upsert_comment_bundle(conn, bundle)
        mark_miner_comment_insights_processed(
            conn,
            [{"insight_id": bundle.insight.insight_id}],
        )


def mark_insight_bundle_failed(insight: dict) -> None:
    with closing(get_conn()) as conn:
        mark_miner_comment_insights_bundle_failed(conn, [insight])


def mark_video_mined(video: dict) -> None:
    with closing(get_conn()) as conn:
        mark_scout_raw_videos_mined(conn, [video])


def mark_video_failed(video: dict, error_message: str) -> None:
    with closing(get_conn()) as conn:
        mark_scout_raw_videos_miner_failed(conn, [video], error_message=error_message)
