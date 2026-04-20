"""
Scout 领域 DuckDB 读写。
"""

from __future__ import annotations

from collections.abc import Callable
import hashlib
import json
import mimetypes
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import duckdb

from meme_detector.config import settings
from meme_detector.logging_utils import get_logger
from meme_detector.archivist.sql_utils import build_where_clause, count_rows

logger = get_logger(__name__)


def upsert_scout_raw_videos(
    conn: duckdb.DuckDBPyConnection,
    videos: list[dict],
    target_date: date,
    *,
    download_media_asset: Callable[[str], dict] | None = None,
) -> dict:
    stats = {
        "input_count": len(videos),
        "prepared_count": 0,
        "inserted_count": 0,
        "updated_count": 0,
        "same_day_unchanged_count": 0,
        "cross_day_duplicate_count": 0,
        "persisted_count": 0,
        "invalid_count": 0,
    }
    if not videos:
        return stats

    media_downloader = download_media_asset or _download_media_asset
    rows = []
    structured_videos: list[dict] = []
    now = datetime.now()
    for video in videos:
        prepared = _prepare_scout_video_payload(video)
        if not prepared["bvid"] or not prepared["partition"] or not prepared["url"]:
            stats["invalid_count"] += 1
            continue
        structured_videos.append(prepared)
        stats["prepared_count"] += 1
        rows.append(
            (
                prepared["bvid"],
                target_date,
                prepared["partition"],
                prepared["title"],
                prepared["description"],
                prepared["url"],
                json.dumps(prepared["tags"], ensure_ascii=False),
                json.dumps(prepared["comments"], ensure_ascii=False),
                len(prepared["comments"]),
                now,
            )
        )

    if not rows:
        logger.info(
            "scout raw videos skipped because no valid rows",
            extra={
                "event": "scout_raw_videos_no_valid_rows",
                "target_date": target_date.isoformat(),
                **stats,
            },
        )
        return stats

    for row, video in zip(rows, structured_videos, strict=False):
        signature = _build_scout_video_signature(
            partition=video["partition"],
            title=video["title"],
            description=video["description"],
            video_url=video["url"],
            tags=video["tags"],
            comments=video["comments"],
        )
        bvid = video["bvid"]

        existing_row = conn.execute(
            """
            SELECT
                partition,
                title,
                description,
                video_url,
                tags_json,
                comments_json
            FROM scout_raw_videos
            WHERE bvid = ? AND collected_date = ?
            """,
            [bvid, target_date],
        ).fetchone()
        if existing_row:
            existing_signature = _build_scout_video_signature(
                partition=str(existing_row[0] or "").strip(),
                title=str(existing_row[1] or "").strip(),
                description=str(existing_row[2] or "").strip(),
                video_url=str(existing_row[3] or "").strip(),
                tags=_load_json_text(existing_row[4], default=[]),
                comments=_load_json_text(existing_row[5], default=[]),
            )
            if existing_signature == signature:
                stats["same_day_unchanged_count"] += 1
                continue

        if not existing_row and _has_duplicate_scout_snapshot(
            conn,
            bvid=bvid,
            signature=signature,
            exclude_date=target_date,
        ):
            stats["cross_day_duplicate_count"] += 1
            continue

        conn.execute(
            """
            INSERT INTO scout_raw_videos (
                bvid,
                collected_date,
                partition,
                title,
                description,
                video_url,
                tags_json,
                comments_json,
                comment_count,
                research_status,
                research_started_at,
                miner_status,
                miner_started_at,
                miner_processed_at,
                miner_failed_at,
                miner_last_error,
                miner_attempt_count,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, 'pending', NULL, NULL, NULL, '', 0, ?, ?)
            ON CONFLICT (bvid, collected_date) DO UPDATE
            SET partition = excluded.partition,
                title = excluded.title,
                description = excluded.description,
                video_url = excluded.video_url,
                tags_json = excluded.tags_json,
                comments_json = excluded.comments_json,
                comment_count = excluded.comment_count,
                research_status = 'pending',
                research_started_at = NULL,
                miner_status = 'pending',
                miner_started_at = NULL,
                miner_processed_at = NULL,
                miner_failed_at = NULL,
                miner_last_error = '',
                miner_attempt_count = 0,
                updated_at = excluded.updated_at
            """,
            [*row, row[-1]],
        )
        if existing_row:
            stats["updated_count"] += 1
        else:
            stats["inserted_count"] += 1
        stats["persisted_count"] += 1
        _upsert_scout_raw_comments(
            conn,
            video=video,
            target_date=target_date,
            collected_at=now,
            download_media_asset=media_downloader,
        )

    logger.info(
        "scout raw videos persisted",
        extra={
            "event": "scout_raw_videos_persisted",
            "target_date": target_date.isoformat(),
            **stats,
        },
    )
    return stats


def get_pending_scout_raw_videos(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int | None = None,
) -> list[dict]:
    sql = """
        SELECT
            bvid,
            collected_date,
            partition,
            title,
            description,
            video_url,
            tags_json,
            comments_json,
            comment_count,
            miner_status,
            miner_started_at,
            miner_processed_at,
            miner_failed_at,
            miner_last_error,
            miner_attempt_count,
            created_at,
            updated_at
        FROM scout_raw_videos
        WHERE miner_status IN ('pending', 'failed')
        ORDER BY collected_date ASC, bvid ASC
    """
    params: list[int] = []
    if limit is not None:
        sql += "\nLIMIT ?"
        params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [
        {
            "bvid": row[0],
            "collected_date": row[1],
            "partition": row[2],
            "title": row[3],
            "description": row[4],
            "url": row[5],
            "tags": _load_json_text(row[6], default=[]),
            "comments": _load_json_text(row[7], default=[]),
            "comment_count": row[8],
            "miner_status": row[9],
            "miner_started_at": row[10],
            "miner_processed_at": row[11],
            "miner_failed_at": row[12],
            "miner_last_error": row[13] or "",
            "miner_attempt_count": int(row[14] or 0),
            "created_at": row[15],
            "updated_at": row[16],
        }
        for row in rows
    ]


def mark_scout_raw_videos_miner_processing(
    conn: duckdb.DuckDBPyConnection,
    videos: list[dict],
) -> None:
    rows = []
    now = datetime.now()
    for video in videos:
        bvid = str(video.get("bvid", "")).strip()
        collected_date = video.get("collected_date")
        if not bvid or not collected_date:
            continue
        rows.append((now, now, bvid, collected_date))
    if not rows:
        return
    conn.executemany(
        """
        UPDATE scout_raw_videos
        SET miner_status = 'processing',
            miner_started_at = ?,
            miner_failed_at = NULL,
            miner_last_error = '',
            miner_attempt_count = COALESCE(miner_attempt_count, 0) + 1,
            updated_at = ?
        WHERE bvid = ? AND collected_date = ?
        """,
        rows,
    )


def mark_scout_raw_videos_mined(
    conn: duckdb.DuckDBPyConnection,
    videos: list[dict],
) -> None:
    rows = []
    now = datetime.now()
    for video in videos:
        bvid = str(video.get("bvid", "")).strip()
        collected_date = video.get("collected_date")
        if not bvid or not collected_date:
            continue
        rows.append((now, bvid, collected_date))
    if not rows:
        return
    conn.executemany(
        """
        UPDATE scout_raw_videos
        SET miner_status = 'processed',
            miner_started_at = NULL,
            miner_processed_at = ?,
            miner_failed_at = NULL,
            miner_last_error = '',
            updated_at = ?
        WHERE bvid = ? AND collected_date = ?
        """,
        [(row[0], row[0], row[1], row[2]) for row in rows],
    )


def mark_scout_raw_videos_miner_failed(
    conn: duckdb.DuckDBPyConnection,
    videos: list[dict],
    *,
    error_message: str,
) -> None:
    rows = []
    now = datetime.now()
    message = str(error_message).strip() or "miner failed"
    for video in videos:
        bvid = str(video.get("bvid", "")).strip()
        collected_date = video.get("collected_date")
        if not bvid or not collected_date:
            continue
        rows.append((now, message, now, bvid, collected_date))
    if not rows:
        return
    conn.executemany(
        """
        UPDATE scout_raw_videos
        SET miner_status = 'failed',
            miner_started_at = NULL,
            miner_failed_at = ?,
            miner_last_error = ?,
            updated_at = ?
        WHERE bvid = ? AND collected_date = ?
        """,
        rows,
    )


def recover_stale_miner_processing_videos(conn: duckdb.DuckDBPyConnection) -> int:
    now = datetime.now()
    result = conn.execute(
        """
        UPDATE scout_raw_videos
        SET miner_status = 'failed',
            miner_started_at = NULL,
            miner_failed_at = ?,
            miner_last_error = CASE
                WHEN COALESCE(TRIM(miner_last_error), '') = '' THEN 'previous miner run interrupted'
                ELSE miner_last_error
            END,
            updated_at = ?
        WHERE miner_status = 'processing'
        """,
        [now, now],
    )
    rowcount = getattr(result, "rowcount", -1)
    return max(0, int(rowcount or 0))


def get_scout_raw_videos_page(
    conn: duckdb.DuckDBPyConnection,
    *,
    research_status: str | None = None,
    partition: str | None = None,
    keyword: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    where_parts: list[str] = []
    params: list[str | int] = []
    if research_status:
        where_parts.append("research_status = ?")
        params.append(research_status)
    if partition:
        where_parts.append("partition LIKE ?")
        params.append(f"%{partition}%")
    if keyword:
        where_parts.append("(bvid LIKE ? OR title LIKE ? OR description LIKE ?)")
        keyword_like = f"%{keyword}%"
        params.extend([keyword_like, keyword_like, keyword_like])

    where_clause = build_where_clause(where_parts)
    total = count_rows(
        conn,
        from_clause="scout_raw_videos",
        where_clause=where_clause,
        params=params,
    )

    rows = conn.execute(
        f"""
        SELECT
            bvid,
            collected_date,
            partition,
            title,
            video_url,
            tags_json,
            comment_count,
            miner_status,
            miner_started_at,
            miner_processed_at,
            miner_failed_at,
            miner_last_error,
            miner_attempt_count,
            research_status,
            research_started_at,
            created_at,
            updated_at,
            comments_json,
            COALESCE((
                SELECT SUM(picture_count)
                FROM scout_raw_comments comments
                WHERE comments.bvid = scout_raw_videos.bvid
                  AND comments.collected_date = scout_raw_videos.collected_date
            ), 0) AS picture_count,
            COALESCE((
                SELECT COUNT(*)
                FROM miner_comment_insights insights
                WHERE insights.bvid = scout_raw_videos.bvid
                  AND insights.collected_date = scout_raw_videos.collected_date
                  AND insights.confidence >= ?
                  AND (insights.is_meme_candidate OR insights.is_insider_knowledge)
            ), 0) AS high_value_comment_count,
            COALESCE((
                SELECT COUNT(*)
                FROM comment_insights bundles
                WHERE bundles.bvid = scout_raw_videos.bvid
                  AND bundles.collected_date = scout_raw_videos.collected_date
            ), 0) AS bundle_count
        FROM scout_raw_videos
        {where_clause}
        ORDER BY collected_date DESC, updated_at DESC, bvid ASC
        LIMIT ?
        OFFSET ?
        """,
        [settings.miner_comment_confidence_threshold, *params, limit, offset],
    ).fetchall()

    return {
        "items": [_serialize_scout_raw_video_summary(row) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def get_scout_raw_video(
    conn: duckdb.DuckDBPyConnection,
    *,
    bvid: str,
    collected_date: date,
) -> dict | None:
    row = conn.execute(
        """
        SELECT
            bvid,
            collected_date,
            partition,
            title,
            description,
            video_url,
            tags_json,
            comments_json,
            comment_count,
            miner_status,
            miner_started_at,
            miner_processed_at,
            miner_failed_at,
            miner_last_error,
            miner_attempt_count,
            research_status,
            research_started_at,
            created_at,
            updated_at,
            COALESCE((
                SELECT SUM(picture_count)
                FROM scout_raw_comments comments
                WHERE comments.bvid = scout_raw_videos.bvid
                  AND comments.collected_date = scout_raw_videos.collected_date
            ), 0) AS picture_count,
            COALESCE((
                SELECT COUNT(*)
                FROM miner_comment_insights insights
                WHERE insights.bvid = scout_raw_videos.bvid
                  AND insights.collected_date = scout_raw_videos.collected_date
                  AND insights.confidence >= ?
                  AND (insights.is_meme_candidate OR insights.is_insider_knowledge)
            ), 0) AS high_value_comment_count,
            COALESCE((
                SELECT COUNT(*)
                FROM comment_insights bundles
                WHERE bundles.bvid = scout_raw_videos.bvid
                  AND bundles.collected_date = scout_raw_videos.collected_date
            ), 0) AS bundle_count
        FROM scout_raw_videos
        WHERE bvid = ? AND collected_date = ?
        """,
        [settings.miner_comment_confidence_threshold, bvid, collected_date],
    ).fetchone()
    if not row:
        return None
    snapshot = _serialize_scout_raw_video(row)
    snapshot["comment_snapshots"] = list_scout_raw_comments_for_video(
        conn,
        bvid=bvid,
        collected_date=collected_date,
    )
    snapshot["comments_with_pictures"] = sum(1 for item in snapshot["comment_snapshots"] if item["pictures"])
    return snapshot


def update_scout_raw_video_stage(
    conn: duckdb.DuckDBPyConnection,
    *,
    bvid: str,
    collected_date: date,
    stage: str,
) -> dict | None:
    if stage not in {"scouted", "mined", "researched"}:
        raise ValueError(f"unsupported scout stage: {stage}")

    existing = conn.execute(
        """
        SELECT 1
        FROM scout_raw_videos
        WHERE bvid = ? AND collected_date = ?
        """,
        [bvid, collected_date],
    ).fetchone()
    if not existing:
        return None

    now = datetime.now()
    affected_insight_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM miner_comment_insights
        WHERE bvid = ? AND collected_date = ?
        """,
        [bvid, collected_date],
    ).fetchone()[0]

    if stage == "scouted":
        conn.execute(
            """
            UPDATE scout_raw_videos
            SET miner_status = 'pending',
                miner_started_at = NULL,
                miner_processed_at = NULL,
                miner_failed_at = NULL,
                miner_last_error = '',
                miner_attempt_count = 0,
                research_status = 'pending',
                research_started_at = NULL,
                updated_at = ?
            WHERE bvid = ? AND collected_date = ?
            """,
            [now, bvid, collected_date],
        )
        conn.execute(
            """
            UPDATE miner_comment_insights
            SET status = CASE
                    WHEN confidence >= ? AND (is_meme_candidate OR is_insider_knowledge)
                        THEN 'pending_bundle'
                    ELSE 'discarded'
                END,
                updated_at = ?
            WHERE bvid = ? AND collected_date = ?
            """,
            [settings.miner_comment_confidence_threshold, now, bvid, collected_date],
        )
    elif stage == "mined":
        conn.execute(
            """
            UPDATE scout_raw_videos
            SET miner_status = 'processed',
                miner_started_at = NULL,
                miner_processed_at = COALESCE(miner_processed_at, ?),
                miner_failed_at = NULL,
                miner_last_error = '',
                research_status = 'pending',
                research_started_at = NULL,
                updated_at = ?
            WHERE bvid = ? AND collected_date = ?
            """,
            [now, now, bvid, collected_date],
        )
        conn.execute(
            """
            UPDATE miner_comment_insights
            SET status = CASE
                    WHEN confidence >= ? AND (is_meme_candidate OR is_insider_knowledge)
                        THEN 'pending_bundle'
                    ELSE 'discarded'
                END,
                updated_at = ?
            WHERE bvid = ? AND collected_date = ?
            """,
            [settings.miner_comment_confidence_threshold, now, bvid, collected_date],
        )
    else:
        conn.execute(
            """
            UPDATE scout_raw_videos
            SET miner_status = 'processed',
                miner_started_at = NULL,
                miner_processed_at = COALESCE(miner_processed_at, ?),
                miner_failed_at = NULL,
                miner_last_error = '',
                research_status = 'processed',
                research_started_at = COALESCE(research_started_at, ?),
                updated_at = ?
            WHERE bvid = ? AND collected_date = ?
            """,
            [now, now, now, bvid, collected_date],
        )
        conn.execute(
            """
            UPDATE miner_comment_insights
            SET status = CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM comment_insights ci
                        WHERE ci.insight_id = miner_comment_insights.insight_id
                    ) THEN 'bundled'
                    WHEN confidence >= ? AND (is_meme_candidate OR is_insider_knowledge)
                        THEN 'pending_bundle'
                    ELSE 'discarded'
                END,
                updated_at = ?
            WHERE bvid = ? AND collected_date = ?
            """,
            [settings.miner_comment_confidence_threshold, now, bvid, collected_date],
        )

    snapshot = get_scout_raw_video(conn, bvid=bvid, collected_date=collected_date)
    if not snapshot:
        return None
    snapshot["requested_stage"] = stage
    snapshot["affected_insight_count"] = affected_insight_count
    return snapshot


def list_scout_raw_comments_for_video(
    conn: duckdb.DuckDBPyConnection,
    *,
    bvid: str,
    collected_date: date,
) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            rpid,
            root_rpid,
            parent_rpid,
            mid,
            uname,
            message,
            like_count,
            reply_count,
            ctime,
            picture_count,
            has_pictures,
            content_json,
            raw_reply_json,
            created_at,
            updated_at
        FROM scout_raw_comments
        WHERE bvid = ? AND collected_date = ?
        ORDER BY like_count DESC, ctime ASC, rpid ASC
        """,
        [bvid, collected_date],
    ).fetchall()
    if not rows:
        return []

    media_rows = conn.execute(
        """
        SELECT
            links.rpid,
            assets.asset_id,
            assets.source_url,
            assets.storage_path,
            assets.width,
            assets.height,
            assets.byte_size,
            assets.download_status,
            assets.mime_type,
            assets.file_ext,
            links.image_index
        FROM comment_media_links links
        JOIN media_assets assets ON assets.asset_id = links.asset_id
        WHERE links.bvid = ? AND links.collected_date = ?
        ORDER BY links.rpid ASC, links.image_index ASC
        """,
        [bvid, collected_date],
    ).fetchall()

    media_by_rpid: dict[int, list[dict]] = {}
    for row in media_rows:
        media_by_rpid.setdefault(int(row[0]), []).append(
            {
                "asset_id": row[1],
                "source_url": row[2],
                "storage_path": row[3],
                "width": row[4],
                "height": row[5],
                "byte_size": row[6],
                "download_status": row[7],
                "mime_type": row[8],
                "file_ext": row[9],
                "image_index": row[10],
            }
        )

    return [
        {
            "rpid": row[0],
            "root_rpid": row[1],
            "parent_rpid": row[2],
            "mid": row[3],
            "uname": row[4],
            "message": row[5],
            "like_count": row[6],
            "reply_count": row[7],
            "ctime": row[8],
            "picture_count": row[9],
            "has_pictures": row[10],
            "content": _load_json_text(row[11], default={}),
            "raw_reply": _load_json_text(row[12], default={}),
            "created_at": row[13],
            "updated_at": row[14],
            "pictures": media_by_rpid.get(int(row[0]), []),
        }
        for row in rows
    ]


def get_media_asset(conn: duckdb.DuckDBPyConnection, asset_id: str) -> dict | None:
    row = conn.execute(
        """
        SELECT
            asset_id,
            asset_type,
            source_url,
            normalized_url,
            storage_path,
            sha256,
            mime_type,
            file_ext,
            width,
            height,
            byte_size,
            download_status,
            last_error,
            collected_at,
            downloaded_at,
            meta_json
        FROM media_assets
        WHERE asset_id = ?
        """,
        [asset_id],
    ).fetchone()
    if not row:
        return None
    return {
        "asset_id": row[0],
        "asset_type": row[1],
        "source_url": row[2],
        "normalized_url": row[3],
        "storage_path": row[4],
        "sha256": row[5],
        "mime_type": row[6],
        "file_ext": row[7],
        "width": row[8],
        "height": row[9],
        "byte_size": row[10],
        "download_status": row[11],
        "last_error": row[12],
        "collected_at": row[13],
        "downloaded_at": row[14],
        "meta": _load_json_text(row[15], default={}),
    }


def mark_scout_raw_videos_researched(
    conn: duckdb.DuckDBPyConnection,
    videos: list[dict],
) -> None:
    rows = []
    now = datetime.now()
    for video in videos:
        bvid = str(video.get("bvid", "")).strip()
        collected_date = video.get("collected_date")
        if not bvid or not collected_date:
            continue
        rows.append((now, bvid, collected_date))
    if not rows:
        return
    conn.executemany(
        """
        UPDATE scout_raw_videos
        SET research_status = 'processed',
            research_started_at = ?,
            updated_at = ?
        WHERE bvid = ? AND collected_date = ?
        """,
        [(row[0], row[0], row[1], row[2]) for row in rows],
    )


def _prepare_scout_video_payload(video: dict) -> dict:
    comments = _normalize_text_items(video.get("comments", []))
    comment_snapshots = _normalize_comment_snapshots(video.get("comment_snapshots", []))
    if comment_snapshots:
        comments = _normalize_text_items(
            [
                *comments,
                *[
                    str(snapshot.get("message", "")).strip()
                    for snapshot in comment_snapshots
                    if str(snapshot.get("message", "")).strip()
                ],
            ]
        )
    return {
        "bvid": str(video.get("bvid", "")).strip(),
        "partition": str(video.get("partition", "")).strip(),
        "title": str(video.get("title", "")).strip(),
        "description": str(video.get("description", "")).strip(),
        "url": str(video.get("url", "")).strip(),
        "tags": _normalize_text_items(video.get("tags", []) or []),
        "comments": comments,
        "comment_snapshots": comment_snapshots,
    }


def _normalize_text_items(values) -> list[str]:
    if not isinstance(values, list):
        return []
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        item = str(value).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        normalized.append(item)
    return normalized


def _normalize_comment_snapshots(values) -> list[dict]:
    if not isinstance(values, list):
        return []
    seen_keys: set[tuple] = set()
    normalized: list[dict] = []
    for value in values:
        if not isinstance(value, dict):
            continue
        message = str(value.get("message", "")).strip()
        rpid = _safe_int(value.get("rpid"))
        ctime = _safe_int(value.get("ctime"))
        uname = str(value.get("uname", "")).strip()
        dedup_key = ("rpid", rpid) if rpid else ("text", message, uname, ctime)
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

        normalized_value = dict(value)
        normalized_value["message"] = message
        if rpid is not None:
            normalized_value["rpid"] = rpid
        if ctime is not None:
            normalized_value["ctime"] = ctime
        pictures = normalized_value.get("pictures", [])
        if isinstance(pictures, list):
            picture_seen: set[str] = set()
            normalized_pictures: list[dict] = []
            for picture in pictures:
                if not isinstance(picture, dict):
                    continue
                source_url = str(picture.get("img_src", "")).strip()
                if not source_url or source_url in picture_seen:
                    continue
                picture_seen.add(source_url)
                normalized_pictures.append(picture)
            normalized_value["pictures"] = normalized_pictures
        else:
            normalized_value["pictures"] = []
        normalized.append(normalized_value)
    return normalized


def _build_scout_video_signature(
    *,
    partition: str,
    title: str,
    description: str,
    video_url: str,
    tags: list,
    comments: list,
) -> str:
    payload = {
        "partition": partition.strip(),
        "title": title.strip(),
        "description": description.strip(),
        "video_url": video_url.strip(),
        "tags": sorted(_normalize_text_items(tags)),
        "comments": sorted(_normalize_text_items(comments)),
    }
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()


def _has_duplicate_scout_snapshot(
    conn: duckdb.DuckDBPyConnection,
    *,
    bvid: str,
    signature: str,
    exclude_date: date,
) -> bool:
    rows = conn.execute(
        """
        SELECT
            collected_date,
            partition,
            title,
            description,
            video_url,
            tags_json,
            comments_json
        FROM scout_raw_videos
        WHERE bvid = ?
        """,
        [bvid],
    ).fetchall()
    for row in rows:
        if row[0] == exclude_date:
            continue
        existing_signature = _build_scout_video_signature(
            partition=str(row[1] or "").strip(),
            title=str(row[2] or "").strip(),
            description=str(row[3] or "").strip(),
            video_url=str(row[4] or "").strip(),
            tags=_load_json_text(row[5], default=[]),
            comments=_load_json_text(row[6], default=[]),
        )
        if existing_signature == signature:
            return True
    return False


def _upsert_scout_raw_comments(
    conn: duckdb.DuckDBPyConnection,
    *,
    video: dict,
    target_date: date,
    collected_at: datetime,
    download_media_asset: Callable[[str], dict],
) -> None:
    comment_snapshots = video.get("comment_snapshots", [])
    if not isinstance(comment_snapshots, list) or not comment_snapshots:
        return

    conn.execute("DELETE FROM comment_media_links WHERE bvid = ? AND collected_date = ?", [video["bvid"], target_date])
    conn.execute("DELETE FROM scout_raw_comments WHERE bvid = ? AND collected_date = ?", [video["bvid"], target_date])

    comment_rows = []
    media_links: list[tuple[int, str, date, str, int]] = []
    for snapshot in comment_snapshots:
        if not isinstance(snapshot, dict):
            continue
        rpid = int(snapshot.get("rpid") or 0)
        message = str(snapshot.get("message", "")).strip()
        if not rpid or not message:
            continue

        pictures = snapshot.get("pictures", [])
        if not isinstance(pictures, list):
            pictures = []

        ctime = snapshot.get("ctime")
        comment_rows.append(
            (
                rpid,
                video["bvid"],
                target_date,
                int(snapshot.get("root_rpid") or 0) or None,
                int(snapshot.get("parent_rpid") or 0) or None,
                int(snapshot.get("mid") or 0) or None,
                str(snapshot.get("uname", "")).strip(),
                message,
                int(snapshot.get("like_count") or 0),
                int(snapshot.get("reply_count") or 0),
                datetime.fromtimestamp(ctime) if ctime else None,
                len(pictures),
                bool(pictures),
                json.dumps(snapshot.get("content") or {}, ensure_ascii=False),
                json.dumps(snapshot.get("raw_reply") or {}, ensure_ascii=False),
                collected_at,
            )
        )

        for picture_index, picture in enumerate(pictures):
            if not isinstance(picture, dict):
                continue
            asset = _materialize_media_asset(
                conn,
                source_url=str(picture.get("img_src", "")).strip(),
                collected_at=collected_at,
                picture=picture,
                download_media_asset=download_media_asset,
            )
            if asset:
                media_links.append((rpid, video["bvid"], target_date, asset["asset_id"], picture_index))

    if comment_rows:
        conn.executemany(
            """
            INSERT INTO scout_raw_comments (
                rpid,
                bvid,
                collected_date,
                root_rpid,
                parent_rpid,
                mid,
                uname,
                message,
                like_count,
                reply_count,
                ctime,
                picture_count,
                has_pictures,
                content_json,
                raw_reply_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (rpid, bvid, collected_date) DO UPDATE
            SET root_rpid = excluded.root_rpid,
                parent_rpid = excluded.parent_rpid,
                mid = excluded.mid,
                uname = excluded.uname,
                message = excluded.message,
                like_count = excluded.like_count,
                reply_count = excluded.reply_count,
                ctime = excluded.ctime,
                picture_count = excluded.picture_count,
                has_pictures = excluded.has_pictures,
                content_json = excluded.content_json,
                raw_reply_json = excluded.raw_reply_json,
                updated_at = excluded.updated_at
            """,
            [(*row, row[-1]) for row in comment_rows],
        )

    if media_links:
        conn.executemany(
            """
            INSERT INTO comment_media_links (
                rpid,
                bvid,
                collected_date,
                asset_id,
                image_index,
                role
            )
            VALUES (?, ?, ?, ?, ?, 'comment_picture')
            ON CONFLICT (rpid, bvid, collected_date, asset_id) DO UPDATE
            SET image_index = excluded.image_index,
                role = excluded.role
            """,
            media_links,
        )


def _materialize_media_asset(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_url: str,
    collected_at: datetime,
    picture: dict,
    download_media_asset: Callable[[str], dict],
) -> dict | None:
    normalized_url = source_url.strip()
    if not normalized_url:
        return None

    existing = conn.execute(
        """
        SELECT
            asset_id,
            storage_path,
            download_status,
            source_url,
            width,
            height,
            byte_size
        FROM media_assets
        WHERE normalized_url = ?
        LIMIT 1
        """,
        [normalized_url],
    ).fetchone()
    if existing and existing[1] and Path(existing[1]).exists():
        return {
            "asset_id": existing[0],
            "storage_path": existing[1],
            "download_status": existing[2],
            "source_url": existing[3],
            "width": existing[4],
            "height": existing[5],
            "byte_size": existing[6],
        }

    asset = download_media_asset(normalized_url)
    asset_id = asset["asset_id"]
    conn.execute(
        """
        INSERT INTO media_assets (
            asset_id,
            asset_type,
            source_url,
            normalized_url,
            storage_path,
            sha256,
            mime_type,
            file_ext,
            width,
            height,
            byte_size,
            download_status,
            last_error,
            collected_at,
            downloaded_at,
            meta_json
        )
        VALUES (?, 'comment_image', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (asset_id) DO UPDATE
        SET source_url = excluded.source_url,
            normalized_url = excluded.normalized_url,
            storage_path = excluded.storage_path,
            sha256 = excluded.sha256,
            mime_type = excluded.mime_type,
            file_ext = excluded.file_ext,
            width = excluded.width,
            height = excluded.height,
            byte_size = excluded.byte_size,
            download_status = excluded.download_status,
            last_error = excluded.last_error,
            downloaded_at = excluded.downloaded_at,
            meta_json = excluded.meta_json
        """,
        [
            asset_id,
            normalized_url,
            normalized_url,
            asset["storage_path"],
            asset["sha256"],
            asset["mime_type"],
            asset["file_ext"],
            asset["width"] or picture.get("img_width"),
            asset["height"] or picture.get("img_height"),
            asset["byte_size"] or _safe_int(picture.get("img_size")),
            asset["download_status"],
            asset["last_error"],
            collected_at,
            datetime.now() if asset["download_status"] == "success" else None,
            json.dumps({"picture_meta": picture}, ensure_ascii=False),
        ],
    )
    return asset


def _download_media_asset(source_url: str) -> dict:
    fallback_asset_id = f"url_{hashlib.sha256(source_url.encode('utf-8')).hexdigest()}"
    try:
        request = Request(source_url, headers={"User-Agent": "Mozilla/5.0 MemeDetector/0.1"})
        with urlopen(request, timeout=settings.scout_request_timeout) as response:
            data = response.read()
            mime_type = response.info().get_content_type() or ""
        if not data:
            raise ValueError("empty image payload")

        sha256 = hashlib.sha256(data).hexdigest()
        file_ext = _guess_file_ext(source_url, mime_type)
        asset_root = Path(settings.media_asset_root).resolve() / "comment-images" / sha256[:2]
        asset_root.mkdir(parents=True, exist_ok=True)
        storage_path = asset_root / f"{sha256}{file_ext}"
        if not storage_path.exists():
            storage_path.write_bytes(data)

        return {
            "asset_id": sha256,
            "storage_path": str(storage_path),
            "sha256": sha256,
            "mime_type": mime_type,
            "file_ext": file_ext,
            "width": None,
            "height": None,
            "byte_size": len(data),
            "download_status": "success",
            "last_error": "",
        }
    except Exception as exc:
        return {
            "asset_id": fallback_asset_id,
            "storage_path": "",
            "sha256": "",
            "mime_type": "",
            "file_ext": "",
            "width": None,
            "height": None,
            "byte_size": None,
            "download_status": "failed",
            "last_error": str(exc),
        }


def _guess_file_ext(source_url: str, mime_type: str) -> str:
    ext = mimetypes.guess_extension(mime_type or "") or ""
    if not ext:
        parsed_path = urlparse(source_url).path
        ext = Path(parsed_path).suffix
    if not ext:
        ext = ".bin"
    if ext == ".jpe":
        return ".jpg"
    return ext.lower()


def _serialize_scout_raw_video(row: tuple) -> dict:
    tags = _load_json_text(row[6], default=[])
    comments = _load_json_text(row[7], default=[])
    return {
        "bvid": row[0],
        "collected_date": row[1],
        "partition": row[2],
        "title": row[3],
        "description": row[4],
        "video_url": row[5],
        "tags": [str(tag).strip() for tag in tags if str(tag).strip()],
        "comments": [str(comment) for comment in comments if str(comment).strip()],
        "comment_count": row[8],
        "miner_status": row[9],
        "miner_started_at": row[10],
        "miner_processed_at": row[11],
        "miner_failed_at": row[12],
        "miner_last_error": row[13] or "",
        "miner_attempt_count": int(row[14] or 0),
        "research_status": row[15],
        "research_started_at": row[16],
        "created_at": row[17],
        "updated_at": row[18],
        "picture_count": row[19],
        "high_value_comment_count": row[20],
        "bundle_count": row[21],
        "pipeline_stage": _build_scout_pipeline_stage(miner_status=row[9], research_status=row[15]),
    }


def _serialize_scout_raw_video_summary(row: tuple) -> dict:
    tags = _load_json_text(row[5], default=[])
    comments = _load_json_text(row[17], default=[])
    first_comment = next((str(comment).strip() for comment in comments if str(comment).strip()), "")
    return {
        "bvid": row[0],
        "collected_date": row[1],
        "partition": row[2],
        "title": row[3],
        "video_url": row[4],
        "tags": [str(tag).strip() for tag in tags if str(tag).strip()],
        "comment_count": row[6],
        "miner_status": row[7],
        "miner_started_at": row[8],
        "miner_processed_at": row[9],
        "miner_failed_at": row[10],
        "miner_last_error": row[11] or "",
        "miner_attempt_count": int(row[12] or 0),
        "research_status": row[13],
        "research_started_at": row[14],
        "created_at": row[15],
        "updated_at": row[16],
        "first_comment": first_comment,
        "picture_count": row[18],
        "high_value_comment_count": row[19],
        "bundle_count": row[20],
        "pipeline_stage": _build_scout_pipeline_stage(miner_status=row[7], research_status=row[13]),
    }


def _build_scout_pipeline_stage(*, miner_status: str, research_status: str) -> str:
    if research_status == "processed":
        return "researched"
    if miner_status == "failed":
        return "miner_failed"
    if miner_status == "processing":
        return "mining"
    if miner_status == "processed":
        return "mined"
    return "scouted"


def _safe_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _load_json_text(value: str | None, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default
