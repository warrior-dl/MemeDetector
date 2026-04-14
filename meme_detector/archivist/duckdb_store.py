"""
DuckDB 存储层：管理采集快照、评论证据包与运行审计数据。
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from uuid import uuid4

import duckdb

from meme_detector.config import settings
from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)

_CREATE_SCOUT_RAW_VIDEOS = """
CREATE TABLE IF NOT EXISTS scout_raw_videos (
    bvid                  TEXT      NOT NULL,
    collected_date        DATE      NOT NULL,
    partition             TEXT      NOT NULL,
    title                 TEXT      DEFAULT '',
    description           TEXT      DEFAULT '',
    video_url             TEXT      NOT NULL,
    tags_json             TEXT      DEFAULT '[]',
    comments_json         TEXT      DEFAULT '[]',
    comment_count         INTEGER   DEFAULT 0,
    research_status       TEXT      NOT NULL DEFAULT 'pending',
    research_started_at   TIMESTAMP,
    miner_status          TEXT      NOT NULL DEFAULT 'pending',
    miner_started_at      TIMESTAMP,
    miner_processed_at    TIMESTAMP,
    miner_failed_at       TIMESTAMP,
    miner_last_error      TEXT      DEFAULT '',
    miner_attempt_count   INTEGER   NOT NULL DEFAULT 0,
    created_at            TIMESTAMP DEFAULT NOW(),
    updated_at            TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (bvid, collected_date)
);
"""

_CREATE_SCOUT_RAW_COMMENTS = """
CREATE TABLE IF NOT EXISTS scout_raw_comments (
    rpid                BIGINT    NOT NULL,
    bvid                TEXT      NOT NULL,
    collected_date      DATE      NOT NULL,
    root_rpid           BIGINT,
    parent_rpid         BIGINT,
    mid                 BIGINT,
    uname               TEXT      DEFAULT '',
    message             TEXT      DEFAULT '',
    like_count          INTEGER   DEFAULT 0,
    reply_count         INTEGER   DEFAULT 0,
    ctime               TIMESTAMP,
    picture_count       INTEGER   DEFAULT 0,
    has_pictures        BOOLEAN   DEFAULT FALSE,
    content_json        TEXT      DEFAULT '{}',
    raw_reply_json      TEXT      DEFAULT '{}',
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (rpid, bvid, collected_date)
);
"""

_CREATE_MEDIA_ASSETS = """
CREATE TABLE IF NOT EXISTS media_assets (
    asset_id            TEXT      PRIMARY KEY,
    asset_type          TEXT      NOT NULL,
    source_url          TEXT      NOT NULL,
    normalized_url      TEXT      DEFAULT '',
    storage_path        TEXT      DEFAULT '',
    sha256              TEXT      DEFAULT '',
    mime_type           TEXT      DEFAULT '',
    file_ext            TEXT      DEFAULT '',
    width               INTEGER,
    height              INTEGER,
    byte_size           BIGINT,
    download_status     TEXT      NOT NULL DEFAULT 'pending',
    last_error          TEXT      DEFAULT '',
    collected_at        TIMESTAMP DEFAULT NOW(),
    downloaded_at       TIMESTAMP,
    meta_json           TEXT      DEFAULT '{}'
);
"""

_CREATE_COMMENT_MEDIA_LINKS = """
CREATE TABLE IF NOT EXISTS comment_media_links (
    rpid                BIGINT    NOT NULL,
    bvid                TEXT      NOT NULL,
    collected_date      DATE      NOT NULL,
    asset_id            TEXT      NOT NULL,
    image_index         INTEGER   NOT NULL,
    role                TEXT      DEFAULT 'comment_picture',
    PRIMARY KEY (rpid, bvid, collected_date, asset_id)
);
"""

_MIGRATE_SCOUT_RAW_VIDEOS_TAGS = """
ALTER TABLE scout_raw_videos ADD COLUMN IF NOT EXISTS tags_json TEXT DEFAULT '[]';
"""

_MIGRATE_SCOUT_RAW_VIDEOS_MINER_STATUS = """
ALTER TABLE scout_raw_videos ADD COLUMN IF NOT EXISTS miner_status TEXT DEFAULT 'pending';
"""

_MIGRATE_SCOUT_RAW_VIDEOS_MINER_PROCESSED_AT = """
ALTER TABLE scout_raw_videos ADD COLUMN IF NOT EXISTS miner_processed_at TIMESTAMP;
"""

_MIGRATE_SCOUT_RAW_VIDEOS_MINER_STARTED_AT = """
ALTER TABLE scout_raw_videos ADD COLUMN IF NOT EXISTS miner_started_at TIMESTAMP;
"""

_MIGRATE_SCOUT_RAW_VIDEOS_MINER_FAILED_AT = """
ALTER TABLE scout_raw_videos ADD COLUMN IF NOT EXISTS miner_failed_at TIMESTAMP;
"""

_MIGRATE_SCOUT_RAW_VIDEOS_MINER_LAST_ERROR = """
ALTER TABLE scout_raw_videos ADD COLUMN IF NOT EXISTS miner_last_error TEXT DEFAULT '';
"""

_MIGRATE_SCOUT_RAW_VIDEOS_MINER_ATTEMPT_COUNT = """
ALTER TABLE scout_raw_videos ADD COLUMN IF NOT EXISTS miner_attempt_count INTEGER DEFAULT 0;
"""

_MIGRATE_SCOUT_RAW_VIDEOS_RESEARCH_STATUS = """
ALTER TABLE scout_raw_videos ADD COLUMN IF NOT EXISTS research_status TEXT DEFAULT 'pending';
"""

_MIGRATE_SCOUT_RAW_VIDEOS_RESEARCH_STARTED_AT = """
ALTER TABLE scout_raw_videos ADD COLUMN IF NOT EXISTS research_started_at TIMESTAMP;
"""

_CREATE_MEME_RECORDS = """
CREATE TABLE IF NOT EXISTS meme_records (
    id              TEXT PRIMARY KEY,
    title           TEXT,
    alias           TEXT,           -- JSON array
    definition      TEXT,
    origin          TEXT,
    category        TEXT,           -- JSON array
    platform        TEXT DEFAULT 'Bilibili',
    heat_index      INTEGER,
    lifecycle_stage TEXT,
    first_detected  DATE,
    source_urls     TEXT,           -- JSON array
    confidence      DOUBLE,
    human_verified  BOOLEAN DEFAULT FALSE,
    updated_at      DATE
);
"""

_CREATE_PIPELINE_RUNS = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                TEXT PRIMARY KEY,
    job_name          TEXT      NOT NULL,
    trigger_mode      TEXT      NOT NULL DEFAULT 'manual',
    status            TEXT      NOT NULL,
    started_at        TIMESTAMP NOT NULL,
    finished_at       TIMESTAMP,
    duration_seconds  DOUBLE,
    result_count      INTEGER   DEFAULT 0,
    summary           TEXT      DEFAULT '',
    error_message     TEXT      DEFAULT '',
    payload_json      TEXT      DEFAULT '{}'
);
"""

_CREATE_VIDEO_CONTEXT_CACHE = """
CREATE TABLE IF NOT EXISTS video_context_cache (
    bvid                TEXT PRIMARY KEY,
    video_url           TEXT      NOT NULL,
    title               TEXT      DEFAULT '',
    status              TEXT      NOT NULL,
    duration_seconds    INTEGER,
    summary             TEXT      DEFAULT '',
    description_text    TEXT      DEFAULT '',
    content_text        TEXT      DEFAULT '',
    transcript_excerpt  TEXT      DEFAULT '',
    chapters_json       TEXT      DEFAULT '[]',
    raw_payload_json    TEXT      DEFAULT '{}',
    skip_reason         TEXT      DEFAULT '',
    updated_at          TIMESTAMP DEFAULT NOW()
);
"""

_CREATE_MINER_COMMENT_INSIGHTS = """
CREATE TABLE IF NOT EXISTS miner_comment_insights (
    insight_id            TEXT      PRIMARY KEY,
    bvid                  TEXT      NOT NULL,
    collected_date        DATE      NOT NULL,
    partition             TEXT      DEFAULT '',
    title                 TEXT      DEFAULT '',
    description           TEXT      DEFAULT '',
    video_url             TEXT      DEFAULT '',
    tags_json             TEXT      DEFAULT '[]',
    comment_text          TEXT      NOT NULL,
    confidence            DOUBLE    DEFAULT 0,
    is_meme_candidate     BOOLEAN   DEFAULT FALSE,
    is_insider_knowledge  BOOLEAN   DEFAULT FALSE,
    reason                TEXT      DEFAULT '',
    video_context_json    TEXT      DEFAULT '{}',
    status                TEXT      NOT NULL DEFAULT 'pending',
    created_at            TIMESTAMP DEFAULT NOW(),
    updated_at            TIMESTAMP DEFAULT NOW()
);
"""

_CREATE_COMMENT_INSIGHTS = """
CREATE TABLE IF NOT EXISTS comment_insights (
    bundle_id               TEXT      PRIMARY KEY,
    insight_id              TEXT      NOT NULL UNIQUE,
    bvid                    TEXT      NOT NULL,
    collected_date          DATE      NOT NULL,
    comment_text            TEXT      NOT NULL,
    worth_investigating     BOOLEAN   NOT NULL DEFAULT FALSE,
    signal_score            DOUBLE    NOT NULL DEFAULT 0,
    reason                  TEXT      DEFAULT '',
    status                  TEXT      NOT NULL DEFAULT 'pending',
    video_refs_json         TEXT      DEFAULT '[]',
    miner_summary_json      TEXT      DEFAULT '{}',
    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW()
);
"""

_CREATE_COMMENT_SPANS = """
CREATE TABLE IF NOT EXISTS comment_spans (
    span_id                 TEXT      PRIMARY KEY,
    insight_id              TEXT      NOT NULL,
    raw_text                TEXT      NOT NULL,
    normalized_text         TEXT      NOT NULL,
    span_type               TEXT      NOT NULL,
    char_start              INTEGER,
    char_end                INTEGER,
    confidence              DOUBLE    NOT NULL DEFAULT 0,
    is_primary              BOOLEAN   NOT NULL DEFAULT FALSE,
    query_priority          TEXT      NOT NULL DEFAULT 'low',
    reason                  TEXT      DEFAULT '',
    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW()
);
"""

_CREATE_HYPOTHESES = """
CREATE TABLE IF NOT EXISTS hypotheses (
    hypothesis_id           TEXT      PRIMARY KEY,
    bundle_id               TEXT      NOT NULL,
    insight_id              TEXT      NOT NULL,
    candidate_title         TEXT      NOT NULL,
    hypothesis_type         TEXT      NOT NULL,
    miner_opinion           TEXT      DEFAULT '',
    support_score           DOUBLE    NOT NULL DEFAULT 0,
    counter_score           DOUBLE    NOT NULL DEFAULT 0,
    uncertainty_score       DOUBLE    NOT NULL DEFAULT 0,
    suggested_action        TEXT      NOT NULL DEFAULT 'search_optional',
    status                  TEXT      NOT NULL DEFAULT 'pending',
    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW()
);
"""

_CREATE_HYPOTHESIS_SPANS = """
CREATE TABLE IF NOT EXISTS hypothesis_spans (
    hypothesis_id           TEXT      NOT NULL,
    span_id                 TEXT      NOT NULL,
    role                    TEXT      NOT NULL DEFAULT 'related',
    PRIMARY KEY (hypothesis_id, span_id)
);
"""

_CREATE_EVIDENCES = """
CREATE TABLE IF NOT EXISTS evidences (
    evidence_id             TEXT      PRIMARY KEY,
    hypothesis_id           TEXT      NOT NULL,
    span_id                 TEXT,
    query                   TEXT      NOT NULL,
    query_mode              TEXT      NOT NULL,
    source_kind             TEXT      NOT NULL,
    source_title            TEXT      DEFAULT '',
    source_url              TEXT      DEFAULT '',
    snippet                 TEXT      DEFAULT '',
    evidence_direction      TEXT      NOT NULL,
    evidence_strength       DOUBLE    NOT NULL DEFAULT 0,
    created_at              TIMESTAMP DEFAULT NOW()
);
"""

_CREATE_RESEARCH_DECISIONS = """
CREATE TABLE IF NOT EXISTS research_decisions (
    decision_id             TEXT      PRIMARY KEY,
    bundle_id               TEXT      NOT NULL,
    hypothesis_id           TEXT      NOT NULL,
    decision                TEXT      NOT NULL,
    final_title             TEXT      DEFAULT '',
    target_record_id        TEXT      DEFAULT '',
    confidence              DOUBLE    NOT NULL DEFAULT 0,
    reason                  TEXT      DEFAULT '',
    evidence_summary_json   TEXT      DEFAULT '{}',
    assessment_json         TEXT      DEFAULT '{}',
    record_json             TEXT      DEFAULT '{}',
    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW()
);
"""

_CREATE_AGENT_CONVERSATIONS = """
CREATE TABLE IF NOT EXISTS agent_conversations (
    id             TEXT PRIMARY KEY,
    run_id         TEXT      NOT NULL,
    agent_name     TEXT      NOT NULL,
    word           TEXT      NOT NULL,
    status         TEXT      NOT NULL,
    summary        TEXT      DEFAULT '',
    started_at     TIMESTAMP NOT NULL,
    finished_at    TIMESTAMP,
    message_count  INTEGER   DEFAULT 0,
    messages_json  TEXT      DEFAULT '[]',
    output_json    TEXT      DEFAULT '{}',
    error_message  TEXT      DEFAULT ''
);
"""


def get_conn() -> duckdb.DuckDBPyConnection:
    """返回持久化的 DuckDB 连接。"""
    path = Path(settings.duckdb_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(_CREATE_SCOUT_RAW_VIDEOS)
    conn.execute(_CREATE_SCOUT_RAW_COMMENTS)
    conn.execute(_CREATE_MEDIA_ASSETS)
    conn.execute(_CREATE_COMMENT_MEDIA_LINKS)
    conn.execute(_CREATE_MEME_RECORDS)
    conn.execute(_CREATE_PIPELINE_RUNS)
    conn.execute(_CREATE_VIDEO_CONTEXT_CACHE)
    conn.execute(_CREATE_MINER_COMMENT_INSIGHTS)
    conn.execute(_CREATE_COMMENT_INSIGHTS)
    conn.execute(_CREATE_COMMENT_SPANS)
    conn.execute(_CREATE_HYPOTHESES)
    conn.execute(_CREATE_HYPOTHESIS_SPANS)
    conn.execute(_CREATE_EVIDENCES)
    conn.execute(_CREATE_RESEARCH_DECISIONS)
    conn.execute(_CREATE_AGENT_CONVERSATIONS)
    # 兼容旧库：补充新增列
    try:
        conn.execute(_MIGRATE_SCOUT_RAW_VIDEOS_TAGS)
    except Exception:
        pass
    try:
        conn.execute(_MIGRATE_SCOUT_RAW_VIDEOS_MINER_STATUS)
    except Exception:
        pass
    try:
        conn.execute(_MIGRATE_SCOUT_RAW_VIDEOS_MINER_PROCESSED_AT)
    except Exception:
        pass
    try:
        conn.execute(_MIGRATE_SCOUT_RAW_VIDEOS_MINER_STARTED_AT)
    except Exception:
        pass
    try:
        conn.execute(_MIGRATE_SCOUT_RAW_VIDEOS_MINER_FAILED_AT)
    except Exception:
        pass
    try:
        conn.execute(_MIGRATE_SCOUT_RAW_VIDEOS_MINER_LAST_ERROR)
    except Exception:
        pass
    try:
        conn.execute(_MIGRATE_SCOUT_RAW_VIDEOS_MINER_ATTEMPT_COUNT)
    except Exception:
        pass
    try:
        _rename_column_if_present(
            conn,
            table_name="scout_raw_videos",
            old_name="candidate_status",
            new_name="research_status",
        )
    except Exception:
        pass
    try:
        _rename_column_if_present(
            conn,
            table_name="scout_raw_videos",
            old_name="candidate_extracted_at",
            new_name="research_started_at",
        )
    except Exception:
        pass
    try:
        conn.execute(_MIGRATE_SCOUT_RAW_VIDEOS_RESEARCH_STATUS)
    except Exception:
        pass
    try:
        conn.execute(_MIGRATE_SCOUT_RAW_VIDEOS_RESEARCH_STARTED_AT)
    except Exception:
        pass


def _column_exists(
    conn: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    column_name: str,
) -> bool:
    rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return any(str(row[1]).strip() == column_name for row in rows)


def _rename_column_if_present(
    conn: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    old_name: str,
    new_name: str,
) -> None:
    if _column_exists(conn, table_name=table_name, column_name=new_name):
        return
    if not _column_exists(conn, table_name=table_name, column_name=old_name):
        return
    conn.execute(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")


def upsert_scout_raw_videos(
    conn: duckdb.DuckDBPyConnection,
    videos: list[dict],
    target_date: date,
) -> dict:
    """批量写入 Scout 采集到的视频元信息和评论快照。"""
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
) -> None:
    comment_snapshots = video.get("comment_snapshots", [])
    if not isinstance(comment_snapshots, list) or not comment_snapshots:
        return

    conn.execute(
        "DELETE FROM comment_media_links WHERE bvid = ? AND collected_date = ?",
        [video["bvid"], target_date],
    )
    conn.execute(
        "DELETE FROM scout_raw_comments WHERE bvid = ? AND collected_date = ?",
        [video["bvid"], target_date],
    )

    comment_rows = []
    media_links: list[tuple[int, str, date, str, int]] = []
    for index, snapshot in enumerate(comment_snapshots):
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
            )
            if not asset:
                continue
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

    asset = _download_media_asset(normalized_url)
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
            json.dumps(
                {
                    "picture_meta": picture,
                },
                ensure_ascii=False,
            ),
        ],
    )
    return asset


def _download_media_asset(source_url: str) -> dict:
    fallback_asset_id = f"url_{hashlib.sha256(source_url.encode('utf-8')).hexdigest()}"
    try:
        request = Request(
            source_url,
            headers={"User-Agent": "Mozilla/5.0 MemeDetector/0.1"},
        )
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


def upsert_miner_comment_insights(
    conn: duckdb.DuckDBPyConnection,
    insights: list[dict],
) -> None:
    if not insights:
        return

    rows = []
    now = datetime.now()
    for item in insights:
        insight_id = str(item.get("insight_id", "")).strip()
        bvid = str(item.get("bvid", "")).strip()
        collected_date = item.get("collected_date")
        comment_text = str(item.get("comment_text", "")).strip()
        if not insight_id or not bvid or not collected_date or not comment_text:
            continue
        tags = item.get("tags", [])
        if not isinstance(tags, list):
            tags = []
        status = str(item.get("status", "")).strip()
        if not status:
            is_high_value = (
                float(item.get("confidence", 0.0) or 0.0) >= settings.miner_comment_confidence_threshold
                and (bool(item.get("is_meme_candidate")) or bool(item.get("is_insider_knowledge")))
            )
            status = "pending_bundle" if is_high_value else "discarded"
        rows.append(
            (
                insight_id,
                bvid,
                collected_date,
                str(item.get("partition", "")).strip(),
                str(item.get("title", "")).strip(),
                str(item.get("description", "")).strip(),
                str(item.get("video_url", "")).strip(),
                json.dumps([str(tag).strip() for tag in tags if str(tag).strip()], ensure_ascii=False),
                comment_text,
                float(item.get("confidence", 0.0) or 0.0),
                bool(item.get("is_meme_candidate")),
                bool(item.get("is_insider_knowledge")),
                str(item.get("reason", "")).strip(),
                json.dumps(item.get("video_context") or {}, ensure_ascii=False),
                status,
                now,
                now,
            )
        )

    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO miner_comment_insights (
            insight_id,
            bvid,
            collected_date,
            partition,
            title,
            description,
            video_url,
            tags_json,
            comment_text,
            confidence,
            is_meme_candidate,
            is_insider_knowledge,
            reason,
            video_context_json,
            status,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (insight_id) DO UPDATE
        SET partition = excluded.partition,
            title = excluded.title,
            description = excluded.description,
            video_url = excluded.video_url,
            tags_json = excluded.tags_json,
            comment_text = excluded.comment_text,
            confidence = excluded.confidence,
            is_meme_candidate = excluded.is_meme_candidate,
            is_insider_knowledge = excluded.is_insider_knowledge,
            reason = excluded.reason,
            video_context_json = excluded.video_context_json,
            updated_at = excluded.updated_at,
            status = CASE
                WHEN miner_comment_insights.status = 'bundled' THEN miner_comment_insights.status
                WHEN miner_comment_insights.status = 'bundling' THEN miner_comment_insights.status
                ELSE excluded.status
            END
        """,
        rows,
    )


def get_pending_miner_comment_insights(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int | None = 200,
) -> list[dict]:
    query = """
        SELECT
            insight_id,
            bvid,
            collected_date,
            partition,
            title,
            description,
            video_url,
            tags_json,
            comment_text,
            confidence,
            is_meme_candidate,
            is_insider_knowledge,
            reason,
            video_context_json,
            status,
            created_at,
            updated_at
        FROM miner_comment_insights
        WHERE status IN ('pending_bundle', 'bundle_failed')
        ORDER BY confidence DESC, collected_date ASC, bvid ASC
    """
    params: list[int] = []
    if limit is not None:
        query += "\n        LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    return [
        {
            "insight_id": row[0],
            "bvid": row[1],
            "collected_date": row[2],
            "partition": row[3],
            "title": row[4],
            "description": row[5],
            "url": row[6],
            "tags": _load_json_text(row[7], default=[]),
            "comment_text": row[8],
            "confidence": row[9],
            "is_meme_candidate": row[10],
            "is_insider_knowledge": row[11],
            "reason": row[12],
            "video_context": _load_json_text(row[13], default={}),
            "status": row[14],
            "created_at": row[15],
            "updated_at": row[16],
        }
        for row in rows
    ]


def get_miner_comment_insights_page(
    conn: duckdb.DuckDBPyConnection,
    *,
    status: str | None = None,
    keyword: str | None = None,
    bvid: str | None = None,
    only_meme_candidates: bool = False,
    only_insider_knowledge: bool = False,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    """分页获取 Miner 评论线索。"""
    where_parts: list[str] = []
    params: list[str | int | bool] = []

    if status:
        where_parts.append("status = ?")
        params.append(status)
    if keyword:
        where_parts.append("(title LIKE ? OR description LIKE ? OR comment_text LIKE ? OR reason LIKE ?)")
        keyword_like = f"%{keyword}%"
        params.extend([keyword_like, keyword_like, keyword_like, keyword_like])
    if bvid:
        where_parts.append("bvid LIKE ?")
        params.append(f"%{bvid}%")
    if only_meme_candidates:
        where_parts.append("is_meme_candidate = TRUE")
    if only_insider_knowledge:
        where_parts.append("is_insider_knowledge = TRUE")

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    total = conn.execute(
        f"SELECT COUNT(*) FROM miner_comment_insights {where_clause}",
        params,
    ).fetchone()[0]

    rows = conn.execute(
        f"""
        SELECT
            insight_id,
            bvid,
            collected_date,
            partition,
            title,
            description,
            video_url,
            tags_json,
            comment_text,
            confidence,
            is_meme_candidate,
            is_insider_knowledge,
            reason,
            video_context_json,
            status,
            created_at,
            updated_at,
            (
                SELECT bundle_id
                FROM comment_insights bundles
                WHERE bundles.insight_id = miner_comment_insights.insight_id
            ) AS bundle_id,
            (
                SELECT status
                FROM comment_insights bundles
                WHERE bundles.insight_id = miner_comment_insights.insight_id
            ) AS bundle_status
        FROM miner_comment_insights
        {where_clause}
        ORDER BY collected_date DESC, confidence DESC, updated_at DESC, bvid ASC
        LIMIT ?
        OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()

    return {
        "items": [_serialize_miner_comment_insight(row) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def get_miner_comment_insight(
    conn: duckdb.DuckDBPyConnection,
    insight_id: str,
) -> dict | None:
    """获取单条 Miner 评论线索详情。"""
    row = conn.execute(
        """
        SELECT
            insight_id,
            bvid,
            collected_date,
            partition,
            title,
            description,
            video_url,
            tags_json,
            comment_text,
            confidence,
            is_meme_candidate,
            is_insider_knowledge,
            reason,
            video_context_json,
            status,
            created_at,
            updated_at,
            (
                SELECT bundle_id
                FROM comment_insights bundles
                WHERE bundles.insight_id = miner_comment_insights.insight_id
            ) AS bundle_id,
            (
                SELECT status
                FROM comment_insights bundles
                WHERE bundles.insight_id = miner_comment_insights.insight_id
            ) AS bundle_status
        FROM miner_comment_insights
        WHERE insight_id = ?
        """,
        [insight_id],
    ).fetchone()
    if not row:
        return None
    return _serialize_miner_comment_insight(row)


def mark_miner_comment_insights_processed(
    conn: duckdb.DuckDBPyConnection,
    insights: list[dict],
) -> None:
    rows = []
    now = datetime.now()
    for item in insights:
        insight_id = str(item.get("insight_id", "")).strip()
        if not insight_id:
            continue
        rows.append((now, insight_id))

    if not rows:
        return

    conn.executemany(
        """
        UPDATE miner_comment_insights
        SET status = 'bundled',
            updated_at = ?
        WHERE insight_id = ?
        """,
        rows,
    )


def mark_miner_comment_insights_bundling(
    conn: duckdb.DuckDBPyConnection,
    insights: list[dict],
) -> None:
    rows = []
    now = datetime.now()
    for item in insights:
        insight_id = str(item.get("insight_id", "")).strip()
        if not insight_id:
            continue
        rows.append((now, insight_id))

    if not rows:
        return

    conn.executemany(
        """
        UPDATE miner_comment_insights
        SET status = 'bundling',
            updated_at = ?
        WHERE insight_id = ?
        """,
        rows,
    )


def mark_miner_comment_insights_bundle_failed(
    conn: duckdb.DuckDBPyConnection,
    insights: list[dict],
) -> None:
    rows = []
    now = datetime.now()
    for item in insights:
        insight_id = str(item.get("insight_id", "")).strip()
        if not insight_id:
            continue
        rows.append((now, insight_id))

    if not rows:
        return

    conn.executemany(
        """
        UPDATE miner_comment_insights
        SET status = 'bundle_failed',
            updated_at = ?
        WHERE insight_id = ?
        """,
        rows,
    )


def upsert_comment_bundle(
    conn: duckdb.DuckDBPyConnection,
    bundle,
) -> None:
    """写入或刷新一条评论证据包。"""
    from meme_detector.pipeline_models import MinerBundle

    if not isinstance(bundle, MinerBundle):
        bundle = MinerBundle.model_validate(bundle)

    now = datetime.now()
    insight = bundle.insight
    conn.execute(
        """
        INSERT INTO comment_insights (
            bundle_id,
            insight_id,
            bvid,
            collected_date,
            comment_text,
            worth_investigating,
            signal_score,
            reason,
            status,
            video_refs_json,
            miner_summary_json,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (bundle_id) DO UPDATE
        SET insight_id = excluded.insight_id,
            bvid = excluded.bvid,
            collected_date = excluded.collected_date,
            comment_text = excluded.comment_text,
            worth_investigating = excluded.worth_investigating,
            signal_score = excluded.signal_score,
            reason = excluded.reason,
            status = excluded.status,
            video_refs_json = excluded.video_refs_json,
            miner_summary_json = excluded.miner_summary_json,
            updated_at = excluded.updated_at
        """,
        [
            bundle.bundle_id,
            insight.insight_id,
            insight.bvid,
            insight.collected_date,
            insight.comment_text,
            insight.worth_investigating,
            insight.signal_score,
            insight.reason,
            insight.status.value,
            json.dumps([item.model_dump(mode="json") for item in bundle.video_refs], ensure_ascii=False),
            json.dumps(bundle.miner_summary.model_dump(mode="json"), ensure_ascii=False),
            now,
            now,
        ],
    )

    existing_hypothesis_rows = conn.execute(
        """
        SELECT hypothesis_id
        FROM hypotheses
        WHERE bundle_id = ?
        """,
        [bundle.bundle_id],
    ).fetchall()
    existing_hypothesis_ids = [str(row[0]).strip() for row in existing_hypothesis_rows if str(row[0]).strip()]
    if existing_hypothesis_ids:
        placeholders = ", ".join("?" for _ in existing_hypothesis_ids)
        conn.execute(
            f"DELETE FROM evidences WHERE hypothesis_id IN ({placeholders})",
            existing_hypothesis_ids,
        )
        conn.execute(
            f"DELETE FROM hypothesis_spans WHERE hypothesis_id IN ({placeholders})",
            existing_hypothesis_ids,
        )
    conn.execute(
        "DELETE FROM hypotheses WHERE bundle_id = ?",
        [bundle.bundle_id],
    )
    conn.execute(
        "DELETE FROM comment_spans WHERE insight_id = ?",
        [insight.insight_id],
    )

    if bundle.spans:
        conn.executemany(
            """
            INSERT INTO comment_spans (
                span_id,
                insight_id,
                raw_text,
                normalized_text,
                span_type,
                char_start,
                char_end,
                confidence,
                is_primary,
                query_priority,
                reason,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    item.span_id,
                    item.insight_id,
                    item.raw_text,
                    item.normalized_text,
                    item.span_type.value,
                    item.char_start,
                    item.char_end,
                    item.confidence,
                    item.is_primary,
                    item.query_priority.value,
                    item.reason,
                    now,
                    now,
                )
                for item in bundle.spans
            ],
        )

    if bundle.hypotheses:
        conn.executemany(
            """
            INSERT INTO hypotheses (
                hypothesis_id,
                bundle_id,
                insight_id,
                candidate_title,
                hypothesis_type,
                miner_opinion,
                support_score,
                counter_score,
                uncertainty_score,
                suggested_action,
                status,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    item.hypothesis_id,
                    bundle.bundle_id,
                    item.insight_id,
                    item.candidate_title,
                    item.hypothesis_type.value,
                    item.miner_opinion,
                    item.support_score,
                    item.counter_score,
                    item.uncertainty_score,
                    item.suggested_action.value,
                    item.status.value,
                    now,
                    now,
                )
                for item in bundle.hypotheses
            ],
        )

    if bundle.hypothesis_spans:
        conn.executemany(
            """
            INSERT INTO hypothesis_spans (
                hypothesis_id,
                span_id,
                role
            )
            VALUES (?, ?, ?)
            """,
            [
                (
                    item.hypothesis_id,
                    item.span_id,
                    item.role.value,
                )
                for item in bundle.hypothesis_spans
            ],
        )

    if bundle.evidences:
        conn.executemany(
            """
            INSERT INTO evidences (
                evidence_id,
                hypothesis_id,
                span_id,
                query,
                query_mode,
                source_kind,
                source_title,
                source_url,
                snippet,
                evidence_direction,
                evidence_strength,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    item.evidence_id,
                    item.hypothesis_id,
                    item.span_id,
                    item.query,
                    item.query_mode.value,
                    item.source_kind.value,
                    item.source_title,
                    item.source_url,
                    item.snippet,
                    item.evidence_direction.value,
                    item.evidence_strength,
                    now,
                )
                for item in bundle.evidences
            ],
        )


def get_comment_bundle(
    conn: duckdb.DuckDBPyConnection,
    *,
    bundle_id: str | None = None,
    insight_id: str | None = None,
):
    """读取一条评论证据包。"""
    from meme_detector.pipeline_models import (
        Evidence,
        Hypothesis,
        HypothesisSpanLink,
        Insight,
        MinerBundle,
        MinerSummary,
        Span,
        VideoRef,
    )

    if not bundle_id and not insight_id:
        raise ValueError("bundle_id or insight_id is required")

    if bundle_id:
        row = conn.execute(
            """
            SELECT
                bundle_id,
                insight_id,
                bvid,
                collected_date,
                comment_text,
                worth_investigating,
                signal_score,
                reason,
                status,
                video_refs_json,
                miner_summary_json
            FROM comment_insights
            WHERE bundle_id = ?
            """,
            [bundle_id],
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT
                bundle_id,
                insight_id,
                bvid,
                collected_date,
                comment_text,
                worth_investigating,
                signal_score,
                reason,
                status,
                video_refs_json,
                miner_summary_json
            FROM comment_insights
            WHERE insight_id = ?
            """,
            [insight_id],
        ).fetchone()

    if not row:
        return None

    resolved_bundle_id = str(row[0]).strip()
    resolved_insight_id = str(row[1]).strip()
    insight = Insight(
        insight_id=resolved_insight_id,
        bvid=str(row[2]).strip(),
        collected_date=row[3],
        comment_text=str(row[4]).strip(),
        worth_investigating=bool(row[5]),
        signal_score=float(row[6] or 0.0),
        reason=str(row[7] or "").strip(),
        status=str(row[8] or "pending"),
    )
    video_refs = [
        VideoRef.model_validate(item)
        for item in _load_json_text(row[9], default=[])
        if isinstance(item, dict)
    ]
    miner_summary = MinerSummary.model_validate(_load_json_text(row[10], default={}))

    span_rows = conn.execute(
        """
        SELECT
            span_id,
            insight_id,
            raw_text,
            normalized_text,
            span_type,
            char_start,
            char_end,
            confidence,
            is_primary,
            query_priority,
            reason
        FROM comment_spans
        WHERE insight_id = ?
        ORDER BY is_primary DESC, confidence DESC, span_id ASC
        """,
        [resolved_insight_id],
    ).fetchall()
    spans = [
        Span(
            span_id=str(item[0]).strip(),
            insight_id=str(item[1]).strip(),
            raw_text=str(item[2]).strip(),
            normalized_text=str(item[3]).strip(),
            span_type=str(item[4]).strip(),
            char_start=item[5],
            char_end=item[6],
            confidence=float(item[7] or 0.0),
            is_primary=bool(item[8]),
            query_priority=str(item[9] or "low"),
            reason=str(item[10] or "").strip(),
        )
        for item in span_rows
    ]

    hypothesis_rows = conn.execute(
        """
        SELECT
            hypothesis_id,
            insight_id,
            candidate_title,
            hypothesis_type,
            miner_opinion,
            support_score,
            counter_score,
            uncertainty_score,
            suggested_action,
            status
        FROM hypotheses
        WHERE bundle_id = ?
        ORDER BY support_score DESC, counter_score ASC, hypothesis_id ASC
        """,
        [resolved_bundle_id],
    ).fetchall()
    hypotheses = [
        Hypothesis(
            hypothesis_id=str(item[0]).strip(),
            insight_id=str(item[1]).strip(),
            candidate_title=str(item[2]).strip(),
            hypothesis_type=str(item[3]).strip(),
            miner_opinion=str(item[4] or "").strip(),
            support_score=float(item[5] or 0.0),
            counter_score=float(item[6] or 0.0),
            uncertainty_score=float(item[7] or 0.0),
            suggested_action=str(item[8] or "search_optional"),
            status=str(item[9] or "pending"),
        )
        for item in hypothesis_rows
    ]

    if hypotheses:
        hypothesis_ids = [item.hypothesis_id for item in hypotheses]
        placeholders = ", ".join("?" for _ in hypothesis_ids)
        link_rows = conn.execute(
            f"""
            SELECT
                hypothesis_id,
                span_id,
                role
            FROM hypothesis_spans
            WHERE hypothesis_id IN ({placeholders})
            ORDER BY hypothesis_id ASC, span_id ASC
            """,
            hypothesis_ids,
        ).fetchall()
        evidence_rows = conn.execute(
            f"""
            SELECT
                evidence_id,
                hypothesis_id,
                span_id,
                query,
                query_mode,
                source_kind,
                source_title,
                source_url,
                snippet,
                evidence_direction,
                evidence_strength
            FROM evidences
            WHERE hypothesis_id IN ({placeholders})
            ORDER BY hypothesis_id ASC, evidence_strength DESC, evidence_id ASC
            """,
            hypothesis_ids,
        ).fetchall()
    else:
        link_rows = []
        evidence_rows = []

    hypothesis_spans = [
        HypothesisSpanLink(
            hypothesis_id=str(item[0]).strip(),
            span_id=str(item[1]).strip(),
            role=str(item[2]).strip(),
        )
        for item in link_rows
    ]
    if hypotheses and spans:
        primary_hypothesis_ids = {
            item.hypothesis_id
            for item in hypothesis_spans
            if item.role.value == "primary"
        }
        linked_span_ids_by_hypothesis: dict[str, list[str]] = {}
        for item in hypothesis_spans:
            linked_span_ids_by_hypothesis.setdefault(item.hypothesis_id, []).append(item.span_id)

        primary_span_ids = [item.span_id for item in spans if item.is_primary]
        fallback_default_span_id = primary_span_ids[0] if primary_span_ids else spans[0].span_id
        missing_primary_hypothesis_ids: list[str] = []

        for hypothesis in hypotheses:
            if hypothesis.hypothesis_id in primary_hypothesis_ids:
                continue

            fallback_span_id = (
                linked_span_ids_by_hypothesis.get(hypothesis.hypothesis_id, [fallback_default_span_id])[0]
            )
            hypothesis_spans.append(
                HypothesisSpanLink(
                    hypothesis_id=hypothesis.hypothesis_id,
                    span_id=fallback_span_id,
                    role="primary",
                )
            )
            missing_primary_hypothesis_ids.append(hypothesis.hypothesis_id)

        if missing_primary_hypothesis_ids:
            logger.warning(
                "comment bundle missing primary span links; reconstructed on read",
                extra={
                    "event": "comment_bundle_primary_span_reconstructed",
                    "bundle_id": resolved_bundle_id,
                    "hypothesis_ids": missing_primary_hypothesis_ids,
                },
            )
    evidences = [
        Evidence(
            evidence_id=str(item[0]).strip(),
            hypothesis_id=str(item[1]).strip(),
            span_id=str(item[2]).strip() or None,
            query=str(item[3]).strip(),
            query_mode=str(item[4]).strip(),
            source_kind=str(item[5]).strip(),
            source_title=str(item[6] or "").strip(),
            source_url=str(item[7] or "").strip(),
            snippet=str(item[8] or "").strip(),
            evidence_direction=str(item[9]).strip(),
            evidence_strength=float(item[10] or 0.0),
        )
        for item in evidence_rows
    ]

    return MinerBundle(
        bundle_id=resolved_bundle_id,
        insight=insight,
        video_refs=video_refs,
        spans=spans,
        hypotheses=hypotheses,
        hypothesis_spans=hypothesis_spans,
        evidences=evidences,
        miner_summary=miner_summary,
    )


def upsert_research_decision(
    conn: duckdb.DuckDBPyConnection,
    decision,
    *,
    persist_record: bool = False,
) -> None:
    """写入 Research 裁决结果，并同步 hypothesis / insight 状态。"""
    from meme_detector.pipeline_models import ResearchDecision, ResearchDecisionType

    if not isinstance(decision, ResearchDecision):
        decision = ResearchDecision.model_validate(decision)

    now = datetime.now()
    conn.execute(
        """
        INSERT INTO research_decisions (
            decision_id,
            bundle_id,
            hypothesis_id,
            decision,
            final_title,
            target_record_id,
            confidence,
            reason,
            evidence_summary_json,
            assessment_json,
            record_json,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (decision_id) DO UPDATE
        SET bundle_id = excluded.bundle_id,
            hypothesis_id = excluded.hypothesis_id,
            decision = excluded.decision,
            final_title = excluded.final_title,
            target_record_id = excluded.target_record_id,
            confidence = excluded.confidence,
            reason = excluded.reason,
            evidence_summary_json = excluded.evidence_summary_json,
            assessment_json = excluded.assessment_json,
            record_json = excluded.record_json,
            updated_at = excluded.updated_at
        """,
        [
            decision.decision_id,
            decision.bundle_id,
            decision.target_hypothesis_id,
            decision.decision.value,
            decision.final_title,
            decision.target_record_id,
            decision.confidence,
            decision.reason,
            json.dumps(decision.evidence_summary.model_dump(mode="json"), ensure_ascii=False),
            json.dumps(decision.assessment.model_dump(mode="json"), ensure_ascii=False),
            json.dumps(decision.record.model_dump(mode="json"), ensure_ascii=False) if decision.record else "{}",
            now,
            now,
        ],
    )

    status_by_decision = {
        ResearchDecisionType.accept: "accepted",
        ResearchDecisionType.rewrite_title: "accepted",
        ResearchDecisionType.reject: "rejected",
        ResearchDecisionType.manual_review: "manual_review",
        ResearchDecisionType.merge_into_existing: "merged",
    }
    conn.execute(
        """
        UPDATE hypotheses
        SET status = ?,
            updated_at = ?
        WHERE hypothesis_id = ?
        """,
        [status_by_decision[decision.decision], now, decision.target_hypothesis_id],
    )
    conn.execute(
        """
        UPDATE comment_insights
        SET status = 'researched',
            updated_at = ?
        WHERE bundle_id = ?
        """,
        [now, decision.bundle_id],
    )
    if persist_record and decision.record is not None:
        upsert_meme_record(conn, decision.record)


def get_research_decision(
    conn: duckdb.DuckDBPyConnection,
    decision_id: str,
):
    """读取单条 Research 裁决结果。"""
    from meme_detector.pipeline_models import (
        EvidenceSummary,
        ResearchAssessment,
        ResearchDecision,
    )

    row = conn.execute(
        """
        SELECT
            decision_id,
            bundle_id,
            hypothesis_id,
            decision,
            final_title,
            target_record_id,
            confidence,
            reason,
            evidence_summary_json,
            assessment_json,
            record_json
        FROM research_decisions
        WHERE decision_id = ?
        """,
        [decision_id],
    ).fetchone()
    if not row:
        return None

    record_payload = _load_json_text(row[10], default={})
    return ResearchDecision(
        decision_id=str(row[0]).strip(),
        bundle_id=str(row[1]).strip(),
        target_hypothesis_id=str(row[2]).strip(),
        decision=str(row[3]).strip(),
        final_title=str(row[4] or "").strip(),
        target_record_id=str(row[5] or "").strip(),
        confidence=float(row[6] or 0.0),
        reason=str(row[7] or "").strip(),
        evidence_summary=EvidenceSummary.model_validate(_load_json_text(row[8], default={})),
        assessment=ResearchAssessment.model_validate(_load_json_text(row[9], default={})),
        record=record_payload if record_payload else None,
    )


def list_queued_comment_bundles(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int | None = 50,
) -> list[dict]:
    """列出待 Research 裁决的评论证据包。"""
    query = """
        SELECT
            ci.bundle_id,
            ci.insight_id,
            ci.bvid,
            ci.collected_date,
            ci.comment_text,
            ci.signal_score,
            ci.status,
            COUNT(h.hypothesis_id) AS hypothesis_count
        FROM comment_insights ci
        JOIN hypotheses h ON h.bundle_id = ci.bundle_id
        WHERE ci.status = 'bundled'
          AND h.status IN ('queued', 'evidenced')
        GROUP BY
            ci.bundle_id,
            ci.insight_id,
            ci.bvid,
            ci.collected_date,
            ci.comment_text,
            ci.signal_score,
            ci.status
        ORDER BY ci.collected_date ASC, ci.signal_score DESC, ci.bundle_id ASC
    """
    params: list[int] = []
    if limit is not None:
        query += "\n        LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    return [
        {
            "bundle_id": row[0],
            "insight_id": row[1],
            "bvid": row[2],
            "collected_date": row[3],
            "comment_text": row[4],
            "signal_score": float(row[5] or 0.0),
            "status": row[6],
            "hypothesis_count": int(row[7] or 0),
        }
        for row in rows
    ]


def get_comment_bundles_page(
    conn: duckdb.DuckDBPyConnection,
    *,
    status: str | None = None,
    queued_only: bool = False,
    keyword: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    """分页获取评论证据包摘要。"""
    where_parts: list[str] = []
    params: list[str | int] = []
    if status:
        where_parts.append("ci.status = ?")
        params.append(status)
    if queued_only:
        where_parts.append(
            "EXISTS (SELECT 1 FROM hypotheses hq WHERE hq.bundle_id = ci.bundle_id AND hq.status = 'queued')"
        )
    if keyword:
        where_parts.append("(ci.comment_text LIKE ? OR ci.reason LIKE ? OR ci.bvid LIKE ?)")
        keyword_like = f"%{keyword}%"
        params.extend([keyword_like, keyword_like, keyword_like])

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    total = conn.execute(
        f"SELECT COUNT(*) FROM comment_insights ci {where_clause}",
        params,
    ).fetchone()[0]

    rows = conn.execute(
        f"""
        SELECT
            ci.bundle_id,
            ci.insight_id,
            ci.bvid,
            ci.collected_date,
            ci.comment_text,
            ci.worth_investigating,
            ci.signal_score,
            ci.reason,
            ci.status,
            ci.video_refs_json,
            ci.miner_summary_json,
            COUNT(h.hypothesis_id) AS hypothesis_count,
            COUNT(h.hypothesis_id) FILTER (WHERE h.status = 'queued') AS queued_hypothesis_count,
            COUNT(h.hypothesis_id) FILTER (WHERE h.status = 'accepted') AS accepted_hypothesis_count,
            COUNT(e.evidence_id) AS evidence_count,
            MAX(rd.decision) AS latest_decision
        FROM comment_insights ci
        LEFT JOIN hypotheses h ON h.bundle_id = ci.bundle_id
        LEFT JOIN evidences e ON e.hypothesis_id = h.hypothesis_id
        LEFT JOIN research_decisions rd ON rd.bundle_id = ci.bundle_id
        {where_clause}
        GROUP BY
            ci.bundle_id,
            ci.insight_id,
            ci.bvid,
            ci.collected_date,
            ci.comment_text,
            ci.worth_investigating,
            ci.signal_score,
            ci.reason,
            ci.status,
            ci.video_refs_json,
            ci.miner_summary_json
        ORDER BY ci.collected_date DESC, ci.signal_score DESC, ci.bundle_id ASC
        LIMIT ?
        OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()

    items = []
    for row in rows:
        miner_summary = _load_json_text(row[10], default={})
        if not isinstance(miner_summary, dict):
            miner_summary = {}
        video_refs = _load_json_text(row[9], default=[])
        if not isinstance(video_refs, list):
            video_refs = []
        items.append(
            {
                "bundle_id": row[0],
                "insight_id": row[1],
                "bvid": row[2],
                "collected_date": row[3],
                "comment_text": row[4],
                "worth_investigating": bool(row[5]),
                "signal_score": float(row[6] or 0.0),
                "reason": row[7],
                "status": row[8],
                "video_refs": video_refs,
                "recommended_hypothesis_id": miner_summary.get("recommended_hypothesis_id"),
                "miner_summary_reason": miner_summary.get("reason", ""),
                "hypothesis_count": int(row[11] or 0),
                "queued_hypothesis_count": int(row[12] or 0),
                "accepted_hypothesis_count": int(row[13] or 0),
                "evidence_count": int(row[14] or 0),
                "latest_decision": row[15] or "",
            }
        )

    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def get_comment_bundle_detail(
    conn: duckdb.DuckDBPyConnection,
    bundle_id: str,
) -> dict | None:
    """获取评论证据包详情及关联裁决。"""
    bundle = get_comment_bundle(conn, bundle_id=bundle_id)
    if bundle is None:
        return None

    decision_rows = conn.execute(
        """
        SELECT
            decision_id,
            decision,
            final_title,
            target_record_id,
            confidence,
            reason,
            created_at
        FROM research_decisions
        WHERE bundle_id = ?
        ORDER BY created_at DESC, decision_id DESC
        """,
        [bundle_id],
    ).fetchall()

    decisions = [
        {
            "decision_id": row[0],
            "decision": row[1],
            "final_title": row[2],
            "target_record_id": row[3],
            "confidence": float(row[4] or 0.0),
            "reason": row[5],
            "created_at": row[6],
        }
        for row in decision_rows
    ]
    return {
        "bundle": bundle.model_dump(mode="json"),
        "decisions": decisions,
    }


def get_pending_scout_raw_videos(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int | None = None,
) -> list[dict]:
    """获取待 Miner 处理或失败待重试的 Scout 原始视频快照。"""
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
    """将视频标记为 Miner 处理中。"""
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
    """将原始视频快照标记为已完成 Miner 评论线索提取。"""
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
    """将视频标记为 Miner 失败，保留错误信息供后续排查或重试。"""
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
    """将上次异常中断遗留的 processing 视频回收为 failed。"""
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
    """分页获取 Scout 原始视频快照简表。"""
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

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    total = conn.execute(
        f"SELECT COUNT(*) FROM scout_raw_videos {where_clause}",
        params,
    ).fetchone()[0]

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
    """获取单条 Scout 原始视频快照详情。"""
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
    snapshot["comments_with_pictures"] = sum(
        1 for item in snapshot["comment_snapshots"] if item["pictures"]
    )
    return snapshot


def update_scout_raw_video_stage(
    conn: duckdb.DuckDBPyConnection,
    *,
    bvid: str,
    collected_date: date,
    stage: str,
) -> dict | None:
    """手动调整 Scout 快照所处阶段，并同步回退/推进关联的 Miner 状态。"""
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


def get_media_asset(
    conn: duckdb.DuckDBPyConnection,
    asset_id: str,
) -> dict | None:
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
    """将原始视频快照标记为已进入 Research。"""
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


def upsert_meme_record(
    conn: duckdb.DuckDBPyConnection,
    record: "MemeRecord",
) -> None:
    from meme_detector.researcher.models import MemeRecord

    if not isinstance(record, MemeRecord):
        record = MemeRecord.model_validate(record)

    conn.execute(
        """
        INSERT INTO meme_records (
            id,
            title,
            alias,
            definition,
            origin,
            category,
            platform,
            heat_index,
            lifecycle_stage,
            first_detected,
            source_urls,
            confidence,
            human_verified,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE
        SET title = excluded.title,
            alias = excluded.alias,
            definition = excluded.definition,
            origin = excluded.origin,
            category = excluded.category,
            platform = excluded.platform,
            heat_index = excluded.heat_index,
            lifecycle_stage = excluded.lifecycle_stage,
            first_detected = excluded.first_detected,
            source_urls = excluded.source_urls,
            confidence = excluded.confidence,
            human_verified = excluded.human_verified,
            updated_at = excluded.updated_at
        """,
        [
            record.id,
            record.title,
            json.dumps(record.alias, ensure_ascii=False),
            record.definition,
            record.origin,
            json.dumps(record.category, ensure_ascii=False),
            record.platform,
            record.heat_index,
            record.lifecycle_stage,
            record.first_detected_at,
            json.dumps(record.source_urls, ensure_ascii=False),
            record.confidence_score,
            record.human_verified,
            record.updated_at,
        ],
    )


def get_video_context_cache(
    conn: duckdb.DuckDBPyConnection,
    bvid: str,
) -> dict | None:
    """按 BVID 获取视频上下文缓存。"""
    row = conn.execute(
        """
        SELECT
            bvid,
            video_url,
            title,
            status,
            duration_seconds,
            summary,
            description_text,
            content_text,
            transcript_excerpt,
            chapters_json,
            raw_payload_json,
            skip_reason,
            updated_at
        FROM video_context_cache
        WHERE bvid = ?
        """,
        [bvid],
    ).fetchone()
    if not row:
        return None

    return {
        "bvid": row[0],
        "video_url": row[1],
        "title": row[2],
        "status": row[3],
        "duration_seconds": row[4],
        "summary": row[5],
        "description_text": row[6],
        "content_text": row[7],
        "transcript_excerpt": row[8],
        "chapters": _load_json_text(row[9], default=[]),
        "raw_payload": _load_json_text(row[10], default={}),
        "skip_reason": row[11],
        "updated_at": row[12].isoformat() if row[12] else None,
    }


def upsert_video_context_cache(
    conn: duckdb.DuckDBPyConnection,
    *,
    bvid: str,
    video_url: str,
    title: str,
    status: str,
    duration_seconds: int | None,
    summary: str = "",
    description_text: str = "",
    content_text: str = "",
    transcript_excerpt: str = "",
    chapters: list[dict] | None = None,
    raw_payload: dict | None = None,
    skip_reason: str = "",
) -> None:
    """写入视频上下文缓存。"""
    conn.execute(
        """
        INSERT INTO video_context_cache (
            bvid,
            video_url,
            title,
            status,
            duration_seconds,
            summary,
            description_text,
            content_text,
            transcript_excerpt,
            chapters_json,
            raw_payload_json,
            skip_reason,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (bvid) DO UPDATE
        SET
            video_url = excluded.video_url,
            title = excluded.title,
            status = excluded.status,
            duration_seconds = excluded.duration_seconds,
            summary = excluded.summary,
            description_text = excluded.description_text,
            content_text = excluded.content_text,
            transcript_excerpt = excluded.transcript_excerpt,
            chapters_json = excluded.chapters_json,
            raw_payload_json = excluded.raw_payload_json,
            skip_reason = excluded.skip_reason,
            updated_at = excluded.updated_at
        """,
        [
            bvid,
            video_url,
            title,
            status,
            duration_seconds,
            summary,
            description_text,
            content_text,
            transcript_excerpt,
            json.dumps(chapters or [], ensure_ascii=False),
            json.dumps(raw_payload or {}, ensure_ascii=False),
            skip_reason,
            datetime.now(),
        ],
    )


def create_agent_conversation(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    agent_name: str,
    word: str,
) -> str:
    """创建一条 agent 对话记录。"""
    conversation_id = uuid4().hex
    conn.execute(
        """
        INSERT INTO agent_conversations (id, run_id, agent_name, word, status, started_at)
        VALUES (?, ?, ?, ?, 'running', ?)
        """,
        [conversation_id, run_id, agent_name, word, datetime.now()],
    )
    return conversation_id


def finish_agent_conversation(
    conn: duckdb.DuckDBPyConnection,
    conversation_id: str,
    *,
    status: str,
    summary: str = "",
    messages_json: str = "[]",
    message_count: int = 0,
    output_json: str = "{}",
    error_message: str = "",
) -> None:
    """更新 agent 对话记录。"""
    conn.execute(
        """
        UPDATE agent_conversations
        SET status = ?,
            summary = ?,
            finished_at = ?,
            message_count = ?,
            messages_json = ?,
            output_json = ?,
            error_message = ?
        WHERE id = ?
        """,
        [
            status,
            summary,
            datetime.now(),
            message_count,
            messages_json,
            output_json,
            error_message,
            conversation_id,
        ],
    )


def list_agent_conversations(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str | None = None,
    agent_name: str | None = None,
    word: str | None = None,
    status: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    """分页获取 agent 对话记录。"""
    where_parts: list[str] = []
    params: list[str | int] = []

    if run_id:
        where_parts.append("run_id = ?")
        params.append(run_id)
    if agent_name:
        where_parts.append("agent_name = ?")
        params.append(agent_name)
    if word:
        where_parts.append("word LIKE ?")
        params.append(f"%{word}%")
    if status:
        where_parts.append("status = ?")
        params.append(status)

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    total = conn.execute(
        f"SELECT COUNT(*) FROM agent_conversations {where_clause}",
        params,
    ).fetchone()[0]

    rows = conn.execute(
        f"""
        SELECT
            id,
            run_id,
            agent_name,
            word,
            status,
            summary,
            started_at,
            finished_at,
            message_count,
            error_message
        FROM agent_conversations
        {where_clause}
        ORDER BY started_at DESC
        LIMIT ?
        OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()
    return {
        "items": [
            {
                "id": row[0],
                "run_id": row[1],
                "agent_name": row[2],
                "word": row[3],
                "status": row[4],
                "summary": row[5],
                "started_at": row[6],
                "finished_at": row[7],
                "message_count": row[8],
                "error_message": row[9],
            }
            for row in rows
        ],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def get_agent_conversation(
    conn: duckdb.DuckDBPyConnection,
    conversation_id: str,
) -> dict | None:
    """获取单条 agent 对话详情。"""
    row = conn.execute(
        """
        SELECT
            id,
            run_id,
            agent_name,
            word,
            status,
            summary,
            started_at,
            finished_at,
            message_count,
            messages_json,
            output_json,
            error_message
        FROM agent_conversations
        WHERE id = ?
        """,
        [conversation_id],
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "run_id": row[1],
        "agent_name": row[2],
        "word": row[3],
        "status": row[4],
        "summary": row[5],
        "started_at": row[6],
        "finished_at": row[7],
        "message_count": row[8],
        "messages": _load_json_text(row[9], default=[]),
        "output": _load_json_text(row[10], default={}),
        "error_message": row[11],
    }


def create_pipeline_run(
    conn: duckdb.DuckDBPyConnection,
    job_name: str,
    trigger_mode: str = "manual",
) -> str:
    """创建一条运行记录，初始状态为 running。"""
    run_id = uuid4().hex
    conn.execute(
        """
        INSERT INTO pipeline_runs (id, job_name, trigger_mode, status, started_at)
        VALUES (?, ?, ?, 'running', ?)
        """,
        [run_id, job_name, trigger_mode, datetime.now()],
    )
    return run_id


def finish_pipeline_run(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    status: str,
    *,
    result_count: int = 0,
    summary: str = "",
    error_message: str = "",
    payload: dict | None = None,
) -> None:
    """更新运行记录结束状态与结果摘要。"""
    started_at_row = conn.execute(
        "SELECT started_at FROM pipeline_runs WHERE id = ?",
        [run_id],
    ).fetchone()
    finished_at = datetime.now()
    duration_seconds = None
    if started_at_row and started_at_row[0]:
        duration_seconds = (finished_at - started_at_row[0]).total_seconds()

    conn.execute(
        """
        UPDATE pipeline_runs
        SET status = ?,
            finished_at = ?,
            duration_seconds = ?,
            result_count = ?,
            summary = ?,
            error_message = ?,
            payload_json = ?
        WHERE id = ?
        """,
        [
            status,
            finished_at,
            duration_seconds,
            result_count,
            summary,
            error_message,
            json.dumps(payload or {}, ensure_ascii=False),
            run_id,
        ],
    )


def list_pipeline_runs(
    conn: duckdb.DuckDBPyConnection,
    *,
    job_name: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """查询运行记录列表。"""
    where_parts: list[str] = []
    params: list[str | int] = []

    if job_name:
        where_parts.append("job_name = ?")
        params.append(job_name)
    if status:
        where_parts.append("status = ?")
        params.append(status)

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT
            id,
            job_name,
            trigger_mode,
            status,
            started_at,
            finished_at,
            duration_seconds,
            result_count,
            summary,
            error_message,
            payload_json
        FROM pipeline_runs
        {where_clause}
        ORDER BY started_at DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [_serialize_pipeline_run(row) for row in rows]


def get_pipeline_run(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
) -> dict | None:
    """查询单条运行记录。"""
    row = conn.execute(
        """
        SELECT
            id,
            job_name,
            trigger_mode,
            status,
            started_at,
            finished_at,
            duration_seconds,
            result_count,
            summary,
            error_message,
            payload_json
        FROM pipeline_runs
        WHERE id = ?
        """,
        [run_id],
    ).fetchone()
    if not row:
        return None
    return _serialize_pipeline_run(row)


def _serialize_pipeline_run(row: tuple) -> dict:
    payload_raw = row[10] or "{}"
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError:
        payload = {}

    return {
        "id": row[0],
        "job_name": row[1],
        "trigger_mode": row[2],
        "status": row[3],
        "started_at": row[4],
        "finished_at": row[5],
        "duration_seconds": row[6],
        "result_count": row[7],
        "summary": row[8],
        "error_message": row[9],
        "payload": payload,
    }


def _serialize_scout_raw_video(row: tuple) -> dict:
    tags = _load_json_text(row[6], default=[])
    if not isinstance(tags, list):
        tags = []

    comments = _load_json_text(row[7], default=[])
    if not isinstance(comments, list):
        comments = []

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
        "pipeline_stage": _build_scout_pipeline_stage(
            miner_status=row[9],
            research_status=row[15],
        ),
    }


def _serialize_scout_raw_video_summary(row: tuple) -> dict:
    tags = _load_json_text(row[5], default=[])
    if not isinstance(tags, list):
        tags = []

    comments = _load_json_text(row[17], default=[])
    if not isinstance(comments, list):
        comments = []

    first_comment = next(
        (str(comment).strip() for comment in comments if str(comment).strip()),
        "",
    )
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
        "pipeline_stage": _build_scout_pipeline_stage(
            miner_status=row[7],
            research_status=row[13],
        ),
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


def _serialize_miner_comment_insight(row: tuple) -> dict:
    tags = _load_json_text(row[7], default=[])
    if not isinstance(tags, list):
        tags = []

    video_context = _load_json_text(row[13], default={})
    if not isinstance(video_context, dict):
        video_context = {}

    return {
        "insight_id": row[0],
        "bvid": row[1],
        "collected_date": row[2],
        "partition": row[3],
        "title": row[4],
        "description": row[5],
        "video_url": row[6],
        "url": row[6],
        "tags": [str(tag).strip() for tag in tags if str(tag).strip()],
        "comment_text": row[8],
        "confidence": row[9],
        "is_meme_candidate": row[10],
        "is_insider_knowledge": row[11],
        "reason": row[12],
        "video_context": video_context,
        "status": row[14],
        "created_at": row[15],
        "updated_at": row[16],
        "bundle_id": row[17] or "",
        "bundle_status": row[18] or "",
    }


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
