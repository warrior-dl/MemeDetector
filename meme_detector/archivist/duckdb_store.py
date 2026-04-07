"""
DuckDB 存储层：管理词频时序数据和候选词队列。
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

_CREATE_WORD_FREQ = """
CREATE TABLE IF NOT EXISTS word_freq (
    word        TEXT    NOT NULL,
    date        DATE    NOT NULL,
    partition   TEXT    NOT NULL,
    freq        INTEGER NOT NULL,
    doc_count   INTEGER NOT NULL,
    PRIMARY KEY (word, date, partition)
);
"""

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
    candidate_status      TEXT      NOT NULL DEFAULT 'pending',
    candidate_extracted_at TIMESTAMP,
    miner_status          TEXT      NOT NULL DEFAULT 'pending',
    miner_processed_at    TIMESTAMP,
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

_CREATE_CANDIDATES = """
CREATE TABLE IF NOT EXISTS candidates (
    word          TEXT      PRIMARY KEY,
    score         DOUBLE    NOT NULL,
    is_new_word   BOOLEAN   NOT NULL,
    sample_comments TEXT    DEFAULT '',
    explanation   TEXT      DEFAULT '',
    video_refs_json TEXT    DEFAULT '[]',
    detected_at   TIMESTAMP DEFAULT NOW(),
    status        TEXT      DEFAULT 'pending'
);
"""

_MIGRATE_CANDIDATES_EXPLANATION = """
ALTER TABLE candidates ADD COLUMN IF NOT EXISTS explanation TEXT DEFAULT '';
"""

_MIGRATE_CANDIDATES_VIDEO_REFS = """
ALTER TABLE candidates ADD COLUMN IF NOT EXISTS video_refs_json TEXT DEFAULT '[]';
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
    conn.execute(_CREATE_WORD_FREQ)
    conn.execute(_CREATE_SCOUT_RAW_VIDEOS)
    conn.execute(_CREATE_SCOUT_RAW_COMMENTS)
    conn.execute(_CREATE_MEDIA_ASSETS)
    conn.execute(_CREATE_COMMENT_MEDIA_LINKS)
    conn.execute(_CREATE_CANDIDATES)
    conn.execute(_CREATE_MEME_RECORDS)
    conn.execute(_CREATE_PIPELINE_RUNS)
    conn.execute(_CREATE_VIDEO_CONTEXT_CACHE)
    conn.execute(_CREATE_MINER_COMMENT_INSIGHTS)
    conn.execute(_CREATE_AGENT_CONVERSATIONS)
    # 兼容旧库：补充新增列
    try:
        conn.execute(_MIGRATE_CANDIDATES_EXPLANATION)
    except Exception:
        pass
    try:
        conn.execute(_MIGRATE_CANDIDATES_VIDEO_REFS)
    except Exception:
        pass
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


def upsert_word_freq(
    conn: duckdb.DuckDBPyConnection,
    records: list[dict],
    partition: str,
    target_date: date,
) -> None:
    """
    批量写入词频数据。
    records: [{"word": str, "freq": int, "doc_count": int}, ...]
    """
    if not records:
        return
    rows = [
        (r["word"], target_date, partition, r["freq"], r["doc_count"])
        for r in records
    ]
    conn.executemany(
        """
        INSERT INTO word_freq (word, date, partition, freq, doc_count)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (word, date, partition) DO UPDATE
            SET freq = excluded.freq,
                doc_count = excluded.doc_count
        """,
        rows,
    )


def upsert_scout_raw_videos(
    conn: duckdb.DuckDBPyConnection,
    videos: list[dict],
    target_date: date,
) -> None:
    """批量写入 Scout 采集到的视频元信息和评论快照。"""
    if not videos:
        return

    rows = []
    structured_videos: list[dict] = []
    now = datetime.now()
    for video in videos:
        comments = video.get("comments", [])
        if not isinstance(comments, list):
            comments = []
        comments = [str(comment).strip() for comment in comments if str(comment).strip()]
        comment_snapshots = video.get("comment_snapshots", [])
        if not isinstance(comment_snapshots, list):
            comment_snapshots = []
        structured_videos.append(
            {
                "bvid": str(video.get("bvid", "")).strip(),
                "partition": str(video.get("partition", "")).strip(),
                "title": str(video.get("title", "")).strip(),
                "description": str(video.get("description", "")).strip(),
                "url": str(video.get("url", "")).strip(),
                "tags": [
                    str(tag).strip()
                    for tag in (video.get("tags", []) or [])
                    if str(tag).strip()
                ],
                "comments": comments,
                "comment_snapshots": comment_snapshots,
            }
        )
        rows.append(
            (
                str(video.get("bvid", "")).strip(),
                target_date,
                str(video.get("partition", "")).strip(),
                str(video.get("title", "")).strip(),
                str(video.get("description", "")).strip(),
                str(video.get("url", "")).strip(),
                json.dumps(
                    [
                        str(tag).strip()
                        for tag in (video.get("tags", []) or [])
                        if str(tag).strip()
                    ],
                    ensure_ascii=False,
                ),
                json.dumps(comments, ensure_ascii=False),
                len(comments),
                now,
            )
        )

    valid_rows = [row for row in rows if row[0] and row[2] and row[5]]
    if not valid_rows:
        return

    conn.executemany(
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
            candidate_status,
            candidate_extracted_at,
            miner_status,
            miner_processed_at,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, 'pending', NULL, ?, ?)
        ON CONFLICT (bvid, collected_date) DO UPDATE
        SET partition = excluded.partition,
            title = excluded.title,
            description = excluded.description,
            video_url = excluded.video_url,
            tags_json = excluded.tags_json,
            comments_json = excluded.comments_json,
            comment_count = excluded.comment_count,
            candidate_status = 'pending',
            candidate_extracted_at = NULL,
            miner_status = 'pending',
            miner_processed_at = NULL,
            updated_at = excluded.updated_at
        """,
        [(*row, row[-1]) for row in valid_rows],
    )

    for video in structured_videos:
        if not video["bvid"] or not video["partition"] or not video["url"]:
            continue
        _upsert_scout_raw_comments(
            conn,
            video=video,
            target_date=target_date,
            collected_at=now,
        )


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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
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
                WHEN miner_comment_insights.status = 'processed' THEN miner_comment_insights.status
                ELSE 'pending'
            END
        """,
        rows,
    )


def get_pending_miner_comment_insights(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int = 200,
) -> list[dict]:
    rows = conn.execute(
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
            created_at,
            updated_at
        FROM miner_comment_insights
        WHERE status = 'pending'
        ORDER BY confidence DESC, collected_date ASC, bvid ASC
        LIMIT ?
        """,
        [limit],
    ).fetchall()
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
            "created_at": row[14],
            "updated_at": row[15],
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
            updated_at
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
            updated_at
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
        SET status = 'processed',
            updated_at = ?
        WHERE insight_id = ?
        """,
        rows,
    )


def get_pending_scout_raw_videos(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int | None = None,
) -> list[dict]:
    """获取尚未被 Miner 消费的 Scout 原始视频快照。"""
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
            miner_processed_at,
            created_at,
            updated_at
        FROM scout_raw_videos
        WHERE miner_status = 'pending'
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
            "miner_processed_at": row[10],
            "created_at": row[11],
            "updated_at": row[12],
        }
        for row in rows
    ]


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
            miner_processed_at = ?,
            updated_at = ?
        WHERE bvid = ? AND collected_date = ?
        """,
        [(row[0], row[0], row[1], row[2]) for row in rows],
    )


def get_scout_raw_videos_page(
    conn: duckdb.DuckDBPyConnection,
    *,
    candidate_status: str | None = None,
    partition: str | None = None,
    keyword: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    """分页获取 Scout 原始视频快照简表。"""
    where_parts: list[str] = []
    params: list[str | int] = []

    if candidate_status:
        where_parts.append("candidate_status = ?")
        params.append(candidate_status)
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
            miner_processed_at,
            candidate_status,
            candidate_extracted_at,
            created_at,
            updated_at,
            comments_json,
            COALESCE((
                SELECT SUM(picture_count)
                FROM scout_raw_comments comments
                WHERE comments.bvid = scout_raw_videos.bvid
                  AND comments.collected_date = scout_raw_videos.collected_date
            ), 0) AS picture_count
        FROM scout_raw_videos
        {where_clause}
        ORDER BY collected_date DESC, updated_at DESC, bvid ASC
        LIMIT ?
        OFFSET ?
        """,
        [*params, limit, offset],
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
            miner_processed_at,
            candidate_status,
            candidate_extracted_at,
            created_at,
            updated_at,
            COALESCE((
                SELECT SUM(picture_count)
                FROM scout_raw_comments comments
                WHERE comments.bvid = scout_raw_videos.bvid
                  AND comments.collected_date = scout_raw_videos.collected_date
            ), 0) AS picture_count
        FROM scout_raw_videos
        WHERE bvid = ? AND collected_date = ?
        """,
        [bvid, collected_date],
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


def mark_scout_raw_videos_processed(
    conn: duckdb.DuckDBPyConnection,
    videos: list[dict],
) -> None:
    """将原始视频快照标记为已完成候选词提取。"""
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
        SET candidate_status = 'processed',
            candidate_extracted_at = ?,
            updated_at = ?
        WHERE bvid = ? AND collected_date = ?
        """,
        [(row[0], row[0], row[1], row[2]) for row in rows],
    )


def compute_candidates(
    conn: duckdb.DuckDBPyConnection,
    current_date: date,
    baseline_days: int = 14,
    score_threshold: float | None = None,
    new_word_min_docs: int | None = None,
) -> list[dict]:
    """
    计算当日候选词：
    - 老词：当日词频 / 过去 baseline_days 天均值 >= score_threshold
    - 新词：baseline 期间从未出现 AND doc_count >= new_word_min_docs
    返回候选词列表，并写入 candidates 表。
    """
    threshold = score_threshold or settings.scout_score_threshold
    min_docs = new_word_min_docs or settings.scout_new_word_min_docs

    rows = conn.execute(
        """
        WITH current AS (
            SELECT word,
                   SUM(freq)      AS curr_freq,
                   SUM(doc_count) AS curr_docs
            FROM word_freq
            WHERE date = ?
            GROUP BY word
        ),
        baseline AS (
            SELECT word,
                   AVG(daily_freq)   AS baseline_avg
            FROM (
                SELECT word, date, SUM(freq) AS daily_freq
                FROM word_freq
                WHERE date BETWEEN (? - INTERVAL (?) DAY) AND (? - INTERVAL 1 DAY)
                GROUP BY word, date
            )
            GROUP BY word
        )
        SELECT
            c.word,
            c.curr_freq,
            c.curr_docs,
            COALESCE(b.baseline_avg, 0) AS baseline_avg,
            CASE
                WHEN COALESCE(b.baseline_avg, 0) < 0.5 THEN TRUE
                ELSE FALSE
            END AS is_new_word,
            CASE
                WHEN COALESCE(b.baseline_avg, 0) < 0.5 THEN 999.0
                ELSE CAST(c.curr_freq AS DOUBLE) / b.baseline_avg
            END AS score
        FROM current c
        LEFT JOIN baseline b ON c.word = b.word
        WHERE
            -- 老词条件
            (COALESCE(b.baseline_avg, 0) >= 0.5 AND
             CAST(c.curr_freq AS DOUBLE) / b.baseline_avg >= ?)
            OR
            -- 新词条件
            (COALESCE(b.baseline_avg, 0) < 0.5 AND c.curr_docs >= ?)
        ORDER BY score DESC
        """,
        [current_date, current_date, baseline_days, current_date, threshold, min_docs],
    ).fetchall()

    candidates = []
    for word, curr_freq, curr_docs, baseline_avg, is_new_word, score in rows:
        candidates.append(
            {
                "word": word,
                "score": score,
                "is_new_word": is_new_word,
                "curr_freq": curr_freq,
                "curr_docs": curr_docs,
            }
        )

    # 写入候选词表（IGNORE 已存在的，保留人工审核状态）
    if candidates:
        conn.executemany(
            """
            INSERT OR IGNORE INTO candidates (word, score, is_new_word)
            VALUES (?, ?, ?)
            """,
            [(c["word"], c["score"], c["is_new_word"]) for c in candidates],
        )

    return candidates


def get_candidates(
    conn: duckdb.DuckDBPyConnection,
    status: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """获取候选梗列表，支持按状态过滤。status=None 返回全部。"""
    if status:
        rows = conn.execute(
            """
            SELECT
                word,
                score,
                is_new_word,
                sample_comments,
                explanation,
                video_refs_json,
                detected_at,
                status
            FROM candidates
            WHERE status = ?
            ORDER BY score DESC
            LIMIT ?
            """,
            [status, limit],
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT
                word,
                score,
                is_new_word,
                sample_comments,
                explanation,
                video_refs_json,
                detected_at,
                status
            FROM candidates
            ORDER BY score DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    return [
        {
            "word": r[0],
            "score": r[1],
            "is_new_word": r[2],
            "sample_comments": r[3],
            "explanation": r[4],
            "video_refs": _load_json_text(r[5], default=[]),
            "detected_at": r[6],
            "status": r[7],
        }
        for r in rows
    ]


def get_candidate(
    conn: duckdb.DuckDBPyConnection,
    word: str,
) -> dict | None:
    """按词条精确获取单个候选梗。"""
    row = conn.execute(
        """
        SELECT
            word,
            score,
            is_new_word,
            sample_comments,
            explanation,
            video_refs_json,
            detected_at,
            status
        FROM candidates
        WHERE word = ?
        """,
        [word],
    ).fetchone()
    if not row:
        return None
    return {
        "word": row[0],
        "score": row[1],
        "is_new_word": row[2],
        "sample_comments": row[3],
        "explanation": row[4],
        "video_refs": _load_json_text(row[5], default=[]),
        "detected_at": row[6],
        "status": row[7],
    }


def get_candidates_page(
    conn: duckdb.DuckDBPyConnection,
    *,
    status: str | None = None,
    keyword: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    """分页获取候选梗完整信息。"""
    where_parts: list[str] = []
    params: list[str | int] = []
    if status:
        where_parts.append("status = ?")
        params.append(status)
    if keyword:
        where_parts.append("(word LIKE ? OR explanation LIKE ? OR sample_comments LIKE ?)")
        wildcard_keyword = f"%{keyword}%"
        params.extend([wildcard_keyword, wildcard_keyword, wildcard_keyword])

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    total = conn.execute(
        f"SELECT COUNT(*) FROM candidates {where_clause}",
        params,
    ).fetchone()[0]

    page_params = [*params, limit, offset]
    rows = conn.execute(
        f"""
        SELECT
            word,
            score,
            is_new_word,
            sample_comments,
            explanation,
            video_refs_json,
            detected_at,
            status
        FROM candidates
        {where_clause}
        ORDER BY detected_at DESC, score DESC, word ASC
        LIMIT ?
        OFFSET ?
        """,
        page_params,
    ).fetchall()

    items = [
        {
            "word": row[0],
            "score": row[1],
            "is_new_word": row[2],
            "sample_comments": row[3],
            "explanation": row[4],
            "video_refs": _load_json_text(row[5], default=[]),
            "detected_at": row[6],
            "status": row[7],
        }
        for row in rows
    ]
    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def get_candidate_source_insights(
    conn: duckdb.DuckDBPyConnection,
    *,
    word: str,
    limit: int = 100,
) -> dict | None:
    """获取候选词及其来源视频/评论线索。"""
    candidate = get_candidate(conn, word)
    if not candidate:
        return None

    video_refs = candidate.get("video_refs", [])
    if not isinstance(video_refs, list):
        video_refs = []
    normalized_video_refs = [item for item in video_refs if isinstance(item, dict)]
    bvids = [str(item.get("bvid", "")).strip() for item in normalized_video_refs if str(item.get("bvid", "")).strip()]
    if not bvids:
        return {
            "candidate": candidate,
            "video_refs": [],
            "source_insights": [],
        }

    placeholders = ", ".join("?" for _ in bvids)
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
            updated_at
        FROM miner_comment_insights
        WHERE bvid IN ({placeholders})
        """,
        bvids,
    ).fetchall()

    matched_comments_by_bvid: dict[str, set[str]] = {}
    for item in normalized_video_refs:
        bvid = str(item.get("bvid", "")).strip()
        if not bvid:
            continue
        comments = item.get("matched_comments", [])
        if not isinstance(comments, list):
            comments = []
        matched_comments_by_bvid.setdefault(bvid, set()).update(
            str(comment).strip() for comment in comments if str(comment).strip()
        )

    enriched: list[tuple[int, dict]] = []
    for row in rows:
        item = _serialize_miner_comment_insight(row)
        comment_text = str(item.get("comment_text", "")).strip()
        reason = str(item.get("reason", "")).strip()
        title = str(item.get("title", "")).strip()
        description = str(item.get("description", "")).strip()
        bvid = str(item.get("bvid", "")).strip()
        matched_comments = matched_comments_by_bvid.get(bvid, set())

        relevance = 0
        if comment_text and comment_text in matched_comments:
            relevance += 6
        if word and word in comment_text:
            relevance += 4
        if word and (word in title or word in description or word in reason):
            relevance += 2
        if item.get("is_meme_candidate"):
            relevance += 1
        if item.get("is_insider_knowledge"):
            relevance += 1

        if relevance <= 0 and matched_comments:
            continue
        item["matched_by_candidate_word"] = bool(word and word in comment_text)
        item["matched_by_video_ref_comments"] = bool(comment_text and comment_text in matched_comments)
        item["relevance_score"] = relevance
        enriched.append((relevance, item))

    enriched.sort(
        key=lambda pair: (
            pair[0],
            float(pair[1].get("confidence", 0.0) or 0.0),
            str(pair[1].get("bvid", "")),
            str(pair[1].get("insight_id", "")),
        ),
        reverse=True,
    )

    return {
        "candidate": candidate,
        "video_refs": normalized_video_refs,
        "source_insights": [item for _, item in enriched[:limit]],
    }


def get_pending_candidates(
    conn: duckdb.DuckDBPyConnection, limit: int = 100
) -> list[dict]:
    """获取待 AI 分析的候选词。"""
    rows = conn.execute(
        """
        SELECT word, score, is_new_word, sample_comments, explanation, video_refs_json, detected_at
        FROM candidates
        WHERE status = 'pending'
        ORDER BY score DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    return [
        {
            "word": r[0],
            "score": r[1],
            "is_new_word": r[2],
            "sample_comments": r[3],
            "explanation": r[4],
            "video_refs": _load_json_text(r[5], default=[]),
            "detected_at": r[6],
        }
        for r in rows
    ]


def update_candidate_status(
    conn: duckdb.DuckDBPyConnection,
    word: str,
    status: str,  # 'accepted' | 'rejected' | 'pending'
) -> None:
    conn.execute(
        "UPDATE candidates SET status = ? WHERE word = ?",
        [status, word],
    )


def update_candidate_comments(
    conn: duckdb.DuckDBPyConnection,
    word: str,
    sample_comments: str,
) -> None:
    conn.execute(
        "UPDATE candidates SET sample_comments = ? WHERE word = ?",
        [sample_comments, word],
    )


def update_candidate_context(
    conn: duckdb.DuckDBPyConnection,
    word: str,
    *,
    sample_comments: str,
    explanation: str,
    video_refs: list[dict],
) -> None:
    conn.execute(
        """
        UPDATE candidates
        SET sample_comments = ?, explanation = ?, video_refs_json = ?
        WHERE word = ?
        """,
        [
            sample_comments,
            explanation,
            json.dumps(video_refs, ensure_ascii=False),
            word,
        ],
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


def delete_all_candidates(conn: duckdb.DuckDBPyConnection) -> int:
    """删除全部候选梗。"""
    deleted_count = conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
    conn.execute("DELETE FROM candidates")
    return deleted_count


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


def upsert_scout_candidates(
    conn: duckdb.DuckDBPyConnection,
    candidates: list[dict],
) -> None:
    """
    写入或刷新 Scout 产出的候选词上下文，保留既有审核状态。

    兼容两种输入格式：
    - 旧格式: {"phrase", "explanation", "examples", "confidence"}
    - 新格式: {"word", "score", "is_new_word", "sample_comments", "video_refs"}
    """
    if not candidates:
        return
    rows = []
    for c in candidates:
        word = str(c.get("word") or c.get("phrase") or "").strip()
        if not word:
            continue
        score = float(c.get("score", c.get("confidence", 0.5)))
        is_new_word = bool(c.get("is_new_word", True))
        explanation = str(c.get("explanation", ""))
        video_refs = c.get("video_refs", [])
        if not isinstance(video_refs, list):
            video_refs = []

        sample_comments = str(c.get("sample_comments", "")).strip()
        if not sample_comments:
            examples = c.get("examples", [])
            if isinstance(examples, list):
                sample_comments = "\n".join(f"- {e}" for e in examples if e)

        rows.append(
            (
                word,
                score,
                is_new_word,
                sample_comments,
                explanation,
                json.dumps(video_refs, ensure_ascii=False),
            )
        )

    conn.executemany(
        """
        INSERT INTO candidates (
            word,
            score,
            is_new_word,
            sample_comments,
            explanation,
            video_refs_json
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (word) DO UPDATE
        SET score = excluded.score,
            is_new_word = excluded.is_new_word,
            sample_comments = excluded.sample_comments,
            explanation = excluded.explanation,
            video_refs_json = excluded.video_refs_json
        """,
        rows,
    )


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
        "miner_processed_at": row[10],
        "candidate_status": row[11],
        "candidate_extracted_at": row[12],
        "created_at": row[13],
        "updated_at": row[14],
        "picture_count": row[15],
        "pipeline_stage": _build_scout_pipeline_stage(
            miner_status=row[9],
            candidate_status=row[11],
        ),
    }


def _serialize_scout_raw_video_summary(row: tuple) -> dict:
    tags = _load_json_text(row[5], default=[])
    if not isinstance(tags, list):
        tags = []

    comments = _load_json_text(row[13], default=[])
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
        "miner_processed_at": row[8],
        "candidate_status": row[9],
        "candidate_extracted_at": row[10],
        "created_at": row[11],
        "updated_at": row[12],
        "first_comment": first_comment,
        "picture_count": row[14],
        "pipeline_stage": _build_scout_pipeline_stage(
            miner_status=row[7],
            candidate_status=row[9],
        ),
    }


def _build_scout_pipeline_stage(*, miner_status: str, candidate_status: str) -> str:
    if candidate_status == "processed":
        return "researched"
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
