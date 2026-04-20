"""
DuckDB schema 与连接管理。
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path

import duckdb

from meme_detector.archivist.sql_utils import quote_identifier
from meme_detector.config import settings
from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)
_SCHEMA_INIT_LOCK = threading.Lock()
_SCHEMA_INITIALIZED_PATHS: set[str] = set()

# 进程级 DuckDB 连接缓存：每个 db_path 保留一条底层连接，``get_conn`` 为调用方
# 下发 ``cursor()``。DuckDB 的 cursor 是一条共享同一数据库/事务上下文的独立连接，
# 关闭 cursor 不会关闭原始连接，所以既能被既有的 ``with closing(get_conn())`` 模式
# 安全使用，又可以避免以前每次 ``duckdb.connect`` 带来的开销。
_CONN_CACHE: dict[str, duckdb.DuckDBPyConnection] = {}
_CONN_CACHE_LOCK = threading.Lock()

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

# 历史上存在过一张 DuckDB ``meme_records`` 表，但实际的梗库写入/读取全部走
# Meilisearch（``meili_store.upsert_meme`` / ``search_memes``）。该表没有任何读路径，
# 保留只会带来 DuckDB / Meili 双写不一致的风险，因此这里显式 DROP（migration 6）。

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
    status                  TEXT      NOT NULL DEFAULT 'queued',
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
    id                      TEXT      PRIMARY KEY,
    run_id                  TEXT      NOT NULL,
    agent_name              TEXT      NOT NULL,
    word                    TEXT      NOT NULL,
    entity_type             TEXT      DEFAULT '',
    entity_id               TEXT      DEFAULT '',
    status                  TEXT      NOT NULL DEFAULT 'running',
    summary                 TEXT      DEFAULT '',
    started_at              TIMESTAMP DEFAULT NOW(),
    finished_at             TIMESTAMP,
    message_count           INTEGER   NOT NULL DEFAULT 0,
    messages_json           TEXT      DEFAULT '[]',
    output_json             TEXT      DEFAULT '{}',
    public_timeline_json    TEXT      DEFAULT '[]',
    raw_timeline_json       TEXT      DEFAULT '[]',
    input_summary_json      TEXT      DEFAULT '{}',
    token_usage_json        TEXT      DEFAULT '{}',
    langfuse_trace_id       TEXT      DEFAULT '',
    langfuse_session_id     TEXT      DEFAULT '',
    langfuse_public_url     TEXT      DEFAULT '',
    error_message           TEXT      DEFAULT ''
);
"""

_MIGRATE_AGENT_CONVERSATIONS_ENTITY_TYPE = """
ALTER TABLE agent_conversations ADD COLUMN IF NOT EXISTS entity_type TEXT DEFAULT '';
"""

_MIGRATE_AGENT_CONVERSATIONS_ENTITY_ID = """
ALTER TABLE agent_conversations ADD COLUMN IF NOT EXISTS entity_id TEXT DEFAULT '';
"""

_MIGRATE_AGENT_CONVERSATIONS_PUBLIC_TIMELINE = """
ALTER TABLE agent_conversations ADD COLUMN IF NOT EXISTS public_timeline_json TEXT DEFAULT '[]';
"""

_MIGRATE_AGENT_CONVERSATIONS_RAW_TIMELINE = """
ALTER TABLE agent_conversations ADD COLUMN IF NOT EXISTS raw_timeline_json TEXT DEFAULT '[]';
"""

_MIGRATE_AGENT_CONVERSATIONS_INPUT_SUMMARY = """
ALTER TABLE agent_conversations ADD COLUMN IF NOT EXISTS input_summary_json TEXT DEFAULT '{}';
"""

_MIGRATE_AGENT_CONVERSATIONS_TOKEN_USAGE = """
ALTER TABLE agent_conversations ADD COLUMN IF NOT EXISTS token_usage_json TEXT DEFAULT '{}';
"""

_MIGRATE_AGENT_CONVERSATIONS_LANGFUSE_TRACE_ID = """
ALTER TABLE agent_conversations ADD COLUMN IF NOT EXISTS langfuse_trace_id TEXT DEFAULT '';
"""

_MIGRATE_AGENT_CONVERSATIONS_LANGFUSE_SESSION_ID = """
ALTER TABLE agent_conversations ADD COLUMN IF NOT EXISTS langfuse_session_id TEXT DEFAULT '';
"""

_MIGRATE_AGENT_CONVERSATIONS_LANGFUSE_PUBLIC_URL = """
ALTER TABLE agent_conversations ADD COLUMN IF NOT EXISTS langfuse_public_url TEXT DEFAULT '';
"""

_CREATE_AGENT_CONVERSATIONS_ENTITY_INDEX = """
CREATE INDEX IF NOT EXISTS idx_agent_conversations_entity
ON agent_conversations(entity_type, entity_id);
"""

_CREATE_AGENT_TRACE_EVENTS = """
CREATE TABLE IF NOT EXISTS agent_trace_events (
    id                        TEXT      PRIMARY KEY,
    conversation_id           TEXT      NOT NULL,
    run_id                    TEXT      NOT NULL,
    agent_name                TEXT      NOT NULL,
    entity_type               TEXT      DEFAULT '',
    entity_id                 TEXT      DEFAULT '',
    parent_event_id           TEXT,
    step_index                INTEGER   NOT NULL DEFAULT 0,
    event_type                TEXT      NOT NULL,
    stage                     TEXT      DEFAULT '',
    title                     TEXT      DEFAULT '',
    status                    TEXT      DEFAULT '',
    started_at                TIMESTAMP,
    finished_at               TIMESTAMP,
    duration_ms               INTEGER   DEFAULT 0,
    summary                   TEXT      DEFAULT '',
    input_json                TEXT      DEFAULT '{}',
    output_json               TEXT      DEFAULT '{}',
    metadata_json             TEXT      DEFAULT '{}',
    is_user_visible           BOOLEAN   NOT NULL DEFAULT TRUE,
    langfuse_observation_id   TEXT      DEFAULT ''
);
"""

_CREATE_AGENT_TRACE_EVENTS_CONVERSATION_INDEX = """
CREATE INDEX IF NOT EXISTS idx_agent_trace_events_conversation
ON agent_trace_events(conversation_id, step_index);
"""


def get_conn() -> duckdb.DuckDBPyConnection:
    """获取与 ``settings.duckdb_path`` 绑定的 DuckDB 连接。

    返回的是底层共享连接的 ``cursor()``，与原先每次 ``duckdb.connect`` 的 API 完全
    兼容（``executemany`` / 事务 / `` with closing(...) as conn `` 都可用）。cursor
    的 ``close()`` 只释放自身，不会连带关闭共享连接，因此可以被重复调用。
    """

    path = Path(settings.duckdb_path)
    path_key = str(path.resolve())
    with _CONN_CACHE_LOCK:
        conn = _CONN_CACHE.get(path_key)
        if conn is None:
            path.parent.mkdir(parents=True, exist_ok=True)
            conn = duckdb.connect(path_key)
            _CONN_CACHE[path_key] = conn
            _ensure_schema_once(conn, db_key=path_key)
    return conn.cursor()


def reset_connection_cache() -> None:
    """关闭并清空所有缓存的 DuckDB 连接。主要给测试/脚本使用。"""

    with _CONN_CACHE_LOCK:
        for cached in _CONN_CACHE.values():
            try:
                cached.close()
            except Exception:  # pragma: no cover - best effort
                logger.debug("duckdb cached conn close failed", exc_info=True)
        _CONN_CACHE.clear()
        _SCHEMA_INITIALIZED_PATHS.clear()


def _ensure_schema_once(conn: duckdb.DuckDBPyConnection, *, db_key: str) -> None:
    if db_key in _SCHEMA_INITIALIZED_PATHS and _schema_marker_exists(conn):
        return

    with _SCHEMA_INIT_LOCK:
        if db_key in _SCHEMA_INITIALIZED_PATHS and _schema_marker_exists(conn):
            return
        _ensure_schema(conn)
        _SCHEMA_INITIALIZED_PATHS.add(db_key)


def _schema_marker_exists(conn: duckdb.DuckDBPyConnection) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = 'pipeline_runs'
        LIMIT 1
        """
    ).fetchone()
    return row is not None


def _ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(_CREATE_SCOUT_RAW_VIDEOS)
    conn.execute(_CREATE_SCOUT_RAW_COMMENTS)
    conn.execute(_CREATE_MEDIA_ASSETS)
    conn.execute(_CREATE_COMMENT_MEDIA_LINKS)
    _run_schema_action(
        conn,
        name="drop_legacy_meme_records",
        action=lambda: conn.execute("DROP TABLE IF EXISTS meme_records"),
    )
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
    conn.execute(_CREATE_AGENT_TRACE_EVENTS)
    conn.execute(_CREATE_AGENT_TRACE_EVENTS_CONVERSATION_INDEX)
    for name, statement in (
        ("migrate_scout_raw_videos_tags", _MIGRATE_SCOUT_RAW_VIDEOS_TAGS),
        ("migrate_scout_raw_videos_miner_status", _MIGRATE_SCOUT_RAW_VIDEOS_MINER_STATUS),
        ("migrate_scout_raw_videos_miner_processed_at", _MIGRATE_SCOUT_RAW_VIDEOS_MINER_PROCESSED_AT),
        ("migrate_scout_raw_videos_miner_started_at", _MIGRATE_SCOUT_RAW_VIDEOS_MINER_STARTED_AT),
        ("migrate_scout_raw_videos_miner_failed_at", _MIGRATE_SCOUT_RAW_VIDEOS_MINER_FAILED_AT),
        ("migrate_scout_raw_videos_miner_last_error", _MIGRATE_SCOUT_RAW_VIDEOS_MINER_LAST_ERROR),
        ("migrate_scout_raw_videos_miner_attempt_count", _MIGRATE_SCOUT_RAW_VIDEOS_MINER_ATTEMPT_COUNT),
        ("migrate_scout_raw_videos_research_status", _MIGRATE_SCOUT_RAW_VIDEOS_RESEARCH_STATUS),
        ("migrate_scout_raw_videos_research_started_at", _MIGRATE_SCOUT_RAW_VIDEOS_RESEARCH_STARTED_AT),
        ("migrate_agent_conversations_entity_type", _MIGRATE_AGENT_CONVERSATIONS_ENTITY_TYPE),
        ("migrate_agent_conversations_entity_id", _MIGRATE_AGENT_CONVERSATIONS_ENTITY_ID),
        ("migrate_agent_conversations_public_timeline", _MIGRATE_AGENT_CONVERSATIONS_PUBLIC_TIMELINE),
        ("migrate_agent_conversations_raw_timeline", _MIGRATE_AGENT_CONVERSATIONS_RAW_TIMELINE),
        ("migrate_agent_conversations_input_summary", _MIGRATE_AGENT_CONVERSATIONS_INPUT_SUMMARY),
        ("migrate_agent_conversations_token_usage", _MIGRATE_AGENT_CONVERSATIONS_TOKEN_USAGE),
        ("migrate_agent_conversations_langfuse_trace_id", _MIGRATE_AGENT_CONVERSATIONS_LANGFUSE_TRACE_ID),
        ("migrate_agent_conversations_langfuse_session_id", _MIGRATE_AGENT_CONVERSATIONS_LANGFUSE_SESSION_ID),
        ("migrate_agent_conversations_langfuse_public_url", _MIGRATE_AGENT_CONVERSATIONS_LANGFUSE_PUBLIC_URL),
    ):
        _run_schema_action(conn, name=name, action=lambda stmt=statement: conn.execute(stmt))

    _run_schema_action(
        conn,
        name="rename_candidate_status_to_research_status",
        action=lambda: _rename_column_if_present(
            conn,
            table_name="scout_raw_videos",
            old_name="candidate_status",
            new_name="research_status",
        ),
    )
    _run_schema_action(
        conn,
        name="rename_candidate_extracted_at_to_research_started_at",
        action=lambda: _rename_column_if_present(
            conn,
            table_name="scout_raw_videos",
            old_name="candidate_extracted_at",
            new_name="research_started_at",
        ),
    )
    _run_schema_action(
        conn,
        name="create_agent_conversations_entity_index",
        action=lambda: conn.execute(_CREATE_AGENT_CONVERSATIONS_ENTITY_INDEX),
    )


def _run_schema_action(
    conn: duckdb.DuckDBPyConnection,
    *,
    name: str,
    action: Callable[[], None],
) -> None:
    try:
        action()
    except Exception:
        logger.warning(
            "duckdb schema action failed",
            extra={"event": "duckdb_schema_action_failed", "action_name": name},
            exc_info=True,
        )


def _column_exists(
    conn: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    column_name: str,
) -> bool:
    rows = conn.execute(f"PRAGMA table_info({quote_identifier(table_name)})").fetchall()
    return any(str(row[1]).strip() == column_name for row in rows)


def _rename_column_if_present(
    conn: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    old_name: str,
    new_name: str,
) -> None:
    quoted_table_name = quote_identifier(table_name)
    quoted_old_name = quote_identifier(old_name)
    quoted_new_name = quote_identifier(new_name)
    if _column_exists(conn, table_name=table_name, column_name=new_name):
        return
    if not _column_exists(conn, table_name=table_name, column_name=old_name):
        return
    conn.execute(f"ALTER TABLE {quoted_table_name} RENAME COLUMN {quoted_old_name} TO {quoted_new_name}")
