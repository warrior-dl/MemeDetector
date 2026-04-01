"""
DuckDB 存储层：管理词频时序数据和候选词队列。
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
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
    comments_json         TEXT      DEFAULT '[]',
    comment_count         INTEGER   DEFAULT 0,
    candidate_status      TEXT      NOT NULL DEFAULT 'pending',
    candidate_extracted_at TIMESTAMP,
    created_at            TIMESTAMP DEFAULT NOW(),
    updated_at            TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (bvid, collected_date)
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
    conn.execute(_CREATE_CANDIDATES)
    conn.execute(_CREATE_MEME_RECORDS)
    conn.execute(_CREATE_PIPELINE_RUNS)
    conn.execute(_CREATE_VIDEO_CONTEXT_CACHE)
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
    now = datetime.now()
    for video in videos:
        comments = video.get("comments", [])
        if not isinstance(comments, list):
            comments = []
        comments = [str(comment).strip() for comment in comments if str(comment).strip()]
        rows.append(
            (
                str(video.get("bvid", "")).strip(),
                target_date,
                str(video.get("partition", "")).strip(),
                str(video.get("title", "")).strip(),
                str(video.get("description", "")).strip(),
                str(video.get("url", "")).strip(),
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
            comments_json,
            comment_count,
            candidate_status,
            candidate_extracted_at,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)
        ON CONFLICT (bvid, collected_date) DO UPDATE
        SET partition = excluded.partition,
            title = excluded.title,
            description = excluded.description,
            video_url = excluded.video_url,
            comments_json = excluded.comments_json,
            comment_count = excluded.comment_count,
            candidate_status = 'pending',
            candidate_extracted_at = NULL,
            updated_at = excluded.updated_at
        """,
        [(*row, row[-1]) for row in valid_rows],
    )


def get_pending_scout_raw_videos(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int | None = None,
) -> list[dict]:
    """获取尚未转成候选词的 Scout 原始视频快照。"""
    sql = """
        SELECT
            bvid,
            collected_date,
            partition,
            title,
            description,
            video_url,
            comments_json,
            comment_count,
            created_at,
            updated_at
        FROM scout_raw_videos
        WHERE candidate_status = 'pending'
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
            "comments": _load_json_text(row[6], default=[]),
            "comment_count": row[7],
            "created_at": row[8],
            "updated_at": row[9],
        }
        for row in rows
    ]


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


def get_candidates_page(
    conn: duckdb.DuckDBPyConnection,
    *,
    status: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    """分页获取候选梗完整信息。"""
    where_clause = ""
    params: list[str | int] = []
    if status:
        where_clause = "WHERE status = ?"
        params.append(status)

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


def _load_json_text(value: str | None, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default
