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

_CREATE_CANDIDATES = """
CREATE TABLE IF NOT EXISTS candidates (
    word          TEXT      PRIMARY KEY,
    score         DOUBLE    NOT NULL,
    is_new_word   BOOLEAN   NOT NULL,
    sample_comments TEXT    DEFAULT '',
    explanation   TEXT      DEFAULT '',
    detected_at   TIMESTAMP DEFAULT NOW(),
    status        TEXT      DEFAULT 'pending'
);
"""

_MIGRATE_CANDIDATES_EXPLANATION = """
ALTER TABLE candidates ADD COLUMN IF NOT EXISTS explanation TEXT DEFAULT '';
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


def get_conn() -> duckdb.DuckDBPyConnection:
    """返回持久化的 DuckDB 连接。"""
    path = Path(settings.duckdb_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(_CREATE_WORD_FREQ)
    conn.execute(_CREATE_CANDIDATES)
    conn.execute(_CREATE_MEME_RECORDS)
    conn.execute(_CREATE_PIPELINE_RUNS)
    # 兼容旧库：补充新增列
    try:
        conn.execute(_MIGRATE_CANDIDATES_EXPLANATION)
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
            SELECT word, score, is_new_word, sample_comments, explanation, detected_at, status
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
            SELECT word, score, is_new_word, sample_comments, explanation, detected_at, status
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
            "detected_at": r[5],
            "status": r[6],
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
        SELECT word, score, is_new_word, sample_comments, explanation, detected_at, status
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
            "detected_at": row[5],
            "status": row[6],
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
        SELECT word, score, is_new_word, sample_comments, explanation, detected_at
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
            "detected_at": r[5],
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


def delete_all_candidates(conn: duckdb.DuckDBPyConnection) -> int:
    """删除全部候选梗。"""
    deleted_count = conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
    conn.execute("DELETE FROM candidates")
    return deleted_count


def upsert_scout_candidates(
    conn: duckdb.DuckDBPyConnection,
    candidates: list[dict],
) -> None:
    """
    写入 Scout LLM 识别的梗候选（IGNORE 已存在的，保留人工审核状态）。

    candidates: [{"phrase": str, "explanation": str, "examples": list[str],
                 "confidence": float}, ...]
    """
    if not candidates:
        return
    rows = []
    for c in candidates:
        phrase = c.get("phrase", "").strip()
        if not phrase:
            continue
        score = float(c.get("confidence", 0.5))
        explanation = c.get("explanation", "")
        examples = c.get("examples", [])
        sample_comments = "\n".join(f"- {e}" for e in examples if e)
        rows.append((phrase, score, True, sample_comments, explanation))

    conn.executemany(
        """
        INSERT OR IGNORE INTO candidates (word, score, is_new_word, sample_comments, explanation)
        VALUES (?, ?, ?, ?, ?)
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
