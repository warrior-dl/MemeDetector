"""
Pipeline run 记录的 DuckDB 读写。
"""

from __future__ import annotations

import json
from datetime import datetime
from uuid import uuid4

import duckdb


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
