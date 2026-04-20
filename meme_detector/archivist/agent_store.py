"""
Agent 对话与 trace 的 DuckDB 读写。
"""

from __future__ import annotations

import json
from datetime import datetime
from uuid import uuid4

import duckdb

from meme_detector.archivist.sql_utils import build_where_clause, count_rows

def create_agent_conversation(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    agent_name: str,
    word: str,
    entity_type: str = "",
    entity_id: str = "",
    langfuse_session_id: str = "",
) -> str:
    """创建一条 agent 对话记录。"""
    conversation_id = uuid4().hex
    conn.execute(
        """
        INSERT INTO agent_conversations (
            id,
            run_id,
            agent_name,
            word,
            entity_type,
            entity_id,
            status,
            started_at,
            langfuse_session_id
        )
        VALUES (?, ?, ?, ?, ?, ?, 'running', ?, ?)
        """,
        [
            conversation_id,
            run_id,
            agent_name,
            word,
            entity_type,
            entity_id,
            datetime.now(),
            langfuse_session_id,
        ],
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
    public_timeline_json: str = "[]",
    raw_timeline_json: str = "[]",
    input_summary_json: str = "{}",
    token_usage_json: str = "{}",
    langfuse_trace_id: str = "",
    langfuse_public_url: str = "",
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
            public_timeline_json = ?,
            raw_timeline_json = ?,
            input_summary_json = ?,
            token_usage_json = ?,
            langfuse_trace_id = ?,
            langfuse_public_url = ?,
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
            public_timeline_json,
            raw_timeline_json,
            input_summary_json,
            token_usage_json,
            langfuse_trace_id,
            langfuse_public_url,
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
    entity_type: str | None = None,
    entity_id: str | None = None,
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
    if entity_type:
        where_parts.append("entity_type = ?")
        params.append(entity_type)
    if entity_id:
        where_parts.append("entity_id = ?")
        params.append(entity_id)
    if status:
        where_parts.append("status = ?")
        params.append(status)

    where_clause = build_where_clause(where_parts)
    total = count_rows(
        conn,
        from_clause="agent_conversations",
        where_clause=where_clause,
        params=params,
    )

    rows = conn.execute(
        f"""
        SELECT
            id,
            run_id,
            agent_name,
            word,
            entity_type,
            entity_id,
            status,
            summary,
            started_at,
            finished_at,
            message_count,
            langfuse_trace_id,
            langfuse_public_url,
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
                "entity_type": row[4],
                "entity_id": row[5],
                "status": row[6],
                "summary": row[7],
                "started_at": row[8],
                "finished_at": row[9],
                "message_count": row[10],
                "langfuse_trace_id": row[11],
                "langfuse_public_url": row[12],
                "error_message": row[13],
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
            entity_type,
            entity_id,
            status,
            summary,
            started_at,
            finished_at,
            message_count,
            messages_json,
            output_json,
            public_timeline_json,
            raw_timeline_json,
            input_summary_json,
            token_usage_json,
            langfuse_trace_id,
            langfuse_session_id,
            langfuse_public_url,
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
        "entity_type": row[4],
        "entity_id": row[5],
        "status": row[6],
        "summary": row[7],
        "started_at": row[8],
        "finished_at": row[9],
        "message_count": row[10],
        "messages": _load_json_text(row[11], default=[]),
        "output": _load_json_text(row[12], default={}),
        "public_timeline": _load_json_text(row[13], default=[]),
        "raw_timeline": _load_json_text(row[14], default=[]),
        "input_summary": _load_json_text(row[15], default={}),
        "token_usage": _load_json_text(row[16], default={}),
        "langfuse_trace_id": row[17],
        "langfuse_session_id": row[18],
        "langfuse_public_url": row[19],
        "error_message": row[20],
    }


def replace_agent_trace_events(
    conn: duckdb.DuckDBPyConnection,
    *,
    conversation_id: str,
    run_id: str,
    agent_name: str,
    entity_type: str = "",
    entity_id: str = "",
    events: list[dict],
) -> None:
    conn.execute("DELETE FROM agent_trace_events WHERE conversation_id = ?", [conversation_id])
    for event in events:
        conn.execute(
            """
            INSERT INTO agent_trace_events (
                id,
                conversation_id,
                run_id,
                agent_name,
                entity_type,
                entity_id,
                parent_event_id,
                step_index,
                event_type,
                stage,
                title,
                status,
                started_at,
                finished_at,
                duration_ms,
                summary,
                input_json,
                output_json,
                metadata_json,
                is_user_visible,
                langfuse_observation_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(event.get("id") or uuid4().hex),
                conversation_id,
                run_id,
                agent_name,
                entity_type,
                entity_id,
                str(event.get("parent_event_id") or "") or None,
                int(event.get("step_index") or 0),
                str(event.get("event_type") or "").strip(),
                str(event.get("stage") or "").strip(),
                str(event.get("title") or "").strip(),
                str(event.get("status") or "").strip(),
                event.get("started_at"),
                event.get("finished_at"),
                int(event.get("duration_ms") or 0),
                str(event.get("summary") or "").strip(),
                json.dumps(event.get("input"), ensure_ascii=False, default=str),
                json.dumps(event.get("output"), ensure_ascii=False, default=str),
                json.dumps(event.get("metadata") or {}, ensure_ascii=False, default=str),
                bool(event.get("is_user_visible", True)),
                str(event.get("langfuse_observation_id") or "").strip(),
            ],
        )


def list_agent_trace_events(
    conn: duckdb.DuckDBPyConnection,
    *,
    conversation_id: str,
) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            id,
            parent_event_id,
            step_index,
            event_type,
            stage,
            title,
            status,
            started_at,
            finished_at,
            duration_ms,
            summary,
            input_json,
            output_json,
            metadata_json,
            is_user_visible,
            langfuse_observation_id
        FROM agent_trace_events
        WHERE conversation_id = ?
        ORDER BY step_index ASC, started_at ASC, id ASC
        """,
        [conversation_id],
    ).fetchall()
    return [
        {
            "id": row[0],
            "parent_event_id": row[1],
            "step_index": int(row[2] or 0),
            "event_type": row[3],
            "stage": row[4],
            "title": row[5],
            "status": row[6],
            "started_at": row[7],
            "finished_at": row[8],
            "duration_ms": int(row[9] or 0),
            "summary": row[10],
            "input": _load_json_text(row[11], default={}),
            "output": _load_json_text(row[12], default={}),
            "metadata": _load_json_text(row[13], default={}),
            "is_user_visible": bool(row[14]),
            "langfuse_observation_id": row[15],
        }
        for row in rows
    ]


def get_agent_conversation_trace(
    conn: duckdb.DuckDBPyConnection,
    conversation_id: str,
) -> dict | None:
    conversation = get_agent_conversation(conn, conversation_id)
    if not conversation:
        return None
    return {
        "conversation": conversation,
        "steps": list_agent_trace_events(conn, conversation_id=conversation_id),
    }


def _load_json_text(value, *, default):
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default
