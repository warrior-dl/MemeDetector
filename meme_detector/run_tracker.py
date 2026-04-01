"""
Pipeline 任务运行记录封装。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any

from meme_detector.archivist.duckdb_store import (
    create_pipeline_run,
    finish_pipeline_run,
    get_conn,
)

_current_run_id: ContextVar[str | None] = ContextVar("current_run_id", default=None)


async def execute_tracked_job(
    job_name: str,
    runner: Callable[[], Awaitable[Any]],
    *,
    trigger_mode: str,
) -> Any:
    """执行异步任务并记录运行结果。"""
    conn = get_conn()
    run_id = create_pipeline_run(conn, job_name=job_name, trigger_mode=trigger_mode)
    conn.close()

    token = _current_run_id.set(run_id)
    try:
        result = await runner()
    except Exception as exc:
        _current_run_id.reset(token)
        conn = get_conn()
        finish_pipeline_run(
            conn,
            run_id,
            status="failed",
            summary=f"{job_name} 运行失败",
            error_message=str(exc),
            payload={"error": str(exc)},
        )
        conn.close()
        raise

    _current_run_id.reset(token)
    summary = _build_job_summary(job_name, result)
    conn = get_conn()
    finish_pipeline_run(
        conn,
        run_id,
        status="success",
        result_count=summary["result_count"],
        summary=summary["summary"],
        payload=summary["payload"],
    )
    conn.close()
    return result


def get_current_run_id() -> str | None:
    """返回当前正在执行的 pipeline run_id。"""
    return _current_run_id.get()


def _build_job_summary(job_name: str, result: Any) -> dict[str, Any]:
    if job_name == "scout":
        payload = result if isinstance(result, dict) else {}
        video_count = int(payload.get("video_count", 0))
        comment_count = int(payload.get("comment_count", 0))
        return {
            "result_count": video_count,
            "summary": f"入库 {video_count} 个视频快照，采集 {comment_count} 条评论",
            "payload": payload,
        }

    if job_name == "research" and isinstance(result, dict):
        accepted_count = int(result.get("accepted_count", 0))
        rejected_count = int(result.get("rejected_count", 0))
        failed_count = len(result.get("failed_words", []))
        return {
            "result_count": accepted_count,
            "summary": (
                f"入库 {accepted_count} 个梗，拒绝 {rejected_count} 个候选，"
                f"失败 {failed_count} 个"
            ),
            "payload": result,
        }

    return {
        "result_count": 0,
        "summary": f"{job_name} 已完成",
        "payload": {},
    }
