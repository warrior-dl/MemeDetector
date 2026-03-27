"""
Pipeline 任务运行记录封装。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from meme_detector.archivist.duckdb_store import (
    create_pipeline_run,
    finish_pipeline_run,
    get_conn,
)


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

    try:
        result = await runner()
    except Exception as exc:
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


def _build_job_summary(job_name: str, result: Any) -> dict[str, Any]:
    if job_name == "scout":
        candidates = result if isinstance(result, list) else []
        candidate_items = [
            {
                "phrase": item.get("phrase", ""),
                "confidence": item.get("confidence", 0),
                "explanation": item.get("explanation", ""),
            }
            for item in candidates[:10]
        ]
        return {
            "result_count": len(candidates),
            "summary": f"发现 {len(candidates)} 个候选梗",
            "payload": {
                "candidate_count": len(candidates),
                "candidates": candidate_items,
            },
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
