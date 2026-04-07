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
from meme_detector.logging_utils import bind_log_context, get_logger, reset_log_context
from meme_detector.miner.models import MinerRunResult
from meme_detector.researcher.models import ResearchRunResult
from meme_detector.scout.models import ScoutRunResult

_current_run_id: ContextVar[str | None] = ContextVar("current_run_id", default=None)
logger = get_logger(__name__)


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
    log_token = bind_log_context(job_name=job_name, run_id=run_id, trigger_mode=trigger_mode)
    logger.info(
        "pipeline run started",
        extra={"event": "pipeline_run_started", "job_name": job_name, "run_id": run_id},
    )
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
        logger.exception(
            "pipeline run failed",
            extra={"event": "pipeline_run_failed", "job_name": job_name, "run_id": run_id},
        )
        raise
    else:
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
        logger.info(
            "pipeline run finished",
            extra={
                "event": "pipeline_run_finished",
                "job_name": job_name,
                "run_id": run_id,
                "result_count": summary["result_count"],
                "summary": summary["summary"],
            },
        )
        return result
    finally:
        _current_run_id.reset(token)
        reset_log_context(log_token)


def get_current_run_id() -> str | None:
    """返回当前正在执行的 pipeline run_id。"""
    return _current_run_id.get()


def _build_job_summary(job_name: str, result: Any) -> dict[str, Any]:
    if job_name == "scout" and isinstance(result, ScoutRunResult):
        payload = result.model_dump(mode="json")
        video_count = int(result.video_count)
        comment_count = int(result.comment_count)
        return {
            "result_count": video_count,
            "summary": f"入库 {video_count} 个视频快照，采集 {comment_count} 条评论",
            "payload": payload,
        }

    if job_name == "scout":
        payload = result if isinstance(result, dict) else {}
        video_count = int(payload.get("video_count", 0))
        comment_count = int(payload.get("comment_count", 0))
        return {
            "result_count": video_count,
            "summary": f"入库 {video_count} 个视频快照，采集 {comment_count} 条评论",
            "payload": payload,
        }

    if job_name == "miner" and isinstance(result, MinerRunResult):
        payload = result.model_dump(mode="json")
        insight_count = int(result.insight_count)
        high_value_count = int(result.high_value_count)
        return {
            "result_count": insight_count,
            "summary": f"写入 {insight_count} 条评论线索，高价值 {high_value_count} 条",
            "payload": payload,
        }

    if job_name == "miner":
        payload = result if isinstance(result, dict) else {}
        insight_count = int(payload.get("insight_count", 0))
        high_value_count = int(payload.get("high_value_count", 0))
        return {
            "result_count": insight_count,
            "summary": f"写入 {insight_count} 条评论线索，高价值 {high_value_count} 条",
            "payload": payload,
        }

    if job_name == "research" and isinstance(result, ResearchRunResult):
        payload = result.model_dump(mode="json")
        accepted_count = int(result.accepted_count)
        rejected_count = int(result.rejected_count)
        failed_count = len(result.failed_words)
        return {
            "result_count": accepted_count,
            "summary": (
                f"入库 {accepted_count} 个梗，拒绝 {rejected_count} 个候选，"
                f"失败 {failed_count} 个"
            ),
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
