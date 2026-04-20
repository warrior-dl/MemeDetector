"""
Pipeline 任务运行记录封装。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import closing
from contextvars import ContextVar
from typing import Any

from meme_detector.archivist.pipeline_run_store import create_pipeline_run, finish_pipeline_run
from meme_detector.archivist.schema import get_conn
from meme_detector.logging_utils import bind_log_context, get_logger, reset_log_context
from meme_detector.miner.models import MinerBundlesRunResult, MinerInsightsRunResult, MinerRunResult
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
    with closing(get_conn()) as conn:
        run_id = create_pipeline_run(conn, job_name=job_name, trigger_mode=trigger_mode)

    token = _current_run_id.set(run_id)
    log_token = bind_log_context(job_name=job_name, run_id=run_id, trigger_mode=trigger_mode)
    logger.info(
        "pipeline run started",
        extra={"event": "pipeline_run_started", "job_name": job_name, "run_id": run_id},
    )
    try:
        result = await runner()
    except Exception as exc:
        with closing(get_conn()) as conn:
            finish_pipeline_run(
                conn,
                run_id,
                status="failed",
                summary=f"{job_name} 运行失败",
                error_message=str(exc),
                payload={"error": str(exc)},
            )
        logger.exception(
            "pipeline run failed",
            extra={"event": "pipeline_run_failed", "job_name": job_name, "run_id": run_id},
        )
        raise
    else:
        summary = _build_job_summary(job_name, result)
        with closing(get_conn()) as conn:
            finish_pipeline_run(
                conn,
                run_id,
                status="success",
                result_count=summary["result_count"],
                summary=summary["summary"],
                payload=summary["payload"],
            )
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
    payload = _result_to_payload(result)

    if job_name == "scout":
        video_count = _int_field(payload, "video_count")
        comment_count = _int_field(payload, "comment_count")
        return {
            "result_count": video_count,
            "summary": f"入库 {video_count} 个视频快照，采集 {comment_count} 条评论",
            "payload": payload,
        }

    if job_name == "miner_insights":
        insight_count = _int_field(payload, "insight_count")
        high_value_count = _int_field(payload, "high_value_count")
        failed_video_count = _int_field(payload, "failed_video_count")
        failure_suffix = f"，失败 {failed_video_count} 个视频" if failed_video_count > 0 else ""
        return {
            "result_count": insight_count,
            "summary": (
                f"完成 {_int_field(payload, 'video_count')} 个视频的评论初筛，"
                f"写入 {insight_count} 条评论线索，高价值 {high_value_count} 条{failure_suffix}"
            ),
            "payload": payload,
        }

    if job_name == "miner_bundles":
        bundled_count = _int_field(payload, "bundled_count")
        failed_count = _int_field(payload, "failed_insight_count")
        failure_suffix = f"，失败 {failed_count} 条评论" if failed_count > 0 else ""
        return {
            "result_count": bundled_count,
            "summary": (
                f"消费 {_int_field(payload, 'queued_insight_count')} 条高价值评论，"
                f"生成 {bundled_count} 个证据包{failure_suffix}"
            ),
            "payload": payload,
        }

    if job_name == "miner":
        insight_count = _int_field(payload, "insight_count")
        high_value_count = _int_field(payload, "high_value_count")
        bundle_count = _int_field(payload, "bundle_count")
        failed_video_count = _int_field(payload, "failed_video_count")
        failure_suffix = f"，失败 {failed_video_count} 个视频" if failed_video_count > 0 else ""
        return {
            "result_count": insight_count,
            "summary": (
                f"写入 {insight_count} 条评论线索，高价值 {high_value_count} 条，"
                f"证据包 {bundle_count} 个{failure_suffix}"
            ),
            "payload": payload,
        }

    if job_name == "research":
        accepted_count = _int_field(payload, "accepted_count")
        rejected_count = _int_field(payload, "rejected_count")
        failed_count = len(payload.get("failed_bundle_ids", []))
        return {
            "result_count": accepted_count,
            "summary": (
                f"入库 {accepted_count} 个梗，驳回 {rejected_count} 个证据包，"
                f"失败 {failed_count} 个"
            ),
            "payload": payload,
        }

    return {
        "result_count": 0,
        "summary": f"{job_name} 已完成",
        "payload": payload,
    }


def _result_to_payload(result: Any) -> dict[str, Any]:
    if isinstance(
        result,
        (
            ScoutRunResult,
            MinerInsightsRunResult,
            MinerBundlesRunResult,
            MinerRunResult,
            ResearchRunResult,
        ),
    ):
        return result.model_dump(mode="json")
    if isinstance(result, dict):
        return result
    return {}


def _int_field(payload: dict[str, Any], name: str) -> int:
    return int(payload.get(name, 0) or 0)
