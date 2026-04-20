"""
统一的 Pipeline 运行编排。

用途：
- 统一 scout / miner / research 的执行入口
- 提供进程内并发保护，避免同一任务重复启动
- 支持 API 在 serve 进程内后台触发任务，规避 DuckDB 跨进程锁冲突
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from threading import Lock
from typing import Any

from meme_detector.logging_utils import get_logger
from meme_detector.run_tracker import execute_tracked_job

logger = get_logger(__name__)

JOB_NAMES = ("scout", "miner_insights", "miner_bundles", "miner", "research")
_state_lock = Lock()
_background_tasks: set[asyncio.Task[Any]] = set()
_job_states: dict[str, dict[str, Any]] = {
    job_name: {
        "running": False,
        "trigger_mode": "",
        "started_at": None,
        "last_started_at": None,
        "last_finished_at": None,
        "last_error": "",
        "phase": "",
        "progress_current": 0,
        "progress_total": 0,
        "progress_unit": "",
        "progress_message": "",
        "updated_at": None,
    }
    for job_name in JOB_NAMES
}


class JobAlreadyRunningError(RuntimeError):
    """任务已在当前进程中运行。"""


def get_job_runtime_state(job_name: str) -> dict[str, Any]:
    with _state_lock:
        state = _job_states.get(job_name)
        if state is None:
            raise KeyError(job_name)
        return dict(state)


def get_all_job_runtime_states() -> dict[str, dict[str, Any]]:
    with _state_lock:
        return {job_name: dict(state) for job_name, state in _job_states.items()}


def _mark_job_started(job_name: str, trigger_mode: str) -> bool:
    with _state_lock:
        state = _job_states[job_name]
        if state["running"]:
            return False
        now = datetime.now()
        state["running"] = True
        state["trigger_mode"] = trigger_mode
        state["started_at"] = now
        state["last_started_at"] = now
        state["last_error"] = ""
        state["phase"] = "starting"
        state["progress_current"] = 0
        state["progress_total"] = 0
        state["progress_unit"] = ""
        state["progress_message"] = "任务启动中"
        state["updated_at"] = now
        logger.info(
            "job runtime state started",
            extra={
                "event": "job_runtime_started",
                "job_name": job_name,
                "trigger_mode": trigger_mode,
            },
        )
        return True


def _mark_job_finished(job_name: str, error: str = "") -> None:
    with _state_lock:
        state = _job_states[job_name]
        state["running"] = False
        state["last_finished_at"] = datetime.now()
        state["last_error"] = error
        state["trigger_mode"] = ""
        state["started_at"] = None
        state["phase"] = ""
        state["progress_current"] = 0
        state["progress_total"] = 0
        state["progress_unit"] = ""
        state["progress_message"] = ""
        state["updated_at"] = state["last_finished_at"]
        logger.info(
            "job runtime state finished",
            extra={
                "event": "job_runtime_finished",
                "job_name": job_name,
                "error": error,
            },
        )


def update_job_runtime_progress(
    job_name: str,
    *,
    phase: str | None = None,
    current: int | None = None,
    total: int | None = None,
    unit: str | None = None,
    message: str | None = None,
) -> None:
    with _state_lock:
        state = _job_states.get(job_name)
        if state is None:
            raise KeyError(job_name)
        if phase is not None:
            state["phase"] = phase
        if current is not None:
            state["progress_current"] = max(0, int(current))
        if total is not None:
            state["progress_total"] = max(0, int(total))
        if unit is not None:
            state["progress_unit"] = unit
        if message is not None:
            state["progress_message"] = message
        state["updated_at"] = datetime.now()


def _resolve_runner(job_name: str):
    if job_name == "scout":
        from meme_detector.scout.scorer import run_scout

        return run_scout
    if job_name == "miner_insights":
        from meme_detector.miner.scorer import run_miner_insights

        return run_miner_insights
    if job_name == "miner_bundles":
        from meme_detector.miner.scorer import run_miner_bundles

        return run_miner_bundles
    if job_name == "miner":
        from meme_detector.miner.scorer import run_miner

        return run_miner
    if job_name == "research":
        from meme_detector.researcher.agent import run_research

        return run_research
    raise KeyError(job_name)


async def run_job(job_name: str, *, trigger_mode: str) -> Any:
    if job_name not in JOB_NAMES:
        raise KeyError(job_name)
    if not _mark_job_started(job_name, trigger_mode):
        logger.warning(
            "job already running",
            extra={"event": "job_already_running", "job_name": job_name, "trigger_mode": trigger_mode},
        )
        raise JobAlreadyRunningError(f"{job_name} is already running")

    try:
        runner = _resolve_runner(job_name)
        result = await execute_tracked_job(job_name, runner, trigger_mode=trigger_mode)
    except Exception as exc:
        _mark_job_finished(job_name, error=str(exc))
        logger.exception(
            "job execution failed",
            extra={"event": "job_execution_failed", "job_name": job_name, "trigger_mode": trigger_mode},
        )
        raise
    else:
        _mark_job_finished(job_name)
        logger.info(
            "job execution succeeded",
            extra={"event": "job_execution_succeeded", "job_name": job_name, "trigger_mode": trigger_mode},
        )
        return result


def run_job_sync(job_name: str, *, trigger_mode: str) -> Any:
    return asyncio.run(run_job(job_name, trigger_mode=trigger_mode))


async def start_background_job(job_name: str, *, trigger_mode: str = "manual") -> dict[str, Any]:
    if job_name not in JOB_NAMES:
        raise KeyError(job_name)

    state = get_job_runtime_state(job_name)
    if state["running"]:
        logger.info(
            "background job skipped because already running",
            extra={"event": "background_job_skipped", "job_name": job_name, "trigger_mode": trigger_mode},
        )
        return {
            "job_name": job_name,
            "started": False,
            "message": f"{job_name} 已在当前进程中运行",
            "runtime_state": state,
        }

    async def _runner() -> None:
        try:
            await run_job(job_name, trigger_mode=trigger_mode)
        except JobAlreadyRunningError:
            pass
        except Exception:
            logger.exception(
                "background job failed",
                extra={"event": "background_job_failed", "job_name": job_name, "trigger_mode": trigger_mode},
            )

    task = asyncio.create_task(_runner(), name=f"pipeline:{job_name}:{trigger_mode}")
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    logger.info(
        "background job started",
        extra={"event": "background_job_started", "job_name": job_name, "trigger_mode": trigger_mode},
    )

    return {
        "job_name": job_name,
        "started": True,
        "message": f"{job_name} 已在后台启动",
        "runtime_state": get_job_runtime_state(job_name),
    }
