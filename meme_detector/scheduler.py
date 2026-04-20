"""
APScheduler 定时任务调度器。

任务表：
- 每日 02:05: Scout（采集 + 原始快照入库）
- 每日 03:00: Miner Stage 1（评论线索初筛）
- 每日 03:20: Miner Stage 2（证据包生成）
- 每周一 06:00: Researcher（证据包裁决 + 入库）
"""

from __future__ import annotations

import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)
_scheduler: AsyncIOScheduler | None = None


def _build_scheduler() -> AsyncIOScheduler:
    return AsyncIOScheduler(
        event_loop=asyncio.get_running_loop(),
        timezone="Asia/Shanghai",
    )


def start_scheduler() -> None:
    """启动后台调度器。"""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return

    _scheduler = _build_scheduler()

    # 每日 02:05 运行 Scout
    _scheduler.add_job(
        func=_scheduled_job,
        args=["scout"],
        trigger=CronTrigger(hour=2, minute=5, timezone="Asia/Shanghai"),
        id="daily_scout",
        name="每日采集与原始入库",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # 每日 03:00 运行 Miner Stage 1
    _scheduler.add_job(
        func=_scheduled_job,
        args=["miner_insights"],
        trigger=CronTrigger(hour=3, minute=0, timezone="Asia/Shanghai"),
        id="daily_miner_insights",
        name="每日评论线索初筛",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # 每日 03:20 运行 Miner Stage 2
    _scheduler.add_job(
        func=_scheduled_job,
        args=["miner_bundles"],
        trigger=CronTrigger(hour=3, minute=20, timezone="Asia/Shanghai"),
        id="daily_miner_bundles",
        name="每日证据包生成",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # 每周一 06:00 运行 Researcher
    _scheduler.add_job(
        func=_scheduled_job,
        args=["research"],
        trigger=CronTrigger(day_of_week="mon", hour=6, minute=0, timezone="Asia/Shanghai"),
        id="weekly_research",
        name="每周证据包裁决与入库",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    _scheduler.start()
    logger.info(
        "scheduler started",
        extra={
            "event": "scheduler_started",
            "jobs": ["daily_scout", "daily_miner_insights", "daily_miner_bundles", "weekly_research"],
        },
    )
    logger.info("schedule: 每日 02:05 → Scout")
    logger.info("schedule: 每日 03:00 → Miner Stage 1")
    logger.info("schedule: 每日 03:20 → Miner Stage 2")
    logger.info("schedule: 每周一 06:00 → Researcher")


def shutdown_scheduler() -> None:
    """关闭后台调度器。"""
    global _scheduler
    if _scheduler is None:
        return
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
    _scheduler = None
    logger.info("scheduler stopped", extra={"event": "scheduler_stopped"})


def get_scheduler_jobs() -> list[dict]:
    """返回调度任务概览。"""
    if _scheduler is None:
        return []
    return [
        {
            "id": job.id,
            "name": job.name,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger),
        }
        for job in _scheduler.get_jobs()
    ]


async def _scheduled_job(job_name: str, trigger_mode: str = "scheduled") -> None:
    from meme_detector.pipeline_service import start_background_job

    logger.info(
        "scheduled job triggered",
        extra={"event": "scheduled_job_triggered", "job_name": job_name},
    )
    try:
        result = await start_background_job(job_name, trigger_mode=trigger_mode)
        logger.info(
            "scheduled job dispatched",
            extra={
                "event": "scheduled_job_dispatched",
                "job_name": job_name,
                "started": result.get("started", False),
            },
        )
    except Exception:
        logger.exception(
            "scheduled job dispatch failed",
            extra={"event": "scheduled_job_failed", "job_name": job_name},
        )
