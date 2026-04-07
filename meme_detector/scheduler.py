"""
APScheduler 定时任务调度器。

任务表：
- 每日 02:05: Scout（采集 + 原始快照入库）
- 每日 03:00: Miner（评论线索挖掘）
- 每周一 06:00: Researcher（候选提取 + AI 分析 + 入库）
"""

from __future__ import annotations

import asyncio

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)
_scheduler = BackgroundScheduler(timezone="Asia/Shanghai")


def _run_async(coro) -> None:
    """在新的事件循环中运行异步任务。"""
    asyncio.run(coro)


def start_scheduler() -> None:
    """启动后台调度器。"""

    # 每日 02:05 运行 Scout
    _scheduler.add_job(
        func=lambda: _run_async(_scout_job(trigger_mode="scheduled")),
        trigger=CronTrigger(hour=2, minute=5, timezone="Asia/Shanghai"),
        id="daily_scout",
        name="每日采集与原始入库",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # 每日 03:00 运行 Miner
    _scheduler.add_job(
        func=lambda: _run_async(_miner_job(trigger_mode="scheduled")),
        trigger=CronTrigger(hour=3, minute=0, timezone="Asia/Shanghai"),
        id="daily_miner",
        name="每日评论线索挖掘",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # 每周一 06:00 运行 Researcher
    _scheduler.add_job(
        func=lambda: _run_async(_research_job(trigger_mode="scheduled")),
        trigger=CronTrigger(day_of_week="mon", hour=6, minute=0, timezone="Asia/Shanghai"),
        id="weekly_research",
        name="每周候选提取与 AI 分析入库",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    _scheduler.start()
    logger.info(
        "scheduler started",
        extra={
            "event": "scheduler_started",
            "jobs": ["daily_scout", "daily_miner", "weekly_research"],
        },
    )
    logger.info("schedule: 每日 02:05 → Scout")
    logger.info("schedule: 每日 03:00 → Miner")
    logger.info("schedule: 每周一 06:00 → Researcher")


def get_scheduler_jobs() -> list[dict]:
    """返回调度任务概览。"""
    return [
        {
            "id": job.id,
            "name": job.name,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger),
        }
        for job in _scheduler.get_jobs()
    ]


async def _scout_job(trigger_mode: str = "scheduled") -> None:
    from meme_detector.pipeline_service import run_job

    logger.info("scheduled scout started", extra={"event": "scheduled_job_started", "job_name": "scout"})
    try:
        await run_job("scout", trigger_mode=trigger_mode)
    except Exception as e:
        logger.exception("scheduled scout failed", extra={"event": "scheduled_job_failed", "job_name": "scout"})


async def _miner_job(trigger_mode: str = "scheduled") -> None:
    from meme_detector.pipeline_service import run_job

    logger.info("scheduled miner started", extra={"event": "scheduled_job_started", "job_name": "miner"})
    try:
        await run_job("miner", trigger_mode=trigger_mode)
    except Exception as e:
        logger.exception("scheduled miner failed", extra={"event": "scheduled_job_failed", "job_name": "miner"})


async def _research_job(trigger_mode: str = "scheduled") -> None:
    from meme_detector.pipeline_service import run_job

    logger.info("scheduled research started", extra={"event": "scheduled_job_started", "job_name": "research"})
    try:
        await run_job("research", trigger_mode=trigger_mode)
    except Exception as e:
        logger.exception(
            "scheduled research failed",
            extra={"event": "scheduled_job_failed", "job_name": "research"},
        )
