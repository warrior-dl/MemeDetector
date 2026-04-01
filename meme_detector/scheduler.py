"""
APScheduler 定时任务调度器。

任务表：
- 每日 02:05: Scout（采集 + 原始快照入库）
- 每周一 06:00: Researcher（候选提取 + AI 分析 + 入库）
"""

from __future__ import annotations

import asyncio

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from rich.console import Console

console = Console()
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
    console.print("[bold green]调度器已启动[/bold green]")
    console.print("  - 每日 02:05 → Scout")
    console.print("  - 每周一 06:00 → Researcher")


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
    from meme_detector.run_tracker import execute_tracked_job
    from meme_detector.scout.scorer import run_scout

    console.print("[bold]定时任务: Scout 开始[/bold]")
    try:
        await execute_tracked_job("scout", run_scout, trigger_mode=trigger_mode)
    except Exception as e:
        console.print(f"[red]Scout 任务异常: {e}[/red]")


async def _research_job(trigger_mode: str = "scheduled") -> None:
    from meme_detector.researcher.agent import run_research
    from meme_detector.run_tracker import execute_tracked_job

    console.print("[bold]定时任务: Researcher 开始[/bold]")
    try:
        await execute_tracked_job("research", run_research, trigger_mode=trigger_mode)
    except Exception as e:
        console.print(f"[red]Researcher 任务异常: {e}[/red]")
