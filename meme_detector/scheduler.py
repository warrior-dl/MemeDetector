"""
APScheduler 定时任务调度器。

任务表：
- 每日 02:05: Scout（采集 + 词频统计）
- 每周一 06:00: Researcher（AI 分析 + 入库）
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
        func=lambda: _run_async(_scout_job()),
        trigger=CronTrigger(hour=2, minute=5, timezone="Asia/Shanghai"),
        id="daily_scout",
        name="每日采集与词频统计",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # 每周一 06:00 运行 Researcher
    _scheduler.add_job(
        func=lambda: _run_async(_research_job()),
        trigger=CronTrigger(day_of_week="mon", hour=6, minute=0, timezone="Asia/Shanghai"),
        id="weekly_research",
        name="每周 AI 分析入库",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    _scheduler.start()
    console.print("[bold green]调度器已启动[/bold green]")
    console.print("  - 每日 02:05 → Scout")
    console.print("  - 每周一 06:00 → Researcher")


async def _scout_job() -> None:
    from meme_detector.scout.scorer import run_scout
    console.print("[bold]定时任务: Scout 开始[/bold]")
    try:
        await run_scout()
    except Exception as e:
        console.print(f"[red]Scout 任务异常: {e}[/red]")


async def _research_job() -> None:
    from meme_detector.researcher.agent import run_research
    console.print("[bold]定时任务: Researcher 开始[/bold]")
    try:
        await run_research()
    except Exception as e:
        console.print(f"[red]Researcher 任务异常: {e}[/red]")
