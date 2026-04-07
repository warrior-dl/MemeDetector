"""
meme_detector 包主入口。
用法：
  python -m meme_detector serve   # 启动 API + 调度器
  python -m meme_detector scout   # 手动触发单次采集
  python -m meme_detector miner    # 手动触发评论挖掘
  python -m meme_detector research # 手动触发 AI 分析
"""

import asyncio
import sys

from rich.console import Console

console = Console()


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "serve"

    if cmd == "serve":
        _serve()
    elif cmd == "scout":
        asyncio.run(_scout())
    elif cmd == "miner":
        asyncio.run(_miner())
    elif cmd == "research":
        asyncio.run(_research())
    else:
        console.print(f"[red]未知命令: {cmd}[/red]")
        console.print("用法: python -m meme_detector [serve|scout|miner|research]")
        sys.exit(1)


def _serve() -> None:
    import uvicorn

    from meme_detector.api.app import create_app
    from meme_detector.scheduler import start_scheduler

    app = create_app()
    start_scheduler()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


async def _scout() -> None:
    from meme_detector.run_tracker import execute_tracked_job
    from meme_detector.scout.scorer import run_scout

    await execute_tracked_job("scout", run_scout, trigger_mode="manual")


async def _miner() -> None:
    from meme_detector.miner.scorer import run_miner
    from meme_detector.run_tracker import execute_tracked_job

    await execute_tracked_job("miner", run_miner, trigger_mode="manual")


async def _research() -> None:
    from meme_detector.researcher.agent import run_research
    from meme_detector.run_tracker import execute_tracked_job

    await execute_tracked_job("research", run_research, trigger_mode="manual")


if __name__ == "__main__":
    main()
