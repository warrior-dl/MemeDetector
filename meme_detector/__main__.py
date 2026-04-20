"""
meme_detector 包主入口。
用法：
  python -m meme_detector serve   # 启动 API + 调度器
  python -m meme_detector scout   # 手动触发单次采集
  python -m meme_detector miner_insights  # 手动触发评论初筛
  python -m meme_detector miner_bundles   # 手动触发证据包生成
  python -m meme_detector miner           # 串行执行两个 Miner 阶段
  python -m meme_detector research # 手动触发 AI 分析
  python -m meme_detector reset    # 清空测试数据
"""

import asyncio
import sys

from meme_detector.logging_utils import get_logger, setup_logging

logger = get_logger(__name__)


def main() -> None:
    setup_logging()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "serve"

    if cmd == "serve":
        _serve()
    elif cmd == "scout":
        asyncio.run(_scout())
    elif cmd == "miner_insights":
        asyncio.run(_miner_insights())
    elif cmd == "miner_bundles":
        asyncio.run(_miner_bundles())
    elif cmd == "miner":
        asyncio.run(_miner())
    elif cmd == "research":
        asyncio.run(_research())
    elif cmd == "reset":
        _reset()
    else:
        logger.error(
            "unknown command",
            extra={"event": "unknown_command", "command": cmd},
        )
        logger.error(
            "usage: python -m meme_detector [serve|scout|miner_insights|miner_bundles|miner|research|reset]",
            extra={"event": "unknown_command_usage", "command": cmd},
        )
        sys.exit(1)


def _serve() -> None:
    import uvicorn

    from meme_detector.api.app import create_app

    app = create_app()
    logger.info("starting api server", extra={"event": "serve_start"})
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info", log_config=None)


async def _scout() -> None:
    from meme_detector.pipeline_service import run_job

    await run_job("scout", trigger_mode="manual")


async def _miner_insights() -> None:
    from meme_detector.pipeline_service import run_job

    await run_job("miner_insights", trigger_mode="manual")


async def _miner_bundles() -> None:
    from meme_detector.pipeline_service import run_job

    await run_job("miner_bundles", trigger_mode="manual")


async def _miner() -> None:
    from meme_detector.miner.scorer import run_miner
    from meme_detector.run_tracker import execute_tracked_job

    await execute_tracked_job("miner", run_miner, trigger_mode="manual")


async def _research() -> None:
    from meme_detector.pipeline_service import run_job

    await run_job("research", trigger_mode="manual")


def _reset() -> None:
    from meme_detector.reset_service import reset_all_data

    result = reset_all_data()
    logger.info(
        "data reset completed",
        extra={
            "event": "data_reset_completed",
            "duckdb_path": result["duckdb_path"],
            "meili_message": result["meili_message"],
        },
    )
    logger.info("data reset summary", extra={"event": "data_reset_summary"})
    logger.info(f"DuckDB: {result['duckdb_path']} ({'deleted' if result['duckdb_deleted'] else 'not found'})")
    logger.info(
        f"Media assets: {result['media_asset_root']} "
        f"({'cleared' if result['media_assets_deleted'] else 'already empty'})"
    )
    logger.info(f"Meilisearch: {'cleared' if result['meili_index_cleared'] else 'warning'} ({result['meili_message']})")


if __name__ == "__main__":
    main()
