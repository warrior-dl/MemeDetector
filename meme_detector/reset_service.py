"""
测试环境数据重置服务。
"""

from __future__ import annotations

import shutil
from pathlib import Path

from meme_detector.archivist.meili_store import clear_index
from meme_detector.config import settings


def reset_all_data() -> dict:
    """清空本地 DuckDB、媒体资产和 Meilisearch 索引。"""
    duckdb_path = Path(settings.duckdb_path)
    media_asset_root = Path(settings.media_asset_root)

    duckdb_deleted = False
    if duckdb_path.exists():
        duckdb_path.unlink()
        duckdb_deleted = True
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    media_assets_deleted = False
    if media_asset_root.exists():
        shutil.rmtree(media_asset_root)
        media_assets_deleted = True
    media_asset_root.mkdir(parents=True, exist_ok=True)

    meili_index_cleared, meili_message = clear_index()

    return {
        "duckdb_deleted": duckdb_deleted,
        "duckdb_path": str(duckdb_path),
        "media_assets_deleted": media_assets_deleted,
        "media_asset_root": str(media_asset_root),
        "meili_index_cleared": meili_index_cleared,
        "meili_message": meili_message,
    }
