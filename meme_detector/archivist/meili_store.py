"""
Meilisearch 存储层：管理梗库的写入与检索。
"""

from __future__ import annotations

import meilisearch
from meilisearch.errors import MeilisearchApiError

from meme_detector.config import settings
from meme_detector.researcher.models import MemeRecord


def get_client() -> meilisearch.Client:
    return meilisearch.Client(settings.meili_url, settings.meili_master_key)


def ensure_index() -> None:
    """确保索引存在，并配置好检索属性。"""
    client = get_client()
    index_name = settings.meili_index_name

    # 创建索引（已存在则忽略）
    try:
        client.create_index(index_name, {"primaryKey": "id"})
    except MeilisearchApiError:
        pass

    index = client.index(index_name)

    # 可搜索字段
    index.update_searchable_attributes(
        ["title", "alias", "definition", "origin"]
    )
    # 可过滤字段
    index.update_filterable_attributes(
        ["category", "platform", "lifecycle_stage", "human_verified"]
    )
    # 可排序字段
    index.update_sortable_attributes(
        ["heat_index", "updated_at", "first_detected_at", "confidence_score"]
    )


async def upsert_meme(record: MemeRecord) -> None:
    """写入或更新一条梗记录。"""
    client = get_client()
    index = client.index(settings.meili_index_name)

    doc = record.model_dump()
    # date 对象转字符串
    doc["first_detected_at"] = str(doc["first_detected_at"])
    doc["updated_at"] = str(doc["updated_at"])

    index.add_documents([doc])


async def search_memes(
    query: str,
    filters: str | None = None,
    sort: list[str] | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    """全文检索梗库。"""
    client = get_client()
    index = client.index(settings.meili_index_name)

    params: dict = {"limit": limit, "offset": offset}
    if filters:
        params["filter"] = filters
    if sort:
        params["sort"] = sort

    return index.search(query, params)


async def get_meme(meme_id: str) -> dict | None:
    """按 ID 获取单条梗记录。"""
    client = get_client()
    index = client.index(settings.meili_index_name)
    try:
        return index.get_document(meme_id)
    except MeilisearchApiError:
        return None


async def update_human_verified(meme_id: str, verified: bool) -> bool:
    """更新人工验证状态。"""
    client = get_client()
    index = client.index(settings.meili_index_name)
    try:
        index.update_documents([{"id": meme_id, "human_verified": verified}])
        return True
    except MeilisearchApiError:
        return False
