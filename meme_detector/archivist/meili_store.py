"""
Meilisearch 存储层：管理梗库的写入与检索。
"""

from __future__ import annotations

import asyncio
import hashlib
import re

import meilisearch
from meilisearch.errors import MeilisearchApiError

from meme_detector.config import settings
from meme_detector.researcher.models import MemeRecord

_MEILI_SAFE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


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


def clear_index() -> tuple[bool, str]:
    """清空梗库索引。"""
    client = get_client()
    index_name = settings.meili_index_name
    try:
        client.delete_index(index_name)
        return True, f"deleted index '{index_name}'"
    except MeilisearchApiError as exc:
        message = str(exc)
        lowered = message.lower()
        if "not found" in lowered or "index_not_found" in lowered:
            return True, f"index '{index_name}' already empty"
        return False, message


def make_meme_document_id(value: str) -> str:
    normalized = str(value).strip()
    if normalized and _MEILI_SAFE_ID_PATTERN.fullmatch(normalized):
        return normalized
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()
    return f"meme_{digest}"


def _normalize_document_output(doc: dict | None) -> dict | None:
    if not isinstance(doc, dict):
        return doc
    normalized = dict(doc)
    source_word = str(normalized.get("source_word", "")).strip()
    if source_word:
        normalized["meili_doc_id"] = normalized.get("id")
        normalized["id"] = source_word
    return normalized


def _extract_task_uid(task) -> int | None:
    if isinstance(task, dict):
        uid = task.get("taskUid", task.get("uid"))
        return int(uid) if uid is not None else None
    uid = getattr(task, "task_uid", None)
    if uid is None:
        uid = getattr(task, "uid", None)
    return int(uid) if uid is not None else None


def _wait_for_task_or_raise(client: meilisearch.Client, task) -> None:
    task_uid = _extract_task_uid(task)
    if task_uid is None:
        raise RuntimeError(f"Meilisearch 未返回 task uid: {task!r}")
    result = client.wait_for_task(task_uid, timeout_in_ms=10_000, interval_in_ms=100)
    status = getattr(result, "status", None)
    error = getattr(result, "error", None)
    if status == "failed":
        raise RuntimeError(f"Meilisearch 写入失败: {error}")


def _build_meili_document(record: MemeRecord) -> dict:
    doc = record.model_dump()
    doc["source_word"] = str(doc["id"])
    doc["id"] = make_meme_document_id(str(doc["id"]))
    doc["first_detected_at"] = str(doc["first_detected_at"])
    doc["updated_at"] = str(doc["updated_at"])
    return doc


async def upsert_meme(record: MemeRecord) -> None:
    """写入或更新一条梗记录。"""
    await asyncio.to_thread(_upsert_meme_sync, record)


def _upsert_meme_sync(record: MemeRecord) -> None:
    ensure_index()
    client = get_client()
    index = client.index(settings.meili_index_name)

    doc = _build_meili_document(record)
    task = index.add_documents([doc])
    _wait_for_task_or_raise(client, task)


async def search_memes(
    query: str,
    filters: str | None = None,
    sort: list[str] | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    """全文检索梗库。"""
    return await asyncio.to_thread(
        _search_memes_sync,
        query,
        filters,
        sort,
        limit,
        offset,
    )


def _search_memes_sync(
    query: str,
    filters: str | None = None,
    sort: list[str] | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    client = get_client()
    index = client.index(settings.meili_index_name)

    params: dict = {"limit": limit, "offset": offset}
    if filters:
        params["filter"] = filters
    if sort:
        params["sort"] = sort

    result = index.search(query, params)
    hits = result.get("hits", [])
    if isinstance(hits, list):
        result["hits"] = [
            _normalize_document_output(hit)
            for hit in hits
        ]
    return result


async def get_meme(meme_id: str) -> dict | None:
    """按 ID 获取单条梗记录。"""
    return await asyncio.to_thread(_get_meme_sync, meme_id)


def _get_meme_sync(meme_id: str) -> dict | None:
    client = get_client()
    index = client.index(settings.meili_index_name)
    try:
        record = index.get_document(meme_id)
    except MeilisearchApiError:
        derived_id = make_meme_document_id(meme_id)
        if derived_id == meme_id:
            return None
        try:
            record = index.get_document(derived_id)
        except MeilisearchApiError:
            return None
    if hasattr(record, "model_dump"):
        return _normalize_document_output(record.model_dump())
    if hasattr(record, "__dict__"):
        return _normalize_document_output(dict(record.__dict__))
    if isinstance(record, dict):
        return _normalize_document_output(record)
    return None


async def update_human_verified(meme_id: str, verified: bool) -> bool:
    """更新人工验证状态。"""
    return await asyncio.to_thread(_update_human_verified_sync, meme_id, verified)


def _update_human_verified_sync(meme_id: str, verified: bool) -> bool:
    client = get_client()
    index = client.index(settings.meili_index_name)
    try:
        doc_id = make_meme_document_id(meme_id)
        task = index.update_documents([{"id": doc_id, "human_verified": verified}])
        _wait_for_task_or_raise(client, task)
        return True
    except MeilisearchApiError:
        return False
    except RuntimeError:
        return False
