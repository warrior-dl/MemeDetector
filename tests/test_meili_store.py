from datetime import date

import pytest

from meme_detector.archivist import meili_store
from meme_detector.researcher.models import MemeRecord


class _FakeErrorResponse:
    status_code = 404
    text = '{"message":"not found","code":"document_not_found","type":"invalid_request","link":""}'


class _FakeIndex:
    def __init__(self):
        self.added_documents = []
        self.updated_documents = []
        self.documents = {}

    def add_documents(self, docs):
        self.added_documents.extend(docs)
        for doc in docs:
            self.documents[doc["id"]] = dict(doc)
        return {"taskUid": 7}

    def update_documents(self, docs):
        self.updated_documents.extend(docs)
        for doc in docs:
            self.documents.setdefault(doc["id"], {}).update(doc)
        return {"taskUid": 8}

    def get_document(self, doc_id):
        if doc_id not in self.documents:
            from meilisearch.errors import MeilisearchApiError

            raise MeilisearchApiError("not found", _FakeErrorResponse())
        return dict(self.documents[doc_id])

    def search(self, _query, _params):
        return {"hits": list(self.documents.values()), "estimatedTotalHits": len(self.documents)}


class _FakeClient:
    def __init__(self, index):
        self._index = index
        self.waited_task_ids = []

    def index(self, _name):
        return self._index

    def wait_for_task(self, uid, timeout_in_ms=5000, interval_in_ms=50):
        self.waited_task_ids.append(uid)
        return type("Task", (), {"status": "succeeded", "error": None})()

    def create_index(self, *_args, **_kwargs):
        return None


@pytest.mark.asyncio
async def test_upsert_meme_uses_safe_meili_id_and_preserves_source_word(monkeypatch):
    fake_index = _FakeIndex()
    fake_client = _FakeClient(fake_index)

    monkeypatch.setattr("meme_detector.archivist.meili_store.ensure_index", lambda: None)
    monkeypatch.setattr("meme_detector.archivist.meili_store.get_client", lambda: fake_client)

    record = MemeRecord(
        id="放蚊山",
        title="放蚊山",
        alias=[],
        definition="测试定义",
        origin="测试来源",
        category=["其他"],
        heat_index=70,
        lifecycle_stage="emerging",
        first_detected_at=date(2026, 4, 7),
        source_urls=["https://example.com"],
        confidence_score=0.9,
        updated_at=date(2026, 4, 7),
    )

    await meili_store.upsert_meme(record)

    assert fake_client.waited_task_ids == [7]
    assert fake_index.added_documents
    doc = fake_index.added_documents[0]
    assert doc["id"].startswith("meme_")
    assert doc["source_word"] == "放蚊山"


@pytest.mark.asyncio
async def test_get_meme_and_search_memes_expose_original_source_word_as_id(monkeypatch):
    fake_index = _FakeIndex()
    fake_client = _FakeClient(fake_index)
    doc_id = meili_store.make_meme_document_id("放蚊山")
    fake_index.documents[doc_id] = {
        "id": doc_id,
        "source_word": "放蚊山",
        "title": "放蚊山",
        "category": ["其他"],
    }

    monkeypatch.setattr("meme_detector.archivist.meili_store.get_client", lambda: fake_client)

    record = await meili_store.get_meme("放蚊山")
    search_result = await meili_store.search_memes("", limit=10)

    assert record is not None
    assert record["id"] == "放蚊山"
    assert record["meili_doc_id"] == doc_id
    assert search_result["hits"][0]["id"] == "放蚊山"
    assert search_result["hits"][0]["meili_doc_id"] == doc_id


@pytest.mark.asyncio
async def test_update_human_verified_uses_normalized_meili_id(monkeypatch):
    fake_index = _FakeIndex()
    fake_client = _FakeClient(fake_index)
    monkeypatch.setattr("meme_detector.archivist.meili_store.get_client", lambda: fake_client)

    ok = await meili_store.update_human_verified("放蚊山", True)

    assert ok is True
    assert fake_client.waited_task_ids == [8]
    assert fake_index.updated_documents[0]["id"] == meili_store.make_meme_document_id("放蚊山")
