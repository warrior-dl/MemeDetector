"""M1-a: scout 弹幕抓取 + schema 迁移 + embedding_cache 去重 测试。

覆盖：
- ``archivist.text_norm`` 归一化与 hash 稳定性
- ``archivist.schema`` 新表/索引幂等创建
- ``archivist.scout_store.upsert_scout_raw_danmaku`` 去重 / 兜底 dmid / 更新
- ``archivist.embedding_cache.get_or_compute`` 命中/miss / 批量去重
- ``scout.collector.collect_danmaku`` 对 bilibili-api ``Video.get_danmakus`` 的包装
"""

from __future__ import annotations

import types
from datetime import UTC, datetime

import pytest

from meme_detector.archivist import embedding_cache, schema, scout_store
from meme_detector.archivist.text_norm import content_hash, normalize_text

# ──────────────────────────────────────────────────────────────────────────
# fixtures
# ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def duckdb_conn(tmp_path, monkeypatch):
    db_path = tmp_path / "m1a.db"
    monkeypatch.setattr(
        "meme_detector.archivist.schema.settings.duckdb_path",
        str(db_path),
    )
    schema.reset_connection_cache()
    conn = schema.get_conn()
    try:
        yield conn
    finally:
        conn.close()
        schema.reset_connection_cache()


# ──────────────────────────────────────────────────────────────────────────
# text_norm
# ──────────────────────────────────────────────────────────────────────────


def test_normalize_text_nfkc_and_strip():
    # 全角 A → 半角 A
    assert normalize_text("Ａbc") == "abc"
    # 首尾空白 + casefold
    assert normalize_text("  Bengbuzhu  ") == "bengbuzhu"
    # 中间空格保留
    assert normalize_text("hello  world") == "hello  world"
    assert normalize_text("") == ""


def test_content_hash_stable_for_variants():
    h1 = content_hash("绷不住了")
    h2 = content_hash("绷不住了 ")  # 尾空格
    h3 = content_hash("绷不住了\n")  # 换行
    h4 = content_hash("绷不住了。")  # 多了句号——应该不同，标点是语义
    assert h1 == h2 == h3
    assert h1 != h4
    assert len(h1) == 64


# ──────────────────────────────────────────────────────────────────────────
# schema
# ──────────────────────────────────────────────────────────────────────────


def test_schema_creates_danmaku_and_embedding_cache(duckdb_conn):
    tables = {
        row[0]
        for row in duckdb_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    assert "scout_raw_danmaku" in tables
    assert "embedding_cache" in tables


def test_schema_is_idempotent_across_get_conn_calls(duckdb_conn):
    # 重复调用 get_conn 不应报错/重建
    second = schema.get_conn()
    try:
        second.execute("SELECT 1").fetchone()
    finally:
        second.close()


# ──────────────────────────────────────────────────────────────────────────
# scout_store.upsert_scout_raw_danmaku
# ──────────────────────────────────────────────────────────────────────────


def _make_dm(
    *,
    dmid: str = "12345",
    content: str = "绷不住了",
    dm_time: float = 10.5,
    send_time: float | None = 1_700_000_000.0,
    mode: int = 1,
) -> dict:
    return {
        "dmid": dmid,
        "content": content,
        "content_hash": content_hash(content),
        "dm_time_seconds": dm_time,
        "send_timestamp": send_time,
        "mode": mode,
        "color": "ffffff",
        "font_size": 25,
        "pool": 0,
        "weight": 5,
        "crc32_uid": "abc123",
        "raw_payload": {"id_str": dmid},
    }


def test_upsert_scout_raw_danmaku_inserts_and_dedupes(duckdb_conn):
    bvid = "BV1xx"
    stats = scout_store.upsert_scout_raw_danmaku(
        duckdb_conn,
        bvid=bvid,
        danmakus=[_make_dm(dmid="1"), _make_dm(dmid="2", content="没绷住"), _make_dm(dmid="1")],
    )
    assert stats == {"input_count": 3, "prepared_count": 3, "invalid_count": 0}

    rows = scout_store.list_scout_raw_danmaku(duckdb_conn, bvid=bvid)
    # dmid=1 被覆盖，最终 2 行
    assert len(rows) == 2
    assert {r["dmid"] for r in rows} == {"1", "2"}


def test_upsert_scout_raw_danmaku_skips_empty_content(duckdb_conn):
    stats = scout_store.upsert_scout_raw_danmaku(
        duckdb_conn,
        bvid="BV1xx",
        danmakus=[
            _make_dm(dmid="1", content="  "),  # whitespace
            _make_dm(dmid="2", content=""),  # empty
            _make_dm(dmid="3", content="OK"),
        ],
    )
    assert stats["invalid_count"] == 2
    assert stats["prepared_count"] == 1


def test_upsert_scout_raw_danmaku_generates_synth_dmid_when_missing(duckdb_conn):
    bvid = "BV1xx"
    stats = scout_store.upsert_scout_raw_danmaku(
        duckdb_conn,
        bvid=bvid,
        danmakus=[
            _make_dm(dmid="", content="hello", dm_time=1.0),
            _make_dm(dmid="", content="hello", dm_time=1.0),  # 同 dm_time+同 content → 同 synth dmid → 覆盖
            _make_dm(dmid="", content="world", dm_time=2.0),
        ],
    )
    assert stats["prepared_count"] == 3
    rows = scout_store.list_scout_raw_danmaku(duckdb_conn, bvid=bvid)
    assert len(rows) == 2  # 第 1、2 条合并


def test_upsert_scout_raw_danmaku_update_overwrites(duckdb_conn):
    bvid = "BV1xx"
    scout_store.upsert_scout_raw_danmaku(
        duckdb_conn,
        bvid=bvid,
        danmakus=[_make_dm(dmid="k", content="旧", dm_time=1.0)],
    )
    scout_store.upsert_scout_raw_danmaku(
        duckdb_conn,
        bvid=bvid,
        danmakus=[_make_dm(dmid="k", content="新", dm_time=2.0)],
    )
    rows = scout_store.list_scout_raw_danmaku(duckdb_conn, bvid=bvid)
    assert len(rows) == 1
    assert rows[0]["content"] == "新"
    assert rows[0]["dm_time_seconds"] == 2.0


def test_upsert_scout_raw_danmaku_handles_datetime_send_time(duckdb_conn):
    bvid = "BV1xx"
    ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC).replace(tzinfo=None)
    scout_store.upsert_scout_raw_danmaku(
        duckdb_conn,
        bvid=bvid,
        danmakus=[_make_dm(dmid="dt", send_time=None) | {"send_timestamp": ts}],
    )
    rows = scout_store.list_scout_raw_danmaku(duckdb_conn, bvid=bvid)
    assert rows[0]["send_timestamp"] == ts


# ──────────────────────────────────────────────────────────────────────────
# embedding_cache
# ──────────────────────────────────────────────────────────────────────────


def test_embedding_cache_get_or_compute_dedupes_identical_texts(duckdb_conn):
    calls: list[list[str]] = []

    def embed(texts: list[str]) -> list[list[float]]:
        calls.append(list(texts))
        return [[float(i), float(i)] for i, _ in enumerate(texts)]

    out = embedding_cache.get_or_compute(
        duckdb_conn,
        ["绷不住了", "绷不住了", "绷不住了 ", "没绷住"],
        model="test-m",
        embed_fn=embed,
    )
    assert len(out) == 4
    # 前三条内容归一化后是同一条，共用向量
    assert out[0] == out[1] == out[2]
    assert out[0] != out[3]
    # embed_fn 只被调一次，参数是 miss 的去重文本（2 条）
    assert len(calls) == 1
    assert len(calls[0]) == 2


def test_embedding_cache_read_through_hits_on_second_call(duckdb_conn):
    call_count = {"n": 0}

    def embed(texts: list[str]) -> list[list[float]]:
        call_count["n"] += 1
        return [[1.0]] * len(texts)

    embedding_cache.get_or_compute(duckdb_conn, ["家人们"], model="m", embed_fn=embed)
    embedding_cache.get_or_compute(duckdb_conn, ["家人们", "家人们"], model="m", embed_fn=embed)
    assert call_count["n"] == 1


def test_embedding_cache_miss_count_mismatch_raises(duckdb_conn):
    def broken_embed(texts: list[str]) -> list[list[float]]:
        return [[1.0]]  # 只返回 1 条

    with pytest.raises(ValueError, match="返回 1 条，期望 2 条"):
        embedding_cache.get_or_compute(
            duckdb_conn,
            ["a", "b"],
            model="m",
            embed_fn=broken_embed,
        )


def test_embedding_cache_put_and_get(duckdb_conn):
    assert embedding_cache.get(duckdb_conn, "hello", "m") is None
    embedding_cache.put(duckdb_conn, "hello", "m", [0.1, 0.2, 0.3])
    assert embedding_cache.get(duckdb_conn, "hello", "m") == [0.1, 0.2, 0.3]
    # normalize 后等价的变体也命中
    assert embedding_cache.get(duckdb_conn, "hello ", "m") == [0.1, 0.2, 0.3]


def test_embedding_cache_different_model_does_not_cross_contaminate(duckdb_conn):
    embedding_cache.put(duckdb_conn, "x", "m1", [1.0])
    embedding_cache.put(duckdb_conn, "x", "m2", [2.0])
    assert embedding_cache.get(duckdb_conn, "x", "m1") == [1.0]
    assert embedding_cache.get(duckdb_conn, "x", "m2") == [2.0]


# ──────────────────────────────────────────────────────────────────────────
# scout.collector.collect_danmaku
# ──────────────────────────────────────────────────────────────────────────


def _fake_dm(
    *,
    id_str: str,
    text: str,
    dm_time: float = 0.0,
    send_time: float = 1_700_000_000.0,
    mode: int = 1,
    color: str = "ffffff",
    font_size: int = 25,
    pool: int = 0,
    weight: int = 5,
    crc32_id: str = "cc",
    uid: int = -1,
    is_sub: bool = False,
    attr: int = -1,
    id_: int = 0,
) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        id_str=id_str,
        id_=id_,
        text=text,
        dm_time=dm_time,
        send_time=send_time,
        mode=mode,
        color=color,
        font_size=font_size,
        pool=pool,
        weight=weight,
        crc32_id=crc32_id,
        uid=uid,
        is_sub=is_sub,
        attr=attr,
    )


@pytest.mark.asyncio
async def test_collect_danmaku_uses_credential_when_configured(monkeypatch):
    from meme_detector.scout import collector as scout_collector

    # 清空 env，让 _build_credential 去读 settings
    monkeypatch.setattr(scout_collector.settings, "bilibili_sessdata", "SESS")
    monkeypatch.setattr(scout_collector.settings, "bilibili_bili_jct", "JCT")
    monkeypatch.setattr(scout_collector.settings, "bilibili_buvid3", "B3")

    captured_cred: dict = {}

    class FakeVideo:
        def __init__(self, bvid, credential=None):
            captured_cred["credential"] = credential
            self.bvid = bvid

        async def get_danmakus(self, date=None, from_seg=None, to_seg=None):
            return [_fake_dm(id_str="1", text="绷不住了")]

    monkeypatch.setattr(scout_collector.video, "Video", FakeVideo)

    result = await scout_collector.collect_danmaku("BV1test")
    assert len(result) == 1
    assert result[0]["content"] == "绷不住了"
    assert captured_cred["credential"] is not None
    assert captured_cred["credential"].sessdata == "SESS"


@pytest.mark.asyncio
async def test_collect_danmaku_falls_back_to_anonymous_when_no_env(monkeypatch):
    from meme_detector.scout import collector as scout_collector

    monkeypatch.setattr(scout_collector.settings, "bilibili_sessdata", "")
    monkeypatch.setattr(scout_collector.settings, "bilibili_bili_jct", "")
    monkeypatch.setattr(scout_collector.settings, "bilibili_buvid3", "")

    captured_cred: dict = {}

    class FakeVideo:
        def __init__(self, bvid, credential=None):
            captured_cred["credential"] = credential

        async def get_danmakus(self, date=None, from_seg=None, to_seg=None):
            return []

    monkeypatch.setattr(scout_collector.video, "Video", FakeVideo)

    await scout_collector.collect_danmaku("BV1test")
    assert captured_cred["credential"] is None


@pytest.mark.asyncio
async def test_collect_danmaku_dedupes_duplicate_dmid(monkeypatch):
    from meme_detector.scout import collector as scout_collector

    class FakeVideo:
        def __init__(self, bvid, credential=None):
            pass

        async def get_danmakus(self, date=None, from_seg=None, to_seg=None):
            return [
                _fake_dm(id_str="1", text="绷不住了", dm_time=10.0),
                _fake_dm(id_str="1", text="绷不住了", dm_time=10.0),  # 重复
                _fake_dm(id_str="2", text="绷不住了", dm_time=11.0),  # 同 text 不同 dmid
            ]

    monkeypatch.setattr(scout_collector.video, "Video", FakeVideo)

    out = await scout_collector.collect_danmaku("BV1x")
    assert len(out) == 2
    assert {r["dmid"] for r in out} == {"1", "2"}
    # 同 text 两条 → content_hash 相同（后续 embedding 缓存可复用）
    assert out[0]["content_hash"] == out[1]["content_hash"]


@pytest.mark.asyncio
async def test_collect_danmaku_handles_fetch_exception(monkeypatch):
    from meme_detector.scout import collector as scout_collector

    class FakeVideo:
        def __init__(self, bvid, credential=None):
            pass

        async def get_danmakus(self, date=None, from_seg=None, to_seg=None):
            raise RuntimeError("network down")

    monkeypatch.setattr(scout_collector.video, "Video", FakeVideo)

    out = await scout_collector.collect_danmaku("BV1x")
    assert out == []


@pytest.mark.asyncio
async def test_collect_danmaku_populates_all_fields(monkeypatch):
    from meme_detector.scout import collector as scout_collector

    class FakeVideo:
        def __init__(self, bvid, credential=None):
            pass

        async def get_danmakus(self, date=None, from_seg=None, to_seg=None):
            return [
                _fake_dm(
                    id_str="777",
                    text="闹麻了",
                    dm_time=42.25,
                    send_time=1_700_000_000.5,
                    mode=4,
                    color="ff0000",
                    font_size=18,
                    pool=1,
                    weight=9,
                    crc32_id="xyz789",
                )
            ]

    monkeypatch.setattr(scout_collector.video, "Video", FakeVideo)

    out = await scout_collector.collect_danmaku("BV1x")
    assert len(out) == 1
    dm = out[0]
    assert dm == {
        "dmid": "777",
        "content": "闹麻了",
        "content_hash": content_hash("闹麻了"),
        "dm_time_seconds": 42.25,
        "send_timestamp": 1_700_000_000.5,
        "mode": 4,
        "color": "ff0000",
        "font_size": 18,
        "pool": 1,
        "weight": 9,
        "crc32_uid": "xyz789",
        "raw_payload": {
            "id_str": "777",
            "uid": -1,
            "is_sub": False,
            "attr": -1,
        },
    }


@pytest.mark.asyncio
async def test_collect_danmaku_passes_date_and_seg_args(monkeypatch):
    from datetime import date as date_t

    from meme_detector.scout import collector as scout_collector

    captured: dict = {}

    class FakeVideo:
        def __init__(self, bvid, credential=None):
            pass

        async def get_danmakus(self, date=None, from_seg=None, to_seg=None):
            captured["date"] = date
            captured["from_seg"] = from_seg
            captured["to_seg"] = to_seg
            return []

    monkeypatch.setattr(scout_collector.video, "Video", FakeVideo)

    await scout_collector.collect_danmaku(
        "BV1x",
        date_filter=date_t(2025, 1, 1),
        from_seg=0,
        to_seg=2,
    )
    assert captured == {"date": date_t(2025, 1, 1), "from_seg": 0, "to_seg": 2}
