"""M1-b: burst_detector 算法 + burst_store 持久化 测试。

覆盖：
- 单簇满足/不满足 min_count / min_unique_users
- 相同 hash 被 window_sec 切成多个簇
- 跨 bvid 隔离
- unique_users 用 crc32_uid 去重（防一人刷屏）
- event_id 在参数不变情况下幂等
- upsert_burst_events 主表 + 成员表的幂等 / 重跑覆盖
- list_burst_events / list_burst_event_members 读取
- delete_burst_events_for_bvid 清理
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from meme_detector.archivist import burst_store, schema
from meme_detector.archivist.text_norm import content_hash as _hash
from meme_detector.candidate_discovery import detect_burst_events
from meme_detector.candidate_discovery.burst_detector import BurstEvent

# ──────────────────────────────────────────────────────────────────────────
# fixtures / helpers
# ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def duckdb_conn(tmp_path, monkeypatch):
    db_path = tmp_path / "m1b.db"
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


def _mk(
    *,
    bvid: str = "BV1xx",
    dmid: str,
    content: str,
    ts: float,
    uid: str = "user-a",
) -> dict:
    return {
        "bvid": bvid,
        "dmid": dmid,
        "content": content,
        "content_hash": _hash(content),
        "dm_time_seconds": ts,
        "crc32_uid": uid,
    }


# ──────────────────────────────────────────────────────────────────────────
# detect_burst_events 算法层
# ──────────────────────────────────────────────────────────────────────────


def test_detect_empty_input():
    assert detect_burst_events([]) == []


def test_detect_below_min_count_dropped():
    # 5 条"绷不住了"分散，但 min_count=5 默认，边界上是通过的，所以调到 6 验证
    dms = [_mk(dmid=str(i), content="绷不住了", ts=float(i), uid=f"u{i}") for i in range(5)]
    events = detect_burst_events(dms, min_count=6)
    assert events == []


def test_detect_below_min_unique_users_dropped():
    # 5 条相同内容、同一用户 → 应被 unique_users 过滤
    dms = [_mk(dmid=str(i), content="绷不住了", ts=float(i), uid="same-user") for i in range(6)]
    events = detect_burst_events(dms, min_count=5, min_unique_users=3)
    assert events == []


def test_detect_basic_burst_detected():
    # 6 条"绷不住了" / 不同用户 / 相邻间隔 ≤ 3 秒
    dms = [_mk(dmid=str(i), content="绷不住了", ts=float(i), uid=f"u{i}") for i in range(6)]
    events = detect_burst_events(dms, min_count=5, min_unique_users=3)
    assert len(events) == 1
    ev = events[0]
    assert ev.bvid == "BV1xx"
    assert ev.danmaku_count == 6
    assert ev.unique_users == 6
    assert ev.signature == "绷不住了"
    assert ev.signature_hash == _hash("绷不住了")
    assert ev.window_sec == 3.0
    assert ev.detector == "hash_exact_v1"
    # center 应在样本时间范围内
    assert 0.0 <= ev.center_ts <= 5.0
    # 成员列表覆盖全部 dmid
    assert set(ev.member_dmids) == {str(i) for i in range(6)}


def test_detect_window_splits_into_two_clusters():
    # 两段爆发之间有 10 秒空白 → 切成两个 burst
    dms = [
        *[_mk(dmid=f"a{i}", content="爆了", ts=float(i), uid=f"ua{i}") for i in range(6)],
        *[_mk(dmid=f"b{i}", content="爆了", ts=20.0 + float(i), uid=f"ub{i}") for i in range(6)],
    ]
    events = detect_burst_events(dms, window_sec=3.0, min_count=5, min_unique_users=3)
    assert len(events) == 2
    bvids = {ev.bvid for ev in events}
    assert bvids == {"BV1xx"}
    # 两个 event_id 应不同（center_ts 差异足够大）
    assert events[0].event_id != events[1].event_id
    # 中心点分别在两段附近
    centers = sorted(ev.center_ts for ev in events)
    assert centers[0] < 10 < centers[1]


def test_detect_multiple_bvids_isolated():
    a = [_mk(bvid="BV_A", dmid=f"a{i}", content="?", ts=float(i), uid=f"ua{i}") for i in range(6)]
    b = [_mk(bvid="BV_B", dmid=f"b{i}", content="?", ts=float(i), uid=f"ub{i}") for i in range(6)]
    events = detect_burst_events(a + b, min_count=5, min_unique_users=3)
    assert len(events) == 2
    assert {ev.bvid for ev in events} == {"BV_A", "BV_B"}


def test_detect_different_contents_not_merged():
    # 同时间段混合两种不同内容，各自 3 条 → 都不够 min_count=5
    dms = [
        *[_mk(dmid=f"a{i}", content="绷不住了", ts=float(i), uid=f"ua{i}") for i in range(3)],
        *[_mk(dmid=f"b{i}", content="哈哈哈", ts=float(i), uid=f"ub{i}") for i in range(3)],
    ]
    events = detect_burst_events(dms, min_count=5, min_unique_users=3)
    assert events == []


def test_detect_event_id_idempotent():
    dms = [_mk(dmid=str(i), content="666", ts=float(i), uid=f"u{i}") for i in range(6)]
    e1 = detect_burst_events(dms, min_count=5, min_unique_users=3)
    e2 = detect_burst_events(dms, min_count=5, min_unique_users=3)
    assert len(e1) == len(e2) == 1
    assert e1[0].event_id == e2[0].event_id


def test_detect_skips_invalid_rows():
    # 缺 dmid / 缺 content_hash / 缺 dm_time_seconds 的行都跳过
    good = [_mk(dmid=str(i), content="kk", ts=float(i), uid=f"u{i}") for i in range(6)]
    bad = [
        {"bvid": "BV1xx", "content": "kk", "content_hash": _hash("kk"), "dm_time_seconds": 1.0},
        {"bvid": "BV1xx", "dmid": "x1", "content": "kk", "dm_time_seconds": 1.0},
        {"bvid": "BV1xx", "dmid": "x2", "content": "kk", "content_hash": _hash("kk")},
    ]
    events = detect_burst_events(good + bad, min_count=5, min_unique_users=3)
    assert len(events) == 1
    assert events[0].danmaku_count == 6


def test_detect_anon_users_not_collapsed_to_one():
    # crc32_uid 为空的弹幕，不应全部折叠成同一个"匿名用户"
    dms = [_mk(dmid=str(i), content="?", ts=float(i), uid="") for i in range(6)]
    events = detect_burst_events(dms, min_count=5, min_unique_users=3)
    assert len(events) == 1
    # 6 条都匿名时，内部用 dmid 兜底 → unique_users ≥ 3
    assert events[0].unique_users == 6


# ──────────────────────────────────────────────────────────────────────────
# burst_store 持久化层
# ──────────────────────────────────────────────────────────────────────────


def _make_event(event_id: str = "burst::BV1xx::5.0::abcd1234") -> BurstEvent:
    return BurstEvent(
        event_id=event_id,
        bvid="BV1xx",
        center_ts=5.0,
        window_sec=3.0,
        signature="绷不住了",
        signature_hash=_hash("绷不住了"),
        danmaku_count=8,
        unique_users=7,
        member_dmids=["d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8"],
    )


def test_upsert_burst_events_writes_main_and_members(duckdb_conn):
    ev = _make_event()
    stats = burst_store.upsert_burst_events(duckdb_conn, events=[ev])
    assert stats == {"events": 1, "members": 8}

    rows = burst_store.list_burst_events(duckdb_conn, bvid="BV1xx")
    assert len(rows) == 1
    assert rows[0]["event_id"] == ev.event_id
    assert rows[0]["danmaku_count"] == 8
    assert rows[0]["detector"] == "hash_exact_v1"

    members = burst_store.list_burst_event_members(duckdb_conn, event_id=ev.event_id)
    assert members == ["d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8"]


def test_upsert_burst_events_idempotent_and_member_refresh(duckdb_conn):
    # 第一次：老成员
    ev_old = _make_event()
    burst_store.upsert_burst_events(duckdb_conn, events=[ev_old])

    # 第二次：同 event_id 但成员变少（模拟阈值调整后簇缩水）
    ev_new = BurstEvent(
        event_id=ev_old.event_id,
        bvid=ev_old.bvid,
        center_ts=ev_old.center_ts,
        window_sec=ev_old.window_sec,
        signature=ev_old.signature,
        signature_hash=ev_old.signature_hash,
        danmaku_count=5,
        unique_users=5,
        member_dmids=["d1", "d2", "d3", "d4", "d5"],
    )
    burst_store.upsert_burst_events(duckdb_conn, events=[ev_new])

    rows = burst_store.list_burst_events(duckdb_conn, bvid="BV1xx")
    assert len(rows) == 1, "主表应该只有一行"
    assert rows[0]["danmaku_count"] == 5

    members = burst_store.list_burst_event_members(duckdb_conn, event_id=ev_old.event_id)
    assert members == ["d1", "d2", "d3", "d4", "d5"], "成员关系应完全覆盖为新集合"


def test_list_burst_events_scoped_by_bvid(duckdb_conn):
    ev_a = replace(_make_event("burst::BV_A::1.0::aaaa"), bvid="BV_A")
    ev_b = replace(_make_event("burst::BV_B::1.0::bbbb"), bvid="BV_B")
    burst_store.upsert_burst_events(duckdb_conn, events=[ev_a, ev_b])

    all_rows = burst_store.list_burst_events(duckdb_conn)
    assert len(all_rows) == 2

    only_a = burst_store.list_burst_events(duckdb_conn, bvid="BV_A")
    assert len(only_a) == 1
    assert only_a[0]["bvid"] == "BV_A"


def test_delete_burst_events_for_bvid_removes_members_too(duckdb_conn):
    ev = _make_event()
    burst_store.upsert_burst_events(duckdb_conn, events=[ev])

    burst_store.delete_burst_events_for_bvid(duckdb_conn, bvid="BV1xx")

    assert burst_store.list_burst_events(duckdb_conn, bvid="BV1xx") == []
    assert burst_store.list_burst_event_members(duckdb_conn, event_id=ev.event_id) == []


def test_burst_event_schema_idempotent_on_reinit(duckdb_conn):
    # 调用一次已经初始化过的 schema 路径不应抛错
    schema._ensure_schema(duckdb_conn)  # type: ignore[attr-defined]
    schema._ensure_schema(duckdb_conn)  # type: ignore[attr-defined]
    # 验证新表依旧可写
    ev = _make_event()
    burst_store.upsert_burst_events(duckdb_conn, events=[ev])
    assert len(burst_store.list_burst_events(duckdb_conn)) == 1
