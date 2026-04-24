"""BurstEvent 持久化（V3 M1-b）。

单独模块而不放进 ``scout_store`` 是因为：

- ``scout_*`` 表是 "采集层" 原始数据；``burst_event`` 是 "候选发现层" 加工结果。
- M1-c 之后 candidate_discovery 还会写更多表（RepeatCluster / Community /
  MemeLibraryEntry），统一放在 ``burst_store`` 命名有点别扭；这里只承担
  burst 一族的读写，后续可以各自加 ``repeat_store.py`` / ``community_store.py``。
"""

from __future__ import annotations

from datetime import datetime

import duckdb

from meme_detector.candidate_discovery.burst_detector import BurstEvent

__all__ = [
    "delete_burst_events_for_bvid",
    "list_burst_events",
    "list_burst_event_members",
    "upsert_burst_events",
]


def upsert_burst_events(
    conn: duckdb.DuckDBPyConnection,
    *,
    events: list[BurstEvent],
) -> dict:
    """批量写 BurstEvent + 成员关系。

    幂等：event_id 重复时主表 ON CONFLICT UPDATE 覆盖；成员关系先删同 event_id
    再 INSERT，避免上一次检测算出的成员残留（例如调整阈值后簇成员变化）。
    """
    stats = {"events": 0, "members": 0}
    if not events:
        return stats

    now = datetime.now()
    event_rows: list[tuple] = []
    member_rows: list[tuple] = []
    event_ids: list[tuple[str]] = []

    for ev in events:
        event_rows.append(
            (
                ev.event_id,
                ev.bvid,
                float(ev.center_ts),
                float(ev.window_sec),
                ev.signature,
                ev.signature_hash,
                int(ev.danmaku_count),
                int(ev.unique_users),
                ev.detector,
                now,
            )
        )
        event_ids.append((ev.event_id,))
        for dmid in ev.member_dmids:
            member_rows.append((ev.event_id, ev.bvid, dmid))

    conn.executemany(
        """
        INSERT INTO burst_event (
            event_id, bvid, center_ts, window_sec,
            signature, signature_hash,
            danmaku_count, unique_users,
            detector, detected_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (event_id) DO UPDATE
        SET bvid = excluded.bvid,
            center_ts = excluded.center_ts,
            window_sec = excluded.window_sec,
            signature = excluded.signature,
            signature_hash = excluded.signature_hash,
            danmaku_count = excluded.danmaku_count,
            unique_users = excluded.unique_users,
            detector = excluded.detector,
            detected_at = excluded.detected_at
        """,
        event_rows,
    )
    stats["events"] = len(event_rows)

    # 成员关系：先清该 event_id 的旧成员，再批量插入
    if event_ids:
        conn.executemany("DELETE FROM burst_event_member WHERE event_id = ?", event_ids)
    if member_rows:
        conn.executemany(
            "INSERT INTO burst_event_member (event_id, bvid, dmid) VALUES (?, ?, ?)",
            member_rows,
        )
        stats["members"] = len(member_rows)

    return stats


def delete_burst_events_for_bvid(
    conn: duckdb.DuckDBPyConnection,
    *,
    bvid: str,
) -> int:
    """清空某视频下所有 burst_event + 成员关系。

    M1-b 使用场景：重跑同 bvid 的检测、或调整阈值后想完全重建。
    不在 ``upsert_burst_events`` 内部做是因为后者只负责"幂等插入/更新"，
    不负责"清理上一次已过期 event"。
    """
    member_deleted = conn.execute(
        "DELETE FROM burst_event_member WHERE bvid = ? RETURNING event_id",
        [bvid],
    ).fetchall()
    event_deleted = conn.execute(
        "DELETE FROM burst_event WHERE bvid = ? RETURNING event_id",
        [bvid],
    ).fetchall()
    return len(event_deleted) + len(member_deleted)


def list_burst_events(
    conn: duckdb.DuckDBPyConnection,
    *,
    bvid: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    """读取 burst_event 列表；给定 bvid 则只读该视频。

    返回顺序：(bvid, center_ts) 升序。不返回成员 dmid，如需要用
    ``list_burst_event_members``。
    """
    sql_parts = [
        """
        SELECT event_id, bvid, center_ts, window_sec,
               signature, signature_hash,
               danmaku_count, unique_users,
               detector, detected_at
        FROM burst_event
        """
    ]
    params: list = []
    if bvid is not None:
        sql_parts.append("WHERE bvid = ?")
        params.append(bvid)
    sql_parts.append("ORDER BY bvid ASC, center_ts ASC")
    if limit is not None:
        sql_parts.append("LIMIT ?")
        params.append(int(limit))

    rows = conn.execute("\n".join(sql_parts), params).fetchall()
    return [
        {
            "event_id": row[0],
            "bvid": row[1],
            "center_ts": row[2],
            "window_sec": row[3],
            "signature": row[4],
            "signature_hash": row[5],
            "danmaku_count": row[6],
            "unique_users": row[7],
            "detector": row[8],
            "detected_at": row[9],
        }
        for row in rows
    ]


def list_burst_event_members(
    conn: duckdb.DuckDBPyConnection,
    *,
    event_id: str,
) -> list[str]:
    """返回某 burst_event 的成员 dmid 列表。"""
    rows = conn.execute(
        "SELECT dmid FROM burst_event_member WHERE event_id = ? ORDER BY dmid",
        [event_id],
    ).fetchall()
    return [r[0] for r in rows]
