"""弹幕共鸣爆点检测（V3 M1-b）。

对应 ``docs/V3梗发现方案.md`` 第 4.2.1 节。给定一段 ``scout_raw_danmaku``
扫描记录，按以下规则输出 ``BurstEvent``::

    1. 按 bvid 分组，按 dm_time_seconds 升序排列。
    2. 同 ``content_hash`` 的弹幕视为"相似"（M1-b v1：精确 hash 匹配；
       M1-b v2 预留 ``similarity_fn`` 钩子，将来可接 embedding cosine）。
    3. 每个 hash 的出现点分裂成若干"时间簇"——相邻两点 ``gap ≤ window_sec``
       合入同簇，否则开新簇。这里的 ``window_sec`` 解读为"相邻间隔上限"，
       即设计文档所说 ``±W`` 窗口的等价形式：一个 burst 可以跨越多个
       ``window_sec``，只要中间没有超过 ``window_sec`` 的空白。
    4. 簇规模 ≥ ``min_count`` 且 unique user 数 ≥ ``min_unique_users``
       才成为一个 BurstEvent，其余丢弃。
    5. ``signature`` 取簇内任意成员的原文（同 hash 下等价）；``event_id``
       由 ``(bvid, center_ts, signature_hash[:8])`` 生成，幂等。

本模块**纯算法**：不连 DB、不做 I/O。调用方负责：
- 从 ``scout_raw_danmaku`` 读取数据传入 ``detect_burst_events``
- 把返回结果通过 ``archivist.scout_store.upsert_burst_events`` 写库
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

__all__ = ["BurstEvent", "detect_burst_events"]


@dataclass(slots=True)
class BurstEvent:
    """一次弹幕共鸣爆点。

    序列化用 ``asdict`` 或手动字段拷贝；``member_dmids`` 可能很长，
    写库时走独立的 ``burst_event_member`` 关系表、不进主表字段。
    """

    event_id: str
    bvid: str
    center_ts: float
    window_sec: float
    signature: str
    signature_hash: str
    danmaku_count: int
    unique_users: int
    detector: str = "hash_exact_v1"
    member_dmids: list[str] = field(default_factory=list)


def _center_ts(cluster: list[dict[str, Any]]) -> float:
    """时间簇的中心点：算术平均，足够当 center_ts 用。"""
    return sum(float(d["dm_time_seconds"]) for d in cluster) / len(cluster)


def _build_event_id(bvid: str, center_ts: float, signature_hash: str) -> str:
    """生成 burst_event.event_id。

    保证同一 ``(bvid, 整数秒 center, signature_hash 前 8 位)`` 重跑幂等。
    小数部分截断到 0.1 秒，防止浮点抖动导致 id 漂移。
    """
    return f"burst::{bvid}::{center_ts:.1f}::{signature_hash[:8]}"


def detect_burst_events(
    danmaku: Iterable[dict[str, Any]],
    *,
    window_sec: float = 3.0,
    min_count: int = 5,
    min_unique_users: int = 3,
) -> list[BurstEvent]:
    """检测弹幕共鸣爆点。

    Args:
        danmaku: ``scout_raw_danmaku`` 行的字典列表（至少含 ``bvid / dmid /
            content / content_hash / dm_time_seconds / crc32_uid``）。
            多个 bvid 的数据可以混在一起传入，内部会分组。
        window_sec: 相邻同 hash 弹幕最大允许间隔（秒）；大于此值视为不同簇。
            默认 3.0，对齐 V3 方案 Q2。
        min_count: 单簇内弹幕数下限。默认 5。
        min_unique_users: 单簇内去重用户数下限。默认 3，防一人连刷。

    Returns:
        ``BurstEvent`` 列表，已按 (bvid, center_ts) 升序。
    """
    # 分组到 (bvid, content_hash)，因为 M1-b v1 的相似度就是 hash 精确匹配。
    # 同时保留原始 dict 以便后续取 content / crc32_uid / dmid。
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for d in danmaku:
        bvid = d.get("bvid")
        content_hash = d.get("content_hash")
        dmid = d.get("dmid")
        if not bvid or not content_hash or not dmid:
            # 缺关键字段直接跳——上游 scout_store 已经兜底过一次
            continue
        if "dm_time_seconds" not in d or d["dm_time_seconds"] is None:
            continue
        grouped[(bvid, content_hash)].append(d)

    events: list[BurstEvent] = []

    for (bvid, content_hash), items in grouped.items():
        if len(items) < min_count:
            # 整个视频这个 hash 都没刷到 min_count 次，不可能成 burst
            continue

        items.sort(key=lambda x: float(x["dm_time_seconds"]))

        # 按相邻间隔切分时间簇
        current: list[dict[str, Any]] = [items[0]]
        clusters: list[list[dict[str, Any]]] = [current]
        for prev, curr in zip(items, items[1:], strict=False):
            gap = float(curr["dm_time_seconds"]) - float(prev["dm_time_seconds"])
            if gap <= window_sec:
                current.append(curr)
            else:
                current = [curr]
                clusters.append(current)

        for cluster in clusters:
            if len(cluster) < min_count:
                continue
            # unique_users：crc32_uid 可能缺失（匿名弹幕）；对缺失项用 dmid 兜底
            # （至少按弹幕维度去重），但这会高估独立用户数——通过同时要求
            # min_count 来抵消。
            users: set[str] = set()
            for c in cluster:
                uid = c.get("crc32_uid") or ""
                users.add(uid if uid else f"__anon__::{c['dmid']}")
            if len(users) < min_unique_users:
                continue

            center = _center_ts(cluster)
            signature = cluster[0]["content"]
            event_id = _build_event_id(bvid, center, content_hash)
            events.append(
                BurstEvent(
                    event_id=event_id,
                    bvid=bvid,
                    center_ts=center,
                    window_sec=window_sec,
                    signature=signature,
                    signature_hash=content_hash,
                    danmaku_count=len(cluster),
                    unique_users=len(users),
                    member_dmids=[c["dmid"] for c in cluster],
                )
            )

    events.sort(key=lambda e: (e.bvid, e.center_ts))
    return events
