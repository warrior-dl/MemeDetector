"""M1-a / M1-b 端到端实测 CLI。

跑一条命令把 V3 M1-a + M1-b 的能力一次性打通：
1. ``scout.collector.collect_danmaku`` 真抓 B 站弹幕（默认登录态）
2. ``archivist.scout_store.upsert_scout_raw_danmaku`` 写 DuckDB
3. ``archivist.embedding_cache.get_or_compute`` 去重缓存，统计命中率
4. ``candidate_discovery.burst_detector.detect_burst_events`` 跑共鸣爆点检测
   （+ ``archivist.burst_store.upsert_burst_events`` 落库），M1-b 默认开启；
   可用 ``--no-burst`` 关闭。

用法::

    # 完整：抓弹幕 + 落库 + stub embedding + burst 检测（M1-a + M1-b）
    uv run python -m tools.m1a_smoke BV1xx...

    # 跳过 embedding 环节（只验弹幕采集/落库 + burst 检测）
    uv run python -m tools.m1a_smoke BV1xx... --no-embed

    # 只抓不落库（纯采集自测；burst 检测会自动降级为"基于内存数据")
    uv run python -m tools.m1a_smoke BV1xx... --no-write-db

    # 关闭 burst 检测
    uv run python -m tools.m1a_smoke BV1xx... --no-burst

    # 调整 burst 阈值
    uv run python -m tools.m1a_smoke BV1xx... --burst-window 3.0 --burst-min-count 5

    # 用火山 Ark 官方 embedding（需 ARK_API_KEY）
    uv run python -m tools.m1a_smoke BV1xx... --real-embed

    # 同样命令再跑一次，观察 DB 里已有 hash 的 miss_count 降到 0
    uv run python -m tools.m1a_smoke BV1xx...

凭据：默认读取 ``BILIBILI_SESSDATA / BILI_JCT / BUVID3``（复用 scout 现有 env 名），
未配置时回落匿名访问——匿名情况下 B 站可能只返回部分分段弹幕。
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import sys
from collections import Counter
from datetime import date
from pathlib import Path

from meme_detector.archivist import burst_store, embedding_cache, schema, scout_store
from meme_detector.candidate_discovery import detect_burst_events
from meme_detector.logging_utils import get_logger
from meme_detector.scout.collector import collect_danmaku

logger = get_logger(__name__)

_STUB_DIM = 16


def _stub_embed(texts: list[str]) -> list[list[float]]:
    """确定性 stub embedding：SHA256 前 16 字节 → 16 维 float。

    不依赖外部 API，适合"只想验证 cache 层是否工作"的快速自测。
    同一文本每次返回同一向量；不同文本返回不同向量。
    """
    out: list[list[float]] = []
    for t in texts:
        h = hashlib.sha256(t.encode("utf-8")).digest()[:_STUB_DIM]
        out.append([float(b) / 255.0 for b in h])
    return out


def _build_real_embed_fn():
    """返回一个同步的 ``embed_fn``，内部走火山 Ark AsyncArk。

    复用 ``tools/embedding_cluster_mvp/pipeline_v2/embedder.py`` 的客户端实现，
    但 **不** 走它那份 JSONL 缓存——我们要演示的就是 DuckDB embedding_cache。
    """
    from tools.embedding_cluster_mvp.pipeline_v2.embedder import embed_texts

    def _sync(texts: list[str]) -> list[list[float]]:
        vectors = asyncio.run(embed_texts(texts, cache_path=None))
        return [v.tolist() for v in vectors]

    return _sync


async def _run(args: argparse.Namespace) -> int:
    # 1. 抓弹幕
    print(f"[scout] collect_danmaku bvid={args.bvid} date={args.date or '-'}", flush=True)
    date_filter: date | None = None
    if args.date:
        date_filter = date.fromisoformat(args.date)

    danmakus = await collect_danmaku(
        args.bvid,
        date_filter=date_filter,
        from_seg=args.from_seg,
        to_seg=args.to_seg,
    )
    if not danmakus:
        print("[scout] 抓到 0 条弹幕（常见原因：BVID 错 / 登录态无效 / 视频无弹幕）", file=sys.stderr)
        return 2

    # 统计原始 / 去重
    total = len(danmakus)
    hashes = Counter(d["content_hash"] for d in danmakus)
    unique = len(hashes)
    dup_ratio = 1.0 - unique / total if total else 0.0
    print(f"[scout] raw_count={total} unique_text={unique} dup_ratio={dup_ratio:.1%}")

    # Top-K 复读：先把所有 hash → sample content 扫出来，再按频次取 Top-K
    preview_by_hash: dict[str, str] = {}
    for dm in danmakus:
        h = dm["content_hash"]
        if h not in preview_by_hash:
            preview_by_hash[h] = dm["content"]
    top = hashes.most_common(args.top_k)
    print(f"[scout] top-{args.top_k} 复读：")
    for h, cnt in top:
        text = preview_by_hash.get(h, "")
        text_short = text[:40] + ("…" if len(text) > 40 else "")
        print(f"  {cnt:>4}×  {text_short}")

    # 2. 落库
    if args.write_db:
        print(f"[db] 写入 scout_raw_danmaku（{schema.settings.duckdb_path}）")
        conn = schema.get_conn()
        try:
            stats = scout_store.upsert_scout_raw_danmaku(conn, bvid=args.bvid, danmakus=danmakus)
        finally:
            conn.close()
        print(f"[db] upsert 完成：{stats}")

        # 再读一次确认已持久化
        conn = schema.get_conn()
        try:
            rows = scout_store.list_scout_raw_danmaku(conn, bvid=args.bvid)
        finally:
            conn.close()
        print(f"[db] scout_raw_danmaku where bvid={args.bvid} 共 {len(rows)} 行")

    # 3. embedding_cache 命中率
    if args.no_embed:
        print("[embed] 跳过 (--no-embed)")
        if not args.no_burst:
            _run_burst_detection(args, danmakus)
        return 0

    if args.real_embed:
        try:
            embed_fn = _build_real_embed_fn()
            model_name = "doubao-embedding-large-text-240515"
            print(f"[embed] 使用 Ark 实 embedding model={model_name}")
        except Exception as exc:
            print(f"[embed] 无法构造 Ark 客户端（{exc}），回落 stub 模式")
            embed_fn = _stub_embed
            model_name = "stub-sha256-16d"
    else:
        embed_fn = _stub_embed
        model_name = "stub-sha256-16d"
        print(f"[embed] 使用 stub embedding model={model_name}（不产生 API 成本）")

    api_call_counter = {"batches": 0, "texts": 0}

    def _wrapped_embed(texts: list[str]) -> list[list[float]]:
        api_call_counter["batches"] += 1
        api_call_counter["texts"] += len(texts)
        return embed_fn(texts)

    texts_in = [d["content"] for d in danmakus]
    conn = schema.get_conn()
    try:
        vectors = embedding_cache.get_or_compute(
            conn,
            texts_in,
            model=model_name,
            embed_fn=_wrapped_embed,
        )
    finally:
        conn.close()

    assert len(vectors) == len(texts_in)

    miss = api_call_counter["texts"]
    hit = total - miss
    hit_rate = hit / total if total else 0.0
    print(
        "[embed] 结果："
        f" input={total} miss={miss} hit={hit} hit_rate={hit_rate:.1%}"
        f" api_batches={api_call_counter['batches']}"
    )
    if miss == 0:
        print("[embed] 全命中——说明上一次 run 已经把这些 hash 缓存住了")
    elif miss == unique:
        print("[embed] 第一次 run 对所有 unique 文本都 miss 属正常；再跑一次应该全命中")
    else:
        print(f"[embed] 部分命中：本次 unique={unique}，其中 miss={miss}（其余历史已缓存）")

    # 4. M1-b burst detector（默认启用；可 --no-burst 跳过）
    if not args.no_burst:
        _run_burst_detection(args, danmakus)

    return 0


def _run_burst_detection(args: argparse.Namespace, danmakus: list[dict]) -> None:
    """M1-b：对本轮拉到的弹幕跑一次 burst detector，打印 Top-K，并落库。

    默认用本轮从 B 站拉下来的弹幕做检测；传 ``--burst-from-db`` 时改为从
    ``scout_raw_danmaku`` 读取该 bvid 的累计数据——这对"匿名只能拿到部分
    分段弹幕"场景很重要：跑多次慢慢把 DB 填满，再按全量检测才能看到
    真正的共鸣爆点。
    """
    if args.burst_from_db:
        conn = schema.get_conn()
        try:
            danmakus = scout_store.list_scout_raw_danmaku(conn, bvid=args.bvid)
        finally:
            conn.close()
        print(f"[burst] source=db bvid={args.bvid} 读到 {len(danmakus)} 条累计弹幕")
    else:
        print(f"[burst] source=fresh 本轮拉到 {len(danmakus)} 条弹幕")

    events = detect_burst_events(
        danmakus,
        window_sec=args.burst_window,
        min_count=args.burst_min_count,
        min_unique_users=args.burst_min_unique_users,
    )
    print(
        f"[burst] 检测完成：events={len(events)}"
        f" window_sec={args.burst_window} min_count={args.burst_min_count}"
        f" min_unique_users={args.burst_min_unique_users}"
    )

    if events:
        ranked = sorted(events, key=lambda e: e.danmaku_count, reverse=True)
        show_k = min(args.burst_top_k, len(ranked))
        print(f"[burst] top-{show_k} 按 danmaku_count 倒序：")
        for ev in ranked[:show_k]:
            sig_short = ev.signature[:40] + ("…" if len(ev.signature) > 40 else "")
            print(f"  [{ev.center_ts:>7.1f}s]  ×{ev.danmaku_count:<3}  uniq_users={ev.unique_users:<3}  {sig_short}")
    else:
        print("[burst] 本视频没有达到阈值的共鸣爆点——要么弹幕分布太散、要么阈值太高，可以 --burst-min-count 调小再跑")

    if args.write_db and events:
        conn = schema.get_conn()
        try:
            stats = burst_store.upsert_burst_events(conn, events=events)
        finally:
            conn.close()
        print(f"[burst] upsert 完成：{stats}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="tools.m1a_smoke",
        description="M1-a 端到端实测：弹幕 → DuckDB → embedding cache",
    )
    parser.add_argument("bvid", help="B 站视频 BVID，例如 BV1xx411c7mD")
    parser.add_argument(
        "--date",
        help="历史弹幕日期 YYYY-MM-DD；不传则抓当前分段",
    )
    parser.add_argument("--from-seg", type=int, default=None, help="起始 6 分钟段索引")
    parser.add_argument("--to-seg", type=int, default=None, help="结束 6 分钟段索引（含）")
    parser.add_argument("--top-k", type=int, default=20, help="打印 Top-K 复读文本（按 content_hash 聚合）")
    parser.add_argument(
        "--no-write-db",
        dest="write_db",
        action="store_false",
        help="只抓不落库（跑完弹幕采集验证即退）",
    )
    parser.set_defaults(write_db=True)
    parser.add_argument(
        "--no-embed",
        action="store_true",
        help="跳过 embedding 环节",
    )
    parser.add_argument(
        "--real-embed",
        action="store_true",
        help="用火山 Ark embedding（需 ARK_API_KEY）；默认走 stub 不产生 API 成本",
    )
    # M1-b burst detector
    parser.add_argument(
        "--no-burst",
        action="store_true",
        help="跳过 M1-b burst detector（只跑 M1-a 部分）",
    )
    parser.add_argument(
        "--burst-window",
        type=float,
        default=3.0,
        help="burst 同 hash 相邻间隔上限（秒），默认 3.0（对齐 V3 Q2）",
    )
    parser.add_argument(
        "--burst-min-count",
        type=int,
        default=5,
        help="单 burst 最小弹幕数，默认 5",
    )
    parser.add_argument(
        "--burst-min-unique-users",
        type=int,
        default=3,
        help="单 burst 最小 unique 用户数（防一人刷屏），默认 3",
    )
    parser.add_argument(
        "--burst-top-k",
        type=int,
        default=10,
        help="打印 Top-K 个 burst（按 danmaku_count 倒序）",
    )
    parser.add_argument(
        "--burst-from-db",
        action="store_true",
        help=(
            "burst 检测从 DB 读该 bvid 的累计弹幕（而非本轮刚抓的快照）。"
            "匿名抓取只能拿到部分分段时，跑多次后用这个 flag 做全量检测。"
        ),
    )
    args = parser.parse_args(argv)

    # 确保 data/duckdb 目录存在
    db_path = Path(schema.settings.duckdb_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
