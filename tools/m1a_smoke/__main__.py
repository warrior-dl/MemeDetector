"""M1-a 端到端实测 CLI。

跑一条命令把 V3 M1-a 的三块能力一次性打通：
1. ``scout.collector.collect_danmaku`` 真抓 B 站弹幕（默认登录态）
2. ``archivist.scout_store.upsert_scout_raw_danmaku`` 写 DuckDB
3. ``archivist.embedding_cache.get_or_compute`` 去重缓存，统计命中率

用法::

    # 基本：抓弹幕 + 落库 + 打印复读 Top20 + stub embedding 命中率
    uv run python -m tools.m1a_smoke BV1xx...

    # 跳过 embedding 环节（只验弹幕采集/落库）
    uv run python -m tools.m1a_smoke BV1xx... --no-embed

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

from meme_detector.archivist import embedding_cache, schema, scout_store
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

    # Top-K 复读
    top = hashes.most_common(args.top_k)
    preview_by_hash: dict[str, str] = {}
    for dm in danmakus:
        h = dm["content_hash"]
        if h not in preview_by_hash:
            preview_by_hash[h] = dm["content"]
        if len(preview_by_hash) >= args.top_k:
            break
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
        print("[embed] ✅ 全命中——说明上一次 run 已经把这些 hash 缓存住了")
    elif miss == unique:
        print("[embed] ⚠️ 第一次 run 对所有 unique 文本都 miss 属正常；再跑一次应该全命中")
    else:
        print(f"[embed] ℹ️ 部分命中：本次 unique={unique}，其中 miss={miss}（其余历史已缓存）")

    return 0


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
    args = parser.parse_args(argv)

    # 确保 data/duckdb 目录存在
    db_path = Path(schema.settings.duckdb_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
