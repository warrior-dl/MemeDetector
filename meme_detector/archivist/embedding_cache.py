"""基于 DuckDB 的文本级 embedding 缓存。

设计目标（V3 M1-a）：
- 弹幕场景下大量完全一致的文本（"绷不住了" 一场几百条刷屏），按 content_hash
  去重后再调 embedding API，命中率预期 ≥70%，直接砍成本。
- 跨输入源共享：弹幕 / 评论 / term 用同一张 ``embedding_cache`` 表；只要规范化
  后的文本相同且 model 相同，就复用向量。
- 与 V2 离线沙盒 JSONL 缓存解耦：V2 的 ``pipeline_v2/embedder.py`` 仍可用它自己
  的 JSONL 缓存（不改），这里是给 V3 生产路径的正式缓存。

公开 API：
- ``get_or_compute(texts, model, embed_fn)``：read-through；miss 的批量调 ``embed_fn``
- ``get(text, model)`` / ``put(text, model, vector)``：单条读写（测试 / 调试用）

``embed_fn`` 的契约：``Callable[[list[str]], list[list[float]]]``，
纯粹的"一组文本 → 一组等长向量"函数；调用方决定是不是 async/分批/限流。
由于 ``embed_fn`` 在 cache miss 时才被调，且本模块不关心并发，所以保持同步接口。
需要 async 的调用方可以在外层用 ``asyncio.to_thread`` 包装 ``get_or_compute``。
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime

import duckdb

from meme_detector.archivist.text_norm import content_hash, normalize_text
from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)

EmbedFn = Callable[[list[str]], list[list[float]]]


def get(
    conn: duckdb.DuckDBPyConnection,
    text: str,
    model: str,
) -> list[float] | None:
    """单条读取；miss 时返回 None。"""
    if not text or not model:
        return None
    h = content_hash(text)
    row = conn.execute(
        "SELECT vector_json FROM embedding_cache WHERE content_hash = ? AND model = ?",
        [h, model],
    ).fetchone()
    if not row:
        return None
    try:
        vec = json.loads(row[0])
    except json.JSONDecodeError:
        logger.warning(
            "embedding_cache 损坏，按 miss 处理",
            extra={"event": "embedding_cache_corrupt", "content_hash": h, "model": model},
        )
        return None
    if not isinstance(vec, list):
        return None
    return vec


def put(
    conn: duckdb.DuckDBPyConnection,
    text: str,
    model: str,
    vector: list[float],
) -> None:
    """单条写入；如已存在则覆盖（应避免同 model 下 dim 变化的情况）。"""
    if not text or not model or not vector:
        return
    h = content_hash(text)
    preview = normalize_text(text)[:64]
    conn.execute(
        """
        INSERT INTO embedding_cache (content_hash, model, dim, vector_json, text_preview, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (content_hash, model) DO UPDATE
        SET dim = excluded.dim,
            vector_json = excluded.vector_json,
            text_preview = excluded.text_preview,
            created_at = excluded.created_at
        """,
        [h, model, len(vector), json.dumps(vector), preview, datetime.now()],
    )


def get_or_compute(
    conn: duckdb.DuckDBPyConnection,
    texts: list[str],
    *,
    model: str,
    embed_fn: EmbedFn,
) -> list[list[float]]:
    """Read-through 缓存：返回与 ``texts`` 等长、同顺序的向量。

    规范化 + 去重：重复文本在一次调用里只查 / 算一次；最终输出按输入顺序贴回。

    不会抛 KeyError：若 ``embed_fn`` 返回条数与 miss 数不符，会 raise ValueError
    以便上层快速失败（embedding 失败的结果如果被静默填 0，下游聚类会污染）。
    """
    if not texts:
        return []
    if not model:
        raise ValueError("embedding_cache.get_or_compute 需要 model 名字")

    # 保序去重：同一批里 "绷不住了" 出现 300 次只查/算一次
    uniq_order: list[str] = []
    seen_hashes: set[str] = set()
    hash_by_text: dict[str, str] = {}
    for t in texts:
        h = content_hash(t)
        hash_by_text[t] = h
        if h not in seen_hashes:
            seen_hashes.add(h)
            uniq_order.append(t)

    # 批量查缓存
    hit_by_hash: dict[str, list[float]] = {}
    if uniq_order:
        placeholders = ",".join(["?"] * len(uniq_order))
        hashes = [hash_by_text[t] for t in uniq_order]
        rows = conn.execute(
            f"SELECT content_hash, vector_json FROM embedding_cache "
            f"WHERE model = ? AND content_hash IN ({placeholders})",  # noqa: S608 (placeholders 固定)
            [model, *hashes],
        ).fetchall()
        for row in rows:
            try:
                vec = json.loads(row[1])
            except json.JSONDecodeError:
                logger.warning(
                    "embedding_cache 损坏跳过",
                    extra={"event": "embedding_cache_corrupt", "content_hash": row[0], "model": model},
                )
                continue
            if isinstance(vec, list):
                hit_by_hash[row[0]] = vec

    miss_texts: list[str] = [t for t in uniq_order if hash_by_text[t] not in hit_by_hash]

    logger.info(
        "embedding_cache 命中统计",
        extra={
            "event": "embedding_cache_stats",
            "model": model,
            "input_count": len(texts),
            "unique_count": len(uniq_order),
            "hit_count": len(hit_by_hash),
            "miss_count": len(miss_texts),
        },
    )

    if miss_texts:
        computed = embed_fn(miss_texts)
        if len(computed) != len(miss_texts):
            raise ValueError(f"embed_fn 返回 {len(computed)} 条，期望 {len(miss_texts)} 条；拒绝部分写入以免污染缓存")
        now = datetime.now()
        for t, vec in zip(miss_texts, computed, strict=True):
            h = hash_by_text[t]
            preview = normalize_text(t)[:64]
            conn.execute(
                """
                INSERT INTO embedding_cache (content_hash, model, dim, vector_json, text_preview, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (content_hash, model) DO UPDATE
                SET dim = excluded.dim,
                    vector_json = excluded.vector_json,
                    text_preview = excluded.text_preview,
                    created_at = excluded.created_at
                """,
                [h, model, len(vec), json.dumps(vec), preview, now],
            )
            hit_by_hash[h] = vec

    # 按原始输入顺序贴回
    return [hit_by_hash[hash_by_text[t]] for t in texts]
