"""阿里云百炼（DashScope）text-embedding 的薄包装。

默认使用 ``text-embedding-v4``，通过 DashScope 的 OpenAI 兼容端点调用
``https://dashscope.aliyuncs.com/compatible-mode/v1``，和用户贴的官方示例
完全对齐。可通过环境变量覆盖：

- ``DASHSCOPE_API_KEY``：阿里云百炼 API Key（兜底读 ``EMBEDDING_API_KEY``）
- ``EMBEDDING_MODEL``：默认 ``text-embedding-v4``
- ``EMBEDDING_BASE_URL``：默认北京 region，新加坡改
  ``https://dashscope-intl.aliyuncs.com/compatible-mode/v1``

使用：

.. code-block:: python

    from pathlib import Path
    texts = ["家人们谁懂啊", "绷不住了", "蚌埠住了"]
    vectors = await embed_texts(texts, cache_path=Path("cache/embed.jsonl"))

``vectors`` 是与 ``texts`` 等长、已做 L2 归一化的 numpy float32 数组列表。
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterable
from pathlib import Path

import numpy as np
from openai import AsyncOpenAI

from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)

_DEFAULT_MODEL = "text-embedding-v4"
_DEFAULT_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
# DashScope text-embedding 单次最多 10 条（v1/v2/v3/v4 限制不同，保守给 10）
_BATCH_SIZE = 10


def _build_client(api_key: str, base_url: str) -> AsyncOpenAI:
    return AsyncOpenAI(api_key=api_key, base_url=base_url)


def _resolve_env() -> tuple[str, str, str]:
    api_key = (os.environ.get("DASHSCOPE_API_KEY") or os.environ.get("EMBEDDING_API_KEY") or "").strip()
    base_url = (os.environ.get("EMBEDDING_BASE_URL") or _DEFAULT_BASE_URL).strip()
    model = (os.environ.get("EMBEDDING_MODEL") or _DEFAULT_MODEL).strip()
    if not api_key:
        raise RuntimeError(
            "缺少 embedding API key。请在 .env 设置 DASHSCOPE_API_KEY=<阿里云百炼 API Key>"
            "（或 EMBEDDING_API_KEY）。"
        )
    return api_key, base_url, model


async def _embed_batch(
    client: AsyncOpenAI,
    model: str,
    batch: list[str],
) -> list[np.ndarray]:
    resp = await client.embeddings.create(model=model, input=batch)
    # response.data 按 input 顺序返回；对向量做 L2 归一化方便后续 cos sim。
    vectors: list[np.ndarray] = []
    for item in resp.data:
        vec = np.asarray(item.embedding, dtype=np.float32)
        norm = float(np.linalg.norm(vec))
        if norm > 0.0:
            vec = vec / norm
        vectors.append(vec)
    return vectors


def _load_cache(path: Path) -> dict[str, list[float]]:
    if not path.exists():
        return {}
    out: dict[str, list[float]] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            text = obj.get("text")
            vec = obj.get("vector")
            if isinstance(text, str) and isinstance(vec, list):
                out[text] = vec
    return out


def _append_cache(path: Path, text: str, vec: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"text": text, "vector": vec.tolist()}, ensure_ascii=False) + "\n")


async def embed_texts(
    texts: Iterable[str],
    *,
    cache_path: Path | None = None,
    concurrency: int = 4,
) -> list[np.ndarray]:
    """对一组文本做 embedding；返回与输入等长、等顺序的向量列表。

    - 文本去重后只调一次 API（相同文本共享向量）
    - ``cache_path`` 存在则做 read-through 缓存，写入 JSONL，每行 ``{text, vector}``
    """
    text_list = [t for t in texts]
    if not text_list:
        return []

    api_key, base_url, model = _resolve_env()

    cache: dict[str, list[float]] = {}
    if cache_path is not None:
        cache = _load_cache(cache_path)

    # 去重
    uniq_order: list[str] = []
    seen: set[str] = set()
    for t in text_list:
        if t not in seen:
            seen.add(t)
            uniq_order.append(t)

    pending = [t for t in uniq_order if t not in cache]

    if pending:
        client = _build_client(api_key, base_url)
        sem = asyncio.Semaphore(concurrency)

        async def _do_batch(batch: list[str]) -> list[tuple[str, np.ndarray]]:
            async with sem:
                vectors = await _embed_batch(client, model, batch)
            return list(zip(batch, vectors, strict=True))

        try:
            batches = [pending[i : i + _BATCH_SIZE] for i in range(0, len(pending), _BATCH_SIZE)]
            results = await asyncio.gather(*[_do_batch(b) for b in batches])
        finally:
            await client.close()

        for pairs in results:
            for text, vec in pairs:
                cache[text] = vec.tolist()
                if cache_path is not None:
                    _append_cache(cache_path, text, vec)

    return [np.asarray(cache[t], dtype=np.float32) for t in text_list]


def cosine_similarity_matrix(vectors: np.ndarray) -> np.ndarray:
    """一次性计算 N × N 余弦相似度矩阵。

    ``vectors`` 形状 (N, D)。对于 N < 几万没问题；再大要走 faiss / lance。
    """
    if vectors.size == 0:
        return np.zeros((0, 0), dtype=np.float32)
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms = np.where(norms == 0.0, 1.0, norms)
    normalized = vectors / norms
    return normalized @ normalized.T
