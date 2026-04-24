"""文本规范化 + content_hash 工具。

Danmaku / 评论 / term 在进入 embedding 缓存之前需要有稳定的 hash 键，
否则 "绷不住了" 与 "绷不住了 " 会被算成两条，缓存命中率会塌。

本模块**不**做语义层面的变体归并（例如 "绷不住了 → 绷"），那是 V2/V3
variant-edge 的职责；这里只做最浅的格式归一：NFKC + strip + casefold。
"""

from __future__ import annotations

import hashlib
import unicodedata


def normalize_text(text: str) -> str:
    """归一化文本，用于 embedding 缓存 / 文本去重。

    - NFKC：全/半角 统一（例如 "Ａ" → "A"、"，" 保持、"﹝ﾎﾟ﹞" 折叠）
    - strip：去掉首尾空白
    - casefold：小写（比 lower 覆盖更广，例如德语 ß → ss）

    不去掉中间空格、不做繁简转换、不去标点——这些涉及语义决策，留给上游。
    """
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKC", text)
    return normalized.strip().casefold()


def content_hash(text: str) -> str:
    """返回 normalize 后文本的 SHA256 hex digest（64 字符）。

    用作 ``scout_raw_danmaku.content_hash`` / ``embedding_cache.content_hash``
    的主键。同一字符串在整条流水线上得到同一 hash，才能实现跨表去重。
    """
    norm = normalize_text(text)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()
