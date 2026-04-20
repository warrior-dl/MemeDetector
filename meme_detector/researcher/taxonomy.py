"""梗分类与生命周期的别名归一化表。

LLM 返回的 ``category`` / ``lifecycle_stage`` 字段经常出现同义但写法不同的词
（例如 "抽象"/"抽象梗"、"emerging"/"新兴期"）。本模块集中维护：
1. 合法的英文/中文枚举集合 (``CATEGORIES`` / ``LIFECYCLE_STAGES``)；
2. 同义词 → 规范值的映射 (``CATEGORY_ALIASES`` / ``LIFECYCLE_ALIASES``)；
3. 归一化入口 (``normalize_category`` / ``normalize_lifecycle_stage``)。

其他模块（decider、tests）应直接从这里 import，避免把硬编码字典散落在业务代码里。
"""

from __future__ import annotations

import re
from collections.abc import Iterable

CATEGORIES: tuple[str, ...] = (
    "抽象",
    "谐音",
    "游戏",
    "影视",
    "音乐",
    "社会现象",
    "二次元",
    "其他",
)

LIFECYCLE_STAGES: tuple[str, ...] = ("emerging", "peak", "declining")

DEFAULT_CATEGORY = "其他"
DEFAULT_LIFECYCLE_STAGE = "emerging"


def _build_identity(values: Iterable[str]) -> dict[str, str]:
    return {value: value for value in values}


# 同义词 → 规范值。所有 key 都会在查表前 ``.strip()``；中文 key 不做小写化，
# 英文 key 在 ``normalize_lifecycle_stage`` 里统一按小写尝试。
CATEGORY_ALIASES: dict[str, str] = {
    **_build_identity(CATEGORIES),
    "抽象梗": "抽象",
    "谐音梗": "谐音",
    "游戏梗": "游戏",
    "影视梗": "影视",
    "音乐梗": "音乐",
    "社会": "社会现象",
    "动漫": "二次元",
    "动画": "二次元",
    "鬼畜": "其他",
    "鬼畜梗": "其他",
}


LIFECYCLE_ALIASES: dict[str, str] = {
    **_build_identity(LIFECYCLE_STAGES),
    "新兴": "emerging",
    "新兴期": "emerging",
    "增长": "emerging",
    "增长期": "emerging",
    "上升": "emerging",
    "上升期": "emerging",
    "高峰": "peak",
    "高峰期": "peak",
    "爆发": "peak",
    "爆发期": "peak",
    "衰退": "declining",
    "衰退期": "declining",
    "下降": "declining",
    "下降期": "declining",
}


_CATEGORY_SPLIT_PATTERN = re.compile(r"[\/|、，,；;]+")


def _split_category_text(value: str) -> list[str]:
    parts = _CATEGORY_SPLIT_PATTERN.split(value)
    return [part.strip() for part in parts if part.strip()]


def normalize_category(value: object) -> list[str]:
    """把 LLM 返回的 category 归一化为 ``CATEGORIES`` 中的值列表。

    - ``list`` / ``tuple`` / ``set`` 会按元素归一化；
    - ``str`` 先按分隔符拆分再归一化；
    - 返回结果会去重并保持顺序；
    - 空结果退化到 ``[DEFAULT_CATEGORY]``，以保证下游 schema 不缺字段。
    """

    raw_items: list[str] = []
    if isinstance(value, list | tuple | set):
        for item in value:
            raw_items.extend(_split_category_text(str(item)))
    elif isinstance(value, str):
        raw_items = _split_category_text(value)

    seen: list[str] = []
    for item in raw_items:
        normalized = CATEGORY_ALIASES.get(item)
        if normalized and normalized not in seen:
            seen.append(normalized)
    return seen or [DEFAULT_CATEGORY]


def normalize_lifecycle_stage(value: object) -> str:
    """把 LLM 返回的 lifecycle_stage 归一化为 ``LIFECYCLE_STAGES`` 中的值。

    优先按原始大小写查表（中文 key 用原串），其次按 ``.lower()`` 查一次（英文 key）。
    如果都不命中，落到 ``DEFAULT_LIFECYCLE_STAGE``。
    """

    key = str(value or "").strip()
    if not key:
        return DEFAULT_LIFECYCLE_STAGE
    return LIFECYCLE_ALIASES.get(key) or LIFECYCLE_ALIASES.get(key.lower(), DEFAULT_LIFECYCLE_STAGE)
