"""Layer 1: LLM 抽取评论里的"候选片段"并分类。

对每条评论调用一次 LLM，返回若干 span，每个 span 携带 ``type``：

- ``meme_candidate``：可能的流行梗 / 网络用语
- ``platform_term``：B 站平台术语（大会员 / UP 主 / 弹幕 …）
- ``generic_phrase``：通用口语套话（只能说 / 这个视频 …）
- ``proper_noun``：人名 / 作品名 / 知识点 / 专业术语

后续只有 ``meme_candidate`` 会进入图结构，其它三类**直接滤掉**——这是
v1 方案 TOP 5 全是 FP 的核心修复点。
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from openai import AsyncOpenAI

from meme_detector.llm_factory import (
    build_async_openai_client,
    load_json_response,
    request_json_chat_completion,
    resolve_llm_config,
)
from meme_detector.logging_utils import get_logger

from .load_corpus import Comment

logger = get_logger(__name__)

SpanType = Literal["meme_candidate", "platform_term", "generic_phrase", "proper_noun"]
VALID_TYPES: set[str] = {"meme_candidate", "platform_term", "generic_phrase", "proper_noun"}

_SYSTEM_PROMPT = """你是一个 B 站评论分析助手。你的任务是从评论中提取"值得关注的片段"并分类。

分类规则（严格按此四选一）：
- meme_candidate：网络流行梗 / 圈层用语 / 有模仿性的句式。
  例："家人们谁懂啊"、"蚌埠住了"、"绷不住"、"一眼 AI"、"这不比 XX 强"
- platform_term：B 站 / 短视频平台特有术语。
  例："大会员"、"UP 主"、"弹幕"、"投币"、"一键三连"、"充电"
- generic_phrase：通用口语 / 万能套话 / 无信息量的短语。
  例："只能说"、"这个视频"、"说句实话"、"我觉得"、"还不错"
- proper_noun：人名 / 作品名 / 学科名 / 专业知识 / 地名。
  例："宋代"、"Python"、"胡塞武装"、"鲁迅"、"线性代数"

输出要求：
1. 只返回 JSON 对象 {"spans": [...]}，不要额外文字
2. text 必须是评论的**精确原文摘录**，不要改写
3. 每条评论可以有 0 个或多个 span；无任何值得提取的返回 {"spans": []}
4. confidence：0-1 浮点，表示你对该分类的把握
5. 宁缺勿滥：不确定是不是梗 → 不要标成 meme_candidate"""

_USER_TEMPLATE = """评论：{text}

返回 JSON：{{"spans": [{{"text": "...", "type": "...", "confidence": 0.9}}, ...]}}"""


@dataclass(frozen=True)
class Span:
    text: str
    type: SpanType
    confidence: float


@dataclass
class ExtractionResult:
    comment_id: str
    bvid: str
    mid: str
    text: str
    ctime_iso: str | None
    spans: list[Span]
    error: str | None = None


def _build_messages(text: str) -> list[dict[str, str]]:
    return [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": _USER_TEMPLATE.format(text=text)},
    ]


def _parse_spans(raw: str) -> list[Span]:
    try:
        payload = load_json_response(raw)
    except (json.JSONDecodeError, ValueError):
        return []
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = payload.get("spans", []) or []
    else:
        return []

    spans: list[Span] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        type_ = str(item.get("type") or "").strip()
        if not text or type_ not in VALID_TYPES:
            continue
        try:
            conf = float(item.get("confidence", 0.5))
        except (TypeError, ValueError):
            conf = 0.5
        conf = max(0.0, min(1.0, conf))
        spans.append(Span(text=text, type=type_, confidence=conf))  # type: ignore[arg-type]
    return spans


async def _extract_one(
    client: AsyncOpenAI,
    model: str,
    comment: Comment,
) -> ExtractionResult:
    ctime_iso = comment.ctime.isoformat() if comment.ctime else None
    try:
        raw = await request_json_chat_completion(
            client=client,
            model_name=model,
            messages=_build_messages(comment.text),
        )
        spans = _parse_spans(raw)
        return ExtractionResult(
            comment_id=comment.comment_id,
            bvid=comment.bvid,
            mid=comment.mid,
            text=comment.text,
            ctime_iso=ctime_iso,
            spans=spans,
        )
    except Exception as exc:  # noqa: BLE001  - 单条评论失败不应拖垮整个 pipeline
        logger.warning(
            "extractor llm call failed",
            extra={
                "event": "extractor_llm_error",
                "comment_id": comment.comment_id,
                "bvid": comment.bvid,
                "error": repr(exc),
            },
        )
        return ExtractionResult(
            comment_id=comment.comment_id,
            bvid=comment.bvid,
            mid=comment.mid,
            text=comment.text,
            ctime_iso=ctime_iso,
            spans=[],
            error=repr(exc),
        )


async def extract_spans(
    comments: Iterable[Comment],
    *,
    concurrency: int = 8,
    target: Literal["default", "miner", "research"] = "miner",
) -> list[ExtractionResult]:
    """并发对评论列表跑 LLM 抽取。

    - ``target``：默认用 miner LLM（已单独配置、预算独立），避免误用昂贵 research LLM。
    - ``concurrency``：同时在途的请求数。8 对大多数提供商友好。
    """
    comment_list = list(comments)
    if not comment_list:
        return []

    config = resolve_llm_config(target)
    if not config.api_key or not config.model:
        raise RuntimeError(
            f"LLM target={target} 未配置（缺 api_key / model），请在 .env 设置 "
            f"{target.upper()}_LLM_API_KEY / {target.upper()}_LLM_MODEL 或 LLM_* 回退。"
        )

    client = build_async_openai_client(target)
    sem = asyncio.Semaphore(concurrency)

    async def _run(c: Comment) -> ExtractionResult:
        async with sem:
            return await _extract_one(client, config.model, c)

    try:
        return await asyncio.gather(*[_run(c) for c in comment_list])
    finally:
        await client.close()


# ─────────────────────────── 缓存 I/O（避免重复烧 LLM 成本）────────────────────────────


def _serialize(result: ExtractionResult) -> dict[str, Any]:
    payload = asdict(result)
    payload["spans"] = [asdict(s) for s in result.spans]
    return payload


def _deserialize(payload: dict[str, Any]) -> ExtractionResult:
    spans_raw = payload.get("spans", []) or []
    spans = [
        Span(text=s["text"], type=s["type"], confidence=float(s.get("confidence", 0.5)))
        for s in spans_raw
        if isinstance(s, dict) and s.get("text") and s.get("type") in VALID_TYPES
    ]
    return ExtractionResult(
        comment_id=str(payload["comment_id"]),
        bvid=str(payload["bvid"]),
        mid=str(payload.get("mid", "")),
        text=str(payload.get("text", "")),
        ctime_iso=payload.get("ctime_iso"),
        spans=spans,
        error=payload.get("error"),
    )


def save_cache(results: Iterable[ExtractionResult], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(_serialize(r), ensure_ascii=False) + "\n")


def load_cache(path: Path) -> list[ExtractionResult]:
    if not path.exists():
        return []
    out: list[ExtractionResult] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(_deserialize(json.loads(line)))
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
    return out


def cached_keys(results: Iterable[ExtractionResult]) -> set[tuple[str, str]]:
    return {(r.comment_id, r.bvid) for r in results}
