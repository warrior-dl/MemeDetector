"""
Research Step 1: 候选词快速筛选。
"""

from __future__ import annotations

import json
from typing import Any

from tenacity import retry, stop_after_attempt, wait_exponential

from meme_detector.config import settings
from meme_detector.llm_factory import (
    build_async_openai_client,
    load_json_response,
    request_json_chat_completion,
    resolve_llm_config,
)
from meme_detector.logging_utils import get_logger
from meme_detector.researcher.models import QuickScreenResult

logger = get_logger(__name__)

_SCREEN_SYSTEM = """\
你是一位专业的互联网亚文化研究员，专注于识别中文网络梗和亚文化词汇。

判断标准（符合任意一条即为梗）：
1. 谐音/谐意：利用汉字谐音创造的新含义（如"依托答辩"=依托大便）
2. 二次元出典：来源于动漫、游戏、小说的台词或梗
3. 社会事件：因某个热点事件催生的特定用语
4. 抽象文化：B站鬼畜、抽象文化圈的专属词汇
5. 圈内黑话：某个内容圈子（游戏圈、美食圈等）专有词汇

不是梗的情况：
- 普通流行语（"内卷"、"躺平"已是通用词）
- 活动关键词（活动名称、UP主名字）
- 普通口语（"真的"、"确实"、"好吧"）

few-shot 示例：
- "依托答辩" → is_meme=true，谐音梗
- "遥遥领先" → is_meme=true，因某事件语境下二次流行
- "内卷" → is_meme=false，已成主流词汇
- "哈哈哈" → is_meme=false，普通口语
- "这波" → is_meme=false，普通网络用语
"""


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def batch_screen(
    candidates: list[dict],
) -> list[QuickScreenResult]:
    """批量快速筛选，每批最多 AI_BATCH_SIZE 个词。"""
    client = build_async_openai_client(
        "research",
        timeout=settings.research_screen_timeout_seconds,
        max_retries=settings.research_screen_max_retries,
    )
    llm_config = resolve_llm_config("research")

    word_list = []
    for candidate in candidates:
        sample = candidate.get("sample_comments", "").strip()
        explanation = candidate.get("explanation", "").strip()
        context = explanation if explanation else (sample[:150] if sample else "无")
        word_list.append(f'- 词: "{candidate["word"]}" | 上下文: {context}')

    user_msg = (
        "请对以下词汇逐一判断是否为网络梗，返回 JSON 数组，"
        "每项格式：{word, is_meme, confidence, candidate_category, reason}\n\n"
        + "\n".join(word_list)
    )
    logger.info(
        "research screening request started",
        extra={
            "event": "research_screen_request_started",
            "candidate_count": len(candidates),
            "model_name": llm_config.model,
            "provider": llm_config.provider,
        },
    )

    raw = await request_json_chat_completion(
        client=client,
        model_name=llm_config.model,
        messages=[
            {"role": "system", "content": _SCREEN_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
    )
    results = extract_screen_results(raw, candidates)
    logger.info(
        "research screening request completed",
        extra={
            "event": "research_screen_request_completed",
            "candidate_count": len(candidates),
            "result_count": len(results),
        },
    )
    if len(results) < len(candidates):
        logger.warning(
            "research screening partially parsed",
            extra={
                "event": "research_screen_partial_parse",
                "candidate_count": len(candidates),
                "result_count": len(results),
                "raw_summary": summarize_screen_raw(raw),
            },
        )
    return results


def partition_screen_results(
    candidates: list[dict],
    screen_results: list[QuickScreenResult],
) -> tuple[list[dict], list[str], list[str]]:
    """拆分通过、明确拒绝、待重试三类候选，避免结果缺失时误拒。"""
    screen_map = {result.word: result for result in screen_results}

    to_deep: list[dict] = []
    rejected: list[str] = []
    pending_retry: list[str] = []

    for candidate in candidates:
        word = candidate["word"]
        screen = screen_map.get(word)
        if screen is None:
            pending_retry.append(word)
            continue
        if screen.is_meme and screen.confidence >= settings.ai_confidence_threshold:
            to_deep.append(candidate)
            continue
        rejected.append(word)

    return to_deep, rejected, pending_retry


def extract_screen_results(raw: str, candidates: list[dict]) -> list[QuickScreenResult]:
    try:
        data = load_json_response(raw)
    except Exception as exc:
        logger.error(
            "research screening returned invalid json",
            extra={
                "event": "research_screen_invalid_json",
                "candidate_count": len(candidates),
                "raw_summary": summarize_screen_raw(raw),
            },
        )
        raise RuntimeError(
            f"Step 1 快筛返回非 JSON，有效响应无法解析：{summarize_screen_raw(raw)}"
        ) from exc

    items = _extract_screen_items(data)
    results: list[QuickScreenResult] = []
    seen_words: set[str] = set()
    for item in items:
        parsed = _normalize_screen_item(item, candidates)
        if parsed is None or parsed.word in seen_words:
            continue
        seen_words.add(parsed.word)
        results.append(parsed)

    if candidates and not results:
        logger.error(
            "research screening produced no valid results",
            extra={
                "event": "research_screen_no_valid_results",
                "candidate_count": len(candidates),
                "raw_summary": summarize_screen_raw(raw),
            },
        )
        raise RuntimeError(
            f"Step 1 快筛未解析到任何有效结果：{summarize_screen_raw(raw)}"
        )
    return results


def _extract_screen_items(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []

    for key in ("results", "items", "data", "candidates", "outputs", "response"):
        value = data.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _extract_screen_items(value)
            if nested:
                return nested

    if _looks_like_single_screen_item(data):
        return [data]

    if all(isinstance(value, dict) for value in data.values()):
        items: list[dict] = []
        for key, value in data.items():
            if not isinstance(value, dict):
                continue
            item = dict(value)
            if not any(field in item for field in ("word", "term", "phrase", "name")):
                item["word"] = str(key)
            items.append(item)
        return items

    return []


def _looks_like_single_screen_item(data: dict) -> bool:
    return any(
        key in data
        for key in (
            "word",
            "term",
            "phrase",
            "name",
            "index",
            "is_meme",
            "isMeme",
            "meme",
        )
    )


def _normalize_screen_item(item: Any, candidates: list[dict]) -> QuickScreenResult | None:
    if not isinstance(item, dict):
        return None

    word = _first_non_empty_string(
        item.get("word"),
        item.get("term"),
        item.get("phrase"),
        item.get("name"),
    )
    index_value = item.get("index")
    if not word and isinstance(index_value, int) and 0 <= index_value < len(candidates):
        word = str(candidates[index_value].get("word", "")).strip()
    if not word:
        return None

    try:
        is_meme = _coerce_bool(
            item.get("is_meme", item.get("isMeme", item.get("meme")))
        )
    except ValueError:
        return None

    confidence = _coerce_float(item.get("confidence", item.get("score", 0.0)))
    candidate_category = (
        _first_non_empty_string(
            item.get("candidate_category"),
            item.get("category"),
            item.get("candidateCategory"),
        )
        or "其他"
    )
    reason = (
        _first_non_empty_string(
            item.get("reason"),
            item.get("explanation"),
            item.get("why"),
            item.get("analysis"),
        )
        or "模型未提供理由"
    )

    try:
        return QuickScreenResult(
            word=word,
            is_meme=is_meme,
            confidence=confidence,
            candidate_category=candidate_category,
            reason=reason,
        )
    except Exception:
        return None


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in {"true", "1", "yes", "y", "是"}:
        return True
    if text in {"false", "0", "no", "n", "否"}:
        return False
    raise ValueError(f"invalid bool value: {value!r}")


def _coerce_float(value: Any) -> float:
    try:
        parsed = float(value)
    except Exception:
        return 0.0
    return max(0.0, min(parsed, 1.0))


def _first_non_empty_string(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def summarize_screen_raw(raw: str, limit: int = 240) -> str:
    text = " ".join(str(raw or "").split())
    if len(text) <= limit:
        return text
    return text[:limit] + "...(truncated)"
