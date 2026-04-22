"""Layer 4: 单轮 LLM pairwise judge。

对每个 Leiden 社区调用一次 LLM，prompt 里**同时**给出"已知是梗"和"已知不是梗"
的少量正负样本做 in-context，让 LLM 对比着判定。

本轮不做完整 ALARM 自迭代，只跑一次；自迭代留给下一步。
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Literal

from openai import AsyncOpenAI

from meme_detector.llm_factory import (
    build_async_openai_client,
    load_json_response,
    request_json_chat_completion,
    resolve_llm_config,
)
from meme_detector.logging_utils import get_logger

from .community import Community

logger = get_logger(__name__)

# in-context 正/负样本。这些是用户此前明确确认过的"真梗"和"FP 典型"，
# 作为 LLM 判定的参照锚。数量保持少（各 4-6 条），避免上下文过长。
KNOWN_POSITIVE_EXAMPLES: list[tuple[list[str], str]] = [
    (["家人们谁懂啊", "家人们谁能懂"], "B 站 / 抖音流行口头禅，表达感同身受"),
    (["绷不住了", "蚌埠住了", "蚌不住"], "网络用语，'忍不住笑/哭'，蚌埠住是谐音梗变体"),
    (["一眼 AI", "一眼假"], "指'一眼看出是 AI 生成'的视频，带戏谑意味"),
    (["爷青回", "爷的青春回来了"], "怀旧梗"),
    (["awsl", "啊我死了"], "表达过度可爱/激动"),
]

KNOWN_NEGATIVE_EXAMPLES: list[tuple[list[str], str]] = [
    (["大会员", "大会员呢"], "B 站平台术语，付费订阅"),
    (["哔哩哔哩", "B 站", "在 b 站总"], "平台名称"),
    (["只能说", "说句实话", "说到底"], "通用口语套话"),
    (["这个视频", "这期", "本期"], "元指代，无信息量"),
    (["UP 主", "up 主"], "B 站创作者称呼"),
    (["打 call", "打call"], "已通用化的老词，不再算新梗"),
]

_SYSTEM_PROMPT = """你是一个网络流行语 / 梗分析专家。判定下面这组候选片段作为整体是否构成流行梗。

判定标准：
- meme（是梗）：短期在社群中流行、有模仿性的用语；通常是新造词 / 谐音 / 梗图台词 / 口头禅
- not_meme（不是梗）：
  * platform_term：B 站平台术语（大会员、UP 主、弹幕、充电）
  * generic_phrase：通用口语套话（只能说、这个视频）
  * proper_noun：人名 / 作品名 / 专业术语
  * old_general：已经完全通用化、不再是"梗"的老词（打 call、yyds）
- uncertain（存疑）：边界情况 / 把握不足

返回严格 JSON：
{"verdict": "meme" | "not_meme" | "uncertain",
 "category": "meme" | "platform_term" | "generic_phrase" | "proper_noun" | "old_general" | "uncertain",
 "canonical_form": "这组候选最规范的表达（若是梗）",
 "reason": "一句话理由（中文，不要超过 40 字）",
 "confidence": 0.0 到 1.0}"""


def _format_examples(title: str, examples: list[tuple[list[str], str]]) -> str:
    lines = [title]
    for terms, note in examples:
        lines.append(f"  - {' / '.join(terms)}    // {note}")
    return "\n".join(lines)


def _build_user_prompt(community: Community) -> str:
    terms_block = " / ".join(community.terms[:30])
    if len(community.terms) > 30:
        terms_block += f" … (共 {len(community.terms)} 个 term)"

    if community.size >= 2:
        cooccur_hint = (
            f"簇内共现事件 {community.cooccur_event_count} 次 / 共现边 {community.cooccur_edge_count} 条"
        )
    else:
        cooccur_hint = "单 term 社区（无共现辅助信号）"

    stats = (
        f"出现 {community.total_freq} 次 · 跨 {community.n_videos} 视频 · "
        f"跨 {community.n_authors} 作者 · "
        f"簇内相似度均值 {community.avg_variant_sim:.2f} · "
        f"跨视频率 {community.cross_video_ratio:.2f} · "
        f"{cooccur_hint}"
    )

    pos = _format_examples("[已确认是梗]", KNOWN_POSITIVE_EXAMPLES)
    neg = _format_examples("[已确认不是梗]", KNOWN_NEGATIVE_EXAMPLES)

    return f"""参考样本对比：

{pos}

{neg}

─────────────────────────

待判定（作为整体）：
候选：{terms_block}
统计：{stats}

返回 JSON。"""


def _parse_verdict(raw: str) -> tuple[str, str, float]:
    payload = load_json_response(raw)
    if not isinstance(payload, dict):
        return ("uncertain", "LLM 返回结构异常", 0.0)
    verdict = str(payload.get("verdict") or "uncertain").strip().lower()
    if verdict not in {"meme", "not_meme", "uncertain"}:
        verdict = "uncertain"
    reason_parts: list[str] = []
    if payload.get("category"):
        reason_parts.append(f"[{payload['category']}]")
    if payload.get("canonical_form"):
        reason_parts.append(f"规范:{payload['canonical_form']}")
    if payload.get("reason"):
        reason_parts.append(str(payload["reason"]))
    reason = " ".join(reason_parts).strip() or "无理由"
    try:
        conf = float(payload.get("confidence", 0.5))
    except (TypeError, ValueError):
        conf = 0.5
    conf = max(0.0, min(1.0, conf))
    return (verdict, reason, conf)


async def _judge_one(
    client: AsyncOpenAI,
    model: str,
    community: Community,
) -> Community:
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": _build_user_prompt(community)},
    ]
    try:
        raw = await request_json_chat_completion(
            client=client,
            model_name=model,
            messages=messages,
        )
        verdict, reason, conf = _parse_verdict(raw)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "judge llm call failed",
            extra={"event": "judge_llm_error", "community_id": community.community_id, "error": repr(exc)},
        )
        verdict, reason, conf = ("uncertain", f"LLM 调用异常: {exc!r}", 0.0)

    community.verdict = verdict
    community.verdict_reason = reason
    community.verdict_confidence = conf
    return community


async def judge_communities(
    communities: Iterable[Community],
    *,
    concurrency: int = 6,
    min_size: int = 1,
    target: Literal["default", "miner", "research"] = "research",
) -> list[Community]:
    """对每个社区跑一次 LLM 裁决。

    - ``min_size``：社区节点数 < 此值直接跳过（打 uncertain）
    - ``target``：默认用 research LLM（reasoning 模型更适合分类判断）
    """
    comm_list = list(communities)
    if not comm_list:
        return []

    config = resolve_llm_config(target)
    if not config.api_key or not config.model:
        raise RuntimeError(f"LLM target={target} 未配置，请在 .env 设置对应的 *_LLM_API_KEY / *_LLM_MODEL。")

    client = build_async_openai_client(target)
    sem = asyncio.Semaphore(concurrency)

    async def _run(c: Community) -> Community:
        if c.size < min_size:
            c.verdict = "uncertain"
            c.verdict_reason = f"size<{min_size}, skipped"
            c.verdict_confidence = 0.0
            return c
        async with sem:
            return await _judge_one(client, config.model, c)

    try:
        return await asyncio.gather(*[_run(c) for c in comm_list])
    finally:
        await client.close()
