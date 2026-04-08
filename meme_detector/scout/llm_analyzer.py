"""
LLM 梗识别模块：直接从高赞评论中提取梗候选短语。
"""

from __future__ import annotations

import json

from tenacity import retry, stop_after_attempt, wait_exponential

from meme_detector.llm_factory import build_async_openai_client, resolve_llm_config
from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)

_SYSTEM_PROMPT = """\
你是一位互联网亚文化观察者，专门从B站评论区识别正在传播的网络梗。

给定一批来自不同视频的高赞评论，请识别其中正在传播的梗、新兴表达或有趣句式。

梗的识别标准（符合其一即可）：
- 多条评论出现相似的表达方式，即使措辞略有不同（说明这个说法在传播）
- 带有特定的网络含义或幽默感，不是字面意思
- 感觉是在引用或模仿某个说法，而非原创表达
- 是句式模板（如"我像X一样Y了"），不同的人填入不同内容

不要输出：
- 单个词汇（如"厉害"、"绝了"），需给出完整短语或句式
- 只在一条评论中出现的内容
- 某圈子的日常用语，不具有向外传播的潜力（如粉圈内部称呼）
- 纯粹在夸赞内容的评论（如"大招帅"——只是感叹，不是梗）

输出 JSON 格式：{"memes": [...]}
每项格式：
{
  "phrase": "梗的短语或句式（保留关键结构，变量部分用X代替）",
  "explanation": "一句话解释：这是什么梗，来源是什么，如何使用",
  "examples": ["包含这个梗的原始评论1", "原始评论2"],
  "confidence": 0.9
}

如果没有发现梗，返回 {"memes": []}
"""


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def _extract_memes_from_batch(comments: list[str]) -> list[dict]:
    """对一批评论调用 LLM，提取梗候选。"""
    client = build_async_openai_client()
    llm_config = resolve_llm_config()

    comment_text = "\n".join(f"{i + 1}. {c}" for i, c in enumerate(comments))
    user_msg = f"以下是来自B站的高赞评论，请识别其中正在传播的梗：\n\n{comment_text}"

    resp = await client.chat.completions.create(
        model=llm_config.model,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        response_format={"type": "json_object"},
    )

    raw = resp.choices[0].message.content or "{}"
    data = json.loads(raw)
    return data.get("memes", [])


async def analyze_all_comments(
    all_comments: list[str],
    batch_size: int = 100,
) -> list[dict]:
    """
    分批分析所有评论，合并去重后返回梗候选列表。

    返回: [{"phrase": str, "explanation": str, "examples": list[str], "confidence": float}, ...]
    """
    all_memes: list[dict] = []

    batches = [all_comments[i : i + batch_size] for i in range(0, len(all_comments), batch_size)]
    for idx, batch in enumerate(batches, 1):
        logger.info(
            "scout llm batch started",
            extra={
                "event": "scout_llm_batch_started",
                "chunk_index": idx - 1,
                "batch_index": idx,
                "batch_total": len(batches),
                "comment_count": len(batch),
            },
        )
        try:
            memes = await _extract_memes_from_batch(batch)
            logger.info(
                "scout llm batch completed",
                extra={
                    "event": "scout_llm_batch_completed",
                    "chunk_index": idx - 1,
                    "batch_index": idx,
                    "batch_total": len(batches),
                    "candidate_count": len(memes),
                },
            )
            all_memes.extend(memes)
        except Exception as e:
            logger.warning(
                "scout llm batch failed",
                extra={
                    "event": "scout_llm_batch_failed",
                    "chunk_index": idx - 1,
                    "batch_index": idx,
                    "batch_total": len(batches),
                },
                exc_info=e,
            )

    # 按 phrase 去重，保留置信度最高的
    seen: dict[str, dict] = {}
    for m in all_memes:
        phrase = m.get("phrase", "").strip()
        if not phrase:
            continue
        if phrase not in seen or m.get("confidence", 0) > seen[phrase].get("confidence", 0):
            seen[phrase] = m

    return list(seen.values())
