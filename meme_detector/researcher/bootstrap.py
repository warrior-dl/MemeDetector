"""
Research Step 0: 从 Miner 线索中抽取候选词并落库。
"""

from __future__ import annotations

import json

from tenacity import retry, stop_after_attempt, wait_exponential

from meme_detector.archivist.duckdb_store import (
    get_conn,
    get_pending_miner_comment_insights,
    get_pending_scout_raw_videos,
    mark_miner_comment_insights_processed,
    mark_scout_raw_videos_processed,
    upsert_scout_candidates,
)
from meme_detector.config import settings
from meme_detector.llm_factory import (
    build_async_openai_client,
    load_json_response,
    request_json_chat_completion,
    resolve_llm_config,
)
from meme_detector.logging_utils import get_logger
from meme_detector.researcher.models import CandidateSeed

logger = get_logger(__name__)

_CANDIDATE_EXTRACTION_SYSTEM = """\
你是一位专业的中文互联网亚文化观察员。
你的任务是从 B站热门视频的元信息和评论中，提取“值得进一步研究的候选梗词/短语”。

候选词要求：
1. 必须是评论里真实出现，或能被评论中的重复表达直接支撑的词/短语
2. 优先选择带有圈层语境、二创语义、谐音、抽象文化色彩的表达
3. 排除普通情绪词、通用口语、单纯夸张词、UP主昵称、活动名、视频标题党
4. 候选词尽量简短具体，优先 2-12 个字

输出 JSON：
{
  "results": [
    {
      "word": "候选词",
      "confidence": 0.0,
      "reason": "简短说明为什么值得进一步研究",
      "related_bvids": ["BV..."],
      "sample_comments": ["评论1", "评论2"]
    }
  ]
}

要求：
- 最多返回 15 个候选词
- 不要编造输入里不存在的 BV 号或评论
- 如果没有合适候选词，返回 {"results": []}
"""


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def extract_candidate_seeds(
    scout_videos: list[dict],
) -> list[dict]:
    """从 Scout 原始视频快照中提取候选词，并整理为候选队列格式。"""
    if not scout_videos:
        return []

    client = build_async_openai_client("research")
    llm_config = resolve_llm_config("research")

    chunks = [
        scout_videos[i : i + 12]
        for i in range(0, len(scout_videos), 12)
    ]
    logger.info(
        "research candidate extraction prepared",
        extra={
            "event": "research_candidate_extraction_prepared",
            "video_count": len(scout_videos),
            "batch_total": len(chunks),
            "model_name": llm_config.model,
            "provider": llm_config.provider,
        },
    )
    merged: dict[str, dict] = {}

    for chunk_index, chunk in enumerate(chunks):
        logger.info(
            "research candidate extraction chunk started",
            extra={
                "event": "research_candidate_extraction_chunk_started",
                "chunk_index": chunk_index,
                "batch_total": len(chunks),
                "video_count": len(chunk),
            },
        )
        payload_lines = []
        for video in chunk:
            comments = video.get("comments", [])
            if not isinstance(comments, list):
                comments = []
            trimmed_comments = [str(comment).strip()[:80] for comment in comments if str(comment).strip()][:8]
            video_context = video.get("video_context", {})
            if not isinstance(video_context, dict):
                video_context = {}
            tags = video.get("tags", [])
            if not isinstance(tags, list):
                tags = []
            payload_lines.append(
                "\n".join(
                    [
                        f'BV: {video.get("bvid", "")}',
                        f'分区: {video.get("partition", "")}',
                        f'标题: {str(video.get("title", "")).strip()[:80]}',
                        f'简介: {str(video.get("description", "")).strip()[:120]}',
                        f'标签: {", ".join(str(tag).strip() for tag in tags if str(tag).strip()) or "无"}',
                        f'视频内容摘要: {str(video_context.get("summary", "")).strip()[:300] or "无"}',
                        f'视频内容正文: {str(video_context.get("content_text", "")).strip()[:500] or "无"}',
                        "评论:",
                        *[f"- {comment}" for comment in trimmed_comments],
                    ]
                )
            )

        user_msg = "请从以下 Scout 原始采集内容中提取候选梗词：\n\n" + "\n\n".join(payload_lines)
        raw = await request_json_chat_completion(
            client=client,
            model_name=llm_config.model,
            messages=[
                {"role": "system", "content": _CANDIDATE_EXTRACTION_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
        )
        data = load_json_response(raw)
        items = data if isinstance(data, list) else data.get("results", [])
        parsed_count = 0
        for item in items:
            try:
                seed = CandidateSeed(**item)
            except Exception:
                continue
            parsed_count += 1
            existing = merged.get(seed.word)
            if not existing or existing["confidence"] < seed.confidence:
                merged[seed.word] = seed.model_dump()
            else:
                existing["related_bvids"] = sorted(
                    {
                        *existing.get("related_bvids", []),
                        *seed.related_bvids,
                    }
                )
                existing["sample_comments"] = list(
                    dict.fromkeys(
                        [
                            *existing.get("sample_comments", []),
                            *seed.sample_comments,
                        ]
                    )
                )[:5]
        logger.info(
            "research candidate extraction chunk completed",
            extra={
                "event": "research_candidate_extraction_chunk_completed",
                "chunk_index": chunk_index,
                "batch_total": len(chunks),
                "result_count": parsed_count,
                "candidate_count": len(merged),
            },
        )

    return materialize_candidate_seeds(list(merged.values()), scout_videos)


def materialize_candidate_seeds(
    seeds: list[dict],
    scout_videos: list[dict],
) -> list[dict]:
    """将 AI 抽取结果补全为候选队列需要的上下文字段。"""
    if not seeds:
        return []

    video_map = {video["bvid"]: video for video in scout_videos if video.get("bvid")}
    candidates: list[dict] = []

    for seed_data in seeds:
        try:
            seed = CandidateSeed(**seed_data)
        except Exception:
            continue

        word = seed.word.strip()
        if len(word) < 2:
            continue

        sample_comments: list[str] = []
        video_refs: list[dict] = []
        related_bvids = set(seed.related_bvids)

        for video in scout_videos:
            comments = video.get("comments", [])
            if not isinstance(comments, list):
                comments = []
            matched_comments = [
                str(comment).strip()
                for comment in comments
                if str(comment).strip() and word in str(comment)
            ]
            title = str(video.get("title", "")).strip()
            description = str(video.get("description", "")).strip()
            tags = video.get("tags", [])
            if not isinstance(tags, list):
                tags = []
            matched_by_metadata = word in title or word in description or any(word in str(tag) for tag in tags)
            if video.get("bvid") in related_bvids and not matched_comments:
                matched_by_metadata = True

            if not matched_comments and not matched_by_metadata:
                continue

            unique_comments = list(dict.fromkeys(matched_comments))[:3]
            sample_comments.extend(unique_comments)
            video_refs.append(
                {
                    "bvid": video.get("bvid", ""),
                    "partition": video.get("partition", ""),
                    "title": title,
                    "description": description,
                    "url": video.get("url", ""),
                    "tags": video.get("tags", []),
                    "video_context": video.get("video_context", {}),
                    "matched_comment_count": len(matched_comments),
                    "matched_comments": unique_comments,
                }
            )

        if not video_refs and related_bvids:
            for bvid in related_bvids:
                video = video_map.get(bvid)
                if not video:
                    continue
                video_refs.append(
                    {
                        "bvid": video.get("bvid", ""),
                        "partition": video.get("partition", ""),
                        "title": video.get("title", ""),
                        "description": video.get("description", ""),
                        "url": video.get("url", ""),
                        "tags": video.get("tags", []),
                        "video_context": video.get("video_context", {}),
                        "matched_comment_count": 0,
                        "matched_comments": [],
                    }
                )

        if not video_refs:
            continue

        sample_comments = list(dict.fromkeys(sample_comments or seed.sample_comments))[:5]
        video_refs = sorted(
            video_refs,
            key=lambda item: (item.get("matched_comment_count", 0), item.get("bvid", "")),
            reverse=True,
        )[:5]
        confidence = max(0.0, min(1.0, seed.confidence))
        candidates.append(
            {
                "word": word,
                "score": round(confidence * 100, 2),
                "is_new_word": True,
                "sample_comments": "\n".join(f"- {comment}" for comment in sample_comments),
                "explanation": (
                    f"Research 预筛候选：提取置信度 {confidence:.2f}，"
                    f"关联 {len(video_refs)} 个视频。{seed.reason}"
                ),
                "video_refs": video_refs,
            }
        )

    return sorted(candidates, key=lambda item: item["score"], reverse=True)


async def bootstrap_candidates_from_miner() -> list[dict]:
    """优先消费 Miner 产出的评论线索，再生成候选词。"""
    conn = get_conn()
    pending_insights = get_pending_miner_comment_insights(conn, limit=5000)
    pending_videos = get_pending_scout_raw_videos(conn)
    conn.close()

    if not pending_insights:
        if pending_videos:
            logger.warning(
                "research blocked by pending miner videos",
                extra={
                    "event": "research_blocked_by_pending_miner_videos",
                    "video_count": len(pending_videos),
                },
            )
        else:
            logger.info(
                "research bootstrap found no pending miner insights",
                extra={
                    "event": "research_bootstrap_no_pending_insights",
                },
            )
        return []

    high_value_insights = [
        item
        for item in pending_insights
        if item.get("confidence", 0.0) >= settings.miner_comment_confidence_threshold
        and (item.get("is_meme_candidate") or item.get("is_insider_knowledge"))
    ]
    logger.info(
        "research bootstrap started",
        extra={
            "event": "research_bootstrap_started",
            "insight_count": len(pending_insights),
            "high_value_count": len(high_value_insights),
        },
    )
    grouped_videos = group_miner_insights_by_video(high_value_insights)
    logger.info(
        "research bootstrap grouped miner insights",
        extra={
            "event": "research_bootstrap_grouped_insights",
            "video_count": len(grouped_videos),
            "comment_count": sum(len(item.get("comments", [])) for item in grouped_videos),
        },
    )
    candidates = await extract_candidate_seeds(grouped_videos)

    touched_videos = [
        {"bvid": item.get("bvid"), "collected_date": item.get("collected_date")}
        for item in pending_insights
    ]
    conn = get_conn()
    upsert_scout_candidates(conn, candidates)
    mark_miner_comment_insights_processed(conn, pending_insights)
    mark_scout_raw_videos_processed(conn, touched_videos)
    conn.close()

    logger.info(
        "research bootstrap completed",
        extra={
            "event": "research_bootstrap_completed",
            "candidate_count": len(candidates),
            "video_count": len(grouped_videos),
            "result_count": len(candidates),
        },
    )
    return candidates


def group_miner_insights_by_video(insights: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, object], dict] = {}
    for item in insights:
        key = (str(item.get("bvid", "")).strip(), item.get("collected_date"))
        if not key[0] or not key[1]:
            continue
        current = grouped.setdefault(
            key,
            {
                "bvid": key[0],
                "collected_date": item.get("collected_date"),
                "partition": item.get("partition", ""),
                "title": item.get("title", ""),
                "description": item.get("description", ""),
                "url": item.get("url", ""),
                "tags": item.get("tags", []),
                "video_context": item.get("video_context", {}),
                "comments": [],
            },
        )
        current["comments"].append(str(item.get("comment_text", "")).strip())
    for item in grouped.values():
        item["comments"] = list(dict.fromkeys([comment for comment in item["comments"] if comment]))[:20]
    return list(grouped.values())
