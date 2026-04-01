"""
AI 分析模块：三步流程对候选词进行梗识别和溯源。

Step 1: DeepSeek 批量快速筛选（低成本）
Step 2: 深度分析 + 工具调用（仅高置信度候选词）
Step 3: 来源 URL 验证（防幻觉）
"""

from __future__ import annotations

import json
from datetime import date

from openai import AsyncOpenAI
from pydantic_ai import Agent, capture_run_messages, messages
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider
from rich.console import Console
from rich.progress import track
from tenacity import retry, stop_after_attempt, wait_exponential

from meme_detector.archivist.duckdb_store import (
    create_agent_conversation,
    finish_agent_conversation,
    get_conn,
    get_pending_candidates,
    get_pending_scout_raw_videos,
    mark_scout_raw_videos_processed,
    upsert_scout_candidates,
    update_candidate_status,
)
from meme_detector.archivist.meili_store import upsert_meme
from meme_detector.config import settings
from meme_detector.researcher.models import CandidateSeed, MemeRecord, QuickScreenResult
from meme_detector.researcher.tools import (
    bilibili_search,
    verify_urls,
    web_search,
)
from meme_detector.researcher.video_context import get_bilibili_video_context
from meme_detector.run_tracker import get_current_run_id

console = Console()

# ── 模型初始化 ──────────────────────────────────────────────

def _get_deepseek_model() -> OpenAIModel:
    client = AsyncOpenAI(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
    )
    return OpenAIModel(
        settings.deepseek_model,
        provider=OpenAIProvider(openai_client=client),
    )


# ── Step 0: 从原始评论提取候选词 ──────────────────────────────

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
async def _extract_candidate_seeds(
    scout_videos: list[dict],
) -> list[dict]:
    """从 Scout 原始视频快照中提取候选词，并整理为候选队列格式。"""
    if not scout_videos:
        return []

    client = AsyncOpenAI(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
    )

    chunks = [
        scout_videos[i : i + 12]
        for i in range(0, len(scout_videos), 12)
    ]
    merged: dict[str, dict] = {}

    for chunk in chunks:
        payload_lines = []
        for video in chunk:
            comments = video.get("comments", [])
            if not isinstance(comments, list):
                comments = []
            trimmed_comments = [str(comment).strip()[:80] for comment in comments if str(comment).strip()][:8]
            payload_lines.append(
                "\n".join(
                    [
                        f'BV: {video.get("bvid", "")}',
                        f'分区: {video.get("partition", "")}',
                        f'标题: {str(video.get("title", "")).strip()[:80]}',
                        f'简介: {str(video.get("description", "")).strip()[:120]}',
                        "评论:",
                        *[f"- {comment}" for comment in trimmed_comments],
                    ]
                )
            )

        user_msg = "请从以下 Scout 原始采集内容中提取候选梗词：\n\n" + "\n\n".join(payload_lines)
        resp = await client.chat.completions.create(
            model=settings.deepseek_model,
            messages=[
                {"role": "system", "content": _CANDIDATE_EXTRACTION_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
        )

        raw = resp.choices[0].message.content or "{}"
        data = json.loads(raw)
        items = data if isinstance(data, list) else data.get("results", [])
        for item in items:
            try:
                seed = CandidateSeed(**item)
            except Exception:
                continue
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

    return _materialize_candidate_seeds(list(merged.values()), scout_videos)


def _materialize_candidate_seeds(
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
            matched_by_metadata = word in title or word in description
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


async def _bootstrap_candidates_from_scout() -> list[dict]:
    """将尚未处理的 Scout 原始视频转换成候选词队列。"""
    conn = get_conn()
    pending_videos = get_pending_scout_raw_videos(conn)
    conn.close()
    if not pending_videos:
        return []

    console.print(
        f"\n[bold]Step 0: 从 {len(pending_videos)} 个 Scout 原始视频快照中提取候选词...[/bold]"
    )
    candidates = await _extract_candidate_seeds(pending_videos)

    conn = get_conn()
    upsert_scout_candidates(conn, candidates)
    mark_scout_raw_videos_processed(conn, pending_videos)
    conn.close()

    console.print(f"  生成 {len(candidates)} 个候选词")
    return candidates


# ── Step 1: 快速批量筛选 ────────────────────────────────────

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
async def _batch_screen(
    candidates: list[dict],
) -> list[QuickScreenResult]:
    """批量快速筛选，每批最多 AI_BATCH_SIZE 个词。"""
    client = AsyncOpenAI(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
    )

    word_list = []
    for c in candidates:
        sample = c.get("sample_comments", "").strip()
        explanation = c.get("explanation", "").strip()
        context = explanation if explanation else (sample[:150] if sample else "无")
        word_list.append(
            f'- 词: "{c["word"]}" | 上下文: {context}'
        )

    user_msg = (
        "请对以下词汇逐一判断是否为网络梗，返回 JSON 数组，"
        "每项格式：{word, is_meme, confidence, candidate_category, reason}\n\n"
        + "\n".join(word_list)
    )

    resp = await client.chat.completions.create(
        model=settings.deepseek_model,
        messages=[
            {"role": "system", "content": _SCREEN_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
    )

    raw = resp.choices[0].message.content or "{}"
    data = json.loads(raw)

    # 兼容返回 {"results": [...]} 或直接 [...]
    items = data if isinstance(data, list) else data.get("results", [])
    results = []
    for item in items:
        try:
            results.append(QuickScreenResult(**item))
        except Exception:
            pass
    return results


# ── Step 2: 深度分析 Agent ──────────────────────────────────

_DEEP_ANALYSIS_SYSTEM = """\
你是一位专业的互联网亚文化研究员，正在为一个梗百科数据库撰写词条。
你会收到 Research 阶段整理出的评论样本，以及系统预取的“评论对应视频”背景。
你有权调用 B站搜索 和 Web搜索 工具来补充查阅资料。

请按以下步骤工作：
1. 先阅读 prompt 中已经提供的评论样本和关联视频背景，它们优先级最高
2. 再调用 bilibili_search 搜索该词，补充公开视频线索
3. 再调用 web_search 搜索 "[词] 梗 来源" 获取外部背景
4. 综合所有信息，填写完整的词条

输出要求：
- definition: 简洁解释含义，说明在网络上如何使用，不超过100字
- origin: 明确说明来源视频/事件，如有 BV 号请写入 source_urls
- category: 从[抽象、谐音、游戏、影视、音乐、社会现象、二次元、其他]中选
- heat_index: 根据搜索结果中的播放量/讨论量估算，0-100
- lifecycle_stage: emerging（最近才出现）/ peak（正在高峰）/ declining（已过热度）

关于系统预取的视频背景：
- status=ready 时，优先使用 summary / chapters / transcript_excerpt 作为视频背景
- status=skipped 且 skip_reason=duration_exceeded 时，表示视频超过 15 分钟，忽略即可
- status=unavailable 或 status=error 时，不要编造视频内容
"""

deep_agent: Agent[None, MemeRecord] = Agent(
    model=_get_deepseek_model(),
    output_type=MemeRecord,
    system_prompt=_DEEP_ANALYSIS_SYSTEM,
    tools=[bilibili_search, web_search],  # type: ignore[arg-type]
)


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=3, max=15))
async def _deep_analyze(
    word: str,
    sample_comments: str,
    video_refs: list[dict],
    score: float,
    today: date,
) -> MemeRecord | None:
    """对单个候选词进行深度分析。"""
    heat = min(100, int(score / 10 * 30 + 40)) if score < 999 else 75
    run_id = get_current_run_id()
    conversation_id: str | None = None
    if run_id:
        conn = get_conn()
        conversation_id = create_agent_conversation(
            conn,
            run_id=run_id,
            agent_name="researcher",
            word=word,
        )
        conn.close()

    video_contexts = await _prepare_linked_video_contexts(video_refs)
    prompt = (
        f'请为网络梗词汇「{word}」撰写完整词条。\n\n'
        f'检测信息：\n'
        f'- Research 候选得分：{score:.1f}/100\n'
        f'- 检测日期：{today}\n'
        f'- B站评论示例：\n{sample_comments or "（无样本）"}\n\n'
        f'- Scout 关联视频背景：\n{_format_video_contexts(video_contexts)}\n\n'
        f'请优先结合这些评论对应的视频背景，再按需调用搜索工具后输出完整词条。'
    )

    try:
        with capture_run_messages() as captured_messages:
            result = await deep_agent.run(prompt)
        record = result.output
        # 补充自动字段
        record.id = word
        record.first_detected_at = today
        record.updated_at = today
        record.heat_index = max(record.heat_index, heat)
        if conversation_id:
            _persist_agent_conversation(
                conversation_id=conversation_id,
                status="success",
                summary=record.title or record.definition[:80],
                messages_json=result.all_messages_json().decode("utf-8"),
                message_count=len(result.all_messages()),
                output_json=json.dumps(record.model_dump(), ensure_ascii=False, default=str),
            )
        return record
    except Exception as e:
        if conversation_id:
            failure_messages = (
                captured_messages if "captured_messages" in locals() else []
            )
            _persist_agent_conversation(
                conversation_id=conversation_id,
                status="failed",
                summary=f"{word} 分析失败",
                messages_json=_serialize_messages(failure_messages),
                message_count=len(failure_messages),
                error_message=str(e),
            )
        console.print(f"[red]  深度分析失败 [{word}]: {e}[/red]")
        return None


# ── 主流程 ──────────────────────────────────────────────────

async def run_research() -> dict:
    """完整的 AI 分析流程。"""
    console.print("\n[bold blue]═══ Researcher 开始运行 ═══[/bold blue]")

    bootstrapped_candidates = await _bootstrap_candidates_from_scout()
    conn = get_conn()
    candidates = get_pending_candidates(conn, limit=settings.ai_batch_size)
    conn.close()
    result = {
        "pending_count": len(candidates),
        "bootstrapped_count": len(bootstrapped_candidates),
        "screened_count": 0,
        "deep_analysis_count": 0,
        "accepted_count": 0,
        "rejected_count": 0,
        "accepted_records": [],
        "rejected_words": [],
        "failed_words": [],
    }

    if not candidates:
        console.print("[yellow]暂无待分析候选词[/yellow]")
        return result

    console.print(f"共 {len(candidates)} 个候选词待分析")

    # ── Step 1: 批量快速筛选 ─────────────────────────────────
    console.print("\n[bold]Step 1: 快速批量筛选...[/bold]")
    screen_results = await _batch_screen(candidates)
    result["screened_count"] = len(screen_results)

    screen_map = {r.word: r for r in screen_results}
    to_deep = [
        c for c in candidates
        if screen_map.get(c["word"]) and
           screen_map[c["word"]].is_meme and
           screen_map[c["word"]].confidence >= settings.ai_confidence_threshold
    ]

    rejected = [
        c["word"] for c in candidates
        if c["word"] not in {x["word"] for x in to_deep}
    ]
    if rejected:
        conn = get_conn()
        for word in rejected:
            update_candidate_status(conn, word, "rejected")
        conn.close()
    result["rejected_words"] = rejected
    result["rejected_count"] = len(rejected)
    result["deep_analysis_count"] = len(to_deep)

    console.print(
        f"  筛选结果：[green]{len(to_deep)} 个通过[/green]，"
        f"[red]{len(rejected)} 个拒绝[/red]"
    )

    if not to_deep:
        return result

    # ── Step 2 & 3: 深度分析 + URL 验证 ──────────────────────
    console.print("\n[bold]Step 2: 深度分析 + 溯源...[/bold]")
    today = date.today()
    success_count = 0

    for c in track(to_deep, description="分析中..."):
        word = c["word"]
        screen = screen_map.get(word)
        console.print(f"\n  → [{word}] confidence={screen.confidence:.2f}")

        record = await _deep_analyze(
            word=word,
            sample_comments=c.get("sample_comments", ""),
            video_refs=c.get("video_refs", []),
            score=c["score"],
            today=today,
        )
        if record is None:
            result["failed_words"].append(word)
            continue

        # Step 3: URL 验证
        if record.source_urls:
            original_source_count = len(record.source_urls)
            valid_urls = await verify_urls(record.source_urls)
            console.print(
                f"     来源验证：{original_source_count} → {len(valid_urls)} 个有效"
            )
            record.source_urls = valid_urls
            # 有效来源少于预期时，适当降低置信度
            if original_source_count > 0 and len(valid_urls) < original_source_count / 2:
                record.confidence_score *= 0.8

        # 写入 Meilisearch
        await upsert_meme(record)
        conn = get_conn()
        update_candidate_status(conn, word, "accepted")
        conn.close()
        success_count += 1
        result["accepted_records"].append(
            {
                "id": record.id,
                "title": record.title,
                "heat_index": record.heat_index,
                "lifecycle_stage": record.lifecycle_stage,
                "confidence_score": record.confidence_score,
            }
        )
        console.print("     [green]✓ 已入库[/green]")

    result["accepted_count"] = success_count
    console.print(
        f"\n[bold green]Researcher 完成：{success_count} 个梗成功入库[/bold green]"
    )
    return result


def _persist_agent_conversation(
    *,
    conversation_id: str,
    status: str,
    summary: str,
    messages_json: str,
    message_count: int,
    output_json: str = "{}",
    error_message: str = "",
) -> None:
    conn = get_conn()
    finish_agent_conversation(
        conn,
        conversation_id,
        status=status,
        summary=summary,
        messages_json=messages_json,
        message_count=message_count,
        output_json=output_json,
        error_message=error_message,
    )
    conn.close()


def _serialize_messages(model_messages: list) -> str:
    if not model_messages:
        return "[]"
    return messages.ModelMessagesTypeAdapter.dump_json(model_messages).decode("utf-8")


async def _prepare_linked_video_contexts(
    video_refs: list[dict],
    limit: int = 2,
) -> list[dict]:
    prepared: list[dict] = []
    for video_ref in _select_linked_videos(video_refs, limit=limit):
        bvid = str(video_ref.get("bvid", "")).strip()
        if not bvid:
            continue
        try:
            context = await get_bilibili_video_context(bvid)
        except Exception as exc:
            context = {
                "bvid": bvid,
                "status": "error",
                "error": str(exc),
            }
        prepared.append(
            {
                "video_ref": video_ref,
                "context": context,
            }
        )
    return prepared


def _select_linked_videos(video_refs: list[dict], limit: int = 2) -> list[dict]:
    normalized = [item for item in video_refs if isinstance(item, dict) and item.get("bvid")]
    normalized.sort(
        key=lambda item: int(item.get("matched_comment_count", 0)),
        reverse=True,
    )
    return normalized[:limit]


def _format_video_contexts(video_contexts: list[dict]) -> str:
    if not video_contexts:
        return "（无与评论直接关联的视频上下文）"

    sections: list[str] = []
    for index, item in enumerate(video_contexts, 1):
        video_ref = item.get("video_ref", {})
        context = item.get("context", {})
        matched_comments = video_ref.get("matched_comments", [])
        if not isinstance(matched_comments, list):
            matched_comments = []

        lines = [
            (
                f"{index}. "
                f"{video_ref.get('title') or context.get('title') or video_ref.get('bvid', '')}"
            ),
            f"   BVID: {video_ref.get('bvid', '')}",
            f"   URL: {video_ref.get('url') or context.get('video_url', '')}",
            f"   分区: {video_ref.get('partition', '')}",
            f"   匹配评论数: {video_ref.get('matched_comment_count', 0)}",
            f"   视频背景状态: {context.get('status', 'unknown')}",
        ]

        if matched_comments:
            lines.append("   匹配评论样本:")
            lines.extend(f"   - {comment}" for comment in matched_comments[:3])

        status = context.get("status")
        if status == "ready":
            summary = str(context.get("summary", "")).strip()
            transcript = str(context.get("transcript_excerpt", "")).strip()
            chapters = context.get("chapters", [])
            if summary:
                lines.append(f"   Bibi 摘要: {summary[:600]}")
            if transcript:
                lines.append(f"   字幕摘录: {transcript[:600]}")
            if isinstance(chapters, list) and chapters:
                chapter_summary = " | ".join(
                    f"{chapter.get('timestamp', '')} {chapter.get('title', '')}".strip()
                    for chapter in chapters[:4]
                    if isinstance(chapter, dict)
                )
                if chapter_summary:
                    lines.append(f"   章节: {chapter_summary}")
        elif status == "skipped":
            lines.append(
                f"   跳过原因: {context.get('skip_reason', 'unknown')}"
            )
        elif status == "unavailable":
            lines.append(
                f"   不可用原因: {context.get('skip_reason', 'missing_api_token')}"
            )
        elif status == "error":
            lines.append(f"   错误: {context.get('error', 'unknown')}")

        sections.append("\n".join(lines))

    return "\n\n".join(sections)
