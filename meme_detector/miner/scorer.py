"""
Miner 主流程：预取视频内容并对评论做初步线索打分。
"""

from __future__ import annotations

from datetime import date

from rich.console import Console

import meme_detector.miner.analysis as _analysis_module
from meme_detector.config import settings
from meme_detector.miner.analysis import AsyncOpenAI
from meme_detector.miner.models import MinerRunResult
from meme_detector.miner.persistence import (
    list_pending_scout_videos as _list_pending_scout_videos,
    mark_video_mined as _mark_video_mined,
    persist_video_insights as _persist_video_insights,
)
from meme_detector.miner.video_context import get_bilibili_video_context
from meme_detector.run_tracker import get_current_run_id

console = Console()

async def _score_video_comments(video: dict, comments: list[str]) -> list[dict]:
    # 保持 scorer 模块作为稳定 monkeypatch 入口。
    _analysis_module.AsyncOpenAI = AsyncOpenAI
    _analysis_module.get_bilibili_video_context = get_bilibili_video_context
    _analysis_module.get_current_run_id = get_current_run_id
    return await _analysis_module.score_video_comments(video, comments)


async def run_miner(target_date: date | None = None) -> MinerRunResult:
    """对待处理的 Scout 原始视频做评论线索打分。"""
    today = target_date or date.today()
    console.print(f"\n[bold blue]═══ Miner 开始运行 {today} ═══[/bold blue]")

    pending_videos = _list_pending_scout_videos()
    if not pending_videos:
        console.print("[yellow]暂无待挖掘的 Scout 原始视频[/yellow]")
        return MinerRunResult(target_date=today.isoformat())

    insight_count = 0
    high_value_count = 0
    for video_index, video in enumerate(pending_videos, 1):
        comments = video.get("comments", [])
        if not isinstance(comments, list):
            comments = []
        comments = [str(comment).strip() for comment in comments if str(comment).strip()]
        if not comments:
            _mark_video_mined(video)
            console.print(
                f"[cyan]处理视频 {video_index}/{len(pending_videos)}: "
                f"{str(video.get('bvid', '')).strip() or 'UNKNOWN'} "
                "[dim](无有效评论，已跳过并落库状态)[/dim][/cyan]"
            )
            continue

        console.print(
            f"[cyan]处理视频 {video_index}/{len(pending_videos)}: "
            f"{str(video.get('bvid', '')).strip() or 'UNKNOWN'} "
            f"({len(comments)} 条评论)[/cyan]"
        )
        insights = await _score_video_comments(video, comments)
        _persist_video_insights(video, insights)
        insight_count += len(insights)
        high_value_count += sum(
            1
            for item in insights
            if item.get("confidence", 0.0) >= settings.miner_comment_confidence_threshold
            and (item.get("is_meme_candidate") or item.get("is_insider_knowledge"))
        )

    console.print(
        f"[bold green]Miner 完成，写入 {insight_count} 条评论线索，"
        f"其中 {high_value_count} 条高价值[/bold green]"
    )
    return MinerRunResult(
        target_date=today.isoformat(),
        video_count=len(pending_videos),
        insight_count=insight_count,
        high_value_count=high_value_count,
    )
