"""
B站数据采集模块。
采集高热分区的 Top 视频评论和弹幕。
"""

from __future__ import annotations

import asyncio
import random
import re
import time
from dataclasses import dataclass

from bilibili_api import video, rank, comment, Credential, request_settings
from bilibili_api.rank import RankType
from bilibili_api.comment import CommentResourceType, OrderType
from rich.console import Console

from meme_detector.config import settings

console = Console()

# MVP 阶段采集的目标分区 (partition_name, RankType)
TARGET_PARTITIONS: list[tuple[str, RankType]] = [
    ("动画", RankType.Douga),
    ("游戏", RankType.Game),
    ("鬼畜", RankType.Kichiku),
    ("生活", RankType.Life),
]

RETRYABLE_STATUS_CODES = {412, 429, 500, 502, 503, 504}


@dataclass
class VideoTexts:
    bvid: str
    partition: str
    comments: list[str]


@dataclass
class CommentRiskState:
    consecutive_hits: int = 0
    cooldown_until: float = 0.0

    def remaining_cooldown(self) -> float:
        return max(0.0, self.cooldown_until - time.monotonic())

    def should_skip_comments(self) -> bool:
        return (
            self.consecutive_hits >= settings.scout_risk_skip_threshold
            and self.remaining_cooldown() > 0
        )

    def note_risk_hit(self, cooldown_seconds: float) -> None:
        self.consecutive_hits += 1
        self.cooldown_until = max(
            self.cooldown_until,
            time.monotonic() + cooldown_seconds,
        )

    def note_success(self) -> None:
        self.consecutive_hits = 0
        self.cooldown_until = 0.0


def _build_credential() -> Credential | None:
    """从配置构建 Bilibili Credential，未配置时返回 None（匿名访问）。"""
    if settings.bilibili_sessdata:
        return Credential(
            sessdata=settings.bilibili_sessdata,
            bili_jct=settings.bilibili_bili_jct,
            buvid3=settings.bilibili_buvid3,
        )
    return None


async def _random_delay() -> None:
    """在请求间加入随机延迟，规避风控。"""
    delay = random.uniform(settings.scout_delay_min, settings.scout_delay_max)
    await asyncio.sleep(delay)


def _extract_status_code(exc: Exception) -> int | None:
    for attr in ("status_code", "code", "status"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)

    text = str(exc)
    patterns = (
        r"状态码[:： ]+(\d{3})",
        r"status code[:： ]+(\d{3})",
        r"错误号[:： ]+(\d{3})",
        r"\b([45]\d{2})\b",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _is_risk_control_error(exc: Exception) -> bool:
    text = str(exc).lower()
    status_code = _extract_status_code(exc)
    return status_code == 412 or any(
        marker in text
        for marker in (
            "安全风控",
            "security control policy",
            "precondition failed",
        )
    )


def _is_retryable_comment_error(exc: Exception) -> bool:
    status_code = _extract_status_code(exc)
    if status_code in RETRYABLE_STATUS_CODES:
        return True

    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "network error",
            "timeout",
            "timed out",
            "temporarily unavailable",
            "connection reset",
        )
    )


def _compute_comment_retry_delay(attempt_index: int, exc: Exception) -> float:
    base_delay = settings.scout_comment_backoff_base * (2 ** attempt_index)
    jitter = random.uniform(0.0, 1.0)
    if _is_risk_control_error(exc):
        return max(settings.scout_risk_cooldown_seconds, base_delay) + jitter
    return base_delay + jitter


async def _fetch_video_comments(
    bvid: str,
    credential: Credential | None,
    max_count: int,
    risk_state: CommentRiskState,
) -> list[str]:
    """获取视频热评文本列表。"""
    v = video.Video(bvid=bvid, credential=credential)
    texts: list[str] = []
    page = 1
    try:
        aid = v.get_aid()
        while len(texts) < max_count:
            retries = 0
            while True:
                result: dict | None = None
                try:
                    result = await comment.get_comments(
                        oid=aid,
                        type_=CommentResourceType.VIDEO,
                        page_index=page,
                        order=OrderType.LIKE,
                        credential=credential,
                    )
                    break
                except Exception as e:
                    retryable = _is_retryable_comment_error(e)
                    risk_control = _is_risk_control_error(e)
                    delay = _compute_comment_retry_delay(retries, e)
                    if risk_control:
                        risk_state.note_risk_hit(delay)

                    if not retryable or retries >= settings.scout_comment_retry_times:
                        label = "评论触发风控" if risk_control else "评论获取失败"
                        console.print(f"[yellow]  {label} {bvid}: {e}[/yellow]")
                        return texts[:max_count]

                    console.print(
                        "[yellow]"
                        f"  评论请求重试 {bvid} 第 {page} 页 "
                        f"({retries + 1}/{settings.scout_comment_retry_times})，"
                        f"{delay:.1f}s 后继续: {e}"
                        "[/yellow]"
                    )
                    retries += 1
                    await asyncio.sleep(delay)

            replies = (result or {}).get("replies") or []
            if not replies:
                break
            for reply in replies:
                content = reply.get("content", {}).get("message", "")
                if content:
                    texts.append(content)
            page += 1
            await _random_delay()
        risk_state.note_success()
    except Exception as e:
        console.print(f"[yellow]  评论获取失败 {bvid}: {e}[/yellow]")
    return texts[:max_count]


async def _fetch_partition_top_videos(
    rank_type: RankType,
    partition_name: str,
    credential: Credential | None,
    top_n: int,
) -> list[VideoTexts]:
    """获取指定分区 Top N 视频的热门评论。"""
    results: list[VideoTexts] = []
    risk_state = CommentRiskState()

    try:
        # 获取分区排行榜
        rank_result = await rank.get_rank(type_=rank_type)
        bvids = [item["bvid"] for item in rank_result["list"][:top_n]]
    except Exception as e:
        console.print(f"[red]获取分区 {partition_name} 排行榜失败: {e}[/red]")
        return []

    console.print(f"[cyan]  分区 [{partition_name}] 共 {len(bvids)} 个视频[/cyan]")

    for i, bvid in enumerate(bvids, 1):
        console.print(f"  [{i}/{len(bvids)}] {bvid} ...", end=" ")
        try:
            comments: list[str] = []
            if risk_state.should_skip_comments():
                console.print(
                    "[yellow]"
                    f"评论接口冷却中，跳过，剩余 "
                    f"{risk_state.remaining_cooldown():.1f}s。"
                    "[/yellow]",
                    end=" ",
                )
            else:
                comments = await _fetch_video_comments(
                    bvid,
                    credential,
                    settings.scout_comments_per_video,
                    risk_state,
                )
            results.append(
                VideoTexts(
                    bvid=bvid,
                    partition=partition_name,
                    comments=comments,
                )
            )
            console.print(f"[green]评论 {len(comments)} 条[/green]")
        except Exception as e:
            console.print(f"[red]失败: {e}[/red]")
        await _random_delay()

    return results


async def collect_all_partitions(
    partitions: list[tuple[str, RankType]] | None = None,
) -> dict[str, list[VideoTexts]]:
    """
    采集所有目标分区数据。

    返回: {partition_name: [VideoTexts, ...]}
    """
    if partitions is None:
        partitions = TARGET_PARTITIONS

    credential = _build_credential()
    request_settings.set_timeout(settings.scout_request_timeout)
    request_settings.set_wbi_retry_times(max(1, settings.scout_comment_retry_times + 1))
    request_settings.set_enable_auto_buvid(True)
    request_settings.set_enable_bili_ticket(True)

    # 配置代理（如果有）
    if settings.scout_proxy_url:
        request_settings.set_proxy(settings.scout_proxy_url)
        console.print(f"[cyan]已启用代理: {settings.scout_proxy_url}[/cyan]")
    elif credential is None:
        console.print("[yellow]未配置 B站 Cookie，评论接口更容易触发风控。[/yellow]")

    result: dict[str, list[VideoTexts]] = {}
    for partition_name, rank_type in partitions:
        console.print(f"\n[bold]正在采集分区: {partition_name}[/bold]")
        videos = await _fetch_partition_top_videos(
            rank_type=rank_type,
            partition_name=partition_name,
            credential=credential,
            top_n=settings.scout_top_n_videos,
        )
        result[partition_name] = videos

    return result
