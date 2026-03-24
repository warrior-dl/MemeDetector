"""
B站数据采集模块。
采集高热分区的 Top 视频评论和弹幕。
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass

from bilibili_api import video, hot, Credential, request_settings
from rich.console import Console

from meme_detector.config import settings

console = Console()

# MVP 阶段采集的目标分区 (partition_name, tid)
TARGET_PARTITIONS: list[tuple[str, int]] = [
    ("动画", 1),
    ("游戏", 4),
    ("鬼畜", 119),
    ("生活", 160),
]


@dataclass
class VideoTexts:
    bvid: str
    partition: str
    comments: list[str]
    danmaku: list[str]

    @property
    def all_texts(self) -> list[str]:
        return self.comments + self.danmaku


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


async def _fetch_video_comments(
    bvid: str,
    credential: Credential | None,
    max_count: int,
) -> list[str]:
    """获取视频热评文本列表。"""
    v = video.Video(bvid=bvid, credential=credential)
    texts: list[str] = []
    page = 1
    try:
        while len(texts) < max_count:
            result = await v.get_comments(page=page, page_size=20)
            replies = result.get("replies") or []
            if not replies:
                break
            for reply in replies:
                content = reply.get("content", {}).get("message", "")
                if content:
                    texts.append(content)
            page += 1
            await _random_delay()
    except Exception as e:
        console.print(f"[yellow]  评论获取失败 {bvid}: {e}[/yellow]")
    return texts[:max_count]


async def _fetch_video_danmaku(
    bvid: str,
    credential: Credential | None,
) -> list[str]:
    """获取视频弹幕文本列表。"""
    v = video.Video(bvid=bvid, credential=credential)
    texts: list[str] = []
    try:
        # 获取视频 cid（取第一个分P）
        pages = await v.get_pages()
        if not pages:
            return []
        cid = pages[0]["cid"]
        await _random_delay()

        danmakus = await video.get_danmaku(bvid=bvid, page_index=0, credential=credential)
        texts = [d.text for d in danmakus if d.text]
    except Exception as e:
        console.print(f"[yellow]  弹幕获取失败 {bvid}: {e}[/yellow]")
    return texts


async def _fetch_partition_top_videos(
    tid: int,
    partition_name: str,
    credential: Credential | None,
    top_n: int,
) -> list[VideoTexts]:
    """获取指定分区 Top N 视频的评论和弹幕。"""
    results: list[VideoTexts] = []

    try:
        # 获取分区排行榜
        rank_list = await hot.get_hot_videos(tid=tid)
        bvids = [item["bvid"] for item in rank_list[:top_n]]
    except Exception as e:
        console.print(f"[red]获取分区 {partition_name} 排行榜失败: {e}[/red]")
        return []

    console.print(f"[cyan]  分区 [{partition_name}] 共 {len(bvids)} 个视频[/cyan]")

    for i, bvid in enumerate(bvids, 1):
        console.print(f"  [{i}/{len(bvids)}] {bvid} ...", end=" ")
        try:
            comments = await _fetch_video_comments(
                bvid, credential, settings.scout_comments_per_video
            )
            danmaku = await _fetch_video_danmaku(bvid, credential)
            results.append(
                VideoTexts(
                    bvid=bvid,
                    partition=partition_name,
                    comments=comments,
                    danmaku=danmaku,
                )
            )
            console.print(
                f"[green]评论 {len(comments)} 条，弹幕 {len(danmaku)} 条[/green]"
            )
        except Exception as e:
            console.print(f"[red]失败: {e}[/red]")
        await _random_delay()

    return results


async def collect_all_partitions(
    partitions: list[tuple[str, int]] | None = None,
) -> dict[str, list[VideoTexts]]:
    """
    采集所有目标分区数据。

    返回: {partition_name: [VideoTexts, ...]}
    """
    if partitions is None:
        partitions = TARGET_PARTITIONS

    credential = _build_credential()

    # 配置代理（如果有）
    if settings.scout_proxy_url:
        request_settings.set_proxy(settings.scout_proxy_url)

    result: dict[str, list[VideoTexts]] = {}
    for partition_name, tid in partitions:
        console.print(f"\n[bold]正在采集分区: {partition_name} (tid={tid})[/bold]")
        videos = await _fetch_partition_top_videos(
            tid=tid,
            partition_name=partition_name,
            credential=credential,
            top_n=settings.scout_top_n_videos,
        )
        result[partition_name] = videos

    return result
