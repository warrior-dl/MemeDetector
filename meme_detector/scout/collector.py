"""
B站数据采集模块。
采集高热分区的 Top 视频评论和弹幕。
"""

from __future__ import annotations

import asyncio
import random
import re
import time
from dataclasses import dataclass, field

from bilibili_api import Credential, comment, rank, request_settings, video
from bilibili_api.comment import CommentResourceType, OrderType
from bilibili_api.rank import RankType

from meme_detector.config import settings
from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)

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
    title: str
    description: str
    url: str
    comments: list[str]
    tags: list[str] = field(default_factory=list)
    comment_snapshots: list[dict] = field(default_factory=list)


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
) -> list[dict]:
    """获取视频热评结构化快照列表。"""
    v = video.Video(bvid=bvid, credential=credential)
    snapshots: list[dict] = []
    page = 1
    pages_fetched = 0
    logger.info(
        "scout comment fetch started",
        extra={
            "event": "scout_comment_fetch_started",
            "bvid": bvid,
            "comment_limit": max_count,
        },
    )
    try:
        aid = v.get_aid()
        while len(snapshots) < max_count:
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
                        logger.warning(
                            label,
                            extra={
                                "event": "scout_comment_fetch_failed",
                                "bvid": bvid,
                                "risk_control": risk_control,
                                "page": page,
                            },
                            exc_info=e,
                        )
                        return snapshots[:max_count]

                    logger.warning(
                        "scout comment request retrying",
                        extra={
                            "event": "scout_comment_retrying",
                            "bvid": bvid,
                            "page": page,
                            "retry_index": retries + 1,
                            "retry_limit": settings.scout_comment_retry_times,
                            "retry_delay_seconds": round(delay, 1),
                            "risk_control": risk_control,
                        },
                    )
                    retries += 1
                    await asyncio.sleep(delay)

            replies = (result or {}).get("replies") or []
            if not replies:
                break
            pages_fetched += 1
            for reply in replies:
                content = reply.get("content", {}).get("message", "")
                if not content:
                    continue
                content_payload = reply.get("content") or {}
                pictures = content_payload.get("pictures") or []
                if not isinstance(pictures, list):
                    pictures = []
                snapshots.append(
                    {
                        "rpid": int(reply.get("rpid") or 0),
                        "root_rpid": int(reply.get("root") or 0) or None,
                        "parent_rpid": int(reply.get("parent") or 0) or None,
                        "mid": int(reply.get("mid") or 0) or None,
                        "uname": str((reply.get("member") or {}).get("uname") or ""),
                        "message": str(content),
                        "like_count": int(reply.get("like") or 0),
                        "reply_count": int(reply.get("rcount") or 0),
                        "ctime": int(reply.get("ctime") or 0) or None,
                        "pictures": [
                            picture for picture in pictures
                            if isinstance(picture, dict) and str(picture.get("img_src", "")).strip()
                        ],
                        "content": content_payload,
                        "raw_reply": reply,
                    }
                )
            page += 1
            await _random_delay()
        risk_state.note_success()
    except Exception as e:
        logger.warning(
            "scout comment fetch failed",
            extra={"event": "scout_comment_fetch_failed", "bvid": bvid},
            exc_info=e,
        )
    logger.info(
        "scout comment fetch completed",
        extra={
            "event": "scout_comment_fetch_completed",
            "bvid": bvid,
            "page": pages_fetched,
            "comment_count": len(snapshots[:max_count]),
        },
    )
    return snapshots[:max_count]


async def _fetch_partition_top_videos(
    rank_type: RankType,
    partition_name: str,
    credential: Credential | None,
    top_n: int,
) -> list[VideoTexts]:
    """获取指定分区 Top N 视频的热门评论。"""
    results: list[VideoTexts] = []
    risk_state = CommentRiskState()
    logger.info(
        "scout partition fetch started",
        extra={
            "event": "scout_partition_fetch_started",
            "partition_name": partition_name,
            "video_count": top_n,
        },
    )

    try:
        # 获取分区排行榜
        rank_result = await rank.get_rank(type_=rank_type)
        video_items = rank_result["list"][:top_n]
    except Exception as e:
        logger.error(
            "failed to fetch partition rank",
            extra={"event": "scout_partition_rank_failed", "partition_name": partition_name},
            exc_info=e,
        )
        return []

    logger.info(
        "scout partition rank fetched",
        extra={
            "event": "scout_partition_rank_fetched",
            "partition_name": partition_name,
            "video_count": len(video_items),
        },
    )

    for i, item in enumerate(video_items, 1):
        bvid = item.get("bvid", "")
        try:
            comments: list[str] = []
            tags: list[str] = []
            comment_snapshots: list[dict] = []
            current_video = video.Video(bvid=bvid, credential=credential)
            try:
                tag_rows = await current_video.get_tags()
                tags = [
                    str(tag.get("tag_name", "")).strip()
                    for tag in tag_rows
                    if isinstance(tag, dict) and str(tag.get("tag_name", "")).strip()
                ]
            except Exception as e:
                logger.warning(
                    "scout tag fetch failed",
                    extra={"event": "scout_tag_fetch_failed", "bvid": bvid, "partition_name": partition_name},
                    exc_info=e,
                )
            if risk_state.should_skip_comments():
                logger.warning(
                    "scout comments skipped due to cooldown",
                    extra={
                        "event": "scout_comment_cooldown_skip",
                        "bvid": bvid,
                        "partition_name": partition_name,
                        "remaining_cooldown_seconds": round(risk_state.remaining_cooldown(), 1),
                    },
                )
            else:
                comment_snapshots = await _fetch_video_comments(
                    bvid,
                    credential,
                    settings.scout_comments_per_video,
                    risk_state,
                )
                comments = [
                    str(snapshot.get("message", "")).strip()
                    for snapshot in comment_snapshots
                    if str(snapshot.get("message", "")).strip()
                ]
            results.append(
                VideoTexts(
                    bvid=bvid,
                    partition=partition_name,
                    title=str(item.get("title", "")),
                    description=str(item.get("description", ""))[:500],
                    url=f"https://www.bilibili.com/video/{bvid}",
                    comments=comments,
                    tags=tags,
                    comment_snapshots=comment_snapshots,
                )
            )
            logger.info(
                "scout video collected",
                extra={
                    "event": "scout_video_collected",
                    "bvid": bvid,
                    "partition_name": partition_name,
                    "video_index": i,
                    "video_total": len(video_items),
                    "comment_count": len(comments),
                    "tag_count": len(tags),
                },
            )
        except Exception as e:
            logger.error(
                "scout video collection failed",
                extra={
                    "event": "scout_video_collection_failed",
                    "bvid": bvid,
                    "partition_name": partition_name,
                    "video_index": i,
                    "video_total": len(video_items),
                },
                exc_info=e,
            )
        await _random_delay()

    logger.info(
        "scout partition fetch completed",
        extra={
            "event": "scout_partition_fetch_completed",
            "partition_name": partition_name,
            "video_count": len(results),
            "comment_count": sum(len(item.comments) for item in results),
        },
    )
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
    logger.info(
        "scout collect all partitions started",
        extra={
            "event": "scout_collect_all_partitions_started",
            "partition_count": len(partitions),
        },
    )

    credential = _build_credential()
    request_settings.set_timeout(settings.scout_request_timeout)
    request_settings.set_wbi_retry_times(max(1, settings.scout_comment_retry_times + 1))
    request_settings.set_enable_auto_buvid(True)
    request_settings.set_enable_bili_ticket(True)

    # 配置代理（如果有）
    if settings.scout_proxy_url:
        request_settings.set_proxy(settings.scout_proxy_url)
        logger.info(
            "scout proxy enabled",
            extra={"event": "scout_proxy_enabled", "proxy_url": settings.scout_proxy_url},
        )
    elif credential is None:
        logger.warning(
            "bilibili cookie not configured; comment API may trigger risk control more easily",
            extra={"event": "scout_cookie_not_configured"},
        )

    result: dict[str, list[VideoTexts]] = {}
    for partition_name, rank_type in partitions:
        logger.info(
            "scout partition collection started",
            extra={"event": "scout_partition_collection_started", "partition_name": partition_name},
        )
        videos = await _fetch_partition_top_videos(
            rank_type=rank_type,
            partition_name=partition_name,
            credential=credential,
            top_n=settings.scout_top_n_videos,
        )
        result[partition_name] = videos

    logger.info(
        "scout collect all partitions completed",
        extra={
            "event": "scout_collect_all_partitions_completed",
            "partition_count": len(result),
            "video_count": sum(len(items) for items in result.values()),
            "comment_count": sum(len(video.comments) for items in result.values() for video in items),
        },
    )
    return result
