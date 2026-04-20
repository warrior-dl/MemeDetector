"""
视频内容背景获取与缓存。
"""

from __future__ import annotations

import asyncio
from contextlib import closing
from typing import Any

import httpx
from bilibili_api import Credential, video

from meme_detector.archivist.research_store import (
    get_video_context_cache,
    upsert_video_context_cache,
)
from meme_detector.archivist.schema import get_conn
from meme_detector.config import settings
from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)


def _build_credential() -> Credential | None:
    if settings.bilibili_sessdata:
        return Credential(
            sessdata=settings.bilibili_sessdata,
            bili_jct=settings.bilibili_bili_jct,
            buvid3=settings.bilibili_buvid3,
        )
    return None


async def get_bilibili_video_context(bvid: str) -> dict:
    """获取单个 B 站视频的内容背景，优先走缓存。"""
    normalized_bvid = bvid.strip()
    if not normalized_bvid:
        return {"status": "error", "error": "缺少有效的 BVID"}

    with closing(get_conn()) as conn:
        cached = get_video_context_cache(conn, normalized_bvid)
    if cached:
        logger.info(
            "video context cache hit",
            extra={
                "event": "video_context_cache_hit",
                "bvid": normalized_bvid,
                "status": cached.get("status", ""),
            },
        )
        return _public_video_context(cached, source="cache")

    video_url = f"https://www.bilibili.com/video/{normalized_bvid}"
    info = await _fetch_bilibili_video_info(normalized_bvid)
    title = str(info.get("title", ""))
    duration_seconds = _extract_duration_seconds(info)
    description_text = str(info.get("desc", ""))[:1200]
    logger.info(
        "video context info loaded",
        extra={
            "event": "video_context_info_loaded",
            "bvid": normalized_bvid,
            "title": title,
            "duration_seconds": duration_seconds,
        },
    )

    if duration_seconds and duration_seconds > settings.bibigpt_max_duration_seconds:
        payload = {
            "bvid": normalized_bvid,
            "video_url": video_url,
            "title": title,
            "status": "skipped",
            "duration_seconds": duration_seconds,
            "summary": "",
            "description_text": description_text,
            "content_text": "",
            "transcript_excerpt": "",
            "chapters": [],
            "raw_payload": {},
            "skip_reason": "duration_exceeded",
            "source": "local",
        }
        _save_cache(payload)
        logger.info(
            "video context skipped because duration exceeded",
            extra={
                "event": "video_context_duration_skipped",
                "bvid": normalized_bvid,
                "duration_seconds": duration_seconds,
            },
        )
        return _public_video_context(payload, source="cache")

    if not settings.bibigpt_api_token:
        logger.warning(
            "video context unavailable because BibiGPT token is missing",
            extra={
                "event": "video_context_missing_api_token",
                "bvid": normalized_bvid,
            },
        )
        return _public_video_context(
            {
            "bvid": normalized_bvid,
            "video_url": video_url,
            "title": title,
            "status": "unavailable",
            "duration_seconds": duration_seconds,
            "summary": "",
            "description_text": description_text,
            "content_text": "",
            "transcript_excerpt": "",
            "chapters": [],
            "raw_payload": {},
            "skip_reason": "missing_api_token",
            "source": "local",
            },
            source="local",
        )

    try:
        logger.info(
            "video context requesting BibiGPT summary",
            extra={
                "event": "video_context_bibigpt_requested",
                "bvid": normalized_bvid,
                "video_url": video_url,
            },
        )
        api_result = await _fetch_bibigpt_summary(video_url)
    except httpx.TimeoutException as exc:
        logger.warning(
            "video context BibiGPT timeout",
            extra={
                "event": "video_context_bibigpt_timeout",
                "bvid": normalized_bvid,
            },
        )
        return _public_video_context(
            _build_bibigpt_error_context(
                bvid=normalized_bvid,
                video_url=video_url,
                title=title,
                duration_seconds=duration_seconds,
                description_text=description_text,
                skip_reason="bibigpt_timeout",
                error=_describe_http_error(exc),
            ),
            source="local",
        )
    except httpx.HTTPError as exc:
        logger.warning(
            "video context BibiGPT request failed",
            extra={
                "event": "video_context_bibigpt_request_failed",
                "bvid": normalized_bvid,
            },
        )
        return _public_video_context(
            _build_bibigpt_error_context(
                bvid=normalized_bvid,
                video_url=video_url,
                title=title,
                duration_seconds=duration_seconds,
                description_text=description_text,
                skip_reason="bibigpt_request_failed",
                error=_describe_http_error(exc),
            ),
            source="local",
        )

    normalized = _normalize_bibigpt_payload(
        bvid=normalized_bvid,
        video_url=video_url,
        title=title,
        duration_seconds=duration_seconds,
        payload=api_result,
    )
    _save_cache(normalized)
    logger.info(
        "video context BibiGPT summary ready",
        extra={
            "event": "video_context_bibigpt_ready",
            "bvid": normalized_bvid,
            "status": normalized.get("status", ""),
            "chapter_count": len(normalized.get("chapters", [])),
        },
    )
    return _public_video_context(normalized, source="bibigpt")


async def _fetch_bilibili_video_info(bvid: str) -> dict:
    v = video.Video(bvid=bvid, credential=_build_credential())
    try:
        return await v.get_info()
    except Exception as exc:
        return {"title": "", "desc": "", "duration": None, "error": str(exc)}


def _extract_duration_seconds(info: dict) -> int | None:
    duration = info.get("duration")
    if isinstance(duration, int):
        return duration
    if isinstance(duration, float):
        return int(duration)

    pages = info.get("pages")
    if isinstance(pages, list) and pages:
        page_duration = pages[0].get("duration")
        if isinstance(page_duration, int):
            return page_duration
    return None


async def _fetch_bibigpt_summary(video_url: str) -> dict:
    from meme_detector.http_client import ClientProfile, get_async_client

    retries = max(settings.bibigpt_request_retries, 0)
    timeout = httpx.Timeout(settings.bibigpt_request_timeout_seconds)
    last_error: httpx.RequestError | None = None
    client = get_async_client(
        ClientProfile(config_key="miner.bibigpt", timeout=timeout)
    )
    for attempt in range(retries + 1):
        try:
            resp = await client.post(
                f"{settings.bibigpt_base_url}/v1/summarizeWithConfig",
                headers={"Authorization": f"Bearer {settings.bibigpt_api_token}"},
                json={
                    "url": video_url,
                    "includeDetail": True,
                    "limitation": {
                        "maxDuration": settings.bibigpt_max_duration_seconds,
                    },
                },
            )
            if resp.status_code == 422:
                return {
                    "status": "skipped",
                    "skip_reason": "duration_exceeded",
                    "raw_payload": resp.json(),
                }

            resp.raise_for_status()
            return resp.json()
        except httpx.RequestError as exc:
            last_error = exc
            if attempt >= retries:
                raise
            await asyncio.sleep(min(1.5 * (attempt + 1), 5.0))

    if last_error is not None:
        raise last_error
    raise RuntimeError("BibiGPT summary request exited without a response")


def _normalize_bibigpt_payload(
    *,
    bvid: str,
    video_url: str,
    title: str,
    duration_seconds: int | None,
    payload: dict,
) -> dict:
    if payload.get("status") == "skipped":
        return {
            "bvid": bvid,
            "video_url": video_url,
            "title": title,
            "status": "skipped",
            "duration_seconds": duration_seconds,
            "summary": "",
            "description_text": "",
            "content_text": "",
            "transcript_excerpt": "",
            "chapters": [],
            "raw_payload": payload.get("raw_payload", payload),
            "skip_reason": payload.get("skip_reason", ""),
            "source": "bibigpt",
        }

    detail = payload.get("detail", {})
    chapters = _extract_chapters(detail)
    transcript_excerpt = _extract_transcript_excerpt(detail)
    summary = str(payload.get("summary", "") or detail.get("summary", ""))
    return {
        "bvid": bvid,
        "video_url": video_url,
        "title": str(detail.get("title") or title),
        "status": "ready",
        "duration_seconds": int(detail.get("duration") or duration_seconds or 0) or None,
        "summary": summary[:4000],
        "description_text": str(detail.get("descriptionText", ""))[:2000],
        "content_text": str(detail.get("contentText", ""))[:3000],
        "transcript_excerpt": transcript_excerpt,
        "chapters": chapters,
        "raw_payload": payload,
        "skip_reason": "",
        "source": "bibigpt",
    }


def _extract_chapters(detail: dict[str, Any]) -> list[dict]:
    chapters = detail.get("chapters")
    if not isinstance(chapters, list):
        return []
    results: list[dict] = []
    for chapter in chapters[:8]:
        if not isinstance(chapter, dict):
            continue
        results.append(
            {
                "timestamp": chapter.get("timestamp"),
                "title": str(chapter.get("title", ""))[:120],
                "summary": str(chapter.get("summary", ""))[:240],
            }
        )
    return results


def _extract_transcript_excerpt(detail: dict[str, Any]) -> str:
    subtitles = detail.get("subtitlesArray")
    if not isinstance(subtitles, list):
        return ""

    parts: list[str] = []
    current_length = 0
    for item in subtitles[:20]:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text", "")).strip()
        if not text:
            continue
        remaining = 2500 - current_length
        if remaining <= 0:
            break
        parts.append(text[:remaining])
        current_length += len(parts[-1])
    return " ".join(parts)


def _build_bibigpt_error_context(
    *,
    bvid: str,
    video_url: str,
    title: str,
    duration_seconds: int | None,
    description_text: str,
    skip_reason: str,
    error: str,
) -> dict:
    return {
        "bvid": bvid,
        "video_url": video_url,
        "title": title,
        "status": "error",
        "duration_seconds": duration_seconds,
        "summary": "",
        "description_text": description_text,
        "content_text": "",
        "transcript_excerpt": "",
        "chapters": [],
        "raw_payload": {},
        "skip_reason": skip_reason,
        "error": error,
        "source": "local",
    }


def _describe_http_error(exc: httpx.HTTPError) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}: {exc.response.text[:200]}"
    return str(exc) or exc.__class__.__name__


def _save_cache(payload: dict) -> None:
    with closing(get_conn()) as conn:
        upsert_video_context_cache(
            conn,
            bvid=payload["bvid"],
            video_url=payload["video_url"],
            title=payload.get("title", ""),
            status=payload.get("status", "ready"),
            duration_seconds=payload.get("duration_seconds"),
            summary=payload.get("summary", ""),
            description_text=payload.get("description_text", ""),
            content_text=payload.get("content_text", ""),
            transcript_excerpt=payload.get("transcript_excerpt", ""),
            chapters=payload.get("chapters", []),
            raw_payload=payload.get("raw_payload", {}),
            skip_reason=payload.get("skip_reason", ""),
        )


def _public_video_context(payload: dict, *, source: str) -> dict:
    return {
        "bvid": payload.get("bvid", ""),
        "video_url": payload.get("video_url", ""),
        "title": payload.get("title", ""),
        "status": payload.get("status", ""),
        "duration_seconds": payload.get("duration_seconds"),
        "summary": payload.get("summary", ""),
        "description_text": payload.get("description_text", ""),
        "content_text": payload.get("content_text", ""),
        "transcript_excerpt": payload.get("transcript_excerpt", ""),
        "chapters": payload.get("chapters", []),
        "skip_reason": payload.get("skip_reason", ""),
        "error": payload.get("error", ""),
        "updated_at": payload.get("updated_at"),
        "source": source,
    }
