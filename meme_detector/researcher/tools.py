"""
AI Agent 工具函数：B站搜索、Web搜索、URL验证。
"""

from __future__ import annotations

import httpx
from bilibili_api import search

from meme_detector.config import settings


async def bilibili_search(keyword: str, limit: int = 5) -> list[dict]:
    """
    搜索 B 站视频，返回相关视频的标题和描述。
    供 AI Agent 用于溯源。
    """
    try:
        result = await search.search_by_type(
            keyword=keyword,
            search_type=search.SearchObjectType.VIDEO,
            page=1,
        )
        videos = result.get("result", [])[:limit]
        return [
            {
                "title": (
                    v.get("title", "")
                    .replace("<em class=\"keyword\">", "")
                    .replace("</em>", "")
                ),
                "bvid": v.get("bvid", ""),
                "url": f'https://www.bilibili.com/video/{v.get("bvid", "")}',
                "description": v.get("description", "")[:200],
                "play": v.get("play", 0),
                "pubdate": v.get("pubdate", 0),
            }
            for v in videos
        ]
    except Exception as e:
        return [{"error": str(e)}]


async def web_search(query: str, num_results: int = 5) -> list[dict]:
    """
    使用 Serper API 搜索 Google，获取梗的外部背景信息。
    """
    if not settings.serper_api_key:
        return [{"error": "SERPER_API_KEY 未配置，跳过 Web 搜索"}]

    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={
                    "X-API-KEY": settings.serper_api_key,
                    "Content-Type": "application/json",
                },
                json={"q": query, "num": num_results, "gl": "cn", "hl": "zh-cn"},
            )
            resp.raise_for_status()
            data = resp.json()
            organic = data.get("organic", [])
            return [
                {
                    "title": item.get("title", ""),
                    "link": item.get("link", ""),
                    "snippet": item.get("snippet", ""),
                }
                for item in organic[:num_results]
            ]
        except Exception as e:
            return [{"error": str(e)}]


async def verify_urls(urls: list[str]) -> list[str]:
    """
    验证 URL 列表的真实性（HTTP HEAD 请求），返回有效的 URL 列表。
    过滤 404/403 等无效链接，防止 AI 幻觉产生的假来源。
    """
    valid: list[str] = []
    async with httpx.AsyncClient(
        timeout=5,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0"},
    ) as client:
        for url in urls:
            if not url.startswith("http"):
                continue
            try:
                resp = await client.head(url)
                if resp.status_code < 400:
                    valid.append(url)
            except Exception:
                pass  # 网络错误 / 超时，视为无效
    return valid
