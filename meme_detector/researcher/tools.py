"""
AI Agent 工具函数：B站搜索、Web搜索、URL验证。
"""

from __future__ import annotations

import json

import httpx
from bilibili_api import search

from meme_detector.config import settings

_VOLCENGINE_TRAFFIC_TAG_HEADER = "X-Traffic-Tag"
_VOLCENGINE_TRAFFIC_TAG_VALUE = "meme_detector_researcher"
_VOLCENGINE_API_KEY_URL = "https://open.feedcoopapi.com/search_api/web_search"


def _build_web_search_body(query: str, count: int, search_type: str) -> dict:
    return {
        "Query": query[:100],
        "SearchType": search_type,
        "Count": max(1, min(count, 50)),
        "QueryControl": {"QueryRewrite": True},
    }


def _parse_volcengine_web_results(data: dict, count: int) -> list[dict]:
    error = (data.get("ResponseMetadata") or {}).get("Error")
    if error:
        code = error.get("Code", "")
        message = error.get("Message", "") or "unknown error"
        return [{"error": f"Volcengine WebSearch error [{code}]: {message}"}]

    results = ((data.get("Result") or {}).get("WebResults") or [])[:count]
    return [
        {
            "title": item.get("Title", ""),
            "link": item.get("Url", ""),
            "snippet": item.get("Summary") or item.get("Snippet", ""),
            "site_name": item.get("SiteName", ""),
            "auth_info": item.get("AuthInfoDes", ""),
        }
        for item in results
    ]


def _parse_volcengine_summary_result(data: dict, count: int) -> dict:
    error = (data.get("ResponseMetadata") or {}).get("Error")
    if error:
        code = error.get("Code", "")
        message = error.get("Message", "") or "unknown error"
        return {"error": f"Volcengine WebSearch error [{code}]: {message}"}

    result = data.get("Result") or data
    web_results = []
    if isinstance(result, dict):
        nested_results = result.get("WebResults")
        if isinstance(nested_results, list):
            web_results = nested_results[:count]
        elif result.get("Title") or result.get("Url"):
            web_results = [result]

    candidate_keys = (
        "Summary",
        "SummaryText",
        "SummaryContent",
        "Answer",
        "Content",
        "FinalSummary",
    )
    summary = ""
    if isinstance(result, dict):
        for key in candidate_keys:
            value = result.get(key)
            if isinstance(value, str) and value.strip():
                summary = value.strip()
                break
    if not summary:
        for item in web_results:
            for key in ("Summary", "Content", "Snippet"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    summary = value.strip()
                    break
            if summary:
                break

    return {
        "summary": summary,
        "results": [
            {
                "title": item.get("Title", ""),
                "link": item.get("Url", ""),
                "snippet": item.get("Summary") or item.get("Snippet", ""),
                "content": item.get("Content", ""),
                "site_name": item.get("SiteName", ""),
                "auth_info": item.get("AuthInfoDes", ""),
                "publish_time": item.get("PublishTime", ""),
                "rank_score": item.get("RankScore"),
            }
            for item in web_results
        ],
    }


async def _call_volcengine_search(query: str, num_results: int, search_type: str) -> dict:
    normalized_query = str(query).strip()
    if not normalized_query:
        return {"error": "搜索词为空，跳过 Web 搜索"}

    count = max(1, min(int(num_results or 5), 50))
    body = _build_web_search_body(normalized_query, count, search_type)
    body_str = json.dumps(body, ensure_ascii=False)

    api_key = settings.web_search_api_key.strip()
    if not api_key:
        return {"error": "WEB_SEARCH_API_KEY 未配置，跳过 Web 搜索"}

    headers = {
        "Content-Type": "application/json",
        _VOLCENGINE_TRAFFIC_TAG_HEADER: _VOLCENGINE_TRAFFIC_TAG_VALUE,
        "Authorization": f"Bearer {api_key}",
    }

    async with httpx.AsyncClient(timeout=settings.web_search_timeout_seconds) as client:
        resp = await client.post(
            _VOLCENGINE_API_KEY_URL,
            headers=headers,
            content=body_str.encode("utf-8"),
        )
        resp.raise_for_status()
        return {"payload": resp.json(), "count": count}


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
    使用火山引擎联网搜索，获取梗的外部背景信息。
    """
    try:
        result = await _call_volcengine_search(query, num_results, "web")
        if "error" in result:
            return [{"error": result["error"]}]
        return _parse_volcengine_web_results(result["payload"], result["count"])
    except Exception as e:
        return [{"error": str(e)}]


async def web_search_summary(query: str, num_results: int = 5) -> dict:
    """
    使用火山引擎总结版联网搜索，优先获取 AI 总结和相关来源。
    """
    try:
        result = await _call_volcengine_search(query, num_results, "web_summary")
        if "error" in result:
            return {"error": result["error"]}
        return _parse_volcengine_summary_result(result["payload"], result["count"])
    except Exception as e:
        return {"error": str(e)}


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
