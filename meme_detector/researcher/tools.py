"""
AI Agent 工具函数：火山引擎联网搜索、URL 验证。
"""

from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from json import JSONDecodeError

import httpx

from meme_detector.config import settings
from meme_detector.http_client import ClientProfile, get_async_client

_VOLCENGINE_TRAFFIC_TAG_HEADER = "X-Traffic-Tag"
_VOLCENGINE_TRAFFIC_TAG_VALUE = "meme_detector_researcher"
_VOLCENGINE_API_KEY_URL = "https://open.feedcoopapi.com/search_api/web_search"
_VOLCENGINE_RESPONSE_SNIPPET_LIMIT = 300
_VOLCENGINE_SSE_DONE_MARKER = "[DONE]"
_VERIFY_URL_CONCURRENCY = 8


def _verify_urls_profile() -> ClientProfile:
    return ClientProfile(
        config_key="researcher.verify_urls",
        timeout=5,
        follow_redirects=True,
        headers=(("User-Agent", "Mozilla/5.0"),),
    )


def _volcengine_profile() -> ClientProfile:
    return ClientProfile(
        config_key="researcher.volcengine",
        timeout=settings.web_search_timeout_seconds,
    )


def _build_web_search_body(query: str, count: int, search_type: str) -> dict:
    body = {
        "Query": query[:100],
        "SearchType": search_type,
        "Count": max(1, min(count, 50)),
        "QueryControl": {"QueryRewrite": True},
    }
    if search_type == "web_summary":
        body["NeedSummary"] = True
    return body


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

    client = get_async_client(_volcengine_profile())
    resp = await client.post(
        _VOLCENGINE_API_KEY_URL,
        headers=headers,
        content=body_str.encode("utf-8"),
    )
    resp.raise_for_status()
    if _is_sse_response(resp):
        sse_result = _parse_volcengine_sse_payload(resp.text, search_type)
        if "error" in sse_result:
            return sse_result
        return {"payload": sse_result["payload"], "count": count}
    try:
        payload = resp.json()
    except JSONDecodeError:
        return {"error": _format_non_json_response_error(resp, search_type)}
    if not isinstance(payload, dict):
        return {
            "error": (
                f"Volcengine WebSearch {search_type} 返回了非对象 JSON："
                f"{type(payload).__name__}"
            )
        }
    return {"payload": payload, "count": count}


def _is_sse_response(resp: httpx.Response) -> bool:
    content_type = resp.headers.get("content-type", "").lower()
    if "text/event-stream" in content_type:
        return True
    return resp.text.lstrip().startswith("data:")


def _parse_volcengine_sse_payload(text: str, search_type: str) -> dict:
    events: list[dict] = []
    malformed_count = 0
    for data in _iter_sse_data_lines(text):
        if not data or data == _VOLCENGINE_SSE_DONE_MARKER:
            continue
        try:
            event = json.loads(data)
        except JSONDecodeError:
            malformed_count += 1
            continue
        if isinstance(event, dict):
            events.append(event)

    if not events:
        message = f"Volcengine WebSearch {search_type} SSE 响应没有可解析的 JSON 事件"
        if malformed_count:
            message += f"（跳过 {malformed_count} 个非法事件）"
        return {"error": message}

    base_payload = _select_sse_base_payload(events)
    result = base_payload.setdefault("Result", {})
    if isinstance(result, dict):
        summary = _collect_sse_summary(events)
        if summary:
            result["Summary"] = summary
    return {"payload": base_payload}


def _iter_sse_data_lines(text: str):
    for line in text.splitlines():
        if line.startswith("data:"):
            yield line.removeprefix("data:").strip()


def _select_sse_base_payload(events: list[dict]) -> dict:
    for event in events:
        result = event.get("Result")
        if not isinstance(result, dict):
            continue
        web_results = result.get("WebResults")
        if isinstance(web_results, list) and web_results:
            return deepcopy(event)
    return deepcopy(events[-1])


def _collect_sse_summary(events: list[dict]) -> str:
    chunks: list[str] = []
    for event in events:
        result = event.get("Result")
        if not isinstance(result, dict):
            continue
        choices = result.get("Choices")
        if not isinstance(choices, list):
            continue
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            delta = choice.get("Delta")
            if isinstance(delta, dict):
                content = delta.get("Content")
                if isinstance(content, str):
                    chunks.append(content)
            message = choice.get("Message")
            if isinstance(message, dict):
                content = message.get("Content")
                if isinstance(content, str):
                    chunks.append(content)
    return "".join(chunks).strip()


def _format_non_json_response_error(resp: httpx.Response, search_type: str) -> str:
    content_type = resp.headers.get("content-type", "")
    text = resp.text.strip()
    snippet = text[:_VOLCENGINE_RESPONSE_SNIPPET_LIMIT] if text else "<empty body>"
    return (
        f"Volcengine WebSearch {search_type} 返回了非 JSON 响应："
        f"status={resp.status_code}, content-type={content_type or '<missing>'}, body={snippet}"
    )


async def volcengine_web_search(query: str, num_results: int = 5) -> list[dict]:
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


async def volcengine_web_search_summary(query: str, num_results: int = 5) -> dict:
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
    semaphore = asyncio.Semaphore(min(_VERIFY_URL_CONCURRENCY, max(len(urls), 1)))
    client = get_async_client(_verify_urls_profile())
    results = await asyncio.gather(
        *[_verify_url(client, semaphore=semaphore, url=url) for url in urls]
    )
    return [url for url in results if isinstance(url, str)]


async def _verify_url(
    client: httpx.AsyncClient,
    *,
    semaphore: asyncio.Semaphore,
    url: str,
) -> str | None:
    if not url.startswith("http"):
        return None

    async with semaphore:
        try:
            resp = await client.head(url)
            if resp.status_code in {405, 501}:
                resp = await client.get(url, headers={"Range": "bytes=0-0"})
            if resp.status_code < 400:
                return url
        except Exception:
            return None
    return None
