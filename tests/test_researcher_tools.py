import pytest

from meme_detector.researcher import tools


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeAsyncClient:
    def __init__(self, recorder: list[dict], payload: dict, **kwargs):
        self._recorder = recorder
        self._payload = payload
        self._kwargs = kwargs

    async def __aenter__(self):
        self._recorder.append({"client_kwargs": self._kwargs})
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url: str, *, headers: dict, content: bytes):
        self._recorder.append(
            {
                "url": url,
                "headers": headers,
                "content": content.decode("utf-8"),
            }
        )
        return _FakeResponse(self._payload)


@pytest.mark.asyncio
async def test_volcengine_web_search_uses_volcengine_api_key(monkeypatch):
    recorder: list[dict] = []
    payload = {
        "Result": {
            "WebResults": [
                {
                    "Title": "依托答辩是什么",
                    "Url": "https://example.com/post",
                    "Summary": "这是总结",
                    "SiteName": "测试站点",
                    "AuthInfoDes": "权威来源",
                }
            ]
        }
    }

    monkeypatch.setattr(
        "meme_detector.researcher.tools.settings.web_search_api_key",
        "test-key",
    )
    monkeypatch.setattr(
        "meme_detector.researcher.tools.httpx.AsyncClient",
        lambda **kwargs: _FakeAsyncClient(recorder, payload, **kwargs),
    )

    results = await tools.volcengine_web_search("依托答辩 梗 来源", num_results=3)

    assert results == [
        {
            "title": "依托答辩是什么",
            "link": "https://example.com/post",
            "snippet": "这是总结",
            "site_name": "测试站点",
            "auth_info": "权威来源",
        }
    ]
    assert recorder[0]["client_kwargs"]["timeout"] == tools.settings.web_search_timeout_seconds
    assert recorder[1]["url"] == tools._VOLCENGINE_API_KEY_URL
    assert recorder[1]["headers"]["Authorization"] == "Bearer test-key"
    assert '"SearchType": "web"' in recorder[1]["content"]
    assert '"QueryRewrite": true' in recorder[1]["content"]


@pytest.mark.asyncio
async def test_volcengine_web_search_summary_uses_summary_mode(monkeypatch):
    recorder: list[dict] = []
    payload = {
        "Result": {
            "WebResults": [
                {
                    "Title": "依托答辩词条",
                    "Url": "https://example.com/wiki",
                    "Summary": "词条摘要",
                    "Content": "更长的总结正文",
                    "SiteName": "站点A",
                    "PublishTime": "2025-06-19T15:10:00+08:00",
                    "RankScore": 0.95,
                }
            ],
        }
    }

    monkeypatch.setattr(
        "meme_detector.researcher.tools.settings.web_search_api_key",
        "test-key",
    )
    monkeypatch.setattr(
        "meme_detector.researcher.tools.httpx.AsyncClient",
        lambda **kwargs: _FakeAsyncClient(recorder, payload, **kwargs),
    )

    result = await tools.volcengine_web_search_summary("依托答辩 梗 来源", num_results=3)

    assert result == {
        "summary": "词条摘要",
        "results": [
            {
                "title": "依托答辩词条",
                "link": "https://example.com/wiki",
                "snippet": "词条摘要",
                "content": "更长的总结正文",
                "site_name": "站点A",
                "auth_info": "",
                "publish_time": "2025-06-19T15:10:00+08:00",
                "rank_score": 0.95,
            }
        ],
    }
    assert '"SearchType": "web_summary"' in recorder[1]["content"]


@pytest.mark.asyncio
async def test_volcengine_web_search_summary_supports_flat_result_payload(monkeypatch):
    recorder: list[dict] = []
    payload = {
        "Title": "北京攻略",
        "Url": "https://example.com/beijing",
        "Summary": "北京五日游总结",
        "Content": "北京五日游详细内容",
        "PublishTime": "2025-06-19T15:10:00+08:00",
        "RankScore": 0.88,
        "SearchType": "web_summary",
    }

    monkeypatch.setattr(
        "meme_detector.researcher.tools.settings.web_search_api_key",
        "test-key",
    )
    monkeypatch.setattr(
        "meme_detector.researcher.tools.httpx.AsyncClient",
        lambda **kwargs: _FakeAsyncClient(recorder, payload, **kwargs),
    )

    result = await tools.volcengine_web_search_summary("北京最新游玩攻略", num_results=2)

    assert result == {
        "summary": "北京五日游总结",
        "results": [
            {
                "title": "北京攻略",
                "link": "https://example.com/beijing",
                "snippet": "北京五日游总结",
                "content": "北京五日游详细内容",
                "site_name": "",
                "auth_info": "",
                "publish_time": "2025-06-19T15:10:00+08:00",
                "rank_score": 0.88,
            }
        ],
    }


@pytest.mark.asyncio
async def test_volcengine_web_search_returns_error_without_credentials(monkeypatch):
    monkeypatch.setattr("meme_detector.researcher.tools.settings.web_search_api_key", "")

    results = await tools.volcengine_web_search("依托答辩")

    assert results == [
        {
            "error": "WEB_SEARCH_API_KEY 未配置，跳过 Web 搜索"
        }
    ]

    summary = await tools.volcengine_web_search_summary("依托答辩")
    assert summary == {"error": "WEB_SEARCH_API_KEY 未配置，跳过 Web 搜索"}
