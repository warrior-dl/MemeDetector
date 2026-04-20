"""按事件循环缓存的 ``httpx.AsyncClient``。

FastAPI 的调度 / agent 管线里同一 event loop 会短时间内发很多下游请求
（volcengine、bibigpt、URL 校验等）。原先的 ``async with httpx.AsyncClient(...)``
每次都新建 TCP 连接池，HTTPS 握手开销在 agent 循环里被放大。

这个模块维护 ``(event_loop, config_key)`` → ``AsyncClient`` 的缓存：

* ``AsyncClient`` 的连接池只有绑定到创建它的 event loop 才有效，所以缓存里显式
  按 loop 分桶；
* 用 ``weakref.WeakValueDictionary`` 让 loop 结束后被 GC 掉、客户端也自然释放；
* 每个调用点通过 ``config_key`` 取到自己想要的超时 / header 配置，不互相影响。

**不** 负责关闭客户端 —— 进程退出时 httpx 会由 GC 一并释放。
"""

from __future__ import annotations

import asyncio
import contextlib
import weakref
from dataclasses import dataclass, field

import httpx

_LoopClients = dict[str, httpx.AsyncClient]
_CLIENT_REGISTRY: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop, _LoopClients
] = weakref.WeakKeyDictionary()


@dataclass(frozen=True)
class ClientProfile:
    """描述一种 httpx 客户端的配置。用 ``config_key`` 做缓存键。"""

    config_key: str
    timeout: float | httpx.Timeout | None = None
    follow_redirects: bool = False
    headers: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    def build(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=self.follow_redirects,
            headers=dict(self.headers) if self.headers else None,
        )


def get_async_client(profile: ClientProfile) -> httpx.AsyncClient:
    """返回绑定到当前 event loop 的 ``AsyncClient``；首次调用时按 profile 懒建。"""

    loop = asyncio.get_running_loop()
    clients = _CLIENT_REGISTRY.get(loop)
    if clients is None:
        clients = {}
        _CLIENT_REGISTRY[loop] = clients

    existing = clients.get(profile.config_key)
    if existing is not None and not existing.is_closed:
        return existing

    client = profile.build()
    clients[profile.config_key] = client
    return client


async def aclose_all() -> None:
    """关闭当前 event loop 下所有缓存的客户端。主要用于测试或 graceful shutdown。"""

    loop = asyncio.get_running_loop()
    clients = _CLIENT_REGISTRY.pop(loop, None)
    if not clients:
        return
    for client in clients.values():
        # pragma: no cover - best effort
        with contextlib.suppress(Exception):
            await client.aclose()
