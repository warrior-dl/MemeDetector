"""
FastAPI 应用入口。
"""

from fastapi import FastAPI

from meme_detector.api.routes import router
from meme_detector.archivist.meili_store import ensure_index


def create_app() -> FastAPI:
    app = FastAPI(
        title="MemeDetector API",
        description="B站亚文化梗实时检测与归档系统",
        version="0.1.0",
    )

    @app.on_event("startup")
    async def on_startup() -> None:
        ensure_index()

    app.include_router(router, prefix="/api/v1")
    return app
