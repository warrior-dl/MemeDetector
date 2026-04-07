"""
FastAPI 应用入口。
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from meme_detector.api.routes import router
from meme_detector.archivist.meili_store import ensure_index
from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)

def create_app() -> FastAPI:
    static_dir = Path(__file__).resolve().parent / "static"

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        logger.info("api lifespan startup", extra={"event": "api_startup"})
        ensure_index()
        logger.info("meilisearch index ensured", extra={"event": "meili_index_ensured"})
        yield
        logger.info("api lifespan shutdown", extra={"event": "api_shutdown"})

    app = FastAPI(
        title="MemeDetector API",
        description="B站亚文化梗实时检测与归档系统",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.mount("/admin-assets", StaticFiles(directory=static_dir), name="admin-assets")

    @app.get("/admin", include_in_schema=False)
    async def admin_page() -> FileResponse:
        return FileResponse(static_dir / "admin.html")

    @app.get("/admin/candidates", include_in_schema=False)
    async def admin_candidates_page() -> FileResponse:
        return FileResponse(static_dir / "candidates.html")

    @app.get("/admin/candidate-sources", include_in_schema=False)
    async def admin_candidate_sources_page() -> FileResponse:
        return FileResponse(static_dir / "candidate_sources.html")

    @app.get("/admin/scout", include_in_schema=False)
    async def admin_scout_page() -> FileResponse:
        return FileResponse(static_dir / "scout.html")

    @app.get("/admin/miner", include_in_schema=False)
    async def admin_miner_page() -> FileResponse:
        return FileResponse(static_dir / "miner.html")

    @app.get("/admin/conversations", include_in_schema=False)
    async def admin_conversations_page() -> FileResponse:
        return FileResponse(static_dir / "conversations.html")

    app.include_router(router, prefix="/api/v1")
    return app
