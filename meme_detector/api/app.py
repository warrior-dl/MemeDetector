"""
FastAPI 应用入口。
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from meme_detector.api.routes import router
from meme_detector.archivist.meili_store import ensure_index
from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)

def create_app() -> FastAPI:
    repo_root = Path(__file__).resolve().parents[2]
    frontend_dist = repo_root / "frontend" / "dist"

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

    app.include_router(router, prefix="/api/v1")

    if frontend_dist.exists():
        frontend_asset_dir = frontend_dist / "assets"

        @app.get("/admin", include_in_schema=False)
        async def removed_admin_index() -> None:
            raise HTTPException(status_code=404, detail="Legacy /admin UI has been removed")

        @app.get("/admin/{path:path}", include_in_schema=False)
        async def removed_admin_path(path: str) -> None:
            raise HTTPException(status_code=404, detail=f"Legacy /admin/{path} UI has been removed")

        @app.get("/workbench", include_in_schema=False)
        async def removed_workbench_index() -> None:
            raise HTTPException(status_code=404, detail="Legacy /workbench UI has been removed")

        @app.get("/workbench/{path:path}", include_in_schema=False)
        async def removed_workbench_path(path: str) -> None:
            raise HTTPException(status_code=404, detail=f"Legacy /workbench/{path} UI has been removed")

        @app.get("/candidates", include_in_schema=False)
        async def removed_candidates_index() -> None:
            raise HTTPException(status_code=404, detail="Legacy /candidates UI has been removed")

        @app.get("/candidates/{path:path}", include_in_schema=False)
        async def removed_candidates_path(path: str) -> None:
            raise HTTPException(status_code=404, detail=f"Legacy /candidates/{path} UI has been removed")

        @app.get("/", include_in_schema=False)
        async def frontend_index() -> FileResponse:
            return FileResponse(frontend_dist / "index.html")

        @app.get("/{path:path}", include_in_schema=False)
        async def frontend_spa(path: str) -> FileResponse:
            if not path:
                return FileResponse(frontend_dist / "index.html")

            requested = frontend_dist / path
            if requested.is_file():
                return FileResponse(requested)

            assets_requested = frontend_asset_dir / path.removeprefix("assets/")
            if path.startswith("assets/") and assets_requested.is_file():
                return FileResponse(assets_requested)

            if "." in Path(path).name:
                raise HTTPException(status_code=404, detail="Frontend asset not found")
            return FileResponse(frontend_dist / "index.html")

    return app
