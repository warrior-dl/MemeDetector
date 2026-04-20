"""
FastAPI 应用入口。
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware

from meme_detector.api.routes import router
from meme_detector.archivist.meili_store import ensure_index
from meme_detector.logging_utils import get_logger
from meme_detector.scheduler import shutdown_scheduler, start_scheduler

logger = get_logger(__name__)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Attach conservative security headers to every response.

    These headers are cheap to add and strictly defensive:
    ``X-Content-Type-Options`` blocks MIME sniffing, ``Referrer-Policy``
    limits cross-origin leakage, and ``X-Frame-Options`` prevents
    click-jacking. A baseline ``Content-Security-Policy`` is also set so
    the bundled SPA only loads same-origin resources.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; img-src 'self' data: blob: https:; "
            "style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; "
            "connect-src 'self'; frame-ancestors 'none'",
        )
        return response


def create_app() -> FastAPI:
    repo_root = Path(__file__).resolve().parents[2]
    frontend_dist = repo_root / "frontend" / "dist"

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        logger.info("api lifespan startup", extra={"event": "api_startup"})
        try:
            ensure_index()
            logger.info(
                "meilisearch index ensured", extra={"event": "meili_index_ensured"}
            )
        except Exception:
            # Meilisearch may be temporarily unreachable at boot; keep the
            # API up and let ``upsert_meme`` retry ``ensure_index`` on the
            # next write rather than failing the whole lifespan.
            logger.warning(
                "meilisearch ensure_index skipped; will retry on first write",
                extra={"event": "meili_index_ensure_failed"},
                exc_info=True,
            )
        start_scheduler()
        try:
            yield
        finally:
            shutdown_scheduler()
            logger.info("api lifespan shutdown", extra={"event": "api_shutdown"})

    app = FastAPI(
        title="MemeDetector API",
        description="B站亚文化梗实时检测与归档系统",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(SecurityHeadersMiddleware)
    app.include_router(router, prefix="/api/v1")

    if frontend_dist.exists():
        frontend_dist_resolved = frontend_dist.resolve()
        frontend_asset_dir = frontend_dist / "assets"
        frontend_asset_dir_resolved = frontend_asset_dir.resolve()

        def _safe_frontend_path(base: Path, base_resolved: Path, candidate: Path) -> Path | None:
            """Resolve ``candidate`` and make sure it stays inside ``base``.

            Returns the resolved path when it is a file inside ``base`` (after
            resolving symlinks and ``..`` segments), otherwise ``None``.
            This blocks path-traversal attempts such as ``GET /..%2F..%2Fetc/passwd``
            from escaping the static frontend directory.
            """

            try:
                resolved = (base / candidate).resolve(strict=False)
            except (OSError, RuntimeError):
                return None
            try:
                resolved.relative_to(base_resolved)
            except ValueError:
                return None
            return resolved if resolved.is_file() else None

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

            requested = _safe_frontend_path(
                frontend_dist, frontend_dist_resolved, Path(path)
            )
            if requested is not None:
                return FileResponse(requested)

            if path.startswith("assets/"):
                asset_candidate = Path(path.removeprefix("assets/"))
                assets_requested = _safe_frontend_path(
                    frontend_asset_dir,
                    frontend_asset_dir_resolved,
                    asset_candidate,
                )
                if assets_requested is not None:
                    return FileResponse(assets_requested)

            if "." in Path(path).name:
                raise HTTPException(status_code=404, detail="Frontend asset not found")
            return FileResponse(frontend_dist / "index.html")

    return app
