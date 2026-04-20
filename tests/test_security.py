"""安全相关的回归测试。"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.testclient import TestClient

from meme_detector.api.routes import (
    _escape_meili_filter_value,
    _validate_meili_sort,
    router,
)


def test_escape_meili_filter_value_escapes_backslash_and_quote():
    assert _escape_meili_filter_value('a"b') == 'a\\"b'
    assert _escape_meili_filter_value("a\\b") == "a\\\\b"
    # Backslash must be escaped before quotes so escaped quotes are stable.
    assert _escape_meili_filter_value('a\\"b') == 'a\\\\\\"b'


def test_validate_meili_sort_accepts_allowed_fields():
    assert _validate_meili_sort("heat_index:desc") == "heat_index:desc"
    assert _validate_meili_sort("updated_at:asc") == "updated_at:asc"


@pytest.mark.parametrize(
    "bad_value",
    [
        "heat_index",
        "heat_index:down",
        "password:asc",
        'heat_index:desc"; DROP INDEX',
        "",
    ],
)
def test_validate_meili_sort_rejects_injection_attempts(bad_value: str):
    with pytest.raises(HTTPException):
        _validate_meili_sort(bad_value)


def _build_spa_test_app(frontend_dist: Path) -> FastAPI:
    """Build an app that mirrors the SPA catch-all logic with a test dist dir."""

    assets_dir = frontend_dist / "assets"
    resolved_dist = frontend_dist.resolve()
    resolved_assets = assets_dir.resolve()

    def _safe(base: Path, base_resolved: Path, candidate: Path) -> Path | None:
        try:
            resolved = (base / candidate).resolve(strict=False)
        except (OSError, RuntimeError):
            return None
        try:
            resolved.relative_to(base_resolved)
        except ValueError:
            return None
        return resolved if resolved.is_file() else None

    app = FastAPI()
    app.include_router(router, prefix="/api/v1")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(frontend_dist / "index.html")

    @app.get("/{path:path}", include_in_schema=False)
    async def spa(path: str) -> FileResponse:
        if not path:
            return FileResponse(frontend_dist / "index.html")
        requested = _safe(frontend_dist, resolved_dist, Path(path))
        if requested is not None:
            return FileResponse(requested)
        if path.startswith("assets/"):
            cand = Path(path.removeprefix("assets/"))
            asset = _safe(assets_dir, resolved_assets, cand)
            if asset is not None:
                return FileResponse(asset)
        if "." in Path(path).name:
            raise HTTPException(status_code=404, detail="Frontend asset not found")
        return FileResponse(frontend_dist / "index.html")

    return app


@pytest.fixture
def spa_client(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "meme_detector.archivist.duckdb_store.settings.duckdb_path",
        str(tmp_path / "security-test.db"),
    )
    monkeypatch.setattr(
        "meme_detector.archivist.duckdb_store.settings.media_asset_root",
        str(tmp_path / "assets"),
    )

    frontend_dist = tmp_path / "frontend" / "dist"
    assets_dir = frontend_dist / "assets"
    assets_dir.mkdir(parents=True)
    (frontend_dist / "index.html").write_text(
        "<html><body>MemeDetector Workbench</body></html>",
        encoding="utf-8",
    )
    (assets_dir / "bundle.js").write_text("console.log('ok')", encoding="utf-8")

    # Sibling file that path-traversal attempts might try to read.
    (tmp_path / "secret.txt").write_text("SECRET-DO-NOT-LEAK", encoding="utf-8")

    with TestClient(_build_spa_test_app(frontend_dist)) as client:
        yield client


def test_spa_catchall_blocks_path_traversal(spa_client):
    for attempt in (
        "/..%2F..%2Fsecret.txt",
        "/../secret.txt",
        "/assets/..%2F..%2Fsecret.txt",
    ):
        resp = spa_client.get(attempt)
        # Either a 404 or a fallthrough to index.html; the crucial invariant
        # is that the secret contents must never be served.
        assert "SECRET-DO-NOT-LEAK" not in resp.text


def test_spa_catchall_still_serves_real_assets(spa_client):
    resp = spa_client.get("/assets/bundle.js")
    assert resp.status_code == 200
    assert "console.log" in resp.text

    index_resp = spa_client.get("/")
    assert index_resp.status_code == 200
    assert "MemeDetector Workbench" in index_resp.text


def test_memes_list_rejects_bad_sort_by(spa_client):
    resp = spa_client.get("/api/v1/memes?sort_by=heat_index")
    assert resp.status_code == 400

    resp2 = spa_client.get("/api/v1/memes?sort_by=../../etc/passwd:asc")
    assert resp2.status_code == 400


def test_memes_list_escapes_filter_injection(monkeypatch, spa_client):
    captured: dict[str, object] = {}

    async def fake_search_memes(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return {"hits": [], "estimatedTotalHits": 0}

    monkeypatch.setattr("meme_detector.api.routes.search_memes", fake_search_memes)

    resp = spa_client.get(
        '/api/v1/memes?category=a" OR human_verified = true OR "b',
    )
    assert resp.status_code == 200
    filters = captured["kwargs"]["filters"]
    # The dangerous ``"`` must be escaped; the attacker cannot inject an
    # additional ``OR human_verified = true`` clause at filter-parse time.
    assert filters == 'category = "a\\" OR human_verified = true OR \\"b"'
