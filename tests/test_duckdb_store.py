from pathlib import Path

from meme_detector.archivist import schema


def test_get_conn_initializes_schema_once_per_database_path(tmp_path, monkeypatch):
    db_path = tmp_path / "schema-once.db"
    monkeypatch.setattr(
        "meme_detector.archivist.schema.settings.duckdb_path",
        str(db_path),
    )

    db_key = str(Path(db_path).resolve())
    schema._SCHEMA_INITIALIZED_PATHS.discard(db_key)

    calls = {"count": 0}
    original_ensure_schema = schema._ensure_schema

    def counted_ensure_schema(conn):
        calls["count"] += 1
        return original_ensure_schema(conn)

    monkeypatch.setattr(
        "meme_detector.archivist.schema._ensure_schema",
        counted_ensure_schema,
    )

    conn = schema.get_conn()
    conn.close()

    conn = schema.get_conn()
    conn.close()

    assert calls["count"] == 1


def test_schema_helpers_reject_invalid_identifiers(tmp_path, monkeypatch):
    db_path = tmp_path / "schema-identifiers.db"
    monkeypatch.setattr(
        "meme_detector.archivist.schema.settings.duckdb_path",
        str(db_path),
    )

    conn = schema.get_conn()
    try:
        try:
            schema._column_exists(conn, table_name="bad-name;drop", column_name="id")
        except ValueError as exc:
            assert "invalid SQL identifier" in str(exc)
        else:
            raise AssertionError("expected ValueError for invalid table name")

        try:
            schema._rename_column_if_present(
                conn,
                table_name="scout_raw_videos",
                old_name="old-name",
                new_name="research_status",
            )
        except ValueError as exc:
            assert "invalid SQL identifier" in str(exc)
        else:
            raise AssertionError("expected ValueError for invalid column name")
    finally:
        conn.close()
