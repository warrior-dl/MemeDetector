"""
DuckDB 查询辅助工具。
"""

from __future__ import annotations

import re
from typing import Any

import duckdb

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def build_where_clause(where_parts: list[str]) -> str:
    return f"WHERE {' AND '.join(where_parts)}" if where_parts else ""


def count_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    from_clause: str,
    where_clause: str = "",
    params: list[Any] | None = None,
) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) FROM {from_clause} {where_clause}".strip(),
        params or [],
    ).fetchone()
    return int(row[0] or 0) if row else 0


def make_in_placeholders(values: list[object]) -> str:
    if not values:
        raise ValueError("values must not be empty")
    return ", ".join("?" for _ in values)


def quote_identifier(name: str) -> str:
    normalized = str(name).strip()
    if not _IDENTIFIER_PATTERN.fullmatch(normalized):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f'"{normalized}"'
