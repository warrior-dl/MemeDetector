"""从 DuckDB 拉评论语料。不做任何过滤（设计文档 Q2：全量拉取）。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import duckdb


@dataclass(frozen=True)
class Comment:
    comment_id: str  # rpid as str
    bvid: str
    mid: str
    uname: str
    text: str
    ctime: datetime | None

    @property
    def key(self) -> tuple[str, str]:
        return (self.comment_id, self.bvid)


def load_comments(db_path: str, limit: int | None = None) -> list[Comment]:
    """拉全量 scout_raw_comments。

    - 不限长度、不过滤内容
    - 去重 (rpid, bvid)
    - ``limit`` 仅用于小规模调试 / smoke test
    """
    conn = duckdb.connect(db_path, read_only=True)
    try:
        sql = """
            SELECT
                CAST(rpid AS VARCHAR) AS comment_id,
                bvid,
                CAST(COALESCE(mid, 0) AS VARCHAR) AS mid,
                COALESCE(uname, '') AS uname,
                COALESCE(message, '') AS text,
                ctime
            FROM scout_raw_comments
            WHERE message IS NOT NULL
        """
        params: list[object] = []
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    comments: list[Comment] = []
    seen: set[tuple[str, str]] = set()
    for r in rows:
        key = (r[0], r[1])
        if key in seen:
            continue
        seen.add(key)
        comments.append(
            Comment(
                comment_id=r[0],
                bvid=r[1],
                mid=r[2],
                uname=r[3],
                text=r[4],
                ctime=r[5],
            )
        )
    return comments
