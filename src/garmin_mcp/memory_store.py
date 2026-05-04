from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class FindingRow:
    id: int
    created_at: str
    summary: str
    detail: str
    tags: str | None


class AgentMemoryStore:
    """SQLite sidecar for agent conclusions (no Influx writes)."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, isolation_level=None)
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS findings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    detail TEXT NOT NULL DEFAULT '',
                    tags TEXT
                )
                """
            )
            con.execute("CREATE INDEX IF NOT EXISTS idx_findings_created ON findings(created_at)")

    def append(self, summary: str, detail: str = "", tags: str | None = None) -> int:
        s = (summary or "").strip()
        if not s:
            raise ValueError("summary is required")
        d = (detail or "").strip()
        t = (tags or "").strip() or None
        with self._connect() as con:
            cur = con.execute(
                "INSERT INTO findings (created_at, summary, detail, tags) VALUES (?, ?, ?, ?)",
                (_utc_iso(), s, d, t),
            )
            return int(cur.lastrowid)

    def recent(self, limit: int = 20) -> list[FindingRow]:
        lim = max(1, min(limit, 100))
        with self._connect() as con:
            rows = con.execute(
                "SELECT id, created_at, summary, detail, tags FROM findings ORDER BY id DESC LIMIT ?",
                (lim,),
            ).fetchall()
        return [
            FindingRow(
                id=int(r["id"]),
                created_at=str(r["created_at"]),
                summary=str(r["summary"]),
                detail=str(r["detail"] or ""),
                tags=r["tags"],
            )
            for r in rows
        ]

    def search(self, query: str, limit: int = 20) -> list[FindingRow]:
        q = (query or "").strip()
        lim = max(1, min(limit, 100))
        if not q:
            return self.recent(limit)
        pat = f"%{q}%"
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT id, created_at, summary, detail, tags FROM findings
                WHERE summary LIKE ? OR detail LIKE ? OR IFNULL(tags,'') LIKE ?
                ORDER BY id DESC LIMIT ?
                """,
                (pat, pat, pat, lim),
            ).fetchall()
        return [
            FindingRow(
                id=int(r["id"]),
                created_at=str(r["created_at"]),
                summary=str(r["summary"]),
                detail=str(r["detail"] or ""),
                tags=r["tags"],
            )
            for r in rows
        ]

    def get(self, finding_id: int) -> FindingRow | None:
        with self._connect() as con:
            r = con.execute(
                "SELECT id, created_at, summary, detail, tags FROM findings WHERE id = ?",
                (finding_id,),
            ).fetchone()
        if r is None:
            return None
        return FindingRow(
            id=int(r["id"]),
            created_at=str(r["created_at"]),
            summary=str(r["summary"]),
            detail=str(r["detail"] or ""),
            tags=r["tags"],
        )


def default_memory_db_path() -> Path:
    import os

    explicit = (os.getenv("GARMIN_AGENT_MEMORY_DB") or "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    d = (os.getenv("GARMIN_AGENT_MEMORY_DIR") or "").strip()
    base = Path(d).expanduser().resolve() if d else Path.cwd() / "data" / "agent-memory"
    return (base / "findings.sqlite").resolve()
