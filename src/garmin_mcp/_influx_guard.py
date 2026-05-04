from __future__ import annotations

import re

_WRITEISH = re.compile(
    r"\b(?:drop|delete|insert|into|create|alter|grant|revoke|show\s+users|set\s+password)\b",
    re.IGNORECASE,
)


def assert_readonly_influxql(q: str) -> None:
    s = (q or "").strip()
    if not s:
        raise ValueError("Empty InfluxQL query")
    if _WRITEISH.search(s):
        raise ValueError("Query rejected: write or admin-like InfluxQL is not allowed")


def clamp_rows(n: int, *, default: int = 500, cap: int = 5000) -> int:
    if n <= 0:
        return default
    return min(n, cap)
