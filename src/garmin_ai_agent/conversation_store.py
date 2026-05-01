from __future__ import annotations

import json
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$")


def sanitize_session_id(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip()
    if not s or len(s) > 128 or not _SESSION_ID_RE.fullmatch(s):
        return None
    return s


@dataclass
class ConversationState:
    window_days: int
    turns: list[dict[str, str]]  # {"user": str, "assistant": str}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_conversation(path: Path) -> ConversationState | None:
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    wd = raw.get("window_days")
    if not isinstance(wd, int) or wd < 1:
        return None
    turns_raw = raw.get("turns")
    if not isinstance(turns_raw, list):
        return None
    turns: list[dict[str, str]] = []
    for item in turns_raw:
        if not isinstance(item, dict):
            continue
        u = item.get("user")
        a = item.get("assistant")
        if isinstance(u, str) and isinstance(a, str):
            turns.append({"user": u, "assistant": a})
    return ConversationState(window_days=wd, turns=turns)


def save_conversation(path: Path, state: ConversationState, *, max_pairs: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    turns = state.turns[-max_pairs:] if max_pairs > 0 else state.turns
    payload: dict[str, Any] = {
        "version": 1,
        "updated_at": _utc_now_iso(),
        "window_days": state.window_days,
        "turns": turns,
    }
    data = json.dumps(payload, ensure_ascii=False, indent=2)
    fd, tmp = tempfile.mkstemp(prefix=".conv-", dir=str(path.parent))
    try:
        with open(fd, "w", encoding="utf-8") as f:
            f.write(data)
        Path(tmp).replace(path)
    finally:
        p = Path(tmp)
        if p.exists():
            try:
                p.unlink()
            except OSError:
                pass


def delete_conversation(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except TypeError:
        if path.is_file():
            path.unlink()
