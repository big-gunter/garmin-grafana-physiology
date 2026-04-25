from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from anthropic import Anthropic


@dataclass(frozen=True, slots=True)
class LLMConfig:
    api_key: str
    analysis_model: str
    planning_model: str


def create_client(cfg: LLMConfig) -> Anthropic:
    return Anthropic(api_key=cfg.api_key)


def summarize_readiness(
    *,
    client: Anthropic,
    model: str,
    snapshot: dict,
    readiness: dict,
    user_prompt: str | None = None,
) -> dict[str, Any]:
    """
    Produces a concise narrative from already-derived numbers.

    Returns markdown text. The server/UI will render it into sections/cards.
    """
    system = (
        "You are a sports-science assistant. "
        "You MUST NOT invent metric values. "
        "You are given a JSON snapshot and readiness result computed from the database. "
        "The snapshot metrics are computed ONLY from raw Garmin-imported measurements (not derived rollups). "
        "IMPORTANT: Do not claim data is unavailable if it is present in snapshot.debug.available_signals. "
        "If a metric is null/missing, say it is missing. "
        "Formatting rules: do not use markdown tables. Use short headings and bullet points."
    )
    prompt = (
        "Write a concise readiness note using ONLY the provided values.\n\n"
        "Output structure:\n"
        "## Title (1 line)\n"
        "### Summary (2-4 bullets)\n"
        "### Key metrics (bullets; include numbers + units where possible; no tables)\n"
        "### Actions (3-6 bullets)\n"
        "### Caveats (0-4 bullets; mention missing metrics or assumptions)\n\n"
        f"Snapshot (DB-derived):\n{snapshot}\n\n"
        f"Readiness (DB-derived):\n{readiness}\n\n"
        f"User context:\n{user_prompt or ''}\n"
    )

    msg = client.messages.create(
        model=model,
        max_tokens=700,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    )
    parts: list[str] = []
    for c in msg.content:
        if getattr(c, "type", None) == "text":
            parts.append(c.text)
    raw = "\n".join(parts).strip()
    return raw

