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


def _load_reference_docs() -> str:
    """
    Best-effort load of runtime reference docs that describe domain + tool surface.
    These are mounted at /app/docs in docker-compose, and also copied into the image.
    """
    try:
        from pathlib import Path

        root = Path("/app/docs")
        parts: list[str] = []
        for name in ("agent-domain.md", "agent-tools.md"):
            p = root / name
            if p.exists():
                txt = p.read_text(encoding="utf-8", errors="replace").strip()
                if txt:
                    parts.append(f"\n\n---\nBEGIN {name}\n---\n{txt}\n---\nEND {name}\n---\n")
        return "".join(parts).strip()
    except Exception:
        return ""


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
    ref = _load_reference_docs()
    system = (
        "You are a sports-science assistant. "
        "You MUST NOT invent metric values. "
        "You are given a JSON snapshot and readiness result computed from the database. "
        "The snapshot metrics are computed ONLY from raw Garmin-imported measurements (not derived rollups). "
        "IMPORTANT: Do not claim data is unavailable if it is present in snapshot.debug.available_signals. "
        "If a metric is null/missing, say it is missing. "
        "If snapshot.metrics.agent_vo2 is present, prefer it for VO₂/VO₂max discussion because it is agent-calculated from raw activity streams "
        "(ACSM running equation and power→VO₂ cycling conversions) rather than Garmin's device-estimated VO₂ fields. "
        "If snapshot.metrics.agent_load is present, prefer it for TRIMP/TSS discussion (agent-derived from streams) rather than claiming it is unavailable. "
        "If snapshot.metrics.on_demand_metrics is present, those metrics were computed on-the-fly from raw streams in response to the user's question. "
        "Prefer answering using on_demand_metrics when the question asks for them (e.g., HR percentiles, TRIMP, TSS, VO2 window summaries) rather than saying the snapshot does not include them. "
        "Do NOT introduce medication topics (e.g., beta blockers) unless the user's prompt explicitly mentions medication/beta blocker. "
        "Tone: write like a helpful coach: 1-2 short paragraphs, then bullets for key metrics and actions. "
        "Formatting rules: do not use markdown tables. Use short headings and bullet points."
    )
    if ref:
        system = system + "\n\nRuntime reference docs (authoritative):\n" + ref
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

