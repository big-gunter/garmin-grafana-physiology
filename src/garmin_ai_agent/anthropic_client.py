from __future__ import annotations

from dataclasses import dataclass

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
    conversation_messages: list[dict[str, str]] | None = None,
) -> str:
    """
    Produces a concise narrative from already-derived numbers.

    Returns plain text (not markdown-heavy). Keep it short to avoid UI truncation.
    """
    ref = _load_reference_docs()
    has_thread = bool(conversation_messages)
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
        + (
            "Conversation threading: earlier turns may only contain the athlete's short messages. "
            "The latest user message always includes fresh JSON snapshot/readiness from the database — use it for numbers. "
            "If this is a follow-up, do NOT repeat a full daily readiness recap; answer the latest message directly unless they ask for a recap. "
            if has_thread
            else ""
        )
        + "Tone: write like a helpful coach: short, conversational, and specific. "
        "Formatting rules: avoid markdown headings (#/##/###) and avoid long bullet lists. "
        "If you include metrics, weave them into sentences or use at most 3 short lines (not nested bullets). "
        "Hard limit: keep the entire response under ~1500 characters unless the user explicitly asks for a detailed breakdown."
    )
    if ref:
        system = system + "\n\nRuntime reference docs (authoritative):\n" + ref
    athlete_line = (user_prompt or "").strip() or "(No additional text — give a brief readiness note.)"
    if has_thread:
        prompt = (
            "Continuing an ongoing chat. Use ONLY the JSON below for numeric facts (this is the current DB fetch).\n"
            "Answer the athlete's latest message directly; do not restate a full daily summary unless they ask.\n"
            "Do not use markdown headings. Prefer 2-4 short paragraphs.\n\n"
            f"Readiness (DB-derived JSON):\n{readiness}\n\n"
            f"Snapshot (DB-derived JSON):\n{snapshot}\n\n"
            f"Athlete's latest message:\n{athlete_line}\n"
        )
    else:
        prompt = (
            "Write a concise readiness note using ONLY the provided values.\n"
            "Do not use markdown headings. Prefer 2-4 short paragraphs.\n"
            "Include: what it means, what to do today, and one key caution if needed.\n"
            "Only mention a few numbers if they materially support the point.\n\n"
            f"Readiness (DB-derived JSON):\n{readiness}\n\n"
            f"Snapshot (DB-derived JSON):\n{snapshot}\n\n"
            f"Athlete message:\n{athlete_line}\n"
        )

    prior: list[dict[str, str]] = []
    if conversation_messages:
        for m in conversation_messages:
            if not isinstance(m, dict):
                continue
            role = m.get("role")
            content = m.get("content")
            if role not in {"user", "assistant"} or not isinstance(content, str) or not content.strip():
                continue
            prior.append({"role": role, "content": content.strip()})
    messages = prior + [{"role": "user", "content": prompt}]

    msg = client.messages.create(
        model=model,
        max_tokens=600 if has_thread else 450,
        system=system,
        messages=messages,
    )
    parts: list[str] = []
    for c in msg.content:
        if getattr(c, "type", None) == "text":
            parts.append(c.text)
    raw = "\n".join(parts).strip()
    return raw

