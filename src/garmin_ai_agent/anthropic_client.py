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


def summarize_readiness(
    *,
    client: Anthropic,
    model: str,
    snapshot: dict,
    readiness: dict,
    user_prompt: str | None = None,
) -> str:
    """
    Produces a concise narrative from already-derived numbers.
    """
    system = (
        "You are a sports-science assistant. "
        "You MUST NOT invent metric values. "
        "You are given a JSON snapshot and readiness result computed from the database. "
        "Explain what it likely means, call out missing inputs, and give 3-6 actionable suggestions "
        "for today and the next 2-3 days. Keep it concise."
    )
    prompt = (
        "Snapshot (DB-derived):\n"
        f"{snapshot}\n\n"
        "Readiness (DB-derived):\n"
        f"{readiness}\n\n"
        "User context:\n"
        f"{user_prompt or ''}\n"
    )

    msg = client.messages.create(
        model=model,
        max_tokens=700,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    )
    # anthropic sdk returns list of content blocks
    parts = []
    for c in msg.content:
        if getattr(c, "type", None) == "text":
            parts.append(c.text)
    return "\n".join(parts).strip()

