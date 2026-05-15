# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:0.6.17-python3.13-bookworm-slim AS build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*
RUN uv sync

# Download supercronic — container-native cron daemon (runs as non-root,
# inherits Docker env vars so INFLUXDB_* are available to cron jobs).
# Pinned to v0.2.33; update here if a newer release is needed.
ARG TARGETARCH
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSLo /supercronic \
    "https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-${TARGETARCH}" \
 && chmod +x /supercronic

FROM python:3.13-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app"

WORKDIR /app

RUN groupadd --gid 1000 appuser && useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

COPY --chown=appuser:appuser --from=build /app/.venv /app/.venv
COPY --chown=appuser:appuser src /app/
COPY --chown=appuser:appuser docs /app/docs

# ── readiness cron ──────────────────────────────────────────────────────────
COPY --from=build /supercronic /usr/local/bin/supercronic

COPY --chown=appuser:appuser deploy/scripts/ /app/scripts/

# 22:00 UTC = 08:00 AEST (Melbourne, non-DST) / 09:00 AEDT (DST).
# No args → computes yesterday + today only (idempotent, cheap).
RUN printf '0 22 * * * python /app/scripts/compute_readiness.py >> /var/log/readiness.log 2>&1\n' \
    > /app/crontab \
 && chown appuser:appuser /app/crontab

# Pre-create log so appuser can write to it without root.
RUN touch /var/log/readiness.log && chown appuser:appuser /var/log/readiness.log

COPY deploy/scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER appuser

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "-m", "garmin_grafana.garmin_fetch"]
