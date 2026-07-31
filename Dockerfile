############################
# STEP 1: Build with uv
############################
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.12.0 /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first (cache layer)
COPY pyproject.toml uv.lock ./

# Install dependencies only, skip building the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --no-install-project

# Copy source code and metadata
COPY README.md ./
COPY src/ src/

# Install the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

############################
# STEP 2: Runtime image
############################
FROM python:3.12-slim

# Copy the virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Set PATH to use the venv
ENV PATH="/app/.venv/bin:$PATH"

# Unbuffer stdout/stderr so the per-table progress log streams to the container
# log (central Loki) in real time instead of block-buffering until exit — the
# same reason dlt-worker sets it; without it a stall shows nothing until a
# multi-KB traceback finally flushes.
ENV PYTHONUNBUFFERED=1

# Non-root user (pinned UID/GID for K8s runAsUser/fsGroup) — matches the box
# CronJob securityContext (runAsUser 1000, runAsNonRoot).
RUN groupadd --gid 1000 iceberg \
    && useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash iceberg
USER iceberg:iceberg

WORKDIR /app

ENTRYPOINT ["python", "-m", "iceberg_maintenance"]
