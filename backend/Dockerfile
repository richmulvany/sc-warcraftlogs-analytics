# Multi-stage image for the SC Analytics chatbot FastAPI backend.
#
# Build:   docker build -f backend/Dockerfile -t sc-analytics-chatbot .
# Run:     docker run --rm -p 8000:8000 --env-file backend/.env sc-analytics-chatbot
# Healthz: curl localhost:8000/healthz
#
# The image is intentionally minimal — only backend/ and scripts/ are copied.
# The semantic registry is regenerated at build time so the contracts and the
# baked-in registry can never drift in production.

FROM python:3.12-slim AS build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install system deps that databricks-sql-connector needs at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first so the layer caches across code edits.
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --prefix=/install -r /app/backend/requirements.txt
# pyyaml is required by scripts/build_semantic_registry.py at build time.
RUN pip install --prefix=/install "pyyaml>=6.0"

# Copy code needed both at build time (registry generation) and at runtime.
COPY backend /app/backend
COPY scripts /app/scripts
COPY pipeline/contracts /app/pipeline/contracts

# Regenerate the semantic registry from the current contracts. This guarantees
# the bundled JSON matches the YAML — no chance of stale manifests in prod.
# Use the prefix install via PYTHONPATH so build-time deps are visible.
ENV PYTHONPATH=/app:/install/lib/python3.12/site-packages
RUN python /app/scripts/build_semantic_registry.py


FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PORT=8000 \
    UVICORN_WORKERS=2 \
    UVICORN_TIMEOUT_KEEP_ALIVE=75

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1001 --shell /sbin/nologin sc \
    && mkdir -p /app && chown -R sc:sc /app

WORKDIR /app
USER sc

COPY --from=build --chown=sc:sc /install /usr/local
COPY --from=build --chown=sc:sc /app/backend /app/backend

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:${PORT}/healthz || exit 1

# Use exec form so signals (SIGTERM) propagate cleanly during Azure rollouts.
CMD ["sh", "-c", "uvicorn backend.app.main:app --host 0.0.0.0 --port ${PORT} --workers ${UVICORN_WORKERS} --timeout-keep-alive ${UVICORN_TIMEOUT_KEEP_ALIVE}"]
