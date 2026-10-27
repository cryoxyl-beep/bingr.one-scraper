# Bingr Scraper — Universal Dockerfile (works on HF Spaces AND Render)
# =============================================================================
# Renders on Render.com free tier (512MB RAM, 0.1 CPU) with Docker runtime.
# Also still works on Hugging Face Spaces (just push to a Space repo).
#
# Render-specific notes:
#   - Render injects $PORT env var automatically (our scraper_api.py reads it)
#   - Render free tier has NO persistent disk — cache lives in /tmp (ephemeral)
#   - Render sleeps the service after 15min idle — use cron-job.org pinger
#   - 512MB RAM is tight for Chromium; we use --no-sandbox + --disable-dev-shm-usage
#     to fit. If OOM, increase Render plan to Starter ($7/mo, 512MB→1GB) or
#     remove the browser fallback entirely (direct API covers ~95%).
# =============================================================================

FROM python:3.11-slim

# System deps that Playwright/Chromium needs at runtime.
# Required for the silent browser fallback path.
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    fonts-liberation \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium binary + system libs Playwright needs (~150MB)
RUN playwright install --with-deps chromium

# Copy app code
COPY . .

# Default env vars — Render overrides these via render.yaml envVars
ENV HOST=0.0.0.0
# PORT is set by Render automatically (injected as $PORT)
ENV PORT=7860
ENV BINGR_CACHE_FILE=/tmp/stream_cache.json
ENV BINGR_OUTPUT_FILE=/tmp/bingr_catalog_data.json
ENV PYTHONUNBUFFERED=1

# Use /tmp for cache (Render free tier has no persistent disk)
RUN mkdir -p /tmp

EXPOSE 7860

# Health check — Render uses healthCheckPath from render.yaml instead.
# This Dockerfile-level HEALTHCHECK is for HF Spaces (and other Docker hosts).
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=120s \
  CMD curl -fsS http://localhost:${PORT:-7860}/api/health || exit 1

# Single worker — required because Playwright + global browser state isn't
# multi-worker safe. FastAPI on a single uvicorn worker handles hundreds of
# concurrent requests via asyncio.
CMD ["python", "scraper_api.py"]
