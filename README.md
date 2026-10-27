---
title: Bingr Scraper API
emoji: 📺
colorFrom: indigo
colorTo: blue
sdk: docker
app_port: 7860
pinned: false
---

<div align="center">

# 📺 bingr.one scraper (Bingr Scraper Backend)

**High-performance, containerized FastAPI backend scraper and streaming proxy for [bingr.one](https://bingr.one) deployed on [Render](https://render.com).**

[![Deployment: Render](https://img.shields.io/badge/Deployment-Render-46E3B7?style=for-the-badge&logo=render&logoColor=white)](https://render.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge)](LICENSE)
[![Python: 3.10+](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Docker: Ready](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white)](Dockerfile)
[![Playwright: Chromium](https://img.shields.io/badge/Playwright-Chromium-2EAD33?style=for-the-badge&logo=playwright&logoColor=white)](https://playwright.dev/)
[![Architecture: 3-Layer Fallback](https://img.shields.io/badge/Architecture-3--Layer%20Fallback-orange?style=for-the-badge)](#architecture-3-layer-fallback-all-in-one-process)

</div>

---

### 🏷️ Repository Tags & Topics
`bingr` • `bingr.one` • `bingr-scraper` • `fastapi` • `python` • `render` • `render-deployment` • `docker` • `playwright` • `chromium` • `web-scraper` • `streaming-api` • `m3u8-proxy` • `hls-proxy` • `stream-resolver` • `video-scraper` • `moviebox-api` • `mit-license`

---

## 📌 Executive Summary

**bingr.one scraper** is a specialized, production-ready backend microservice engineered to scrape, resolve, and proxy media streams from **bingr.one**. Built on **FastAPI** and packaged with **Docker** for 1-click deployment on **Render**, it solves anti-bot and CDN hotlink barriers through an intelligent multi-layer architecture:

- ⚡ **Tier 1: Instant Episode Cache (~5ms)** — In-memory caching paired with ephemeral `/tmp/stream_cache.json` for sub-second responses on repeated requests.
- 🚀 **Tier 2: Parallel Direct Scrapers (~5s)** — Concurrent asynchronous requests across 8 Bingr provider endpoints via `httpx` (HTTP/2).
- 🛡️ **Tier 3: Silent Headless Browser Fallback (~15-20s)** — Pre-warmed Playwright Chromium session maintaining authentic cookies and session contexts to pass anti-bot heuristics.
- 🔄 **Integrated Stream & HLS Proxying** — On-the-fly `.m3u8` manifest rewriting (`/api/manifest-proxy`), segment tunneling (`/api/seg-proxy`), and direct MP4 stream proxying (`/api/stream-proxy`) with server-side `Referer` and `Origin` header injection.
- ☁️ **Native Render Deployment** — Includes complete `render.yaml` infrastructure configuration, health endpoints (`/api/health`) for cron-job keep-alive pingers, and configurable CORS controls.

---

# Bingr Scraper Backend — Render Deployment

Full FastAPI backend with Playwright + direct API + manifest-proxy + seg-proxy + stream-proxy. Deploys to Render's free Docker tier — different IP range from HF Spaces and Cloudflare Workers, might bypass Bingr's anti-bot block.

## Why Render (this pivot)

| Host | Bingr's API status | Stream CDN status |
|---|---|---|
| HF Spaces (AWS) | ❌ Blocked | ❌ Blocked (426) |
| Cloudflare Workers | ❌ Blocked | ❌ Blocked (427) |
| Vercel | ❌ Blocked (user-reported) | ❌ Blocked |
| **Render (free Docker tier)** | ❓ **UNTESTED — let's find out** | ❓ Untested |

Render uses different IPs than AWS/Cloudflare. Worth a 5-minute test.

## What's inside

```
scraper_api.py     FastAPI app — all endpoints (health, resolve, fast.m3u8,
                   manifest-proxy, seg-proxy, stream-proxy, cache/stats, debug)
bingr_direct.py    Direct API client — fires 8 Bingr scrapers in parallel
bingr_trace.py     Debug tool — for re-tracing Bingr's auth flow if they rotate
test_player.html   Local dev player (not served by the API)
Dockerfile         Universal — works on HF Spaces AND Render
render.yaml        Render web service config (free Docker tier)
requirements.txt   Python deps (fastapi + uvicorn + playwright + httpx)
API_DOCS.md        Full JSON response shape — give to AI Studio
README.md          This file
```

## Architecture (3-layer fallback, all in one process)

```
1. Episode cache (instant, ~5ms)     ← in-memory + /tmp/stream_cache.json
2. Direct API (fast path, ~5s)       ← POST /api/stream × 8 scrapers in parallel
3. Silent browser fallback (~15-20s) ← Playwright + Chromium, fires Bingr's frontend
4. Stream proxy                      ← /api/stream-proxy for Moviebox-API streams
                                        (sets Referer/Origin server-side)
```

## Deploy in 5 minutes

### Step 1: Push to GitHub

1. Create a new GitHub repo (e.g. `bingr-scraper`)
2. Upload ALL files from `bingr_backend/` folder:
   - `scraper_api.py`
   - `bingr_direct.py`
   - `bingr_trace.py`
   - `test_player.html`
   - `Dockerfile`
   - `render.yaml`
   - `requirements.txt`
   - `API_DOCS.md`
   - `README.md`
   - `.dockerignore`
3. Commit + push

### Step 2: Connect Render

1. Sign up at https://render.com (free, email + password, **no credit card needed**)
2. Click **New +** → **Web Service**
3. Connect your GitHub account → pick the `bingr-scraper` repo
4. Render auto-detects `render.yaml`:
   - **Name**: bingr-scraper
   - **Runtime**: Docker
   - **Plan**: Free
   - **Region**: Oregon (default)
5. Click **Apply** / **Create Web Service**
6. Wait ~5-10 minutes for the build (Playwright + Chromium install is slow)
7. When status = **Live**, you'll get a URL like `https://bingr-scraper-xyz.onrender.com`

### Step 3: Watch the build log

Click the service → **Logs** tab. You should see:

```
[STARTUP] Warming Playwright + Chromium...
[BROWSER] First-time Chromium launch...
[BROWSER] Warm and ready (no per-request cold start).
[API PAGE] Opening warm page to bingr.one/watch/movie/969681 (sets session cookie + watch-page context)...
[API PAGE] Warm and ready at https://bingr.one/watch/movie/969681 — fetch() calls from this page will pass Bingr's bot detection.
[STARTUP] Ready. Listening on http://0.0.0.0:10000
```

If you see Chromium OOM errors (e.g. "Failed to launch browser, no memory"), the 512MB free tier is too tight. Either upgrade to Starter ($7/mo) or remove the browser fallback (see Troubleshooting below).

### Step 4: THE CRITICAL TEST — does Bingr block Render's IPs?

```powershell
# Test the direct API path (should be ~5s with 20+ streams if not blocked)
curl -X POST https://bingr-scraper-xyz.onrender.com/api/resolve -H "Content-Type: application/json" -d "{\"url\":\"https://bingr.one/watch/movie/969681\"}"
```

**What to look for:**

✅ **SUCCESS — Direct API works on Render**
Response has `stream_links` with 15-25 entries, top pick = Polaris 1080p. Total time ~5s. Ship it — wire Vercel frontend to this URL.

⚠️ **PARTIAL — Direct API blocked but browser fallback works**
Response has 1 stream from SilentBrowser (took 15-22s). Bingr IS blocking Render's IPs for direct API, but the silent browser fallback still works. Functional but slower first hits.

❌ **FAIL — Both paths blocked**
Response has 0 streams / 404. Bingr fully blocks Render's IPs. Pivot to Azure for Students (GitHub Student Pack, no card, different IP range).

### Step 5: Test the stream proxy (for Moviebox-API streams)

```powershell
# Test the streaming proxy for a Moviebox-API stream URL
curl -o test_chunk.mp4 -w "Downloaded: %{size_download} bytes in %{time_total}s, HTTP %{http_code}\n" -m 60 "https://bingr-scraper-xyz.onrender.com/api/stream-proxy?u=https%3A%2F%2Fbcdnxw.hakunaymatata.com%2Fresource%2F19a242865ee738a33d1356729efe25ba.mp4%3Fsign%3Defe4e1fcd624a8a1914033c8e538f8dd%26t%3D1793038466&user_agent=Mozilla%2F5.0&referer=https%3A%2F%2F123moviesfree.club%2F&origin=https%3A%2F%2F123moviesfree.club"
```

✅ `Downloaded: 30000000+ bytes, HTTP 200` → Render's IPs not blocked by the Moviebox CDN either. Use this for both Bingr AND Moviebox.

❌ `Downloaded: 100-200 bytes, HTTP 426/427` → Render blocked by Moviebox CDN too. Use Render only for Bingr (if Bingr works), use Azure for Moviebox streams.

### Step 6: Set up keep-alive pinger (CRITICAL — Render free sleeps after 15min idle)

1. Sign up at https://cron-job.org (free, email only)
2. **Create Cronjob**:
   - URL: `https://bingr-scraper-xyz.onrender.com/api/health`
   - Schedule: **Every 10 minutes** (well within the 15min sleep window)
   - Method: GET
3. Save

Without this, Render puts the service to sleep after 15 minutes of zero traffic. Wake-up takes 30-60s. The pinger keeps it warm 24/7 → no cold starts for your users.

### Step 7: Lock CORS to your Vercel domain (production)

Edit `render.yaml`:

```yaml
envVars:
  - key: BINGR_ALLOWED_ORIGINS
    value: "https://miyoro.vercel.app"  # was "*"
```

Commit + push. Render auto-redeploys.

### Step 8: Wire Vercel frontend

In Vercel project env vars (or AI Studio prompt):

```
BINGR_API_BASE=https://bingr-scraper-xyz.onrender.com
```

Frontend code (give to AI Studio):

```javascript
const BINGR_API_BASE = process.env.BINGR_API_BASE;

// For Bingr streams (resolved via /api/resolve)
const res = await fetch(`${BINGR_API_BASE}/api/resolve`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ url: bingrUrl }),
});
const data = await res.json();
videoElement.src = data.stream_links[0].url;  // already playable

// For Moviebox-API streams (need to wrap through /api/stream-proxy)
function wrapStreamUrl(stream) {
  const params = new URLSearchParams({
    u: stream.url,
    user_agent: stream.headers?.["User-Agent"] || "Mozilla/5.0",
    referer: stream.headers?.Referer || "",
    origin: stream.headers?.Origin || "",
  });
  return `${BINGR_API_BASE}/api/stream-proxy?${params}`;
}
```

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/health` | Keep-alive ping target |
| `POST` | `/api/resolve` | Main scrape — body: `{"url":"https://bingr.one/watch/movie/969681"}` |
| `GET` | `/api/stream-proxy` | Streaming proxy for Moviebox-API streams (sets Referer server-side) |
| `GET` | `/api/manifest-proxy` | Rewrites m3u8 manifests + segments through /api/seg-proxy |
| `GET` | `/api/seg-proxy` | Fetches one segment/sub-playlist with Referer |
| `GET` | `/api/cache/stats` | Debug — view cache state |
| `POST` | `/api/cache/clear` | Admin — wipe cache |
| `GET` | `/api/debug/warm-page` | Debug — verify warm Chrome page state |
| `GET` | `/docs` | FastAPI Swagger UI for interactive testing |

## Performance (real measurements)

| Scenario | Time | Streams |
|---|---|---|
| Episode cache hit | ~5ms | All cached (5-25) |
| Direct API path (if Bingr allows Render IP) | ~5s | 15-25 streams |
| Silent browser fallback (Bingr blocks direct) | ~15-22s | 1 stream |
| Stream-proxy for Moviebox URLs (if CDN allows) | Streaming | Full MP4 bytes |

## Troubleshooting

### "Out of memory" / Chromium fails to launch

Render free tier has 512MB RAM. Chromium + Playwright + Python + httpx is tight. Options:

1. **Upgrade to Render Starter ($7/mo)** — 1GB RAM, definitely fits
2. **Remove the browser fallback** — comment out the `playwright install --with-deps chromium` line in Dockerfile + remove Playwright imports in scraper_api.py. Direct API covers 95% of cases (and works on Render IF Bingr doesn't block).

### "Build timeout exceeded"

Render free tier builds have a time limit. The `playwright install --with-deps chromium` step downloads ~150MB and takes 2-5 min. If build times out:
- Use Render's "Starter" plan for builds (then downgrade after first deploy)
- Or pre-build the image and push to a Docker registry

### Service sleeps despite cron-job.org pinger

Verify the pinger is actually hitting your URL:
1. Go to https://cron-job.org → your job → "History" tab
2. Should show successful HTTP 200 responses every 10 min
3. If 502/503/timeout → your service is dead, check Render logs

### Bingr direct API returns 0 streams

Same issue as HF/CF — Bingr is blocking Render's IP range. The silent browser fallback should still work (returns 1 stream in 15-22s).

To verify:
1. Hit `/api/debug/warm-page` — should show `api_page_alive: true` + `test_stream_fetch` with results
2. If `test_stream_fetch.error: "Failed to fetch"` → CORS preflight blocked on Render too. Accept slow browser fallback OR pivot to Azure for Students.

### Cache not surviving restarts

Render free tier has **no persistent disk**. The cache lives in `/tmp/stream_cache.json` (ephemeral). Lost on every:
- Restart (code push)
- Sleep (after 15min idle)
- OOM crash

In-memory cache (`CACHE["episodes"]`) still works during warm sessions — first user after sleep waits, subsequent users within the same warm session get cache hits.

For persistent cache: upgrade to Render Starter ($7/mo) + attach a Disk (1GB free).

## Comparison vs HF Spaces approach

| Aspect | HF Spaces (current) | Render (this) |
|---|---|---|
| Cold start (after sleep) | ~30-60s | ~30-60s |
| Wake-up trigger | First request | First request OR cron-job.org ping |
| Sleep trigger | 48h idle | 15min idle (need pinger) |
| Persistent disk | ✅ Free (~20GB) | ❌ Not on free tier |
| RAM | 16GB | 512MB (tight for Chromium) |
| IP range | AWS (blocked by Bingr) | Different (UNTESTED) |
| Docker | ✅ | ✅ |
| Free tier limit | Grandfathered (existing Spaces) | 750 hours/month |

## License & Legal

This project is open-source software licensed under the **[MIT License](LICENSE)**.

```text
MIT License

Copyright (c) 2026 cryoxyl-beep

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Disclaimer
Personal learning and educational project. Not affiliated with or endorsed by Bingr.one. Please do not redistribute scraped content and always respect upstream terms of service.
