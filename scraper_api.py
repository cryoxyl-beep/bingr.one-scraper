#!/usr/bin/env python3
"""
Bingr Scraper — FastAPI Backend (Production Edition)
=====================================================
Refactored from the raw asyncio.start_server version into a proper FastAPI
service. Runs locally with `python scraper_api.py` (no Docker needed for
testing) AND on Hugging Face Spaces (Dockerized).

Speed wins vs the old asyncio version:
  1. Browser warmed ONCE at startup (lifespan event) — no 3-5s Chromium spawn
     on every request that hits Method 2.
  2. Proper keep-alive HTTP — no TCP re-handshake per request.
  3. Episode cache stores fast_manifest_text per episode key — the m3u8
     endpoint returns instantly without recomputing.
  4. CORS preflight handled by Starlette middleware (was broken before).
  5. POST body parsing (was impossible before — only worked with query strings).

Endpoints:
  GET  /api/health           -> {status: "ok", uptime, cache_size, browser}
  POST /api/resolve           -> JSON scrape result for a bingr URL
       body: {"url": "https://bingr.one/watch/tv/1396/1/1"}
  GET  /api/fast.m3u8?ep=KEY  -> cached stripped m3u8 manifest (CORS-enabled)
  GET  /api/manifest-proxy    -> fetch upstream m3u8 with referer, rewrite
                                 segment URLs through /api/seg-proxy, return
                                 rewritten manifest with CORS headers.
                                 This is what makes anime streams actually
                                 play — browsers can't set Referer, but our
                                 backend can.
  GET  /api/seg-proxy        -> fetch a segment with referer, stream bytes
                                 back to the browser with CORS headers.
  GET  /api/cache/stats       -> cache statistics (debug)
  POST /api/cache/clear       -> wipe cache (admin)
"""

import asyncio
import json
import os
import re
import time
import urllib.parse
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel
from playwright.async_api import async_playwright, Browser, BrowserContext
import httpx  # used by /api/stream-proxy for streaming video through Referer/Origin

# Direct API client — no browser needed. Discovered Bingr exposes a public
# POST /api/stream endpoint that returns m3u8 URLs without auth. We fire all
# 8 known scrapers (s31, s40, s50, s51, s62, s70, s80, s81) in parallel.
# Falls back to browser if direct API returns nothing (edge cases).
from bingr_direct import resolve_via_api

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CACHE_FILE = os.environ.get("BINGR_CACHE_FILE", "stream_cache.json")
OUTPUT_FILE = os.environ.get("BINGR_OUTPUT_FILE", "bingr_catalog_data.json")
SERVER_PORT = int(os.environ.get("PORT", "8765"))
SERVER_HOST = os.environ.get("HOST", "0.0.0.0")  # 0.0.0.0 so HF Spaces reverse-proxy can reach us

# CORS — lock this down to your actual Vercel domain in production.
# For dev, "*" is fine. For prod, set BINGR_ALLOWED_ORIGINS env var.
ALLOWED_ORIGINS = os.environ.get(
    "BINGR_ALLOWED_ORIGINS",
    "*",  # dev default
).split(",")

START_TIME = time.time()

# ---------------------------------------------------------------------------
# Persistent cache (same disk format as your local version — drop-in compat)
# ---------------------------------------------------------------------------
def load_persistent_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Ensure episodes have the fast_manifest_text field (migration)
                for ep_key, ep in data.get("episodes", {}).items():
                    ep.setdefault("fast_manifest_text", None)
                # Migration: strip any locked routes that don't contain `{id}`
                # (these were locked by the old buggy code that locked garbage
                # like `https://api.bingr.one/api/stream` when Bingr's frontend
                # probed the bare base URL).
                bad_routes = [k for k, v in data.get("series_templates", {}).items() if "{id}" not in v]
                for k in bad_routes:
                    print(f"[CACHE] Stripping garbage route for {k}: {data['series_templates'][k]}")
                    del data["series_templates"][k]
                return data
        except Exception as e:
            print(f"[CACHE] Load error: {e}")
    return {
        "episodes": {},          # key: "tv_5920_1_1" -> {streams, subtitles, fast_manifest_text}
        "series_templates": {},  # key: "tv_5920"    -> "https://.../stream/aphelion-tv/{id}/{season}/{ep}"
    }


def save_persistent_cache(cache_data: dict):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[CACHE] Save error: {e}")


CACHE = load_persistent_cache()

# ---------------------------------------------------------------------------
# Global state — Playwright lives across requests via lifespan
# ---------------------------------------------------------------------------
STATE = {
    "playwright": None,
    "browser": None,
    "b_context": None,
    "req_ctx": None,
    "api_page": None,  # warm page navigated to bingr.one — used by bingr_direct
                       # to fire fetch() calls from inside Chrome's JS context
                       # (bypasses Bingr's bot detection on cloud IPs)
    "lock": asyncio.Lock(),  # serialize browser scrape ops (one Chromium tab at a time)
}


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------
def parse_watch_url(url: str):
    """Extracts media type (tv, movie, anime), ID, season, episode from a Bingr URL."""
    series_match = re.search(r"/watch/(tv|anime)/(\d+)/(\d+)/(\d+)", url)
    if series_match:
        return {
            "type": series_match.group(1),
            "id": series_match.group(2),
            "season": series_match.group(3),
            "episode": series_match.group(4),
        }
    single_match = re.search(r"/watch/(movie|anime|tv)/(\d+)", url)
    if single_match:
        return {
            "type": single_match.group(1),
            "id": single_match.group(2),
            "season": "1",
            "episode": "1",
        }
    return None


# ---------------------------------------------------------------------------
# Manifest optimizer — keeps ALL audio + subtitle tracks (multi-lang aware)
# ---------------------------------------------------------------------------
async def build_fast_m3u8_with_captions(req_ctx, master_url: str) -> Optional[str]:
    """Build a CORS-friendly 'fast' manifest by resolving all relative URLs to
    absolute and keeping ALL audio + subtitle tracks intact.

    Previously this function filtered to English-only audio/subtitle tracks and
    dropped everything else — that broke multi-language sites (Bingr serves
    Spanish, French, German, Japanese, Hindi, Tamil etc. audio on many movies
    and series). We now keep EVERY track and let the player's audio/subtitle
    picker UI handle switching.

    Returns None (so the frontend falls back to the live manifest-proxy or the
    raw CDN URL) when:
      - URL is None/empty
      - URL contains /proxy/m3u8 (hianime-style — manifest-proxy handles those)
      - URL basename has a query string (signed URL — served directly, CORS-ok)
      - Upstream returns non-200
      - Upstream body isn't an m3u8 master playlist
    """
    if not master_url or "/proxy/m3u8" in master_url:
        return None
    # Skip URLs whose basename has a query string (CloudFront signed URLs —
    # they're served via direct CDN fetch and are already CORS-friendly).
    if "?" in os.path.basename(master_url):
        return None

    try:
        resp = await req_ctx.get(
            master_url,
            headers={
                "Referer": "https://bingr.one/",
                "Accept": "*/*",  # some CDNs reject Accept: application/json
            },
            timeout=8000,
        )
        if resp.status != 200:
            print(f"[FAST M3U8] Upstream {master_url[:80]} returned {resp.status}")
            return None

        raw_text = await resp.text()
        if "#EXTM3U" not in raw_text or "#EXT-X-STREAM-INF" not in raw_text:
            return None

        base_url = master_url.rsplit("/", 1)[0] + "/"
        out_lines = []

        for line in raw_text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue

            # Make URI="..." and KEY_URI="..." attribute values absolute so the
            # frontend never has to resolve relative URLs (HLS.js handles both,
            # but absolute URLs make debugging easier).
            stripped = re.sub(
                r'\b(URI|KEY_URI)="([^"]+)"',
                lambda m: f'{m.group(1)}="{urllib.parse.urljoin(base_url, m.group(2))}"',
                stripped,
            )

            # For non-# lines (segment / sub-playlist URLs), make absolute
            if not stripped.startswith("#"):
                stripped = urllib.parse.urljoin(base_url, stripped)

            out_lines.append(stripped)

        clean_manifest = "\n".join(out_lines) + "\n"
        return clean_manifest
    except Exception as e:
        print(f"[M3U8] Build error: {e}")
        return None


# ---------------------------------------------------------------------------
# Stream + subtitle extraction (identical to your logic)
# ---------------------------------------------------------------------------
def extract_streams_and_subs(data: dict, endpoint_url: str, captured_streams: list, captured_subs: list):
    """Extracts stream URLs and subtitles from any Bingr API JSON payload."""
    if not isinstance(data, dict):
        return

    scraper_name = data.get("scraperName") or data.get("provider") or data.get("server") or "BingrAPI"
    sources = data.get("sources") or data.get("streams") or []

    if isinstance(sources, list):
        for src in sources:
            if isinstance(src, dict) and src.get("url"):
                stream_info = {
                    "quality": src.get("quality", "auto"),
                    "url": src.get("url"),
                    "type": src.get("type", "application/x-mpegurl"),
                    "label": src.get("label", scraper_name),
                    "scraper": scraper_name,
                    "api_endpoint": endpoint_url,
                }
                if not any(s["url"] == stream_info["url"] for s in captured_streams):
                    captured_streams.append(stream_info)

    for key in ("url", "m3u8", "m3u8_path", "file", "stream"):
        val = data.get(key)
        if isinstance(val, str) and (".m3u8" in val or ".mp4" in val):
            if not any(s["url"] == val for s in captured_streams):
                captured_streams.append(
                    {
                        "quality": "auto",
                        "url": val,
                        "type": "application/x-mpegurl",
                        "label": scraper_name,
                        "scraper": scraper_name,
                        "api_endpoint": endpoint_url,
                    }
                )

    raw_subs = data.get("subtitles") or data.get("tracks") or data.get("captions") or []
    if isinstance(raw_subs, list):
        for sub in raw_subs:
            if isinstance(sub, dict):
                sub_url = sub.get("url") or sub.get("file") or sub.get("src")
                sub_lang = sub.get("label") or sub.get("lang") or sub.get("language") or "English"
                if sub_url and not any(x["url"] == sub_url for x in captured_subs):
                    captured_subs.append(
                        {"url": sub_url, "label": str(sub_lang), "lang": str(sub_lang)[:2].lower()}
                    )


# ---------------------------------------------------------------------------
# Warm Playwright browser (started ONCE at app startup via lifespan)
# ---------------------------------------------------------------------------
async def _kill_browser():
    """Tear down Playwright entirely so the next call re-launches from scratch."""
    print("[BROWSER] Tearing down dead browser context...")
    try:
        if STATE["b_context"]:
            await STATE["b_context"].close()
    except Exception:
        pass
    try:
        if STATE["browser"]:
            await STATE["browser"].close()
    except Exception:
        pass
    try:
        if STATE["playwright"]:
            await STATE["playwright"].stop()
    except Exception:
        pass
    STATE["b_context"] = None
    STATE["browser"] = None
    STATE["playwright"] = None
    # Re-create the request context too — it shares the Playwright instance
    try:
        STATE["playwright"] = await async_playwright().start()
        STATE["req_ctx"] = await STATE["playwright"].request.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            extra_http_headers={
                "Referer": "https://bingr.one/",
                # Don't set Origin — some CDNs reject requests where Origin
                # doesn't match the actual request origin.
                "Accept": "*/*",
                # sec-ch-ua headers — Chrome sends these automatically on
                # every request. Bingr's bot detection checks for them when
                # called from cloud IPs (HF Spaces). Without them, Bingr
                # silently returns empty responses to httpx/curl requests.
                "sec-ch-ua": '"Not:A-Brand";v="8", "Chromium";v="156"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
            },
        )
        print("[BROWSER] Request context rebuilt.")
    except Exception as e:
        print(f"[BROWSER] Failed to rebuild req_ctx: {e}")


async def ensure_warm_browser():
    """Idempotently ensures the Playwright + Chromium context is alive.
    Auto-detects dead context and restarts from scratch."""
    # Health check — is the existing browser still alive?
    if STATE["browser"] is not None:
        try:
            # Cheap liveness probe: count open contexts. Throws if dead.
            _contexts = STATE["browser"].contexts
            if STATE["b_context"] is None:
                raise RuntimeError("Context gone")
        except Exception as e:
            print(f"[BROWSER] Context dead ({e}) — restarting...")
            await _kill_browser()

    if STATE["browser"] is None:
        print("[BROWSER] First-time Chromium launch...")
        if STATE["playwright"] is None:
            STATE["playwright"] = await async_playwright().start()
        STATE["browser"] = await STATE["playwright"].chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-infobars",
                # NOTE: --single-process is brittle on Windows — removed.
                # HF Spaces Docker build will re-add it via env if needed.
            ],
        )
        STATE["b_context"] = await STATE["browser"].new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )
        stealth_js = """
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        const originalSetInterval = window.setInterval;
        window.setInterval = function(callback, delay) {
            if (typeof callback === 'function' && typeof delay === 'number' && delay <= 1500) {
                return originalSetInterval(function() {}, delay);
            }
            return originalSetInterval.apply(this, arguments);
        };
        Object.defineProperty(window, 'outerWidth', { get: () => window.innerWidth });
        Object.defineProperty(window, 'outerHeight', { get: () => window.innerHeight });
        window.close = function() {};
        window.DisableDevtool = { isSuspend: true, isRunning: false, md5: () => '', version: '1.0.0' };
        """
        await STATE["b_context"].add_init_script(stealth_js)
        print("[BROWSER] Warm and ready (no per-request cold start).")

        # Open the warm API page — navigates to bingr.one so the
        # bingr.browser_session cookie gets set. The page stays open forever
        # and is used by bingr_direct.resolve_via_api() to fire fetch()
        # calls from inside Chrome's JS context (bypasses Bingr's cloud IP
        # bot detection).
        await _warm_api_page()
    return STATE["b_context"]


async def _warm_api_page():
    """Open a page to bingr.one and keep it warm. Used by bingr_direct.py
    to fire fetch() calls from inside Chrome's JS context.

    The page navigation sets the `bingr.browser_session=1` cookie that
    Bingr's API expects. Without this cookie, Bingr returns empty responses
    to API calls from cloud IPs.

    CRITICAL: navigate to a WATCH URL (not the homepage). Bingr's frontend
    fires /api/stream POSTs from /watch/... pages — these pages have a
    service worker + page context that makes Bingr's API happy. Firing the
    same POST from /home fails with "TypeError: Failed to fetch" because
    the page context doesn't match.
    """
    if STATE["b_context"] is None:
        print("[API PAGE] No browser context yet — skipping warm")
        return
    try:
        if STATE["api_page"] is not None:
            try:
                # Check if it's still alive
                _ = STATE["api_page"].url
                return  # already warm
            except Exception:
                STATE["api_page"] = None

        print("[API PAGE] Opening warm page to bingr.one/watch/movie/969681 (sets session cookie + watch-page context)...")
        page = await STATE["b_context"].new_page()
        # Navigate to a watch URL — this puts the page in the same context
        # Bingr's frontend uses to fire /api/stream POSTs. The service worker
        # + cookies + referrer policy all get set up correctly here.
        await page.goto(
            "https://bingr.one/watch/movie/969681",
            wait_until="domcontentloaded",
            timeout=20000,
        )
        # Give Bingr's frontend a moment to set cookies + register service
        # worker + do its initial /api/stream fetch (which we can ignore —
        # we'll fire our own fetches later).
        await page.wait_for_timeout(3000)
        STATE["api_page"] = page
        print(f"[API PAGE] Warm and ready at {page.url} — fetch() calls from this page will pass Bingr's bot detection.")
    except Exception as e:
        print(f"[API PAGE] Warm failed: {e}")
        STATE["api_page"] = None


# ---------------------------------------------------------------------------
# Background next-episode pre-fetcher (kept from your original)
# ---------------------------------------------------------------------------
async def prefetch_next_episode(m_type: str, m_id: str, season: int, next_ep: int):
    """Silently pre-fetches episode N+1 in the background so switching is instant."""
    ep_key = f"{m_type}_{m_id}_{season}_{next_ep}"
    if ep_key in CACHE["episodes"]:
        return

    series_key = f"{m_type}_{m_id}"
    tpl = CACHE["series_templates"].get(series_key)
    if not tpl or "{id}" not in tpl:
        return

    url = tpl.format(id=m_id, season=season, ep=next_ep)
    try:
        r = await STATE["req_ctx"].get(url, timeout=6000)
        if r.status == 200:
            data = await r.json()
            streams, subs = [], []
            extract_streams_and_subs(data, url, streams, subs)
            if streams:
                # Light prefetch: just store streams + subs. Skip the heavy
                # build_fast_m3u8_with_captions call — the frontend uses the
                # stream URL directly anyway, not the embedded manifest text.
                # This avoids competing with the main playback for bandwidth/connections.
                CACHE["episodes"][ep_key] = {
                    "streams": streams,
                    "subtitles": subs,
                    "fast_manifest_text": None,
                    "cached_at": time.time(),
                }
                save_persistent_cache(CACHE)
                print(f"[PREFETCH] {m_type.upper()} S{season}:E{next_ep} ready in cache!")
        else:
            print(f"[PREFETCH] {m_type.upper()} S{season}:E{next_ep} API returned {r.status}")
    except Exception as e:
        print(f"[PREFETCH] error: {e}")


# ---------------------------------------------------------------------------
# Main resolver (refactored from your original — same logic, cleaner I/O)
# ---------------------------------------------------------------------------
async def resolve_media(target_url: str, build_m3u8_endpoint_origin: str = "") -> dict:
    """
    Resolves stream, subtitles, metadata, and episode list using multi-series
    persistent cache. Same priority ladder as your original:
      1. Episode cache (instant)
      2. Cached series route template + direct API probe
      3. Silent background headless browser (only if 1 + 2 miss)
    """
    req_ctx = STATE["req_ctx"]
    parsed = parse_watch_url(target_url)
    if not parsed:
        return {"error": "Invalid Bingr URL"}

    t_type = parsed["type"]
    t_id = parsed["id"]
    t_season = parsed["season"]
    t_ep = parsed["episode"]
    series_key = f"{t_type}_{t_id}"
    ep_key = f"{t_type}_{t_id}_{t_season}_{t_ep}"

    captured_streams = []
    captured_subs = []
    metadata = {}
    episodes_summary = []
    seasons_list = []

    print(f"\n[RESOLVE] {t_type.upper()} id={t_id} S{t_season} E{t_ep}")

    # 1. Episode cache (instant)
    if ep_key in CACHE["episodes"]:
        cached = CACHE["episodes"][ep_key]
        captured_streams = list(cached.get("streams", []))
        captured_subs = list(cached.get("subtitles", []))
        cached_manifest = cached.get("fast_manifest_text")
        print(f"  [EPISODE CACHE HIT] {ep_key}")

        # Still need metadata + episodes list — fetch in parallel.
        # _fetch_parallel returns a flat list of (url, payload) tuples.
        # With empty stream_urls, results = [(details_url, details_data), (episodes_url, ep_data)].
        details_ep, episodes_ep = _get_details_episodes_endpoints(t_type, t_id, t_season)
        results = await _fetch_parallel(req_ctx, [], details_ep, episodes_ep)
        # Last two entries are always details + episodes (in that order)
        _, details_data = results[-2]
        _, ep_data = results[-1]
        _parse_details_and_episodes(details_data, ep_data, metadata, episodes_summary, seasons_list, t_type, t_id, t_season, t_ep)

        return _build_response(
            target_url, parsed, captured_streams, captured_subs, cached_manifest,
            metadata, seasons_list, episodes_summary, ep_key, build_m3u8_endpoint_origin,
        )

    # 1.5. Direct API (no browser navigation per request) — uses a WARM
    # Chrome page that's already navigated to bingr.one (opened at startup).
    # We fire `fetch()` calls from INSIDE that page's JS context via
    # page.evaluate(). This uses Chrome's actual TLS fingerprint + the
    # bingr.browser_session cookie — Bingr's bot detection can't distinguish
    # our fetch() calls from Bingr's own frontend JS fetches.
    #
    # This is the FAST PATH: ~3-5s per scrape (vs 22s silent browser).
    # Falls back to silent browser if direct API returns nothing.
    try:
        # Make sure the warm API page is alive — re-warm if it died
        if STATE.get("api_page") is None:
            await _warm_api_page()
        direct_result = await resolve_via_api(
            target_url,
            page=STATE.get("api_page"),
        )
        if direct_result and direct_result.get("captured_streams"):
            captured_streams = direct_result["captured_streams"]
            captured_subs = direct_result["captured_subs"]
            metadata.update(direct_result.get("metadata", {}))
            seasons_list = direct_result.get("seasons_list", [])
            episodes_summary = direct_result.get("episodes_summary", [])
            # Jump to the build-fast-manifest + cache + return step
            # (skipping the old direct API probe path entirely — the new direct
            # API supersedes it; the old aphelion-tv/{id}/{season}/{ep} guess
            # was just a partial guess at what we now know is POST /api/stream)
            print("  [DIRECT API SUCCESS] skipping browser fallback")
            # Build fast manifest + save to cache
            fast_manifest_text = None
            if captured_streams and captured_streams[0].get("url"):
                primary_u = captured_streams[0]["url"]
                fast_manifest_text = await build_fast_m3u8_with_captions(req_ctx, primary_u)
                for s in captured_streams:
                    if s["url"] == primary_u:
                        s["raw_url"] = primary_u

            if captured_streams:
                CACHE["episodes"][ep_key] = {
                    "streams": captured_streams,
                    "subtitles": captured_subs,
                    "fast_manifest_text": fast_manifest_text,
                    "cached_at": time.time(),
                }
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(asyncio.to_thread(save_persistent_cache, CACHE))
                except RuntimeError:
                    save_persistent_cache(CACHE)

                # Background next-episode prefetch (TV only)
                if t_type == "tv":
                    try:
                        asyncio.create_task(
                            prefetch_next_episode(t_type, t_id, int(t_season), int(t_ep) + 1)
                        )
                    except Exception:
                        pass

            if not episodes_summary and t_type == "tv":
                for i in range(1, 25):
                    episodes_summary.append({
                        "episode": i,
                        "title": f"Episode {i}",
                        "rating": "",
                        "air_date": "",
                    })

            return _build_response(
                target_url, parsed, captured_streams, captured_subs, fast_manifest_text,
                metadata, seasons_list, episodes_summary, ep_key, build_m3u8_endpoint_origin,
            )
        else:
            print("  [DIRECT API EMPTY] falling back to legacy direct-API-probe + browser path")
    except Exception as e:
        print(f"  [DIRECT API ERROR] {e} — falling back to browser path")

    # 2. Direct API probing (with series template cache)
    candidate_stream_urls = []

    cached_series_tpl = CACHE["series_templates"].get(series_key)
    if cached_series_tpl and "{id}" in cached_series_tpl:
        candidate_stream_urls.append(cached_series_tpl.format(id=t_id, season=t_season, ep=t_ep))
        print(f"  [SERIES ROUTE CACHE] {cached_series_tpl}")

    if t_type == "tv":
        for tpl in [
            "https://api.bingr.one/api/stream/aphelion-tv/{id}/{season}/{ep}",
            "https://api.bingr.one/api/stream/aphelion/{id}/{season}/{ep}",
        ]:
            u = tpl.format(id=t_id, season=t_season, ep=t_ep)
            if u not in candidate_stream_urls:
                candidate_stream_urls.append(u)
    elif t_type == "anime":
        for tpl in [
            "https://api.bingr.one/api/anime/{id}/stream/{season}/{ep}",
            "https://api.bingr.one/api/anime/{id}/watch/{ep}",
            "https://api.bingr.one/api/stream/aphelion-anime/{id}/{season}/{ep}",
            "https://api.bingr.one/api/stream/aphelion-tv/{id}/{season}/{ep}",
        ]:
            u = tpl.format(id=t_id, season=t_season, ep=t_ep)
            if u not in candidate_stream_urls:
                candidate_stream_urls.append(u)
    else:  # movie
        for tpl in [
            "https://api.bingr.one/api/stream/aphelion-movie/{id}",
            "https://api.bingr.one/api/stream/aphelion/{id}",
            "https://api.bingr.one/api/stream/movie/{id}",
        ]:
            u = tpl.format(id=t_id, season=t_season, ep=t_ep)
            if u not in candidate_stream_urls:
                candidate_stream_urls.append(u)

    details_ep, episodes_ep = _get_details_episodes_endpoints(t_type, t_id, t_season)

    # Run all fetches in parallel
    results = await _fetch_parallel(req_ctx, candidate_stream_urls, details_ep, episodes_ep)
    stream_results = results[:-2]
    _, details_data = results[-2]
    _, ep_data = results[-1]

    # Extract streams + lock series template
    for url_called, payload in stream_results:
        if payload:
            prev_len = len(captured_streams)
            extract_streams_and_subs(payload, url_called, captured_streams, captured_subs)
            if len(captured_streams) > prev_len and "api.bingr.one" in url_called:
                _lock_series_route(series_key, url_called, t_id, t_season, t_ep, t_type)

    _parse_details_and_episodes(details_data, ep_data, metadata, episodes_summary, seasons_list, t_type, t_id, t_season, t_ep)

    # 3. Silent headless browser fallback (only if direct routes missed)
    if not captured_streams:
        print("  [SILENT BROWSER] Falling back to headless scraper...")
        async with STATE["lock"]:
            page = None
            # Track last-capture time so we can wait for a cooldown period
            # (3s of no new captures) before breaking — this catches scrapers
            # like Polaris that fire AFTER DarkMatter on the same movie, which
            # the previous "break after 1s" logic was missing entirely.
            browser_state = {"last_capture_ts": time.time(), "started_at": time.time()}
            try:
                b_ctx = await ensure_warm_browser()
                page = await b_ctx.new_page()

                async def handle_response(response):
                    try:
                        url = response.url
                        content_type = response.headers.get("content-type", "").lower()

                        if (".vtt" in url or ".srt" in url) and not any(x["url"] == url for x in captured_subs):
                            captured_subs.append({"url": url, "label": "English (Auto-Captions)", "lang": "en"})
                            browser_state["last_capture_ts"] = time.time()
                            print(f"  >>> [CAPTION CAPTURED] {url}")

                        if ".m3u8" in url and not any(s["url"] == url for s in captured_streams):
                            captured_streams.append({
                                "quality": "1080p/Auto",
                                "url": url,
                                "type": "application/x-mpegurl",
                                "label": "LiveStream",
                                "scraper": "SilentBrowser",
                                "api_endpoint": url,
                            })
                            browser_state["last_capture_ts"] = time.time()
                            print(f"  >>> [M3U8 CAPTURED] {url}")

                        if "application/json" in content_type and "umami" not in url:
                            data = await response.json()
                            prev_len = len(captured_streams)
                            extract_streams_and_subs(data, url, captured_streams, captured_subs)
                            if len(captured_streams) > prev_len:
                                browser_state["last_capture_ts"] = time.time()
                                if "api.bingr.one" in url:
                                    _lock_series_route(series_key, url, t_id, t_season, t_ep, t_type)

                            if ("/details/" in url or "/anime/" in url) and isinstance(data, dict) and not metadata.get("title"):
                                _parse_details_and_episodes(data, None, metadata, episodes_summary, seasons_list, t_type, t_id, t_season, t_ep)
                            if "episodes" in url and not episodes_summary:
                                _parse_details_and_episodes(None, data, metadata, episodes_summary, seasons_list, t_type, t_id, t_season, t_ep)
                    except Exception:
                        pass

                page.on("response", handle_response)
                await page.goto(target_url, wait_until="domcontentloaded", timeout=25000)
                # Wait for a cooldown of 3 seconds with no new captures, or a
                # hard cap of 20 seconds. The previous code broke 1 second
                # after the FIRST capture, which meant if Bingr's frontend
                # fired a second scraper (e.g. Polaris after DarkMatter) we'd
                # miss it. Now we keep waiting as long as new URLs are coming
                # in, then break 3s after the LAST capture.
                MAX_BROWSER_WAIT = 20.0  # seconds hard cap
                COOLDOWN_SEC = 3.0       # break this long after last capture
                while time.time() - browser_state["started_at"] < MAX_BROWSER_WAIT:
                    await page.wait_for_timeout(500)
                    if captured_streams and (time.time() - browser_state["last_capture_ts"]) > COOLDOWN_SEC:
                        break
            except Exception as e:
                print(f"  [BROWSER ERROR] {e}")
                # Browser crashed mid-scrape — nuke context so next call rebuilds.
                await _kill_browser()
            finally:
                if page is not None:
                    try:
                        await page.close()
                    except Exception:
                        pass

        # Dedupe sub-playlists (e.g. when DarkMatter fired and we captured
        # index.m3u8 + video_480p.m3u8 + track_english.m3u8 + ... — only keep
        # the master).
        before_dedup = len(captured_streams)
        captured_streams = _dedupe_sub_playlists(captured_streams)
        if before_dedup != len(captured_streams):
            print(f"  [DEDUP] {before_dedup} → {len(captured_streams)} streams")

    # 4. Build fast manifest + save to cache
    fast_manifest_text = None
    if captured_streams and captured_streams[0].get("url"):
        primary_u = captured_streams[0]["url"]
        fast_manifest_text = await build_fast_m3u8_with_captions(req_ctx, primary_u)
        # Stash raw URL so frontend can fall back to direct CDN if m3u8 endpoint fails
        for s in captured_streams:
            if s["url"] == primary_u:
                s["raw_url"] = primary_u

    if captured_streams:
        # Save to cache IN BACKGROUND — don't block the response. The cache write
        # can be slow (file I/O) when the cache grows large with many episodes.
        CACHE["episodes"][ep_key] = {
            "streams": captured_streams,
            "subtitles": captured_subs,
            "fast_manifest_text": fast_manifest_text,
            "cached_at": time.time(),  # for staleness check (apiKeys expire at 3h)
        }
        # Snapshot the cache to disk in background — fire-and-forget
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(asyncio.to_thread(save_persistent_cache, CACHE))
        except RuntimeError:
            # No running loop — synchronous save
            save_persistent_cache(CACHE)

        # Background next-episode prefetch (fire-and-forget)
        if t_type in ("tv", "anime"):
            try:
                asyncio.create_task(
                    prefetch_next_episode(t_type, t_id, int(t_season), int(t_ep) + 1)
                )
            except Exception:
                pass

    if not episodes_summary and t_type in ("tv", "anime"):
        for i in range(1, 25):
            episodes_summary.append({
                "episode": i,
                "title": f"Episode {i}",
                "rating": "",
                "air_date": "",
            })

    return _build_response(
        target_url, parsed, captured_streams, captured_subs, fast_manifest_text,
        metadata, seasons_list, episodes_summary, ep_key, build_m3u8_endpoint_origin,
    )


# ----- helper split-out so the function above stays readable -----
def _normalize_stream_url(url: str) -> str:
    """Strip the `direct=1` query param from hianime-style proxy URLs.
    
    Without this fix, the proxy returns the raw upstream manifest with absolute
    segment URLs (e.g. https://fetch.nexabloom.top/.../segment_1.ts) which the
    browser can't play because they need Referer: https://megaplay.buzz/ and
    browsers can't set Referer from JS.
    
    By removing `direct=1`, the proxy rewrites segment URLs to also go through
    itself, adding the referer server-side. This makes the URL browser-playable.
    
    Also handles direct=1 appearing anywhere in the query string.
    """
    if not url or "direct=1" not in url:
        return url
    try:
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        params.pop("direct", None)
        # Re-encode without empty params
        new_query = "&".join(
            f"{k}={urllib.parse.quote(v[0], safe='')}"
            for k, v in params.items() if v and v[0] != ""
        )
        return urllib.parse.urlunparse(parsed._replace(query=new_query))
    except Exception:
        # Fallback: regex strip
        return re.sub(r"[?&]direct=1(?=&|$)", "", url).replace("?&", "?").rstrip("&?")


# Domain → referer mapping. Updated as we discover more upstream CDNs.
# These are the referers Bingr's upstream CDNs check for. Our backend uses
# this table when proxying segments through /api/seg-proxy.
DOMAIN_REFERER_MAP = {
    "fetch.nexabloom.top": "https://megaplay.buzz/",
    "ravok.buzz": "https://animesalt.cx/",
    "embermeadow.world": "https://megaplay.buzz/",
    # For these the upstream CDN is permissive — no referer needed, but adding
    # bingr.one never hurts:
    "img.animecurx.tech": "https://bingr.one/",
    "img.rousav.tech": "https://bingr.one/",
    "sacdn.hakunaymatata.com": "https://bingr.one/",
}


def _referer_for_url(upstream_url: str) -> str:
    """Look up the right Referer header to send when fetching this upstream URL.
    Falls back to https://bingr.one/ if unknown."""
    try:
        host = urllib.parse.urlparse(upstream_url).netloc.lower()
        for domain, referer in DOMAIN_REFERER_MAP.items():
            if host == domain or host.endswith("." + domain):
                return referer
    except Exception:
        pass
    return "https://bingr.one/"


# Domains that send `Access-Control-Allow-Origin: *` on their responses, so the
# browser can fetch them directly without our proxy. Add to this set as we
# discover more CORS-friendly CDNs.
CORS_FRIENDLY_DOMAINS = {
    "img.animecurx.tech",
    "img.rousav.tech",
}


# ---------------------------------------------------------------------------
# Manifest URL rewriter — used by both /api/manifest-proxy AND /api/seg-proxy
# (when seg-proxy detects a sub-playlist like an audio-only m3u8 returned
# by an audio URL we proxied).
#
# This is the heart of multi-audio + multi-subtitle support. Without rewriting
# ALL URLs in the manifest (not just hianime ones), the browser can't fetch
# audio sub-playlists or subtitle sub-playlists from CDNs that need a Referer
# header (which the browser can't set from JS).
# ---------------------------------------------------------------------------
URI_ATTR_RE = re.compile(r'\b(URI|KEY_URI|URL)="([^"]+)"')
HIANIME_URL_RE = re.compile(r'(?:https?://hianime\.filmu\.in)?/proxy/m3u8\?[^\s",]+')


def _is_cors_friendly_or_signed(url_str: str) -> bool:
    """True if the URL is already directly fetchable by the browser:
      - it's already wrapped in our /api/seg-proxy or /api/manifest-proxy
      - it's a CloudFront signed URL (Policy= + Signature= + Key-Pair-Id=)
      - it's on a known CORS-friendly CDN
    """
    if not url_str:
        return True
    if "/api/seg-proxy?" in url_str or "/api/manifest-proxy?" in url_str:
        return True
    if re.search(r"[?&](Policy|Signature|Key-Pair-Id)=", url_str):
        return True
    try:
        host = urllib.parse.urlparse(url_str).netloc.lower()
        for d in CORS_FRIENDLY_DOMAINS:
            if host == d or host.endswith("." + d):
                return True
    except Exception:
        pass
    return False


def _build_seg_proxy_url(target_url: str, base_url: str, referer: str, seg_proxy_origin: str) -> str:
    """Wrap an arbitrary URL (relative or absolute) so it points through
    /api/seg-proxy on our backend with the correct referer.

    If the URL is already directly fetchable (CORS-friendly, signed, or already
    proxied), return it unchanged.
    """
    if not target_url or not seg_proxy_origin:
        return target_url
    if _is_cors_friendly_or_signed(target_url):
        return target_url
    abs_url = urllib.parse.urljoin(base_url, target_url)
    if _is_cors_friendly_or_signed(abs_url):
        return abs_url
    encoded_u = urllib.parse.quote(abs_url, safe="")
    if referer:
        encoded_r = urllib.parse.quote(referer, safe="")
        return f"{seg_proxy_origin}/api/seg-proxy?u={encoded_u}&referer={encoded_r}"
    return f"{seg_proxy_origin}/api/seg-proxy?u={encoded_u}"


def _rewrite_manifest_urls(body: str, base_url: str, referer: str, seg_proxy_origin: str) -> str:
    """Rewrite ALL URLs in an m3u8 manifest to point through /api/seg-proxy.

    Catches:
      1. URI="..." and KEY_URI="..." attribute values (covers #EXT-X-MEDIA
         audio/subtitle, #EXT-X-KEY encryption key, #EXT-X-SESSION-DATA)
      2. Hianime-style /proxy/m3u8?... URLs ANYWHERE on the line — these use
         raw `&` chars (not URL-encoded as %26) and might appear in URI="..."
         attributes OR as plain values, so we scan the whole line for them
      3. Plain URL lines (segments, sub-playlists)

    Skips:
      - URLs already pointing at /api/seg-proxy or /api/manifest-proxy
        (idempotency — prevents double-wrapping on cached manifests)
      - CloudFront signed URLs (already CORS-friendly, referer embedded)
      - URLs on CORS-friendly domains (img.animecurx.tech, etc.)

    Returns the body unchanged if seg_proxy_origin is empty (can't build
    absolute self-URLs without it).
    """
    if not seg_proxy_origin or not body:
        return body

    out_lines = []
    for line in body.splitlines():
        s = line.strip()
        if not s:
            continue

        # 1. Rewrite URI="..." / KEY_URI="..." / URL="..." attribute values
        s = URI_ATTR_RE.sub(
            lambda m: f'{m.group(1)}="{_build_seg_proxy_url(m.group(2), base_url, referer, seg_proxy_origin)}"',
            s,
        )

        # 2. Catch hianime-style /proxy/m3u8?... URLs anywhere on the line
        #    (encryption key URLs with raw & chars that aren't in URI="..." attrs)
        s = HIANIME_URL_RE.sub(
            lambda m: _build_seg_proxy_url(m.group(0), base_url, referer, seg_proxy_origin),
            s,
        )

        # 3. Plain URL lines (segments, sub-playlists) — non-# lines
        if not s.startswith("#"):
            if not _is_cors_friendly_or_signed(s):
                s = _build_seg_proxy_url(s, base_url, referer, seg_proxy_origin)

        out_lines.append(s)

    return "\n".join(out_lines) + "\n"


def _wrap_sub_url(sub_url: str, m3u8_origin: str) -> str:
    """Wrap a sidecar subtitle URL (.vtt/.srt) in /api/seg-proxy.

    Sidecar URLs returned in Bingr's JSON `subtitles` array typically:
      1. Lack CORS headers — browser can't fetch directly
      2. Need a Referer header — browser can't set Referer from JS

    So we proxy them through /api/seg-proxy. CloudFront signed URLs and
    known CORS-friendly domains are left alone.
    """
    if not sub_url or not m3u8_origin:
        return sub_url
    if _is_cors_friendly_or_signed(sub_url):
        return sub_url
    referer = _referer_for_url(sub_url)
    encoded_u = urllib.parse.quote(sub_url, safe="")
    encoded_r = urllib.parse.quote(referer, safe="")
    return f"{m3u8_origin}/api/seg-proxy?u={encoded_u}&referer={encoded_r}"


# ---------------------------------------------------------------------------
# Series route locking — only locks VALID templates (must contain {id})
# ---------------------------------------------------------------------------
def _lock_series_route(series_key: str, url_called: str, t_id: str, t_season: str, t_ep: str, t_type: str):
    """Try to derive a reusable route template from a working API URL and
    cache it so future requests for the same series skip the silent browser.

    Only locks if the substitution actually placed `{id}` in the result —
    otherwise we'd pollute the cache with garbage like
    `https://api.bingr.one/api/stream` (locked when Bingr's frontend probed
    the bare base URL).

    For movies (t_type='movie') we ONLY try the `/{t_id}$` → `/{id}` substitution
    (movies don't have season/episode suffix). For tv/anime we try the full
    `/{t_id}/{t_season}/{t_ep}$` → `/{id}/{season}/{ep}` pattern first.
    """
    if not url_called or "api.bingr.one" not in url_called:
        return
    tpl = url_called
    if t_type in ("tv", "anime"):
        # Try the season/ep pattern first
        new_tpl = re.sub(rf"/{re.escape(t_id)}/{re.escape(t_season)}/{re.escape(t_ep)}$", "/{id}/{season}/{ep}", tpl)
        if new_tpl != tpl:
            tpl = new_tpl
    # Always also try the bare ID pattern (covers movies + tv/anime single-ep URLs)
    tpl = re.sub(rf"/{re.escape(t_id)}$", "/{id}", tpl)
    # ONLY lock if the result actually contains {id} — prevents garbage locks
    if "{id}" in tpl and tpl != url_called:
        CACHE["series_templates"][series_key] = tpl
        # Fire-and-forget save. Use the running-loop check (not try/except)
        # because asyncio.to_thread() returns a coroutine even outside a loop,
        # and that leaked coroutine raises RuntimeWarning.
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(asyncio.to_thread(save_persistent_cache, CACHE))
        except RuntimeError:
            save_persistent_cache(CACHE)
        print(f"  [LOCKED ROUTE] {series_key} -> {tpl}")


# ---------------------------------------------------------------------------
# Sub-playlist dedup — drop sub-playlists that are components of an
# already-captured master playlist
# ---------------------------------------------------------------------------
MASTER_PLAYLIST_NAMES = {"index.m3u8", "master.m3u8", "playlist.m3u8"}


def _dedupe_sub_playlists(streams: list) -> list:
    """Drop sub-playlist URLs (video_480p.m3u8, track_english.m3u8,
    subtitles_english.m3u8, tiles.m3u8, etc.) that live in the SAME directory
    as an already-captured master playlist (index.m3u8 / master.m3u8 /
    playlist.m3u8).

    Why: when Bingr's player loads a DarkMatter-style stream, it fetches the
    master `index.m3u8` AND every sub-playlist it references (video variants,
    audio tracks, subtitle tracks, thumbnail tiles). Our response interceptor
    captures all of them, so the user gets 6 "streams" for what's really ONE
    master playlist with variants inside. The player should fetch the master
    and let HLS.js discover the variants — not be handed each sub-playlist as
    a separate stream.

    Streams from DIFFERENT directories (different CDNs / different scrapers)
    are all kept — they're genuinely different sources.
    """
    if not streams or len(streams) <= 1:
        return streams

    # Find directories that contain a master playlist
    master_dirs = set()
    for s in streams:
        url = s.get("url", "")
        if not url:
            continue
        # Strip query string before checking basename
        basename = url.rsplit("/", 1)[-1].split("?", 1)[0].lower()
        if basename in MASTER_PLAYLIST_NAMES:
            master_dirs.add(url.rsplit("/", 1)[0])

    if not master_dirs:
        return streams  # no masters captured → keep everything

    kept = []
    dropped = []
    for s in streams:
        url = s.get("url", "")
        if not url:
            continue
        dir_part = url.rsplit("/", 1)[0]
        if dir_part in master_dirs:
            # Same directory as a master — only keep if THIS is the master
            basename = url.rsplit("/", 1)[-1].split("?", 1)[0].lower()
            if basename in MASTER_PLAYLIST_NAMES:
                kept.append(s)
            else:
                dropped.append(url)
        else:
            # Different directory (different CDN / scraper) — keep
            kept.append(s)

    for d in dropped:
        print(f"  [DEDUP] Dropped sub-playlist (master captured in same dir): {d[:100]}")

    return kept


def _build_proxy_urls(stream_url: str, m3u8_origin: str) -> tuple:
    """Convert any stream URL into a (proxy_url, referer_used) tuple
    that the browser can actually play.
    
    IMPORTANT DISCOVERIES (from testing):
    
    1. ravok.buzz's md5+expires token is IP-bound to hianime proxy's server IP.
       Our backend CANNOT fetch ravok.buzz directly — returns 403.
    
    2. hianime.filmu.in proxy returns 200 to browser requests BUT doesn't send
       Access-Control-Allow-Origin, so the browser blocks reading the response.
       The browser CANNOT fetch hianime proxy URLs directly.
    
    3. hianime proxy uses apiKey JWT (not Referer) for authentication, and the
       apiKey IP must match the requester's IP. For LOCAL testing: backend IP
       == browser IP == user's PC IP == apiKey IP → match → works. For
       PRODUCTION (HF Spaces): backend IP == apiKey IP (both are HF's IP) →
       also works because our backend always makes the request from the same
       machine that scraped.
    
    SOLUTION: wrap hianime proxy URLs in our /api/manifest-proxy. Our backend
    fetches the hianime proxy URL (server-side, no CORS, IP matches apiKey),
    gets the manifest with segment URLs that also go through hianime proxy,
    rewrites those through our /api/seg-proxy, and serves the rewritten
    manifest to the browser WITH CORS headers.
    
    Rules:
      0. IDEMPOTENCY: if URL is already a /api/manifest-proxy or /api/seg-proxy
         URL, return as-is. Prevents the double-wrapping bug on cache hits.
      1. CloudFront signed URLs (Policy= + Signature= + Key-Pair-Id=):
         ALWAYS CORS-friendly (CloudFront guarantees this). Leave alone.
      2. Known CORS-friendly domains (img.animecurx.tech, img.rousav.tech):
         Leave alone — browser can fetch directly.
      3. hianime proxy URLs: WRAP in our /api/manifest-proxy with EMPTY
         referer (hianime uses apiKey, not Referer). Our backend fetches it
         server-side and serves to browser with CORS.
      4. Raw upstream CDN URLs without hianime wrapping: also wrap in our
         /api/manifest-proxy. Best-effort — works for CDNs without IP-binding,
         fails for IP-bound ones (ravok.buzz direct URLs from JSON responses
         will 403, but at least the hianime proxy URLs from browser intercept
         will work).
    """
    if not stream_url or not m3u8_origin:
        return stream_url, None
    
    # Bug fix #1: idempotency — never double-wrap
    if "/api/manifest-proxy?" in stream_url or "/api/seg-proxy?" in stream_url:
        return stream_url, None
    
    # CloudFront signed URLs always have CORS — leave alone
    if re.search(r"[?&](Policy|Signature|Key-Pair-Id)=", stream_url):
        return stream_url, None
    
    # CORS-friendly domains — leave alone
    try:
        host = urllib.parse.urlparse(stream_url).netloc.lower()
        for d in CORS_FRIENDLY_DOMAINS:
            if host == d or host.endswith("." + d):
                return stream_url, None
    except Exception:
        pass
    
    # hianime proxy URL — WRAP in our manifest-proxy with EMPTY referer.
    # hianime proxy uses apiKey JWT for auth (not Referer). The apiKey IP must
    # match the requester IP. Our backend's IP matches (local: user PC IP,
    # prod: HF IP — same as scraper). Browser can't fetch hianime directly
    # because hianime doesn't send CORS headers.
    if "hianime.filmu.in/proxy/m3u8" in stream_url:
        # Use empty referer — hianime proxy doesn't need Referer, just apiKey
        referer = ""
        encoded_u = urllib.parse.quote(stream_url, safe="")
        encoded_r = urllib.parse.quote(referer, safe="")
        return f"{m3u8_origin}/api/manifest-proxy?u={encoded_u}&referer={encoded_r}", referer
    
    # All other raw upstream CDN URLs — wrap in manifest-proxy (best-effort).
    # May fail if the URL is IP-bound to a different server's IP.
    try:
        referer = _referer_for_url(stream_url)
        encoded_u = urllib.parse.quote(stream_url, safe="")
        encoded_r = urllib.parse.quote(referer, safe="")
        return f"{m3u8_origin}/api/manifest-proxy?u={encoded_u}&referer={encoded_r}", referer
    except Exception:
        return stream_url, None

def _get_details_episodes_endpoints(t_type, t_id, t_season):
    if t_type == "tv":
        return (
            f"https://api.bingr.one/api/details/tv/{t_id}?v=1",
            f"https://api.bingr.one/api/episodes/{t_id}/{t_season}",
        )
    if t_type == "anime":
        return (
            f"https://api.bingr.one/api/anime/{t_id}?v=1",
            f"https://api.bingr.one/api/anime/{t_id}/episodes?chunk=1",
        )
    # movie
    return (f"https://api.bingr.one/api/details/movie/{t_id}?v=1", None)


async def _fetch_parallel(req_ctx, stream_urls, details_url, episodes_url):
    """Returns [(url, payload_or_None), ...] same length as input stream_urls + 2."""
    async def fetch_json(url):
        if not url:
            return url, None
        try:
            r = await req_ctx.get(url, timeout=7000)
            if r.status == 200:
                return url, await r.json()
        except Exception:
            pass
        return url, None

    tasks = [fetch_json(u) for u in stream_urls]
    tasks.append(fetch_json(details_url))
    tasks.append(fetch_json(episodes_url))
    return await asyncio.gather(*tasks)


def _parse_details_and_episodes(d_data, e_data, metadata, episodes_summary, seasons_list, t_type, t_id, t_season, t_ep):
    if isinstance(d_data, dict):
        metadata.update({
            "id": d_data.get("id") or t_id,
            "type": t_type,
            "season": int(t_season) if str(t_season).isdigit() else 1,
            "episode": int(t_ep) if str(t_ep).isdigit() else 1,
            "title": d_data.get("title") or d_data.get("name") or f"{t_type.title()} #{t_id}",
            "year": d_data.get("year") or d_data.get("releaseDate") or "",
            "genres": d_data.get("genres", []),
            "rating": d_data.get("rating") or d_data.get("score") or "N/A",
            "overview": d_data.get("overview") or d_data.get("description") or "",
        })
        if isinstance(d_data.get("seasons"), list):
            for s in d_data["seasons"]:
                if isinstance(s, dict) and s.get("season", 0) > 0:
                    seasons_list.append({
                        "season": s.get("season"),
                        "name": s.get("name") or f"Season {s.get('season')}",
                        "episodes_count": s.get("episodes", 0),
                    })

    raw_eps = None
    if isinstance(e_data, dict):
        raw_eps = e_data.get("episodes") or e_data.get("data") or []
    elif isinstance(e_data, list):
        raw_eps = e_data

    if isinstance(raw_eps, list):
        for idx_ep, ep in enumerate(raw_eps, 1):
            if isinstance(ep, dict):
                ep_num = ep.get("episode") or ep.get("number") or idx_ep
                episodes_summary.append({
                    "episode": ep_num,
                    "title": ep.get("title") or ep.get("name") or f"Episode {ep_num}",
                    "rating": ep.get("rating", ""),
                    "air_date": ep.get("air_date", ""),
                })


def _build_response(target_url, parsed, captured_streams, captured_subs, fast_manifest_text,
                    metadata, seasons_list, episodes_summary, ep_key, m3u8_origin):
    """Compose the JSON the frontend receives. Includes a fast_m3u8_url the frontend
    can hand directly to HLS.js (CORS-enabled on our side).
    
    ALSO:
    - Strips `direct=1` from hianime-style proxy URLs (so segments get proxied)
    - Rewrites all raw upstream CDN URLs through our /api/manifest-proxy endpoint
      so the backend adds the correct Referer header (browsers can't set Referer)
    - Wraps sidecar subtitle URLs through /api/seg-proxy so they're CORS-friendly
      + get the right Referer server-side
    - Detects self-signed URLs (md5/expires/Policy/Signature/Key-Pair-Id) and
      leaves them alone — they're already auth-embedded and need no referer.
    - Auto-busts cache entries older than 2.5 hours (apiKey JWTs expire at 3h)
    """
    # Auto-bust stale cache: apiKeys expire at 3 hours, give 30min safety margin
    STALE_AFTER_SEC = 2.5 * 3600
    cached = CACHE["episodes"].get(ep_key)
    if cached and cached.get("cached_at"):
        age = time.time() - cached["cached_at"]
        if age > STALE_AFTER_SEC:
            print(f"  [CACHE STALE] {ep_key} is {age/3600:.1f}h old — busting + re-scrape")
            del CACHE["episodes"][ep_key]
            save_persistent_cache(CACHE)
            # Caller (resolve_endpoint) should re-call resolve_media — but since
            # we're already past the scrape phase here, just mark it for next time.
            # The user's next click will re-scrape.

    # Update cached_at timestamp on every fresh build
    if cached is not None:
        cached["cached_at"] = time.time()
        save_persistent_cache(CACHE)

    # Normalize + proxy every stream URL we're about to hand to the browser
    for s in captured_streams:
        if s.get("url"):
            s["url"] = _normalize_stream_url(s["url"])
            proxy_url, used_referer = _build_proxy_urls(s["url"], m3u8_origin)
            s["url"] = proxy_url
            s["proxied"] = (proxy_url != s.get("raw_url"))
            if used_referer:
                s["referer_used"] = used_referer
        if s.get("raw_url"):
            s["raw_url"] = _normalize_stream_url(s["raw_url"])

    # Wrap sidecar subtitle URLs (returned in Bingr's JSON `subtitles` array)
    # through /api/seg-proxy so the browser can fetch them with CORS + correct
    # Referer. Without this, sidecar .vtt/.srt URLs that lack CORS headers or
    # require a Referer would be silently blocked by the browser.
    for sub in captured_subs:
        if sub.get("url"):
            sub["url"] = _wrap_sub_url(sub["url"], m3u8_origin)

    fast_m3u8_url = None
    if fast_manifest_text and m3u8_origin:
        fast_m3u8_url = f"{m3u8_origin}/api/fast.m3u8?ep={ep_key}"

    return {
        "page_url": target_url,
        "parsed": parsed,
        "title": metadata.get("title") or f"{parsed['type'].upper()} #{parsed['id']}",
        "stream_links": captured_streams,
        "subtitles": captured_subs,
        "fast_manifest_text": fast_manifest_text,
        "fast_m3u8_url": fast_m3u8_url,
        "cached_route": CACHE["series_templates"].get(f"{parsed['type']}_{parsed['id']}"),
        "media_info": metadata,
        "seasons": seasons_list,
        "episodes": episodes_summary,
        "episode_key": ep_key,
    }


# ---------------------------------------------------------------------------
# FastAPI app + lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: warm Playwright + Chromium + req_ctx. Shutdown: tear down cleanly."""
    print("[STARTUP] Warming Playwright + Chromium...")
    STATE["playwright"] = await async_playwright().start()
    STATE["req_ctx"] = await STATE["playwright"].request.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        extra_http_headers={
            "Referer": "https://bingr.one/",
            # Don't set Origin — some CDNs (ravok.buzz, fetch.nexabloom.top)
            # reject requests where Origin doesn't match the actual request
            # origin. Referer alone is enough.
            "Accept": "*/*",
            # sec-ch-ua headers — Chrome sends these automatically. Bingr's
            # bot detection checks for them on cloud IPs. Critical for the
            # direct API path to work on HF Spaces.
            "sec-ch-ua": '"Not:A-Brand";v="8", "Chromium";v="156"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        },
    )
    # Eagerly start the browser so first request doesn't pay the cold-start tax
    await ensure_warm_browser()
    print(f"[STARTUP] Ready. Listening on http://{SERVER_HOST}:{SERVER_PORT}")
    yield
    print("[SHUTDOWN] Cleaning up Playwright...")
    try:
        if STATE["browser"]:
            await STATE["browser"].close()
        if STATE["playwright"]:
            await STATE["playwright"].stop()
    except Exception as e:
        print(f"[SHUTDOWN] {e}")


app = FastAPI(
    title="Bingr Scraper API",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
class ResolveRequest(BaseModel):
    url: str


@app.get("/api/health")
async def health():
    """Keep-alive endpoint for cron-job.org / UptimeRobot pingers."""
    return {
        "status": "ok",
        "uptime_seconds": round(time.time() - START_TIME, 1),
        "browser_warm": STATE["browser"] is not None,
        "cache_episodes": len(CACHE["episodes"]),
        "cache_series_routes": len(CACHE["series_templates"]),
    }


@app.post("/api/resolve")
async def resolve_endpoint(req: ResolveRequest, request: Request):
    """Main scrape endpoint. Frontend POSTs a bingr URL, gets back stream + metadata JSON."""
    target_url = req.url.strip()
    if not target_url or "bingr.one/watch/" not in target_url:
        raise HTTPException(status_code=400, detail="URL must be a bingr.one/watch/... URL")

    # The m3u8 endpoint URL we hand back to the frontend must be reachable from
    # the user's browser. Derive from the Host header so it works both locally
    # (http://127.0.0.1:8765) and on HF Spaces (https://username-spacename.hf.space).
    host_header = request.headers.get("x-forwarded-host") or request.headers.get("host", "")
    scheme = "https" if "hf.space" in host_header else request.url.scheme
    m3u8_origin = f"{scheme}://{host_header}" if host_header else ""

    try:
        result = await resolve_media(target_url, build_m3u8_endpoint_origin=m3u8_origin)
        # Detect "no streams captured" — return 404 so the frontend shows a real error
        # rather than silently rendering a black player.
        if not result.get("stream_links"):
            raise HTTPException(
                status_code=404,
                detail={
                    "message": "No stream links resolved for this URL.",
                    "url": target_url,
                    "tried_browser_fallback": True,
                    "media_info": result.get("media_info", {}),
                    "episode_key": result.get("episode_key"),
                },
            )
        # Log what we're returning so we can diagnose playback issues
        primary = result["stream_links"][0]
        print(f"  [RETURNED] {len(result['stream_links'])} streams. Picked: [{primary.get('quality')}] {primary.get('scraper')}")
        print(f"           URL: {primary['url'][:150]}")
        if primary.get("proxied"):
            print(f"           (proxied through /api/manifest-proxy)")
        else:
            print(f"           (direct browser fetch — no proxy)")
        # Persist a snapshot (drop the manifest text to keep the file small)
        try:
            snapshot = {k: v for k, v in result.items() if k != "fast_manifest_text"}
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, indent=2, ensure_ascii=False)
        except Exception:
            pass
        return JSONResponse(result)
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"[ERROR] {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/fast.m3u8")
async def fast_m3u8(ep: str):
    """Returns the cached stripped m3u8 manifest for a given episode key.
    CORS-enabled so HLS.js can fetch it cross-origin from your Vercel site."""
    if not ep:
        raise HTTPException(status_code=400, detail="Missing ?ep= parameter")
    cached = CACHE["episodes"].get(ep)
    if not cached or not cached.get("fast_manifest_text"):
        raise HTTPException(status_code=404, detail=f"No cached manifest for episode key '{ep}'")
    return PlainTextResponse(
        cached["fast_manifest_text"],
        media_type="application/vnd.apple.mpegurl",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=3600",
        },
    )


@app.api_route("/api/manifest-proxy", methods=["GET", "HEAD", "OPTIONS"])
async def manifest_proxy(u: str, referer: Optional[str] = None, request: Request = None):
    """Fetch upstream m3u8 with the correct Referer header, rewrite ALL inner
    URLs (segments, audio playlists, subtitle playlists, key URIs) to point
    through /api/seg-proxy on our backend, and return the rewritten manifest
    with CORS headers.
    
    This is what makes anime streams play — browsers can't set Referer from
    JavaScript, so we proxy the manifest + all its segments through our
    backend which CAN set Referer server-side.
    
    Query params:
      u        — the upstream m3u8 URL (URL-encoded)
      referer  — the Referer header to send upstream (URL-encoded)
    """
    if not u:
        raise HTTPException(status_code=400, detail="Missing ?u= parameter")
    # Only fall back to default referer if the param was MISSING (None).
    # If it was passed as empty string "" (e.g. for hianime proxy URLs that
    # use apiKey JWT, not Referer), keep it empty.
    if referer is None:
        referer = _referer_for_url(u)
    
    # Build the origin for our own /api/seg-proxy URLs (so the browser can reach them)
    host_header = (request.headers.get("x-forwarded-host") if request else None) or \
                  (request.headers.get("host", "") if request else "")
    scheme = "https" if (host_header and "hf.space" in host_header) else "http"
    if request and request.url.scheme == "https" and "127.0.0.1" not in host_header:
        scheme = "https"
    seg_proxy_origin = f"{scheme}://{host_header}" if host_header else ""
    
    # Vidstack/HLS.js sends a HEAD request before GET to probe reachability.
    # Respond with just the headers — no upstream fetch needed, saves bandwidth.
    # Without this, HEAD returns 405 Method Not Allowed and Vidstack bails out.
    if request and request.method == "HEAD":
        return Response(
            content=b"",
            media_type="application/vnd.apple.mpegurl",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                "Access-Control-Allow-Headers": "*",
                "Cache-Control": "no-cache",
            },
        )
    
    try:
        # Build headers. If referer is empty (e.g. for hianime proxy URLs
        # that use apiKey JWT instead of Referer), don't send a Referer
        # header at all — some servers reject requests with weird referers.
        headers = {
            "Accept": "*/*",
        }
        if referer:
            headers["Referer"] = referer
        # Override Accept — default req_ctx sends Accept: application/json
        # which some CDNs reject for m3u8/segment requests.
        # Don't send Origin header — some CDNs (ravok.buzz, fetch.nexabloom.top)
        # reject requests where Origin doesn't match the actual request origin.
        # Referer alone is enough.
        
        resp = await STATE["req_ctx"].get(u, headers=headers, timeout=15000)
        if resp.status != 200:
            print(f"[MANIFEST PROXY] Upstream {u[:80]} returned {resp.status}")
            raise HTTPException(status_code=resp.status, detail=f"Upstream returned {resp.status}")
        
        body = await resp.text()
        if "#EXTM3U" not in body:
            raise HTTPException(status_code=500, detail="Upstream didn't return a valid m3u8 manifest")
        
        # Rewrite ALL URLs in the manifest (URI attrs, KEY_URI, plain URL lines,
        # hianime /proxy/m3u8?... URLs) to point through /api/seg-proxy so the
        # browser can fetch them with the correct Referer. This is what makes
        # multi-audio + multi-subtitle manifests actually play in the browser.
        rewritten = _rewrite_manifest_urls(body, u, referer, seg_proxy_origin)
        
        # Debug: log rewritten manifest so we can see what URLs end up in the response
        if os.environ.get("BINGR_DEBUG_MANIFEST", ""):
            print(f"[MANIFEST PROXY] Rewritten manifest:\n{rewritten[:500]}...")
        return PlainTextResponse(
            rewritten,
            media_type="application/vnd.apple.mpegurl",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-cache",  # don't cache — segments are time-limited
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"[MANIFEST PROXY] {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Manifest proxy error: {e}")


# ---------------------------------------------------------------------------
# Streaming proxy for Moviebox-API (and other sources with custom headers)
# =============================================================================
# Use case: Moviebox-API returns stream URLs that REQUIRE specific headers
# (User-Agent, Referer, Origin) — the browser can't set Referer/Origin from
# JS, so direct browser fetch → 429 Too Many Requests.
#
# Solution: wrap the stream URL through this proxy. Our backend fetches it
# with the correct headers server-side, streams bytes back to the browser
# with CORS + Range support (for video seeking).
#
# Frontend integration:
#   const proxyUrl = `${HF_URL}/api/stream-proxy?` + new URLSearchParams({
#     u: stream.url,
#     user_agent: stream.headers["User-Agent"],
#     referer: stream.headers["Referer"],
#     origin: stream.headers["Origin"],
#   });
#   videoElement.src = proxyUrl;  // works for MP4
#   hls.loadSource(proxyUrl);     // works for HLS .m3u8
# =============================================================================
@app.api_route("/api/stream-proxy", methods=["GET", "HEAD", "OPTIONS"])
async def stream_proxy(
    request: Request,
    u: str,
    user_agent: Optional[str] = "Mozilla/5.0",
    referer: Optional[str] = None,
    origin: Optional[str] = None,
):
    """Streaming proxy for video URLs that require custom headers (Referer,
    Origin, User-Agent) the browser can't set.

    Supports:
      - HTTP Range requests (for video seeking — `Range: bytes=0-1023`)
      - HEAD probes (HLS.js / video player sends HEAD before GET)
      - CORS preflight
      - Streaming responses (doesn't load full file into memory — important
        for large MP4 files like 5GB movies)

    Query params:
      u           — the upstream stream URL (URL-encoded)
      user_agent  — User-Agent header to send upstream (URL-encoded)
      referer     — Referer header to send upstream (URL-encoded)
      origin      — Origin header to send upstream (URL-encoded)
    """
    # CORS preflight
    if request.method == "OPTIONS":
        return Response(
            content=b"",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Max-Age": "86400",
            },
        )

    if not u:
        raise HTTPException(status_code=400, detail="Missing ?u= parameter")

    # Build upstream headers — only send what's provided
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "identity",  # don't accept gzip — we're streaming raw bytes
    }
    if user_agent:
        headers["User-Agent"] = user_agent
    if referer:
        headers["Referer"] = referer
    if origin:
        headers["Origin"] = origin

    # Forward Range header (for video seeking)
    range_header = request.headers.get("range")
    if range_header:
        headers["Range"] = range_header

    # HEAD probe — make a HEAD request to upstream, return headers only
    if request.method == "HEAD":
        try:
            async with httpx.AsyncClient() as client:
                r = await client.head(u, headers=headers, timeout=10, follow_redirects=True)
                out_headers = {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
                    "Cache-Control": "public, max-age=86400",
                }
                # Pass through video-relevant headers
                for h in ["content-type", "content-length", "accept-ranges", "content-range", "etag", "last-modified"]:
                    v = r.headers.get(h)
                    if v:
                        out_headers[h.title()] = v
                return Response(content=b"", status_code=r.status_code, headers=out_headers)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"HEAD probe error: {e}")

    # GET — stream the bytes back
    try:
        # httpx.AsyncClient with stream() — doesn't load full body into memory,
        # streams chunks. Important for large MP4 files (5GB+).
        async with httpx.AsyncClient(follow_redirects=True) as client:
            async with client.stream("GET", u, headers=headers, timeout=60.0) as r:
                if r.status_code != 200 and r.status_code != 206 and r.status_code != 304:
                    # Forward the upstream status code so the player can handle 4xx/5xx
                    raise HTTPException(
                        status_code=r.status_code,
                        detail=f"Upstream {r.status_code} for {u[:80]}",
                    )

                # Build response headers — pass through video-relevant ones
                out_headers = {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges, ETag",
                    "Cache-Control": "public, max-age=86400",
                }
                for h in ["content-type", "content-length", "accept-ranges", "content-range", "etag", "last-modified"]:
                    v = r.headers.get(h)
                    if v:
                        out_headers[h.title()] = v

                # StreamingResponse — Starlette streams the chunks back without
                # buffering the whole body in memory
                async def stream_bytes():
                    try:
                        async for chunk in r.aiter_raw():
                            yield chunk
                    except Exception as e:
                        print(f"[STREAM PROXY] stream error: {e}")
                        # If the client disconnects, we'll get an exception — fine.

                return StreamingResponse(
                    stream_bytes(),
                    status_code=r.status_code,
                    headers=out_headers,
                )
    except HTTPException:
        raise
    except Exception as e:
        print(f"[STREAM PROXY] {e} for {u[:80]}")
        raise HTTPException(status_code=500, detail=f"Stream proxy error: {e}")


@app.api_route("/api/seg-proxy", methods=["GET", "HEAD", "OPTIONS"])
async def seg_proxy(u: str, referer: Optional[str] = None, request: Request = None):
    """Fetch a single segment (or sub-playlist, or .ts/.m4s/.vtt/.key) with the
    correct Referer header, stream the bytes back to the browser with CORS.

    This is called by HLS.js for every URL we rewrote in /api/manifest-proxy.

    TWO MODES:

    1. **Binary mode** (original behavior): upstream returns a .ts/.m4s/.vtt
       segment or encryption key. We stream raw bytes through with CORS +
       24h browser cache.

    2. **Sub-playlist mode** (NEW — enables multi-audio + multi-subtitle):
       upstream returns an m3u8 sub-playlist (audio-only playlist from an
       #EXT-X-MEDIA AUDIO URI, or a subtitle playlist from a SUBTITLES URI).
       We detect this by Content-Type OR by sniffing the body for #EXTM3U,
       rewrite the inner URLs through /api/seg-proxy (recursively), and
       return as application/vnd.apple.mpegurl with CORS.

       Without sub-playlist mode, multi-audio is silently broken — HLS.js
       fetches the audio playlist URL we proxied, but the segment URLs inside
       still need a Referer the browser can't set, so audio playback fails.

    Query params:
      u        — the upstream segment URL (URL-encoded)
      referer  — the Referer header to send upstream (URL-encoded)
    """
    if not u:
        raise HTTPException(status_code=400, detail="Missing ?u= parameter")
    # Only fall back to default referer if param was MISSING (None).
    # Empty string "" (for hianime proxy URLs that use apiKey, not Referer)
    # is preserved as-is.
    if referer is None:
        referer = _referer_for_url(u)
    
    # HEAD probe — Vidstack/HLS.js sends HEAD to check segment availability
    # before sending GET. Respond with headers only, no upstream fetch.
    if request and request.method == "HEAD":
        return Response(
            content=b"",
            media_type="video/mp2t",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                "Access-Control-Allow-Headers": "*",
                "Cache-Control": "public, max-age=86400",
            },
        )
    
    try:
        # Build headers — only send Referer if non-empty (hianime proxy URLs
        # don't need Referer, they use apiKey JWT).
        headers = {
            "Accept": "*/*",
        }
        if referer:
            headers["Referer"] = referer
        
        resp = await STATE["req_ctx"].get(u, headers=headers, timeout=30000)
        if resp.status != 200:
            print(f"[SEG PROXY] Upstream {u[:80]} returned {resp.status}")
            raise HTTPException(status_code=resp.status, detail=f"Upstream returned {resp.status} for {u[:80]}")
        
        body = await resp.body()
        content_type = (resp.headers.get("content-type") or "video/mp2t").lower()

        # ---- Sub-playlist detection ----
        # An m3u8 sub-playlist can come back with various content-types:
        #   - application/vnd.apple.mpegurl  (correct)
        #   - audio/x-mpegurl / application/x-mpegurl  (also valid)
        #   - text/plain  (some CDNs are lazy)
        #   - video/mp2t  (downright wrong, but happens)
        # So we ALSO sniff the body for the #EXTM3U magic.
        is_m3u8 = (
            "mpegurl" in content_type
            or "x-mpegurl" in content_type
            or (len(body) >= 7 and body[:7] == b"#EXTM3U")
        )

        if is_m3u8:
            # It's a sub-playlist (audio-only or subtitle). Rewrite its inner
            # URLs through /api/seg-proxy so the browser can fetch the
            # sub-playlist's segments with the correct Referer.
            try:
                text = body.decode("utf-8", errors="replace")
            except Exception:
                text = body.decode("latin-1", errors="replace")

            host_header = (request.headers.get("x-forwarded-host") if request else None) or \
                          (request.headers.get("host", "") if request else "")
            scheme = "https" if (host_header and "hf.space" in host_header) else "http"
            if request and request.url.scheme == "https" and "127.0.0.1" not in host_header:
                scheme = "https"
            seg_proxy_origin = f"{scheme}://{host_header}" if host_header else ""

            rewritten = _rewrite_manifest_urls(text, u, referer, seg_proxy_origin)

            if os.environ.get("BINGR_DEBUG_MANIFEST", ""):
                print(f"[SEG PROXY] Sub-playlist rewritten:\n{rewritten[:300]}...")

            return PlainTextResponse(
                rewritten,
                media_type="application/vnd.apple.mpegurl",
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Cache-Control": "no-cache",
                },
            )

        # ---- Binary mode — pass through segment bytes ----
        # Pass through some useful headers
        out_headers = {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=86400",  # 24h browser cache — segments are immutable
        }
        # Preserve content-length if upstream sent it (helps HLS.js size estimation)
        if "content-length" in {k.lower() for k in resp.headers.keys()}:
            try:
                cl = resp.headers.get("content-length")
                if cl:
                    out_headers["Content-Length"] = cl
            except Exception:
                pass
        
        return Response(
            content=body,
            media_type=content_type,
            headers=out_headers,
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"[SEG PROXY] {e} for {u[:80]}")
        raise HTTPException(status_code=500, detail=f"Seg proxy error: {e}")


@app.get("/api/cache/stats")
async def cache_stats():
    """Debug endpoint — view what's cached without exposing the streams themselves."""
    return {
        "episodes_cached": list(CACHE["episodes"].keys()),
        "series_routes_locked": CACHE["series_templates"],
        "total_episodes": len(CACHE["episodes"]),
        "total_series": len(CACHE["series_templates"]),
    }


@app.get("/api/debug/warm-page")
async def debug_warm_page():
    """Debug — check the warm API page state + try a real fetch() to Bingr.
    Used to diagnose why direct API fails on HF Spaces."""
    info = {
        "api_page_alive": False,
        "api_page_url": None,
        "api_page_cookies": [],
        "browser_warm": STATE["browser"] is not None,
        "test_fetch_result": None,
    }
    page = STATE.get("api_page")
    if page is None:
        info["error"] = "api_page is None — _warm_api_page() failed at startup"
        return info
    try:
        info["api_page_url"] = page.url
        info["api_page_alive"] = True
    except Exception as e:
        info["error"] = f"page dead: {e}"
        return info

    # Get cookies on the page's context
    try:
        cookies = await page.context.cookies()
        info["api_page_cookies"] = [
            {"name": c["name"], "value": c.get("value", "")[:50], "domain": c.get("domain")}
            for c in cookies
        ]
    except Exception as e:
        info["cookies_error"] = str(e)

    # Try ONE real fetch() to Bingr's API via page.evaluate
    try:
        result = await page.evaluate("""
        async () => {
            try {
                const r = await fetch('https://api.bingr.one/api/version', {
                    method: 'GET',
                    credentials: 'include',
                });
                return { ok: r.ok, status: r.status, body: await r.text() };
            } catch (e) {
                return { error: String(e) };
            }
        }
        """)
        info["test_fetch_result"] = result
    except Exception as e:
        info["test_fetch_result"] = {"error": str(e)}

    # Test multiple POST variants to find which one bypasses the CORS preflight issue.
    # The "Failed to fetch" with application/json suggests the OPTIONS preflight
    # is being blocked by Cloudflare for HF's IP. Try variants that DON'T trigger
    # a preflight (text/plain, no headers) to see if we can bypass it.
    test_variants = []

    # Variant 1: application/json (triggers preflight) — known to fail
    test_variants.append(("json_ct", """
        async () => {
            try {
                const r = await fetch('https://api.bingr.one/api/stream', {
                    method: 'POST',
                    mode: 'cors',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({srv:'s70',t:'movie',id:'969681',query:{title:'Spider-Man: Brand New Day',year:'2026'}}),
                    credentials: 'omit',
                });
                const t = await r.text();
                return { ok: r.ok, status: r.status, body: t.slice(0, 200) };
            } catch (e) { return { error: String(e) }; }
        }
    """))

    # Variant 2: text/plain (SIMPLE request, no preflight) — body is JSON-as-text
    test_variants.append(("text_plain_ct", """
        async () => {
            try {
                const r = await fetch('https://api.bingr.one/api/stream', {
                    method: 'POST',
                    mode: 'cors',
                    headers: { 'Content-Type': 'text/plain' },
                    body: JSON.stringify({srv:'s70',t:'movie',id:'969681',query:{title:'Spider-Man: Brand New Day',year:'2026'}}),
                    credentials: 'omit',
                });
                const t = await r.text();
                return { ok: r.ok, status: r.status, body: t.slice(0, 200) };
            } catch (e) { return { error: String(e) }; }
        }
    """))

    # Variant 3: no Content-Type (browser defaults to text/plain)
    test_variants.append(("no_ct", """
        async () => {
            try {
                const r = await fetch('https://api.bingr.one/api/stream', {
                    method: 'POST',
                    mode: 'cors',
                    body: JSON.stringify({srv:'s70',t:'movie',id:'969681',query:{title:'Spider-Man: Brand New Day',year:'2026'}}),
                    credentials: 'omit',
                });
                const t = await r.text();
                return { ok: r.ok, status: r.status, body: t.slice(0, 200) };
            } catch (e) { return { error: String(e) }; }
        }
    """))

    # Variant 4: application/x-www-form-urlencoded (simple, no preflight)
    test_variants.append(("form_ct", """
        async () => {
            try {
                const r = await fetch('https://api.bingr.one/api/stream', {
                    method: 'POST',
                    mode: 'cors',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: 'srv=s70&t=movie&id=969681',
                    credentials: 'omit',
                });
                const t = await r.text();
                return { ok: r.ok, status: r.status, body: t.slice(0, 200) };
            } catch (e) { return { error: String(e) }; }
        }
    """))

    # Variant 5: explicit OPTIONS preflight test
    test_variants.append(("options_preflight", """
        async () => {
            try {
                const r = await fetch('https://api.bingr.one/api/stream', {
                    method: 'OPTIONS',
                    mode: 'cors',
                    headers: {
                        'Access-Control-Request-Method': 'POST',
                        'Access-Control-Request-Headers': 'content-type',
                    },
                    credentials: 'omit',
                });
                const hdrs = {};
                r.headers.forEach((v, k) => { hdrs[k] = v; });
                return { ok: r.ok, status: r.status, headers: hdrs };
            } catch (e) { return { error: String(e) }; }
        }
    """))

    info["test_variants"] = {}
    for name, js_code in test_variants:
        try:
            r = await page.evaluate(js_code)
            info["test_variants"][name] = r
        except Exception as e:
            info["test_variants"][name] = {"error": str(e)}

    return info


@app.post("/api/cache/clear")
async def cache_clear():
    """Wipe the cache (admin/debug)."""
    CACHE["episodes"].clear()
    CACHE["series_templates"].clear()
    save_persistent_cache(CACHE)
    return {"status": "cleared"}


# ---------------------------------------------------------------------------
# Local run: `python scraper_api.py`
# (No Docker needed — uvicorn runs in-process)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    print("=" * 60)
    print("  Bingr Scraper API — Local Mode")
    print(f"  Open: http://127.0.0.1:{SERVER_PORT}/api/health")
    print(f"  Docs: http://127.0.0.1:{SERVER_PORT}/docs")
    print("=" * 60)
    uvicorn.run(
        "scraper_api:app",
        host=SERVER_HOST,
        port=SERVER_PORT,
        reload=False,  # reload breaks lifespan state — never use with Playwright
        workers=1,    # Chromium + global state MUST be single-worker
        log_level="info",
    )
