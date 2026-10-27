#!/usr/bin/env python3
"""
Bingr Direct API Client — uses a warm Chrome page to bypass Bingr's bot
detection.

Discovery (from running bingr_trace.py on a movie URL):
  Bingr's frontend POSTs to https://api.bingr.one/api/stream with a JSON body
  like {"srv":"s40","t":"movie","id":"969681","query":{"title":"...","year":"..."}}
  and gets back {"scraperName":"DarkMatter","sources":[{"url":"...m3u8",...}],...}

  No cookies. No auth headers. No CSRF token. No signatures.

  BUT — Bingr's API silently blocks server-side HTTP clients (httpx, curl,
  Playwright's standalone req_ctx) when called from cloud IPs like HF Spaces.
  It returns HTTP 200 with `{"meta":null,"source":null,"subtitles":[]}` for
  all 8 srv values.

  Solution: use a WARM CHROME PAGE that's already navigated to bingr.one.
  Fire `fetch()` calls from INSIDE that page's JS context via page.evaluate().
  This uses Chrome's actual TLS fingerprint + the bingr.browser_session=1
  cookie that Bingr's frontend sets on page load. Bingr can't tell the
  difference between our fetch() and Bingr's own frontend JS fetches.

  The warm page is opened once at scraper_api.py startup (lifespan event)
  and stored in STATE["api_page"]. Subsequent scrapes reuse it — no per-
  request page load cost.

  Bingr has 8 known scrapers (probed from srv 1-99):
    s31 -> Orion          (proxy.streamrip.fun)
    s40 -> DarkMatter      (img.rousav.tech)
    s50 -> Delta English  (img1.hscow.com)
    s51 -> Delta Hindi    (Hindi audio)
    s62 -> Bastion        (9 sources per movie)
    s70 -> Polaris        (14 sources per movie — Cloudflare Workers + CloudFront!)
    s80 -> Moscow         (proxy.streamrip.fun)
    s81 -> Novo            (proxy.streamrip.fun)

  We fire all 8 in PARALLEL using Promise.all() inside the page's JS —
  one round-trip to the page, 8 parallel HTTP requests in JS land. ~3-5s
  per scrape (vs the 22s silent browser fallback).

Usage:
  from bingr_direct import resolve_via_api
  # page is a Playwright Page already navigated to bingr.one, kept warm
  # at scraper_api.py startup
  result = await resolve_via_api(target_url, page=STATE["api_page"])
"""
import asyncio
import json
import re
import time
from typing import Optional

# httpx is only used as a fallback when no page is provided (e.g., local
# testing without a warm browser). On HF Spaces, we always pass a page.
try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BINGR_API = "https://api.bingr.one"

# Common headers — sent on every request. Matches what a real Chrome browser
# sends. When using page.evaluate(fetch()), the page automatically adds
# sec-ch-ua, cookies, and other headers — we only need to set Content-Type
# and Accept explicitly.
COMMON_HEADERS = {
    "Referer": "https://bingr.one/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

POST_HEADERS = {
    **COMMON_HEADERS,
    "Content-Type": "application/json",
}

# All 8 known Bingr scrapers. Fire them all in parallel — Bingr handles the
# invalid ones (returns 200 with empty sources), so we just filter those out.
ALL_SRVS = ["s31", "s40", "s50", "s51", "s62", "s70", "s80", "s81"]

# Per-request timeouts (seconds).
DETAILS_TIMEOUT = 6.0
EPISODES_TIMEOUT = 6.0
STREAM_PROBE_TIMEOUT = 4.0


# ---------------------------------------------------------------------------
# URL parser
# ---------------------------------------------------------------------------
def parse_watch_url(url: str):
    """Extract type (tv/movie/anime), id, season, episode from a Bingr URL."""
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
# HTTP via page.evaluate(fetch()) — uses Chrome's TLS fingerprint + cookies
# ---------------------------------------------------------------------------
# JavaScript that fires a POST request from inside the page context.
# Runs entirely in the browser — Chrome's TLS, Chrome's cookies, Chrome's
# sec-ch-ua headers. Bingr's bot detection can't distinguish this from
# Bingr's own frontend JS fetches.
#
# CRITICAL: use credentials: 'omit' (NOT 'include'). Bingr's own frontend
# uses 'omit' (we verified this from bingr_trace.py). With 'include',
# the browser fires a stricter CORS preflight that Bingr's API rejects,
# resulting in "TypeError: Failed to fetch".
_JS_FETCH_POST = """
async ([url, body, timeoutMs]) => {
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
        const r = await fetch(url, {
            method: 'POST',
            mode: 'cors',
            headers: {
                'Content-Type': 'application/json',
                'Accept': '*/*',
            },
            body: JSON.stringify(body),
            signal: controller.signal,
            credentials: 'omit',  // matches Bingr's frontend — avoids stricter CORS preflight
        });
        clearTimeout(timeoutId);
        const status = r.status;
        let data = null;
        try { data = await r.json(); } catch (e) { data = null; }
        return { status, data };
    } catch (e) {
        return { status: 0, data: null, error: String(e) };
    }
}
"""

_JS_FETCH_GET = """
async ([url, timeoutMs]) => {
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
        const r = await fetch(url, {
            method: 'GET',
            mode: 'cors',
            headers: { 'Accept': '*/*' },
            signal: controller.signal,
            credentials: 'omit',
        });
        clearTimeout(timeoutId);
        const status = r.status;
        let data = null;
        try { data = await r.json(); } catch (e) { data = null; }
        return { status, data };
    } catch (e) {
        return { status: 0, data: null, error: String(e) };
    }
}
"""

# JavaScript that fires MULTIPLE POST requests in parallel from inside the
# page context. One page.evaluate() round-trip → 8 parallel HTTP requests.
# This is the FAST PATH — much faster than 8 separate page.evaluate() calls
# because we save 8 round-trips between Python and the page.
_JS_FETCH_POST_BATCH = """
async ([calls, timeoutMs]) => {
    // calls is an array of {url, body}
    const results = await Promise.all(calls.map(async ({url, body}) => {
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
            const r = await fetch(url, {
                method: 'POST',
                mode: 'cors',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': '*/*',
                },
                body: JSON.stringify(body),
                signal: controller.signal,
                credentials: 'omit',  // matches Bingr's frontend
            });
            clearTimeout(timeoutId);
            const status = r.status;
            let data = null;
            try { data = await r.json(); } catch (e) { data = null; }
            return { url, status, data };
        } catch (e) {
            return { url, status: 0, data: null, error: String(e) };
        }
    }));
    return results;
}
"""


async def _do_get_via_page(page, url, timeout_sec=10.0):
    """GET request via page.evaluate(fetch()). Uses Chrome's TLS + cookies."""
    try:
        result = await page.evaluate(_JS_FETCH_GET, [url, int(timeout_sec * 1000)])
        if not result or result.get("status") != 200:
            return None
        return result.get("data")
    except Exception as e:
        print(f"  [DIRECT] page fetch GET error: {e}")
        return None


async def _do_post_via_page(page, url, body, timeout_sec=10.0):
    """POST request via page.evaluate(fetch()). Uses Chrome's TLS + cookies."""
    try:
        result = await page.evaluate(_JS_FETCH_POST, [url, body, int(timeout_sec * 1000)])
        if not result or result.get("status") != 200:
            return None
        return result.get("data")
    except Exception as e:
        print(f"  [DIRECT] page fetch POST error: {e}")
        return None


async def _do_post_batch_via_page(page, calls, timeout_sec=10.0):
    """Fire MULTIPLE POST requests in parallel from inside the page.

    Args:
        page: Playwright Page (already navigated to bingr.one)
        calls: list of {url, body} dicts
        timeout_sec: per-request timeout

    Returns: list of {url, status, data} dicts in same order as calls.
    """
    try:
        results = await page.evaluate(
            _JS_FETCH_POST_BATCH,
            [calls, int(timeout_sec * 1000)],
        )
        return results or []
    except Exception as e:
        print(f"  [DIRECT] page fetch BATCH error: {e}")
        return [{"url": c["url"], "status": 0, "data": None, "error": str(e)} for c in calls]


# ---------------------------------------------------------------------------
# httpx fallback (local testing only — doesn't work on HF Spaces due to
# Bingr's cloud IP bot detection)
# ---------------------------------------------------------------------------
async def _do_get_via_httpx(client, url, timeout_sec=10.0):
    try:
        r = await client.get(url, headers=COMMON_HEADERS, timeout=timeout_sec)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        print(f"  [DIRECT] httpx GET error: {e}")
        return None


async def _do_post_via_httpx(client, url, body, timeout_sec=10.0):
    try:
        r = await client.post(url, json=body, headers=POST_HEADERS, timeout=timeout_sec)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        print(f"  [DIRECT] httpx POST error: {e}")
        return None


# ---------------------------------------------------------------------------
# Endpoint helpers
# ---------------------------------------------------------------------------
async def _fetch_details(client, t_type: str, t_id: str, use_page: bool):
    """GET /api/details/{type}/{id} → (title, year, raw_data_dict)."""
    if t_type == "tv":
        url = f"{BINGR_API}/api/details/tv/{t_id}?v=1"
    elif t_type == "anime":
        url = f"{BINGR_API}/api/anime/{t_id}?v=1"
    else:  # movie
        url = f"{BINGR_API}/api/details/movie/{t_id}?v=1"

    if use_page:
        d = await _do_get_via_page(client, url, timeout_sec=DETAILS_TIMEOUT)
    else:
        d = await _do_get_via_httpx(client, url, timeout_sec=DETAILS_TIMEOUT)

    if d is None:
        return None, None, None
    return (
        d.get("title") or d.get("name"),
        d.get("year") or d.get("releaseDate"),
        d,
    )


async def _fetch_episodes(client, t_id: str, t_season: str, use_page: bool):
    """GET /api/episodes/{id}/{season} → list of episode summary dicts."""
    if not t_season:
        return []
    url = f"{BINGR_API}/api/episodes/{t_id}/{t_season}"
    if use_page:
        d = await _do_get_via_page(client, url, timeout_sec=EPISODES_TIMEOUT)
    else:
        d = await _do_get_via_httpx(client, url, timeout_sec=EPISODES_TIMEOUT)
    if d is None:
        return []
    raw_eps = d.get("episodes") if isinstance(d, dict) else d
    if not isinstance(raw_eps, list):
        return []
    out = []
    for idx, ep in enumerate(raw_eps, 1):
        if isinstance(ep, dict):
            ep_num = ep.get("episode") or ep.get("number") or idx
            out.append({
                "episode": ep_num,
                "title": ep.get("title") or ep.get("name") or f"Episode {ep_num}",
                "rating": ep.get("rating", ""),
                "air_date": ep.get("air_date", ""),
            })
    return out


def _parse_probe_response(d, srv):
    """Parse one POST /api/stream response into (sources, subs, scraper_name)."""
    if not d:
        return [], [], srv
    scraper = d.get("scraperName", srv)
    sources = []
    for src in d.get("sources", []) or []:
        if isinstance(src, dict) and src.get("url"):
            sources.append({
                "quality": src.get("quality", "auto"),
                "url": src.get("url"),
                "type": src.get("type", "application/x-mpegurl"),
                "label": src.get("label", scraper),
                "scraper": scraper,
                "api_endpoint": f"{BINGR_API}/api/stream",
            })
    subs = []
    for sub in d.get("subtitles", []) or []:
        if isinstance(sub, dict):
            sub_url = sub.get("url") or sub.get("file") or sub.get("src")
            if sub_url:
                subs.append({
                    "url": sub_url,
                    "label": sub.get("label") or sub.get("lang") or sub.get("language") or "English",
                    "lang": (sub.get("lang") or sub.get("language") or "en")[:2].lower(),
                })
    return sources, subs, scraper


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
async def resolve_via_api(target_url: str, page=None, req_ctx=None) -> Optional[dict]:
    """Direct API resolver — uses a warm Chrome page to fire fetch() calls.

    Args:
        target_url: Bingr watch URL like
            "https://bingr.one/watch/movie/969681" or
            "https://bingr.one/watch/tv/1396/1/1"
        page: Playwright Page already navigated to bingr.one (RECOMMENDED).
            fetch() calls run inside this page's JS context, using Chrome's
            actual TLS fingerprint + the bingr.browser_session cookie. This
            bypasses Bingr's cloud IP bot detection that blocks httpx/curl/
            Playwright's standalone req_ctx.
        req_ctx: Playwright APIRequestContext (FALLBACK only — uses
            Playwright's HTTP client which gets blocked on cloud IPs).

    Returns a partial result dict matching the shape that scraper_api's
    resolve_media() expects to feed into _build_response():
      {
        "captured_streams": [...],
        "captured_subs": [...],
        "metadata": {...},
        "seasons_list": [...],
        "episodes_summary": [...],
        "ep_key": "movie_969681_1_1",
      }

    Returns None if direct API failed entirely.
    """
    parsed = parse_watch_url(target_url)
    if not parsed:
        return None

    t_type = parsed["type"]
    t_id = parsed["id"]
    t_season = parsed["season"]
    t_ep = parsed["episode"]
    ep_key = f"{t_type}_{t_id}_{t_season}_{t_ep}"

    print(f"\n[DIRECT API] {t_type.upper()} id={t_id} S{t_season} E{t_ep}")

    started = time.time()

    # Pick the HTTP client — prefer warm page (real Chrome TLS), fall back
    # to httpx for local testing
    if page is not None:
        client = page
        use_page = True
        client_label = "warm Chrome page"
    elif req_ctx is not None:
        # Fall back to req_ctx (Playwright standalone HTTP client — gets
        # blocked on cloud IPs but works locally)
        client = req_ctx
        use_page = False
        client_label = "Playwright req_ctx (may be blocked on HF)"
    elif HAS_HTTPX:
        client = httpx.AsyncClient(http2=False)
        client_cm = client
        client = await client.__aenter__()
        use_page = False
        client_label = "httpx (works locally, blocked on HF)"
    else:
        print("  [DIRECT API] No HTTP client available")
        return None

    try:
        # 1. Fetch details (and episodes for TV) — these are sequential
        if t_type == "tv":
            details_task = asyncio.create_task(_fetch_details(client, t_type, t_id, use_page))
            episodes_task = asyncio.create_task(_fetch_episodes(client, t_id, t_season, use_page))
            title, year, details_data = await details_task
            episodes_summary = await episodes_task
        else:
            title, year, details_data = await _fetch_details(client, t_type, t_id, use_page)
            episodes_summary = []

        if not title:
            print(f"  [DIRECT API] No title in details response — direct path failed")
            return None

        # 2. Build the 8 srv probe bodies
        probe_bodies = []
        for srv in ALL_SRVS:
            query = {"title": title or "", "year": str(year) if year else ""}
            if t_type == "tv":
                try:
                    query["season"] = int(t_season) if str(t_season).isdigit() else 1
                    query["episode"] = int(t_ep) if str(t_ep).isdigit() else 1
                except Exception:
                    query["season"] = 1
                    query["episode"] = 1
                body = {"srv": srv, "t": "tv", "id": t_id, "query": query}
            else:
                body = {"srv": srv, "t": "movie", "id": t_id, "query": query}
            probe_bodies.append(body)

        # 3. Fire all 8 probes in parallel — using page.evaluate with
        # Promise.all() for one round-trip (fast) or asyncio.gather for httpx
        if use_page:
            # WARM PAGE PATH: one page.evaluate() call, 8 parallel fetches in JS
            calls = [{"url": f"{BINGR_API}/api/stream", "body": body} for body in probe_bodies]
            results_raw = await _do_post_batch_via_page(
                client, calls, timeout_sec=STREAM_PROBE_TIMEOUT,
            )
            # results_raw is a list of {url, status, data} dicts
            # Convert to (sources, subs, scraper_name) tuples
            parsed_results = []
            for i, r in enumerate(results_raw):
                if r and r.get("status") == 200 and r.get("data"):
                    parsed_results.append(_parse_probe_response(r["data"], ALL_SRVS[i]))
                else:
                    parsed_results.append(([], [], ALL_SRVS[i]))
            results_raw = parsed_results
        else:
            # HTTPX PATH: 8 parallel asyncio.gather calls
            async def _probe_one(srv, body):
                d = await _do_post_via_httpx(client, f"{BINGR_API}/api/stream", body, timeout_sec=STREAM_PROBE_TIMEOUT)
                return _parse_probe_response(d, srv)
            tasks = [_probe_one(srv, body) for srv, body in zip(ALL_SRVS, probe_bodies)]
            results_raw = await asyncio.gather(*tasks, return_exceptions=True)

    finally:
        if page is None and req_ctx is None and HAS_HTTPX:
            await client_cm.__aexit__(None, None, None)

    # 4. Combine all sources, dedup by URL
    all_streams = []
    all_subs = []
    scrapers_used = []
    for result in results_raw:
        if isinstance(result, Exception) or not isinstance(result, tuple):
            continue
        streams, subs, scraper_name = result
        if streams:
            all_streams.extend(streams)
            all_subs.extend(subs)
            if scraper_name not in scrapers_used:
                scrapers_used.append(scraper_name)

    seen_urls = set()
    deduped_streams = []
    for s in all_streams:
        if s["url"] not in seen_urls:
            seen_urls.add(s["url"])
            deduped_streams.append(s)

    seen_sub_urls = set()
    deduped_subs = []
    for s in all_subs:
        if s["url"] not in seen_sub_urls:
            seen_sub_urls.add(s["url"])
            deduped_subs.append(s)

    if not deduped_streams:
        print(f"  [DIRECT API] No streams from any srv — falling back to browser")
        return None

    # 5. Sort streams — Polaris first, then by quality
    SCRAPER_PRIORITY = {"Polaris": 0, "Bastion": 1, "DarkMatter": 2, "Delta (English)": 3}
    def sort_key(s):
        prio = SCRAPER_PRIORITY.get(s.get("scraper", ""), 9)
        q = s.get("quality", "")
        m = re.search(r"(\d+)", q)
        qnum = -int(m.group(1)) if m else 0
        return (prio, qnum)
    deduped_streams.sort(key=sort_key)

    # 6. Build metadata
    metadata = {}
    seasons_list = []
    if isinstance(details_data, dict):
        metadata = {
            "id": details_data.get("id") or t_id,
            "type": t_type,
            "season": int(t_season) if str(t_season).isdigit() else 1,
            "episode": int(t_ep) if str(t_ep).isdigit() else 1,
            "title": details_data.get("title") or title,
            "year": details_data.get("year") or "",
            "genres": details_data.get("genres", []),
            "rating": details_data.get("rating") or "N/A",
            "overview": details_data.get("overview") or "",
        }
        if isinstance(details_data.get("seasons"), list):
            for s in details_data["seasons"]:
                if isinstance(s, dict) and s.get("season", 0) > 0:
                    seasons_list.append({
                        "season": s.get("season"),
                        "name": s.get("name") or f"Season {s.get('season')}",
                        "episodes_count": s.get("episodes", 0),
                    })

    elapsed_ms = int((time.time() - started) * 1000)
    print(f"  [DIRECT API] Got {len(deduped_streams)} streams from {len(scrapers_used)} scrapers via {client_label} in {elapsed_ms}ms")
    print(f"  [DIRECT API] Scrapers used: {', '.join(scrapers_used)}")
    if deduped_streams:
        primary = deduped_streams[0]
        print(f"  [DIRECT API] Top pick: [{primary.get('quality')}] {primary.get('scraper')}")

    return {
        "captured_streams": deduped_streams,
        "captured_subs": deduped_subs,
        "metadata": metadata,
        "seasons_list": seasons_list,
        "episodes_summary": episodes_summary,
        "ep_key": ep_key,
        "scrapers_used": scrapers_used,
        "elapsed_ms": elapsed_ms,
    }

