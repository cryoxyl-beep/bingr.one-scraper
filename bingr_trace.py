#!/usr/bin/env python3
"""
Bingr API Trace — inspects what Bingr's frontend sends to api.bingr.one
WITHOUT opening DevTools (which Bingr's anti-debugger kills).

Why this exists:
  Bingr's frontend probably sets a session cookie / signed token that the
  bare API rejects for movies. We can't see those headers via DevTools
  because Bingr force-closes the tab when DevTools opens. But Playwright
  runs the page FROM OUTSIDE the browser, so we can intercept every
  request and response without triggering the anti-debugger.

Usage:
    python bingr_trace.py https://bingr.one/watch/movie/969681
    python bingr_trace.py https://bingr.one/watch/tv/1396/1/1

Output:
  - Prints every request/response to api.bingr.one to the console
  - Saves the full trace to bingr_trace_YYYYMMDD_HHMMSS.jsonl
  - Dumps cookies before + after navigation so you see what gets set

What to look for in the output:
  1. The actual API URL Bingr hits for movies (we currently guess)
  2. The Cookie: header on those requests — that's the session we need
  3. Any X-* custom headers (CSRF, signature, etc.)
  4. POST body if any
  5. The response JSON — that's what the API actually returns
"""
import asyncio
import json
import sys
from datetime import datetime
from playwright.async_api import async_playwright

LOG_FILE = f"bingr_trace_{datetime.now():%Y%m%d_%H%M%S}.jsonl"
API_HOST = "api.bingr.one"

# Stealth script — same as the main scraper. Disables webdriver flag, fakes
# window.chrome, kills window.close() (Bingr's anti-debugger calls this when
# it thinks you're debugging).
STEALTH_JS = """
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


async def main(target_url: str):
    if "bingr.one/watch/" not in target_url:
        print("ERROR: URL must be a bingr.one/watch/... URL")
        print(f"Example: python bingr_trace.py https://bingr.one/watch/movie/969681")
        sys.exit(1)

    # Clear the log file
    open(LOG_FILE, "w").close()

    pw = await async_playwright().start()
    # headless=False so we don't trigger any "headless detection" Bingr might have.
    # The page just loads normally in a real visible Chrome window.
    browser = await pw.chromium.launch(
        headless=False,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    )

    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 800},
    )
    await context.add_init_script(STEALTH_JS)

    page = await context.new_page()

    def write_entry(entry):
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    async def on_request(request):
        if API_HOST not in request.url:
            return
        try:
            entry = {
                "ts": datetime.now().isoformat(),
                "type": "request",
                "url": request.url,
                "method": request.method,
                "headers": dict(request.headers),
                "post_data": request.post_data,
            }
            write_entry(entry)
            print(f"\n{'='*70}")
            print(f">>> REQUEST {request.method} {request.url}")
            print(f"{'='*70}")
            for k, v in request.headers.items():
                # Truncate long values for console (full value is in the JSONL file)
                v_str = v if len(v) < 200 else v[:200] + f"... ({len(v)} chars total)"
                print(f"  {k}: {v_str}")
            if request.post_data:
                print(f"  BODY: {request.post_data[:500]}")
        except Exception as e:
            print(f"  [TRACE ERR request] {e}")

    async def on_response(response):
        if API_HOST not in response.url:
            return
        try:
            body = ""
            try:
                body = await response.text()
                body = body[:1500]
            except Exception:
                pass
            entry = {
                "ts": datetime.now().isoformat(),
                "type": "response",
                "url": response.url,
                "status": response.status,
                "headers": dict(response.headers),
                "body_preview": body,
            }
            write_entry(entry)
            print(f"\n{'-'*70}")
            print(f"<<< RESPONSE {response.status} {response.url}")
            print(f"{'-'*70}")
            if body:
                print(f"  BODY: {body[:800]}")
        except Exception as e:
            print(f"  [TRACE ERR response] {e}")

    page.on("request", lambda r: asyncio.create_task(on_request(r)))
    page.on("response", lambda r: asyncio.create_task(on_response(r)))

    # Get cookies BEFORE navigation
    cookies_before = await context.cookies()
    print(f"\n{'#'*70}")
    print(f"# Cookies BEFORE navigation ({len(cookies_before)}):")
    print(f"{'#'*70}")
    for c in cookies_before:
        print(f"  {c['name']}={c['value'][:80]}  (domain={c.get('domain')}, http_only={c.get('httpOnly')})")

    print(f"\n{'#'*70}")
    print(f"# Navigating to {target_url}")
    print(f"# The browser window will open — DON'T touch it. Let it run.")
    print(f"{'#'*70}")

    try:
        await page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
    except Exception as e:
        print(f"  [GOTO ERR] {e}")

    # Wait for all API requests to fire
    print("\n# Waiting 25 seconds for all API requests to fire...")
    await page.wait_for_timeout(25000)

    # Get cookies AFTER navigation
    cookies_after = await context.cookies()
    print(f"\n{'#'*70}")
    print(f"# Cookies AFTER navigation ({len(cookies_after)}):")
    print(f"{'#'*70}")
    for c in cookies_after:
        new = " (NEW!)" if not any(b["name"] == c["name"] for b in cookies_before) else ""
        print(f"  {c['name']}={c['value'][:120]}  (domain={c.get('domain')}, http_only={c.get('httpOnly')}, expires={c.get('expires', 'session')}){new}")

    # Print summary
    print(f"\n{'#'*70}")
    print(f"# TRACE COMPLETE")
    print(f"# Full trace saved to: {LOG_FILE}")
    print(f"{'#'*70}")
    print("""
WHAT TO LOOK FOR IN THE OUTPUT ABOVE:

1. The URL pattern Bingr actually hits for movies
   - Is it /api/stream/aphelion-movie/{id}?  Or something else?
   - Does it include query params we're missing?

2. The Cookie: header on those requests
   - Is there a session ID? A cf_clearance cookie?
   - Are there any custom-named cookies like 'bingr_session' or '_b_token'?

3. Any X-* custom headers
   - X-CSRF-Token?  X-Auth-Token?  X-Signature?

4. POST body if any
   - Some APIs need a JSON body with a nonce/timestamp

5. The response JSON
   - Does it actually return stream URLs for movies?
   - Or does it return an auth challenge we need to solve?

Copy the contents of the JSONL file or the console output and paste it back
to me — I'll write the Python code to replicate whatever Bingr's frontend does.
""")

    print("\n# Closing browser in 3 seconds... (Ctrl+C to keep open)")
    try:
        await page.wait_for_timeout(3000)
    except Exception:
        pass

    await browser.close()
    await pw.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python bingr_trace.py https://bingr.one/watch/movie/969681")
        print("       python bingr_trace.py https://bingr.one/watch/tv/1396/1/1")
        sys.exit(1)
    try:
        asyncio.run(main(sys.argv[1]))
    except KeyboardInterrupt:
        print("\nInterrupted — partial trace saved.")
