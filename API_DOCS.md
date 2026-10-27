# Bingr Scraper API — JSON Response Reference

**Version**: 2.0 (Direct API + browser fallback)
**Endpoint**: `POST /api/resolve`
**Content-Type**: `application/json`

Give this entire document to your frontend developer / Google AI Studio so it knows exactly how to consume the API.

---

## Request

### Endpoint
```
POST {BINGR_API_BASE}/api/resolve
```

### Headers
```
Content-Type: application/json
```

### Body
```json
{
  "url": "https://bingr.one/watch/tv/1396/1/1"
}
```

### URL formats accepted
| Bingr URL | Description |
|---|---|
| `https://bingr.one/watch/movie/{tmdb_id}` | A movie |
| `https://bingr.one/watch/tv/{tmdb_id}/{season}/{episode}` | A TV episode |
| `https://bingr.one/watch/anime/{tmdb_id}/{season}/{episode}` | An anime episode (browser fallback only — direct API doesn't support anime yet) |

### Your Vercel route → Bingr URL mapping
Since your site also uses TMDB IDs, the mapping is straightforward:

| Your Vercel route | Bingr URL to scrape |
|---|---|
| `/play/movie/{tmdb_id}` | `https://bingr.one/watch/movie/{tmdb_id}` |
| `/play/series/{tmdb_id}/{season}/{episode}` | `https://bingr.one/watch/tv/{tmdb_id}/{season}/{episode}` |

Example frontend code:
```js
const type = route.startsWith('/play/series') ? 'tv' : 'movie';
const bingrUrl = type === 'tv'
  ? `https://bingr.one/watch/tv/${tmdbId}/${season}/${episode}`
  : `https://bingr.one/watch/movie/${tmdbId}`;
```

---

## Response

### Success (200 OK)

```json
{
  "page_url": "https://bingr.one/watch/tv/1396/1/1",
  "parsed": {
    "type": "tv",
    "id": "1396",
    "season": "1",
    "episode": "1"
  },
  "title": "Breaking Bad",
  "stream_links": [
    {
      "quality": "1080p",
      "url": "https://dash-hls-bridge.johndoesfmc.workers.dev/?url=...&Policy=...&Signature=...&Key-Pair-Id=...",
      "type": "application/x-mpegurl",
      "label": "English — 1080p",
      "scraper": "Polaris",
      "api_endpoint": "https://api.bingr.one/api/stream",
      "raw_url": "https://dash-hls-bridge.johndoesfmc.workers.dev/?url=...&Policy=...&Signature=...&Key-Pair-Id=...",
      "proxied": true,
      "referer_used": "https://bingr.one/"
    },
    ...
  ],
  "subtitles": [
    {
      "url": "https://USERNAME-bingr-scraper.hf.space/api/seg-proxy?u=...&referer=...",
      "label": "English",
      "lang": "en"
    }
  ],
  "fast_manifest_text": null,
  "fast_m3u8_url": null,
  "cached_route": "https://api.bingr.one/api/stream",
  "media_info": {
    "id": 1396,
    "type": "tv",
    "season": 1,
    "episode": 1,
    "title": "Breaking Bad",
    "year": "2008",
    "genres": ["Drama", "Crime", "Thriller"],
    "rating": "9.5",
    "overview": "A high school chemistry teacher diagnosed with inoperable lung cancer..."
  },
  "seasons": [
    {
      "season": 1,
      "name": "Season 1",
      "episodes_count": 7
    },
    ...
  ],
  "episodes": [
    {
      "episode": 1,
      "title": "Pilot",
      "rating": "",
      "air_date": "2008-01-20"
    },
    ...
  ],
  "episode_key": "tv_1396_1_1"
}
```

### Error responses

**400 Bad Request** — URL not a Bingr watch URL:
```json
{
  "detail": "URL must be a bingr.one/watch/... URL"
}
```

**404 Not Found** — No streams resolved:
```json
{
  "detail": {
    "message": "No stream links resolved for this URL.",
    "url": "https://bingr.one/watch/movie/9999999",
    "tried_browser_fallback": true,
    "media_info": {...},
    "episode_key": "movie_9999999_1_1"
  }
}
```

**500 Internal Server Error** — Scraper error:
```json
{
  "detail": "Exception message here"
}
```

---

## Field reference

### Top-level fields

| Field | Type | Description |
|---|---|---|
| `page_url` | string | The Bingr URL you sent in the request body |
| `parsed` | object | Parsed components of the URL (type, id, season, episode) |
| `title` | string | Movie/series title — use this for the page `<title>` |
| `stream_links` | array | **List of playable streams** — see below |
| `subtitles` | array | List of sidecar subtitle files (VTT/SRT) |
| `fast_manifest_text` | string\|null | Cached stripped m3u8 manifest text (usually null — use `stream_links[0].url` instead) |
| `fast_m3u8_url` | string\|null | URL to fetch cached m3u8 (usually null) |
| `cached_route` | string\|null | The Bingr API template that worked (debug) |
| `media_info` | object | TMDB-style metadata (title, year, genres, rating, overview, cast, etc.) |
| `seasons` | array | Seasons list (TV only) — for season selector UI |
| `episodes` | array | Episodes in current season (TV only) — for episode list UI |
| `episode_key` | string | Internal cache key — pass back if you want to hit `/api/fast.m3u8?ep=KEY` |

### `stream_links[]` — the main playable URLs

```typescript
interface StreamLink {
  quality: string;        // "1080p", "720p", "480p", "Auto"
  url: string;           // Playable HLS URL — hand this to your player
  type: string;          // MIME type — usually "application/x-mpegurl"
  label: string;         // Human label — often "English — 1080p" or "Hindi — 720p"
  scraper: string;       // Which Bingr scraper provided it
                         //   ("Polaris" = Cloudflare Workers + CloudFront — best UX)
                         //   ("DarkMatter" = img.rousav.tech — direct CDN, CORS-friendly)
                         //   ("Bastion" = proxy.streamrip.fun — multi-quality)
                         //   ("Orion", "Moscow", "Novo" = streamrip.fun variants)
                         //   ("Delta (English)", "Delta (Hindi)" = img1.hscow.com)
  api_endpoint: string;  // The Bingr API endpoint that returned it (debug)
  raw_url: string;       // The original upstream URL before our proxy wrapping
  proxied: boolean;      // true = URL was wrapped through /api/manifest-proxy
                         // false = direct CDN (CORS-friendly, CloudFront signed)
  referer_used?: string; // What Referer header our backend uses when proxying
}
```

**How to use `stream_links`**:

The array is **sorted by quality** with the best playable stream first. For most users, just hand `stream_links[0].url` to your HLS player. To build a "server picker" UI (like Bingr has), iterate `stream_links` and let the user pick.

#### Picking the best stream

```js
// Option A: just play the top pick (recommended)
hls.loadSource(data.stream_links[0].url);

// Option B: server picker UI — group by scraper, dedup by language
const servers = {};
data.stream_links.forEach(s => {
  const key = s.scraper;  // or use s.label.split(' — ')[0] for language grouping
  if (!servers[key]) servers[key] = [];
  servers[key].push(s);
});
// Render each `servers[key]` as a button
```

#### Quality selector

The `quality` field is already extracted. To build a quality selector for ONE scraper's streams:

```js
const polarisStreams = data.stream_links.filter(s => s.scraper === 'Polaris');
// polarisStreams has entries like:
//   { quality: "1080p", label: "English — 1080p", url: "..." }
//   { quality: "1080p", label: "Hindi — 1080p", url: "..." }
//   { quality: "720p", label: "English — 720p", url: "..." }
// Each URL is a different language+quality combo — pick one
```

### `subtitles[]`

```typescript
interface Subtitle {
  url: string;   // Proxied through /api/seg-proxy (CORS-friendly, referer set)
  label: string; // Human label — "English", "Spanish", etc.
  lang: string;  // 2-letter language code — "en", "es", "hi", "ta", "te"
}
```

**How to use subtitles**:

The `url` is already proxied through our backend (so it's CORS-friendly and has the right Referer). You can pass it directly to HLS.js as a sidecar `<track>` element:

```html
<video id="player" controls>
  <track kind="subtitles" src={sub.url} srclang={sub.lang} label={sub.label} />
</video>
```

Or programmatically via HLS.js:
```js
// Add as text track
const track = document.createElement('track');
track.kind = 'subtitles';
track.src = sub.url;
track.srclang = sub.lang;
track.label = sub.label;
videoElement.appendChild(track);
track.addEventListener('load', () => { track.track.mode = 'showing'; });
```

Note: HLS manifests often **also** contain embedded subtitle tracks (declared inside the m3u8). Those are separate from the `subtitles[]` array here. Use HLS.js's `subtitleTracks` API to access them:

```js
hls.on(Hls.Events.MANIFEST_PARSED, () => {
  console.log('Embedded subtitle tracks:', hls.subtitleTracks);
  console.log('Sidecar subtitles from API:', data.subtitles);
});
```

### `media_info` — TMDB-style metadata

```typescript
interface MediaInfo {
  id: number;
  type: "movie" | "tv" | "anime";
  season: number;
  episode: number;
  title: string;
  year: string;
  genres: string[];
  rating: string;        // "9.5" or "N/A"
  overview: string;
}
```

Use this to render the page header — title, year, rating chips, overview text.

### `seasons[]` — season selector (TV only)

```typescript
interface Season {
  season: number;
  name: string;           // "Season 1" or custom name
  episodes_count: number;
}
```

Render as a dropdown — when user picks a season, refetch `/api/resolve` with the new season's episode 1 URL.

### `episodes[]` — episode list (TV only)

```typescript
interface Episode {
  episode: number;
  title: string;
  rating: string;
  air_date: string;       // ISO date
}
```

Render as a list — when user picks an episode, refetch `/api/resolve` with the new episode URL.

### `episode_key`

Internal cache key, format `{type}_{id}_{season}_{episode}`. Useful if you want to hit the cached manifest endpoint directly:

```
GET /api/fast.m3u8?ep=tv_1396_1_1
```

Returns the cached m3u8 manifest as plain text (CORS-enabled). Usually not needed — `stream_links[0].url` is the right entry point.

---

## Streaming through our proxy

Some `stream_links[].url` values are wrapped through our backend:

```
https://USERNAME-bingr-scraper.hf.space/api/manifest-proxy?u=...&referer=...
```

These work transparently — hand them to HLS.js and they play. Our backend:
1. Fetches the upstream m3u8 with the correct Referer (which browsers can't set)
2. Rewrites all inner URLs (segments, audio sub-playlists, subtitle playlists, encryption keys) to also go through `/api/seg-proxy`
3. Returns the rewritten manifest with CORS headers

For each segment the player fetches:
```
GET https://USERNAME-bingr-scraper.hf.space/api/seg-proxy?u=...&referer=...
```

Our backend fetches the segment with the right Referer, streams bytes back with CORS + 24h browser cache.

**HEAD requests**: Both `/api/manifest-proxy` and `/api/seg-proxy` accept HEAD method (returns empty body with CORS headers) — HLS.js sends HEAD probes before GET, and we respond correctly.

---

## Sample end-to-end flow

```js
// 1. User navigates to /play/series/1396/1/1 on your Vercel site
const tmdbId = '1396';
const season = 1, episode = 1;
const bingrUrl = `https://bingr.one/watch/tv/${tmdbId}/${season}/${episode}`;

// 2. POST to backend
const res = await fetch(`${process.env.BINGR_API_BASE}/api/resolve`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ url: bingrUrl }),
});
const data = await res.json();

// 3. Render metadata
document.title = data.title;
renderSeasons(data.seasons);
renderEpisodes(data.episodes);
renderMediaInfo(data.media_info);

// 4. Render server picker
data.stream_links.forEach((s, i) => {
  const btn = document.createElement('button');
  btn.textContent = `${s.scraper} — ${s.label}`;
  btn.onclick = () => playStream(s.url, data.subtitles);
  document.getElementById('server-list').appendChild(btn);
});

// 5. Auto-play top pick
playStream(data.stream_links[0].url, data.subtitles);

function playStream(url, subtitles) {
  if (Hls.isSupported()) {
    const hls = new Hls({ maxBufferLength: 30, backBufferLength: 60 });
    hls.loadSource(url);
    hls.attachMedia(videoElement);
    // Add sidecar subtitle tracks
    subtitles.forEach(sub => {
      const track = document.createElement('track');
      track.kind = 'subtitles';
      track.src = sub.url;
      track.srclang = sub.lang;
      track.label = sub.label;
      videoElement.appendChild(track);
    });
  }
}
```

---

## Cache + freshness

- **Episode cache**: every successful resolve is cached for 2.5 hours (Bingr's apiKey JWTs expire at 3h, we leave 30min margin)
- **Repeat requests**: sub-5ms response from cache
- **Cache bust**: POST `/api/cache/clear` to wipe (admin/debug)
- **Cache stats**: GET `/api/cache/stats` to see what's cached

---

## Rate limits

None enforced by us. Bingr's API has no documented rate limit but typical good-citizen behavior:
- Don't fire more than 10 concurrent scrapes per second
- Episode cache means most users never hit the same URL twice

If Bingr starts rate-limiting your HF Space IP, you'll see `[DIRECT API EMPTY]` in the logs and the browser fallback will kick in.

---

## Health check

```
GET /api/health
```

Returns:
```json
{
  "status": "ok",
  "uptime_seconds": 1234.5,
  "browser_warm": true,
  "cache_episodes": 47,
  "cache_series_routes": 12
}
```

Use this as the URL for your cron-job.org / UptimeRobot keep-alive pinger (every 5-10 minutes prevents HF Spaces from sleeping).
