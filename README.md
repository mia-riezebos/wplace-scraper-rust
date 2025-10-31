# wplace scraper / pumpkin (event) matcher

vibe coded wplace scraper with proxy rotation, and template matcher.

dont expect any support on this cuz this codebase is fucking ass and i dont understand rust well enough to know how to
fix most if any issues other than asking cursor "please fix".

## Usage

1. copy `.env.example` to `.env`, and configure settings
2. For file-based proxies: create `assets/proxies.json` and add an array of proxy connection strings (ie:
   `["http://user:pass@host:port", "http://user:pass@host:port"]`)
3. For rotating proxy endpoint: set `PROXY_MODE=rotating`, `PROXY_ENDPOINT`, and `PROXY_COUNT` in `.env`
4. run `cargo run --release`
5. Use the keybinds displayed in the TUI to start/stop fetch workers & match workers.

## Environment Variables

All environment variables are namespaced. See `.env.example` for complete documentation.

### Quick Reference

**WPLACE_*** - Main application settings
- `WPLACE_API_ENDPOINT` (required) - API endpoint URL
- `WPLACE_TILE_PATH` (required) - Tile path prefix
- `WPLACE_RATE_LIMIT` - Rate limit (default: 1.8 tiles/sec)
- `WPLACE_ZOOM_LEVEL` - Zoom level (default: 15)

**FETCHER_*** - Fetcher configuration
- `FETCHER_WORKER_COUNT` - Fetch worker count (auto-calculated if not set)
- `FETCHER_TILE_SELECTION_MODE` - random/consecutive/grid (default: random)
- `FETCHER_TILE_X` - Specific tile X coordinate to fetch immediately
- `FETCHER_TILE_Y` - Specific tile Y coordinate to fetch immediately

**PROXY_*** - Proxy configuration
- `PROXY_MODE` - "file" or "rotating" (default: file)
- `PROXY_ENDPOINT` - Required for rotating mode
- `PROXY_COUNT` - Required for rotating mode (number of proxy slots)
- `PROXY_MAX_CONCURRENCY` - Max concurrent requests per worker
- `PROXY_RETRY_COUNT` - Retry count for failed fetches (default: 3)

**MATCHER_*** - Matcher configuration
- `MATCHER_THRESHOLD` - Match threshold 0.0-1.0 (default: 0.25)
- `MATCHER_WORKER_COUNT` - Match worker count (auto-calculated if not set)

**CACHE_*** - Cache configuration
- `CACHE_TTL_MS` - Cache TTL in milliseconds (default: 3600000 = 1 hour)
- `CACHE_EXPIRE_ON_HOUR` - Expire cache on the hour (default: false)

## Requirements

Rust (obviously) - <https://rust-lang.org/> You need gcc+mingw, if on windows, you should get
[WinLibs GCC+MinGW-W64](https://winlibs.com/)

## Proxies

(Not sponsored) get some free proxies from <https://webshare.io>, or pay for their shared datacenter proxies (their ToS
doesn't allow residential proxies for gaming purposes, and wplace is a game).

## Proxy Modes

### File Mode (default)
Load proxies from `assets/proxies.json`. Each proxy is a URL in JSON array format.

### Rotating Mode
Use a single rotating proxy endpoint that rotates remotely. Set:
- `PROXY_MODE=rotating`
- `PROXY_ENDPOINT=http://user:pass@proxy.example.com:8080`
- `PROXY_COUNT=100` (number of proxy slots to simulate)
