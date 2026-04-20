# Squawk

A self-hosted system for historizing ADS-B flight data from a FlightRadar24 feeder station, with Telegram digest broadcasts.

## Components

| Component | Location | Purpose |
|-----------|----------|---------|
| Squawk | `squawk/` | Polls Pi every 5s, writes sightings to TimescaleDB, Telegram digest |
| Feeder | `feeder/` | readsb + tar1090 + fr24feed on the Pi |

## Architecture

```
                        ┌──────────────────────────────────────────────┐
                        │                squawk service                │
                        │                                              │
  tar1090 ──────────────► run_pipeline()                               │
                        │   ├─ record sightings (SightingRepository)   │
                        │   └─ enrich_batch()  (EnrichmentRepository)  │
                        │                                              │
                        │ Scheduler ──► generate_digest()              │
                        │ /debug    ──┘      │                         │
                        │                    ├─ DigestQuery             │
                        │                    ├─ DigestRepository        │
                        │                    ├─ ChartQuery              │
                        │                    └─ Broadcaster ──► Telegram│
                        │                                              │
                        │ TelegramBot: /start /digest /stats /debug    │
                        └──────────────────────────────────────────────┘
                                          │
                                     TimescaleDB
```

Two async tasks run in an `asyncio.TaskGroup`:

1. **Pipeline** (`run_pipeline`): continuous loop — polls tar1090, records sightings,
   batches new/expired aircraft, enriches them via Gemini, stores results.
2. **Bot** (`TelegramBot.run`): handles `/start`, `/digest`, `/stats`, `/debug`. The scheduler
   triggers `generate_digest` (cron); `/debug` triggers it on demand.

### Repository Layout

```
squawk/
  __main__.py               ← wiring only: construct everything, start TaskGroup
  config.py                 ← all env vars in one frozen dataclass
  db.py                     ← asyncpg pool creation
  scheduler.py              ← Scheduler protocol + APSchedulerBackend
  pipeline.py               ← run_pipeline(): polling loop + enrichment batching
  enrichment.py             ← ScoringClient protocol, enrich_batch(), ADK impl
  digest.py                 ← DigestClient protocol, generate_digest(), ADK impl
  charts.py                 ← matplotlib traffic charts for digests
  tags.py                   ← StoryTag enum + TAG_DESCRIPTIONS

  clients/                  ← typed HTTP clients, each behind a Protocol
    adsbdb.py               ← AircraftInfo, AircraftLookupClient
    hexdb.py                ← HexdbClient (hexdb.io lookups)
    planespotters.py        ← PhotoInfo, PhotoClient
    routes.py               ← RouteInfo, RouteClient
    mictronics.py           ← bulk aircraft DB download + ingest

  repositories/             ← write repositories, one per table-owner
    sightings.py            ← SightingRepository (aircraft, sightings, position_updates)
    enrichment.py           ← EnrichmentRepository (enriched_aircraft, callsign_routes)
    bulk_aircraft.py        ← BulkAircraftRepository (bulk_aircraft)
    digest.py               ← DigestRepository (digests)

  queries/                  ← read-only, cross-table
    digest.py               ← DigestQuery (joins sightings + enriched_aircraft)
    charts.py               ← ChartQuery (daily/hourly traffic counts)

  bot/
    app.py                  ← TelegramBot: PTB wiring, command registration, run()
    handlers.py             ← /start /digest /stats /debug command handlers
    broadcaster.py          ← Broadcaster protocol + TelegramBroadcaster

libs/
  tar1090/                  ← pure package: polls tar1090 HTTP API
```

### Table Ownership

Each repository owns specific tables for writes. Cross-table reads go through
query objects.

```
Table                  Writer                    Read by
──────────────────────────────────────────────────────────────────────
aircraft               SightingRepository        DigestQuery
sightings              SightingRepository        DigestQuery, ChartQuery
position_updates       SightingRepository        DigestQuery
enriched_aircraft      EnrichmentRepository      DigestQuery, run_pipeline (expiry)
callsign_routes        EnrichmentRepository      DigestQuery
bulk_aircraft          BulkAircraftRepository    enrich_batch
digests                DigestRepository          —
```

## Infrastructure

- **Pi:** `tracker@flighttracker.local` — runs the feeder stack
- **NAS / server:** `coolify.local` — runs squawk via Coolify (auto-deploys from master)
- **Data endpoint:** `http://<pi-ip>/data/aircraft.json`
- **Database:** TimescaleDB (shared between squawk and feeder)

## Data Source

Squawk polls the Pi's tar1090 endpoint:

```
http://<pi-ip>/data/aircraft.json
```

Key fields per aircraft:

| Field | Description |
|-------|-------------|
| `hex` | ICAO 24-bit address — stable aircraft identifier |
| `flight` | Callsign |
| `alt_baro` | Barometric altitude (feet), or `"ground"` |
| `gs` | Ground speed (knots) |
| `lat`, `lon` | Position |
| `r_dst` | Distance from receiver (nautical miles) |
| `rssi` | Signal strength (dBFS) |
| `seen` | Seconds since last message received |

## Database Schema

Seven tables in TimescaleDB:

- **`aircraft`** — registry, one row per unique ICAO hex
- **`sightings`** — one row per continuous observation session (start/end time, altitude/distance aggregates, callsign)
- **`position_updates`** — high-frequency position samples (hypertable, 1-day chunks, compressed after 7 days, retained 90 days)
- **`enriched_aircraft`** — AI scores, annotations, registration data per aircraft (TTL-based expiry)
- **`callsign_routes`** — origin/destination per flight callsign
- **`bulk_aircraft`** — bulk aircraft registry from mictronics.de
- **`digests`** — cached digests

## Deployment

Deployed via Coolify on `coolify.local`. Pushes to `master` auto-deploy.

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ADSB_URL` | — | tar1090 aircraft.json URL |
| `DATABASE_URL` | — | PostgreSQL connection string |
| `BOT_TOKEN` | — | Telegram bot token from @BotFather |
| `CHANNEL_ID` | — | Telegram channel ID for digest broadcast |
| `GEMINI_API_KEY` | — | Google Gemini API key |
| `ADMIN_CHAT_ID` | — | Telegram chat ID allowed to use `/debug` |
| `POLL_INTERVAL` | `5` | Seconds between polls |
| `SESSION_TIMEOUT` | `300` | Seconds of silence before a sighting session ends |
| `DIGEST_SCHEDULE` | `15 7 * * *` | Cron schedule for digest generation |
| `ENRICHMENT_TTL_DAYS` | `30` | Days before re-enriching a known aircraft |
| `ENRICHMENT_BATCH_SIZE` | `20` | Max aircraft per Gemini scoring call |
| `ENRICHMENT_FLUSH_SECS` | `30` | Max seconds to wait before flushing batch |
| `CLIENT_MAX_RETRIES` | `3` | Max retries for 429/5xx from external APIs |

## Dev Environment

```bash
nix develop   # provides Python 3.13, uv, ruff, mypy, psql, pre-commit, dbmate
```

Single `pyproject.toml` and `uv.lock` at repo root. Run tools from repo root:

```bash
uv run pytest
uv run ruff check .
uv run mypy squawk
```
