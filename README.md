# Squawk

A self-hosted system for historizing ADS-B flight data from a FlightRadar24 feeder station, with a weekly Telegram digest.

## Components

| Component | Location | Purpose |
|-----------|----------|---------|
| Collector | `collector/` | Polls Pi every 5s, writes sightings to TimescaleDB |
| Bot | `bot/` | Weekly Telegram digest via ADK + Claude Haiku |
| Feeder | `feeder/` | readsb + tar1090 + fr24feed on the Pi |

## Infrastructure

- **Pi:** `tracker@flighttracker.local` — runs the feeder stack
- **NAS / server:** runs collector + bot via Docker Compose
- **Data endpoint:** `http://<pi-ip>/data/aircraft.json`
- **Database:** TimescaleDB (shared between collector and bot)

## Data Source

The collector polls the Pi's tar1090 endpoint:

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

Three tables in TimescaleDB:

- **`aircraft`** — registry, one row per unique ICAO hex
- **`sightings`** — one row per continuous observation session (start/end time, altitude/distance aggregates, callsign)
- **`position_updates`** — high-frequency position samples (hypertable, 1-day chunks, compressed after 7 days)

## Deployment

### Docker Compose

```bash
docker compose up -d
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ADSB_URL` | — | tar1090 aircraft.json URL (e.g. `http://192.168.0.111:8080/data/aircraft.json`) |
| `DB_PASSWORD` | — | TimescaleDB password (used by the db service) |
| `DATABASE_URL` | — | Full connection string for collector and bot: `postgresql://squawk:<password>@db:5432/squawk` |
| `BOT_TOKEN` | — | Telegram bot token from @BotFather |
| `ANTHROPIC_API_KEY` | — | Anthropic API key |
| `ADMIN_CHAT_ID` | — | Telegram chat ID allowed to use `/debug` (set after first `/start`) |
| `POLL_INTERVAL` | `5` | Seconds between collector polls |
| `SESSION_TIMEOUT` | `300` | Seconds of silence before a sighting session ends |
| `DIGEST_SCHEDULE` | `0 8 * * 0` | Cron schedule for weekly digest |
| `READSB_LAT` | — | Feeder receiver latitude |
| `READSB_LON` | — | Feeder receiver longitude |
| `FR24KEY` | — | FlightRadar24 sharing key |

## Dev Environment

```bash
nix develop   # provides Python 3.13, uv, ruff, mypy, psql
```

Each component has its own `pyproject.toml` and `uv.lock`. Run tools from within the component directory:

```bash
cd collector
uv run pytest
uv run ruff check .
uv run mypy collector
```

## Feeder Configuration

The `feeder/` directory contains version-controlled config files for the Pi services. See `feeder/README.md` for sync instructions.
