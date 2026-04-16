# FlightTracker

A self-hosted system for historizing ADS-B flight data from a FlightRadar24 feeder station.

## Overview

This repo has two responsibilities:

1. **Feeder configuration** — version-controlled config files for the Raspberry Pi ADS-B feeder.
2. **Collector application** — a Python service that polls the feeder's live data endpoint and stores everything in a PostgreSQL (TimescaleDB) database.

## Infrastructure

### Raspberry Pi (ADS-B Feeder)

- **SSH:** `tracker@flighttracker.local`
- **Location:** Stuttgart area, ~85 km range
- **Services:** `readsb` (ADS-B decoder) + `fr24feed` (FlightRadar24 feeder) + `tar1090` (web UI + JSON API)

### Data Source

The collector polls:

```
http://<pi-ip>/data/aircraft.json
```

Response shape:
```json
{
  "now": 1234567890.123,
  "aircraft": [
    {
      "hex": "3c6444",
      "flight": "DLH123",
      "alt_baro": 35000,
      "gs": 450.2,
      "lat": 48.76,
      "lon": 9.15,
      "track": 270.0,
      "squawk": "1234",
      "category": "A3",
      "r_dst": 12.4,
      "rssi": -18.5,
      "messages": 42,
      "seen": 0.8
    }
  ]
}
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

## Repo Structure

```
adsb-collector/
├── collector/                  # ADS-B data collector service
│   ├── docker-compose.yml      # TimescaleDB + collector stack (NAS)
│   ├── __main__.py             # Entry point (python -m collector)
│   ├── config.py               # Configuration via env vars
│   ├── models.py               # AircraftState dataclass
│   ├── poller.py               # HTTP polling of aircraft.json
│   ├── tracker.py              # Session state machine → DB writes
│   ├── db.py                   # asyncpg pool + schema init
│   └── schema.sql              # PostgreSQL/TimescaleDB schema
├── digest/                     # Weekly flight digest agent
│   └── docker-compose.yml      # Digest agent stack (NAS)
├── feeder/                     # Pi feeder stack
│   └── docker-compose.yml      # readsb + tar1090 + fr24feed (Pi)
├── tests/                      # Pytest test suite
├── Dockerfile                  # Collector container image
└── .env.example                # Required environment variables
```

## Database Schema

Three tables in PostgreSQL (TimescaleDB):

- **`aircraft`** — registry, one row per unique ICAO hex
- **`sightings`** — one row per continuous observation session (start/end time, altitude/distance aggregates, callsign)
- **`position_updates`** — high-frequency position samples (TimescaleDB hypertable, 1-day chunks, compressed after 7 days)

## Running the Collector

### With Docker (recommended)

```bash
cp .env.example .env
# Edit .env with your values
cd collector

docker compose up -d
```

### Locally (development)

```bash
nix develop          # enter dev shell — provides Python 3.13 + all deps + ruff + mypy
cp .env.example .env
# Edit .env

python -m collector
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ADSB_URL` | — | tar1090 aircraft.json URL |
| `POLL_INTERVAL` | `5` | Seconds between polls |
| `SESSION_TIMEOUT` | `300` | Seconds of silence before a sighting ends |
| `DATABASE_URL` | — | PostgreSQL connection string |

### Running Tests / Linting

All tools are available inside `nix develop`:

```bash
pytest
ruff check collector tests
ruff format --check collector tests
mypy collector
```

## Feeder Configuration

The `feeder/` directory contains configuration files for the services running on the Pi. See `feeder/README.md` for sync instructions.

## Deployment

The collector is packaged as a Docker image. Deployment target and orchestration are TBD — the focus right now is getting the collector working and validated against live data.
