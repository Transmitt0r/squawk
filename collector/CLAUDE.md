# Collector

Async Python service that polls the Pi's tar1090 ADS-B endpoint every 5 seconds
and writes aircraft sightings to TimescaleDB.

## Structure

```
collector/
  collector/        ← Python package
    __main__.py     ← entry point (python -m collector)
    config.py       ← env var config
    models.py       ← AircraftState dataclass
    poller.py       ← HTTP polling of aircraft.json
    tracker.py      ← session state machine → DB writes
    db.py           ← asyncpg pool + schema init
    schema.sql      ← TimescaleDB schema
  tests/            ← pytest test suite
  Dockerfile        ← builds ghcr.io/transmitt0r/squawk/collector
  pyproject.toml
  uv.lock
```

## Key facts

- Data source: `http://<pi-ip>/data/aircraft.json` (tar1090 Docker container, no /tar1090/ prefix)
- `alt_baro` can be the string `"ground"` — always type-check before using as int
- `flight` callsign has trailing spaces — strip before storing
- `seen` is seconds since last message — compute timestamp as `now - seen`
- asyncpg `pool.acquire()` is a sync context manager
- Schema uses TimescaleDB hypertable for `position_updates` (1-day chunks, compressed after 7 days)

## Dev workflow

```bash
# from repo root
nix develop

# then from collector/
cd collector
uv run pytest
uv run ruff check collector tests
uv run mypy collector
uv run python -m collector
```

## Deploy

Via the root `docker-compose.yml` (together with the bot and shared TimescaleDB):
```bash
# from repo root
docker compose up -d
```
