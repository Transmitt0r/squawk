@README.md

## Repo Structure

Three independent components, each self-contained:

| Component | Location | Runs on | Purpose |
|-----------|----------|---------|---------|
| Collector | `collector/` | NAS | Polls Pi every 5s, writes to TimescaleDB |
| Bot | `bot/` | NAS | Weekly Telegram digest via ADK + Claude Haiku |
| Feeder | `feeder/` | Pi | readsb + tar1090 + fr24feed in Docker |

See each component's `CLAUDE.md` for details:
- @collector/CLAUDE.md
- @bot/CLAUDE.md
- @feeder/CLAUDE.md

## Dev Environment

Nix devshell provides Python 3.13, uv, ruff, mypy, psql:

```bash
nix develop   # from repo root
```

Each component has its own `pyproject.toml` and `uv.lock`. Run tools from within the component directory.

## Infrastructure

- **Pi:** `tracker@flighttracker.local` — runs the feeder stack
- **NAS:** runs collector + bot stack
- **Data endpoint:** `http://<pi-ip>/data/aircraft.json`
- **TimescaleDB:** shared between collector and bot
