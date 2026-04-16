@README.md

## Repo Structure

Three independent components, each self-contained:

| Component | Location | Runs on | Purpose |
|-----------|----------|---------|---------|
| Collector | `collector/` | NAS | Polls Pi every 5s, writes to TimescaleDB |
| Digest | `digest/` | NAS | Weekly Telegram digest via ADK + Claude Haiku |
| Feeder | `feeder/` | Pi | readsb + tar1090 + fr24feed in Docker |

See each component's `CLAUDE.md` for details:
- @collector/CLAUDE.md
- @digest/CLAUDE.md
- @feeder/CLAUDE.md

## Dev Environment

Nix devshell provides Python 3.13, ruff, mypy, psql:

```bash
nix develop   # from repo root
```

Each component has its own `pyproject.toml`. Run tools from within the component directory.

## Infrastructure

- **Pi:** `tracker@flighttracker.local` — runs the feeder stack
- **NAS:** runs collector (port 5431) and digest stacks
- **Data endpoint:** `http://<pi-ip>/data/aircraft.json`
- **TimescaleDB:** port 5431 on NAS
