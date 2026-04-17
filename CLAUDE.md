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

Nix devshell provides Python 3.13, uv, ruff, mypy, psql, pre-commit:

```bash
nix develop          # from repo root
pre-commit install   # once, sets up git hooks
```

Each component has its own `pyproject.toml` and `uv.lock`. Run tools from within the component directory.

## Pre-commit hooks

`.pre-commit-config.yaml` runs on every `git commit`:
- **ruff format** — auto-formats staged `.py` files
- **ruff check --fix** — lints and auto-fixes staged `.py` files
- **pytest (collector)** — runs collector test suite when `collector/` files change
- **pytest (bot)** — runs bot test suite when `bot/` files change

The Claude Code PostToolUse hook (`.claude/hooks/ruff-check.sh`) also runs ruff check immediately after each file edit, for faster feedback during development.

**IMPORTANT:** `ruff` and other hook tools are only available inside the nix devshell. Always run `git commit` via `nix develop --command git commit ...` — running it outside the devshell will fail the hooks because the executables are not on PATH.

## Testing

Tests are colocated with the code they test — no top-level `tests/` directory.
Examples: `libs/tar1090/test_tar1090.py`, `squawk/actors/test_polling.py`.

## AI / LLM

All AI and LLM calls go through **google-adk** (`google-adk` package). Do not use
`google-genai` or any other AI SDK directly. Validation scripts and prototypes may
use lower-level APIs, but production code in `squawk/` must go through google-adk.

## Infrastructure

- **Pi:** `tracker@flighttracker.local` — runs the feeder stack
- **NAS:** runs collector + bot stack
- **Data endpoint:** `http://<pi-ip>/data/aircraft.json`
- **TimescaleDB:** shared between collector and bot
