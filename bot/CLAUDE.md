# Bot

Weekly flight digest agent that reads from TimescaleDB, uses Google ADK with
Claude Haiku to write an engaging German-language digest, and delivers it via
Telegram bot.

## Structure

```
bot/
  bot/
    __main__.py     ← entry point (python -m bot)
    config.py       ← env var config
    db.py           ← user registration + digest cache
    tools.py        ← ADK tools: get_sightings, lookup_aircraft, lookup_route
    agent.py        ← ADK agent (LiteLlm → Claude Haiku) + runner
    bot.py          ← Telegram handlers: /start, /stop, /debug
    scheduler.py    ← weekly cron via APScheduler
  Dockerfile        ← builds ghcr.io/transmitt0r/squawk/bot
  pyproject.toml
  uv.lock
```

## Key facts

- Uses Google ADK with `LiteLlm(model="anthropic/claude-haiku-4-5-20251001")`
- Shares TimescaleDB with the collector — `users` and `digests` tables live alongside flight data tables
- Weekly digest is cached — tokens spent once, sent to all users from cache
- `/debug` is admin-only, gated by `ADMIN_CHAT_ID` env var
- ADK tools (`get_sightings`, `lookup_aircraft`, `lookup_route`) are sync functions using psycopg2
- Digest is written in German

## Commands

| Command  | Access | Description |
|----------|--------|-------------|
| `/start` | all    | Register for weekly digest |
| `/stop`  | all    | Unregister |
| `/debug` | admin  | Generate and send fresh digest immediately |

## Dev workflow

```bash
# from repo root
nix develop

# then from bot/
cd bot
uv run python -m bot
```

## Deploy

Via the root `docker-compose.yml` (together with collector and TimescaleDB):
```bash
# from repo root
docker compose up -d
# Send /start to bot → check logs for chat_id → set ADMIN_CHAT_ID → redeploy
```
