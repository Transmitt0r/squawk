# Squawk — Event-Driven Architecture Redesign

## Goals

- Single deployable service instead of two (`collector` + `bot` → `squawk`)
- Stateless: restart or redeploy at any time without data loss
- Clear, enforced table ownership — no two actors write to the same table
- All inter-actor communication goes through the event bus — no shared repositories
- Typed, protocol-based boundaries everywhere: easy to test, easy to reason about
- Auditable: every state change is a logged event
- Observable: data flow is visible and graspable from `__main__.py` alone

---

## High-Level Architecture

```
                        ┌─────────────────────────────────────────────┐
                        │                  squawk service              │
                        │                                              │
  tar1090 ──────────────► PollingActor                                 │
                        │     │ emits: HexFirstSeen                    │
                        │     │         EnrichmentExpired              │
                        │     ▼                                        │
                        │  EventBus ──────────────────────────────┐   │
                        │     │                                   │   │
                        │     ▼                                   ▼   │
                        │ EnrichmentActor               DigestActor   │
                        │  (AI: Gemini)                 (AI: Gemini)  │
                        │     │                              │        │
                        │     ▼                              ▼        │
                        │ enriched_aircraft             digests       │
                        │ callsign_routes                    │        │
                        │                                   ▼        │
  Telegram ◄────────────────────────────────────── TelegramBot       │
                        │                                             │
                        │  Scheduler ──► emits: DigestRequested      │
                        │               onto EventBus                 │
                        └─────────────────────────────────────────────┘
                                          │
                                     TimescaleDB
```

The **scheduler fires domain events onto the bus** — it has no direct dependency on
any actor. `DigestRequested` lands on the bus the same way `HexFirstSeen` does.

---

## Repository Layout

One `pyproject.toml` at the repo root covers the entire project. `libs/` packages
are part of the same project — the boundary is enforced by import discipline, not
package isolation.

```
pyproject.toml               ← single, all dependencies live here
uv.lock
Dockerfile
docker-compose.yml

libs/
  tar1090/                   ← pure package: polls tar1090 HTTP API, returns AircraftState list
    __init__.py              ← public API: async def poll(url, timeout) -> list[AircraftState]
    models.py                ← AircraftState dataclass
    _http.py                 ← aiohttp internals (private)

  eventbus/                  ← pure package: typed async event bus, no domain knowledge
    __init__.py              ← public API: EventBus, Handler protocol
    bus.py                   ← dispatcher + asyncio delivery
    log.py                   ← TimescaleDB-backed event log
    protocols.py             ← Handler protocol

squawk/
  __init__.py
  __main__.py                ← wiring only: construct everything, start TaskGroup
  config.py                  ← all env vars in one dataclass
  db.py                      ← asyncpg pool creation
  events.py                  ← domain event dataclasses
  scheduler.py               ← Scheduler protocol + APSchedulerBackend

  clients/                   ← typed HTTP clients, each behind a Protocol
    protocols.py             ← AircraftLookupClient, PhotoClient, RouteClient
    adsbdb.py                ← AdsbbClient (implements AircraftLookupClient)
    planespotters.py         ← PlanespottersClient (implements PhotoClient)
    routes.py                ← RoutesClient (implements RouteClient)

  repositories/              ← write repositories, one per table-owner
    sightings.py             ← SightingRepository (aircraft, sightings, position_updates)
    enrichment.py            ← EnrichmentRepository (enriched_aircraft, callsign_routes)
    digest.py                ← DigestRepository (digests)
    users.py                 ← UserRepository (users)

  queries/                   ← read-only, cross-table, used only for building digest
    digest.py                ← DigestQuery (joins sightings + enriched_aircraft)

  actors/
    polling.py               ← PollingActor
    enrichment.py            ← EnrichmentActor
    digest.py                ← DigestActor

  bot/
    handlers.py              ← /start /stop /debug Telegram command handlers
    broadcaster.py           ← sends digest to all active users

tests/
  libs/
    test_tar1090.py
    test_eventbus.py
  squawk/
    test_repositories.py
    test_actors.py
    test_clients.py
```

---

## Libraries

`libs/tar1090` and `libs/eventbus` are Python packages within the single project.
They have no dependency on `squawk` domain types. This is enforced by convention:
nothing inside `libs/` may import from `squawk/`. Checked in code review and by
keeping their internal `import` statements visibly clean.

### `libs/tar1090`

No database dependency. No concept of sessions or state.

**Public API:**

```python
# tar1090/__init__.py
async def poll(url: str, timeout: float = 5.0) -> list[AircraftState]: ...

@dataclass(frozen=True)
class AircraftState:
    hex: str
    flight: str | None
    alt_baro: int | None       # None when on ground
    gs: float | None
    track: float | None
    lat: float | None
    lon: float | None
    r_dst: float | None
    rssi: float | None
    squawk: str | None
    seen: float
    timestamp: datetime
```

Nothing else is public. Internal HTTP logic lives in `_http.py`.

---

### `libs/eventbus`

Pure asyncio library. No domain knowledge. No database schema opinions beyond
what is needed to persist and replay events.

**Public API:**

```python
# eventbus/__init__.py

class Handler(Protocol[E]):
    async def handle(self, events: list[E]) -> None: ...

class EventBus:
    def __init__(self, pool: asyncpg.Pool) -> None: ...

    def subscribe(self, event_type: type[E], handler: Handler[E]) -> None: ...

    async def emit(self, event: DomainEvent) -> None:
        """Write to event_log, deliver to subscribed handlers."""

    async def replay_unprocessed(self, since: timedelta = timedelta(hours=24)) -> None:
        """On startup: re-deliver events with processed_at IS NULL within window."""
```

**Event log schema (owned by EventBus, not by any actor):**

```sql
-- OWNER: EventBus
CREATE TABLE event_log (
    id           BIGSERIAL,
    type         TEXT         NOT NULL,
    payload      JSONB        NOT NULL,
    emitted_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,

    PRIMARY KEY (id, emitted_at)  -- composite for hypertable
);

SELECT create_hypertable('event_log', 'emitted_at');
SELECT add_compression_policy('event_log', INTERVAL '7 days');
SELECT add_retention_policy('event_log', INTERVAL '90 days');
```

---

## Domain Events

All domain events are **frozen dataclasses**. They are the only mechanism by which
actors communicate. No actor imports another actor's repository.

```python
# squawk/events.py

@dataclass(frozen=True)
class HexFirstSeen:
    """Emitted when the polling pipeline records an aircraft hex for the first time."""
    hex: str
    callsign: str | None
    first_seen_at: datetime

@dataclass(frozen=True)
class EnrichmentExpired:
    """Emitted by PollingActor when a known aircraft's enrichment TTL has elapsed."""
    hex: str
    callsign: str | None

@dataclass(frozen=True)
class DigestRequested:
    """Emitted by Scheduler on cron schedule."""
    period_start: datetime
    period_end: datetime

DomainEvent = HexFirstSeen | EnrichmentExpired | DigestRequested
```

---

## Table Ownership

**Enforced structurally:** each actor receives only its own write repository via
constructor injection. An actor that tries to write to a table it does not own
has no repository to do so with — it is a wiring error visible in `__main__.py`.

```
Table                  Owner (write)          Accessed by (read)
─────────────────────────────────────────────────────────────────────
aircraft               SightingRepository     DigestQuery (read)
sightings              SightingRepository     DigestQuery (read)
position_updates       SightingRepository     —
enriched_aircraft      EnrichmentRepository   DigestQuery (read)
callsign_routes        EnrichmentRepository   DigestQuery (read)
event_log              EventBus               —
digests                DigestRepository       —
users                  UserRepository         —
```

Read access for digest generation goes through `DigestQuery` (read-only query
service, no write methods). It may join any table. It is not a repository.

---

## External Service Clients

All external HTTP calls go through typed clients. No `requests.get()` or
`aiohttp.ClientSession.get()` anywhere outside `squawk/clients/`.

**Protocols:**

```python
# squawk/clients/protocols.py

@dataclass(frozen=True)
class AircraftInfo:
    registration: str | None
    type: str | None
    operator: str | None
    flag: str | None

@dataclass(frozen=True)
class RouteInfo:
    origin_iata: str | None
    origin_city: str | None
    origin_country: str | None
    dest_iata: str | None
    dest_city: str | None
    dest_country: str | None

@dataclass(frozen=True)
class PhotoInfo:
    url: str
    caption: str

class AircraftLookupClient(Protocol):
    async def lookup(self, hex: str) -> AircraftInfo | None: ...

class RouteClient(Protocol):
    async def lookup(self, callsign: str) -> RouteInfo | None: ...

class PhotoClient(Protocol):
    async def lookup(self, hex: str) -> PhotoInfo | None: ...
```

Each concrete client (`AdsbbClient`, `RoutesClient`, `PlanespottersClient`) takes
an `aiohttp.ClientSession` and a base URL. Returns `None` on not-found. Raises on
unrecoverable errors.

---

## Scheduler

A thin `Protocol` inside `squawk`. Not a library — it is domain-coupled (cron
expressions, timezones). The protocol exists for testability.

```python
# squawk/scheduler.py

class Scheduler(Protocol):
    def add_cron_job(
        self,
        func: Callable[[], Coroutine[Any, Any, None]],
        expr: str,
        tz: str = "UTC",
    ) -> None: ...

    def add_interval_job(
        self,
        func: Callable[[], Coroutine[Any, Any, None]],
        seconds: int,
    ) -> None: ...

    def start(self) -> None: ...
    def shutdown(self) -> None: ...
```

`APSchedulerBackend` is the only concrete implementation. Nothing outside
`scheduler.py` imports APScheduler.

The scheduler emits events, it does not call actors directly:

```python
# inside APSchedulerBackend setup
scheduler.add_cron_job(
    lambda: bus.emit(DigestRequested(period_start=..., period_end=...)),
    config.digest_schedule,
    tz="Europe/Berlin",
)
```

---

## Actors

Each actor has exactly one `async def run(self) -> None` method. It runs until
cancelled. Constructor receives only what it needs — never another actor's
repository.

### PollingActor

```python
class PollingActor:
    def __init__(
        self,
        poll_url: str,
        poll_interval: float,
        sightings: SightingRepository,
        bus: EventBus,
        enrichment_ttl: timedelta,
    ) -> None: ...

    async def run(self) -> None:
        """Poll every poll_interval seconds.
        - Record all observed aircraft via SightingRepository
        - Emit HexFirstSeen for aircraft never seen before
        - Emit EnrichmentExpired for aircraft whose enriched_aircraft.expires_at < now()
        """
```

`SightingRepository.record_poll()` returns two lists: `new_hexes` and
`expired_hexes`. PollingActor turns these into events and emits them. It does not
know EnrichmentActor exists.

### EnrichmentActor

```python
class EnrichmentActor:
    def __init__(
        self,
        enrichment: EnrichmentRepository,
        aircraft_client: AircraftLookupClient,
        route_client: RouteClient,
        gemini_api_key: str,
        batch_size: int,
        flush_interval: float,
        enrichment_ttl: timedelta,
    ) -> None: ...

    async def handle(self, events: list[HexFirstSeen | EnrichmentExpired]) -> None:
        """Collect events, batch-score with one Gemini call, store results."""

    async def run(self) -> None:
        """Drain inbox: collect up to batch_size events or flush_interval seconds,
        whichever comes first. Then process batch."""
```

One Gemini call per batch. The scoring prompt receives all aircraft data at once
and returns a JSON array of `ScoreResult`. No per-aircraft agent instantiation.

### DigestActor

```python
class DigestActor:
    def __init__(
        self,
        query: DigestQuery,
        digest_repo: DigestRepository,
        photo_client: PhotoClient,
        broadcaster: Broadcaster,
        gemini_api_key: str,
    ) -> None: ...

    async def handle(self, events: list[DigestRequested]) -> None:
        """For each DigestRequested:
        - Check digest cache (DigestRepository); skip if already generated for period
        - Query enriched candidates + stats via DigestQuery
        - Call Gemini to generate digest text
        - Cache result via DigestRepository
        - Broadcast to all active users via Broadcaster
        """
```

---

## Main Wiring

`__main__.py` is the only place where concrete types are assembled. Reading it
gives a complete picture of the system.

```python
async def main() -> None:
    config = Config.from_env()
    pool = await create_pool(config.database_url)

    # Repositories (write)
    sightings_repo   = SightingRepository(pool)
    enrichment_repo  = EnrichmentRepository(pool)
    digest_repo      = DigestRepository(pool)
    user_repo        = UserRepository(pool)

    # Read query
    digest_query = DigestQuery(pool)

    # External clients
    async with aiohttp.ClientSession() as http:
        aircraft_client = AdsbbClient(http, config.adsbdb_url)
        route_client    = RoutesClient(http, config.routes_url)
        photo_client    = PlanespottersClient(http, config.planespotters_url)

        # Event bus
        bus = EventBus(pool)

        # Actors
        polling_actor = PollingActor(
            poll_url=config.adsb_url,
            poll_interval=config.poll_interval,
            sightings=sightings_repo,
            bus=bus,
            enrichment_ttl=config.enrichment_ttl,
        )
        enrichment_actor = EnrichmentActor(
            enrichment=enrichment_repo,
            aircraft_client=aircraft_client,
            route_client=route_client,
            gemini_api_key=config.gemini_api_key,
            batch_size=config.enrichment_batch_size,
            flush_interval=config.enrichment_flush_interval,
            enrichment_ttl=config.enrichment_ttl,
        )
        broadcaster = Broadcaster(user_repo, config.bot_token)
        digest_actor = DigestActor(
            query=digest_query,
            digest_repo=digest_repo,
            photo_client=photo_client,
            broadcaster=broadcaster,
            gemini_api_key=config.gemini_api_key,
        )

        # Bus subscriptions
        bus.subscribe(HexFirstSeen,       enrichment_actor)
        bus.subscribe(EnrichmentExpired,  enrichment_actor)
        bus.subscribe(DigestRequested,    digest_actor)

        # Scheduler (fires events onto bus, no actor coupling)
        scheduler = APSchedulerBackend()
        scheduler.add_cron_job(
            lambda: bus.emit(DigestRequested(...)),
            config.digest_schedule,
            tz="Europe/Berlin",
        )
        scheduler.start()

        # Telegram bot (independent, reads from user_repo only)
        bot = TelegramBot(config.bot_token, user_repo, bus)

        # Startup: replay any unprocessed events from last 24h
        await bus.replay_unprocessed()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(polling_actor.run())
            tg.create_task(enrichment_actor.run())
            tg.create_task(bot.run())
```

---

## Schema (Full)

```sql
-- OWNER: SightingRepository (written by PollingActor)
CREATE TABLE aircraft (
    hex         TEXT        PRIMARY KEY,
    first_seen  TIMESTAMPTZ NOT NULL,
    last_seen   TIMESTAMPTZ NOT NULL,
    callsigns   TEXT[]      NOT NULL DEFAULT '{}'
);

CREATE TABLE sightings (
    id           BIGSERIAL   PRIMARY KEY,
    hex          TEXT        NOT NULL REFERENCES aircraft(hex),
    callsign     TEXT,
    started_at   TIMESTAMPTZ NOT NULL,
    ended_at     TIMESTAMPTZ,
    min_altitude INT,
    max_altitude INT,
    min_distance FLOAT,
    max_distance FLOAT
);

CREATE TABLE position_updates (
    time     TIMESTAMPTZ NOT NULL,
    hex      TEXT        NOT NULL,
    lat      FLOAT,
    lon      FLOAT,
    alt_baro INT,
    gs       FLOAT,
    track    FLOAT,
    squawk   TEXT,
    rssi     FLOAT
);
SELECT create_hypertable('position_updates', 'time');
SELECT add_compression_policy('position_updates', INTERVAL '7 days');
SELECT add_retention_policy('position_updates', INTERVAL '90 days');

-- OWNER: EnrichmentRepository (written by EnrichmentActor)
CREATE TABLE enriched_aircraft (
    hex           TEXT        PRIMARY KEY REFERENCES aircraft(hex),
    registration  TEXT,
    type          TEXT,
    operator      TEXT,
    flag          TEXT,
    story_score   INT,
    story_tags    TEXT[]      NOT NULL DEFAULT '{}',
    annotation    TEXT,
    enriched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE callsign_routes (
    callsign        TEXT        PRIMARY KEY,
    origin_iata     TEXT,
    origin_city     TEXT,
    origin_country  TEXT,
    dest_iata       TEXT,
    dest_city       TEXT,
    dest_country    TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- OWNER: EventBus
CREATE TABLE event_log (
    id           BIGSERIAL,
    type         TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    emitted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,
    PRIMARY KEY (id, emitted_at)
);
SELECT create_hypertable('event_log', 'emitted_at');
SELECT add_compression_policy('event_log', INTERVAL '7 days');
SELECT add_retention_policy('event_log', INTERVAL '90 days');

-- OWNER: DigestRepository (written by DigestActor)
CREATE TABLE digests (
    id           SERIAL      PRIMARY KEY,
    period_start TIMESTAMPTZ NOT NULL,
    period_end   TIMESTAMPTZ NOT NULL,
    content      TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- OWNER: UserRepository (written by TelegramBot)
CREATE TABLE users (
    chat_id       BIGINT      PRIMARY KEY,
    username      TEXT,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    active        BOOLEAN     NOT NULL DEFAULT true
);
```

---

## Configuration

All environment variables in one `Config` dataclass. New variables needed:

| Variable                  | Default       | Description                                |
|---------------------------|---------------|--------------------------------------------|
| `ADSB_URL`                | —             | tar1090 aircraft.json URL                  |
| `DATABASE_URL`            | —             | PostgreSQL connection string               |
| `BOT_TOKEN`               | —             | Telegram bot token                         |
| `GEMINI_API_KEY`          | —             | Google Gemini API key                      |
| `ADMIN_CHAT_ID`           | —             | Telegram chat ID for /debug                |
| `POLL_INTERVAL`           | `5`           | Seconds between collector polls            |
| `SESSION_TIMEOUT`         | `300`         | Seconds before a sighting session closes   |
| `DIGEST_SCHEDULE`         | `0 8 * * 0`   | Cron schedule for weekly digest            |
| `ENRICHMENT_TTL_DAYS`     | `30`          | Days before re-enriching a known aircraft  |
| `ENRICHMENT_BATCH_SIZE`   | `20`          | Max aircraft per Gemini scoring call       |
| `ENRICHMENT_FLUSH_SECS`   | `30`          | Max seconds to wait before flushing batch  |
| `ADSBDB_URL`              | (public API)  | adsbdb base URL                            |
| `PLANESPOTTERS_URL`       | (public API)  | Planespotters base URL                     |
| `ROUTES_URL`              | (public API)  | Route lookup base URL                      |

---

## Migration Plan

The existing `bot/` and `collector/` services will be replaced. The database is
shared and must be migrated in place.

**Data migration (one-time):**
1. Copy enrichment columns from `aircraft` → new `enriched_aircraft` table
2. Set `expires_at = enriched_at + INTERVAL '30 days'` for migrated rows
3. Drop enrichment columns from `aircraft`

**Service migration:**
1. Deploy `squawk` service
2. Remove `collector` and `bot` services from `docker-compose.yml`
3. Remove old `collector/` and `bot/` top-level directories (or archive them)

---

## Implementation Checklist

Tasks are ordered by dependency. Each task should be completed and tested before
the next begins.

### Phase 1 — Project Restructure & Libraries

- [ ] **1.1** Consolidate to single root `pyproject.toml`: merge all dependencies from `collector/pyproject.toml` and `bot/pyproject.toml`, remove uv workspace config, configure hatchling to include `libs/` and `squawk/` packages
- [ ] **1.2** Create `libs/tar1090/`: move and strip `collector/` of all DB dependencies (`asyncpg`, `db.py`, `tracker.py`, `schema.sql`, `python-dotenv`)
- [ ] **1.3** Reduce `tar1090` public API to `poll(url, timeout) -> list[AircraftState]` in `__init__.py`; move HTTP logic to `_http.py`
- [ ] **1.4** Update `tar1090` tests to only test polling and parsing logic; move to `tests/libs/`
- [ ] **1.5** Create `libs/eventbus/` package
- [ ] **1.6** Define `Handler` protocol in `eventbus/protocols.py`
- [ ] **1.7** Implement `EventBus` in `eventbus/bus.py`: `subscribe()`, `emit()` (in-memory delivery only for now)
- [ ] **1.8** Implement `EventLog` in `eventbus/log.py`: `write()`, `mark_processed()`, `fetch_unprocessed()`
- [ ] **1.9** Wire `EventLog` into `EventBus.emit()` and `replay_unprocessed()`
- [ ] **1.10** Write `eventbus` tests in `tests/libs/`; cover: subscribe/emit, replay on startup, handler error does not crash bus

### Phase 2 — Squawk Service Skeleton

- [ ] **2.1** Create `squawk/` package with `__init__.py`
- [ ] **2.2** Write `squawk/config.py` with all env vars as a frozen `Config` dataclass
- [ ] **2.3** Write `squawk/db.py`: `create_pool(database_url) -> asyncpg.Pool`
- [ ] **2.4** Write `squawk/scheduler.py`: `Scheduler` protocol + `APSchedulerBackend`
- [ ] **2.5** Write `squawk/events.py`: `HexFirstSeen`, `EnrichmentExpired`, `DigestRequested` frozen dataclasses

### Phase 3 — Schema

- [ ] **3.1** Write `squawk/schema.sql` with full schema as documented above, including ownership comments
- [ ] **3.2** Add migration script: copy enrichment data from `aircraft` → `enriched_aircraft`, set `expires_at`, drop old columns
- [ ] **3.3** Verify schema applies cleanly against a fresh TimescaleDB instance

### Phase 4 — External Clients

- [ ] **4.1** Write `squawk/clients/protocols.py`: `AircraftInfo`, `RouteInfo`, `PhotoInfo` dataclasses + `AircraftLookupClient`, `RouteClient`, `PhotoClient` protocols
- [ ] **4.2** Implement `squawk/clients/adsbdb.py`: `AdsbbClient` using `aiohttp`, returning `AircraftInfo | None`
- [ ] **4.3** Implement `squawk/clients/routes.py`: `RoutesClient` using `aiohttp`, returning `RouteInfo | None`
- [ ] **4.4** Implement `squawk/clients/planespotters.py`: `PlanespottersClient` using `aiohttp`, returning `PhotoInfo | None`
- [ ] **4.5** Write tests for each client with mocked `aiohttp` responses, including 404 and error cases

### Phase 5 — Repositories

- [ ] **5.1** Write `squawk/repositories/sightings.py`: `SightingRepository` with `record_poll(states) -> tuple[list[str], list[str]]` (returns `new_hexes`, `expired_enrichment_hexes`). Owns: `aircraft`, `sightings`, `position_updates`
- [ ] **5.2** Write `squawk/repositories/enrichment.py`: `EnrichmentRepository` with `store(hex, result, expires_at)`, `get_callsign(hex) -> str | None`. Owns: `enriched_aircraft`, `callsign_routes`
- [ ] **5.3** Write `squawk/repositories/digest.py`: `DigestRepository` with `get_cached(period_start, period_end)`, `cache(period_start, period_end, digest)`. Owns: `digests`
- [ ] **5.4** Write `squawk/repositories/users.py`: `UserRepository` with `register(chat_id, username)`, `unregister(chat_id)`, `get_active() -> list[int]`. Owns: `users`
- [ ] **5.5** Write `squawk/queries/digest.py`: `DigestQuery` with `get_candidates(days) -> list[DigestCandidate]`, `get_stats(days) -> DigestStats`. Read-only. May join any table.
- [ ] **5.6** Write repository tests using a real test database (no mocks for DB layer)

### Phase 6 — Actors

- [ ] **6.1** Write `squawk/actors/polling.py`: `PollingActor` — polls via `tar1090.poll()`, calls `SightingRepository.record_poll()`, emits `HexFirstSeen` and `EnrichmentExpired` via `EventBus`
- [ ] **6.2** Write `squawk/actors/enrichment.py`: `EnrichmentActor` — collects events into batch buffer, flushes on `batch_size` or `flush_interval`, makes single Gemini batch scoring call, stores via `EnrichmentRepository`
- [ ] **6.3** Update Gemini scoring prompt to accept and return a JSON array (batch input/output)
- [ ] **6.4** Write `squawk/actors/digest.py`: `DigestActor` — handles `DigestRequested`, checks cache, queries via `DigestQuery`, calls Gemini, caches via `DigestRepository`, broadcasts
- [ ] **6.5** Write `squawk/bot/broadcaster.py`: `Broadcaster` — sends digest text + optional photo to all active users via `UserRepository` and Telegram API
- [ ] **6.6** Write `squawk/bot/handlers.py`: `/start`, `/stop`, `/debug` Telegram handlers
- [ ] **6.7** Write actor tests with fake/mock bus, repositories, and clients

### Phase 7 — Wiring & Deployment

- [ ] **7.1** Write `squawk/__main__.py` as documented in the wiring section above
- [ ] **7.2** Write `Dockerfile` (single, at repo root)
- [ ] **7.3** Update `docker-compose.yml`: replace `collector` and `bot` services with single `squawk` service
- [ ] **7.4** Update `CLAUDE.md` to reflect new structure

### Phase 8 — Migration & Cutover

- [ ] **8.1** Run data migration script against production database (enrichment columns → `enriched_aircraft`)
- [ ] **8.2** Deploy `squawk` service alongside existing services temporarily to verify event flow
- [ ] **8.3** Stop and remove `collector` and `bot` services
- [ ] **8.4** Remove `bot/` directory
- [ ] **8.5** Archive or remove old `collector/` and replace with `libs/collector/`

---

## Invariants (must hold throughout)

These are the rules that keep the system graspable. Any PR that violates them
should be rejected.

1. **No actor imports another actor's repository.** Cross-actor data flow goes
   through the event bus only.
2. **No raw HTTP calls outside `squawk/clients/`.** All external service
   communication goes through typed clients.
3. **No APScheduler imports outside `squawk/scheduler.py`.**
4. **No database writes outside the owning repository.** See table ownership
   table above.
5. **`libs/tar1090` and `libs/eventbus` have zero knowledge of squawk domain
   types.** Nothing inside `libs/` may import from `squawk/`. Enforced by
   convention and code review, not package isolation.
6. **`__main__.py` contains wiring only.** No business logic.
