"""EventBus integration tests against a real TimescaleDB instance.

Uses the actual dbmate migrations from db/migrations/ — same schema as
production. Requires Docker and dbmate on PATH.

Run with:
    uv run pytest libs/eventbus/test_eventbus.py -v
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import subprocess
from datetime import timedelta

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from eventbus.bus import EventBus
from eventbus.log import EventLog

TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16"
MIGRATIONS_DIR = "db/migrations"


def dbmate(db_url: str, *args: str) -> None:
    subprocess.run(
        ["dbmate", "--migrations-dir", MIGRATIONS_DIR, "--no-dump-schema", *args],
        env={**os.environ, "DATABASE_URL": db_url},
        check=True,
    )


# ---------------------------------------------------------------------------
# Test domain events (no squawk import)
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class ThingHappened:
    name: str
    value: int


@dataclasses.dataclass(frozen=True)
class OtherThingHappened:
    label: str


# ---------------------------------------------------------------------------
# Minimal Actor implementation for testing
# ---------------------------------------------------------------------------


class CollectingActor:
    def __init__(self) -> None:
        self._inbox: asyncio.Queue = asyncio.Queue()

    @property
    def inbox(self) -> asyncio.Queue:
        return self._inbox

    async def run(self) -> None:  # pragma: no cover
        while True:
            await self._inbox.get()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_url():
    with PostgresContainer(image=TIMESCALE_IMAGE) as container:
        url = container.get_connection_url(driver=None)
        dbmate(url, "up")
        yield url


@pytest.fixture
async def pool(db_url: str):
    p = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5)
    async with p.acquire() as conn:
        await conn.execute("TRUNCATE event_log")
    yield p
    await p.close()


@pytest.fixture
def bus(pool: asyncpg.Pool) -> EventBus:
    return EventBus(pool)


# ---------------------------------------------------------------------------
# EventLog tests
# ---------------------------------------------------------------------------


async def test_event_log_write_and_fetch(pool: asyncpg.Pool) -> None:
    log = EventLog(pool)
    entry = await log.write("ThingHappened", {"name": "test", "value": 42})

    assert entry.type == "ThingHappened"
    assert entry.payload == {"name": "test", "value": 42}
    assert entry.id > 0

    unprocessed = await log.fetch_unprocessed(timedelta(hours=1))
    assert len(unprocessed) == 1
    assert unprocessed[0].id == entry.id
    assert unprocessed[0].type == "ThingHappened"


async def test_event_log_mark_processed_uses_composite_key(pool: asyncpg.Pool) -> None:
    log = EventLog(pool)
    entry = await log.write("ThingHappened", {"name": "x", "value": 1})

    await log.mark_processed(entry.id, entry.emitted_at)

    unprocessed = await log.fetch_unprocessed(timedelta(hours=1))
    assert not any(e.id == entry.id for e in unprocessed)


async def test_event_log_fetch_unprocessed_excludes_old_events(
    pool: asyncpg.Pool,
) -> None:
    log = EventLog(pool)
    await log.write("ThingHappened", {"name": "old", "value": 0})
    unprocessed = await log.fetch_unprocessed(timedelta(seconds=0))
    assert unprocessed == []


# ---------------------------------------------------------------------------
# EventBus subscribe/emit tests
# ---------------------------------------------------------------------------


async def test_bus_emit_delivers_to_subscribed_actor(bus: EventBus) -> None:
    actor = CollectingActor()
    bus.subscribe(ThingHappened, actor)

    event = ThingHappened(name="hello", value=7)
    await bus.emit(event)

    assert actor.inbox.qsize() == 1
    log_entry, received = actor.inbox.get_nowait()
    assert received == event
    assert log_entry.type == "ThingHappened"


async def test_bus_emit_delivers_to_multiple_actors(bus: EventBus) -> None:
    actor1 = CollectingActor()
    actor2 = CollectingActor()
    bus.subscribe(ThingHappened, actor1)
    bus.subscribe(ThingHappened, actor2)

    await bus.emit(ThingHappened(name="multi", value=1))

    assert actor1.inbox.qsize() == 1
    assert actor2.inbox.qsize() == 1
    _entry1, event1 = actor1.inbox.get_nowait()
    _entry2, event2 = actor2.inbox.get_nowait()
    assert event1 == event2


async def test_bus_emit_does_not_deliver_to_unsubscribed_actor(bus: EventBus) -> None:
    actor = CollectingActor()
    bus.subscribe(OtherThingHappened, actor)

    await bus.emit(ThingHappened(name="ignored", value=0))

    assert actor.inbox.qsize() == 0


async def test_bus_emit_persists_to_event_log(
    bus: EventBus, pool: asyncpg.Pool
) -> None:
    actor = CollectingActor()
    bus.subscribe(ThingHappened, actor)

    await bus.emit(ThingHappened(name="persisted", value=99))

    log = EventLog(pool)
    entries = await log.fetch_unprocessed(timedelta(hours=1))
    assert len(entries) == 1
    assert entries[0].type == "ThingHappened"
    assert entries[0].payload == {"name": "persisted", "value": 99}


async def test_bus_emit_persists_even_without_subscriber(
    bus: EventBus, pool: asyncpg.Pool
) -> None:
    """Events with no subscriber are still written to the log."""
    await bus.emit(ThingHappened(name="no-subscriber", value=0))

    log = EventLog(pool)
    entries = await log.fetch_unprocessed(timedelta(hours=1))
    assert any(e.type == "ThingHappened" for e in entries)


# ---------------------------------------------------------------------------
# EventBus replay tests
# ---------------------------------------------------------------------------


async def test_bus_replay_delivers_unprocessed_events(
    bus: EventBus, pool: asyncpg.Pool
) -> None:
    actor = CollectingActor()
    bus.subscribe(ThingHappened, actor)

    log = EventLog(pool)
    await log.write("ThingHappened", {"name": "replayed", "value": 5})

    await bus.replay_unprocessed(since=timedelta(hours=1))

    assert actor.inbox.qsize() == 1
    _entry, event = actor.inbox.get_nowait()
    assert isinstance(event, ThingHappened)
    assert event.name == "replayed"
    assert event.value == 5


async def test_bus_replay_skips_processed_events(
    bus: EventBus, pool: asyncpg.Pool
) -> None:
    actor = CollectingActor()
    bus.subscribe(ThingHappened, actor)

    log = EventLog(pool)
    entry = await log.write("ThingHappened", {"name": "done", "value": 1})
    await log.mark_processed(entry.id, entry.emitted_at)

    await bus.replay_unprocessed(since=timedelta(hours=1))

    assert actor.inbox.qsize() == 0


async def test_bus_replay_skips_unknown_event_type(
    bus: EventBus, pool: asyncpg.Pool
) -> None:
    """Unknown event types during replay are logged and skipped — bus does not crash."""
    log = EventLog(pool)
    await log.write("UnknownEvent", {"x": 1})

    await bus.replay_unprocessed(since=timedelta(hours=1))


async def test_bus_replay_skips_malformed_payload(
    bus: EventBus, pool: asyncpg.Pool
) -> None:
    """Events whose payload can't be deserialized are skipped — bus does not crash."""
    actor = CollectingActor()
    bus.subscribe(ThingHappened, actor)

    log = EventLog(pool)
    await log.write("ThingHappened", {"wrong_field": "oops"})

    await bus.replay_unprocessed(since=timedelta(hours=1))
    assert actor.inbox.qsize() == 0
