"""SightingRepository integration tests against a real TimescaleDB instance.

Uses the actual dbmate migrations from db/migrations/ — same schema as production.
Requires Docker and dbmate on PATH.

Run with:
    uv run pytest squawk/repositories/test_sightings.py -v
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from squawk.repositories.sightings import NewSighting, SightingRepository
from tar1090 import AircraftState

TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16"
MIGRATIONS_DIR = "db/migrations"


def dbmate(db_url: str, *args: str) -> None:
    subprocess.run(
        ["dbmate", "--migrations-dir", MIGRATIONS_DIR, "--no-dump-schema", *args],
        env={**os.environ, "DATABASE_URL": db_url},
        check=True,
    )


def make_state(
    hex: str = "abc123",
    flight: str | None = "BA123",
    alt_baro: int | None = 35000,
    gs: float | None = 450.0,
    track: float | None = 90.0,
    lat: float | None = 51.5,
    lon: float | None = -0.1,
    r_dst: float | None = 10.5,
    rssi: float | None = -14.0,
    squawk: str | None = None,
    seen: float = 0.5,
) -> AircraftState:
    return AircraftState(
        hex=hex,
        flight=flight,
        alt_baro=alt_baro,
        gs=gs,
        track=track,
        lat=lat,
        lon=lon,
        r_dst=r_dst,
        rssi=rssi,
        squawk=squawk,
        seen=seen,
        timestamp=datetime.now(tz=timezone.utc),
    )


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
        await conn.execute("TRUNCATE aircraft, sightings, position_updates CASCADE")
    yield p
    await p.close()


@pytest.fixture
def repo(pool: asyncpg.Pool) -> SightingRepository:
    return SightingRepository(pool)


# ---------------------------------------------------------------------------
# close_open_sightings
# ---------------------------------------------------------------------------


async def test_close_open_sightings_is_noop_when_none_open(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    await repo.close_open_sightings()  # should not raise

    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM sightings WHERE ended_at IS NULL"
        )
    assert count == 0


async def test_close_open_sightings_closes_all_open(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    # Create two aircraft and open sightings via record_poll
    await repo.record_poll([make_state("aaa"), make_state("bbb")], session_timeout=300)

    async with pool.acquire() as conn:
        open_before = await conn.fetchval(
            "SELECT COUNT(*) FROM sightings WHERE ended_at IS NULL"
        )
    assert open_before == 2

    await repo.close_open_sightings()

    async with pool.acquire() as conn:
        open_after = await conn.fetchval(
            "SELECT COUNT(*) FROM sightings WHERE ended_at IS NULL"
        )
    assert open_after == 0


# ---------------------------------------------------------------------------
# record_poll — new aircraft (path b, new to aircraft table)
# ---------------------------------------------------------------------------


async def test_record_poll_new_aircraft_returned_as_new_sighting(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    new = await repo.record_poll(
        [make_state("hex1", flight="BA1")], session_timeout=300
    )

    assert len(new) == 1
    assert new[0] == NewSighting(hex="hex1", callsign="BA1")


async def test_record_poll_new_aircraft_inserted_into_aircraft_table(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    await repo.record_poll([make_state("hex1", flight="BA1")], session_timeout=300)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT hex, callsigns FROM aircraft WHERE hex = 'hex1'"
        )
    assert row is not None
    assert row["callsigns"] == ["BA1"]


async def test_record_poll_new_aircraft_opens_sighting(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    await repo.record_poll(
        [make_state("hex1", alt_baro=35000, r_dst=10.5)], session_timeout=300
    )

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT callsign, ended_at, min_altitude, max_altitude,"
            " min_distance, max_distance"
            " FROM sightings WHERE hex = 'hex1' AND ended_at IS NULL"
        )
    assert row is not None
    assert row["ended_at"] is None
    assert row["min_altitude"] == 35000
    assert row["max_altitude"] == 35000
    assert abs(row["min_distance"] - 10.5) < 0.001
    assert abs(row["max_distance"] - 10.5) < 0.001


async def test_record_poll_multiple_new_aircraft_all_returned(
    repo: SightingRepository,
) -> None:
    states = [make_state("h1"), make_state("h2"), make_state("h3")]
    new = await repo.record_poll(states, session_timeout=300)

    assert {n.hex for n in new} == {"h1", "h2", "h3"}


async def test_record_poll_null_callsign_aircraft_no_callsign_stored(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    await repo.record_poll([make_state("hex1", flight=None)], session_timeout=300)

    async with pool.acquire() as conn:
        callsigns = await conn.fetchval(
            "SELECT callsigns FROM aircraft WHERE hex = 'hex1'"
        )
    assert callsigns == []


# ---------------------------------------------------------------------------
# record_poll — post-gap reappearance (path b, known aircraft)
# ---------------------------------------------------------------------------


async def test_record_poll_post_gap_reappearance_not_returned(
    repo: SightingRepository,
) -> None:
    # First poll: new aircraft
    await repo.record_poll([make_state("hex1")], session_timeout=300)
    # Close the sighting manually to simulate a gap
    await repo.close_open_sightings()

    # Second poll: same aircraft reappears
    new = await repo.record_poll([make_state("hex1")], session_timeout=300)

    assert new == []


async def test_record_poll_post_gap_opens_new_sighting(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    await repo.record_poll([make_state("hex1")], session_timeout=300)
    await repo.close_open_sightings()
    await repo.record_poll([make_state("hex1")], session_timeout=300)

    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM sightings WHERE hex = 'hex1'")
        open_count = await conn.fetchval(
            "SELECT COUNT(*) FROM sightings WHERE hex = 'hex1' AND ended_at IS NULL"
        )
    assert count == 2  # original + reopened
    assert open_count == 1


# ---------------------------------------------------------------------------
# record_poll — update open sighting (path a)
# ---------------------------------------------------------------------------


async def test_record_poll_updates_last_seen_for_open_sighting(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    await repo.record_poll([make_state("hex1")], session_timeout=300)

    async with pool.acquire() as conn:
        last_seen_before = await conn.fetchval(
            "SELECT last_seen FROM sightings WHERE hex = 'hex1' AND ended_at IS NULL"
        )

    # Second poll — last_seen should advance
    await repo.record_poll([make_state("hex1")], session_timeout=300)

    async with pool.acquire() as conn:
        last_seen_after = await conn.fetchval(
            "SELECT last_seen FROM sightings WHERE hex = 'hex1' AND ended_at IS NULL"
        )

    assert last_seen_after >= last_seen_before


async def test_record_poll_updates_altitude_stats(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    await repo.record_poll([make_state("hex1", alt_baro=30000)], session_timeout=300)
    await repo.record_poll([make_state("hex1", alt_baro=40000)], session_timeout=300)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT min_altitude, max_altitude FROM sightings"
            " WHERE hex = 'hex1' AND ended_at IS NULL"
        )
    assert row["min_altitude"] == 30000
    assert row["max_altitude"] == 40000


async def test_record_poll_updates_distance_stats(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    await repo.record_poll([make_state("hex1", r_dst=5.0)], session_timeout=300)
    await repo.record_poll([make_state("hex1", r_dst=15.0)], session_timeout=300)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT min_distance, max_distance FROM sightings"
            " WHERE hex = 'hex1' AND ended_at IS NULL"
        )
    assert abs(row["min_distance"] - 5.0) < 0.001
    assert abs(row["max_distance"] - 15.0) < 0.001


async def test_record_poll_update_does_not_return_new_sighting(
    repo: SightingRepository,
) -> None:
    await repo.record_poll([make_state("hex1")], session_timeout=300)
    new = await repo.record_poll([make_state("hex1")], session_timeout=300)
    assert new == []


# ---------------------------------------------------------------------------
# record_poll — close timed-out sightings (path c)
# ---------------------------------------------------------------------------


async def test_record_poll_closes_absent_hex_when_timeout_exceeded(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    # Open a sighting for hex1, then poll without hex1 (timeout=0 → always close)
    await repo.record_poll([make_state("hex1")], session_timeout=300)
    await repo.record_poll([], session_timeout=0)

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT ended_at FROM sightings WHERE hex = 'hex1'")
    assert row["ended_at"] is not None


async def test_record_poll_leaves_absent_hex_open_within_timeout(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    await repo.record_poll([make_state("hex1")], session_timeout=300)
    # Large session_timeout → sighting stays open even when hex1 is absent
    await repo.record_poll([], session_timeout=86400)

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT ended_at FROM sightings WHERE hex = 'hex1'")
    assert row["ended_at"] is None


async def test_record_poll_closes_only_timed_out_sightings(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    """hex1 times out; hex2 stays visible — only hex1's sighting is closed."""
    await repo.record_poll(
        [make_state("hex1"), make_state("hex2")], session_timeout=300
    )
    # Second poll: hex2 still visible, hex1 absent with timeout=0
    await repo.record_poll([make_state("hex2")], session_timeout=0)

    async with pool.acquire() as conn:
        hex1_ended = await conn.fetchval(
            "SELECT ended_at FROM sightings WHERE hex = 'hex1'"
        )
        hex2_ended = await conn.fetchval(
            "SELECT ended_at FROM sightings WHERE hex = 'hex2' AND ended_at IS NULL"
        )
    assert hex1_ended is not None
    assert hex2_ended is None  # still open


# ---------------------------------------------------------------------------
# record_poll — aircraft callsign accumulation
# ---------------------------------------------------------------------------


async def test_record_poll_accumulates_unique_callsigns(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    await repo.record_poll([make_state("hex1", flight="BA1")], session_timeout=300)
    await repo.record_poll([make_state("hex1", flight="BA2")], session_timeout=300)

    async with pool.acquire() as conn:
        callsigns = await conn.fetchval(
            "SELECT callsigns FROM aircraft WHERE hex = 'hex1'"
        )
    assert set(callsigns) == {"BA1", "BA2"}


async def test_record_poll_does_not_duplicate_callsign(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    await repo.record_poll([make_state("hex1", flight="BA1")], session_timeout=300)
    await repo.record_poll([make_state("hex1", flight="BA1")], session_timeout=300)

    async with pool.acquire() as conn:
        callsigns = await conn.fetchval(
            "SELECT callsigns FROM aircraft WHERE hex = 'hex1'"
        )
    assert callsigns == ["BA1"]


# ---------------------------------------------------------------------------
# record_poll — position updates
# ---------------------------------------------------------------------------


async def test_record_poll_inserts_position_updates(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    state = make_state("hex1", lat=51.5, lon=-0.1, alt_baro=35000, gs=450.0)
    await repo.record_poll([state], session_timeout=300)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT lat, lon, alt_baro, gs FROM position_updates WHERE hex = 'hex1'"
        )
    assert row is not None
    assert abs(row["lat"] - 51.5) < 0.001
    assert abs(row["lon"] - (-0.1)) < 0.001
    assert row["alt_baro"] == 35000
    assert abs(row["gs"] - 450.0) < 0.001


async def test_record_poll_inserts_position_update_with_null_fields(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    state = make_state("hex1", lat=None, lon=None, alt_baro=None)
    await repo.record_poll([state], session_timeout=300)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT lat, lon, alt_baro FROM position_updates WHERE hex = 'hex1'"
        )
    assert row is not None
    assert row["lat"] is None
    assert row["lon"] is None
    assert row["alt_baro"] is None


async def test_record_poll_inserts_one_position_update_per_aircraft(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    states = [make_state("h1"), make_state("h2"), make_state("h3")]
    await repo.record_poll(states, session_timeout=300)

    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM position_updates")
    assert count == 3


# ---------------------------------------------------------------------------
# record_poll — empty states
# ---------------------------------------------------------------------------


async def test_record_poll_empty_states_returns_no_new_sightings(
    repo: SightingRepository,
) -> None:
    new = await repo.record_poll([], session_timeout=300)
    assert new == []


async def test_record_poll_empty_states_closes_timed_out(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    await repo.record_poll([make_state("hex1")], session_timeout=300)
    await repo.record_poll([], session_timeout=0)

    async with pool.acquire() as conn:
        ended_at = await conn.fetchval(
            "SELECT ended_at FROM sightings WHERE hex = 'hex1'"
        )
    assert ended_at is not None


# ---------------------------------------------------------------------------
# record_poll — idempotency
# ---------------------------------------------------------------------------


async def test_record_poll_idempotent_on_repeated_new_aircraft(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    """Second call with the same state after gap: no duplicate aircraft row."""
    await repo.record_poll([make_state("hex1", flight="BA1")], session_timeout=300)
    await repo.close_open_sightings()
    # Simulate replay: same aircraft appears "new" again but aircraft row already exists
    await repo.record_poll([make_state("hex1", flight="BA1")], session_timeout=300)

    async with pool.acquire() as conn:
        aircraft_count = await conn.fetchval(
            "SELECT COUNT(*) FROM aircraft WHERE hex = 'hex1'"
        )
    assert aircraft_count == 1  # only one aircraft row ever


async def test_record_poll_does_not_open_duplicate_sighting_for_present_hex(
    repo: SightingRepository, pool: asyncpg.Pool
) -> None:
    """Calling record_poll twice with the same hex open: only one open sighting."""
    await repo.record_poll([make_state("hex1")], session_timeout=300)
    await repo.record_poll([make_state("hex1")], session_timeout=300)

    async with pool.acquire() as conn:
        open_count = await conn.fetchval(
            "SELECT COUNT(*) FROM sightings WHERE hex = 'hex1' AND ended_at IS NULL"
        )
    assert open_count == 1
