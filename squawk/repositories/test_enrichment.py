"""EnrichmentRepository integration tests against a real TimescaleDB instance.

Uses the actual dbmate migrations from db/migrations/ — same schema as production.
Requires Docker and dbmate on PATH.

Run with:
    uv run pytest squawk/repositories/test_enrichment.py -v
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from squawk.clients.adsbdb import AircraftInfo
from squawk.clients.routes import RouteInfo
from squawk.repositories.enrichment import EnrichmentRepository
from squawk.tags import StoryTag

TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16"
MIGRATIONS_DIR = "db/migrations"


def dbmate(db_url: str, *args: str) -> None:
    subprocess.run(
        ["dbmate", "--migrations-dir", MIGRATIONS_DIR, "--no-dump-schema", *args],
        env={**os.environ, "DATABASE_URL": db_url},
        check=True,
    )


AIRCRAFT_INFO = AircraftInfo(
    registration="G-EUUU",
    type="A320",
    operator="British Airways",
    flag="🇬🇧",
)

ROUTE_INFO = RouteInfo(
    origin_iata="LHR",
    origin_icao="EGLL",
    origin_city="London",
    origin_country="United Kingdom",
    dest_iata="CDG",
    dest_icao="LFPG",
    dest_city="Paris",
    dest_country="France",
)

TTL_30_DAYS = timedelta(days=30)
TTL_EXPIRED = timedelta(seconds=-1)  # already expired


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
        await conn.execute(
            "TRUNCATE enriched_aircraft, callsign_routes, aircraft CASCADE"
        )
    yield p
    await p.close()


@pytest.fixture
def repo(pool: asyncpg.Pool) -> EnrichmentRepository:
    return EnrichmentRepository(pool)


async def _insert_aircraft(
    pool: asyncpg.Pool, hex: str, callsign: str | None = None
) -> None:
    """Insert a minimal aircraft row (prerequisite for enriched_aircraft FK)."""
    now = datetime.now(tz=timezone.utc)
    callsigns = [callsign] if callsign else []
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO aircraft (hex, first_seen, last_seen, callsigns)
            VALUES ($1, $2, $2, $3)
            ON CONFLICT (hex) DO NOTHING
            """,
            hex,
            now,
            callsigns,
        )


# ---------------------------------------------------------------------------
# store — enriched_aircraft
# ---------------------------------------------------------------------------


async def test_store_inserts_enriched_aircraft(
    repo: EnrichmentRepository, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123", "BA123")

    await repo.store(
        hex="abc123",
        score=7,
        tags=[StoryTag.COMMERCIAL, StoryTag.WIDEBODY],
        annotation="Interessantes Flugzeug.",
        aircraft_info=AIRCRAFT_INFO,
        route_info=ROUTE_INFO,
        callsign="BA123",
        enrichment_ttl=TTL_30_DAYS,
    )

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM enriched_aircraft WHERE hex = 'abc123'"
        )

    assert row is not None
    assert row["registration"] == "G-EUUU"
    assert row["type"] == "A320"
    assert row["operator"] == "British Airways"
    assert row["story_score"] == 7
    assert row["story_tags"] == ["commercial", "widebody"]
    assert row["annotation"] == "Interessantes Flugzeug."
    assert row["expires_at"] > row["enriched_at"]


async def test_store_upserts_enriched_aircraft(
    repo: EnrichmentRepository, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123", "BA123")

    await repo.store(
        hex="abc123",
        score=3,
        tags=[StoryTag.COMMERCIAL],
        annotation="Alt.",
        aircraft_info=AIRCRAFT_INFO,
        route_info=None,
        callsign=None,
        enrichment_ttl=TTL_30_DAYS,
    )
    await repo.store(
        hex="abc123",
        score=8,
        tags=[StoryTag.MILITARY],
        annotation="Neu.",
        aircraft_info=AIRCRAFT_INFO,
        route_info=None,
        callsign=None,
        enrichment_ttl=TTL_30_DAYS,
    )

    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM enriched_aircraft WHERE hex = 'abc123'"
        )
        row = await conn.fetchrow(
            "SELECT story_score, story_tags, annotation"
            " FROM enriched_aircraft WHERE hex = 'abc123'"
        )

    assert count == 1
    assert row["story_score"] == 8
    assert row["story_tags"] == ["military"]
    assert row["annotation"] == "Neu."


async def test_store_handles_null_aircraft_info(
    repo: EnrichmentRepository, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123")

    await repo.store(
        hex="abc123",
        score=1,
        tags=[],
        annotation="",
        aircraft_info=None,
        route_info=None,
        callsign=None,
        enrichment_ttl=TTL_30_DAYS,
    )

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT registration, type, operator, flag"
            " FROM enriched_aircraft WHERE hex = 'abc123'"
        )

    assert row["registration"] is None
    assert row["type"] is None
    assert row["operator"] is None
    assert row["flag"] is None


# ---------------------------------------------------------------------------
# store — callsign_routes
# ---------------------------------------------------------------------------


async def test_store_inserts_callsign_route(
    repo: EnrichmentRepository, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123", "BA123")

    await repo.store(
        hex="abc123",
        score=5,
        tags=[],
        annotation="",
        aircraft_info=None,
        route_info=ROUTE_INFO,
        callsign="BA123",
        enrichment_ttl=TTL_30_DAYS,
    )

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM callsign_routes WHERE callsign = 'BA123'"
        )

    assert row is not None
    assert row["origin_iata"] == "LHR"
    assert row["dest_iata"] == "CDG"
    assert row["origin_city"] == "London"
    assert row["dest_country"] == "France"


async def test_store_upserts_callsign_route(
    repo: EnrichmentRepository, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123", "BA123")

    old_route = RouteInfo(
        origin_iata="JFK",
        origin_icao="KJFK",
        origin_city="New York",
        origin_country="United States",
        dest_iata="LHR",
        dest_icao="EGLL",
        dest_city="London",
        dest_country="United Kingdom",
    )
    await repo.store(
        hex="abc123",
        score=5,
        tags=[],
        annotation="",
        aircraft_info=None,
        route_info=old_route,
        callsign="BA123",
        enrichment_ttl=TTL_30_DAYS,
    )
    await repo.store(
        hex="abc123",
        score=5,
        tags=[],
        annotation="",
        aircraft_info=None,
        route_info=ROUTE_INFO,
        callsign="BA123",
        enrichment_ttl=TTL_30_DAYS,
    )

    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM callsign_routes WHERE callsign = 'BA123'"
        )
        row = await conn.fetchrow(
            "SELECT origin_iata FROM callsign_routes WHERE callsign = 'BA123'"
        )

    assert count == 1
    assert row["origin_iata"] == "LHR"  # updated to ROUTE_INFO value


async def test_store_skips_route_when_callsign_none(
    repo: EnrichmentRepository, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123")

    await repo.store(
        hex="abc123",
        score=5,
        tags=[],
        annotation="",
        aircraft_info=None,
        route_info=ROUTE_INFO,  # route_info provided but callsign=None → skip
        callsign=None,
        enrichment_ttl=TTL_30_DAYS,
    )

    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM callsign_routes")

    assert count == 0


async def test_store_skips_route_when_route_info_none(
    repo: EnrichmentRepository, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123", "BA123")

    await repo.store(
        hex="abc123",
        score=5,
        tags=[],
        annotation="",
        aircraft_info=None,
        route_info=None,
        callsign="BA123",
        enrichment_ttl=TTL_30_DAYS,
    )

    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM callsign_routes")

    assert count == 0


# ---------------------------------------------------------------------------
# get_expired
# ---------------------------------------------------------------------------


async def test_get_expired_returns_empty_for_empty_input(
    repo: EnrichmentRepository,
) -> None:
    result = await repo.get_expired([], TTL_30_DAYS)
    assert result == []


async def test_get_expired_excludes_non_expired(
    repo: EnrichmentRepository, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123", "BA123")
    await repo.store(
        hex="abc123",
        score=5,
        tags=[],
        annotation="",
        aircraft_info=None,
        route_info=None,
        callsign=None,
        enrichment_ttl=TTL_30_DAYS,  # expires 30 days from now
    )

    result = await repo.get_expired(["abc123"], TTL_30_DAYS)
    assert result == []


async def test_get_expired_returns_expired_hex(
    repo: EnrichmentRepository, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123", "BA123")
    await repo.store(
        hex="abc123",
        score=5,
        tags=[],
        annotation="",
        aircraft_info=None,
        route_info=None,
        callsign=None,
        enrichment_ttl=TTL_EXPIRED,  # expires immediately (in the past)
    )

    result = await repo.get_expired(["abc123"], TTL_30_DAYS)
    assert len(result) == 1
    assert result[0][0] == "abc123"


async def test_get_expired_returns_callsign(
    repo: EnrichmentRepository, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123", "BA123")
    await repo.store(
        hex="abc123",
        score=5,
        tags=[],
        annotation="",
        aircraft_info=None,
        route_info=None,
        callsign=None,
        enrichment_ttl=TTL_EXPIRED,
    )

    result = await repo.get_expired(["abc123"], TTL_30_DAYS)
    assert result[0] == ("abc123", "BA123")


async def test_get_expired_excludes_hexes_without_enrichment_row(
    repo: EnrichmentRepository, pool: asyncpg.Pool
) -> None:
    """Brand-new hexes (no enriched_aircraft row) must be excluded."""
    await _insert_aircraft(pool, "newone", "EZ500")
    # No store() call → no enriched_aircraft row

    result = await repo.get_expired(["newone"], TTL_30_DAYS)
    assert result == []


async def test_get_expired_handles_multiple_hexes(
    repo: EnrichmentRepository, pool: asyncpg.Pool
) -> None:
    for hex_, callsign in [
        ("aaa111", "LH100"),
        ("bbb222", "FR200"),
        ("ccc333", "EZY300"),
    ]:
        await _insert_aircraft(pool, hex_, callsign)

    # Two expired, one fresh
    await repo.store("aaa111", 5, [], "", None, None, None, TTL_EXPIRED)
    await repo.store("bbb222", 5, [], "", None, None, None, TTL_30_DAYS)
    await repo.store("ccc333", 5, [], "", None, None, None, TTL_EXPIRED)

    result = await repo.get_expired(["aaa111", "bbb222", "ccc333"], TTL_30_DAYS)
    expired_hexes = {r[0] for r in result}
    assert expired_hexes == {"aaa111", "ccc333"}
