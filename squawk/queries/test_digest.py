"""DigestQuery integration tests against a real TimescaleDB instance.

Uses the actual dbmate migrations from db/migrations/ — same schema as production.
Requires Docker and dbmate on PATH.

Run with:
    uv run pytest squawk/queries/test_digest.py -v
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from squawk.queries.digest import DigestQuery

TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16"
MIGRATIONS_DIR = "db/migrations"


def dbmate(db_url: str, *args: str) -> None:
    subprocess.run(
        ["dbmate", "--migrations-dir", MIGRATIONS_DIR, "--no-dump-schema", *args],
        env={**os.environ, "DATABASE_URL": db_url},
        check=True,
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
        # Order matters: children before parents for TRUNCATE without CASCADE
        await conn.execute(
            "TRUNCATE enriched_aircraft, callsign_routes, sightings,"
            " position_updates, aircraft CASCADE"
        )
    yield p
    await p.close()


@pytest.fixture
def query(pool: asyncpg.Pool) -> DigestQuery:
    return DigestQuery(pool)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _insert_aircraft(
    pool: asyncpg.Pool,
    hex: str,
    *,
    first_seen: datetime | None = None,
    callsigns: list[str] | None = None,
) -> None:
    now = first_seen or datetime.now(tz=timezone.utc)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO aircraft (hex, first_seen, last_seen, callsigns)
            VALUES ($1, $2, $2, $3)
            ON CONFLICT (hex) DO NOTHING
            """,
            hex,
            now,
            callsigns or [],
        )


async def _insert_sighting(
    pool: asyncpg.Pool,
    hex: str,
    *,
    callsign: str | None = None,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    min_distance: float | None = 5.0,
    max_altitude: int | None = 35000,
) -> None:
    started = started_at or datetime.now(tz=timezone.utc)
    ended = ended_at or (started + timedelta(minutes=10))
    last_seen = ended
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO sightings
                (hex, callsign, started_at, ended_at, last_seen,
                 min_altitude, max_altitude, min_distance, max_distance)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            hex,
            callsign,
            started,
            ended,
            last_seen,
            10000,
            max_altitude,
            min_distance,
            min_distance,
        )


async def _insert_enrichment(
    pool: asyncpg.Pool,
    hex: str,
    *,
    story_score: int = 5,
    story_tags: list[str] | None = None,
    annotation: str = "",
    registration: str | None = None,
    type_: str | None = None,
    operator: str | None = None,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO enriched_aircraft
                (hex, registration, type, operator, flag,
                 story_score, story_tags, annotation,
                 enriched_at, expires_at)
            VALUES ($1, $2, $3, $4, NULL, $5, $6, $7, now(), now() + interval '30 days')
            ON CONFLICT (hex) DO UPDATE SET
                story_score  = EXCLUDED.story_score,
                story_tags   = EXCLUDED.story_tags,
                annotation   = EXCLUDED.annotation
            """,
            hex,
            registration,
            type_,
            operator,
            story_score,
            story_tags or [],
            annotation,
        )


async def _insert_route(
    pool: asyncpg.Pool,
    callsign: str,
    *,
    origin_iata: str = "LHR",
    origin_city: str = "London",
    origin_country: str = "United Kingdom",
    dest_iata: str = "CDG",
    dest_city: str = "Paris",
    dest_country: str = "France",
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO callsign_routes
                (callsign, origin_iata, origin_icao, origin_city, origin_country,
                 dest_iata, dest_icao, dest_city, dest_country, fetched_at)
            VALUES ($1, $2, NULL, $3, $4, $5, NULL, $6, $7, now())
            ON CONFLICT (callsign) DO NOTHING
            """,
            callsign,
            origin_iata,
            origin_city,
            origin_country,
            dest_iata,
            dest_city,
            dest_country,
        )


async def _insert_position_update(
    pool: asyncpg.Pool,
    hex: str,
    *,
    time: datetime | None = None,
    squawk: str | None = None,
) -> None:
    ts = time or datetime.now(tz=timezone.utc)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO position_updates (time, hex, lat, lon, alt_baro, squawk)
            VALUES ($1, $2, 51.5, -0.1, 35000, $3)
            """,
            ts,
            hex,
            squawk,
        )


# ---------------------------------------------------------------------------
# get_candidates — basic cases
# ---------------------------------------------------------------------------


async def test_get_candidates_empty_when_no_sightings(query: DigestQuery) -> None:
    result = await query.get_candidates(days=7)
    assert result == []


async def test_get_candidates_returns_aircraft_in_window(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123")
    await _insert_sighting(pool, "abc123", callsign="BA100")

    result = await query.get_candidates(days=7)
    assert len(result) == 1
    assert result[0].hex == "abc123"
    assert result[0].callsign == "BA100"


async def test_get_candidates_excludes_aircraft_outside_window(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    old_start = datetime.now(tz=timezone.utc) - timedelta(days=10)
    await _insert_aircraft(pool, "old001")
    await _insert_sighting(pool, "old001", started_at=old_start)

    result = await query.get_candidates(days=7)
    assert result == []


async def test_get_candidates_ranked_by_story_score(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    for hex_, score in [("aaa111", 3), ("bbb222", 9), ("ccc333", 6)]:
        await _insert_aircraft(pool, hex_)
        await _insert_sighting(pool, hex_)
        await _insert_enrichment(pool, hex_, story_score=score)

    result = await query.get_candidates(days=7)
    scores = [r.story_score for r in result]
    assert scores == sorted(scores, reverse=True)
    assert scores[0] == 9


async def test_get_candidates_unenriched_after_enriched(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    """Aircraft without enrichment row appear after enriched ones (NULLS LAST)."""
    await _insert_aircraft(pool, "enriched1")
    await _insert_sighting(pool, "enriched1")
    await _insert_enrichment(pool, "enriched1", story_score=5)

    await _insert_aircraft(pool, "notenriched")
    await _insert_sighting(pool, "notenriched")
    # No enrichment insert — story_score will be NULL

    result = await query.get_candidates(days=7)
    assert len(result) == 2
    assert result[0].hex == "enriched1"
    assert result[1].story_score is None


async def test_get_candidates_aggregates_multiple_sightings(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123")
    now = datetime.now(tz=timezone.utc)
    await _insert_sighting(
        pool,
        "abc123",
        callsign="BA100",
        started_at=now - timedelta(hours=4),
        min_distance=8.0,
        max_altitude=30000,
    )
    await _insert_sighting(
        pool,
        "abc123",
        callsign="BA100",
        started_at=now - timedelta(hours=2),
        min_distance=3.0,
        max_altitude=35000,
    )

    result = await query.get_candidates(days=7)
    assert len(result) == 1
    assert result[0].visit_count == 2
    assert result[0].closest_nm == pytest.approx(3.0)
    assert result[0].max_alt_ft == 35000


async def test_get_candidates_includes_route_info(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123")
    await _insert_sighting(pool, "abc123", callsign="BA100")
    await _insert_enrichment(pool, "abc123")
    await _insert_route(pool, "BA100")

    result = await query.get_candidates(days=7)
    assert len(result) == 1
    c = result[0]
    assert c.origin_iata == "LHR"
    assert c.origin_city == "London"
    assert c.dest_iata == "CDG"
    assert c.dest_country == "France"


async def test_get_candidates_route_none_when_callsign_missing(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123")
    await _insert_sighting(pool, "abc123", callsign=None)

    result = await query.get_candidates(days=7)
    assert len(result) == 1
    assert result[0].callsign is None
    assert result[0].origin_iata is None
    assert result[0].dest_iata is None


async def test_get_candidates_enrichment_fields_none_without_row(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    """Aircraft with no enriched_aircraft row returns None for enrichment fields."""
    await _insert_aircraft(pool, "abc123")
    await _insert_sighting(pool, "abc123")

    result = await query.get_candidates(days=7)
    assert len(result) == 1
    c = result[0]
    assert c.story_score is None
    assert c.story_tags == []
    assert c.annotation == ""
    assert c.registration is None


async def test_get_candidates_story_tags_returned_as_list(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "abc123")
    await _insert_sighting(pool, "abc123")
    await _insert_enrichment(pool, "abc123", story_tags=["military", "fighter"])

    result = await query.get_candidates(days=7)
    assert result[0].story_tags == ["military", "fighter"]


async def test_get_candidates_limited_to_20(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    for i in range(25):
        hex_ = f"hex{i:04d}"
        await _insert_aircraft(pool, hex_)
        await _insert_sighting(pool, hex_)

    result = await query.get_candidates(days=7)
    assert len(result) <= 20


# ---------------------------------------------------------------------------
# get_stats — counts
# ---------------------------------------------------------------------------


async def test_get_stats_zero_counts_when_empty(query: DigestQuery) -> None:
    stats = await query.get_stats(days=7)
    assert stats.total_sightings == 0
    assert stats.unique_aircraft == 0
    assert stats.new_aircraft == 0
    assert stats.peak_hour is None
    assert stats.peak_count is None
    assert stats.squawk_alerts == []


async def test_get_stats_counts_sightings_in_window(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "aaa111")
    await _insert_aircraft(pool, "bbb222")
    await _insert_sighting(pool, "aaa111")
    await _insert_sighting(pool, "aaa111")
    await _insert_sighting(pool, "bbb222")

    stats = await query.get_stats(days=7)
    assert stats.total_sightings == 3
    assert stats.unique_aircraft == 2


async def test_get_stats_excludes_sightings_outside_window(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    old_start = datetime.now(tz=timezone.utc) - timedelta(days=10)
    await _insert_aircraft(pool, "old001")
    await _insert_sighting(pool, "old001", started_at=old_start)

    stats = await query.get_stats(days=7)
    assert stats.total_sightings == 0


async def test_get_stats_counts_new_aircraft(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    now = datetime.now(tz=timezone.utc)
    await _insert_aircraft(pool, "new001", first_seen=now - timedelta(days=2))
    await _insert_aircraft(pool, "old001", first_seen=now - timedelta(days=10))
    await _insert_sighting(pool, "new001")
    await _insert_sighting(pool, "old001")

    stats = await query.get_stats(days=7)
    assert stats.new_aircraft == 1


# ---------------------------------------------------------------------------
# get_stats — peak hour
# ---------------------------------------------------------------------------


async def test_get_stats_returns_peak_hour(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    """Peak hour is the local hour (Europe/Berlin) with the most sightings."""
    # Insert 2 sightings at a fixed UTC time so we can predict the local hour
    fixed_utc = datetime(
        2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc
    )  # 12:00 Berlin (CEST)
    await _insert_aircraft(pool, "aaa111")
    await _insert_aircraft(pool, "bbb222")
    await _insert_sighting(pool, "aaa111", started_at=fixed_utc)
    await _insert_sighting(pool, "bbb222", started_at=fixed_utc + timedelta(minutes=30))

    stats = await query.get_stats(days=7)
    assert stats.peak_hour == 12  # Europe/Berlin = UTC+2 in April (CEST)
    assert stats.peak_count == 2


# ---------------------------------------------------------------------------
# get_stats — squawk alerts
# ---------------------------------------------------------------------------


async def test_get_stats_detects_emergency_squawk(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "sos001")
    await _insert_position_update(pool, "sos001", squawk="7700")

    stats = await query.get_stats(days=7)
    assert len(stats.squawk_alerts) == 1
    alert = stats.squawk_alerts[0]
    assert alert.hex == "sos001"
    assert alert.squawk == "7700"
    assert alert.meaning == "General emergency"


async def test_get_stats_detects_all_emergency_codes(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    now = datetime.now(tz=timezone.utc)
    for i, code in enumerate(["7500", "7600", "7700"]):
        hex_ = f"sos{i:03d}"
        await _insert_aircraft(pool, hex_)
        await _insert_position_update(
            pool, hex_, squawk=code, time=now - timedelta(minutes=i)
        )

    stats = await query.get_stats(days=7)
    observed_codes = {a.squawk for a in stats.squawk_alerts}
    assert observed_codes == {"7500", "7600", "7700"}


async def test_get_stats_ignores_normal_squawk(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    await _insert_aircraft(pool, "normal1")
    await _insert_position_update(pool, "normal1", squawk="1234")

    stats = await query.get_stats(days=7)
    assert stats.squawk_alerts == []


async def test_get_stats_excludes_squawk_outside_window(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    old_time = datetime.now(tz=timezone.utc) - timedelta(days=10)
    await _insert_aircraft(pool, "old001")
    await _insert_position_update(pool, "old001", squawk="7700", time=old_time)

    stats = await query.get_stats(days=7)
    assert stats.squawk_alerts == []


async def test_get_stats_deduplicates_squawk_per_hex(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    """DISTINCT ON (hex, squawk) — same hex+squawk combo appears at most once."""
    now = datetime.now(tz=timezone.utc)
    await _insert_aircraft(pool, "dup001")
    await _insert_position_update(
        pool, "dup001", squawk="7700", time=now - timedelta(minutes=2)
    )
    await _insert_position_update(
        pool, "dup001", squawk="7700", time=now - timedelta(minutes=1)
    )

    stats = await query.get_stats(days=7)
    alerts_for_hex = [a for a in stats.squawk_alerts if a.hex == "dup001"]
    assert len(alerts_for_hex) == 1


async def test_get_stats_squawk_alert_time_format(
    query: DigestQuery, pool: asyncpg.Pool
) -> None:
    """time_local is formatted as 'Weekday HH:MM' (non-empty string)."""
    await _insert_aircraft(pool, "sos001")
    await _insert_position_update(pool, "sos001", squawk="7700")

    stats = await query.get_stats(days=7)
    assert len(stats.squawk_alerts) == 1
    # Format: 'Mon 14:32' — 3-char weekday + space + HH:MM
    time_str = stats.squawk_alerts[0].time_local
    assert len(time_str) == 9  # 'Mon 14:32'
    assert time_str[3] == " "
    assert time_str[6] == ":"
