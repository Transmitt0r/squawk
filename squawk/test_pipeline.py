"""Pipeline integration tests against a real TimescaleDB instance.

Uses the actual dbmate migrations from db/migrations/ — same schema as production.
Requires Docker and dbmate on PATH.

Run with:
    uv run pytest squawk/test_pipeline.py -v
"""

from __future__ import annotations

import asyncio
import os
import subprocess
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from squawk.clients.adsbdb import AircraftInfo
from squawk.clients.routes import RouteInfo
from squawk.enrichment import ScoreResult
from squawk.pipeline import run_pipeline
from squawk.repositories.enrichment import EnrichmentRepository
from squawk.repositories.sightings import SightingRepository
from tar1090 import AircraftState

TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16"
MIGRATIONS_DIR = "db/migrations"

AIRCRAFT_INFO = AircraftInfo(
    registration="G-TEST",
    type="A320",
    operator="Test Airways",
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
# Mock clients
# ---------------------------------------------------------------------------


class _MockAircraftClient:
    def __init__(self, info: AircraftInfo | None = None) -> None:
        self._info = info or AIRCRAFT_INFO

    async def lookup(self, hex: str) -> AircraftInfo | None:
        return self._info


class _MockRouteClient:
    def __init__(self, info: RouteInfo | None = None) -> None:
        self._info = info or ROUTE_INFO

    async def lookup(self, callsign: str) -> RouteInfo | None:
        return self._info


class _MockScoringClient:
    def __init__(self, score: int = 5, tags: list[str] | None = None) -> None:
        self._score = score
        self._tags = tags or ["test"]
        self.calls: list[list] = []

    async def score_batch(
        self,
        aircraft: list[tuple[str, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]:
        self.calls.append(aircraft)
        return [
            ScoreResult(
                score=self._score,
                tags=self._tags,
                annotation="Test annotation.",
            )
            for _ in aircraft
        ]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _run_pipeline(
    states: list[AircraftState],
    sightings_repo: SightingRepository,
    enrichment_repo: EnrichmentRepository,
    scoring_client: _MockScoringClient,
    enrichment_ttl: timedelta = timedelta(days=30),
    batch_size: int = 2,
    max_cycles: int = 3,
    aircraft_client: _MockAircraftClient | None = None,
    route_client: _MockRouteClient | None = None,
) -> None:
    call_count = 0

    async def mock_poll(url: str, timeout: float = 5.0) -> list[AircraftState]:
        nonlocal call_count
        call_count += 1
        if call_count > max_cycles:
            raise asyncio.CancelledError()
        return states

    with patch("squawk.pipeline.tar1090.poll", side_effect=mock_poll):
        task = asyncio.create_task(
            run_pipeline(
                poll_url="http://test",
                poll_interval=0.001,
                session_timeout=300,
                sightings=sightings_repo,
                enrichment_repo=enrichment_repo,
                aircraft_client=aircraft_client or _MockAircraftClient(),
                route_client=route_client or _MockRouteClient(),
                scoring_client=scoring_client,
                enrichment_ttl=enrichment_ttl,
                batch_size=batch_size,
                flush_interval=0.001,
            )
        )
        try:
            await task
        except asyncio.CancelledError:
            pass


async def _insert_aircraft(
    pool: asyncpg.Pool, hex: str, callsign: str | None = None
) -> None:
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
            "TRUNCATE enriched_aircraft, callsign_routes, sightings,"
            " position_updates, aircraft CASCADE"
        )
    yield p
    await p.close()


@pytest.fixture
def sightings_repo(pool: asyncpg.Pool) -> SightingRepository:
    return SightingRepository(pool)


@pytest.fixture
def enrichment_repo(pool: asyncpg.Pool) -> EnrichmentRepository:
    return EnrichmentRepository(pool)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_pipeline_creates_sightings_and_enriches_new_aircraft(
    sightings_repo: SightingRepository,
    enrichment_repo: EnrichmentRepository,
    pool: asyncpg.Pool,
) -> None:
    states = [
        make_state(hex="aaa111", flight="FL111"),
        make_state(hex="bbb222", flight="FL222"),
    ]
    scoring = _MockScoringClient(score=7)

    await _run_pipeline(states, sightings_repo, enrichment_repo, scoring)

    async with pool.acquire() as conn:
        hexes = await conn.fetch("SELECT hex FROM aircraft ORDER BY hex")
        assert [r["hex"] for r in hexes] == ["aaa111", "bbb222"]

        sightings = await conn.fetch("SELECT hex, ended_at FROM sightings ORDER BY hex")
        assert len(sightings) == 2
        assert all(s["ended_at"] is not None for s in sightings)

        enriched = await conn.fetch(
            "SELECT hex, story_score FROM enriched_aircraft ORDER BY hex"
        )
        assert len(enriched) == 2
        assert all(r["story_score"] == 7 for r in enriched)

    assert len(scoring.calls) == 1
    assert len(scoring.calls[0]) == 2


async def test_pipeline_ttl_expiry_triggers_re_enrichment(
    sightings_repo: SightingRepository,
    enrichment_repo: EnrichmentRepository,
    pool: asyncpg.Pool,
) -> None:
    await _insert_aircraft(pool, "aaa111", "FL111")
    await enrichment_repo.store(
        hex="aaa111",
        score=3,
        tags=["old"],
        annotation="Old annotation.",
        aircraft_info=None,
        route_info=None,
        callsign=None,
        enrichment_ttl=timedelta(seconds=-1),
    )

    scoring = _MockScoringClient(score=9, tags=["military"])
    states = [make_state(hex="aaa111", flight="FL111")]

    await _run_pipeline(
        states,
        sightings_repo,
        enrichment_repo,
        scoring,
        enrichment_ttl=timedelta(days=30),
        batch_size=1,
    )

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT story_score, story_tags, annotation"
            " FROM enriched_aircraft WHERE hex = 'aaa111'"
        )

    assert row is not None
    assert row["story_score"] == 9
    assert row["story_tags"] == ["military"]
    assert row["annotation"] == "Test annotation."

    assert len(scoring.calls) == 1


async def test_pipeline_handles_poll_error(
    sightings_repo: SightingRepository,
    enrichment_repo: EnrichmentRepository,
    pool: asyncpg.Pool,
) -> None:
    call_count = 0

    async def mock_poll(url: str, timeout: float = 5.0) -> list[AircraftState]:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("poll failed")
        if call_count > 4:
            raise asyncio.CancelledError()
        return [make_state(hex="aaa111", flight="FL111")]

    scoring = _MockScoringClient()

    with patch("squawk.pipeline.tar1090.poll", side_effect=mock_poll):
        task = asyncio.create_task(
            run_pipeline(
                poll_url="http://test",
                poll_interval=0.001,
                session_timeout=300,
                sightings=sightings_repo,
                enrichment_repo=enrichment_repo,
                aircraft_client=_MockAircraftClient(),
                route_client=_MockRouteClient(),
                scoring_client=scoring,
                enrichment_ttl=timedelta(days=30),
                batch_size=1,
                flush_interval=0.001,
            )
        )
        try:
            await task
        except asyncio.CancelledError:
            pass

    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM aircraft WHERE hex = 'aaa111'"
        )

    assert count == 1
    assert len(scoring.calls) >= 1


async def test_pipeline_close_open_sightings_on_shutdown(
    sightings_repo: SightingRepository,
    enrichment_repo: EnrichmentRepository,
    pool: asyncpg.Pool,
) -> None:
    states = [make_state(hex="aaa111", flight="FL111")]

    await _run_pipeline(
        states,
        sightings_repo,
        enrichment_repo,
        _MockScoringClient(),
        batch_size=1,
        max_cycles=2,
    )

    async with pool.acquire() as conn:
        open_count = await conn.fetchval(
            "SELECT COUNT(*) FROM sightings WHERE ended_at IS NULL"
        )

    assert open_count == 0
