"""Digest flow integration tests against a real TimescaleDB instance.

Tests the generate_digest() function end-to-end: query, cache, broadcast.
Uses mock DigestClient, PhotoClient, and Broadcaster; real DB for queries and caching.

Run with:
    uv run pytest squawk/test_digest.py -v
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from squawk.clients.planespotters import PhotoInfo
from squawk.digest import DigestOutput, generate_digest
from squawk.queries.digest import DigestQuery
from squawk.repositories.digest import DigestRepository

TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16"
MIGRATIONS_DIR = "db/migrations"

DIGEST_OUTPUT = DigestOutput(
    text="Test digest: Diese Woche flogen Flugzeuge.",
    photo_url=None,
    photo_caption=None,
)

DIGEST_WITH_PHOTO = DigestOutput(
    text="Test digest with photo.",
    photo_url="https://example.com/photo.jpg",
    photo_caption="📸 D-ABCD — Airbus A320",
)


def dbmate(db_url: str, *args: str) -> None:
    subprocess.run(
        ["dbmate", "--migrations-dir", MIGRATIONS_DIR, "--no-dump-schema", *args],
        env={**os.environ, "DATABASE_URL": db_url},
        check=True,
    )


# ---------------------------------------------------------------------------
# Mock clients
# ---------------------------------------------------------------------------


class _MockDigestClient:
    def __init__(self, output: DigestOutput | None = None) -> None:
        self._output = output or DIGEST_OUTPUT
        self.calls: list[tuple] = []

    async def generate(self, candidates, stats, photos):
        self.calls.append((candidates, stats, photos))
        return self._output


class _MockBroadcaster:
    def __init__(self) -> None:
        self.calls: list[DigestOutput] = []

    async def broadcast(self, digest: DigestOutput) -> None:
        self.calls.append(digest)


class _MockPhotoClient:
    def __init__(self, photo: PhotoInfo | None = None) -> None:
        self._photo = photo
        self.calls: list[str] = []

    async def lookup(self, hex: str) -> PhotoInfo | None:
        self.calls.append(hex)
        return self._photo


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


async def _insert_aircraft(
    pool: asyncpg.Pool,
    hex: str,
    *,
    callsigns: list[str] | None = None,
) -> None:
    now = datetime.now(tz=timezone.utc)
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


async def _seed_data(pool: asyncpg.Pool) -> None:
    await _insert_aircraft(pool, "aaa111", callsigns=["FL111"])
    await _insert_sighting(pool, "aaa111", callsign="FL111")
    await _insert_enrichment(
        pool,
        "aaa111",
        story_score=8,
        story_tags=["widebody", "long-haul"],
        annotation="Boeing 787 Dreamliner.",
        registration="G-TEST",
        type_="B789",
        operator="Test Airways",
    )
    await _insert_route(pool, "FL111")


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
            "TRUNCATE digests, enriched_aircraft, callsign_routes, sightings,"
            " position_updates, aircraft CASCADE"
        )
    yield p
    await p.close()


@pytest.fixture
def digest_repo(pool: asyncpg.Pool) -> DigestRepository:
    return DigestRepository(pool)


@pytest.fixture
def query(pool: asyncpg.Pool) -> DigestQuery:
    return DigestQuery(pool)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_generate_digest_fresh(
    pool: asyncpg.Pool,
    digest_repo: DigestRepository,
    query: DigestQuery,
) -> None:
    await _seed_data(pool)

    digest_client = _MockDigestClient()
    broadcaster = _MockBroadcaster()
    photo_client = _MockPhotoClient()

    now = datetime.now(tz=timezone.utc)
    period_start = now - timedelta(days=7)
    period_end = now

    await generate_digest(
        query=query,
        digest_repo=digest_repo,
        photo_client=photo_client,
        digest_client=digest_client,
        broadcaster=broadcaster,
        period_start=period_start,
        period_end=period_end,
        force=False,
    )

    assert len(digest_client.calls) == 1
    candidates, stats, photos = digest_client.calls[0]
    assert len(candidates) == 1
    assert candidates[0]["hex"] == "aaa111"
    assert stats["total_sightings"] == 1

    assert len(photo_client.calls) == 1
    assert photo_client.calls[0] == "aaa111"

    assert len(broadcaster.calls) == 1
    assert broadcaster.calls[0].text == DIGEST_OUTPUT.text

    cached = await digest_repo.get_cached(period_end.date(), 7)
    assert cached is not None
    assert cached.text == DIGEST_OUTPUT.text


async def test_generate_digest_cache_hit(
    pool: asyncpg.Pool,
    digest_repo: DigestRepository,
    query: DigestQuery,
) -> None:
    cached_text = "Cached digest from last week."
    cached_digest = DigestOutput(text=cached_text, photo_url=None, photo_caption=None)

    now = datetime.now(tz=timezone.utc)
    period_end = now
    reference_date = period_end.date()
    n_days = 7
    period_start = now - timedelta(days=n_days)

    await digest_repo.cache(reference_date, n_days, cached_digest)

    digest_client = _MockDigestClient()
    broadcaster = _MockBroadcaster()

    await generate_digest(
        query=query,
        digest_repo=digest_repo,
        photo_client=_MockPhotoClient(),
        digest_client=digest_client,
        broadcaster=broadcaster,
        period_start=period_start,
        period_end=period_end,
        force=False,
    )

    assert len(digest_client.calls) == 0
    assert len(broadcaster.calls) == 1
    assert broadcaster.calls[0].text == cached_text


async def test_generate_digest_force_bypasses_cache(
    pool: asyncpg.Pool,
    digest_repo: DigestRepository,
    query: DigestQuery,
) -> None:
    await _seed_data(pool)

    cached_digest = DigestOutput(
        text="Old cached digest.", photo_url=None, photo_caption=None
    )

    now = datetime.now(tz=timezone.utc)
    period_end = now
    reference_date = period_end.date()
    n_days = 7
    period_start = now - timedelta(days=n_days)

    await digest_repo.cache(reference_date, n_days, cached_digest)

    new_output = DigestOutput(
        text="Freshly generated digest.", photo_url=None, photo_caption=None
    )
    digest_client = _MockDigestClient(output=new_output)
    broadcaster = _MockBroadcaster()

    await generate_digest(
        query=query,
        digest_repo=digest_repo,
        photo_client=_MockPhotoClient(),
        digest_client=digest_client,
        broadcaster=broadcaster,
        period_start=period_start,
        period_end=period_end,
        force=True,
    )

    assert len(digest_client.calls) == 1
    assert len(broadcaster.calls) == 1
    assert broadcaster.calls[0].text == "Freshly generated digest."

    cached = await digest_repo.get_cached(reference_date, n_days)
    assert cached is not None
    assert cached.text == "Freshly generated digest."


async def test_generate_digest_fetches_photos_for_top_candidates(
    pool: asyncpg.Pool,
    digest_repo: DigestRepository,
    query: DigestQuery,
) -> None:
    await _insert_aircraft(pool, "aaa111", callsigns=["FL111"])
    await _insert_sighting(pool, "aaa111", callsign="FL111")
    await _insert_enrichment(pool, "aaa111", story_score=9)
    await _insert_route(pool, "FL111")

    await _insert_aircraft(pool, "bbb222", callsigns=["FL222"])
    await _insert_sighting(pool, "bbb222", callsign="FL222")
    await _insert_enrichment(pool, "bbb222", story_score=7)
    await _insert_route(pool, "FL222")

    await _insert_aircraft(pool, "ccc333", callsigns=["FL333"])
    await _insert_sighting(pool, "ccc333", callsign="FL333")
    await _insert_enrichment(pool, "ccc333", story_score=3)

    photo = PhotoInfo(url="https://example.com/aaa111.jpg", caption="A320")
    photo_client = _MockPhotoClient(photo=photo)
    digest_client = _MockDigestClient()
    broadcaster = _MockBroadcaster()

    now = datetime.now(tz=timezone.utc)
    await generate_digest(
        query=query,
        digest_repo=digest_repo,
        photo_client=photo_client,
        digest_client=digest_client,
        broadcaster=broadcaster,
        period_start=now - timedelta(days=7),
        period_end=now,
        force=True,
    )

    assert set(photo_client.calls) == {"aaa111", "bbb222"}

    _, _, photos = digest_client.calls[0]
    assert "aaa111" in photos
    assert photos["aaa111"].url == "https://example.com/aaa111.jpg"
