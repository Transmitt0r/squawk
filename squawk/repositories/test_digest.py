"""DigestRepository integration tests against a real TimescaleDB instance.

Uses the actual dbmate migrations from db/migrations/ — same schema as production.
Requires Docker and dbmate on PATH.

Run with:
    uv run pytest squawk/repositories/test_digest.py -v
"""

from __future__ import annotations

import datetime
import os
import subprocess

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from squawk.digest import DigestOutput
from squawk.repositories.digest import DigestRepository

TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16"
MIGRATIONS_DIR = "db/migrations"

REF_DATE = datetime.date(2026, 4, 13)  # a Sunday
N_DAYS = 7

DIGEST = DigestOutput(
    text="Diese Woche flogen 42 Flugzeuge über uns.",
    photo_url="https://example.com/photo.jpg",
    photo_caption="Boeing 737",
)


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
        await conn.execute("TRUNCATE digests")
    yield p
    await p.close()


@pytest.fixture
def repo(pool: asyncpg.Pool) -> DigestRepository:
    return DigestRepository(pool)


# ---------------------------------------------------------------------------
# get_cached
# ---------------------------------------------------------------------------


async def test_get_cached_returns_none_when_absent(repo: DigestRepository) -> None:
    result = await repo.get_cached(REF_DATE, N_DAYS)
    assert result is None


async def test_get_cached_returns_none_for_different_date(
    repo: DigestRepository,
) -> None:
    await repo.cache(REF_DATE, N_DAYS, DIGEST)

    other_date = REF_DATE + datetime.timedelta(days=7)
    result = await repo.get_cached(other_date, N_DAYS)
    assert result is None


async def test_get_cached_returns_none_for_different_n_days(
    repo: DigestRepository,
) -> None:
    await repo.cache(REF_DATE, N_DAYS, DIGEST)

    result = await repo.get_cached(REF_DATE, 1)
    assert result is None


# ---------------------------------------------------------------------------
# cache + get_cached round-trip
# ---------------------------------------------------------------------------


async def test_cache_and_retrieve(repo: DigestRepository) -> None:
    await repo.cache(REF_DATE, N_DAYS, DIGEST)

    result = await repo.get_cached(REF_DATE, N_DAYS)
    assert result is not None
    assert result.text == DIGEST.text


async def test_get_cached_photo_fields_are_none(repo: DigestRepository) -> None:
    """Photo URL and caption are not persisted — always None from cache."""
    await repo.cache(REF_DATE, N_DAYS, DIGEST)

    result = await repo.get_cached(REF_DATE, N_DAYS)
    assert result is not None
    assert result.photo_url is None
    assert result.photo_caption is None


# ---------------------------------------------------------------------------
# cache — idempotency (upsert)
# ---------------------------------------------------------------------------


async def test_cache_upserts_on_same_key(
    repo: DigestRepository, pool: asyncpg.Pool
) -> None:
    """Second cache() call for the same key updates the content — no duplicate row."""
    await repo.cache(REF_DATE, N_DAYS, DIGEST)

    updated = DigestOutput(
        text="Aktualisierter Digest.",
        photo_url=None,
        photo_caption=None,
    )
    await repo.cache(REF_DATE, N_DAYS, updated)

    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM digests WHERE reference_date = $1 AND n_days = $2",
            REF_DATE,
            N_DAYS,
        )
        row = await conn.fetchrow(
            "SELECT content FROM digests WHERE reference_date = $1 AND n_days = $2",
            REF_DATE,
            N_DAYS,
        )

    assert count == 1
    assert row["content"] == "Aktualisierter Digest."


async def test_cache_allows_different_keys(
    repo: DigestRepository, pool: asyncpg.Pool
) -> None:
    other = DigestOutput(text="Anderer Digest.", photo_url=None, photo_caption=None)
    other_date = REF_DATE + datetime.timedelta(days=7)

    await repo.cache(REF_DATE, N_DAYS, DIGEST)
    await repo.cache(other_date, N_DAYS, other)

    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM digests")

    assert count == 2


# ---------------------------------------------------------------------------
# get_recent
# ---------------------------------------------------------------------------


async def test_get_recent_empty_when_no_digests(repo: DigestRepository) -> None:
    result = await repo.get_recent(3, REF_DATE)
    assert result == []


async def test_get_recent_excludes_on_or_after_before_date(
    repo: DigestRepository,
) -> None:
    await repo.cache(REF_DATE, N_DAYS, DIGEST)

    result = await repo.get_recent(3, REF_DATE)
    assert result == []


async def test_get_recent_returns_prior_digests_newest_first(
    repo: DigestRepository,
) -> None:
    d1 = DigestOutput(text="Digest A", photo_url=None, photo_caption=None)
    d2 = DigestOutput(text="Digest B", photo_url=None, photo_caption=None)
    d3 = DigestOutput(text="Digest C", photo_url=None, photo_caption=None)

    date_a = REF_DATE - datetime.timedelta(days=14)
    date_b = REF_DATE - datetime.timedelta(days=7)
    date_c = REF_DATE - datetime.timedelta(days=1)

    await repo.cache(date_a, 1, d1)
    await repo.cache(date_b, 1, d2)
    await repo.cache(date_c, 1, d3)

    result = await repo.get_recent(3, REF_DATE)
    assert result == ["Digest C", "Digest B", "Digest A"]


async def test_get_recent_respects_n_limit(repo: DigestRepository) -> None:
    for i in range(5):
        date = REF_DATE - datetime.timedelta(days=i + 1)
        d = DigestOutput(text=f"Digest {i}", photo_url=None, photo_caption=None)
        await repo.cache(date, 1, d)

    result = await repo.get_recent(3, REF_DATE)
    assert len(result) == 3
