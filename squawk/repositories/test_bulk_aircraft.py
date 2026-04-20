"""Integration tests for BulkAircraftRepository against a real TimescaleDB instance."""

from __future__ import annotations

import os
import subprocess

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from squawk.repositories.bulk_aircraft import BulkAircraftRepository

TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16"
MIGRATIONS_DIR = "db/migrations"


def dbmate(db_url: str, *args: str) -> None:
    subprocess.run(
        ["dbmate", "--migrations-dir", MIGRATIONS_DIR, "--no-dump-schema", *args],
        env={**os.environ, "DATABASE_URL": db_url},
        check=True,
    )


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
        await conn.execute("TRUNCATE bulk_aircraft")
    yield p
    await p.close()


@pytest.fixture
def repo(pool: asyncpg.Pool) -> BulkAircraftRepository:
    return BulkAircraftRepository(pool)


async def _ingest(
    repo: BulkAircraftRepository,
    records: list[tuple[str, str | None, str | None, str | None]],
) -> None:
    """Helper: full ingest cycle into the live table via staging."""
    await repo.prepare_ingest()
    await repo.insert_batch_staging(records)
    await repo.commit_ingest()


async def test_lookup_returns_none_when_empty(repo: BulkAircraftRepository) -> None:
    result = await repo.lookup("aabbcc")
    assert result is None


async def test_ingest_and_lookup(repo: BulkAircraftRepository) -> None:
    records: list[tuple[str, str | None, str | None, str | None]] = [
        ("4d216e", "9H-EUC", "A320", "Airbus A320neo"),
        ("406a72", "G-EZWD", "A320", "Airbus A320"),
    ]
    await _ingest(repo, records)

    result = await repo.lookup("4d216e")
    assert result is not None
    assert result.registration == "9H-EUC"
    assert result.icao_type == "A320"
    assert result.type == "Airbus A320neo"

    result2 = await repo.lookup("406a72")
    assert result2 is not None
    assert result2.registration == "G-EZWD"


async def test_lookup_is_case_insensitive(repo: BulkAircraftRepository) -> None:
    await _ingest(repo, [("aabbcc", "D-AIWE", "A320", "Airbus A320neo")])

    assert await repo.lookup("AABBCC") is not None
    assert await repo.lookup("aabbcc") is not None
    assert await repo.lookup("AaBbCc") is not None


async def test_lookup_returns_none_for_all_null_fields(
    repo: BulkAircraftRepository,
) -> None:
    await _ingest(repo, [("ffffff", None, None, None)])
    result = await repo.lookup("ffffff")
    assert result is None


async def test_commit_ingest_replaces_previous_data(
    repo: BulkAircraftRepository,
) -> None:
    """A second ingest replaces all previous data."""
    await _ingest(repo, [("aabbcc", "D-AIWE", "A320", "Airbus A320")])
    await _ingest(repo, [("112233", "G-EZWD", "A320", "Airbus A320")])

    assert await repo.lookup("aabbcc") is None  # previous entry gone
    assert await repo.lookup("112233") is not None  # new entry present


async def test_prepare_ingest_does_not_clear_live_table(
    repo: BulkAircraftRepository,
) -> None:
    """prepare_ingest() must not touch the live table — live reads stay intact."""
    await _ingest(repo, [("aabbcc", "D-AIWE", "A320", "Airbus A320")])

    await repo.prepare_ingest()  # starts a new ingest cycle but does NOT swap yet
    result = await repo.lookup("aabbcc")
    assert result is not None  # old data still visible


async def test_insert_batch_prefers_model_over_icao_type(
    repo: BulkAircraftRepository,
) -> None:
    await _ingest(repo, [("aabbcc", "D-AIWE", "A20N", "Airbus A320neo")])
    result = await repo.lookup("aabbcc")
    assert result is not None
    assert result.type == "Airbus A320neo"
    assert result.icao_type == "A20N"


async def test_insert_batch_falls_back_to_icao_type_when_no_model(
    repo: BulkAircraftRepository,
) -> None:
    await _ingest(repo, [("aabbcc", "D-AIWE", "A20N", None)])
    result = await repo.lookup("aabbcc")
    assert result is not None
    assert result.type == "A20N"
