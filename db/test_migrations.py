"""
Migration tests using a real TimescaleDB instance (via testcontainers).

Requires Docker. Run with:
    uv run pytest db/test_migrations.py -v
"""

import os
import subprocess

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16"
MIGRATIONS_DIR = "db/migrations"

EXPECTED_TABLES = {
    "aircraft",
    "sightings",
    "position_updates",
    "enriched_aircraft",
    "callsign_routes",
    "digests",
    "users",
}


def dbmate(db_url: str, *args: str) -> None:
    subprocess.run(
        ["dbmate", "--migrations-dir", MIGRATIONS_DIR, "--no-dump-schema", *args],
        env={**os.environ, "DATABASE_URL": db_url},
        check=True,
    )


@pytest.fixture(scope="module")
def db_url():
    with PostgresContainer(image=TIMESCALE_IMAGE) as container:
        yield container.get_connection_url(driver=None)


async def user_tables(conn: asyncpg.Connection) -> set[str]:
    rows = await conn.fetch(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """
    )
    return {row["table_name"] for row in rows}


@pytest.mark.asyncio
async def test_migrate_up_and_down(db_url: str) -> None:
    # --- up ---
    dbmate(db_url, "up")

    conn = await asyncpg.connect(dsn=db_url)
    try:
        tables = await user_tables(conn)
        assert EXPECTED_TABLES.issubset(tables), (
            f"Missing tables after migrate up: {EXPECTED_TABLES - tables}"
        )

        hypertables = await conn.fetch(
            "SELECT hypertable_name FROM timescaledb_information.hypertables"
        )
        hypertable_names = {row["hypertable_name"] for row in hypertables}
        expected_hypertables = {"position_updates"}
        assert expected_hypertables.issubset(hypertable_names), (
            f"Missing hypertables: {expected_hypertables - hypertable_names}"
        )
    finally:
        await conn.close()

    # --- down (roll back both migrations) ---
    dbmate(db_url, "down")
    dbmate(db_url, "down")

    conn = await asyncpg.connect(dsn=db_url)
    try:
        tables = await user_tables(conn)
        remaining = EXPECTED_TABLES & tables
        assert not remaining, f"Tables still present after migrate down: {remaining}"
    finally:
        await conn.close()
