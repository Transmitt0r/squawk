"""UserRepository integration tests against a real TimescaleDB instance.

Uses the actual dbmate migrations from db/migrations/ — same schema as production.
Requires Docker and dbmate on PATH.

Run with:
    uv run pytest squawk/repositories/test_users.py -v
"""

from __future__ import annotations

import os
import subprocess

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from squawk.repositories.users import UserRepository

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
        await conn.execute("TRUNCATE users")
    yield p
    await p.close()


@pytest.fixture
def repo(pool: asyncpg.Pool) -> UserRepository:
    return UserRepository(pool)


# ---------------------------------------------------------------------------
# register
# ---------------------------------------------------------------------------


async def test_register_new_user_returns_true(repo: UserRepository) -> None:
    result = await repo.register(chat_id=123, username="alice")
    assert result is True


async def test_register_already_active_returns_false(repo: UserRepository) -> None:
    await repo.register(chat_id=123, username="alice")
    result = await repo.register(chat_id=123, username="alice")
    assert result is False


async def test_register_reactivates_inactive_user(
    repo: UserRepository, pool: asyncpg.Pool
) -> None:
    await repo.register(chat_id=123, username="alice")
    await repo.unregister(chat_id=123)

    result = await repo.register(chat_id=123, username="alice")
    assert result is True

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT active FROM users WHERE chat_id = 123")
    assert row["active"] is True


async def test_register_updates_username(
    repo: UserRepository, pool: asyncpg.Pool
) -> None:
    await repo.register(chat_id=123, username="alice")
    await repo.register(chat_id=123, username="alice_renamed")

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT username FROM users WHERE chat_id = 123")
    assert row["username"] == "alice_renamed"


async def test_register_with_none_username(
    repo: UserRepository, pool: asyncpg.Pool
) -> None:
    result = await repo.register(chat_id=456, username=None)
    assert result is True

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT username FROM users WHERE chat_id = 456")
    assert row["username"] is None


# ---------------------------------------------------------------------------
# unregister
# ---------------------------------------------------------------------------


async def test_unregister_active_user_returns_true(repo: UserRepository) -> None:
    await repo.register(chat_id=123, username="alice")
    result = await repo.unregister(chat_id=123)
    assert result is True


async def test_unregister_sets_active_false(
    repo: UserRepository, pool: asyncpg.Pool
) -> None:
    await repo.register(chat_id=123, username="alice")
    await repo.unregister(chat_id=123)

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT active FROM users WHERE chat_id = 123")
    assert row["active"] is False


async def test_unregister_unknown_user_returns_false(repo: UserRepository) -> None:
    result = await repo.unregister(chat_id=999)
    assert result is False


async def test_unregister_already_inactive_returns_false(repo: UserRepository) -> None:
    await repo.register(chat_id=123, username="alice")
    await repo.unregister(chat_id=123)
    result = await repo.unregister(chat_id=123)
    assert result is False


# ---------------------------------------------------------------------------
# get_active
# ---------------------------------------------------------------------------


async def test_get_active_empty(repo: UserRepository) -> None:
    result = await repo.get_active()
    assert result == []


async def test_get_active_returns_active_users(repo: UserRepository) -> None:
    await repo.register(chat_id=1, username="alice")
    await repo.register(chat_id=2, username="bob")

    result = await repo.get_active()
    assert sorted(result) == [1, 2]


async def test_get_active_excludes_inactive(repo: UserRepository) -> None:
    await repo.register(chat_id=1, username="alice")
    await repo.register(chat_id=2, username="bob")
    await repo.unregister(chat_id=2)

    result = await repo.get_active()
    assert result == [1]


async def test_get_active_after_reregister(repo: UserRepository) -> None:
    await repo.register(chat_id=1, username="alice")
    await repo.unregister(chat_id=1)
    await repo.register(chat_id=1, username="alice")

    result = await repo.get_active()
    assert result == [1]
