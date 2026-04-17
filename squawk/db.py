"""Database connection pool factory."""

from __future__ import annotations

import asyncpg


async def create_pool(database_url: str) -> asyncpg.Pool:
    """Create and return an asyncpg connection pool."""
    return await asyncpg.create_pool(dsn=database_url, min_size=2, max_size=10)
