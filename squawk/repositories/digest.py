"""DigestRepository — owns the digests table.

Written by generate_digest() only. Caches generated digest text keyed by
(reference_date, n_days) to avoid regenerating the same digest on restart.

Cache semantics
---------------
The cache key is (reference_date, n_days) where:
  reference_date = period_end.date() (UTC)
  n_days         = (period_end - period_start).days

Both period_start and period_end shift on every restart, so equality on
raw timestamps is unreliable. A date + window-length key is stable:
the scheduler fires once per week, so reference_date is the same for any
restart within the same UTC day, and n_days is always 7.

Only the generated text is persisted — photo_url and photo_caption are
not cached (they are fetched fresh at generation time and not reproduced
from a cached digest). get_cached returns DigestOutput with null photo
fields.
"""

from __future__ import annotations

import datetime

import asyncpg

from squawk.digest import DigestOutput


class DigestRepository:
    """Write repository for the digests table."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get_cached(
        self,
        reference_date: datetime.date,
        n_days: int,
    ) -> DigestOutput | None:
        """Return cached digest for (reference_date, n_days), or None if absent.

        Photo fields are always None in the returned value — they are not stored.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT content FROM digests WHERE reference_date = $1 AND n_days = $2",
                reference_date,
                n_days,
            )
        if row is None:
            return None
        return DigestOutput(text=row["content"], photo_url=None, photo_caption=None)

    async def get_latest(self) -> DigestOutput | None:
        """Return the most recently cached digest, or None if the table is empty."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT content FROM digests ORDER BY created_at DESC LIMIT 1"
            )
        if row is None:
            return None
        return DigestOutput(text=row["content"], photo_url=None, photo_caption=None)

    async def cache(
        self,
        reference_date: datetime.date,
        n_days: int,
        digest: DigestOutput,
    ) -> None:
        """Persist digest text for (reference_date, n_days).

        Upserts on the UNIQUE (reference_date, n_days) constraint — safe to
        call on replay (idempotent: the stored text is overwritten with the
        same value).
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO digests (reference_date, n_days, content)
                VALUES ($1, $2, $3)
                ON CONFLICT (reference_date, n_days) DO UPDATE
                    SET content    = EXCLUDED.content,
                        created_at = now()
                """,
                reference_date,
                n_days,
                digest.text,
            )
