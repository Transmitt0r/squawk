"""BulkAircraftRepository — owns the bulk_aircraft table.

Written exclusively by the mictronics downloader (daily refresh).
Read by enrich_batch() as a fast local lookup.

Ingest is zero-downtime via a staging table:
    1. prepare_ingest()     — create staging table if needed, truncate it
    2. insert_batch_staging() — stream records into staging (no lock on live table)
    3. commit_ingest()      — atomic swap: truncate live, bulk-copy from staging
                              lock is held only for the INSERT … SELECT (~seconds)

Public API:
    BulkAircraftLookup  — Protocol for read-only lookup (used by pipeline/enrichment)
    BulkAircraftRepository — concrete implementation (also handles writes)
"""

from __future__ import annotations

from typing import Protocol

import asyncpg

from squawk.clients.adsbdb import AircraftInfo


class BulkAircraftLookup(Protocol):
    """Read-only protocol for bulk aircraft DB lookup.

    Satisfied by BulkAircraftRepository and test doubles alike.
    """

    async def lookup(self, hex: str) -> AircraftInfo | None: ...


class BulkAircraftRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def lookup(self, hex: str) -> AircraftInfo | None:
        """Return aircraft info for a given ICAO hex, or None if not found."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT registration, icao_type, model
                FROM bulk_aircraft WHERE hex = $1
                """,
                hex.lower(),
            )
        if row is None:
            return None
        reg = row["registration"]
        icao_type = row["icao_type"]
        model = row["model"]
        if not any([reg, icao_type, model]):
            return None
        return AircraftInfo(
            registration=reg,
            type=model or icao_type,  # prefer human-readable desc, fall back to code
            operator=None,  # mictronics doesn't provide operator
            flag=None,
            icao_type=icao_type,
        )

    async def prepare_ingest(self) -> None:
        """Create the staging table if it doesn't exist, then truncate it.

        Safe to call even while lookups are running — touches only the staging
        table, not the live bulk_aircraft table.
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bulk_aircraft_staging
                    (LIKE bulk_aircraft INCLUDING ALL)
                """
            )
            await conn.execute("TRUNCATE TABLE bulk_aircraft_staging")

    async def insert_batch_staging(
        self,
        records: list[tuple[str, str | None, str | None, str | None]],
    ) -> None:
        """Bulk-insert (hex, registration, icao_type, model) tuples into staging.

        Does not touch the live bulk_aircraft table — no impact on readers.
        """
        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO bulk_aircraft_staging (hex, registration, icao_type, model)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (hex) DO NOTHING
                """,
                records,
            )

    async def commit_ingest(self) -> None:
        """Atomically swap staging data into the live table.

        Acquires ACCESS EXCLUSIVE on bulk_aircraft only for the duration of the
        INSERT … SELECT from the already-populated staging table — typically
        a few seconds. Readers arriving during that window will block briefly
        then see the new data; readers already running continue unaffected.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("TRUNCATE TABLE bulk_aircraft")
                await conn.execute(
                    """
                    INSERT INTO bulk_aircraft (hex, registration, icao_type, model)
                    SELECT hex, registration, icao_type, model
                    FROM bulk_aircraft_staging
                    """
                )
