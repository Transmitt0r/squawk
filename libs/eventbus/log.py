"""TimescaleDB-backed event log.

Stores and retrieves events as (type, payload) pairs. Serialization and
deserialization of the payload is handled by the caller (EventBus), not here.
This module has no knowledge of domain event types.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import asyncpg


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


@dataclass
class LogEntry:
    id: int
    type: str
    payload: dict
    emitted_at: datetime


class EventLog:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def write(self, event_type: str, payload: dict) -> LogEntry:
        """Write an event to the log and return the created entry.

        Returns the (id, emitted_at) needed for mark_processed().
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO event_log (type, payload)
                VALUES ($1, $2::jsonb)
                RETURNING id, emitted_at
                """,
                event_type,
                json.dumps(payload, default=_json_default),
            )
        return LogEntry(
            id=row["id"],
            type=event_type,
            payload=payload,
            emitted_at=row["emitted_at"],
        )

    async def mark_processed(self, id: int, emitted_at: datetime) -> None:
        """Mark an event as processed.

        Both id and emitted_at are required to hit the composite primary key
        on the hypertable — querying by id alone would require a full scan.
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE event_log
                SET processed_at = now()
                WHERE id = $1 AND emitted_at = $2
                """,
                id,
                emitted_at,
            )

    async def fetch_unprocessed(self, since: timedelta) -> list[LogEntry]:
        """Fetch events that have not been processed within the given window."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, type, payload, emitted_at
                FROM event_log
                WHERE processed_at IS NULL
                  AND emitted_at > now() - $1::interval
                ORDER BY emitted_at ASC
                """,
                since,
            )
        return [
            LogEntry(
                id=row["id"],
                type=row["type"],
                payload=json.loads(row["payload"]),
                emitted_at=row["emitted_at"],
            )
            for row in rows
        ]
