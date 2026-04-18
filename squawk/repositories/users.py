"""UserRepository — owns the users table.

Written by TelegramBot only. Tracks which Telegram users have registered
for the weekly digest.
"""

from __future__ import annotations

import asyncpg


class UserRepository:
    """Write repository for the users table."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def register(self, chat_id: int, username: str | None) -> bool:
        """Register a user for the digest.

        Upserts on chat_id. Sets active=True and updates username.

        Returns True if the user was newly inserted or reactivated (was
        inactive). Returns False if they were already active.

        Idempotency: safe to call on replay — a duplicate call for an already
        active user returns False and leaves the row unchanged.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH prev AS (
                    SELECT active FROM users WHERE chat_id = $1
                ),
                upserted AS (
                    INSERT INTO users (chat_id, username, active)
                    VALUES ($1, $2, true)
                    ON CONFLICT (chat_id) DO UPDATE
                        SET username = EXCLUDED.username,
                            active   = true
                    RETURNING active
                )
                SELECT
                    (SELECT active FROM prev) AS prev_active
                FROM upserted
                """,
                chat_id,
                username,
            )
        # prev_active is NULL (new row) or False (reactivated) → return True
        # prev_active is True (already active) → return False
        prev_active = row["prev_active"]
        return prev_active is None or prev_active is False

    async def unregister(self, chat_id: int) -> bool:
        """Unregister a user from the digest.

        Sets active=False. Returns True if the user existed and was active,
        False if they were not found or already inactive.

        Idempotency: safe to call twice — second call returns False (already
        inactive) without error.
        """
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE users SET active = false WHERE chat_id = $1 AND active = true",
                chat_id,
            )
        # result is a string like "UPDATE 1" or "UPDATE 0"
        return result == "UPDATE 1"

    async def get_active(self) -> list[int]:
        """Return chat_ids of all active users."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT chat_id FROM users WHERE active = true")
        return [row["chat_id"] for row in rows]
