"""Bot database: user registration and digest cache."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def get_conn(database_url: str):
    return psycopg2.connect(database_url)


def init_schema(database_url: str) -> None:
    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id   BIGINT PRIMARY KEY,
                username  TEXT,
                registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                active    BOOLEAN NOT NULL DEFAULT true
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS digests (
                id           SERIAL PRIMARY KEY,
                period_start TIMESTAMPTZ NOT NULL,
                period_end   TIMESTAMPTZ NOT NULL,
                content      TEXT NOT NULL,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
    logger.info("Bot database schema initialized")


def register_user(database_url: str, chat_id: int, username: str | None) -> bool:
    """Register a user. Returns True if newly registered."""
    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (chat_id, username, active)
            VALUES (%s, %s, true)
            ON CONFLICT (chat_id) DO UPDATE
                SET active = true, username = EXCLUDED.username
            RETURNING (xmax = 0) AS inserted
        """, (chat_id, username))
        row = cur.fetchone()
        return bool(row and row[0])


def unregister_user(database_url: str, chat_id: int) -> bool:
    """Unregister a user. Returns True if was active."""
    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE users SET active = false
            WHERE chat_id = %s AND active = true
        """, (chat_id,))
        return cur.rowcount > 0


def get_active_users(database_url: str) -> list[int]:
    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT chat_id FROM users WHERE active = true")
        return [row[0] for row in cur.fetchall()]


def get_cached_digest(database_url: str, period_start: datetime, period_end: datetime) -> str | None:
    """Return cached digest for this period if it exists."""
    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT content FROM digests
            WHERE period_start = %s AND period_end = %s
            ORDER BY created_at DESC LIMIT 1
        """, (period_start, period_end))
        row = cur.fetchone()
        return row[0] if row else None


def cache_digest(database_url: str, period_start: datetime, period_end: datetime, content: str) -> None:
    """Store a generated digest."""
    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO digests (period_start, period_end, content)
            VALUES (%s, %s, %s)
        """, (period_start, period_end, content))
