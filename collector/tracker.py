"""Session tracker — core state machine for aircraft sighting management."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import asyncpg

from collector.config import Config
from collector.models import AircraftState

logger = logging.getLogger(__name__)


@dataclass
class ActiveSighting:
    """In-memory state for a currently tracked aircraft."""

    sighting_id: int
    hex: str
    callsign: str | None
    started_at: datetime
    last_seen: datetime
    min_altitude: int | float | None = None
    max_altitude: int | float | None = None
    min_distance: float | None = None
    max_distance: float | None = None


def _min_opt(a: int | float | None, b: int | float | None) -> int | float | None:
    if a is None:
        return b
    if b is None:
        return a
    return min(a, b)


def _max_opt(a: int | float | None, b: int | float | None) -> int | float | None:
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


class SessionTracker:
    """Tracks aircraft sighting sessions across poll cycles.

    Maintains a dict of active sightings keyed by ICAO hex.  Each call to
    ``process_poll`` inserts position rows, opens new sightings, and expires
    stale ones based on ``Config.session_timeout``.
    """

    def __init__(self, pool: asyncpg.Pool, config: Config) -> None:
        self._pool = pool
        self._config = config
        self._active: dict[str, ActiveSighting] = {}

    @property
    def active_count(self) -> int:
        return len(self._active)

    # ------------------------------------------------------------------
    # Startup / shutdown
    # ------------------------------------------------------------------

    async def recover(self) -> None:
        """Close any sightings left open from a previous crash."""
        now = datetime.now(tz=timezone.utc)
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE sightings SET ended_at = $1
                WHERE ended_at IS NULL
                """,
                now,
            )
        logger.info("Crash recovery: closed orphaned sightings (%s)", result)

    async def shutdown(self) -> None:
        """Gracefully close all active sightings."""
        if not self._active:
            return
        now = datetime.now(tz=timezone.utc)
        rows = [
            (
                s.sighting_id,
                now,
                s.min_altitude,
                s.max_altitude,
                s.min_distance,
                s.max_distance,
            )
            for s in self._active.values()
        ]
        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                UPDATE sightings
                SET ended_at = $2, min_altitude = $3, max_altitude = $4,
                    min_distance = $5, max_distance = $6
                WHERE id = $1
                """,
                rows,
            )
        logger.info("Shutdown: closed %d active sightings", len(rows))
        self._active.clear()

    # ------------------------------------------------------------------
    # Core poll processing
    # ------------------------------------------------------------------

    async def process_poll(self, states: list[AircraftState]) -> None:
        """Process a batch of aircraft states from one poll cycle."""
        if not states:
            return

        # Build lookup of current observations
        current: dict[str, AircraftState] = {}
        for s in states:
            # Skip stale observations (seen > 60s ago)
            if s.seen is not None and s.seen > 60.0:
                continue
            current[s.hex] = s

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Expire stale sightings (not in current poll)
                await self._expire_stale(conn, current)

                # 2. Open new sightings / update existing
                await self._upsert_sightings(conn, current)

                # 3. Batch-insert position updates
                await self._insert_positions(conn, current)

    async def _expire_stale(
        self,
        conn: asyncpg.Connection,
        current: dict[str, AircraftState],
    ) -> None:
        """Expire active sightings whose hex is no longer in the poll."""
        to_expire = [
            s for hex_code, s in self._active.items() if hex_code not in current
        ]
        if not to_expire:
            return

        rows = [
            (
                s.sighting_id,
                s.last_seen,
                s.min_altitude,
                s.max_altitude,
                s.min_distance,
                s.max_distance,
            )
            for s in to_expire
        ]
        await conn.executemany(
            """
            UPDATE sightings
            SET ended_at = $2, min_altitude = $3, max_altitude = $4,
                min_distance = $5, max_distance = $6
            WHERE id = $1
            """,
            rows,
        )
        for s in to_expire:
            del self._active[s.hex]
            logger.debug("Expired sighting %d for %s", s.sighting_id, s.hex)

    async def _upsert_sightings(
        self,
        conn: asyncpg.Connection,
        current: dict[str, AircraftState],
    ) -> None:
        """Open new sightings for unseen aircraft, update aggregates for existing."""
        new_states: list[AircraftState] = []
        timeout = self._config.session_timeout

        for hex_code, state in current.items():
            active = self._active.get(hex_code)
            if active is None:
                new_states.append(state)
            else:
                # Check if gap exceeds session timeout → expire + reopen
                gap = (state.timestamp - active.last_seen).total_seconds()
                if gap > timeout:
                    # Expire the old session
                    await conn.execute(
                        """
                        UPDATE sightings
                        SET ended_at = $2, min_altitude = $3, max_altitude = $4,
                            min_distance = $5, max_distance = $6
                        WHERE id = $1
                        """,
                        active.sighting_id,
                        active.last_seen,
                        active.min_altitude,
                        active.max_altitude,
                        active.min_distance,
                        active.max_distance,
                    )
                    del self._active[hex_code]
                    new_states.append(state)
                else:
                    # Update aggregates
                    active.last_seen = state.timestamp
                    active.min_altitude = _min_opt(active.min_altitude, state.alt_baro)
                    active.max_altitude = _max_opt(active.max_altitude, state.alt_baro)
                    active.min_distance = _min_opt(active.min_distance, state.r_dst)
                    active.max_distance = _max_opt(active.max_distance, state.r_dst)

        # Batch-insert new aircraft + sightings
        if new_states:
            await self._open_sightings(conn, new_states)

    async def _open_sightings(
        self,
        conn: asyncpg.Connection,
        states: list[AircraftState],
    ) -> None:
        """Register aircraft and open new sighting sessions."""
        # Upsert aircraft registry
        await conn.executemany(
            """
            INSERT INTO aircraft (hex, first_seen, last_seen, callsigns)
            VALUES ($1, $2, $2, CASE WHEN $3::text IS NOT NULL THEN ARRAY[$3::text] ELSE '{}' END)
            ON CONFLICT (hex) DO UPDATE
            SET last_seen = EXCLUDED.last_seen,
                callsigns = CASE
                    WHEN $3::text IS NOT NULL AND NOT aircraft.callsigns @> ARRAY[$3::text]
                    THEN array_append(aircraft.callsigns, $3::text)
                    ELSE aircraft.callsigns
                END
            """,
            [(s.hex, s.timestamp, s.flight) for s in states],
        )

        # Insert sightings and retrieve IDs
        for state in states:
            sighting_id = await conn.fetchval(
                """
                INSERT INTO sightings (hex, callsign, started_at,
                                       min_altitude, max_altitude,
                                       min_distance, max_distance)
                VALUES ($1, $2, $3, $4, $4, $5, $5)
                RETURNING id
                """,
                state.hex,
                state.flight,
                state.timestamp,
                state.alt_baro,
                state.r_dst,
            )
            self._active[state.hex] = ActiveSighting(
                sighting_id=sighting_id,
                hex=state.hex,
                callsign=state.flight,
                started_at=state.timestamp,
                last_seen=state.timestamp,
                min_altitude=state.alt_baro,
                max_altitude=state.alt_baro,
                min_distance=state.r_dst,
                max_distance=state.r_dst,
            )
            logger.debug(
                "Opened sighting %d for %s (%s)",
                sighting_id,
                state.hex,
                state.flight,
            )

    async def _insert_positions(
        self,
        conn: asyncpg.Connection,
        current: dict[str, AircraftState],
    ) -> None:
        """Batch-insert position updates for all current aircraft."""
        rows = [
            (
                s.timestamp,
                s.hex,
                s.lat,
                s.lon,
                s.alt_baro,
                s.gs,
                s.track,
                s.squawk,
                s.rssi,
            )
            for s in current.values()
        ]
        await conn.executemany(
            """
            INSERT INTO position_updates
                (time, hex, lat, lon, alt_baro, gs, track, squawk, rssi)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            rows,
        )
        logger.debug("Inserted %d position updates", len(rows))
