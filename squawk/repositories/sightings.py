"""SightingRepository — owns aircraft, sightings, position_updates tables.

Written by PollingActor only. No other actor may write to these tables.

Idempotency contract
--------------------
``record_poll`` is safe to call multiple times with the same data (e.g. on
event replay after a crash):

* Aircraft upsert: ``ON CONFLICT (hex) DO UPDATE`` — idempotent.
* Sighting open: guarded by "no open sighting exists for this hex" check
  (step 4 in the poll loop). If a sighting was already opened, the hex falls
  into the "update" branch instead of "open new" — no duplicate created.
* Sighting close: ``UPDATE WHERE ended_at IS NULL`` — idempotent (no-op if
  already closed).
* Position updates: append-only; duplicate rows are accepted for the
  position history (the cost is one extra row per replay, which is negligible).

``close_open_sightings`` is always a no-op when no sightings are open.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import asyncpg

from tar1090 import AircraftState


@dataclass
class NewSighting:
    """Aircraft hex that appeared in the aircraft table for the first time."""

    hex: str
    callsign: str | None


class SightingRepository:
    """Write repository for aircraft, sightings, and position_updates.

    All methods are safe to call concurrently from a single asyncio task
    (PollingActor.run). They are not designed for concurrent multi-task access.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def close_open_sightings(self) -> None:
        """Close every open sighting (ended_at = last_seen).

        Called on PollingActor startup and shutdown for crash recovery.
        Safe to call when no sightings are open (no-op).
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE sightings SET ended_at = last_seen WHERE ended_at IS NULL"
            )

    async def record_poll(
        self,
        states: list[AircraftState],
        session_timeout: float,
    ) -> list[NewSighting]:
        """Process one tar1090 poll cycle.

        Args:
            states: Aircraft visible in this poll. May be empty.
            session_timeout: Seconds of absence before a sighting is closed.

        Returns:
            NewSighting for each hex that was inserted into the aircraft table
            for the first time. Post-gap reappearances are NOT included.

        Three paths per hex per call:
        a) Hex present + open sighting exists → update last_seen, min/max stats.
        b) Hex present + no open sighting:
           - New to aircraft table → INSERT aircraft + sighting, returned as
             NewSighting.
           - Known aircraft (post-gap) → INSERT sighting only, not returned.
        c) Hex absent + open sighting exists → close if timeout exceeded.
        """
        now = datetime.now(tz=timezone.utc)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                return await self._record_poll_tx(conn, states, now, session_timeout)

    # ------------------------------------------------------------------
    # Private implementation
    # ------------------------------------------------------------------

    async def _record_poll_tx(
        self,
        conn: asyncpg.Connection,
        states: list[AircraftState],
        now: datetime,
        session_timeout: float,
    ) -> list[NewSighting]:
        states_by_hex = {s.hex: s for s in states}
        current_hexes = set(states_by_hex)

        # Step 1: Detect hexes that are new to the aircraft table (before upsert).
        new_hexes: set[str] = set()
        if current_hexes:
            rows = await conn.fetch(
                """
                SELECT t.h FROM unnest($1::text[]) AS t(h)
                WHERE NOT EXISTS (SELECT 1 FROM aircraft WHERE hex = t.h)
                """,
                list(current_hexes),
            )
            new_hexes = {r["h"] for r in rows}

        # Step 2: Upsert aircraft for all present hexes.
        if current_hexes:
            await self._upsert_aircraft(conn, states, now)

        # Step 3: Insert position updates for all present aircraft.
        if states:
            await self._insert_positions(conn, states, now)

        # Step 4: Load all currently open sightings (for both present and absent hexes).
        open_rows = await conn.fetch(
            "SELECT id, hex, last_seen, callsign FROM sightings WHERE ended_at IS NULL"
        )
        open_by_hex: dict[str, asyncpg.Record] = {r["hex"]: r for r in open_rows}
        open_hexes = set(open_by_hex)

        # Step 5: Update open sightings for hexes still visible (path a).
        to_update = current_hexes & open_hexes
        if to_update:
            await self._update_sightings(
                conn, states_by_hex, open_by_hex, to_update, now
            )

        # Step 6: Close timed-out sightings for absent hexes (path c).
        absent = open_hexes - current_hexes
        if absent:
            timed_out_ids = [
                open_by_hex[h]["id"]
                for h in absent
                if (now - open_by_hex[h]["last_seen"]).total_seconds() > session_timeout
            ]
            if timed_out_ids:
                await conn.execute(
                    "UPDATE sightings SET ended_at = last_seen"
                    " WHERE id = ANY($1::bigint[])",
                    timed_out_ids,
                )

        # Step 7: Open new sightings for present hexes with no open sighting (path b).
        to_open = current_hexes - open_hexes
        if to_open:
            await self._open_sightings(conn, states_by_hex, to_open, now)

        return [NewSighting(hex=h, callsign=states_by_hex[h].flight) for h in new_hexes]

    async def _upsert_aircraft(
        self, conn: asyncpg.Connection, states: list[AircraftState], now: datetime
    ) -> None:
        """Upsert aircraft rows. Accumulates unique callsigns in the callsigns array."""
        hexes = [s.hex for s in states]
        callsigns = [s.flight for s in states]
        await conn.execute(
            """
            INSERT INTO aircraft (hex, first_seen, last_seen, callsigns)
            SELECT
                t.hex,
                $1,
                $1,
                CASE WHEN t.callsign IS NOT NULL
                     THEN ARRAY[t.callsign]::text[]
                     ELSE '{}'::text[]
                END
            FROM unnest($2::text[], $3::text[]) AS t(hex, callsign)
            ON CONFLICT (hex) DO UPDATE SET
                last_seen = $1,
                callsigns = CASE
                    WHEN EXCLUDED.callsigns = '{}'::text[] THEN aircraft.callsigns
                    WHEN aircraft.callsigns @> EXCLUDED.callsigns
                        THEN aircraft.callsigns
                    ELSE array_cat(aircraft.callsigns, EXCLUDED.callsigns)
                END
            """,
            now,
            hexes,
            callsigns,
        )

    async def _insert_positions(
        self, conn: asyncpg.Connection, states: list[AircraftState], now: datetime
    ) -> None:
        """Append one position_updates row per aircraft in this poll."""
        await conn.execute(
            """
            INSERT INTO position_updates
                (time, hex, lat, lon, alt_baro, gs, track, squawk, rssi)
            SELECT $1, t.hex, t.lat, t.lon, t.alt_baro, t.gs, t.track, t.squawk, t.rssi
            FROM unnest(
                $2::text[],
                $3::float8[],
                $4::float8[],
                $5::int4[],
                $6::float8[],
                $7::float8[],
                $8::text[],
                $9::float8[]
            ) AS t(hex, lat, lon, alt_baro, gs, track, squawk, rssi)
            """,
            now,
            [s.hex for s in states],
            [s.lat for s in states],
            [s.lon for s in states],
            [s.alt_baro for s in states],
            [s.gs for s in states],
            [s.track for s in states],
            [s.squawk for s in states],
            [s.rssi for s in states],
        )

    async def _update_sightings(
        self,
        conn: asyncpg.Connection,
        states_by_hex: dict[str, AircraftState],
        open_by_hex: dict[str, asyncpg.Record],
        hexes: set[str],
        now: datetime,
    ) -> None:
        """Update last_seen and min/max stats for open sightings still visible."""
        ids = []
        alt_baros: list[int | None] = []
        r_dsts: list[float | None] = []
        for h in hexes:
            ids.append(open_by_hex[h]["id"])
            s = states_by_hex[h]
            alt_baros.append(s.alt_baro)
            r_dsts.append(s.r_dst)

        await conn.execute(
            """
            UPDATE sightings SET
                last_seen    = $1,
                min_altitude = LEAST(sightings.min_altitude, t.alt_baro),
                max_altitude = GREATEST(sightings.max_altitude, t.alt_baro),
                min_distance = LEAST(sightings.min_distance, t.r_dst),
                max_distance = GREATEST(sightings.max_distance, t.r_dst)
            FROM unnest($2::bigint[], $3::int4[], $4::float8[])
                AS t(id, alt_baro, r_dst)
            WHERE sightings.id = t.id
            """,
            now,
            ids,
            alt_baros,
            r_dsts,
        )

    async def _open_sightings(
        self,
        conn: asyncpg.Connection,
        states_by_hex: dict[str, AircraftState],
        hexes: set[str],
        now: datetime,
    ) -> None:
        """Open a new sighting row for each hex that has no open sighting."""
        hexes_list = list(hexes)
        callsigns = [states_by_hex[h].flight for h in hexes_list]
        alt_baros = [states_by_hex[h].alt_baro for h in hexes_list]
        r_dsts = [states_by_hex[h].r_dst for h in hexes_list]

        await conn.execute(
            """
            INSERT INTO sightings
                (hex, callsign, started_at, ended_at, last_seen,
                 min_altitude, max_altitude, min_distance, max_distance)
            SELECT
                t.hex, t.callsign, $1, NULL, $1,
                t.alt_baro, t.alt_baro,
                t.r_dst,    t.r_dst
            FROM unnest(
                $2::text[],
                $3::text[],
                $4::int4[],
                $5::float8[]
            ) AS t(hex, callsign, alt_baro, r_dst)
            """,
            now,
            hexes_list,
            callsigns,
            alt_baros,
            r_dsts,
        )
