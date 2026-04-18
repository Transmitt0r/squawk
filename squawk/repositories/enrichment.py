"""EnrichmentRepository — owns enriched_aircraft and callsign_routes tables.

Written by EnrichmentActor only. No other actor may write to these tables.
PollingActor has read-only access via get_expired().

Idempotency contract
--------------------
``store`` uses upserts on both tables:

* enriched_aircraft: ``ON CONFLICT (hex) DO UPDATE`` — idempotent on replay.
* callsign_routes: ``ON CONFLICT (callsign) DO UPDATE`` — idempotent on replay.

Replaying the same EnrichmentExpired or HexFirstSeen event after a crash
produces the same final DB state. The only observable difference is that
``expires_at`` is recomputed from ``now()`` at store time — for a crash replay
within seconds, this is negligible.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import asyncpg

from squawk.clients.adsbdb import AircraftInfo
from squawk.clients.routes import RouteInfo


class EnrichmentRepository:
    """Write repository for enriched_aircraft and callsign_routes.

    All methods are safe to call concurrently from a single asyncio task
    (EnrichmentActor.run). They are not designed for concurrent multi-task access.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def store(
        self,
        hex: str,
        score: int,
        tags: list[str],
        annotation: str,
        aircraft_info: AircraftInfo | None,
        route_info: RouteInfo | None,
        callsign: str | None,
        enrichment_ttl: timedelta,
    ) -> None:
        """Upsert enrichment data for one aircraft.

        Writes enriched_aircraft unconditionally (upsert on hex).
        Writes callsign_routes if callsign is not None (upsert on callsign).
        Both writes happen in a single transaction.

        Idempotency: both upserts are safe to replay — the result is the same
        row being updated with identical values.
        """
        now = datetime.now(tz=timezone.utc)
        expires_at = now + enrichment_ttl

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO enriched_aircraft
                        (hex, registration, type, operator, flag,
                         story_score, story_tags, annotation,
                         enriched_at, expires_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (hex) DO UPDATE SET
                        registration = EXCLUDED.registration,
                        type         = EXCLUDED.type,
                        operator     = EXCLUDED.operator,
                        flag         = EXCLUDED.flag,
                        story_score  = EXCLUDED.story_score,
                        story_tags   = EXCLUDED.story_tags,
                        annotation   = EXCLUDED.annotation,
                        enriched_at  = EXCLUDED.enriched_at,
                        expires_at   = EXCLUDED.expires_at
                    """,
                    hex,
                    aircraft_info.registration if aircraft_info else None,
                    aircraft_info.type if aircraft_info else None,
                    aircraft_info.operator if aircraft_info else None,
                    aircraft_info.flag if aircraft_info else None,
                    score,
                    tags,
                    annotation,
                    now,
                    expires_at,
                )

                if callsign is not None and route_info is not None:
                    await conn.execute(
                        """
                        INSERT INTO callsign_routes
                            (callsign, origin_iata, origin_icao, origin_city,
                             origin_country, dest_iata, dest_icao, dest_city,
                             dest_country, fetched_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        ON CONFLICT (callsign) DO UPDATE SET
                            origin_iata    = EXCLUDED.origin_iata,
                            origin_icao    = EXCLUDED.origin_icao,
                            origin_city    = EXCLUDED.origin_city,
                            origin_country = EXCLUDED.origin_country,
                            dest_iata      = EXCLUDED.dest_iata,
                            dest_icao      = EXCLUDED.dest_icao,
                            dest_city      = EXCLUDED.dest_city,
                            dest_country   = EXCLUDED.dest_country,
                            fetched_at     = EXCLUDED.fetched_at
                        """,
                        callsign,
                        route_info.origin_iata,
                        route_info.origin_icao,
                        route_info.origin_city,
                        route_info.origin_country,
                        route_info.dest_iata,
                        route_info.dest_icao,
                        route_info.dest_city,
                        route_info.dest_country,
                        now,
                    )

    async def get_expired(
        self,
        hexes: list[str],
        ttl: timedelta,
    ) -> list[tuple[str, str | None]]:
        """Return (hex, callsign) pairs for aircraft whose enrichment has expired.

        Only considers hexes that already have an enriched_aircraft row.
        Brand-new hexes (not yet enriched) are excluded — HexFirstSeen covers them.
        This prevents emitting both HexFirstSeen and EnrichmentExpired for the same
        hex in the same poll cycle.

        ``ttl`` is used to compute the expiry cutoff: any row with
        ``expires_at <= now()`` is considered expired.

        The callsign is read from the aircraft table (most recent callsign seen)
        to avoid a separate lookup when constructing EnrichmentExpired events.
        Returns empty list if hexes is empty.
        """
        if not hexes:
            return []

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ea.hex,
                       (SELECT callsigns[array_length(callsigns, 1)]
                        FROM aircraft
                        WHERE hex = ea.hex) AS callsign
                FROM enriched_aircraft ea
                WHERE ea.hex = ANY($1::text[])
                  AND ea.expires_at <= now()
                """,
                hexes,
            )
        return [(r["hex"], r["callsign"]) for r in rows]
