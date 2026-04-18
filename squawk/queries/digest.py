"""DigestQuery — read-only cross-table queries for digest generation.

Used exclusively by DigestActor. May join any table; owns no tables.

DigestCandidate holds the per-aircraft data needed to generate a digest:
enrichment metadata, sighting aggregates, and route information joined
across enriched_aircraft, sightings, aircraft, and callsign_routes.

DigestStats holds the period-level aggregate statistics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import asyncpg

_SQUAWK_MEANINGS: dict[str, str] = {
    "7700": "General emergency",
    "7600": "Radio communication failure",
    "7500": "Hijack declared",
}


@dataclass(frozen=True)
class DigestCandidate:
    """One aircraft seen during the digest period, with enrichment and route data."""

    hex: str
    visit_count: int
    closest_nm: float | None
    max_alt_ft: int | None
    first_seen: datetime
    callsign: str | None
    registration: str | None
    type: str | None
    operator: str | None
    flag: str | None
    story_score: int | None
    story_tags: list[str]
    annotation: str
    origin_iata: str | None
    origin_city: str | None
    origin_country: str | None
    dest_iata: str | None
    dest_city: str | None
    dest_country: str | None


@dataclass(frozen=True)
class SquawkAlert:
    """One emergency squawk observation during the digest period."""

    time_local: str  # formatted as "Weekday HH:MM" in Europe/Berlin
    hex: str
    squawk: str
    meaning: str


@dataclass(frozen=True)
class DigestStats:
    """Aggregate statistics for the digest period."""

    total_sightings: int
    unique_aircraft: int
    new_aircraft: int
    peak_hour: int | None  # local hour (Europe/Berlin) with most sightings
    peak_count: int | None  # number of sightings in peak_hour
    squawk_alerts: list[SquawkAlert] = field(default_factory=list)


class DigestQuery:
    """Read-only query object for fetching digest data.

    Joins across sightings, aircraft, enriched_aircraft, callsign_routes,
    and position_updates. No writes. Safe to use from multiple tasks.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get_candidates(self, days: int) -> list[DigestCandidate]:
        """Return enriched aircraft seen in the last N days, ranked by story_score.

        Aggregates per-hex across all sightings in the window. The most-used
        callsign for each hex is used for route lookups. Returns up to 20
        candidates ordered by story_score DESC (NULLs last), then visit_count DESC.
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH recent AS (
                    SELECT
                        s.hex,
                        COUNT(*)                                  AS visit_count,
                        MIN(s.min_distance)                       AS closest_nm,
                        MAX(s.max_altitude)                       AS max_alt_ft,
                        MIN(s.started_at)                         AS first_seen,
                        MODE() WITHIN GROUP (ORDER BY s.callsign) AS most_used_callsign
                    FROM sightings s
                    WHERE s.started_at > now() - $1 * interval '1 day'
                    GROUP BY s.hex
                )
                SELECT
                    r.hex,
                    r.visit_count,
                    r.closest_nm,
                    r.max_alt_ft,
                    r.first_seen,
                    r.most_used_callsign          AS callsign,
                    ea.registration,
                    ea.type,
                    ea.operator,
                    ea.flag,
                    ea.story_score,
                    COALESCE(ea.story_tags, '{}') AS story_tags,
                    COALESCE(ea.annotation, '')   AS annotation,
                    cr.origin_iata,
                    cr.origin_city,
                    cr.origin_country,
                    cr.dest_iata,
                    cr.dest_city,
                    cr.dest_country
                FROM recent r
                LEFT JOIN enriched_aircraft ea ON ea.hex = r.hex
                LEFT JOIN callsign_routes cr ON cr.callsign = r.most_used_callsign
                ORDER BY ea.story_score DESC NULLS LAST, r.visit_count DESC
                LIMIT 20
                """,
                days,
            )
        return [
            DigestCandidate(
                hex=row["hex"],
                visit_count=row["visit_count"],
                closest_nm=row["closest_nm"],
                max_alt_ft=row["max_alt_ft"],
                first_seen=row["first_seen"],
                callsign=row["callsign"],
                registration=row["registration"],
                type=row["type"],
                operator=row["operator"],
                flag=row["flag"],
                story_score=row["story_score"],
                story_tags=list(row["story_tags"]),
                annotation=row["annotation"],
                origin_iata=row["origin_iata"],
                origin_city=row["origin_city"],
                origin_country=row["origin_country"],
                dest_iata=row["dest_iata"],
                dest_city=row["dest_city"],
                dest_country=row["dest_country"],
            )
            for row in rows
        ]

    async def get_stats(self, days: int) -> DigestStats:
        """Return aggregate statistics for sightings in the last N days.

        Includes total/unique sighting counts, new aircraft count, peak
        activity hour (Europe/Berlin), and any emergency squawk observations.
        """
        async with self._pool.acquire() as conn:
            counts_row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*)            AS total_sightings,
                    COUNT(DISTINCT hex) AS unique_aircraft
                FROM sightings
                WHERE started_at > now() - $1 * interval '1 day'
                """,
                days,
            )

            new_row = await conn.fetchrow(
                """
                SELECT COUNT(*) AS new_aircraft
                FROM aircraft
                WHERE first_seen > now() - $1 * interval '1 day'
                """,
                days,
            )

            peak_row = await conn.fetchrow(
                """
                SELECT
                    EXTRACT(HOUR FROM started_at AT TIME ZONE 'Europe/Berlin')::int
                        AS hr,
                    COUNT(*) AS cnt
                FROM sightings
                WHERE started_at > now() - $1 * interval '1 day'
                GROUP BY hr
                ORDER BY cnt DESC
                LIMIT 1
                """,
                days,
            )

            squawk_rows = await conn.fetch(
                """
                SELECT DISTINCT ON (hex, squawk)
                    time AT TIME ZONE 'Europe/Berlin' AS time_local,
                    hex,
                    squawk
                FROM position_updates
                WHERE time > now() - $1 * interval '1 day'
                  AND squawk = ANY($2::text[])
                ORDER BY hex, squawk, time DESC
                """,
                days,
                list(_SQUAWK_MEANINGS.keys()),
            )

        squawk_alerts = [
            SquawkAlert(
                time_local=row["time_local"].strftime("%a %H:%M"),
                hex=row["hex"],
                squawk=row["squawk"],
                meaning=_SQUAWK_MEANINGS.get(row["squawk"], "Unknown"),
            )
            for row in squawk_rows
        ]

        return DigestStats(
            total_sightings=counts_row["total_sightings"],
            unique_aircraft=counts_row["unique_aircraft"],
            new_aircraft=new_row["new_aircraft"],
            peak_hour=peak_row["hr"] if peak_row else None,
            peak_count=peak_row["cnt"] if peak_row else None,
            squawk_alerts=squawk_alerts,
        )
