"""ADK agent tools: flight data queries and aircraft lookup."""

from __future__ import annotations

import json
import logging
from typing import Literal

import psycopg2
import psycopg2.extras
import requests
from cachetools import TTLCache

logger = logging.getLogger(__name__)

# Aircraft registrations and routes are stable — cache for 7 days
_aircraft_cache: TTLCache = TTLCache(maxsize=1024, ttl=7 * 24 * 3600)
_route_cache: TTLCache = TTLCache(maxsize=1024, ttl=7 * 24 * 3600)
_photo_cache: TTLCache = TTLCache(maxsize=256, ttl=7 * 24 * 3600)

_SQUAWK_MEANINGS = {
    "7700": "General emergency",
    "7600": "Radio communication failure",
    "7500": "Hijack declared",
}

_SORT_COLUMNS = {
    "closest": ("s.min_distance", "ASC NULLS LAST"),
    "highest": ("s.max_altitude", "DESC NULLS LAST"),
    "longest": ("duration_minutes", "DESC NULLS LAST"),
    "recent": ("s.started_at", "DESC"),
}


def make_tools(collector_database_url: str) -> list:
    """Create agent tools closed over the collector DB URL."""

    def get_stats(days: int = 7) -> str:
        """Get aggregate statistics for the digest Fakten section.

        Returns counts only — no row lists — so it is very compact:
        - total_sightings, unique_aircraft, new_aircraft_count
        - top_operators: list of {prefix, count} sorted by frequency
        - squawk_alert_count: number of emergency squawk events

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT
                            COUNT(*)                          AS total_sightings,
                            COUNT(DISTINCT hex)               AS unique_aircraft,
                            COUNT(DISTINCT callsign)
                                FILTER (WHERE callsign IS NOT NULL) AS unique_callsigns
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                    """, {"days": days})
                    counts = dict(cur.fetchone())

                    cur.execute("""
                        SELECT LEFT(callsign, 3) AS prefix, COUNT(*) AS cnt
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                          AND callsign IS NOT NULL
                          AND LENGTH(callsign) >= 3
                          AND LEFT(callsign, 3) ~ '^[A-Z]{3}$'
                        GROUP BY prefix
                        ORDER BY cnt DESC
                        LIMIT 5
                    """, {"days": days})
                    top_operators = [{"prefix": r["prefix"], "count": int(r["cnt"])}
                                     for r in cur.fetchall()]

                    cur.execute("""
                        SELECT COUNT(*) AS cnt
                        FROM aircraft
                        WHERE first_seen > now() - (%(days)s || ' days')::interval
                    """, {"days": days})
                    new_aircraft_count = int(cur.fetchone()["cnt"])

                    cur.execute("""
                        SELECT COUNT(*) AS cnt
                        FROM position_updates
                        WHERE time > now() - (%(days)s || ' days')::interval
                          AND squawk = ANY(%(codes)s)
                    """, {"days": days, "codes": list(_SQUAWK_MEANINGS.keys())})
                    squawk_alert_count = int(cur.fetchone()["cnt"])

            return json.dumps({
                **{k: int(v) for k, v in counts.items()},
                "new_aircraft_count": new_aircraft_count,
                "top_operators": top_operators,
                "squawk_alert_count": squawk_alert_count,
            })
        except Exception as exc:
            logger.exception("get_stats failed")
            return json.dumps({"error": str(exc)})

    def get_top_sightings(
        days: int = 7,
        sort_by: Literal["closest", "highest", "longest", "recent"] = "closest",
        limit: int = 10,
    ) -> str:
        """Get a ranked list of sightings from the past N days.

        Use this to find interesting flights to highlight. Call it multiple times
        with different sort_by values if you need different angles.

        sort_by options:
        - "closest":  nearest to the receiver (most likely overhead)
        - "highest":  highest altitude seen
        - "longest":  longest continuous observation session
        - "recent":   most recently seen

        Returns hex, callsign, started_at, duration_minutes, max_altitude (feet),
        min_distance (nautical miles) for each sighting.

        Args:
            days: How many days back to look (default 7).
            sort_by: Ranking criterion (default "closest").
            limit: Max rows to return, 1–20 (default 10).
        """
        if sort_by not in _SORT_COLUMNS:
            return json.dumps({"error": f"sort_by must be one of {list(_SORT_COLUMNS)}"})
        limit = max(1, min(limit, 20))
        col, direction = _SORT_COLUMNS[sort_by]

        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(f"""
                        SELECT
                            s.hex,
                            s.callsign,
                            s.started_at,
                            EXTRACT(EPOCH FROM (COALESCE(s.ended_at, now()) - s.started_at)) / 60
                                AS duration_minutes,
                            s.max_altitude,
                            s.min_distance
                        FROM sightings s
                        WHERE s.started_at > now() - (%(days)s || ' days')::interval
                        ORDER BY {col} {direction}
                        LIMIT %(limit)s
                    """, {"days": days, "limit": limit})
                    rows = cur.fetchall()

            lines = ["hex,callsign,started_at_utc,duration_min,max_alt_ft,min_dist_nm"]
            for r in rows:
                lines.append(
                    f"{r['hex']},"
                    f"{r['callsign'] or ''},"
                    f"{r['started_at'].strftime('%Y-%m-%dT%H:%M') if r['started_at'] else ''},"
                    f"{round(float(r['duration_minutes']), 1) if r['duration_minutes'] else ''},"
                    f"{int(r['max_altitude']) if r['max_altitude'] else ''},"
                    f"{round(float(r['min_distance']), 1) if r['min_distance'] else ''}"
                )
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_top_sightings failed")
            return json.dumps({"error": str(exc)})

    def get_record(
        days: int = 7,
        record_type: Literal["furthest", "highest", "fastest", "longest", "return_visitors"] = "furthest",
    ) -> str:
        """Get a single record extreme from the past N days.

        Call once per record type you want to highlight. Each call returns
        a small, focused result.

        record_type options:
        - "furthest":        aircraft seen at greatest distance (nautical miles)
        - "highest":         aircraft seen at greatest altitude (feet)
        - "fastest":         aircraft with highest ground speed (knots)
        - "longest":         aircraft observed for longest continuous session (minutes)
        - "return_visitors": aircraft seen multiple times (top 5, sorted by visit count)

        Args:
            days: How many days back to look (default 7).
            record_type: Which record to fetch (default "furthest").
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    if record_type == "furthest":
                        cur.execute("""
                            SELECT hex, callsign, max_distance AS value
                            FROM sightings
                            WHERE started_at > now() - (%(days)s || ' days')::interval
                              AND max_distance IS NOT NULL
                            ORDER BY max_distance DESC LIMIT 1
                        """, {"days": days})
                        row = cur.fetchone()
                        return json.dumps({"record_type": "furthest_nm", **(dict(row) if row else {})}, default=str)

                    elif record_type == "highest":
                        cur.execute("""
                            SELECT hex, callsign, max_altitude AS value
                            FROM sightings
                            WHERE started_at > now() - (%(days)s || ' days')::interval
                              AND max_altitude IS NOT NULL
                            ORDER BY max_altitude DESC LIMIT 1
                        """, {"days": days})
                        row = cur.fetchone()
                        return json.dumps({"record_type": "highest_ft", **(dict(row) if row else {})}, default=str)

                    elif record_type == "fastest":
                        cur.execute("""
                            WITH top AS (
                                SELECT hex, MAX(gs) AS value
                                FROM position_updates
                                WHERE time > now() - (%(days)s || ' days')::interval
                                  AND gs IS NOT NULL
                                GROUP BY hex ORDER BY value DESC LIMIT 1
                            )
                            SELECT t.hex, t.value, s.callsign
                            FROM top t
                            LEFT JOIN sightings s ON s.hex = t.hex
                                AND s.started_at > now() - (%(days)s || ' days')::interval
                            ORDER BY s.started_at DESC LIMIT 1
                        """, {"days": days})
                        row = cur.fetchone()
                        return json.dumps({"record_type": "fastest_kt", **(dict(row) if row else {})}, default=str)

                    elif record_type == "longest":
                        cur.execute("""
                            SELECT hex, callsign,
                                   EXTRACT(EPOCH FROM (COALESCE(ended_at, now()) - started_at)) / 60 AS value
                            FROM sightings
                            WHERE started_at > now() - (%(days)s || ' days')::interval
                            ORDER BY value DESC LIMIT 1
                        """, {"days": days})
                        row = cur.fetchone()
                        return json.dumps({"record_type": "longest_min", **(dict(row) if row else {})}, default=str)

                    elif record_type == "return_visitors":
                        cur.execute("""
                            SELECT hex, COUNT(*) AS visit_count,
                                   MODE() WITHIN GROUP (ORDER BY callsign) AS callsign
                            FROM sightings
                            WHERE started_at > now() - (%(days)s || ' days')::interval
                            GROUP BY hex HAVING COUNT(*) > 1
                            ORDER BY visit_count DESC LIMIT 5
                        """, {"days": days})
                        rows = cur.fetchall()
                        return json.dumps({
                            "record_type": "return_visitors",
                            "visitors": [dict(r) for r in rows],
                        }, default=str)

                    return json.dumps({"error": f"unknown record_type: {record_type}"})
        except Exception as exc:
            logger.exception("get_record failed")
            return json.dumps({"error": str(exc)})

    def get_new_aircraft(days: int = 7) -> str:
        """Get aircraft seen by our receiver for the very first time during the past N days.

        Returns a compact CSV list: hex, callsign, first_seen_utc.
        Total count is also returned. Use lookup_aircraft(hex) to get full
        details (type, operator, registration) for any that look interesting.

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT a.hex, s.callsign, a.first_seen
                        FROM aircraft a
                        LEFT JOIN LATERAL (
                            SELECT callsign FROM sightings
                            WHERE hex = a.hex
                              AND callsign IS NOT NULL
                            ORDER BY started_at DESC LIMIT 1
                        ) s ON true
                        WHERE a.first_seen > now() - (%(days)s || ' days')::interval
                        ORDER BY a.first_seen DESC
                        LIMIT 20
                    """, {"days": days})
                    rows = cur.fetchall()

                    cur.execute("""
                        SELECT COUNT(*) FROM aircraft
                        WHERE first_seen > now() - (%(days)s || ' days')::interval
                    """, {"days": days})
                    total = cur.fetchone()[0]

            lines = ["hex,callsign,first_seen_utc"]
            for hex_, callsign, first_seen in rows:
                lines.append(f"{hex_},{callsign or ''},"
                             f"{first_seen.strftime('%Y-%m-%dT%H:%M') if first_seen else ''}")

            return f"total_new:{total}\n" + "\n".join(lines)
        except Exception as exc:
            logger.exception("get_new_aircraft failed")
            return json.dumps({"error": str(exc)})

    def get_squawk_alerts(days: int = 7) -> str:
        """Check if any aircraft broadcast emergency squawk codes while over our area.

        Emergency squawk codes: 7700 (general emergency), 7600 (radio failure), 7500 (hijack).
        If alerts exist, treat them as the lead story of the digest.

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT DISTINCT ON (hex, squawk) time, hex, squawk
                        FROM position_updates
                        WHERE time > now() - (%(days)s || ' days')::interval
                          AND squawk = ANY(%(codes)s)
                        ORDER BY hex, squawk, time DESC
                    """, {"days": days, "codes": list(_SQUAWK_MEANINGS.keys())})
                    rows = cur.fetchall()

            alerts = []
            for r in rows:
                d = dict(r)
                d["meaning"] = _SQUAWK_MEANINGS.get(d["squawk"], "Unknown")
                if d.get("time"):
                    d["time"] = d["time"].isoformat()
                alerts.append(d)

            return json.dumps({"alert_count": len(alerts), "alerts": alerts}, default=str)
        except Exception as exc:
            logger.exception("get_squawk_alerts failed")
            return json.dumps({"error": str(exc)})

    def lookup_aircraft(icao_hex: str) -> str:
        """Look up registration, aircraft type, and operator for an ICAO hex code.

        Uses the public adsbdb.com API. Returns registration, type, icao_type,
        operator, country, or an error message.

        Args:
            icao_hex: The 6-character ICAO 24-bit hex address (e.g. "3c6444").
        """
        key = icao_hex.lower()
        if key in _aircraft_cache:
            return _aircraft_cache[key]
        try:
            url = f"https://api.adsbdb.com/v0/aircraft/{key}"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 404:
                return json.dumps({"error": "aircraft not found in database"})
            resp.raise_for_status()
            data = resp.json()
            aircraft = data.get("response", {}).get("aircraft", {})
            result = json.dumps({
                "registration": aircraft.get("registration"),
                "type": aircraft.get("type"),
                "icao_type": aircraft.get("icao_type"),
                "operator": aircraft.get("registered_owner"),
                "country": aircraft.get("registered_owner_country_name"),
                "flag": aircraft.get("registered_owner_country_iso_name"),
            })
            _aircraft_cache[key] = result
            return result
        except Exception as exc:
            logger.exception("lookup_aircraft failed for %s", icao_hex)
            return json.dumps({"error": str(exc)})

    def lookup_route(callsign: str) -> str:
        """Look up the origin and destination airports for a flight callsign.

        Uses the public adsbdb.com API. Returns origin and destination airport
        details (IATA/ICAO codes, city, country), or an error if unknown.

        Args:
            callsign: The flight callsign (e.g. "DLH123", "EZY4241").
        """
        key = callsign.upper().strip()
        if key in _route_cache:
            return _route_cache[key]
        try:
            url = f"https://api.adsbdb.com/v0/callsign/{key}"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 404:
                return json.dumps({"error": "route not found in database"})
            resp.raise_for_status()
            data = resp.json()
            route = data.get("response", {}).get("flightroute", {})
            if not route:
                return json.dumps({"error": "no route data available"})

            def _airport(ap: dict) -> dict:
                return {
                    "iata": ap.get("iata_code"),
                    "icao": ap.get("icao_code"),
                    "name": ap.get("name"),
                    "city": ap.get("municipality"),
                    "country": ap.get("country_name"),
                }

            result = json.dumps({
                "callsign": route.get("callsign"),
                "origin": _airport(route.get("origin", {})),
                "destination": _airport(route.get("destination", {})),
            })
            _route_cache[key] = result
            return result
        except Exception as exc:
            logger.exception("lookup_route failed for %s", callsign)
            return json.dumps({"error": str(exc)})

    def lookup_photo(icao_hex: str) -> str:
        """Look up a photo of an aircraft by its ICAO hex code.

        Uses the planespotters.net public API. Returns the direct image URL
        and photographer credit if a photo is available.

        Args:
            icao_hex: The 6-character ICAO 24-bit hex address (e.g. "3c6444").
        """
        key = icao_hex.lower()
        if key in _photo_cache:
            return _photo_cache[key]
        try:
            url = f"https://api.planespotters.net/pub/photos/hex/{key}"
            resp = requests.get(url, timeout=10, headers={"User-Agent": "squawk-bot/1.0"})
            if resp.status_code == 404:
                return json.dumps({"error": "no photo found"})
            resp.raise_for_status()
            data = resp.json()
            photos = data.get("photos", [])
            if not photos:
                return json.dumps({"error": "no photo found"})
            photo = photos[0]
            result = json.dumps({
                "photo_url": photo.get("thumbnail_large", {}).get("src"),
                "link": photo.get("link"),
                "photographer": photo.get("photographer"),
                "registration": photo.get("aircraft", {}).get("reg"),
            })
            _photo_cache[key] = result
            return result
        except Exception as exc:
            logger.exception("lookup_photo failed for %s", icao_hex)
            return json.dumps({"error": str(exc)})

    return [
        get_stats,
        get_top_sightings,
        get_record,
        get_new_aircraft,
        get_squawk_alerts,
        lookup_aircraft,
        lookup_route,
        lookup_photo,
    ]
