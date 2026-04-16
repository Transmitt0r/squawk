"""ADK agent tools: flight data queries and aircraft lookup."""

from __future__ import annotations

import json
import logging

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


def make_tools(collector_database_url: str) -> list:
    """Create agent tools closed over the collector DB URL."""

    def get_sightings(days: int = 7) -> str:
        """Get a summary of aircraft sightings observed over the past N days.

        Returns a JSON string with:
        - total_sightings: number of sighting sessions
        - unique_aircraft: number of distinct ICAO hex codes
        - sightings: list of individual sightings with callsign, hex, start time,
          duration, min/max altitude (in feet), min/max distance from receiver (in nautical miles)
        - top_operators: most frequent callsign prefixes (airline codes)

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT
                            s.hex,
                            s.callsign,
                            s.started_at,
                            s.ended_at,
                            EXTRACT(EPOCH FROM (COALESCE(s.ended_at, now()) - s.started_at)) / 60
                                AS duration_minutes,
                            s.min_altitude,
                            s.max_altitude,
                            s.min_distance,
                            s.max_distance
                        FROM sightings s
                        WHERE s.started_at > now() - (%(days)s || ' days')::interval
                        ORDER BY s.started_at DESC
                    """, {"days": days})
                    rows = cur.fetchall()

            sightings = [dict(r) for r in rows]

            # Compute top operator prefixes (first 3 chars of callsign = airline ICAO code)
            prefixes: dict[str, int] = {}
            for s in sightings:
                cs = s.get("callsign") or ""
                if len(cs) >= 3 and cs[:3].isalpha():
                    prefix = cs[:3].upper()
                    prefixes[prefix] = prefixes.get(prefix, 0) + 1
            top_operators = sorted(prefixes.items(), key=lambda x: x[1], reverse=True)[:10]

            # Serialize datetimes
            for s in sightings:
                for k in ("started_at", "ended_at"):
                    if s[k] is not None:
                        s[k] = s[k].isoformat()
                for k in ("duration_minutes", "min_altitude", "max_altitude",
                          "min_distance", "max_distance"):
                    if s[k] is not None:
                        s[k] = float(s[k])

            result = {
                "total_sightings": len(sightings),
                "unique_aircraft": len({s["hex"] for s in sightings}),
                "top_operators": [{"prefix": p, "count": c} for p, c in top_operators],
                "sightings": sightings,
            }
            return json.dumps(result, default=str)

        except Exception as exc:
            logger.exception("get_sightings failed")
            return json.dumps({"error": str(exc)})

    def get_records(days: int = 7) -> str:
        """Get record-breaking sightings from the past N days.

        Returns the week's extremes from what our receiver picked up:
        - furthest_sighting: aircraft seen at greatest distance (nautical miles)
        - highest_altitude: aircraft seen at greatest altitude (feet)
        - fastest_ground_speed: aircraft with highest ground speed (knots)
        - longest_session: aircraft observed continuously for longest time (minutes)

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT hex, callsign, max_distance AS value
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                          AND max_distance IS NOT NULL
                        ORDER BY max_distance DESC
                        LIMIT 1
                    """, {"days": days})
                    furthest = dict(cur.fetchone() or {})

                    cur.execute("""
                        SELECT hex, callsign, max_altitude AS value
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                          AND max_altitude IS NOT NULL
                        ORDER BY max_altitude DESC
                        LIMIT 1
                    """, {"days": days})
                    highest = dict(cur.fetchone() or {})

                    cur.execute("""
                        WITH top AS (
                            SELECT hex, MAX(gs) AS value
                            FROM position_updates
                            WHERE time > now() - (%(days)s || ' days')::interval
                              AND gs IS NOT NULL
                            GROUP BY hex
                            ORDER BY value DESC
                            LIMIT 1
                        )
                        SELECT t.hex, t.value, s.callsign
                        FROM top t
                        LEFT JOIN sightings s ON s.hex = t.hex
                            AND s.started_at > now() - (%(days)s || ' days')::interval
                        ORDER BY s.started_at DESC
                        LIMIT 1
                    """, {"days": days})
                    fastest = dict(cur.fetchone() or {})

                    cur.execute("""
                        SELECT hex, callsign,
                               EXTRACT(EPOCH FROM (COALESCE(ended_at, now()) - started_at)) / 60
                                   AS value
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                        ORDER BY value DESC
                        LIMIT 1
                    """, {"days": days})
                    longest = dict(cur.fetchone() or {})

                    cur.execute("""
                        SELECT hex,
                               COUNT(*) AS visit_count,
                               MODE() WITHIN GROUP (ORDER BY callsign) AS callsign
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                        GROUP BY hex
                        HAVING COUNT(*) > 1
                        ORDER BY visit_count DESC
                        LIMIT 5
                    """, {"days": days})
                    return_visitors = [dict(r) for r in cur.fetchall()]

            for d in (furthest, highest, fastest, longest):
                if d.get("value") is not None:
                    d["value"] = float(d["value"])
            for d in return_visitors:
                d["visit_count"] = int(d["visit_count"])

            return json.dumps({
                "furthest_sighting_nm": furthest,
                "highest_altitude_ft": highest,
                "fastest_ground_speed_kt": fastest,
                "longest_session_min": longest,
                "return_visitors": return_visitors,
            }, default=str)

        except Exception as exc:
            logger.exception("get_records failed")
            return json.dumps({"error": str(exc)})

    def get_new_aircraft(days: int = 7) -> str:
        """Get aircraft seen by our receiver for the very first time during the past N days.

        These are brand-new visitors — never picked up before in the entire history
        of the receiver. Returns hex, first_seen timestamp, callsigns observed,
        and altitude/distance from the sighting.

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT
                            a.hex,
                            a.first_seen,
                            a.callsigns,
                            s.callsign,
                            s.max_altitude,
                            s.max_distance
                        FROM aircraft a
                        LEFT JOIN LATERAL (
                            SELECT callsign, max_altitude, max_distance
                            FROM sightings
                            WHERE hex = a.hex
                            ORDER BY started_at DESC
                            LIMIT 1
                        ) s ON true
                        WHERE a.first_seen > now() - (%(days)s || ' days')::interval
                        ORDER BY a.first_seen DESC
                    """, {"days": days})
                    rows = cur.fetchall()

            result = []
            for r in rows:
                d = dict(r)
                if d.get("first_seen"):
                    d["first_seen"] = d["first_seen"].isoformat()
                if d.get("max_altitude") is not None:
                    d["max_altitude"] = int(d["max_altitude"])
                if d.get("max_distance") is not None:
                    d["max_distance"] = float(d["max_distance"])
                result.append(d)

            return json.dumps({
                "new_aircraft_count": len(result),
                "new_aircraft": result,
            }, default=str)

        except Exception as exc:
            logger.exception("get_new_aircraft failed")
            return json.dumps({"error": str(exc)})

    def get_squawk_alerts(days: int = 7) -> str:
        """Check if any aircraft broadcast emergency squawk codes while over our area.

        Emergency squawk codes:
        - 7700: General emergency
        - 7600: Radio communication failure
        - 7500: Hijack declared

        Returns any incidents with timestamp, aircraft hex, squawk code, and meaning.
        If alerts exist, treat them as the lead story of the digest.

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT DISTINCT ON (hex, squawk)
                            time, hex, squawk
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

            return json.dumps({
                "alert_count": len(alerts),
                "alerts": alerts,
            }, default=str)

        except Exception as exc:
            logger.exception("get_squawk_alerts failed")
            return json.dumps({"error": str(exc)})

    def lookup_aircraft(icao_hex: str) -> str:
        """Look up registration, aircraft type, and operator for an ICAO hex code.

        Uses the public adsbdb.com API. Returns a JSON string with fields:
        registration, type, icao_type, operator, country, or an error message.

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

        Uses the public adsbdb.com API. Returns a JSON string with origin and
        destination airport details (IATA/ICAO codes, city, country), or an
        error message if the route is not known.

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
        get_sightings,
        get_records,
        get_new_aircraft,
        get_squawk_alerts,
        lookup_aircraft,
        lookup_route,
        lookup_photo,
    ]
