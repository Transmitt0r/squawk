"""ADK agent tools: flight data queries and aircraft lookup."""

from __future__ import annotations

import json
import logging

import psycopg2
import psycopg2.extras
import requests

logger = logging.getLogger(__name__)


def make_tools(collector_database_url: str) -> list:
    """Create agent tools closed over the collector DB URL."""

    def get_sightings(days: int = 7) -> str:
        """Get a summary of aircraft sightings observed over the past N days.

        Returns a JSON string with:
        - total_sightings: number of sighting sessions
        - unique_aircraft: number of distinct ICAO hex codes
        - sightings: list of individual sightings with callsign, hex, start time,
          duration, min/max altitude, min/max distance from receiver
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

    def lookup_aircraft(icao_hex: str) -> str:
        """Look up registration, aircraft type, and operator for an ICAO hex code.

        Uses the public adsbdb.com API. Returns a JSON string with fields:
        registration, type, icao_type, operator, country, or an error message.

        Args:
            icao_hex: The 6-character ICAO 24-bit hex address (e.g. "3c6444").
        """
        try:
            url = f"https://api.adsbdb.com/v0/aircraft/{icao_hex.lower()}"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 404:
                return json.dumps({"error": "aircraft not found in database"})
            resp.raise_for_status()
            data = resp.json()
            aircraft = data.get("response", {}).get("aircraft", {})
            return json.dumps({
                "registration": aircraft.get("registration"),
                "type": aircraft.get("type"),
                "icao_type": aircraft.get("icao_type"),
                "operator": aircraft.get("registered_owner"),
                "country": aircraft.get("registered_owner_country_name"),
                "flag": aircraft.get("registered_owner_country_iso_name"),
            })
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
        try:
            url = f"https://api.adsbdb.com/v0/callsign/{callsign.upper().strip()}"
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

            return json.dumps({
                "callsign": route.get("callsign"),
                "origin": _airport(route.get("origin", {})),
                "destination": _airport(route.get("destination", {})),
            })
        except Exception as exc:
            logger.exception("lookup_route failed for %s", callsign)
            return json.dumps({"error": str(exc)})

    return [get_sightings, lookup_aircraft, lookup_route]
