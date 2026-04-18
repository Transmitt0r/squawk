"""adsbdb route lookup client.

Public API:
    RouteInfo    — frozen dataclass with origin/dest airport fields
    RouteClient  — Protocol
    RoutesClient — concrete implementation
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Protocol

import aiohttp

logger = logging.getLogger(__name__)

_TIMEOUT = aiohttp.ClientTimeout(total=10)


@dataclass(frozen=True)
class RouteInfo:
    origin_iata: str | None
    origin_icao: str | None
    origin_city: str | None
    origin_country: str | None
    dest_iata: str | None
    dest_icao: str | None
    dest_city: str | None
    dest_country: str | None


class RouteClient(Protocol):
    async def lookup(self, callsign: str) -> RouteInfo | None: ...


class RoutesClient:
    """Async adsbdb route lookup with retry policy.

    Retry policy:
        404  → return None
        429  → exponential backoff, up to max_retries
        5xx  → exponential backoff, up to max_retries
        other → raise immediately
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        base_url: str = "https://api.adsbdb.com/v0",
        max_retries: int = 3,
    ) -> None:
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._max_retries = max_retries

    async def lookup(self, callsign: str) -> RouteInfo | None:
        url = f"{self._base_url}/callsign/{callsign.upper().strip()}"
        for attempt in range(self._max_retries + 1):
            async with self._session.get(url, timeout=_TIMEOUT) as resp:
                if resp.status == 404:
                    return None
                if resp.status == 429 or resp.status >= 500:
                    if attempt < self._max_retries:
                        await asyncio.sleep(2**attempt)
                        continue
                    resp.raise_for_status()
                resp.raise_for_status()
                data = await resp.json()
                route = (data.get("response") or {}).get("flightroute") or {}
                if not route:
                    return None
                origin = route.get("origin") or {}
                dest = route.get("destination") or {}
                return RouteInfo(
                    origin_iata=origin.get("iata_code"),
                    origin_icao=origin.get("icao_code"),
                    origin_city=origin.get("municipality"),
                    origin_country=origin.get("country_name"),
                    dest_iata=dest.get("iata_code"),
                    dest_icao=dest.get("icao_code"),
                    dest_city=dest.get("municipality"),
                    dest_country=dest.get("country_name"),
                )
        return None  # unreachable
