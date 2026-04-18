"""adsbdb aircraft registry client.

Public API:
    AircraftInfo       — frozen dataclass with registration, type, operator, flag
    AircraftLookupClient — Protocol
    AdsbbClient        — concrete implementation
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
class AircraftInfo:
    registration: str | None
    type: str | None
    operator: str | None
    flag: str | None


class AircraftLookupClient(Protocol):
    async def lookup(self, hex: str) -> AircraftInfo | None: ...


class AdsbbClient:
    """Async adsbdb aircraft lookup with retry policy.

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

    async def lookup(self, hex: str) -> AircraftInfo | None:
        url = f"{self._base_url}/aircraft/{hex.lower()}"
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
                aircraft = (data.get("response") or {}).get("aircraft") or {}
                if not aircraft:
                    return None
                return AircraftInfo(
                    registration=aircraft.get("registration"),
                    type=aircraft.get("type"),
                    operator=aircraft.get("registered_owner"),
                    flag=aircraft.get("registered_owner_country_iso_name"),
                )
        return None  # unreachable; loop always returns or raises
