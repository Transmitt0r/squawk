"""Planespotters photo lookup client.

Public API:
    PhotoInfo            — frozen dataclass with url and caption
    PhotoClient          — Protocol
    PlanespottersClient  — concrete implementation
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Protocol

import aiohttp

logger = logging.getLogger(__name__)

_TIMEOUT = aiohttp.ClientTimeout(total=10)
_HEADERS = {"User-Agent": "squawk/1.0"}


@dataclass(frozen=True)
class PhotoInfo:
    url: str
    caption: str


class PhotoClient(Protocol):
    async def lookup(self, hex: str) -> PhotoInfo | None: ...


class PlanespottersClient:
    """Async Planespotters photo lookup with retry policy.

    Retry policy:
        404  → return None
        429  → exponential backoff, up to max_retries
        5xx  → exponential backoff, up to max_retries
        other → raise immediately
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        base_url: str = "https://api.planespotters.net/pub",
        max_retries: int = 3,
    ) -> None:
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._max_retries = max_retries

    async def lookup(self, hex: str) -> PhotoInfo | None:
        url = f"{self._base_url}/photos/hex/{hex.lower()}"
        for attempt in range(self._max_retries + 1):
            async with self._session.get(
                url, timeout=_TIMEOUT, headers=_HEADERS
            ) as resp:
                if resp.status == 404:
                    return None
                if resp.status == 429 or resp.status >= 500:
                    if attempt < self._max_retries:
                        await asyncio.sleep(2**attempt)
                        continue
                    resp.raise_for_status()
                resp.raise_for_status()
                data = await resp.json()
                photos = data.get("photos") or []
                if not photos:
                    return None
                photo = photos[0]
                photo_url = (photo.get("thumbnail_large") or {}).get("src")
                if not photo_url:
                    return None
                registration = (photo.get("aircraft") or {}).get("reg") or ""
                photographer = photo.get("photographer") or ""
                caption = f"📸 {registration}" if registration else ""
                if photographer:
                    caption = f"{caption} — {photographer}".lstrip(" — ")
                return PhotoInfo(url=photo_url, caption=caption)
        return None  # unreachable
