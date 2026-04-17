"""Internal HTTP polling logic for tar1090 aircraft.json."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from tar1090.models import AircraftState

logger = logging.getLogger(__name__)


async def poll(url: str, timeout: float = 5.0) -> list[AircraftState]:
    """Fetch aircraft.json from *url* and return parsed AircraftState list.

    Returns an empty list on transient errors (timeout, connection refused)
    so the caller can simply retry on the next poll cycle.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                resp.raise_for_status()
                payload: dict[str, Any] = await resp.json()
    except TimeoutError:
        logger.warning("Poll timed out for %s", url)
        return []
    except aiohttp.ClientError as exc:
        logger.warning("Poll failed: %s", exc)
        return []

    now: float = payload.get("now", 0.0)
    if now == 0.0:
        logger.warning("Response missing 'now' timestamp, skipping")
        return []

    raw_aircraft: list[dict[str, Any]] = payload.get("aircraft", [])
    states: list[AircraftState] = []

    for entry in raw_aircraft:
        if "hex" not in entry:
            continue
        try:
            states.append(AircraftState.from_json(entry, now))
        except (KeyError, TypeError, ValueError) as exc:
            logger.debug(
                "Skipping malformed aircraft entry %s: %s", entry.get("hex"), exc
            )

    logger.debug("Polled %d aircraft (%d raw)", len(states), len(raw_aircraft))
    return states
