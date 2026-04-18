"""PollingActor — polls tar1090 and emits domain events.

Idempotency
-----------
PollingActor does not process events from the bus — it only emits. It does not
receive events that need to be marked processed. Crash recovery is handled by
close_open_sightings() on startup and shutdown, which is always a safe no-op
when no sightings are open.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import tar1090
from eventbus import EventBus
from squawk.events import EnrichmentExpired, HexFirstSeen
from squawk.repositories.enrichment import EnrichmentRepository
from squawk.repositories.sightings import SightingRepository

logger = logging.getLogger(__name__)


class PollingActor:
    """Polls tar1090 on a fixed interval and emits HexFirstSeen / EnrichmentExpired.

    This actor does not subscribe to any bus events — it has no inbox. It emits
    events only.

    Crash recovery: close_open_sightings() is called on both startup and shutdown.
    It runs UPDATE WHERE ended_at IS NULL, which is a no-op when no sightings
    are open. Under TaskGroup cancellation, both the startup and shutdown paths
    may run sequentially — the double-call is harmless.
    """

    def __init__(
        self,
        poll_url: str,
        poll_interval: float,
        session_timeout: float,
        sightings: SightingRepository,
        enrichment: EnrichmentRepository,
        bus: EventBus,
        enrichment_ttl: timedelta,
    ) -> None:
        self._poll_url = poll_url
        self._poll_interval = poll_interval
        self._session_timeout = session_timeout
        self._sightings = sightings
        self._enrichment = enrichment
        self._bus = bus
        self._enrichment_ttl = enrichment_ttl

    async def run(self) -> None:
        """Poll tar1090 indefinitely, emitting events for new/expired aircraft."""
        # Crash recovery: close any sightings left open by a previous run.
        await self._sightings.close_open_sightings()
        logger.info(
            "polling actor started, polling %s every %.1fs",
            self._poll_url,
            self._poll_interval,
        )

        try:
            while True:
                await self._poll_once()
                await asyncio.sleep(self._poll_interval)
        finally:
            # Graceful shutdown: close open sightings so they don't appear stuck.
            await self._sightings.close_open_sightings()

    async def _poll_once(self) -> None:
        try:
            states = await tar1090.poll(self._poll_url, timeout=self._poll_interval)
        except Exception:
            logger.exception("polling actor: tar1090 poll failed, skipping cycle")
            return

        try:
            new_sightings = await self._sightings.record_poll(
                states, self._session_timeout
            )
        except Exception:
            logger.exception("polling actor: record_poll failed, skipping cycle")
            return

        # Emit HexFirstSeen for aircraft new to the aircraft table.
        now = datetime.now(tz=timezone.utc)
        for ns in new_sightings:
            try:
                await self._bus.emit(
                    HexFirstSeen(hex=ns.hex, callsign=ns.callsign, first_seen_at=now)
                )
            except Exception:
                logger.exception(
                    "polling actor: failed to emit HexFirstSeen for hex=%s", ns.hex
                )

        # Check for enrichment TTL expiry among currently visible aircraft.
        current_hexes = [s.hex for s in states]
        try:
            expired = await self._enrichment.get_expired(
                current_hexes, self._enrichment_ttl
            )
        except Exception:
            logger.exception(
                "polling actor: get_expired failed, skipping enrichment expiry check"
            )
            return

        for hex_, callsign in expired:
            try:
                await self._bus.emit(EnrichmentExpired(hex=hex_, callsign=callsign))
            except Exception:
                logger.exception(
                    "polling actor: failed to emit EnrichmentExpired for hex=%s", hex_
                )
