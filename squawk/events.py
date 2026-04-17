"""Domain events.

All inter-actor communication goes through the event bus using these frozen
dataclasses. No actor imports another actor's repository.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class HexFirstSeen:
    """Emitted when the polling pipeline records an aircraft hex for the first time."""

    hex: str
    callsign: str | None
    first_seen_at: datetime


@dataclass(frozen=True)
class EnrichmentExpired:
    """Emitted by PollingActor when a known aircraft's enrichment TTL has elapsed.

    Only emitted for hexes that already have an enriched_aircraft row. Brand-new
    hexes are covered by HexFirstSeen — emitting both for the same hex in the same
    poll cycle would cause EnrichmentActor to enrich the same aircraft twice.
    """

    hex: str
    callsign: str | None


@dataclass(frozen=True)
class DigestRequested:
    """Emitted by Scheduler on cron schedule, or by /debug command handler.

    Period windows:
      Scheduler (weekly):  period_start = now - 7 days, period_end = now
      /debug (on-demand):  period_start = now - 24 hours, period_end = now, force = True
    """

    period_start: datetime
    period_end: datetime
    force: bool = False
