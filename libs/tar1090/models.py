"""AircraftState dataclass — single aircraft observation from tar1090."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class AircraftState:
    """A single aircraft observation from tar1090 JSON."""

    hex: str
    flight: str | None
    alt_baro: int | None  # None when on ground or unavailable
    gs: float | None
    track: float | None
    lat: float | None
    lon: float | None
    r_dst: float | None
    rssi: float | None
    squawk: str | None
    seen: float
    timestamp: datetime

    @classmethod
    def from_json(cls, data: dict[str, Any], now: float) -> AircraftState:
        """Parse an aircraft entry from tar1090 aircraft.json.

        Args:
            data: Single aircraft dict from the ``aircraft`` array.
            now: The ``now`` timestamp from the top-level JSON response.
        """
        seen = float(data.get("seen", 0.0))
        ts = datetime.fromtimestamp(now - seen, tz=timezone.utc)

        return cls(
            hex=data["hex"],
            flight=data.get("flight", "").strip() or None,
            alt_baro=data.get("alt_baro")
            if isinstance(data.get("alt_baro"), int)
            else None,
            gs=data.get("gs"),
            track=data.get("track"),
            lat=data.get("lat"),
            lon=data.get("lon"),
            r_dst=data.get("r_dst"),
            rssi=data.get("rssi"),
            squawk=data.get("squawk"),
            seen=seen,
            timestamp=ts,
        )
