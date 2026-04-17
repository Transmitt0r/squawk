"""Tests for tar1090.poll() and AircraftState."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tar1090 import AircraftState, poll

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE: dict[str, Any] = {
    "now": 1700000000.0,
    "aircraft": [
        {
            "hex": "3c6752",
            "flight": "DLH1A  ",
            "alt_baro": 36000,
            "gs": 450.0,
            "track": 180.0,
            "lat": 48.76,
            "lon": 9.15,
            "squawk": "1000",
            "r_dst": 5.2,
            "rssi": -20.0,
            "seen": 1.5,
        },
        {
            "hex": "aaaaaa",
            "seen": 0.0,
        },
    ],
}


def _mock_http(payload: dict[str, Any] | Exception) -> MagicMock:
    """Patch aiohttp.ClientSession so GET returns *payload*."""
    resp = AsyncMock()
    resp.raise_for_status = MagicMock()
    resp.json = AsyncMock(return_value=payload)

    get_ctx = AsyncMock()
    get_ctx.__aenter__ = AsyncMock(return_value=resp)
    get_ctx.__aexit__ = AsyncMock(return_value=False)

    session = MagicMock()
    if isinstance(payload, Exception):
        session.get = MagicMock(side_effect=payload)
    else:
        session.get = MagicMock(return_value=get_ctx)

    session_ctx = MagicMock()
    session_ctx.__aenter__ = AsyncMock(return_value=session)
    session_ctx.__aexit__ = AsyncMock(return_value=False)

    return session_ctx


# ---------------------------------------------------------------------------
# Tests — poll()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_poll_parses_aircraft() -> None:
    with patch(
        "tar1090._http.aiohttp.ClientSession", return_value=_mock_http(SAMPLE_RESPONSE)
    ):
        states = await poll("http://localhost/data/aircraft.json")

    assert len(states) == 2
    assert states[0].hex == "3c6752"
    assert states[0].flight == "DLH1A"  # trailing spaces stripped
    assert states[0].alt_baro == 36000
    assert states[1].hex == "aaaaaa"
    assert states[1].flight is None


@pytest.mark.asyncio
async def test_poll_skips_entries_without_hex() -> None:
    payload = {
        "now": 1700000000.0,
        "aircraft": [{"flight": "NOHEX"}],
    }
    with patch("tar1090._http.aiohttp.ClientSession", return_value=_mock_http(payload)):
        states = await poll("http://localhost/data/aircraft.json")

    assert states == []


@pytest.mark.asyncio
async def test_poll_returns_empty_on_timeout() -> None:
    with patch(
        "tar1090._http.aiohttp.ClientSession",
        return_value=_mock_http(TimeoutError("timed out")),
    ):
        states = await poll("http://localhost/data/aircraft.json")

    assert states == []


@pytest.mark.asyncio
async def test_poll_returns_empty_on_connection_error() -> None:
    import aiohttp as _aiohttp

    with patch(
        "tar1090._http.aiohttp.ClientSession",
        return_value=_mock_http(_aiohttp.ClientConnectionError("refused")),
    ):
        states = await poll("http://localhost/data/aircraft.json")

    assert states == []


@pytest.mark.asyncio
async def test_poll_returns_empty_when_now_missing() -> None:
    payload: dict[str, Any] = {"aircraft": [{"hex": "abc123"}]}
    with patch("tar1090._http.aiohttp.ClientSession", return_value=_mock_http(payload)):
        states = await poll("http://localhost/data/aircraft.json")

    assert states == []


# ---------------------------------------------------------------------------
# Tests — AircraftState
# ---------------------------------------------------------------------------


def test_aircraft_state_ground_alt_baro_is_none() -> None:
    data = {"hex": "abc123", "alt_baro": "ground", "seen": 0.0}
    state = AircraftState.from_json(data, now=1700000000.0)
    assert state.alt_baro is None


def test_aircraft_state_int_alt_baro_preserved() -> None:
    data = {"hex": "abc123", "alt_baro": 10000, "seen": 0.0}
    state = AircraftState.from_json(data, now=1700000000.0)
    assert state.alt_baro == 10000


def test_aircraft_state_flight_stripped() -> None:
    data = {"hex": "abc123", "flight": "  BAW123  ", "seen": 0.0}
    state = AircraftState.from_json(data, now=1700000000.0)
    assert state.flight == "BAW123"


def test_aircraft_state_empty_flight_is_none() -> None:
    data = {"hex": "abc123", "flight": "   ", "seen": 0.0}
    state = AircraftState.from_json(data, now=1700000000.0)
    assert state.flight is None
