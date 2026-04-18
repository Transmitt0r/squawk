"""Tests for squawk external clients.

Covers: 200, 404 (→ None), 429 (→ retry), 500 (→ retry),
        exhausted retries (→ raise), and response parsing.

Uses aiohttp's built-in test utilities (aiohttp.test_utils) and pytest-asyncio.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from squawk.clients.adsbdb import AdsbbClient, AircraftInfo
from squawk.clients.planespotters import PlanespottersClient
from squawk.clients.routes import RouteInfo, RoutesClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_session(responses: list[tuple[int, dict | None]]) -> aiohttp.ClientSession:
    """Build a mock aiohttp.ClientSession returning the given (status, json) pairs."""
    session = MagicMock(spec=aiohttp.ClientSession)
    call_count = 0

    @asynccontextmanager
    async def _get(url: str, **kwargs) -> AsyncIterator[MagicMock]:
        nonlocal call_count
        status, body = responses[min(call_count, len(responses) - 1)]
        call_count += 1
        resp = MagicMock()
        resp.status = status
        resp.json = AsyncMock(return_value=body)
        resp.raise_for_status = MagicMock(
            side_effect=aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=status
            )
            if status >= 400
            else None
        )
        yield resp

    session.get = _get
    return session


# ---------------------------------------------------------------------------
# AdsbbClient
# ---------------------------------------------------------------------------


class TestAdsbbClient:
    async def test_200_returns_aircraft_info(self) -> None:
        body = {
            "response": {
                "aircraft": {
                    "registration": "D-ABCD",
                    "type": "Boeing 737-800",
                    "registered_owner": "Ryanair",
                    "registered_owner_country_iso_name": "IE",
                }
            }
        }
        session = _mock_session([(200, body)])
        client = AdsbbClient(session, max_retries=3)
        result = await client.lookup("3c6547")
        assert result == AircraftInfo(
            registration="D-ABCD",
            type="Boeing 737-800",
            operator="Ryanair",
            flag="IE",
        )

    async def test_404_returns_none(self) -> None:
        session = _mock_session([(404, None)])
        client = AdsbbClient(session, max_retries=3)
        result = await client.lookup("000000")
        assert result is None

    async def test_429_retries_then_succeeds(self) -> None:
        body = {
            "response": {
                "aircraft": {
                    "registration": "D-EFAB",
                    "type": "Piper PA-28",
                    "registered_owner": "Private",
                    "registered_owner_country_iso_name": "DE",
                }
            }
        }
        session = _mock_session([(429, None), (200, body)])
        client = AdsbbClient(session, max_retries=3)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await client.lookup("3d1111")
        assert result is not None
        assert result.registration == "D-EFAB"

    async def test_500_retries_then_succeeds(self) -> None:
        body = {
            "response": {
                "aircraft": {
                    "registration": "D-EFAB",
                    "type": "Piper PA-28",
                    "registered_owner": "Private",
                    "registered_owner_country_iso_name": "DE",
                }
            }
        }
        session = _mock_session([(500, None), (200, body)])
        client = AdsbbClient(session, max_retries=3)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await client.lookup("3d1111")
        assert result is not None

    async def test_exhausted_retries_raises(self) -> None:
        session = _mock_session([(500, None)] * 4)
        client = AdsbbClient(session, max_retries=3)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(aiohttp.ClientResponseError):
                await client.lookup("3d1111")

    async def test_other_4xx_raises_immediately(self) -> None:
        session = _mock_session([(403, None)])
        client = AdsbbClient(session, max_retries=3)
        with pytest.raises(aiohttp.ClientResponseError):
            await client.lookup("3d1111")

    async def test_empty_aircraft_returns_none(self) -> None:
        body = {"response": {"aircraft": {}}}
        session = _mock_session([(200, body)])
        client = AdsbbClient(session, max_retries=3)
        result = await client.lookup("3d1111")
        assert result is None

    async def test_missing_response_returns_none(self) -> None:
        body = {}
        session = _mock_session([(200, body)])
        client = AdsbbClient(session, max_retries=3)
        result = await client.lookup("3d1111")
        assert result is None


# ---------------------------------------------------------------------------
# RoutesClient
# ---------------------------------------------------------------------------


class TestRoutesClient:
    async def test_200_returns_route_info(self) -> None:
        body = {
            "response": {
                "flightroute": {
                    "origin": {
                        "iata_code": "STR",
                        "icao_code": "EDDS",
                        "municipality": "Stuttgart",
                        "country_name": "Germany",
                    },
                    "destination": {
                        "iata_code": "BCN",
                        "icao_code": "LEBL",
                        "municipality": "Barcelona",
                        "country_name": "Spain",
                    },
                }
            }
        }
        session = _mock_session([(200, body)])
        client = RoutesClient(session, max_retries=3)
        result = await client.lookup("FR1234")
        assert result == RouteInfo(
            origin_iata="STR",
            origin_icao="EDDS",
            origin_city="Stuttgart",
            origin_country="Germany",
            dest_iata="BCN",
            dest_icao="LEBL",
            dest_city="Barcelona",
            dest_country="Spain",
        )

    async def test_404_returns_none(self) -> None:
        session = _mock_session([(404, None)])
        client = RoutesClient(session, max_retries=3)
        assert await client.lookup("UNKNOWN") is None

    async def test_429_retries_then_succeeds(self) -> None:
        body = {
            "response": {
                "flightroute": {
                    "origin": {
                        "iata_code": "STR",
                        "icao_code": "EDDS",
                        "municipality": "Stuttgart",
                        "country_name": "Germany",
                    },
                    "destination": {
                        "iata_code": "LHR",
                        "icao_code": "EGLL",
                        "municipality": "London",
                        "country_name": "UK",
                    },
                }
            }
        }
        session = _mock_session([(429, None), (200, body)])
        client = RoutesClient(session, max_retries=3)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await client.lookup("BA456")
        assert result is not None
        assert result.dest_iata == "LHR"

    async def test_exhausted_retries_raises(self) -> None:
        session = _mock_session([(429, None)] * 4)
        client = RoutesClient(session, max_retries=3)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(aiohttp.ClientResponseError):
                await client.lookup("BA456")

    async def test_empty_flightroute_returns_none(self) -> None:
        body = {"response": {"flightroute": {}}}
        session = _mock_session([(200, body)])
        client = RoutesClient(session, max_retries=3)
        assert await client.lookup("XX999") is None

    async def test_callsign_uppercased(self) -> None:
        """Callsign is uppercased before sending to API."""
        body = {
            "response": {
                "flightroute": {
                    "origin": {
                        "iata_code": "A",
                        "icao_code": "B",
                        "municipality": "C",
                        "country_name": "D",
                    },
                    "destination": {
                        "iata_code": "E",
                        "icao_code": "F",
                        "municipality": "G",
                        "country_name": "H",
                    },
                }
            }
        }
        captured_urls: list[str] = []
        session = MagicMock(spec=aiohttp.ClientSession)

        @asynccontextmanager
        async def _get(url: str, **kwargs) -> AsyncIterator[MagicMock]:
            captured_urls.append(url)
            resp = MagicMock()
            resp.status = 200
            resp.json = AsyncMock(return_value=body)
            resp.raise_for_status = MagicMock()
            yield resp

        session.get = _get
        client = RoutesClient(
            session, base_url="https://api.adsbdb.com/v0", max_retries=0
        )
        await client.lookup("dlh123")
        assert captured_urls[0].endswith("/DLH123")


# ---------------------------------------------------------------------------
# PlanespottersClient
# ---------------------------------------------------------------------------


class TestPlanespottersClient:
    async def test_200_returns_photo_info(self) -> None:
        body = {
            "photos": [
                {
                    "thumbnail_large": {"src": "https://example.com/photo.jpg"},
                    "photographer": "John Doe",
                    "aircraft": {"reg": "D-ABCD"},
                }
            ]
        }
        session = _mock_session([(200, body)])
        client = PlanespottersClient(session, max_retries=3)
        result = await client.lookup("3c6547")
        assert result is not None
        assert result.url == "https://example.com/photo.jpg"
        assert "D-ABCD" in result.caption
        assert "John Doe" in result.caption

    async def test_404_returns_none(self) -> None:
        session = _mock_session([(404, None)])
        client = PlanespottersClient(session, max_retries=3)
        assert await client.lookup("000000") is None

    async def test_empty_photos_returns_none(self) -> None:
        session = _mock_session([(200, {"photos": []})])
        client = PlanespottersClient(session, max_retries=3)
        assert await client.lookup("3c6547") is None

    async def test_429_retries_then_succeeds(self) -> None:
        body = {
            "photos": [
                {
                    "thumbnail_large": {"src": "https://example.com/img.jpg"},
                    "photographer": "Jane",
                    "aircraft": {"reg": "OE-LXA"},
                }
            ]
        }
        session = _mock_session([(429, None), (200, body)])
        client = PlanespottersClient(session, max_retries=3)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await client.lookup("440011")
        assert result is not None
        assert result.url == "https://example.com/img.jpg"

    async def test_500_retries_then_raises(self) -> None:
        session = _mock_session([(500, None)] * 4)
        client = PlanespottersClient(session, max_retries=3)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(aiohttp.ClientResponseError):
                await client.lookup("440011")

    async def test_missing_thumbnail_src_returns_none(self) -> None:
        body = {
            "photos": [
                {
                    "thumbnail_large": {},
                    "photographer": "Jane",
                    "aircraft": {"reg": "OE-LXA"},
                }
            ]
        }
        session = _mock_session([(200, body)])
        client = PlanespottersClient(session, max_retries=3)
        assert await client.lookup("440011") is None

    async def test_caption_without_registration(self) -> None:
        body = {
            "photos": [
                {
                    "thumbnail_large": {"src": "https://example.com/img.jpg"},
                    "photographer": "Jane",
                    "aircraft": {},
                }
            ]
        }
        session = _mock_session([(200, body)])
        client = PlanespottersClient(session, max_retries=3)
        result = await client.lookup("440011")
        assert result is not None
        assert result.caption == "Jane"
