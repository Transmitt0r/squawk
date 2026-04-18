from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from squawk.clients.routes import RouteInfo, RoutesClient


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
        result = await RoutesClient(session, max_retries=3).lookup("FR1234")
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
        assert await RoutesClient(session, max_retries=3).lookup("UNKNOWN") is None

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
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await RoutesClient(session, max_retries=3).lookup("BA456")
        assert result is not None
        assert result.dest_iata == "LHR"

    async def test_exhausted_retries_raises(self) -> None:
        session = _mock_session([(429, None)] * 4)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(aiohttp.ClientResponseError):
                await RoutesClient(session, max_retries=3).lookup("BA456")

    async def test_empty_flightroute_returns_none(self) -> None:
        body = {"response": {"flightroute": {}}}
        session = _mock_session([(200, body)])
        assert await RoutesClient(session, max_retries=3).lookup("XX999") is None

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
