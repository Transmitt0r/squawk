from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from squawk.clients.adsbdb import AdsbbClient, AircraftInfo


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
        result = await AdsbbClient(session, max_retries=3).lookup("3c6547")
        assert result == AircraftInfo(
            registration="D-ABCD",
            type="Boeing 737-800",
            operator="Ryanair",
            flag="IE",
        )

    async def test_404_returns_none(self) -> None:
        session = _mock_session([(404, None)])
        assert await AdsbbClient(session, max_retries=3).lookup("000000") is None

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
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await AdsbbClient(session, max_retries=3).lookup("3d1111")
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
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await AdsbbClient(session, max_retries=3).lookup("3d1111")
        assert result is not None

    async def test_exhausted_retries_raises(self) -> None:
        session = _mock_session([(500, None)] * 4)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(aiohttp.ClientResponseError):
                await AdsbbClient(session, max_retries=3).lookup("3d1111")

    async def test_other_4xx_raises_immediately(self) -> None:
        session = _mock_session([(403, None)])
        with pytest.raises(aiohttp.ClientResponseError):
            await AdsbbClient(session, max_retries=3).lookup("3d1111")

    async def test_empty_aircraft_returns_none(self) -> None:
        session = _mock_session([(200, {"response": {"aircraft": {}}})])
        assert await AdsbbClient(session, max_retries=3).lookup("3d1111") is None

    async def test_missing_response_returns_none(self) -> None:
        session = _mock_session([(200, {})])
        assert await AdsbbClient(session, max_retries=3).lookup("3d1111") is None
