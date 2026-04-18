from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from squawk.clients.planespotters import PlanespottersClient


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
        result = await PlanespottersClient(session, max_retries=3).lookup("3c6547")
        assert result is not None
        assert result.url == "https://example.com/photo.jpg"
        assert "D-ABCD" in result.caption
        assert "John Doe" in result.caption

    async def test_404_returns_none(self) -> None:
        session = _mock_session([(404, None)])
        assert (
            await PlanespottersClient(session, max_retries=3).lookup("000000") is None
        )

    async def test_empty_photos_returns_none(self) -> None:
        session = _mock_session([(200, {"photos": []})])
        assert (
            await PlanespottersClient(session, max_retries=3).lookup("3c6547") is None
        )

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
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await PlanespottersClient(session, max_retries=3).lookup("440011")
        assert result is not None
        assert result.url == "https://example.com/img.jpg"

    async def test_500_retries_then_raises(self) -> None:
        session = _mock_session([(500, None)] * 4)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(aiohttp.ClientResponseError):
                await PlanespottersClient(session, max_retries=3).lookup("440011")

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
        assert (
            await PlanespottersClient(session, max_retries=3).lookup("440011") is None
        )

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
        result = await PlanespottersClient(session, max_retries=3).lookup("440011")
        assert result is not None
        assert result.caption == "Jane"
