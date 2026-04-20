"""Bot handler tests — pure unit tests with mocked dependencies.

Run with:
    uv run pytest squawk/bot/test_handlers.py -v
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from squawk.bot.handlers import make_handlers


def _make_update(chat_id: int = 12345) -> MagicMock:
    update = MagicMock()
    update.effective_chat = MagicMock()
    update.effective_chat.id = chat_id
    update.message = MagicMock()
    update.message.reply_text = AsyncMock()
    return update


def _make_context() -> MagicMock:
    return MagicMock()


# ---------------------------------------------------------------------------
# /start
# ---------------------------------------------------------------------------


async def test_start_sends_welcome() -> None:
    handlers = make_handlers(
        on_debug_digest=AsyncMock(),
        admin_chat_id=99999,
        on_digest_request=AsyncMock(),
        on_stats_request=AsyncMock(),
    )
    update = _make_update()
    await handlers["start"](update, _make_context())

    update.message.reply_text.assert_called_once()
    text = update.message.reply_text.call_args[0][0]
    assert "Flight Tracker" in text
    assert "/digest" in text
    assert "/stats" in text


# ---------------------------------------------------------------------------
# /debug
# ---------------------------------------------------------------------------


async def test_debug_rejects_non_admin() -> None:
    on_debug = AsyncMock()

    handlers = make_handlers(
        on_debug_digest=on_debug,
        admin_chat_id=99999,
        on_digest_request=AsyncMock(),
        on_stats_request=AsyncMock(),
    )
    update = _make_update(chat_id=12345)
    await handlers["debug"](update, _make_context())

    text = update.message.reply_text.call_args[0][0]
    assert "Nicht autorisiert" in text
    on_debug.assert_not_awaited()


async def test_debug_calls_on_debug_for_admin() -> None:
    on_debug = AsyncMock()

    handlers = make_handlers(
        on_debug_digest=on_debug,
        admin_chat_id=99999,
        on_digest_request=AsyncMock(),
        on_stats_request=AsyncMock(),
    )
    update = _make_update(chat_id=99999)

    with patch("squawk.bot.handlers.asyncio.create_task") as mock_create:
        await handlers["debug"](update, _make_context())

    mock_create.assert_called_once()
    coro = mock_create.call_args[0][0]
    coro.close()
    text = update.message.reply_text.call_args[0][0]
    assert "generiert" in text


async def test_debug_ignores_no_chat() -> None:
    on_debug = AsyncMock()
    update = MagicMock()
    update.effective_chat = None

    handlers = make_handlers(
        on_debug_digest=on_debug,
        admin_chat_id=99999,
        on_digest_request=AsyncMock(),
        on_stats_request=AsyncMock(),
    )
    await handlers["debug"](update, _make_context())

    on_debug.assert_not_awaited()


# ---------------------------------------------------------------------------
# /digest
# ---------------------------------------------------------------------------


async def test_digest_calls_on_digest_request() -> None:
    on_digest = AsyncMock()

    handlers = make_handlers(
        on_debug_digest=AsyncMock(),
        admin_chat_id=99999,
        on_digest_request=on_digest,
        on_stats_request=AsyncMock(),
    )
    update = _make_update()
    await handlers["digest"](update, _make_context())

    update.message.reply_text.assert_called_once()
    on_digest.assert_awaited_once()


async def test_digest_ignores_no_chat() -> None:
    on_digest = AsyncMock()
    update = MagicMock()
    update.effective_chat = None
    update.message = None

    handlers = make_handlers(
        on_debug_digest=AsyncMock(),
        admin_chat_id=99999,
        on_digest_request=on_digest,
        on_stats_request=AsyncMock(),
    )
    await handlers["digest"](update, _make_context())

    on_digest.assert_not_awaited()


# ---------------------------------------------------------------------------
# /stats
# ---------------------------------------------------------------------------


async def test_stats_calls_on_stats_request() -> None:
    on_stats = AsyncMock()

    handlers = make_handlers(
        on_debug_digest=AsyncMock(),
        admin_chat_id=99999,
        on_digest_request=AsyncMock(),
        on_stats_request=on_stats,
    )
    update = _make_update()
    await handlers["stats"](update, _make_context())

    on_stats.assert_awaited_once()


async def test_stats_ignores_no_chat() -> None:
    on_stats = AsyncMock()
    update = MagicMock()
    update.effective_chat = None
    update.message = None

    handlers = make_handlers(
        on_debug_digest=AsyncMock(),
        admin_chat_id=99999,
        on_digest_request=AsyncMock(),
        on_stats_request=on_stats,
    )
    await handlers["stats"](update, _make_context())

    on_stats.assert_not_awaited()
