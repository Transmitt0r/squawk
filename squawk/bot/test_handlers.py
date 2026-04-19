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
# /debug
# ---------------------------------------------------------------------------


async def test_debug_rejects_non_admin() -> None:
    on_debug = AsyncMock()

    handlers = make_handlers(on_debug, admin_chat_id=99999)
    update = _make_update(chat_id=12345)
    await handlers["debug"](update, _make_context())

    text = update.message.reply_text.call_args[0][0]
    assert "Nicht autorisiert" in text
    on_debug.assert_not_awaited()


async def test_debug_calls_on_debug_for_admin() -> None:
    on_debug = AsyncMock()

    handlers = make_handlers(on_debug, admin_chat_id=99999)
    update = _make_update(chat_id=99999)

    with patch("squawk.bot.handlers.asyncio.create_task") as mock_create:
        await handlers["debug"](update, _make_context())

    mock_create.assert_called_once()
    # The coroutine passed to create_task should be on_debug(99999)
    coro = mock_create.call_args[0][0]
    coro.close()  # clean up unawaited coroutine
    text = update.message.reply_text.call_args[0][0]
    assert "generiert" in text


async def test_debug_ignores_no_chat() -> None:
    on_debug = AsyncMock()
    update = MagicMock()
    update.effective_chat = None

    handlers = make_handlers(on_debug, admin_chat_id=99999)
    await handlers["debug"](update, _make_context())

    on_debug.assert_not_awaited()
