"""Bot handler tests — pure unit tests with mocked dependencies.

Run with:
    uv run pytest squawk/bot/test_handlers.py -v
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from squawk.bot.handlers import make_handlers


def _make_update(
    chat_id: int = 12345,
    username: str | None = "testuser",
) -> MagicMock:
    update = MagicMock()
    update.effective_chat = MagicMock()
    update.effective_chat.id = chat_id
    update.effective_user = MagicMock()
    update.effective_user.username = username
    update.message = MagicMock()
    update.message.reply_text = AsyncMock()
    return update


def _make_context() -> MagicMock:
    return MagicMock()


# ---------------------------------------------------------------------------
# /start
# ---------------------------------------------------------------------------


async def test_start_registers_new_user() -> None:
    users = AsyncMock()
    users.register.return_value = True

    handlers = make_handlers(users, AsyncMock(), admin_chat_id=99999)
    update = _make_update()
    await handlers["start"](update, _make_context())

    users.register.assert_awaited_once_with(12345, "testuser")
    text = update.message.reply_text.call_args[0][0]
    assert "benachrichtigt" in text


async def test_start_already_registered() -> None:
    users = AsyncMock()
    users.register.return_value = False

    handlers = make_handlers(users, AsyncMock(), admin_chat_id=99999)
    update = _make_update()
    await handlers["start"](update, _make_context())

    users.register.assert_awaited_once_with(12345, "testuser")
    text = update.message.reply_text.call_args[0][0]
    assert "bereits registriert" in text


async def test_start_ignores_no_chat() -> None:
    users = AsyncMock()
    update = MagicMock()
    update.effective_chat = None

    handlers = make_handlers(users, AsyncMock(), admin_chat_id=99999)
    await handlers["start"](update, _make_context())

    users.register.assert_not_awaited()


# ---------------------------------------------------------------------------
# /stop
# ---------------------------------------------------------------------------


async def test_stop_unregisters_active_user() -> None:
    users = AsyncMock()
    users.unregister.return_value = True

    handlers = make_handlers(users, AsyncMock(), admin_chat_id=99999)
    update = _make_update()
    await handlers["stop"](update, _make_context())

    users.unregister.assert_awaited_once_with(12345)
    text = update.message.reply_text.call_args[0][0]
    assert "nicht mehr" in text


async def test_stop_not_registered() -> None:
    users = AsyncMock()
    users.unregister.return_value = False

    handlers = make_handlers(users, AsyncMock(), admin_chat_id=99999)
    update = _make_update()
    await handlers["stop"](update, _make_context())

    text = update.message.reply_text.call_args[0][0]
    assert "nicht registriert" in text


async def test_stop_ignores_no_chat() -> None:
    users = AsyncMock()
    update = MagicMock()
    update.effective_chat = None

    handlers = make_handlers(users, AsyncMock(), admin_chat_id=99999)
    await handlers["stop"](update, _make_context())

    users.unregister.assert_not_awaited()


# ---------------------------------------------------------------------------
# /debug
# ---------------------------------------------------------------------------


async def test_debug_rejects_non_admin() -> None:
    users = AsyncMock()
    on_debug = AsyncMock()

    handlers = make_handlers(users, on_debug, admin_chat_id=99999)
    update = _make_update(chat_id=12345)
    await handlers["debug"](update, _make_context())

    text = update.message.reply_text.call_args[0][0]
    assert "Nicht autorisiert" in text
    on_debug.assert_not_awaited()


async def test_debug_calls_on_debug_for_admin() -> None:
    users = AsyncMock()
    on_debug = AsyncMock()

    handlers = make_handlers(users, on_debug, admin_chat_id=99999)
    update = _make_update(chat_id=99999)

    with patch("squawk.bot.handlers.asyncio.create_task") as mock_create:
        await handlers["debug"](update, _make_context())

    mock_create.assert_called_once()
    text = update.message.reply_text.call_args[0][0]
    assert "generiert" in text


async def test_debug_ignores_no_chat() -> None:
    on_debug = AsyncMock()
    update = MagicMock()
    update.effective_chat = None

    handlers = make_handlers(AsyncMock(), on_debug, admin_chat_id=99999)
    await handlers["debug"](update, _make_context())

    on_debug.assert_not_awaited()
