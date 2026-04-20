"""Telegram command handlers: /start, /digest, /stats, /debug.

These are registered on the PTB Application by TelegramBot. Each handler
receives the bot's dependencies via closure — no global state.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Coroutine

from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

_WELCOME_TEXT = (
    "✈️ Willkommen beim Flight Tracker Bot!\n\n"
    "Ich beobachte den Luftraum über Stuttgart mit einem ADS-B Empfänger "
    "und schicke tägliche Digests in den Kanal.\n\n"
    "<b>Verfügbare Befehle:</b>\n"
    "/digest — Den letzten Digest anzeigen\n"
    "/stats — Heutige Flugstatistiken\n"
    "/debug — Digest neu generieren (Admin)"
)


def make_handlers(
    on_debug_digest: Callable[[int], Coroutine],
    admin_chat_id: int,
    on_digest_request: Callable[[], Coroutine],
    on_stats_request: Callable[[], Coroutine],
) -> dict[str, Any]:
    """Return a dict of {command: handler_coroutine} for registration on PTB."""

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message:
            await update.message.reply_text(_WELCOME_TEXT, parse_mode="HTML")

    async def digest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message or not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        logger.info("bot: /digest triggered by chat_id=%d", chat_id)
        await update.message.reply_text("Sende letzten Digest…")
        await on_digest_request()

    async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message or not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        logger.info("bot: /stats triggered by chat_id=%d", chat_id)
        await on_stats_request()

    async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_chat is None:
            return
        chat_id = update.effective_chat.id
        if chat_id != admin_chat_id:
            if update.message:
                await update.message.reply_text("Nicht autorisiert.")
            return
        logger.info("bot: /debug triggered by chat_id=%d", chat_id)
        asyncio.create_task(on_debug_digest(chat_id))
        if update.message:
            await update.message.reply_text("Digest wird generiert…")

    return {"start": start, "digest": digest, "stats": stats, "debug": debug}
