"""Telegram bot command handlers."""

from __future__ import annotations

import logging

from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes

from .agent import DigestOutput, Runner, generate_digest
from .config import Config
from .db import get_active_users, register_user, unregister_user

logger = logging.getLogger(__name__)


def build_app(config: Config, runner: Runner, scheduler) -> Application:
    async def _post_init(app: Application) -> None:
        scheduler.start()
        logger.info("Scheduler started")

    async def _post_stop(app: Application) -> None:
        if scheduler.running:
            scheduler.shutdown(wait=False)

    app = (
        Application.builder()
        .token(config.bot_token)
        .post_init(_post_init)
        .post_stop(_post_stop)
        .build()
    )
    app.add_handler(CommandHandler("start", _start(config)))
    app.add_handler(CommandHandler("stop", _stop(config)))
    app.add_handler(CommandHandler("debug", _debug(config, runner)))
    return app


def _start(config: Config):
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user
        chat_id = update.effective_chat.id
        username = user.username if user else None

        is_new = register_user(config.database_url, chat_id, username)
        logger.info("User registered: chat_id=%d username=%s new=%s", chat_id, username, is_new)

        if is_new:
            await update.message.reply_text(
                "✈️ Willkommen! Du bekommst ab jetzt jeden Sonntag einen Digest "
                "der interessantesten Flüge über Stuttgart. Bis bald!"
            )
        else:
            await update.message.reply_text(
                "✈️ Du bist bereits angemeldet! Jeden Sonntag kommt dein Digest."
            )

    return handler


def _stop(config: Config):
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        was_active = unregister_user(config.database_url, chat_id)
        logger.info("User unregistered: chat_id=%d", chat_id)

        if was_active:
            await update.message.reply_text(
                "👋 Du wurdest abgemeldet. Schick /start wenn du wieder dabei sein möchtest."
            )
        else:
            await update.message.reply_text("Du warst gar nicht angemeldet.")

    return handler


def _debug(config: Config, runner: Runner):
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id

        if config.admin_chat_id is None or chat_id != config.admin_chat_id:
            await update.message.reply_text("🚫 Kein Zugriff.")
            return

        await update.message.reply_text("⏳ Generiere Digest… (kann eine Minute dauern)")
        try:
            digest = await generate_digest(runner, days=1)
            bot = update.get_bot()
            await _send_digest(bot, update.effective_chat.id, digest)
        except Exception as exc:
            logger.exception("Debug digest failed")
            await update.message.reply_text(f"❌ Fehler: {exc}")

    return handler


async def _send_digest(bot: Bot, chat_id: int, digest: DigestOutput) -> None:
    """Send digest to a single chat, with photo if the agent found one."""
    if digest.photo_url:
        try:
            await bot.send_photo(
                chat_id=chat_id,
                photo=digest.photo_url,
                caption=digest.text[:1024],
            )
            if len(digest.text) > 1024:
                await bot.send_message(chat_id=chat_id, text=digest.text[1024:])
            return
        except Exception:
            logger.warning("Failed to send photo to chat_id=%d, falling back to text", chat_id)
    await bot.send_message(chat_id=chat_id, text=digest.text)


async def broadcast(config: Config, digest: DigestOutput) -> None:
    """Send digest to all registered users."""
    chat_ids = get_active_users(config.database_url)
    logger.info("Broadcasting digest to %d users", len(chat_ids))
    bot = Bot(token=config.bot_token)
    async with bot:
        for chat_id in chat_ids:
            try:
                await _send_digest(bot, chat_id, digest)
            except Exception:
                logger.exception("Failed to send to chat_id=%d", chat_id)
