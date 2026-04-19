"""Broadcaster protocol and TelegramBroadcaster / DmBroadcaster implementations.

generate_digest() receives a Broadcaster — it does not import Telegram directly.
- TelegramBroadcaster posts to a private channel (weekly digest).
- DmBroadcaster posts to a single chat_id (debug replies to admin).
"""

from __future__ import annotations

import logging
from typing import Protocol

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import Application

from squawk.digest import DigestOutput

logger = logging.getLogger(__name__)

# Telegram caption limit is 1024 chars; message text limit is 4096 chars.
_CAPTION_LIMIT = 1024


class Broadcaster(Protocol):
    async def broadcast(
        self, digest: DigestOutput, chart_png: bytes | None = None
    ) -> None:
        """Send digest to the configured destination."""


async def _send_digest(
    bot: Bot,
    chat_id: int,
    digest: DigestOutput,
    chart_png: bytes | None = None,
) -> None:
    await bot.send_message(
        chat_id=chat_id,
        text=digest.text,
        parse_mode=ParseMode.HTML,
    )
    if digest.photo_url:
        caption = digest.photo_caption or ""
        await bot.send_photo(
            chat_id=chat_id,
            photo=digest.photo_url,
            caption=caption[:_CAPTION_LIMIT] if caption else None,
        )
    if chart_png:
        await bot.send_photo(
            chat_id=chat_id,
            photo=chart_png,
            caption="📈 Flugverkehr der Woche",
        )


class TelegramBroadcaster:
    """Sends a DigestOutput to the private channel.

    Shares the PTB Application built in __main__.py with TelegramBot to avoid
    two Bot instances on the same token.
    """

    def __init__(self, app: Application, channel_id: int) -> None:
        self._bot = app.bot
        self._channel_id = channel_id

    async def broadcast(
        self, digest: DigestOutput, chart_png: bytes | None = None
    ) -> None:
        """Post digest once to the channel."""
        try:
            await _send_digest(self._bot, self._channel_id, digest, chart_png)
        except TelegramError as exc:
            logger.warning("broadcaster: failed to deliver to channel: %s", exc)


class DmBroadcaster:
    """Sends a DigestOutput to a single chat_id via DM.

    Used by the /debug command to reply directly to the admin.
    """

    def __init__(self, app: Application, chat_id: int) -> None:
        self._bot = app.bot
        self._chat_id = chat_id

    async def broadcast(
        self, digest: DigestOutput, chart_png: bytes | None = None
    ) -> None:
        try:
            await _send_digest(self._bot, self._chat_id, digest, chart_png)
        except TelegramError as exc:
            logger.warning(
                "dm_broadcaster: failed to deliver to chat_id=%s: %s",
                self._chat_id,
                exc,
            )
