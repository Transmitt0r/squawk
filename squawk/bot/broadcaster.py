"""Broadcaster protocol and TelegramBroadcaster implementation.

DigestActor receives a Broadcaster — it does not import Telegram directly.
TelegramBroadcaster is the only concrete implementation.
"""

from __future__ import annotations

import logging
from typing import Protocol

from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import Application

from squawk.actors.digest import DigestOutput
from squawk.repositories.users import UserRepository

logger = logging.getLogger(__name__)

# Telegram caption limit is 1024 chars; message text limit is 4096 chars.
_CAPTION_LIMIT = 1024


class Broadcaster(Protocol):
    async def broadcast(self, digest: DigestOutput) -> None:
        """Send digest to all active users."""


class TelegramBroadcaster:
    """Sends a DigestOutput to all active Telegram users.

    Shares the PTB Application built in __main__.py with TelegramBot to avoid
    two Bot instances on the same token.
    """

    def __init__(self, app: Application, users: UserRepository) -> None:
        self._bot = app.bot
        self._users = users

    async def broadcast(self, digest: DigestOutput) -> None:
        """Send digest to all active users.

        If photo_url is set, sends a photo with caption. If the digest text
        exceeds Telegram's caption limit the photo is sent with the truncated
        caption and the full text follows as a separate message.

        Failures for individual users are logged and skipped so a single bad
        chat_id does not abort delivery to the remaining recipients.
        """
        chat_ids = await self._users.get_active()
        if not chat_ids:
            logger.info("broadcaster: no active users, skipping")
            return

        logger.info("broadcaster: sending digest to %d user(s)", len(chat_ids))

        for chat_id in chat_ids:
            try:
                await self._send_to(chat_id, digest)
            except TelegramError as exc:
                logger.warning(
                    "broadcaster: failed to deliver to chat_id=%s: %s", chat_id, exc
                )

    async def _send_to(self, chat_id: int, digest: DigestOutput) -> None:
        if digest.photo_url:
            caption = digest.photo_caption or ""
            # If the full text fits in a caption, send everything in one message.
            if len(digest.text) <= _CAPTION_LIMIT:
                await self._bot.send_photo(
                    chat_id=chat_id,
                    photo=digest.photo_url,
                    caption=digest.text,
                    parse_mode=ParseMode.HTML,
                )
            else:
                # Photo with its own caption, then the full digest as text.
                await self._bot.send_photo(
                    chat_id=chat_id,
                    photo=digest.photo_url,
                    caption=caption[:_CAPTION_LIMIT] if caption else None,
                )
                await self._bot.send_message(
                    chat_id=chat_id,
                    text=digest.text,
                    parse_mode=ParseMode.HTML,
                )
        else:
            await self._bot.send_message(
                chat_id=chat_id,
                text=digest.text,
                parse_mode=ParseMode.HTML,
            )
