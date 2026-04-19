"""TelegramBot — PTB wiring using the low-level Application API.

Uses Application.initialize() / Application.start() / Updater.start_polling()
instead of run_polling(), which manages its own event loop and is incompatible
with asyncio.TaskGroup.

Teardown under TaskGroup cancellation is guaranteed via try/finally — validated
in scripts/validate_ptb_taskgroup.py (task 1.2).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Coroutine

from telegram.ext import Application, CommandHandler

from squawk.bot.handlers import make_handlers

logger = logging.getLogger(__name__)


class TelegramBot:
    """Wraps PTB Application for use inside asyncio.TaskGroup."""

    def __init__(
        self,
        app: Application,
        on_debug_digest: Callable[[int], Coroutine],
        admin_chat_id: int,
    ) -> None:
        self._app = app
        self._on_debug_digest = on_debug_digest
        self._admin_chat_id = admin_chat_id

    def _register_handlers(self) -> None:
        handlers = make_handlers(self._on_debug_digest, self._admin_chat_id)
        for command, handler in handlers.items():
            self._app.add_handler(CommandHandler(command, handler))

    async def run(self) -> None:
        """Start PTB polling and run until cancelled.

        Guarantees PTB teardown via try/finally even when cancelled by a
        sibling TaskGroup task.
        """
        self._register_handlers()
        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling()
        logger.info("telegram bot started")
        try:
            # Run forever — receives CancelledError on TaskGroup shutdown.
            await asyncio.get_event_loop().create_future()
        finally:
            logger.info("telegram bot shutting down")
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()
