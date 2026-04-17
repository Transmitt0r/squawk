"""Entry point: starts Telegram bot + weekly scheduler."""

from __future__ import annotations

import logging
import os

from .bot import build_app
from .config import Config
from .db import init_schema
from .scheduler import create_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger("digest")


def main() -> None:
    config = Config.from_env()

    os.environ["GEMINI_API_KEY"] = config.gemini_api_key

    logger.info("Initializing bot database")
    init_schema(config.database_url)

    scheduler = create_scheduler(config)

    logger.info("Building Telegram app")
    app = build_app(config, scheduler)

    if config.admin_chat_id is None:
        logger.warning(
            "ADMIN_CHAT_ID not set — /debug is disabled. "
            "Send /start to the bot and check logs for your chat_id."
        )

    logger.info("Starting bot (polling)")
    app.run_polling()


if __name__ == "__main__":
    main()
