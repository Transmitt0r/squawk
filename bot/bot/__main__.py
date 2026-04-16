"""Entry point: starts Telegram bot + weekly scheduler."""

from __future__ import annotations

import asyncio
import logging
import os

from .agent import create_runner
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

    # Set API key for LiteLLM
    os.environ["ANTHROPIC_API_KEY"] = config.anthropic_api_key

    logger.info("Initializing bot database")
    init_schema(config.database_url)

    logger.info("Creating ADK runner")
    runner = create_runner(config)

    scheduler = create_scheduler(config, runner)

    logger.info("Building Telegram app")
    app = build_app(config, runner, scheduler)

    if config.admin_chat_id is None:
        logger.warning(
            "ADMIN_CHAT_ID not set — /debug is disabled. "
            "Send /start to the bot and check logs for your chat_id."
        )

    logger.info("Starting bot (polling)")
    app.run_polling()


if __name__ == "__main__":
    main()
