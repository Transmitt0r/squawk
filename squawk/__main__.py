"""squawk — entry point. Wiring only; no business logic."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import aiohttp
from telegram.ext import Application

from squawk.bot.app import TelegramBot
from squawk.bot.broadcaster import DmBroadcaster, TelegramBroadcaster
from squawk.clients.adsbdb import AdsbbClient
from squawk.clients.planespotters import PlanespottersClient
from squawk.clients.routes import RoutesClient
from squawk.config import Config
from squawk.db import create_pool
from squawk.digest import _GeminiDigestClient, generate_digest
from squawk.enrichment import _GeminiScoringClient
from squawk.pipeline import run_pipeline
from squawk.queries.charts import ChartQuery
from squawk.queries.digest import DigestQuery
from squawk.repositories.digest import DigestRepository
from squawk.repositories.enrichment import EnrichmentRepository
from squawk.repositories.sightings import SightingRepository
from squawk.scheduler import APSchedulerBackend

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
# Silence noisy third-party loggers — only keep warnings and errors.
for _noisy in ("httpx", "google_adk", "google_genai", "apscheduler", "telegram"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


async def main() -> None:
    config = Config.from_env()
    pool = await create_pool(config.database_url)

    # Repositories
    sightings_repo = SightingRepository(pool)
    enrichment_repo = EnrichmentRepository(pool)
    digest_repo = DigestRepository(pool)

    # Read queries
    digest_query = DigestQuery(pool)
    chart_query = ChartQuery(pool)

    async with aiohttp.ClientSession() as http:
        # HTTP clients
        aircraft_client = AdsbbClient(http, config.adsbdb_url)
        route_client = RoutesClient(http, config.routes_url)
        photo_client = PlanespottersClient(http, config.planespotters_url)

        # AI clients
        scoring_client = _GeminiScoringClient()
        digest_client = _GeminiDigestClient()

        # Telegram
        ptb_app = Application.builder().token(config.bot_token).build()
        channel_broadcaster = TelegramBroadcaster(ptb_app, config.channel_id)

        # Digest helper — used by scheduler and /debug
        async def _do_digest(
            period_start: datetime,
            period_end: datetime,
            broadcaster: TelegramBroadcaster | DmBroadcaster,
            force: bool = False,
        ) -> None:
            await generate_digest(
                query=digest_query,
                chart_query=chart_query,
                digest_repo=digest_repo,
                photo_client=photo_client,
                digest_client=digest_client,
                broadcaster=broadcaster,
                period_start=period_start,
                period_end=period_end,
                force=force,
            )

        async def _debug_digest(chat_id: int) -> None:
            now = datetime.now(tz=timezone.utc)
            await _do_digest(
                now - timedelta(hours=24),
                now,
                broadcaster=DmBroadcaster(ptb_app, chat_id),
                force=True,
            )

        async def _scheduled_digest() -> None:
            now = datetime.now(tz=timezone.utc)
            await _do_digest(
                now - timedelta(days=7), now, broadcaster=channel_broadcaster
            )

        # Scheduler
        scheduler = APSchedulerBackend()
        scheduler.add_cron_job(
            _scheduled_digest, config.digest_schedule, tz="Europe/Berlin"
        )
        scheduler.start()

        # Bot
        bot = TelegramBot(
            ptb_app,
            on_debug_digest=_debug_digest,
            admin_chat_id=config.admin_chat_id,
        )

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    run_pipeline(
                        poll_url=config.adsb_url,
                        poll_interval=config.poll_interval,
                        session_timeout=config.session_timeout,
                        sightings=sightings_repo,
                        enrichment_repo=enrichment_repo,
                        aircraft_client=aircraft_client,
                        route_client=route_client,
                        scoring_client=scoring_client,
                        enrichment_ttl=config.enrichment_ttl,
                        batch_size=config.enrichment_batch_size,
                        flush_interval=config.enrichment_flush_interval,
                    )
                )
                tg.create_task(bot.run())
        finally:
            scheduler.shutdown()


def run() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    run()
