"""Weekly digest scheduler."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .agent import Runner, generate_digest
from .bot import broadcast
from .config import Config
from .db import cache_digest, get_cached_digest

logger = logging.getLogger(__name__)


def _week_bounds() -> tuple[datetime, datetime]:
    """Return (start, end) for the past 7 days as UTC datetimes."""
    now = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    return now - timedelta(days=7), now


async def run_weekly_digest(config: Config, runner: Runner) -> None:
    logger.info("Weekly digest job started")
    period_start, period_end = _week_bounds()

    # Use cached digest if already generated for this period
    digest = get_cached_digest(config.database_url, period_start, period_end)
    if digest:
        logger.info("Using cached digest")
    else:
        logger.info("Generating new digest")
        digest = await generate_digest(runner, days=7)
        cache_digest(config.database_url, period_start, period_end, digest)

    await broadcast(config, digest)
    logger.info("Weekly digest sent")


def create_scheduler(config: Config, runner: Runner) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()

    # Parse DIGEST_SCHEDULE as a cron expression (e.g. "0 8 * * 0" = Sunday 8am)
    parts = config.digest_schedule.split()
    trigger = CronTrigger(
        minute=parts[0],
        hour=parts[1],
        day=parts[2],
        month=parts[3],
        day_of_week=parts[4],
        timezone="Europe/Berlin",
    )

    scheduler.add_job(
        run_weekly_digest,
        trigger=trigger,
        kwargs={"config": config, "runner": runner},
        name="weekly_digest",
    )

    logger.info("Digest scheduled: %s", config.digest_schedule)
    return scheduler
