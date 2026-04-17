"""Scheduler protocol and APScheduler-backed implementation.

Only this module imports APScheduler. All other modules depend on the
Scheduler protocol, not the concrete class.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any, Protocol

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger


class Scheduler(Protocol):
    def add_cron_job(
        self,
        func: Callable[[], Coroutine[Any, Any, None]],
        expr: str,
        tz: str = "UTC",
    ) -> None: ...

    def start(self) -> None: ...

    def shutdown(self) -> None: ...


class APSchedulerBackend:
    """APScheduler 3.x AsyncIOScheduler wrapped behind the Scheduler protocol."""

    def __init__(self) -> None:
        self._scheduler = AsyncIOScheduler()

    def add_cron_job(
        self,
        func: Callable[[], Coroutine[Any, Any, None]],
        expr: str,
        tz: str = "UTC",
    ) -> None:
        trigger = CronTrigger.from_crontab(expr, timezone=tz)
        self._scheduler.add_job(func, trigger)

    def start(self) -> None:
        self._scheduler.start()

    def shutdown(self) -> None:
        self._scheduler.shutdown(wait=False)
