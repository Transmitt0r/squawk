"""Typed async event bus backed by TimescaleDB.

Events are persisted to event_log before delivery to actor inboxes. This
guarantees that a crash between emit() and actor processing can be recovered
via replay_unprocessed() on the next startup.

Serialization contract: domain events must be frozen dataclasses. EventBus
uses dataclasses.asdict() to serialize and the registered type's constructor
(**payload) to deserialize. Field names in the payload must match the
dataclass field names exactly.
"""

from __future__ import annotations

import dataclasses
import logging
from datetime import timedelta
from typing import Any

import asyncpg

from eventbus.log import EventLog, LogEntry
from eventbus.protocols import Actor

logger = logging.getLogger(__name__)


class EventBus:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._log = EventLog(pool)
        # Maps event type name → (event class, list of subscribed actors)
        self._subscriptions: dict[str, tuple[type, list[Actor]]] = {}

    def subscribe(self, event_type: type, actor: Actor) -> None:
        """Register an actor to receive events of the given type."""
        name = event_type.__name__
        if name not in self._subscriptions:
            self._subscriptions[name] = (event_type, [])
        self._subscriptions[name][1].append(actor)

    async def emit(self, event: Any) -> None:
        """Persist event to event_log, then deliver to subscribed actor inboxes.

        Step 1 (DB write) must complete before step 2 (inbox delivery). If the
        DB write raises, inbox delivery is skipped — an event that never reached
        event_log cannot be replayed on crash.
        """
        event_type = type(event).__name__
        payload = dataclasses.asdict(event)

        # Step 1: persist — must succeed before any inbox delivery
        entry = await self._log.write(event_type, payload)

        # Step 2: deliver to each subscribed actor's inbox
        self._deliver(entry, event)

    def _deliver(self, entry: LogEntry, event: Any) -> None:
        """Put event into each subscribed actor's inbox (non-blocking)."""
        event_type = type(event).__name__
        if event_type not in self._subscriptions:
            return
        _cls, actors = self._subscriptions[event_type]
        for actor in actors:
            actor.inbox.put_nowait(event)

    async def replay_unprocessed(self, since: timedelta = timedelta(hours=24)) -> None:
        """On startup: fetch unprocessed events and deliver to actor inboxes.

        Only events from the last `since` window are replayed — prevents
        replaying stale events after a long outage.
        """
        entries = await self._log.fetch_unprocessed(since)
        for entry in entries:
            if entry.type not in self._subscriptions:
                logger.warning("replay: no subscription for event type %r", entry.type)
                continue
            cls, actors = self._subscriptions[entry.type]
            try:
                event = cls(**entry.payload)
            except Exception:
                logger.exception(
                    "replay: failed to deserialize event id=%d type=%r",
                    entry.id,
                    entry.type,
                )
                continue
            for actor in actors:
                actor.inbox.put_nowait(event)
