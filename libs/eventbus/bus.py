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
import types
import typing
from datetime import datetime, timedelta
from typing import Any

import asyncpg

from eventbus.log import EventLog, LogEntry
from eventbus.protocols import Actor

logger = logging.getLogger(__name__)


def _coerce_payload(cls: type, payload: dict) -> dict:
    """Coerce JSON-deserialized values to match dataclass field types.

    Handles datetime fields stored as ISO strings. Supports both Optional[datetime]
    and datetime | None annotations.
    """
    try:
        hints = typing.get_type_hints(cls)
    except Exception:
        return payload
    result = {}
    for key, val in payload.items():
        hint = hints.get(key)
        if hint is not None and isinstance(val, str):
            inner = _unwrap_optional(hint)
            if inner is datetime:
                val = datetime.fromisoformat(val)
        result[key] = val
    return result


def _unwrap_optional(hint: Any) -> Any:
    """Return the inner type of Optional[X] / X | None, or hint unchanged."""
    # Python 3.10+ union: X | None
    if isinstance(hint, types.UnionType):
        args = [a for a in hint.__args__ if a is not type(None)]
        if len(args) == 1:
            return args[0]
    # typing.Optional[X] = typing.Union[X, None]
    origin = getattr(hint, "__origin__", None)
    if origin is typing.Union:
        args = [a for a in hint.__args__ if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return hint


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

        Actors receive (LogEntry, event) tuples so they can call
        mark_processed() after handling each event.
        """
        event_type = type(event).__name__
        payload = dataclasses.asdict(event)

        # Step 1: persist — must succeed before any inbox delivery
        entry = await self._log.write(event_type, payload)

        # Step 2: deliver to each subscribed actor's inbox
        self._deliver(entry, event)

    def _deliver(self, entry: LogEntry, event: Any) -> None:
        """Put (LogEntry, event) into each subscribed actor's inbox (non-blocking)."""
        event_type = type(event).__name__
        if event_type not in self._subscriptions:
            return
        _cls, actors = self._subscriptions[event_type]
        for actor in actors:
            actor.inbox.put_nowait((entry, event))

    async def mark_processed(self, log_id: int, emitted_at: datetime) -> None:
        """Mark an event as processed in the event log.

        Actors call this after successfully handling an event. Both log_id and
        emitted_at are required to hit the composite primary key on the hypertable.
        """
        await self._log.mark_processed(log_id, emitted_at)

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
                event = cls(**_coerce_payload(cls, entry.payload))
            except Exception:
                logger.exception(
                    "replay: failed to deserialize event id=%d type=%r",
                    entry.id,
                    entry.type,
                )
                continue
            for actor in actors:
                actor.inbox.put_nowait((entry, event))
