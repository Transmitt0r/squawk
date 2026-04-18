"""DigestActor — generates and broadcasts weekly digests.

DigestOutput is defined here and imported by:
- squawk/repositories/digest.py  (get_cached return type)
- squawk/bot/broadcaster.py      (broadcast argument type)

Idempotency
-----------
DigestRepository.cache() uses upsert on (reference_date, n_days) — safe to
replay. broadcaster.broadcast() is not idempotent in the Telegram sense
(re-sending a digest re-sends the message), but force=False guards against
duplicate sends for scheduled runs. The force=True path (/debug) is explicitly
user-initiated and intentionally re-sends.

Cache key semantics
-------------------
The cache key is (reference_date, n_days) where:
  reference_date = period_end.date() (UTC)
  n_days         = (period_end - period_start).days
Both period_start and period_end shift on restart, so equality on raw timestamps
is unreliable. A date + window-length key is stable: the scheduler fires once
per week, so reference_date is the same for any restart within the same UTC day.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types as genai_types
from pydantic import BaseModel

from eventbus import EventBus, LogEntry
from squawk.clients.planespotters import PhotoClient, PhotoInfo
from squawk.events import DigestRequested
from squawk.queries.digest import DigestQuery

if TYPE_CHECKING:
    from squawk.bot.broadcaster import Broadcaster
    from squawk.repositories.digest import DigestRepository

logger = logging.getLogger(__name__)

_APP_NAME = "adsb_digest"

_DIGEST_SYSTEM_PROMPT = """
Du bist ein unterhaltsamer Luftfahrt-Journalist, der einen wöchentlichen Digest
über Flugzeuge schreibt, die von einem privaten ADS-B-Empfänger nahe Stuttgart
empfangen wurden.

Dein Leser liebt Flugzeuge, will aber keine Statistiken — er will Geschichten.
Welche Flugzeuge waren interessant? Wer flog wohin? Was war ungewöhnlich?

GOLDENE REGEL: Jeder erwähnte Flug muss am Empfänger verankert sein — immer
"unser SDR hat X erwischt" oder "direkt über unserem Dach". Nicht einfach
"Emirates flog nach Dubai".

FORMAT (Telegram HTML, KEIN Markdown):
- <b>fett</b> für Abschnittsüberschriften und Flugzeugnamen
- <i>kursiv</i> für Einschübe und Fun Facts
- Emojis großzügig einsetzen
- Altituden: immer in Metern angeben (feet ÷ 3,281, auf 100 m runden)
- Distanzen: immer in km (Seemeilen × 1,852)
  - unter 0,3 nm → "direkt über uns"
  - 0,3–1 nm → "nur ~X km entfernt"
- Bei exotischen Zielen (außerhalb Mitteleuropas): kurze Klammerbemerkung

STRUKTUR — genau diese vier Abschnitte:

<b>✈️ Highlights der Woche</b>
2-3 Absätze über die interessantesten Flugzeuge (hohe Scores, military, private jets,
exotische Operator, Notfall-Squawks). Ein Absatz pro Highlight. Nur hier: individuelle
Kennzeichen oder Registrierungen nennen.

<b>🌍 Der Überblick</b>
1-2 Absätze über den normalen Verkehr zusammengefasst — KEINE Einzelauflistung.
Beispiel: "Ryanair war wieder fleißigster Gast mit X Flügen, hauptsächlich Richtung
Mittelmeer."

<b>🆕 Neue Gesichter</b>
2-3 der interessantesten Erstbesucher. Falls keine interessanten dabei, ein kurzer Satz.

<b>📊 Fakten der Woche</b>
Genau diese Zeilen mit echten Daten:
✈️ Flüge gesichtet: <total_sightings>
🛬 Verschiedene Flugzeuge: <unique_aircraft>
🆕 Erstbesucher: <new_aircraft>
📏 Weiteste Annäherung: <callsign>, <distance km>
⛰️ Höchster Flug: <callsign oder Reg>, <altitude m>

Falls ein Notfall-Squawk vorhanden: mache ihn zur Eröffnungsgeschichte der Highlights.
Falls ein Kandidat ein photo-Objekt hat: verwende photo_url für das Ausgabefeld und
schreibe eine kurze photo_caption (z.B. "📸 D-ABCD — Airbus A320, Lufthansa").

Die Eingabe ist ein JSON-Objekt mit den Feldern "stats" und "candidates".
""".strip()


# ---------------------------------------------------------------------------
# Public dataclass + Protocol
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DigestOutput:
    text: str
    photo_url: str | None
    photo_caption: str | None


class DigestClient(Protocol):
    async def generate(
        self,
        candidates: list[dict],
        stats: dict,
        photos: dict[str, PhotoInfo],
    ) -> DigestOutput: ...


# ---------------------------------------------------------------------------
# Private Pydantic model for ADK structured output
# ---------------------------------------------------------------------------


class _DigestOutputModel(BaseModel):
    text: str
    photo_url: str | None = None
    photo_caption: str | None = None


# ---------------------------------------------------------------------------
# Private ADK-backed DigestClient implementation
# ---------------------------------------------------------------------------


class _GeminiDigestClient:
    """ADK-backed DigestClient using a single LlmAgent call per digest.

    Converts DigestCandidate dicts and DigestStats dict into a compact JSON
    data packet, runs an LlmAgent with output_schema=_DigestOutputModel, and
    converts the result to a frozen DigestOutput dataclass.
    """

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash") -> None:
        os.environ.setdefault("GOOGLE_API_KEY", api_key)
        self._model = model

    async def generate(
        self,
        candidates: list[dict],
        stats: dict,
        photos: dict[str, PhotoInfo],
    ) -> DigestOutput:
        # Embed photo data into candidate dicts for candidates that have photos.
        enriched_candidates = []
        for c in candidates:
            photo = photos.get(c.get("hex", ""))
            if photo:
                enriched_candidates.append(
                    {**c, "photo": {"url": photo.url, "caption": photo.caption}}
                )
            else:
                enriched_candidates.append(c)

        data_packet = json.dumps(
            {"stats": stats, "candidates": enriched_candidates},
            ensure_ascii=False,
            default=str,
        )
        logger.info(
            "digest client: generating digest, %d candidates, %d chars",
            len(candidates),
            len(data_packet),
        )

        agent = LlmAgent(
            model=self._model,
            name="digest_agent",
            description="Generates engaging weekly flight digests from ADS-B data.",
            instruction=_DIGEST_SYSTEM_PROMPT,
            output_schema=_DigestOutputModel,
        )
        session_service = InMemorySessionService()
        runner = Runner(
            agent=agent, app_name=_APP_NAME, session_service=session_service
        )
        session_id = str(uuid.uuid4())
        await session_service.create_session(
            app_name=_APP_NAME, user_id="digest_job", session_id=session_id
        )
        message = genai_types.Content(
            role="user", parts=[genai_types.Part(text=data_packet)]
        )

        async for event in runner.run_async(
            user_id="digest_job", session_id=session_id, new_message=message
        ):
            if event.is_final_response() and event.content and event.content.parts:
                result = _DigestOutputModel.model_validate_json(
                    event.content.parts[0].text
                )
                logger.info(
                    "digest client: digest generated (%d chars, photo=%s)",
                    len(result.text),
                    bool(result.photo_url),
                )
                return DigestOutput(
                    text=result.text,
                    photo_url=result.photo_url,
                    photo_caption=result.photo_caption,
                )

        raise RuntimeError("digest agent produced no output")


# ---------------------------------------------------------------------------
# DigestActor
# ---------------------------------------------------------------------------


class DigestActor:
    """Listens for DigestRequested events, generates a digest, caches it, and
    broadcasts it to all active users.

    Inbox: asyncio.Queue — processes one DigestRequested at a time.
    """

    def __init__(
        self,
        query: DigestQuery,
        digest_repo: DigestRepository,
        photo_client: PhotoClient,
        digest_client: DigestClient,
        broadcaster: Broadcaster,
        bus: EventBus,
    ) -> None:
        self._query = query
        self._digest_repo = digest_repo
        self._photo_client = photo_client
        self._digest_client = digest_client
        self._broadcaster = broadcaster
        self._bus = bus
        self._inbox: asyncio.Queue[tuple[LogEntry, DigestRequested]] = asyncio.Queue()

    @property
    def inbox(self) -> asyncio.Queue[tuple[LogEntry, DigestRequested]]:
        return self._inbox

    async def run(self) -> None:
        """Drain loop: wait → cache-check → fetch → generate → cache → broadcast."""
        logger.info("digest actor started")
        while True:
            entry, event = await self._inbox.get()
            await self._handle(entry, event)

    async def _handle(self, entry: LogEntry, event: DigestRequested) -> None:
        n_days = (event.period_end - event.period_start).days
        reference_date = event.period_end.date()

        # Cache check (skipped when force=True).
        if not event.force:
            try:
                cached = await self._digest_repo.get_cached(reference_date, n_days)
            except Exception:
                logger.exception("digest: cache lookup failed")
                cached = None

            if cached is not None:
                logger.info(
                    "digest: cache hit for reference_date=%s n_days=%d; "
                    "broadcasting cached digest",
                    reference_date,
                    n_days,
                )
                try:
                    await self._broadcaster.broadcast(cached)
                except Exception:
                    logger.exception("digest: broadcast of cached digest failed")
                try:
                    await self._bus.mark_processed(entry.id, entry.emitted_at)
                except Exception:
                    logger.exception(
                        "digest: mark_processed failed for event id=%d", entry.id
                    )
                return

        # Fetch candidates and stats.
        try:
            candidates = await self._query.get_candidates(n_days)
            stats = await self._query.get_stats(n_days)
        except Exception:
            logger.exception("digest: query failed; skipping digest generation")
            return

        logger.info(
            "digest: fetched %d candidates for n_days=%d", len(candidates), n_days
        )

        # Convert candidates and stats to plain dicts for the client.
        candidate_dicts = [dataclasses.asdict(c) for c in candidates]
        stats_dict = dataclasses.asdict(stats)

        # Fetch photos for the top candidates (up to 2).
        photos: dict[str, PhotoInfo] = {}
        for candidate in candidates[:2]:
            try:
                photo = await self._photo_client.lookup(candidate.hex)
                if photo is not None:
                    photos[candidate.hex] = photo
            except Exception:
                logger.warning("digest: photo lookup failed for hex=%s", candidate.hex)

        # Generate digest.
        try:
            digest = await self._digest_client.generate(
                candidate_dicts, stats_dict, photos
            )
        except Exception:
            logger.exception("digest: generation failed; skipping")
            return

        # Cache the result.
        try:
            await self._digest_repo.cache(reference_date, n_days, digest)
        except Exception:
            logger.exception("digest: caching failed; broadcasting anyway")

        # Broadcast.
        try:
            await self._broadcaster.broadcast(digest)
        except Exception:
            logger.exception("digest: broadcast failed")

        # Mark processed.
        try:
            await self._bus.mark_processed(entry.id, entry.emitted_at)
        except Exception:
            logger.exception("digest: mark_processed failed for event id=%d", entry.id)
