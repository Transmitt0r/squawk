"""EnrichmentActor — enriches aircraft with external data and AI scores them.

Idempotency
-----------
EnrichmentRepository.store() uses upserts on both enriched_aircraft (ON CONFLICT hex)
and callsign_routes (ON CONFLICT callsign). Replaying the same HexFirstSeen or
EnrichmentExpired event produces the same final DB state. The only observable
difference is that expires_at is recomputed from now() at store time — for a crash
replay within seconds this is negligible.

Duplicate Gemini calls
----------------------
Two scenarios can cause the same hex to appear in the inbox twice:
a) Post-crash replay: a batch was processed but processed_at was not marked before
   the crash — score_batch() runs again on replay.
b) Normal startup: replay_unprocessed() fills the inbox with last-24h events, then
   polling_actor.run() immediately emits new HexFirstSeen events for aircraft still
   visible.
In both cases the DB write (upsert) is idempotent. score_batch() deduplicates by
hex before calling the API. The 24h replay window bounds the maximum re-spend to
one batch of up to ENRICHMENT_BATCH_SIZE aircraft.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types as genai_types
from pydantic import BaseModel

from eventbus import EventBus, LogEntry
from squawk.clients.adsbdb import AircraftInfo, AircraftLookupClient
from squawk.clients.routes import RouteClient, RouteInfo
from squawk.events import EnrichmentExpired, HexFirstSeen
from squawk.repositories.enrichment import EnrichmentRepository

logger = logging.getLogger(__name__)

_APP_NAME = "adsb_enrichment"

_SCORING_SYSTEM_PROMPT = """
Du bewertest Flugzeuge für einen wöchentlichen ADS-B-Digest nahe Stuttgart.
Für jedes Flugzeug in der Liste gibst du einen Score (1–10) zurück:

Score-Richtlinien:
- 1–3: Alltäglicher Linienverkehr (Ryanair, Eurowings, kurze Inlandsrouten)
- 4–6: Interessant aber normal (Langstrecke, Frachter, unbekannte Operator)
- 7–8: Ungewöhnlich (Militär, Privatjet, exotisches Ziel, seltener Typ)
- 9–10: Sehr selten oder außergewöhnlich (historisches Flugzeug, Notfall-Squawk,
        medizinische Evakuierung, VIP-Transport)

WICHTIG: Squawk 7500 (Entführung), 7600 (Funkausfall), 7700 (Notfall) → Score ≥ 9.

tags: kurze englische Schlagwörter (z.B. "military", "cargo", "bizjet",
      "emergency", "long-haul", "low-altitude", "unusual-operator")

annotation: ein einziger Satz auf Deutsch, der erklärt, warum das Flugzeug
interessant ist. Leer lassen (""), wenn das Flugzeug unremarkable ist (Score ≤ 3).

Die Ausgabe ist ein JSON-Objekt mit einem Feld "results", das ein Array mit genau
so vielen Einträgen enthält wie die Eingabeliste — in der gleichen Reihenfolge.
""".strip()


# ---------------------------------------------------------------------------
# Public dataclass + Protocol
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScoreResult:
    score: int  # 1–10
    tags: list[str]
    annotation: str  # one German sentence; empty string if unremarkable


class ScoringClient(Protocol):
    async def score_batch(
        self,
        aircraft: list[tuple[str, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]: ...


# ---------------------------------------------------------------------------
# Private Pydantic models for ADK structured output
# ---------------------------------------------------------------------------


class _ScoreResultModel(BaseModel):
    score: int
    tags: list[str]
    annotation: str


class _ScoreBatchModel(BaseModel):
    results: list[_ScoreResultModel]


# ---------------------------------------------------------------------------
# Private ADK-backed ScoringClient implementation
# ---------------------------------------------------------------------------

_FALLBACK_SCORE = ScoreResult(score=1, tags=[], annotation="")


def _aircraft_to_dict(
    hex_: str,
    info: AircraftInfo | None,
    route: RouteInfo | None,
) -> dict:
    return {
        "hex": hex_,
        "registration": info.registration if info else None,
        "type": info.type if info else None,
        "operator": info.operator if info else None,
        "flag": info.flag if info else None,
        "origin_city": route.origin_city if route else None,
        "origin_country": route.origin_country if route else None,
        "dest_city": route.dest_city if route else None,
        "dest_country": route.dest_country if route else None,
    }


class _GeminiScoringClient:
    """ADK-backed ScoringClient using a single LlmAgent per batch call.

    Uses output_schema=_ScoreBatchModel (a Pydantic wrapper) because ADK's
    output_schema does not support plain list types directly.

    Deduplicates by hex before the API call. Falls back to per-aircraft calls
    if the returned array length mismatches the input.
    """

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash") -> None:
        os.environ.setdefault("GOOGLE_API_KEY", api_key)
        self._api_key = api_key
        self._model = model

    async def score_batch(
        self,
        aircraft: list[tuple[str, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]:
        if not aircraft:
            return []

        # Deduplicate by hex — same hex appearing twice in the batch window is
        # processed once; the duplicate gets the same ScoreResult.
        seen: dict[str, int] = {}
        deduped: list[tuple[str, AircraftInfo | None, RouteInfo | None]] = []
        index_map: list[int] = []
        for hex_, info, route in aircraft:
            if hex_ not in seen:
                seen[hex_] = len(deduped)
                deduped.append((hex_, info, route))
            index_map.append(seen[hex_])

        scores = await self._score_deduped(deduped)
        return [scores[i] for i in index_map]

    async def _score_deduped(
        self,
        aircraft: list[tuple[str, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]:
        input_dicts = [_aircraft_to_dict(h, i, r) for h, i, r in aircraft]
        user_text = "Flugzeuge:\n" + json.dumps(
            input_dicts, ensure_ascii=False, indent=2
        )

        agent = LlmAgent(
            model=self._model,
            name="scoring_agent",
            description="Scores aircraft for ADS-B digest.",
            instruction=_SCORING_SYSTEM_PROMPT,
            output_schema=_ScoreBatchModel,
        )
        session_service = InMemorySessionService()
        runner = Runner(
            agent=agent, app_name=_APP_NAME, session_service=session_service
        )
        session_id = str(uuid.uuid4())
        await session_service.create_session(
            app_name=_APP_NAME, user_id="enrichment", session_id=session_id
        )
        message = genai_types.Content(
            role="user", parts=[genai_types.Part(text=user_text)]
        )

        try:
            async for event in runner.run_async(
                user_id="enrichment", session_id=session_id, new_message=message
            ):
                if event.is_final_response() and event.content and event.content.parts:
                    batch = _ScoreBatchModel.model_validate_json(
                        event.content.parts[0].text
                    )
                    if len(batch.results) != len(aircraft):
                        logger.warning(
                            "score_batch: length mismatch — expected %d got %d; "
                            "falling back to per-aircraft calls",
                            len(aircraft),
                            len(batch.results),
                        )
                        return await self._fallback(aircraft)
                    return [
                        ScoreResult(
                            score=r.score,
                            tags=list(r.tags),
                            annotation=r.annotation,
                        )
                        for r in batch.results
                    ]
        except Exception:
            logger.exception(
                "score_batch: Gemini call failed for %d aircraft; using fallback",
                len(aircraft),
            )
            return await self._fallback(aircraft)

        logger.warning("score_batch: agent produced no output; using fallback")
        return await self._fallback(aircraft)

    async def _fallback(
        self,
        aircraft: list[tuple[str, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]:
        """Score each aircraft individually. Returns _FALLBACK_SCORE on failure."""
        results: list[ScoreResult] = []
        for hex_, info, route in aircraft:
            input_dicts = [_aircraft_to_dict(hex_, info, route)]
            user_text = "Flugzeuge:\n" + json.dumps(input_dicts, ensure_ascii=False)

            agent = LlmAgent(
                model=self._model,
                name="scoring_agent_single",
                description="Scores one aircraft for ADS-B digest.",
                instruction=_SCORING_SYSTEM_PROMPT,
                output_schema=_ScoreBatchModel,
            )
            session_service = InMemorySessionService()
            runner = Runner(
                agent=agent, app_name=_APP_NAME, session_service=session_service
            )
            session_id = str(uuid.uuid4())
            await session_service.create_session(
                app_name=_APP_NAME, user_id="enrichment", session_id=session_id
            )
            message = genai_types.Content(
                role="user", parts=[genai_types.Part(text=user_text)]
            )

            score: ScoreResult | None = None
            try:
                async for event in runner.run_async(
                    user_id="enrichment",
                    session_id=session_id,
                    new_message=message,
                ):
                    if (
                        event.is_final_response()
                        and event.content
                        and event.content.parts
                    ):
                        batch = _ScoreBatchModel.model_validate_json(
                            event.content.parts[0].text
                        )
                        if batch.results:
                            r = batch.results[0]
                            score = ScoreResult(
                                score=r.score,
                                tags=list(r.tags),
                                annotation=r.annotation,
                            )
            except Exception:
                logger.exception("fallback: Gemini call failed for hex=%s", hex_)

            if score is None:
                logger.warning("fallback: no result for hex=%s; using default", hex_)
                results.append(_FALLBACK_SCORE)
            else:
                results.append(score)

        return results


# ---------------------------------------------------------------------------
# EnrichmentActor
# ---------------------------------------------------------------------------


class EnrichmentActor:
    """Listens for HexFirstSeen and EnrichmentExpired events, fetches external
    data, scores aircraft in batches via AI, and stores enrichment results.

    Inbox: asyncio.Queue — unbounded, deliberate choice (see DESIGN.md
    §EnrichmentActor). At this scale (~200 new hexes/day) the queue cannot
    grow to a problematic size under normal operation.
    """

    def __init__(
        self,
        enrichment: EnrichmentRepository,
        aircraft_client: AircraftLookupClient,
        route_client: RouteClient,
        scoring_client: ScoringClient,
        enrichment_ttl: timedelta,
        batch_size: int,
        flush_interval: float,
        bus: EventBus,
    ) -> None:
        self._enrichment = enrichment
        self._aircraft_client = aircraft_client
        self._route_client = route_client
        self._scoring_client = scoring_client
        self._enrichment_ttl = enrichment_ttl
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._bus = bus
        self._inbox: asyncio.Queue[
            tuple[LogEntry, HexFirstSeen | EnrichmentExpired]
        ] = asyncio.Queue()

    @property
    def inbox(
        self,
    ) -> asyncio.Queue[tuple[LogEntry, HexFirstSeen | EnrichmentExpired]]:
        return self._inbox

    async def run(self) -> None:
        """Drain loop: collect → fetch → score → store → mark processed."""
        logger.info("enrichment actor started")
        while True:
            batch = await self._collect_batch()
            if batch:
                await self._process_batch(batch)

    async def _collect_batch(
        self,
    ) -> list[tuple[LogEntry, HexFirstSeen | EnrichmentExpired]]:
        """Collect events until batch_size reached or flush_interval elapses.

        Blocks until at least one event arrives, then drains the inbox for up
        to flush_interval seconds more.
        """
        batch: list[tuple[LogEntry, HexFirstSeen | EnrichmentExpired]] = []

        # Block until the first event.
        item = await self._inbox.get()
        batch.append(item)

        # Drain up to batch_size within flush_interval.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._flush_interval
        while len(batch) < self._batch_size:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                item = await asyncio.wait_for(self._inbox.get(), timeout=remaining)
                batch.append(item)
            except asyncio.TimeoutError:
                break

        return batch

    async def _process_batch(
        self,
        batch: list[tuple[LogEntry, HexFirstSeen | EnrichmentExpired]],
    ) -> None:
        hexes = [event.hex for _, event in batch]
        callsigns = [event.callsign for _, event in batch]

        # Fetch AircraftInfo and RouteInfo in parallel across the whole batch.
        aircraft_infos: list[AircraftInfo | None] = list(
            await asyncio.gather(*[self._fetch_aircraft(h) for h in hexes])
        )
        route_infos: list[RouteInfo | None] = list(
            await asyncio.gather(*[self._fetch_route(c) for c in callsigns])
        )

        scoring_input = list(zip(hexes, aircraft_infos, route_infos))

        try:
            scores = await self._scoring_client.score_batch(scoring_input)
        except Exception:
            logger.exception(
                "enrichment: score_batch failed for batch of %d; skipping", len(batch)
            )
            return

        if len(scores) != len(batch):
            logger.error(
                "enrichment: score_batch returned %d results for %d inputs; skipping",
                len(scores),
                len(batch),
            )
            return

        for i, (entry, event) in enumerate(batch):
            score = scores[i]
            try:
                await self._enrichment.store(
                    hex=event.hex,
                    score=score.score,
                    tags=score.tags,
                    annotation=score.annotation,
                    aircraft_info=aircraft_infos[i],
                    route_info=route_infos[i],
                    callsign=event.callsign,
                    enrichment_ttl=self._enrichment_ttl,
                )
            except Exception:
                logger.exception(
                    "enrichment: store failed for hex=%s; skipping mark_processed",
                    event.hex,
                )
                continue

            try:
                await self._bus.mark_processed(entry.id, entry.emitted_at)
            except Exception:
                logger.exception(
                    "enrichment: mark_processed failed for event id=%d", entry.id
                )

    async def _fetch_aircraft(self, hex_: str) -> AircraftInfo | None:
        try:
            return await self._aircraft_client.lookup(hex_)
        except Exception:
            logger.warning("enrichment: aircraft lookup failed for hex=%s", hex_)
            return None

    async def _fetch_route(self, callsign: str | None) -> RouteInfo | None:
        if callsign is None:
            return None
        try:
            return await self._route_client.lookup(callsign)
        except Exception:
            logger.warning("enrichment: route lookup failed for callsign=%s", callsign)
            return None
