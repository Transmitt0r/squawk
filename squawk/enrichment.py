"""Enrichment — fetch external data, score via AI, store results."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types as genai_types
from pydantic import BaseModel

from squawk.clients.adsbdb import AircraftInfo, AircraftLookupClient
from squawk.clients.routes import RouteClient, RouteInfo
from squawk.repositories.enrichment import EnrichmentRepository

logger = logging.getLogger(__name__)

_APP_NAME = "adsb_enrichment"

_SCORING_SYSTEM_PROMPT = """
You rate aircraft for a weekly ADS-B digest near Stuttgart, Germany.
For each aircraft in the list, return a score (1–10):

Score guidelines:
- 1–3: Routine commercial traffic (Ryanair, Eurowings, short domestic routes)
- 4–6: Interesting but normal (long-haul, cargo, unfamiliar operator)
- 7–8: Unusual (military, private jet, exotic destination, rare type)
- 9–10: Very rare or extraordinary (historic aircraft, emergency squawk,
        medical evacuation, VIP transport)

IMPORTANT: Squawk 7500 (hijack), 7600 (radio failure), 7700 (emergency) → Score ≥ 9.

tags: short English keywords (e.g. "military", "cargo", "bizjet",
      "emergency", "long-haul", "low-altitude", "unusual-operator")

annotation: a single English sentence explaining why the aircraft is interesting.
Leave empty ("") if unremarkable (score ≤ 3).

The output is a JSON object with a field "results" containing an array with exactly
as many entries as the input list — in the same order.
""".strip()


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScoreResult:
    score: int  # 1–10
    tags: list[str]
    annotation: str  # one English sentence; empty string if unremarkable


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

    def __init__(self, model: str = "gemini-3-flash-preview") -> None:
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
        user_text = "Aircraft:\n" + json.dumps(
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
            user_text = "Aircraft:\n" + json.dumps(input_dicts, ensure_ascii=False)

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
# enrich_batch — public function
# ---------------------------------------------------------------------------


async def enrich_batch(
    items: list[tuple[str, str | None]],  # (hex, callsign) pairs
    aircraft_client: AircraftLookupClient,
    route_client: RouteClient,
    scoring_client: ScoringClient,
    enrichment_repo: EnrichmentRepository,
    enrichment_ttl: timedelta,
) -> None:
    """Fetch external data, score via AI, store results.

    1. Fetch AircraftInfo and RouteInfo in parallel for all items.
    2. Call scoring_client.score_batch() — one Gemini call for the whole batch.
    3. Store each result via enrichment_repo.store() (upsert, idempotent).

    Errors: if score_batch fails, logs and returns (batch skipped). Individual
    store failures are logged and skipped — the rest of the batch continues.
    """
    if not items:
        return

    logger.info("enrich_batch: scoring %d aircraft", len(items))
    hexes = [hex_ for hex_, _ in items]
    callsigns = [callsign for _, callsign in items]

    aircraft_infos: list[AircraftInfo | None] = list(
        await asyncio.gather(*[_fetch_aircraft(aircraft_client, h) for h in hexes])
    )
    route_infos: list[RouteInfo | None] = list(
        await asyncio.gather(*[_fetch_route(route_client, c) for c in callsigns])
    )

    scoring_input = list(zip(hexes, aircraft_infos, route_infos))

    try:
        scores = await scoring_client.score_batch(scoring_input)
    except Exception:
        logger.exception(
            "enrich_batch: score_batch failed for batch of %d; skipping", len(items)
        )
        return

    if len(scores) != len(items):
        logger.error(
            "enrich_batch: score_batch returned %d results for %d inputs; skipping",
            len(scores),
            len(items),
        )
        return

    for i, (hex_, callsign) in enumerate(items):
        score = scores[i]
        try:
            await enrichment_repo.store(
                hex=hex_,
                score=score.score,
                tags=score.tags,
                annotation=score.annotation,
                aircraft_info=aircraft_infos[i],
                route_info=route_infos[i],
                callsign=callsign,
                enrichment_ttl=enrichment_ttl,
            )
        except Exception:
            logger.exception("enrich_batch: store failed for hex=%s; skipping", hex_)


async def _fetch_aircraft(
    client: AircraftLookupClient, hex_: str
) -> AircraftInfo | None:
    try:
        return await client.lookup(hex_)
    except Exception:
        logger.warning("enrich_batch: aircraft lookup failed for hex=%s", hex_)
        return None


async def _fetch_route(client: RouteClient, callsign: str | None) -> RouteInfo | None:
    if callsign is None:
        return None
    try:
        return await client.lookup(callsign)
    except Exception:
        logger.warning("enrich_batch: route lookup failed for callsign=%s", callsign)
        return None
