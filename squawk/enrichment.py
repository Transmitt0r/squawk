"""Enrichment — fetch external data, score via AI, store results."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Protocol

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types as genai_types
from pydantic import BaseModel

from squawk.clients.adsbdb import AircraftInfo, AircraftLookupClient
from squawk.clients.routes import RouteClient, RouteInfo
from squawk.repositories.bulk_aircraft import BulkAircraftLookup
from squawk.repositories.enrichment import EnrichmentRepository
from squawk.tags import TAG_DESCRIPTIONS, StoryTag

logger = logging.getLogger(__name__)

_APP_NAME = "adsb_enrichment"

_EMERGENCY_SQUAWKS = frozenset({"7500", "7600", "7700"})

ROUTINE_TYPES = frozenset(
    {
        "A318",
        "A319",
        "A320",
        "A321",
        "A20N",
        "A21N",
        "B712",
        "B733",
        "B734",
        "B735",
        "B737",
        "B738",
        "B739",
        "B38M",
        "B39M",
        "E190",
        "E195",
        "E290",
        "E295",
        "CRJ7",
        "CRJ9",
        "DH8A",
        "DH8B",
        "DH8C",
        "DH8D",
        "AT43",
        "AT45",
        "AT72",
        "AT76",
    }
)

ROUTINE_OPERATORS = frozenset(
    {
        "ryanair",
        "eurowings",
        "easyjet",
        "wizz air",
        "vueling",
        "lauda europe",
        "malta air",
        "lufthansa",
        "air france",
        "klm",
        "british airways",
        "swiss",
        "austrian",
        "turkish airlines",
        "air dolomiti",
        "tap air portugal",
        "iberia",
        "alitalia",
        "ita airways",
        "italia trasporto",
        "sas",
        "scandinavian airlines",
        "norwegian",
        "buzz",
        "tui",
        "transavia",
        "air baltic",
        "brussels",
        "lot polish",
        "finnair",
        "pegasus",
        "condor",
        "aegean",
        "sunexpress",
        "helvetic",
        "luxair",
        "discover",
        "aer lingus",
        "volotea",
        "jet2",
    }
)


def _build_tag_list() -> str:
    lines = []
    for tag in StoryTag:
        desc = TAG_DESCRIPTIONS[tag]
        lines.append(f'  "{tag.value}" — {desc}')
    return "\n".join(lines)


_SCORING_SYSTEM_PROMPT = (
    """
You rate aircraft for a daily ADS-B digest near Stuttgart, Germany.
Return a score (1–10) and relevant tags for each aircraft.

Score guidelines:
- 1–3: Routine commercial traffic (Ryanair, Eurowings, short domestic routes),
       and medical/air ambulance flights — these are very common near Stuttgart
       due to proximity to a major hospital and should not be highlighted
- 4–6: Interesting but normal (long-haul, cargo, unfamiliar operator)
- 7–8: Unusual (military, private jet, exotic destination, rare type)
- 9–10: Very rare or extraordinary (historic aircraft, emergency squawk, VIP transport)

Rules:
1. Squawk 7500 (hijack), 7600 (radio failure), 7700 (emergency) → score ≥ 9.
2. No hallucination: if type/registration/operator are all null,
   do NOT infer aircraft identity. Score ≤ 4 unless the callsign or squawk
   provides clear evidence of something unusual.

Input fields per aircraft:
- hex, callsign: ADS-B identity
- alt_baro_ft, gs_knots, squawk: live telemetry (ground truth)
- aircraft: merged registry data (null if no source had data),
   with: registration, icao_type, type (human-readable), operator, flag
- origin/dest fields: route information from a route database

Tag descriptions:
"""
    + _build_tag_list()
    + """

annotation: one English sentence explaining why interesting; "" if score ≤ 3
"""
).strip()


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EnrichItem:
    """One aircraft to enrich. Telemetry fields are from the live sighting."""

    hex: str
    callsign: str | None
    alt_baro: int | None  # feet
    gs: float | None  # knots
    squawk: str | None


@dataclass(frozen=True)
class ScoreResult:
    score: int  # 1–10
    tags: list[StoryTag]
    annotation: str  # one English sentence; empty string if unremarkable


class ScoringClient(Protocol):
    async def score_batch(
        self,
        aircraft: list[tuple[EnrichItem, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]: ...


# ---------------------------------------------------------------------------
# Pre-filter — deterministic scoring without LLM
# ---------------------------------------------------------------------------


def _is_routine_operator(operator: str | None) -> bool:
    if not operator:
        return False
    lower = operator.lower()
    return any(op in lower for op in ROUTINE_OPERATORS)


def pre_filter_score(
    merged: AircraftInfo | None,
    route: RouteInfo | None,
    squawk: str | None,
) -> ScoreResult | None:
    """Return a deterministic ScoreResult or None if the LLM is needed."""
    if squawk in _EMERGENCY_SQUAWKS:
        return ScoreResult(
            score=9,
            tags=[StoryTag.EMERGENCY],
            annotation=f"Emergency squawk {squawk} detected.",
        )

    if merged is not None and merged.mil:
        return ScoreResult(score=5, tags=[StoryTag.MILITARY], annotation="")

    if merged is not None and _is_routine_operator(merged.operator):
        icao = merged.icao_type
        if icao and icao in ROUTINE_TYPES:
            return ScoreResult(score=1, tags=[StoryTag.COMMERCIAL], annotation="")

    return None


# ---------------------------------------------------------------------------
# Private Pydantic models for ADK structured output
# ---------------------------------------------------------------------------


class _ScoreResultModel(BaseModel):
    hex: str
    score: int
    tags: list[StoryTag]
    annotation: str


class _ScoreBatchModel(BaseModel):
    results: list[_ScoreResultModel]


# ---------------------------------------------------------------------------
# Private ADK-backed ScoringClient implementation
# ---------------------------------------------------------------------------


_FALLBACK_SCORE = ScoreResult(score=1, tags=[], annotation="")


def _aircraft_to_dict(
    item: EnrichItem,
    merged: AircraftInfo | None,
    route: RouteInfo | None,
) -> dict:
    info_dict = None
    if merged is not None:
        info_dict = {
            "registration": merged.registration,
            "icao_type": merged.icao_type,
            "type": merged.type,
            "operator": merged.operator,
            "flag": merged.flag,
        }
    return {
        "hex": item.hex,
        "callsign": item.callsign,
        "alt_baro_ft": item.alt_baro,
        "gs_knots": item.gs,
        "squawk": item.squawk,
        "aircraft": info_dict,
        "origin_iata": route.origin_iata if route else None,
        "origin_icao": route.origin_icao if route else None,
        "origin_city": route.origin_city if route else None,
        "origin_country": route.origin_country if route else None,
        "dest_iata": route.dest_iata if route else None,
        "dest_icao": route.dest_icao if route else None,
        "dest_city": route.dest_city if route else None,
        "dest_country": route.dest_country if route else None,
    }


class _GeminiScoringClient:
    """ADK-backed ScoringClient using a single LlmAgent per batch call."""

    def __init__(self, model: str = "gemini-3-flash-preview") -> None:
        self._model = model

    async def score_batch(
        self,
        aircraft: list[tuple[EnrichItem, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]:
        if not aircraft:
            return []

        seen: dict[str, int] = {}
        deduped: list[tuple[EnrichItem, AircraftInfo | None, RouteInfo | None]] = []
        index_map: list[int] = []
        for item, merged, route in aircraft:
            if item.hex not in seen:
                seen[item.hex] = len(deduped)
                deduped.append((item, merged, route))
            index_map.append(seen[item.hex])

        scores = await self._score_deduped(deduped)
        return [scores[i] for i in index_map]

    async def _score_deduped(
        self,
        aircraft: list[tuple[EnrichItem, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]:
        input_dicts = [
            _aircraft_to_dict(item, merged, route) for item, merged, route in aircraft
        ]
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
                    text = event.content.parts[0].text
                    if text is None:
                        continue
                    batch = _ScoreBatchModel.model_validate_json(text)
                    results_by_hex = {r.hex: r for r in batch.results}
                    missing = [
                        item.hex
                        for item, _, _ in aircraft
                        if item.hex not in results_by_hex
                    ]
                    if missing:
                        logger.warning(
                            "score_batch: %d hexes missing from response: %s; "
                            "falling back to per-aircraft calls",
                            len(missing),
                            missing[:5],
                        )
                        return await self._fallback(aircraft)
                    return [
                        ScoreResult(
                            score=results_by_hex[item.hex].score,
                            tags=list(results_by_hex[item.hex].tags),
                            annotation=results_by_hex[item.hex].annotation,
                        )
                        for item, _, _ in aircraft
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
        aircraft: list[tuple[EnrichItem, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]:
        results: list[ScoreResult] = []
        for item, merged, route in aircraft:
            input_dicts = [_aircraft_to_dict(item, merged, route)]
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
                        text = event.content.parts[0].text
                        if text is None:
                            continue
                        batch = _ScoreBatchModel.model_validate_json(text)
                        if batch.results:
                            r = batch.results[0]
                            if r.hex != item.hex:
                                logger.warning(
                                    "fallback: hex mismatch expected=%s got=%s",
                                    item.hex,
                                    r.hex,
                                )
                                continue
                            score = ScoreResult(
                                score=r.score,
                                tags=list(r.tags),
                                annotation=r.annotation,
                            )
            except Exception:
                logger.exception("fallback: Gemini call failed for hex=%s", item.hex)

            if score is None:
                logger.warning(
                    "fallback: no result for hex=%s; using default", item.hex
                )
                results.append(_FALLBACK_SCORE)
            else:
                results.append(score)

        return results


# ---------------------------------------------------------------------------
# Source merging
# ---------------------------------------------------------------------------


def _merge_aircraft_info(
    bulk: AircraftInfo | None,
    hexdb: AircraftInfo | None,
    adsbdb: AircraftInfo | None,
) -> AircraftInfo | None:
    """Merge aircraft info from three sources.

    Priority per field:
    - registration: bulk (mictronics) > hexdb > adsbdb
    - icao_type:    bulk > hexdb
    - type (human): adsbdb > hexdb > bulk  (adsbdb has best human-readable names)
    - operator:     hexdb > adsbdb          (mictronics has no operator)
    - flag:         adsbdb > hexdb          (adsbdb has ISO country name)
    - mil:          bulk only               (only mictronics has this flag)

    Returns None only if all three sources returned None.
    """
    if bulk is None and hexdb is None and adsbdb is None:
        return None

    def first(*values: str | None) -> str | None:
        return next((v for v in values if v), None)

    return AircraftInfo(
        registration=first(
            bulk.registration if bulk else None,
            hexdb.registration if hexdb else None,
            adsbdb.registration if adsbdb else None,
        ),
        type=first(
            adsbdb.type if adsbdb else None,
            hexdb.type if hexdb else None,
            bulk.type if bulk else None,
        ),
        operator=first(
            hexdb.operator if hexdb else None,
            adsbdb.operator if adsbdb else None,
        ),
        flag=first(
            adsbdb.flag if adsbdb else None,
            hexdb.flag if hexdb else None,
        ),
        icao_type=first(
            bulk.icao_type if bulk else None,
            hexdb.icao_type if hexdb else None,
        ),
        mil=bulk.mil if bulk else None,
        short_type=bulk.short_type if bulk else None,
    )


# ---------------------------------------------------------------------------
# enrich_batch — public function
# ---------------------------------------------------------------------------


async def enrich_batch(
    items: list[EnrichItem],
    aircraft_client: AircraftLookupClient,
    hexdb_client: AircraftLookupClient,
    bulk_repo: BulkAircraftLookup,
    route_client: RouteClient,
    scoring_client: ScoringClient,
    enrichment_repo: EnrichmentRepository,
) -> None:
    """Fetch external data from three sources, score via AI, store results.

    1. Fetch AircraftInfo from adsbdb, hexdb, and bulk_aircraft in parallel.
    2. Merge the three sources into one AircraftInfo per aircraft.
    3. Fetch RouteInfo in parallel.
    4. Pre-filter: deterministically score routine aircraft without the LLM.
    5. Call scoring_client.score_batch() for remaining aircraft.
    6. Store each result via enrichment_repo.store() (upsert, idempotent).
    """
    if not items:
        return

    logger.info("enrich_batch: processing %d aircraft", len(items))

    adsbdb_infos, hexdb_infos, bulk_infos, route_infos = await asyncio.gather(
        asyncio.gather(*[_fetch_aircraft(aircraft_client, i.hex) for i in items]),
        asyncio.gather(*[_fetch_aircraft(hexdb_client, i.hex) for i in items]),
        asyncio.gather(*[_fetch_bulk(bulk_repo, i.hex) for i in items]),
        asyncio.gather(*[_fetch_route(route_client, i.callsign) for i in items]),
    )

    merged_infos = [
        _merge_aircraft_info(bulk, hexdb, adsbdb)
        for bulk, hexdb, adsbdb in zip(bulk_infos, hexdb_infos, adsbdb_infos)
    ]

    pre_filtered: list[tuple[int, ScoreResult]] = []
    needs_llm: list[tuple[int, EnrichItem, AircraftInfo | None, RouteInfo | None]] = []

    for i, item in enumerate(items):
        result = pre_filter_score(merged_infos[i], route_infos[i], item.squawk)
        if result is not None:
            pre_filtered.append((i, result))
        else:
            needs_llm.append((i, item, merged_infos[i], route_infos[i]))

    for i, result in pre_filtered:
        try:
            await enrichment_repo.store(
                hex=items[i].hex,
                score=result.score,
                tags=result.tags,
                annotation=result.annotation,
                aircraft_info=merged_infos[i],
                route_info=route_infos[i],
                callsign=items[i].callsign,
            )
        except Exception:
            logger.exception(
                "enrich_batch: store failed for hex=%s; skipping", items[i].hex
            )

    if not needs_llm:
        logger.info(
            "enrich_batch: all %d aircraft pre-filtered, no LLM call needed",
            len(items),
        )
        return

    logger.info(
        "enrich_batch: %d/%d aircraft need LLM scoring",
        len(needs_llm),
        len(items),
    )

    scoring_input = [(item, merged, route) for _, item, merged, route in needs_llm]

    try:
        scores = await scoring_client.score_batch(scoring_input)
    except Exception:
        logger.exception(
            "enrich_batch: score_batch failed for batch of %d; skipping",
            len(needs_llm),
        )
        return

    if len(scores) != len(needs_llm):
        logger.error(
            "enrich_batch: score_batch returned %d results for %d inputs; skipping",
            len(scores),
            len(needs_llm),
        )
        return

    for j, (orig_idx, item, _, _) in enumerate(needs_llm):
        score = scores[j]
        try:
            await enrichment_repo.store(
                hex=item.hex,
                score=score.score,
                tags=score.tags,
                annotation=score.annotation,
                aircraft_info=merged_infos[orig_idx],
                route_info=route_infos[orig_idx],
                callsign=item.callsign,
            )
        except Exception:
            logger.exception(
                "enrich_batch: store failed for hex=%s; skipping", item.hex
            )


async def _fetch_aircraft(
    client: AircraftLookupClient, hex_: str
) -> AircraftInfo | None:
    try:
        return await client.lookup(hex_)
    except Exception:
        logger.warning("enrich_batch: aircraft lookup failed for hex=%s", hex_)
        return None


async def _fetch_bulk(repo: BulkAircraftLookup, hex_: str) -> AircraftInfo | None:
    try:
        return await repo.lookup(hex_)
    except Exception:
        logger.warning("enrich_batch: bulk lookup failed for hex=%s", hex_)
        return None


async def _fetch_route(client: RouteClient, callsign: str | None) -> RouteInfo | None:
    if callsign is None:
        return None
    try:
        return await client.lookup(callsign)
    except Exception:
        logger.warning("enrich_batch: route lookup failed for callsign=%s", callsign)
        return None
