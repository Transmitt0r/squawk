"""Background enrichment job: score and annotate aircraft every 15 minutes."""

from __future__ import annotations

import json
import logging
import uuid

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from pydantic import BaseModel

from .config import Config
from .db import get_unenriched_aircraft, store_enrichment
from .tools import lookup_aircraft, lookup_route

logger = logging.getLogger(__name__)

_APP_NAME = "score_agent"

_SYSTEM_PROMPT = """\
Score this aircraft for story interest in a weekly aviation digest (score 1–10, 10 = most compelling).

Scoring guide:
- Military aircraft (any country, e.g. GAF, RCH, REACH, NATO prefixes): 9–10
- Private/executive jet (Gulfstream, Bombardier Global, Dassault Falcon, etc.): 7–8
- Long-haul exotic operator or unusual destination (intercontinental, Middle East, Asia): 6–8
- Cargo aircraft or unusual type (A380, 747F, C-130, etc.): 5–7
- Unknown aircraft with no registration data: 5–6
- Regular short-haul (Ryanair, Wizz, EasyJet, Eurowings, TUI, Condor): 1–3
- Standard charter or regional flight: 3–5

Return:
- score: int 1–10
- tags: list of 1–3 short English tags, e.g. ["military", "luftwaffe"] or ["private_jet"]
- annotation: one German sentence about why interesting; empty string if unremarkable\
"""


class ScoreResult(BaseModel):
    score: int
    tags: list[str]
    annotation: str


async def _score_and_annotate(
    hex_: str,
    callsign: str | None,
    aircraft_json: str,
    route_json: str | None,
) -> dict:
    """Run a scoring agent for one aircraft. Never raises — returns safe defaults on failure."""
    try:
        agent = LlmAgent(
            model="gemini-3-flash-preview",
            name="score_agent",
            instruction=_SYSTEM_PROMPT,
            output_schema=ScoreResult,
        )
        session_service = InMemorySessionService()
        runner = Runner(
            agent=agent, app_name=_APP_NAME, session_service=session_service
        )

        session_id = str(uuid.uuid4())
        await session_service.create_session(
            app_name=_APP_NAME, user_id="enrichment", session_id=session_id
        )

        data = (
            f"hex: {hex_}\n"
            f"callsign: {callsign or 'unknown'}\n"
            f"aircraft data: {aircraft_json}\n"
            f"route data: {route_json or 'unknown'}"
        )
        message = types.Content(role="user", parts=[types.Part(text=data)])

        async for event in runner.run_async(
            user_id="enrichment", session_id=session_id, new_message=message
        ):
            if event.is_final_response() and event.content and event.content.parts:
                result = ScoreResult.model_validate_json(event.content.parts[0].text)
                return {
                    "score": result.score,
                    "tags": result.tags,
                    "annotation": result.annotation,
                }
    except Exception:
        logger.exception("_score_and_annotate failed for %s", hex_)
    return {"score": 3, "tags": [], "annotation": ""}


async def run_enrichment(config: Config) -> None:
    """Enrich up to 50 unenriched aircraft. Called every 15 minutes by the scheduler."""
    try:
        rows = get_unenriched_aircraft(config.database_url, limit=50)
    except Exception:
        logger.exception("get_unenriched_aircraft failed")
        return

    if not rows:
        logger.debug("No aircraft to enrich")
        return

    logger.info("Enriching %d aircraft", len(rows))

    for hex_, callsign in rows:
        try:
            aircraft_json = lookup_aircraft(hex_)
            route_json = lookup_route(callsign) if callsign else None
            aircraft_dict = json.loads(aircraft_json)
            route_dict = json.loads(route_json) if route_json else None

            score_result = await _score_and_annotate(
                hex_, callsign, aircraft_json, route_json
            )
            store_enrichment(
                config.database_url,
                hex_,
                callsign,
                aircraft_dict,
                route_dict,
                score_result,
            )
            logger.debug(
                "Enriched %s (callsign=%s score=%s tags=%s)",
                hex_,
                callsign,
                score_result["score"],
                score_result["tags"],
            )
        except Exception:
            logger.exception("Failed to enrich %s", hex_)
