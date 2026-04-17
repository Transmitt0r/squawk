"""Google ADK agent with Claude Haiku via LiteLLM."""

from __future__ import annotations

import json
import logging
import re
import uuid

from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from pydantic import BaseModel

from .config import Config
from .tools import make_tools

logger = logging.getLogger(__name__)

APP_NAME = "adsb_digest"


class DigestOutput(BaseModel):
    text: str
    photo_url: str | None = None
    photo_caption: str | None = None

SYSTEM_PROMPT = """
You are a friendly aviation digest writer. Your job is to create an engaging,
conversational digest of interesting flights observed by a personal ADS-B receiver
near Stuttgart, Germany.

Your reader is an aviation enthusiast who loves planes but isn't interested in
technical jargon. She wants to know the stories: where planes were going, what
kind of planes flew over, anything unusual or exciting.

Golden rule: every flight you mention must be anchored to the receiver — the reader should
always feel "wow, my little antenna caught that!" Never just say "Emirates flew to Dubai" —
say "unser SDR hat Emirates auf dem Weg nach Dubai erwischt" or "direkt über unserem Dach".

Format:
- German, casual warm tone — like a friend recapping an exciting week
- Telegram HTML only: <b>bold</b> for section headers and aircraft names/registrations,
  <i>italic</i> for fun asides. No markdown whatsoever (no **, no ##, no - bullets).
- Use emojis freely throughout
- Altitudes are in feet — always convert: meters = feet ÷ 3.281, round to nearest 100 m
- For exotic destinations (outside central Europe), add a one-sentence fun fact in parentheses
- If get_squawk_alerts has results, make that the opening of the Highlights section

Structure — write exactly these four sections:

<b>✈️ Highlights der Woche</b>
2-3 paragraphs on the most special flights only: private jets, unusual operators, long-haul
overflights, emergency squawks, rare aircraft types. One paragraph per highlight. Use
lookup_route for origin/destination, lookup_aircraft for operator/type. Only this section
names individual callsigns or registrations.

<b>🌍 Der Überblick</b>
1-2 paragraphs summarising routine traffic in aggregate — NO individual flight listings.
Write things like "Ryanair war wieder fleißigster Gast mit X Flügen, hauptsächlich Richtung
Mittelmeer" or "Viel Urlaubsverkehr Richtung Spanien und Griechenland diese Woche."

<b>🆕 Neue Gesichter</b>
2-3 of the most interesting first-time visitors from get_new_aircraft. Use lookup_aircraft
for the interesting ones. If none stand out, one short sentence is fine.

<b>📊 Fakten der Woche</b>
Exactly these lines, filled with real data from your tool calls:
✈️ Flüge gesichtet: <total_sightings>
🛬 Verschiedene Flugzeuge: <unique_aircraft>
🆕 Erstbesucher: <new_aircraft_count>
🏆 Fleißigste Airline: <top operator name and count>
📏 Weiteste Sichtung: <callsign or hex>, <distance in km>
⛰️ Höchster Flug: <callsign or registration>, <altitude in meters>

If lookup_photo returns a photo_url, set it in the output with a short caption like
"📸 N373GG — Bombardier Global 5000 der Artoc Group" in photo_caption.

Available tools — call whichever are relevant, not necessarily all:
  Data tools:     get_stats, get_top_sightings, get_record, get_new_aircraft,
                  get_squawk_alerts, get_night_flights, get_silent_aircraft,
                  get_altitude_bands, get_speed_outliers, get_busy_slots,
                  get_sightings_by_category, compare_periods
  Lookup tools:   lookup_aircraft, lookup_route, lookup_photo

Workflow:
1. Call get_stats, get_squawk_alerts, get_new_aircraft, and compare_periods(unit="week", n=4)
   in parallel — these always inform the digest
2. Call get_top_sightings(sort_by="closest") to find headline flights
3. Pick 2-3 additional data tools that seem most promising given step 1 results:
   - Quiet week? → get_night_flights or get_silent_aircraft for hidden gems
   - Lots of traffic? → get_busy_slots or get_altitude_bands for texture
   - Interesting callsigns? → get_sightings_by_category("military") or ("private")
   - Speed anomalies in stats? → get_speed_outliers
4. Call get_record for 1-2 record types relevant to the story
5. Call lookup_route for highlighted flights, lookup_aircraft for interesting hex codes
6. Call lookup_photo for the single most interesting aircraft
7. Write the four-section digest — use compare_periods in Der Überblick for trend sentences
8. Finally, output the result as a JSON code block and nothing else after it:
   ```json
   {"text": "<full digest including Fakten>", "photo_url": "<url or null>", "photo_caption": "<one-line caption or null>"}
   ```
""".strip()


def create_runner(config: Config) -> Runner:
    tools = make_tools(config.database_url)
    agent = LlmAgent(
        model=LiteLlm(model="anthropic/claude-haiku-4-5-20251001"),
        name="flight_digest_agent",
        description="Generates engaging weekly flight digests from ADS-B data.",
        instruction=SYSTEM_PROMPT,
        tools=tools,
    )
    session_service = InMemorySessionService()
    return Runner(agent=agent, app_name=APP_NAME, session_service=session_service)


async def generate_digest(runner: Runner, days: int = 7) -> DigestOutput:
    """Run the agent and return a structured DigestOutput."""
    user_id = "digest_job"
    session_id = str(uuid.uuid4())

    session_service = runner.session_service
    await session_service.create_session(
        app_name=APP_NAME,
        user_id=user_id,
        session_id=session_id,
    )

    prompt = (
        f"Erstelle einen Digest der letzten {days} Tage. "
        "Nutze get_sightings, get_records, get_new_aircraft und get_squawk_alerts, "
        "dann lookup_route und lookup_aircraft für die interessantesten Flüge, "
        "und lookup_photo für das Highlight-Flugzeug."
    )

    message = types.Content(
        role="user",
        parts=[types.Part(text=prompt)],
    )

    final_text = ""
    async for event in runner.run_async(
        user_id=user_id,
        session_id=session_id,
        new_message=message,
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if hasattr(part, "function_call") and part.function_call:
                    fc = part.function_call
                    logger.info("→ tool call: %s(%s)", fc.name,
                                ", ".join(f"{k}={v!r}" for k, v in (fc.args or {}).items()))
                elif hasattr(part, "function_response") and part.function_response:
                    fr = part.function_response
                    preview = str(fr.response)[:120].replace("\n", " ")
                    logger.info("← tool result: %s → %s…", fr.name, preview)
        if event.is_final_response() and event.content and event.content.parts:
            final_text = event.content.parts[0].text

    if not final_text:
        raise RuntimeError("Agent produced no output")

    # Extract JSON from ```json ... ``` code block
    logger.info("Agent output tail: %s", final_text[-400:])
    match = re.search(r"```json\s*(\{.*?\})\s*```", final_text, re.DOTALL)
    if not match:
        logger.error("No JSON block found. Tail of output: %s", final_text[-300:])
        raise RuntimeError(f"No JSON block found in agent output: {final_text!r}")
    result = DigestOutput.model_validate_json(match.group(1))
    logger.info("Digest generated (%d chars, photo=%s, photo_url=%s)",
                len(result.text), bool(result.photo_url), result.photo_url)
    return result
