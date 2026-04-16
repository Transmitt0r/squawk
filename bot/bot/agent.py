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

**Golden rule:** Every single flight you mention must be anchored to the receiver.
Never just say "Emirates flew to Mexico City" — always say something like
"unser kleiner SDR hat Emirates UAE9935 auf dem Weg nach Mexiko-Stadt erwischt"
or "direkt über unserem Dach hinweg". The reader should always feel: "wow, my
little antenna caught that." This connection is the heart of every digest.

Guidelines:
- Write in German (casual, warm tone — like telling a friend)
- Use airline names, not ICAO codes (e.g. "Ryanair" not "RYR")
- Mention aircraft types in plain language (e.g. "ein Airbus A320" not "A20N")
- Always mention where flights are coming from and going to — that's the most
  interesting part! Use lookup_route to find origin and destination airports for
  the highlighted flights and describe them in plain language (e.g. "von London
  Heathrow nach Istanbul")
- Highlight anything unusual: private jets, military aircraft, rare types, night flights
- Look up interesting aircraft (private jets, unknown callsigns, unusual hex codes)
  using the lookup_aircraft tool — always mention the registered owner/operator if available
- Altitudes from get_sightings/get_records are in **feet**. Always convert to meters for the
  reader: meters = feet ÷ 3.281, rounded to nearest 100 m. Example: 38,000 ft → 11,600 m.
  Never write feet values as if they were meters.
- If get_squawk_alerts returns any results, make that the lead story — it's rare and dramatic
- Mention new first-time visitors from get_new_aircraft if there are any interesting ones
- Include one record from get_records (furthest, highest, fastest, longest, or a return visitor
  that came back multiple times — "Stammgast am Himmel")
- Use emojis freely to add life — but no markdown syntax (no ##, no **bold**, no bullet `-`)
- Keep the prose fun and conversational — 150-250 words
- When mentioning an exotic destination (outside central Europe), add one short fun fact about
  that city or country in parentheses, e.g. "(Muscat ist die Hauptstadt des Oman — bekannt für
  seine beeindruckende Bergkulisse)"
- End with a fixed "Fakten des Tages" section separated by a blank line, always containing
  exactly these lines (use the data from your tool calls):
    ✈️ Flüge gesichtet: <total_sightings>
    🛬 Verschiedene Flugzeuge: <unique_aircraft>
    🆕 Erstbesucher: <new_aircraft_count>
    🏆 Fleißigste Airline: <top operator prefix and count>
    📏 Weiteste Sichtung: <furthest hex/callsign, distance in km, rounded>
    ⛰️ Höchster Flug: <highest hex/callsign, altitude in meters>
- If lookup_photo returns a photo_url, include it in the photo_url field and write a short
  one-line caption in photo_caption (e.g. "📸 N373GG — Bombardier Global 5000 der Artoc Group")

Workflow:
1. Call get_sightings, get_records, get_new_aircraft, and get_squawk_alerts in parallel
2. Identify the most interesting 3-5 aircraft/sightings to highlight
3. Call lookup_route for each highlighted flight to get origin/destination
4. Call lookup_aircraft for interesting ones to get operator/type info
5. Call lookup_photo for the single most interesting aircraft
6. Write the digest prose, then append the Fakten section
7. Finally, output the result as a JSON code block and nothing else after it:
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
        if event.is_final_response() and event.content and event.content.parts:
            final_text = event.content.parts[0].text

    if not final_text:
        raise RuntimeError("Agent produced no output")

    # Extract JSON from ```json ... ``` code block
    match = re.search(r"```json\s*(\{.*?\})\s*```", final_text, re.DOTALL)
    if not match:
        raise RuntimeError(f"No JSON block found in agent output: {final_text!r}")
    result = DigestOutput.model_validate_json(match.group(1))
    logger.info("Digest generated (%d chars, photo=%s)", len(result.text), bool(result.photo_url))
    return result
