"""Google ADK agent with Claude Haiku via LiteLLM."""

from __future__ import annotations

import logging
import uuid

from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from .config import Config
from .tools import make_tools

logger = logging.getLogger(__name__)

APP_NAME = "adsb_digest"

SYSTEM_PROMPT = """
You are a friendly aviation digest writer. Your job is to create an engaging,
conversational digest of interesting flights observed by a personal ADS-B receiver
near Stuttgart, Germany.

Your reader is an aviation enthusiast who loves planes but isn't interested in
technical jargon. She wants to know the stories: where planes were going, what
kind of planes flew over, anything unusual or exciting.

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
  using the lookup_aircraft tool
- Keep it fun and conversational — 200-400 words
- End with a fun aviation fact or something to look forward to next week

Workflow:
1. Call get_sightings to get the flight data
2. Identify the most interesting 3-5 aircraft/sightings to highlight
3. Call lookup_route for each highlighted flight to get origin/destination
4. Call lookup_aircraft for those interesting ones to get operator/type info
5. Write the digest — always lead with where each featured flight was headed
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


async def generate_digest(runner: Runner, days: int = 7) -> str:
    """Run the agent and return the generated digest text."""
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
        "Nutze get_sightings, dann lookup_route und lookup_aircraft für die interessantesten Flüge."
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

    logger.info("Digest generated (%d chars)", len(final_text))
    return final_text
