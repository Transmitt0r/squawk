"""Digest generation: ADK agent with structured output from pre-enriched data."""

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
from .db import get_digest_candidates, get_digest_stats
from .tools import lookup_photo

logger = logging.getLogger(__name__)

_APP_NAME = "adsb_digest"


class DigestOutput(BaseModel):
    text: str
    photo_url: str | None = None
    photo_caption: str | None = None


SYSTEM_PROMPT = """
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


def _build_data_packet(
    candidates: list[dict], stats: dict, photos: dict[str, dict]
) -> str:
    for c in candidates:
        photo = photos.get(c["hex"])
        if photo:
            c["photo"] = photo
    return json.dumps(
        {"stats": stats, "candidates": candidates}, ensure_ascii=False, default=str
    )


async def generate_digest(config: Config, days: int = 7) -> DigestOutput:
    """Generate a digest from pre-enriched data with a single ADK agent call."""
    candidates = get_digest_candidates(config.database_url, days)
    stats = get_digest_stats(config.database_url, days)

    photos: dict[str, dict] = {}
    for candidate in candidates[:2]:
        hex_ = candidate["hex"]
        try:
            photo_data = json.loads(lookup_photo(hex_))
            if "error" not in photo_data:
                photos[hex_] = photo_data
        except Exception:
            logger.warning("Photo lookup failed for %s", hex_)

    data_packet = _build_data_packet(candidates, stats, photos)
    logger.info(
        "Digest data packet: %d candidates, %d chars", len(candidates), len(data_packet)
    )

    agent = LlmAgent(
        model="gemini-3-flash-preview",
        name="flight_digest_agent",
        description="Generates engaging weekly flight digests from ADS-B data.",
        instruction=SYSTEM_PROMPT,
        output_schema=DigestOutput,
    )
    session_service = InMemorySessionService()
    runner = Runner(agent=agent, app_name=_APP_NAME, session_service=session_service)

    session_id = str(uuid.uuid4())
    await session_service.create_session(
        app_name=_APP_NAME, user_id="digest_job", session_id=session_id
    )

    message = types.Content(role="user", parts=[types.Part(text=data_packet)])

    async for event in runner.run_async(
        user_id="digest_job", session_id=session_id, new_message=message
    ):
        if event.is_final_response() and event.content and event.content.parts:
            result = DigestOutput.model_validate_json(event.content.parts[0].text)
            logger.info(
                "Digest generated (%d chars, photo=%s)",
                len(result.text),
                bool(result.photo_url),
            )
            return result

    raise RuntimeError("Agent produced no output")
