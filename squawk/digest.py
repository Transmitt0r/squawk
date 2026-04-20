"""Digest — generate and broadcast daily flight digests."""

from __future__ import annotations

import dataclasses
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Protocol

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types as genai_types
from pydantic import BaseModel

from squawk.charts import render_traffic_chart
from squawk.clients.planespotters import PhotoClient, PhotoInfo
from squawk.queries.charts import ChartQuery
from squawk.queries.digest import DigestQuery

if TYPE_CHECKING:
    from squawk.bot.broadcaster import Broadcaster
    from squawk.repositories.digest import DigestRepository

logger = logging.getLogger(__name__)

_APP_NAME = "adsb_digest"

_DIGEST_SYSTEM_PROMPT = """
Du bist ein Luftfahrt-Journalist, der einen täglichen Digest über Flugzeuge
schreibt, die von einem privaten ADS-B-Empfänger nahe Stuttgart empfangen wurden.

Deine Leserin interessiert sich für Flugzeuge, ist aber keine Expertin. Erkläre
Fachbegriffe, Abkürzungen und warum etwas erwähnenswert ist. Schreibe faktisch
und nah an den Daten — erfinde keine Reaktionen, Emotionen oder Dramatik.
Beschreibe nur das, was aus den Daten hervorgeht.

Beispiele für gute Erklärungen:
- Statt "ELINT-Maschine" → "ein Flugzeug zur elektronischen Aufklärung, das
  Funk- und Radarsignale aus der Ferne abfängt"
- Statt "AEW&C" → "ein fliegendes Frühwarnradar, das Luftbewegungen in großer
  Entfernung überwacht (AWACS-ähnlich)"
- Statt "C-12V Huron" → "eine C-12V Huron, ein zweimotoriges Propellerflugzeug,
  das die US-Armee für Transport und Verbindungsaufgaben nutzt"

GOLDENE REGEL: Jeder erwähnte Flug muss am Empfänger verankert sein — immer
"unser Empfänger hat X erwischt" oder "die Flugroute führte direkt über uns".
Nicht einfach "Emirates flog nach Dubai".

NÄHE UND DISTANZ — immer beides nennen:
Die Horizontaldistanz allein ist nicht aussagekräftig. Wenn ein Flugzeug in
4.300 m Höhe und 0,2 km horizontaler Entfernung vorbeifliegt, ist es nicht
"zum Greifen nah" — es ist über 4 km entfernt. Beschreibe Nähe immer ehrlich:
- Nenne die Flughöhe nur wenn sie erwähnenswert ist
  (z.B. ungewöhnlich hoch/niedrig, oder um eine Distanzangabe
  richtig einzuordnen)
- "direkt über uns" bedeutet die Flugroute verlief über unserer Position,
  nicht dass das Flugzeug zum Greifen nah war
- Vermeide Übertreibungen wie "man könnte die Nieten zählen"

FORMAT (Telegram HTML, KEIN Markdown):
- <b>fett</b> für Abschnittsüberschriften und Flugzeugnamen
- <i>kursiv</i> für Einschübe und Fun Facts
- Emojis zurückhaltend einsetzen
- Altituden: immer in Metern angeben (feet ÷ 3,281, auf 100 m runden)
- Distanzen: immer in km (Seemeilen × 1,852)
- Bei exotischen Zielen (außerhalb Mitteleuropas): kurze Klammerbemerkung
- WICHTIG: Verwende doppelte Zeilenumbrüche (\\n\\n) zwischen allen Absätzen und
  Abschnitten. Jeder Abschnitt muss mit \\n\\n vom vorherigen getrennt sein.
  Auch innerhalb von Abschnitten: ein Zeilenumbruch (\\n) nach jedem Satz-Block.

STRUKTUR — genau diese vier Abschnitte:

<b>✈️ Highlights des Tages</b>
2-3 Absätze über die interessantesten Flugzeuge (hohe Scores, military, private jets,
exotische Operator, Notfall-Squawks). Ein Absatz pro Highlight. Nur hier: individuelle
Kennzeichen oder Registrierungen nennen.
Nutze das Flaggen-Emoji (aus "flag" Feld, ISO-Code → Emoji, z.B. "DE"→🇩🇪)
vor Flugzeugnamen. Erkläre bei jedem Highlight, was das
Flugzeug ist und warum seine Sichtung hier ungewöhnlich oder erwähnenswert ist.

<b>🌍 Der Überblick</b>
1-2 Absätze über den normalen Verkehr zusammengefasst — KEINE Einzelauflistung.
Beispiel: "Ryanair war wieder fleißigster Gast mit X Flügen, hauptsächlich Richtung
Mittelmeer."

<b>🆕 Neue Gesichter</b>
2-3 der interessantesten Erstbesucher. Falls keine interessanten dabei, ein kurzer Satz.

<b>📊 Fakten des Tages</b>
Genau diese Zeilen mit echten Daten:
✈️ Flüge gesichtet: <total_sightings>
🛬 Verschiedene Flugzeuge: <unique_aircraft>
🆕 Erstbesucher: <new_aircraft>
📏 Weiteste Annäherung: <Flaggen-Emoji> <Typ (Operator)> oder <Reg>, <distance km>
⛰️ Höchster Flug: <Flaggen-Emoji> <Typ (Operator)> oder <Reg>, <altitude m>
Beispiel: "📏 Weiteste Annäherung: 🇮🇪 Pilatus PC-12 (Babcock), 0,2 km"
Jeder Kandidat hat ein "flag" Feld (ISO-Ländercode, z.B. "DE", "US", "AE").
Wandle es in das passende Flaggen-Emoji um (z.B. "DE" → 🇩🇪, "GB" → 🇬🇧).
Nur wenn > 0 anzeigen (je eigene Zeile):
🏥 Sanitätsflüge: <medical_count>
🚔 Polizeiflüge: <police_count>

Falls ein Notfall-Squawk vorhanden: mache ihn zur Eröffnungsgeschichte der Highlights.
Falls ein Kandidat ein photo-Objekt hat: verwende photo_url für das Ausgabefeld und
schreibe eine kurze photo_caption (z.B. "📸 D-ABCD — Airbus A320, Lufthansa").

Die Eingabe ist ein JSON-Objekt mit den Feldern "stats" und "candidates".
""".strip()


# ---------------------------------------------------------------------------
# Public types
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

    def __init__(self, model: str = "gemini-3-flash-preview") -> None:
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
            description="Generates engaging daily flight digests from ADS-B data.",
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
                text = event.content.parts[0].text
                if text is None:
                    continue
                result = _DigestOutputModel.model_validate_json(text)
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
# generate_digest — public function
# ---------------------------------------------------------------------------


async def generate_digest(
    query: DigestQuery,
    chart_query: ChartQuery,
    digest_repo: DigestRepository,
    photo_client: PhotoClient,
    digest_client: DigestClient,
    broadcaster: Broadcaster,
    period_start: datetime,
    period_end: datetime,
    force: bool = False,
) -> None:
    """Generate and broadcast a digest for the given time window.

    1. Check cache (skipped if force=True). Cache key = (period_end.date(), n_days).
    2. Fetch candidates + stats via DigestQuery.
    3. Fetch photos for top candidates.
    4. Call digest_client.generate() — one Gemini call.
    5. Generate traffic chart.
    6. Cache result via digest_repo.
    7. Broadcast to all active users via broadcaster.
    """
    n_days = (period_end - period_start).days
    reference_date = period_end.date()

    # Cache check (skipped when force=True).
    if not force:
        try:
            cached = await digest_repo.get_cached(reference_date, n_days)
        except Exception:
            logger.exception("generate_digest: cache lookup failed")
            cached = None

        if cached is not None:
            logger.info(
                "generate_digest: cache hit for reference_date=%s n_days=%d; "
                "broadcasting cached digest",
                reference_date,
                n_days,
            )
            try:
                chart_png = await _generate_chart(chart_query, n_days)
                await broadcaster.broadcast(cached, chart_png)
            except Exception:
                logger.exception("generate_digest: broadcast of cached digest failed")
            return

    # Fetch candidates and stats.
    try:
        candidates = await query.get_candidates(n_days)
        stats = await query.get_stats(n_days)
    except Exception:
        logger.exception("generate_digest: query failed; skipping digest generation")
        return

    logger.info(
        "generate_digest: fetched %d candidates for n_days=%d", len(candidates), n_days
    )

    candidate_dicts = [dataclasses.asdict(c) for c in candidates]
    stats_dict = dataclasses.asdict(stats)

    # Fetch photos for the top candidates (up to 2).
    photos: dict[str, PhotoInfo] = {}
    for candidate in candidates[:2]:
        try:
            photo = await photo_client.lookup(candidate.hex)
            if photo is not None:
                photos[candidate.hex] = photo
        except Exception:
            logger.warning(
                "generate_digest: photo lookup failed for hex=%s", candidate.hex
            )

    # Generate digest.
    try:
        digest = await digest_client.generate(candidate_dicts, stats_dict, photos)
    except Exception:
        logger.exception("generate_digest: generation failed; skipping")
        return

    # Generate traffic chart.
    chart_png = await _generate_chart(chart_query, n_days)

    # Cache the result.
    try:
        await digest_repo.cache(reference_date, n_days, digest)
    except Exception:
        logger.exception("generate_digest: caching failed; broadcasting anyway")

    # Broadcast.
    try:
        await broadcaster.broadcast(digest, chart_png)
    except Exception:
        logger.exception("generate_digest: broadcast failed")


async def _generate_chart(chart_query: ChartQuery, n_days: int) -> bytes | None:
    try:
        hourly = await chart_query.get_hourly(n_days)
        return render_traffic_chart(hourly)
    except Exception:
        logger.exception("generate_digest: chart generation failed")
        return None
