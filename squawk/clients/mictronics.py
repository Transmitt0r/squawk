"""Mictronics aircraft database bulk downloader.

Downloads https://www.mictronics.de/aircraft-database/aircraft_db.php (ZIP,
~daily updates) and ingests into the bulk_aircraft table.

The ZIP contains JSON files named by ICAO hex prefix (e.g. "48.json").
Within each file, keys are hex suffixes so the full hex = prefix + suffix.
Values: {"r": registration, "t": ICAO type code, "desc": model description}

Public API:
    download_and_ingest(session, repo, url) -> int
"""

from __future__ import annotations

import io
import json
import logging
import zipfile

import aiohttp

from squawk.repositories.bulk_aircraft import BulkAircraftRepository

logger = logging.getLogger(__name__)

_DEFAULT_URL = "https://www.mictronics.de/aircraft-database/aircraft_db.php"
_TIMEOUT = aiohttp.ClientTimeout(total=120)
_BATCH_SIZE = 5000
# Non-aircraft entries to skip
_SKIP_TYPES = frozenset({"TWR", "SERV", "GND", "MLAT", ""})


async def download_and_ingest(
    session: aiohttp.ClientSession,
    repo: BulkAircraftRepository,
    url: str = _DEFAULT_URL,
) -> int:
    """Download the mictronics aircraft DB ZIP and ingest into bulk_aircraft.

    Truncates the existing bulk_aircraft table before inserting so stale
    entries are not kept. Returns number of records ingested.
    """
    logger.info("mictronics: downloading from %s", url)
    async with session.get(url, timeout=_TIMEOUT) as resp:
        resp.raise_for_status()
        content = await resp.read()

    logger.info("mictronics: downloaded %.1f MB, parsing...", len(content) / 1_000_000)

    records: list[tuple[str, str | None, str | None, str | None]] = []
    total = 0

    await repo.prepare_ingest()

    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = [n for n in zf.namelist() if n.endswith(".json")]
        for name in names:
            prefix = name[: -len(".json")].upper()
            try:
                data: dict = json.loads(zf.read(name))
            except (json.JSONDecodeError, KeyError):
                logger.warning("mictronics: failed to parse %s, skipping", name)
                continue
            if not isinstance(data, dict):
                continue
            for suffix, entry in data.items():
                if not isinstance(entry, dict):
                    continue
                icao_type = entry.get("t") or None
                reg = entry.get("r") or None
                # Skip ground vehicles, towers, and placeholder entries
                if icao_type in _SKIP_TYPES or reg in _SKIP_TYPES:
                    continue
                full_hex = (prefix + suffix).lower()
                model = entry.get("desc") or None
                records.append((full_hex, reg, icao_type, model))
                if len(records) >= _BATCH_SIZE:
                    await repo.insert_batch_staging(records)
                    total += len(records)
                    records.clear()

    if records:
        await repo.insert_batch_staging(records)
        total += len(records)

    await repo.commit_ingest()

    logger.info("mictronics: ingested %d records", total)
    return total
