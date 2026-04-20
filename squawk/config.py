"""Application configuration.

All environment variables are read once at startup via Config.from_env().
No other module reads os.environ directly.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta


@dataclass(frozen=True)
class Config:
    # Required — no defaults
    adsb_url: str
    database_url: str
    bot_token: str
    gemini_api_key: str
    admin_chat_id: int
    channel_id: int

    radar_url: str = "https://radar.grotz.io/"

    # Optional with defaults
    poll_interval: float = 5.0
    session_timeout: float = 300.0
    digest_schedule: str = "15 7 * * *"
    enrichment_ttl: timedelta = field(default_factory=lambda: timedelta(days=30))
    enrichment_batch_size: int = 20
    enrichment_flush_interval: float = 30.0
    client_max_retries: int = 3
    adsbdb_url: str = "https://api.adsbdb.com/v0"
    hexdb_url: str = "https://hexdb.io/api/v1"
    mictronics_url: str = "https://www.mictronics.de/aircraft-database/aircraft_db.php"
    planespotters_url: str = "https://api.planespotters.net/pub"
    routes_url: str = "https://api.adsbdb.com/v0"

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> Config:
        return cls(
            adsb_url=env["ADSB_URL"],
            database_url=env["DATABASE_URL"],
            bot_token=env["BOT_TOKEN"],
            gemini_api_key=env["GEMINI_API_KEY"],
            admin_chat_id=int(env["ADMIN_CHAT_ID"]),
            channel_id=int(env["CHANNEL_ID"]),
            poll_interval=float(env.get("POLL_INTERVAL", "5")),
            session_timeout=float(env.get("SESSION_TIMEOUT", "300")),
            digest_schedule=env.get("DIGEST_SCHEDULE", "15 7 * * *"),
            enrichment_ttl=timedelta(days=int(env.get("ENRICHMENT_TTL_DAYS", "30"))),
            enrichment_batch_size=int(env.get("ENRICHMENT_BATCH_SIZE", "20")),
            enrichment_flush_interval=float(env.get("ENRICHMENT_FLUSH_SECS", "30")),
            client_max_retries=int(env.get("CLIENT_MAX_RETRIES", "3")),
            adsbdb_url=env.get("ADSBDB_URL", "https://api.adsbdb.com/v0"),
            radar_url=env.get("RADAR_URL", "https://radar.grotz.io/"),
            hexdb_url=env.get("HEXDB_URL", "https://hexdb.io/api/v1"),
            mictronics_url=env.get(
                "MICTRONICS_URL",
                "https://www.mictronics.de/aircraft-database/aircraft_db.php",
            ),
            planespotters_url=env.get(
                "PLANESPOTTERS_URL", "https://api.planespotters.net/pub"
            ),
            routes_url=env.get("ROUTES_URL", "https://api.adsbdb.com/v0"),
        )
