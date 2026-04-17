"""Configuration from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Config:
    bot_token: str
    gemini_api_key: str
    database_url: str  # shared TimescaleDB (flight data + bot state)
    admin_chat_id: int | None  # chat ID allowed to use /debug
    digest_schedule: str  # cron expression, default Sunday 8am

    @classmethod
    def from_env(cls) -> Config:
        admin = os.environ.get("ADMIN_CHAT_ID")
        return cls(
            bot_token=os.environ["BOT_TOKEN"],
            gemini_api_key=os.environ["GEMINI_API_KEY"],
            database_url=os.environ["DATABASE_URL"],
            admin_chat_id=int(admin) if admin else None,
            digest_schedule=os.environ.get("DIGEST_SCHEDULE", "0 8 * * 0"),
        )
