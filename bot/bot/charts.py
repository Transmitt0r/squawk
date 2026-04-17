"""Generate flight traffic charts for the weekly digest."""

from __future__ import annotations

import io
import logging
from datetime import datetime, timezone

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import psycopg2

matplotlib.use("Agg")  # headless, no display needed

logger = logging.getLogger(__name__)


def generate_traffic_chart(database_url: str, days: int = 7) -> bytes | None:
    """
    Generate a two-panel PNG chart:
      - Top: bar chart of flights per day
      - Bottom: line chart of flights per hour of day (aggregated across the period)

    Returns PNG bytes, or None on failure.
    """
    try:
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                # Flights per day (in local time Europe/Berlin)
                cur.execute("""
                    SELECT
                        DATE(started_at AT TIME ZONE 'Europe/Berlin') AS day,
                        COUNT(*) AS flights
                    FROM sightings
                    WHERE started_at > now() - (%(days)s || ' days')::interval
                    GROUP BY day
                    ORDER BY day
                """, {"days": days})
                daily = cur.fetchall()

                # Flights per hour of day (aggregated, local time)
                cur.execute("""
                    SELECT
                        EXTRACT(HOUR FROM started_at AT TIME ZONE 'Europe/Berlin')::int AS hour,
                        COUNT(*) AS flights
                    FROM sightings
                    WHERE started_at > now() - (%(days)s || ' days')::interval
                    GROUP BY hour
                    ORDER BY hour
                """, {"days": days})
                hourly = cur.fetchall()

        if not daily and not hourly:
            return None

        # --- build figure (xkcd style!) ---
        with plt.xkcd():
            fig, (ax_day, ax_hour) = plt.subplots(2, 1, figsize=(7, 5))
            fig.subplots_adjust(hspace=0.55)

            # --- daily bars ---
            if daily:
                days_x = [row[0] for row in daily]
                counts = [row[1] for row in daily]
                ax_day.bar(days_x, counts, color="steelblue", width=0.6, zorder=3)
                ax_day.xaxis.set_major_formatter(mdates.DateFormatter("%-d. %b"))
                ax_day.xaxis.set_major_locator(mdates.DayLocator())
                plt.setp(ax_day.xaxis.get_majorticklabels(), rotation=30, ha="right")
                ax_day.set_title("Flüge pro Tag")
                ax_day.set_ylabel("Flüge")

            # --- hourly curve ---
            if hourly:
                hourly_map = {row[0]: row[1] for row in hourly}
                hours = list(range(24))
                counts_h = [hourly_map.get(h, 0) for h in hours]
                ax_hour.plot(hours, counts_h, color="steelblue", linewidth=2, zorder=3)
                ax_hour.fill_between(hours, counts_h, alpha=0.15, color="steelblue")
                ax_hour.set_xticks(range(0, 24, 3))
                ax_hour.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 3)])
                ax_hour.set_title("Flüge nach Uhrzeit")
                ax_hour.set_ylabel("Flüge")
                ax_hour.set_xlim(0, 23)

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    except Exception:
        logger.exception("Failed to generate traffic chart")
        return None
