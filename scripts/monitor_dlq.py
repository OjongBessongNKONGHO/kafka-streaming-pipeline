"""
Dead Letter Queue monitoring script.

Provides a point-in-time snapshot of the DLQ — how many messages are
waiting, what error types caused them, which cities are affected, and
how long the oldest message has been sitting unresolved.

Run this script on a schedule (e.g. every 15 minutes via cron or Airflow)
to detect DLQ accumulation before it becomes a production incident.
A growing DLQ means the live consumer is consistently failing on certain
message types — the sooner you catch it, the less data you lose.

Unlike reprocess_dlq.py which modifies data, this script is read-only.
It never deletes or modifies DLQ rows — safe to run at any time.

Usage:
    python -m scripts.monitor_dlq
"""

import logging
import sys
from datetime import datetime, UTC
from sqlalchemy import text
from consumer.processor import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def fetch_dlq_summary(engine) -> dict:
    """
    Fetch aggregated DLQ statistics in a single query.

    Returns total message count, breakdown by error type, breakdown
    by affected city, and the age of the oldest unresolved message.
    """
    with engine.connect() as conn:
        # Overall count and oldest message age
        summary = conn.execute(text("""
            SELECT
                COUNT(*)                          AS total,
                MIN(failed_at)                    AS oldest_failure,
                MAX(failed_at)                    AS latest_failure
            FROM weather_events_dlq
        """)).fetchone()

        # Breakdown by error type
        by_error = conn.execute(text("""
            SELECT error_type, COUNT(*) AS count
            FROM weather_events_dlq
            GROUP BY error_type
            ORDER BY count DESC
        """)).fetchall()

        # Breakdown by affected city
        by_city = conn.execute(text("""
            SELECT
                raw_message::json->>'city' AS city,
                COUNT(*) AS count
            FROM weather_events_dlq
            GROUP BY raw_message::json->>'city'
            ORDER BY count DESC
            LIMIT 10
        """)).fetchall()

    return {
        "total": summary.total if summary else 0,
        "oldest_failure": summary.oldest_failure if summary else None,
        "latest_failure": summary.latest_failure if summary else None,
        "by_error": [{"error_type": r.error_type, "count": r.count} for r in by_error],
        "by_city": [{"city": r.city, "count": r.count} for r in by_city],
    }


def compute_age_minutes(timestamp: datetime | None) -> float | None:
    """Return how many minutes ago a timestamp occurred."""
    if timestamp is None:
        return None
    now = datetime.now(UTC).replace(tzinfo=None)
    return round((now - timestamp).total_seconds() / 60, 1)


def monitor_dlq() -> None:
    """
    Print a structured DLQ health report to stdout.

    Exit code 0 — DLQ is empty, pipeline is healthy.
    Exit code 1 — DLQ contains messages, investigation required.
    """
    engine = get_engine()
    summary = fetch_dlq_summary(engine)
    total = summary["total"]
    now_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

    logger.info("DLQ monitor report — %s", now_str)
    logger.info("Total messages in DLQ: %d", total)

    if total == 0:
        logger.info("DLQ is empty — pipeline is healthy.")
        sys.exit(0)

    # Oldest message age
    age_minutes = compute_age_minutes(summary["oldest_failure"])
    if age_minutes is not None:
        logger.warning(
            "Oldest unresolved message: %s (%.1f minutes ago)",
            summary["oldest_failure"],
            age_minutes,
        )

    # Latest failure
    logger.warning("Most recent failure: %s", summary["latest_failure"])

    # Breakdown by error type
    logger.warning("Breakdown by error type:")
    for entry in summary["by_error"]:
        logger.warning("  %-40s %d message(s)", entry["error_type"], entry["count"])

    # Breakdown by city
    logger.warning("Breakdown by affected city (top 10):")
    for entry in summary["by_city"]:
        logger.warning("  %-20s %d message(s)", entry["city"] or "unknown", entry["count"])

    logger.warning(
        "Action required — run python -m scripts.reprocess_dlq to attempt reprocessing."
    )

    sys.exit(1)


if __name__ == "__main__":
    monitor_dlq()