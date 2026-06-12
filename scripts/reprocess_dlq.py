"""
Dead Letter Queue reprocessing script.

Reads failed messages from the weather_events_dlq table, attempts to
reprocess each one through the same validation and insertion logic used
by the live consumer (process_message), and removes successfully
reprocessed rows from the DLQ.

The DLQ is treated as a queue of unresolved failures: once a message is
successfully reprocessed, its data lives in weather_events (with the
original kafka_offset preserved for traceability) and the DLQ row is
deleted. Messages that fail again remain in the DLQ for further
investigation.

Usage:
    python -m scripts.reprocess_dlq
"""

import json
from sqlalchemy import text

from src.utils.logger import get_logger
from consumer.processor import get_engine, process_message

logger = get_logger(__name__)


def fetch_dlq_rows(engine) -> list[dict]:
    """Fetch all rows currently in the dead letter queue."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, raw_message, error_type, error_detail,
                   kafka_topic, kafka_offset, failed_at
            FROM weather_events_dlq
            ORDER BY failed_at ASC
        """))
        return [dict(row._mapping) for row in result]


def delete_dlq_row(engine, dlq_id: int) -> None:
    """Remove a successfully reprocessed row from the DLQ."""
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM weather_events_dlq WHERE id = :id"),
            {"id": dlq_id}
        )


def reprocess_dlq() -> None:
    """
    Attempt to reprocess every message currently in the DLQ.

    Each message is re-validated and re-inserted using the same
    process_message() function the live consumer uses, so a message
    that failed due to a transient issue (e.g. a DB outage at the time)
    can succeed now without any duplicated logic.
    """
    engine = get_engine()
    rows = fetch_dlq_rows(engine)

    if not rows:
        logger.info("DLQ is empty — nothing to reprocess.")
        return

    logger.info(f"Found {len(rows)} message(s) in DLQ — starting reprocessing.")

    succeeded = 0
    still_failing = 0

    for row in rows:
        dlq_id = row["id"]
        kafka_offset = row["kafka_offset"]
        kafka_topic = row["kafka_topic"]

        try:
            raw_message = json.loads(row["raw_message"])
        except json.JSONDecodeError as e:
            logger.error(
                f"DLQ id={dlq_id} | unparseable raw_message, skipping | error={e}"
            )
            still_failing += 1
            continue

        city = raw_message.get("city", "unknown")

        success = process_message(
            raw_message=raw_message,
            kafka_offset=kafka_offset,
            kafka_partition=0,  # original partition not stored in DLQ
            engine=engine
        )

        if success:
            delete_dlq_row(engine, dlq_id)
            succeeded += 1
            logger.info(
                f"Reprocessed successfully | dlq_id={dlq_id} | city={city} | "
                f"original_error={row['error_type']} | "
                f"originally_failed_at={row['failed_at']} | "
                f"kafka_topic={kafka_topic} | kafka_offset={kafka_offset} | "
                f"removed from DLQ"
            )
        else:
            still_failing += 1
            logger.warning(
                f"Reprocessing failed again | dlq_id={dlq_id} | city={city} | "
                f"original_error={row['error_type']} | remains in DLQ"
            )

    logger.info(
        f"Reprocessing complete | succeeded={succeeded} | "
        f"still_failing={still_failing} | total={len(rows)}"
    )


if __name__ == "__main__":
    reprocess_dlq()