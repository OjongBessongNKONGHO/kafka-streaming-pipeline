import os
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from src.utils.logger import get_logger
from src.validation.schema import WeatherData, WeatherDataDB

load_dotenv()
logger = get_logger(__name__)


def get_engine():
    """
    Create and return a SQLAlchemy engine for weather_streaming DB.
    Connection pool is configured for stability under continuous load.
    """
    user = os.getenv("POSTGRES_USER", "streaming_user")
    password = os.getenv("POSTGRES_PASSWORD", "streaming_pass")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "weather_streaming")

    conn_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

    return create_engine(
        conn_string,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True  # Verifies connection is alive before using it
    )


def process_message(
    raw_message: dict,
    kafka_offset: int,
    kafka_partition: int,
    engine
) -> bool:
    """
    Validate, transform and persist a single Kafka message to PostgreSQL.

    Args:
        raw_message: Deserialized JSON from Kafka
        kafka_offset: Kafka message offset for traceability
        kafka_partition: Kafka partition for traceability
        engine: SQLAlchemy engine

    Returns:
        True if successfully processed, False if failed
    """
    city = raw_message.get("city", "unknown")

    # ── Step 1: Validate against Pydantic schema ──────────────
    try:
        weather = WeatherDataDB(**raw_message)
    except Exception as e:
        logger.error(f"Validation failed | city={city} | error={e}")
        return False

    # ── Step 2: Insert into PostgreSQL ────────────────────────
    try:
        with engine.begin() as conn:

            # Check for duplicate — same city and recorded_at
            check = text("""
                SELECT 1 FROM weather_events
                WHERE city = :city AND recorded_at = :recorded_at
            """)
            exists = conn.execute(check, {
                "city": weather.city,
                "recorded_at": weather.recorded_at
            }).fetchone()

            if exists:
                logger.info(f"Duplicate skipped | city={weather.city} | recorded_at={weather.recorded_at}")
                return True

            # Insert new record
            insert = text("""
                INSERT INTO weather_events (
                    city, country, temperature, feels_like,
                    humidity, pressure, weather_description,
                    wind_speed, visibility, recorded_at,
                    inserted_at, kafka_offset, kafka_partition
                ) VALUES (
                    :city, :country, :temperature, :feels_like,
                    :humidity, :pressure, :weather_description,
                    :wind_speed, :visibility, :recorded_at,
                    :inserted_at, :kafka_offset, :kafka_partition
                )
            """)

            conn.execute(insert, {
                "city": weather.city,
                "country": weather.country,
                "temperature": weather.temperature,
                "feels_like": weather.feels_like,
                "humidity": weather.humidity,
                "pressure": weather.pressure,
                "weather_description": weather.weather_description,
                "wind_speed": weather.wind_speed,
                "visibility": weather.visibility,
                "recorded_at": weather.recorded_at,
                "inserted_at": weather.inserted_at,
                "kafka_offset": kafka_offset,
                "kafka_partition": kafka_partition
            })

        logger.info(
            f"Persisted | city={weather.city} | "
            f"temp={weather.temperature}°C | "
            f"offset={kafka_offset} | "
            f"partition={kafka_partition}"
        )
        return True

    except Exception as e:
        logger.error(f"DB error | city={city} | error={e}")
        return False


def save_to_dlq(raw_message: str, error_type: str, error_detail: str,
                kafka_topic: str, kafka_offset: int, engine) -> None:
    """
    Save a failed message to the Dead Letter Queue table.
    Ensures no message is ever lost — failed records can be
    investigated and reprocessed later.
    """
    try:
        with engine.begin() as conn:
            insert = text("""
                INSERT INTO weather_events_dlq (
                    raw_message, error_type, error_detail,
                    kafka_topic, kafka_offset, failed_at
                ) VALUES (
                    :raw_message, :error_type, :error_detail,
                    :kafka_topic, :kafka_offset, :failed_at
                )
            """)
            conn.execute(insert, {
                "raw_message": raw_message,
                "error_type": error_type,
                "error_detail": error_detail,
                "kafka_topic": kafka_topic,
                "kafka_offset": kafka_offset,
                "failed_at": datetime.utcnow()
            })
        logger.warning(f"Saved to DLQ | offset={kafka_offset} | error={error_type}")
    except Exception as e:
        logger.critical(f"Failed to save to DLQ: {e}")