import os
import sys
import requests
import yaml
from datetime import datetime
from kafka import KafkaAdminClient
from kafka.errors import KafkaError
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from src.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


def load_config() -> dict:
    """Load configuration from config.yaml."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def check_kafka(bootstrap_servers: str) -> dict:
    """
    Verify Kafka broker is reachable and responsive.
    Returns status dict with result and latency.
    """
    start = datetime.utcnow()
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="health_check",
            request_timeout_ms=5000
        )
        topics = admin.list_topics()
        admin.close()
        latency = (datetime.utcnow() - start).total_seconds() * 1000
        return {
            "status": "healthy",
            "latency_ms": round(latency, 2),
            "topics": topics
        }
    except KafkaError as e:
        return {"status": "unhealthy", "error": str(e)}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def check_database(engine) -> dict:
    """
    Verify PostgreSQL is reachable and the tables exist.
    Returns status dict with record counts.
    """
    start = datetime.utcnow()
    try:
        with engine.connect() as conn:
            # Check connection
            conn.execute(text("SELECT 1"))

            # Count records in main table
            result = conn.execute(
                text("SELECT COUNT(*) FROM weather_events")
            )
            event_count = result.scalar()

            # Count records in DLQ table
            dlq_result = conn.execute(
                text("SELECT COUNT(*) FROM weather_events_dlq")
            )
            dlq_count = dlq_result.scalar()

            latency = (datetime.utcnow() - start).total_seconds() * 1000
            return {
                "status": "healthy",
                "latency_ms": round(latency, 2),
                "weather_events_count": event_count,
                "dlq_count": dlq_count
            }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def check_api(api_key: str, config: dict) -> dict:
    """
    Verify OpenWeatherMap API is reachable and the key is valid.
    Uses Paris as a lightweight test city.
    """
    start = datetime.utcnow()
    try:
        response = requests.get(
            config['api']['base_url'],
            params={
                "q": "Paris",
                "appid": api_key,
                "units": config['api']['units']
            },
            timeout=config['api']['timeout_seconds']
        )
        response.raise_for_status()
        latency = (datetime.utcnow() - start).total_seconds() * 1000
        return {
            "status": "healthy",
            "latency_ms": round(latency, 2),
            "http_status": response.status_code
        }
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            return {"status": "unhealthy", "error": "Invalid API key"}
        return {"status": "unhealthy", "error": str(e)}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def run_health_check():
    """
    Run all health checks and print a full system status report.
    Exits with code 1 if any component is unhealthy.
    """
    config = load_config()
    api_key = os.getenv("OPENWEATHER_API_KEY")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    user = os.getenv("POSTGRES_USER", "streaming_user")
    password = os.getenv("POSTGRES_PASSWORD", "streaming_pass")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "weather_streaming")
    conn_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(conn_string)

    logger.info("=" * 60)
    logger.info("WEATHER STREAMING PIPELINE — HEALTH CHECK")
    logger.info(f"Timestamp: {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)

    # Run all checks
    kafka_status = check_kafka(bootstrap_servers)
    db_status = check_database(engine)
    api_status = check_api(api_key, config)

    # Report results
    logger.info(f"Kafka Broker    : {kafka_status['status'].upper()}"
                + (f" | latency={kafka_status.get('latency_ms')}ms" if kafka_status['status'] == 'healthy' else f" | error={kafka_status.get('error')}"))

    logger.info(f"PostgreSQL      : {db_status['status'].upper()}"
                + (f" | latency={db_status.get('latency_ms')}ms | records={db_status.get('weather_events_count')} | dlq={db_status.get('dlq_count')}" if db_status['status'] == 'healthy' else f" | error={db_status.get('error')}"))

    logger.info(f"OpenWeatherMap  : {api_status['status'].upper()}"
                + (f" | latency={api_status.get('latency_ms')}ms" if api_status['status'] == 'healthy' else f" | error={api_status.get('error')}"))

    logger.info("=" * 60)

    # Exit with error code if anything is unhealthy
    all_healthy = all(
        s['status'] == 'healthy'
        for s in [kafka_status, db_status, api_status]
    )

    if not all_healthy:
        logger.error("One or more components are UNHEALTHY.")
        sys.exit(1)
    else:
        logger.info("All systems HEALTHY. Pipeline is ready.")
        sys.exit(0)


if __name__ == "__main__":
    run_health_check()