import json
import time
import os
import requests
import yaml
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

from src.utils.logger import get_logger
from src.validation.schema import WeatherData

load_dotenv()
logger = get_logger(__name__)


def load_config() -> dict:
    """Load configuration from config.yaml."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Create and return a KafkaProducer with JSON serialization.
    acks='all' ensures the message is written to all replicas
    before being acknowledged — maximum durability.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3,
        max_block_ms=10000
    )


def fetch_weather(city: str, api_key: str, config: dict) -> dict | None:
    """
    Fetch weather data from OpenWeatherMap API for a single city.
    Returns None on failure after all retries are exhausted.
    """
    base_url = config['api']['base_url']
    timeout = config['api']['timeout_seconds']
    retries = config['api']['retry_attempts']
    delay = config['api']['retry_delay_seconds']

    params = {
        "q": city,
        "appid": api_key,
        "units": config['api']['units']
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(base_url, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            return {
                "city": data["name"],
                "country": data["sys"]["country"],
                "temperature": data["main"]["temp"],
                "feels_like": data["main"]["feels_like"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "weather_description": data["weather"][0]["description"],
                "wind_speed": data["wind"]["speed"],
                "visibility": data.get("visibility", 0),
                "recorded_at": datetime.utcfromtimestamp(data["dt"]).isoformat()
            }

        except requests.exceptions.Timeout:
            logger.warning(f"Attempt {attempt}/{retries} — Timeout fetching {city}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for {city}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {city}: {e}")

        if attempt < retries:
            logger.info(f"Retrying {city} in {delay}s...")
            time.sleep(delay)

    logger.error(f"All {retries} attempts failed for {city}. Skipping.")
    return None


def validate_weather(raw: dict) -> WeatherData | None:
    """
    Validate raw API data against the Pydantic schema.
    Returns None if validation fails — data goes to DLQ.
    """
    try:
        return WeatherData(**raw)
    except Exception as e:
        logger.error(f"Validation failed for {raw.get('city', 'unknown')}: {e}")
        return None


def produce_message(
    producer: KafkaProducer,
    topic: str,
    weather: WeatherData,
    dlq_topic: str
) -> bool:
    """
    Send a validated weather record to the Kafka topic.
    Uses city as the message key for partition consistency —
    all messages for the same city go to the same partition.
    On failure, sends to the dead letter queue topic.
    """
    try:
        future = producer.send(
            topic=topic,
            key=weather.city,
            value=weather.model_dump()
        )
        record_metadata = future.get(timeout=10)
        logger.info(
            f"Produced | {weather.city} | "
            f"partition={record_metadata.partition} | "
            f"offset={record_metadata.offset} | "
            f"temp={weather.temperature}°C | "
            f"{weather.weather_description}"
        )
        return True

    except KafkaError as e:
        logger.error(f"Failed to produce message for {weather.city}: {e}")
        # Send to dead letter queue
        try:
            producer.send(
                topic=dlq_topic,
                value={
                    "city": weather.city,
                    "error": str(e),
                    "failed_at": datetime.utcnow().isoformat()
                }
            )
            logger.warning(f"Sent {weather.city} to DLQ: {dlq_topic}")
        except Exception as dlq_error:
            logger.critical(f"Failed to send to DLQ: {dlq_error}")
        return False


def run_producer():
    """
    Main producer loop.
    Fetches weather data for all configured cities,
    validates each record, and streams to Kafka.
    Runs continuously with a configurable poll interval.
    """
    config = load_config()
    api_key = os.getenv("OPENWEATHER_API_KEY")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = config['kafka']['topic']
    dlq_topic = config['kafka']['dead_letter_topic']
    poll_interval = config['producer']['poll_interval_seconds']
    cities = [c['name'] for c in config['api']['cities']]

    if not api_key:
        logger.critical("OPENWEATHER_API_KEY not set. Exiting.")
        return

    logger.info(f"Starting producer | topic={topic} | cities={cities}")
    producer = create_producer(bootstrap_servers)

    try:
        while True:
            logger.info(f"--- Poll cycle started | {datetime.utcnow().isoformat()} ---")
            success_count = 0
            fail_count = 0

            for city in cities:
                raw = fetch_weather(city, api_key, config)

                if raw is None:
                    fail_count += 1
                    continue

                weather = validate_weather(raw)

                if weather is None:
                    fail_count += 1
                    continue

                produced = produce_message(producer, topic, weather, dlq_topic)
                if produced:
                    success_count += 1
                else:
                    fail_count += 1

            logger.info(
                f"--- Poll cycle complete | "
                f"success={success_count} | "
                f"failed={fail_count} | "
                f"next poll in {poll_interval}s ---"
            )
            time.sleep(poll_interval)

    except KeyboardInterrupt:
        logger.info("Producer shutting down gracefully...")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    run_producer()