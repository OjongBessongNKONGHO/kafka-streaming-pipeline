"""
Async concurrent weather fetching for the Kafka streaming pipeline.
Replaces the sequential city-by-city fetch with concurrent requests
using asyncio and aiohttp — all 12 cities fetched simultaneously.

Performance improvement: sequential fetch takes ~12 * API_latency.
Concurrent fetch takes ~1 * API_latency regardless of city count.
"""

import asyncio
import aiohttp
from datetime import datetime, timezone
from src.utils.logger import get_logger

logger = get_logger(__name__)


async def fetch_city_async(
    session: aiohttp.ClientSession,
    city: str,
    api_key: str,
    config: dict,
) -> dict | None:
    """
    Fetch weather data for a single city asynchronously.
    Uses aiohttp instead of requests — non-blocking HTTP call.
    Returns None on failure so other cities are not affected.
    """
    base_url = config["api"]["base_url"]
    timeout = aiohttp.ClientTimeout(total=config["api"]["timeout_seconds"])
    retries = config["api"]["retry_attempts"]
    delay = config["api"]["retry_delay_seconds"]

    params = {
        "q": city,
        "appid": api_key,
        "units": config["api"]["units"],
    }

    for attempt in range(1, retries + 1):
        try:
            async with session.get(base_url, params=params, timeout=timeout) as response:
                response.raise_for_status()
                data = await response.json()

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
                    "recorded_at": datetime.fromtimestamp(data["dt"], tz=timezone.utc).isoformat(),
                }

        except aiohttp.ClientResponseError as e:
            logger.error(f"HTTP error for {city}: {e}")
            return None
        except asyncio.TimeoutError:
            logger.warning(f"Attempt {attempt}/{retries} — Timeout fetching {city}")
        except Exception as e:
            logger.error(f"Unexpected error fetching {city}: {e}")

        if attempt < retries:
            logger.info(f"Retrying {city} in {delay}s...")
            await asyncio.sleep(delay)

    logger.error(f"All {retries} attempts failed for {city}. Skipping.")
    return None


async def fetch_all_cities_async(
    cities: list[str],
    api_key: str,
    config: dict,
) -> list[dict]:
    """
    Fetch weather data for all cities concurrently.
    Creates one aiohttp session shared across all requests —
    more efficient than one session per request.
    Returns only successful results — failed cities are skipped.
    """
    async with aiohttp.ClientSession() as session:
        # Fire all requests simultaneously
        tasks = [
            fetch_city_async(session, city, api_key, config)
            for city in cities
        ]
        # Wait for all to complete — return_exceptions=True means
        # one failure does not cancel the others
        results = await asyncio.gather(*tasks, return_exceptions=True)

    successful = []
    for city, result in zip(cities, results):
        if isinstance(result, Exception):
            logger.error(f"Exception fetching {city}: {result}")
        elif result is not None:
            successful.append(result)

    logger.info(
        f"Async fetch complete — "
        f"{len(successful)}/{len(cities)} cities successful"
    )
    return successful


def fetch_all_cities(
    cities: list[str],
    api_key: str,
    config: dict,
) -> list[dict]:
    """
    Synchronous wrapper around the async fetch function.
    Allows the existing synchronous producer loop to use
    async fetching without changing its structure.
    Call this instead of the sequential fetch loop.
    """
    return asyncio.run(fetch_all_cities_async(cities, api_key, config))