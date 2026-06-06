"""
Tests for the async concurrent weather fetching module.
Uses pytest-asyncio and aiohttp mock to test without
making real API calls.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from producer.async_fetch import (
    fetch_city_async,
    fetch_all_cities_async,
    fetch_all_cities,
)

# Sample config matching config.yaml structure
MOCK_CONFIG = {
    "api": {
        "base_url": "http://api.openweathermap.org/data/2.5/weather",
        "timeout_seconds": 10,
        "retry_attempts": 3,
        "retry_delay_seconds": 1,
        "units": "metric",
    }
}

# Sample API response matching OpenWeatherMap format
MOCK_API_RESPONSE = {
    "name": "Paris",
    "sys": {"country": "FR"},
    "main": {
        "temp": 14.5,
        "feels_like": 13.2,
        "humidity": 72,
        "pressure": 1012,
    },
    "weather": [{"description": "scattered clouds"}],
    "wind": {"speed": 3.5},
    "visibility": 10000,
    "dt": 1717200000,
}


@pytest.fixture
def mock_session():
    """Create a mock aiohttp ClientSession."""
    session = MagicMock()
    mock_response = AsyncMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json = AsyncMock(return_value=MOCK_API_RESPONSE)
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=None)
    session.get = MagicMock(return_value=mock_response)
    return session


@pytest.mark.asyncio
async def test_fetch_city_async_success(mock_session):
    """fetch_city_async returns correct data on success."""
    result = await fetch_city_async(mock_session, "Paris", "test_key", MOCK_CONFIG)

    assert result is not None
    assert result["city"] == "Paris"
    assert result["country"] == "FR"
    assert result["temperature"] == 14.5
    assert result["humidity"] == 72
    assert "recorded_at" in result


@pytest.mark.asyncio
async def test_fetch_city_async_http_error(mock_session):
    """fetch_city_async returns None on HTTP error."""
    import aiohttp
    mock_response = AsyncMock()
    mock_response.raise_for_status = MagicMock(
        side_effect=aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=404
        )
    )
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=None)
    mock_session.get = MagicMock(return_value=mock_response)

    result = await fetch_city_async(mock_session, "InvalidCity", "test_key", MOCK_CONFIG)
    assert result is None


@pytest.mark.asyncio
async def test_fetch_all_cities_async_success(mock_session):
    """fetch_all_cities_async returns results for all cities."""
    cities = ["Paris", "London", "Tokyo"]

    with patch("producer.async_fetch.aiohttp.ClientSession") as mock_cs:
        mock_cs.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_cs.return_value.__aexit__ = AsyncMock(return_value=None)

        results = await fetch_all_cities_async(cities, "test_key", MOCK_CONFIG)

    assert len(results) == 3
    for result in results:
        assert result["city"] == "Paris"  # all return mock Paris data


@pytest.mark.asyncio
async def test_fetch_all_cities_async_partial_failure():
    """fetch_all_cities_async skips None results and returns only successful ones."""
    cities = ["Paris", "London"]

    async def mock_fetch(session, city, api_key, config):
        if city == "London":
            return None
        return {"city": "Paris", "temperature": 14.5}

    with patch("producer.async_fetch.fetch_city_async", side_effect=mock_fetch):
        with patch("producer.async_fetch.aiohttp.ClientSession") as mock_cs:
            mock_session = AsyncMock()
            mock_cs.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_cs.return_value.__aexit__ = AsyncMock(return_value=None)
            results = await fetch_all_cities_async(cities, "test_key", MOCK_CONFIG)

    assert len(results) == 1
    assert results[0]["city"] == "Paris"


def test_fetch_all_cities_sync_wrapper():
    """fetch_all_cities synchronous wrapper returns list of results."""
    cities = ["Paris", "London"]

    with patch("producer.async_fetch.asyncio.run") as mock_run:
        mock_run.return_value = [{"city": "Paris"}, {"city": "London"}]
        results = fetch_all_cities(cities, "test_key", MOCK_CONFIG)

    assert len(results) == 2
    mock_run.assert_called_once()