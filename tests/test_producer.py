import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from src.validation.schema import WeatherData
from producer.weather_producer import (
    fetch_weather,
    validate_weather,
    produce_message
)


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────
@pytest.fixture
def config():
    return {
        "api": {
            "base_url": "http://api.openweathermap.org/data/2.5/weather",
            "units": "metric",
            "timeout_seconds": 10,
            "retry_attempts": 2,
            "retry_delay_seconds": 0
        }
    }


@pytest.fixture
def mock_api_response():
    return {
        "name": "Paris",
        "sys": {"country": "FR"},
        "main": {
            "temp": 23.29,
            "feels_like": 22.85,
            "humidity": 45,
            "pressure": 1012
        },
        "weather": [{"description": "clear sky"}],
        "wind": {"speed": 5.66},
        "visibility": 10000,
        "dt": 1746878400
    }


@pytest.fixture
def valid_weather():
    return WeatherData(
        city="Paris",
        country="FR",
        temperature=23.29,
        feels_like=22.85,
        humidity=45,
        pressure=1012,
        weather_description="clear sky",
        wind_speed=5.66,
        visibility=10000,
        recorded_at=datetime(2026, 5, 12, 14, 0, 0)
    )


# ─────────────────────────────────────────────────────────────
# fetch_weather tests
# ─────────────────────────────────────────────────────────────
class TestFetchWeather:

    @patch("producer.weather_producer.requests.get")
    def test_successful_fetch(self, mock_get, config, mock_api_response):
        """Successful API call should return a properly structured dict."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_weather("Paris", "fake_key", config)

        assert result is not None
        assert result["city"] == "Paris"
        assert result["country"] == "FR"
        assert result["temperature"] == 23.29
        assert result["humidity"] == 45

    @patch("producer.weather_producer.requests.get")
    def test_api_timeout_returns_none(self, mock_get, config):
        """API timeout after all retries should return None."""
        import requests
        mock_get.side_effect = requests.exceptions.Timeout

        result = fetch_weather("Paris", "fake_key", config)
        assert result is None

    @patch("producer.weather_producer.requests.get")
    def test_http_error_returns_none(self, mock_get, config):
        """HTTP error should return None immediately."""
        import requests
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=MagicMock(status_code=404)
        )
        mock_get.return_value = mock_response

        result = fetch_weather("InvalidCity", "fake_key", config)
        assert result is None


# ─────────────────────────────────────────────────────────────
# validate_weather tests
# ─────────────────────────────────────────────────────────────
class TestValidateWeather:

    def test_valid_data_returns_weather_object(self):
        """Valid raw data should return a WeatherData object."""
        raw = {
            "city": "Paris",
            "country": "FR",
            "temperature": 23.29,
            "feels_like": 22.85,
            "humidity": 45,
            "pressure": 1012,
            "weather_description": "clear sky",
            "wind_speed": 5.66,
            "visibility": 10000,
            "recorded_at": datetime(2026, 5, 12, 14, 0, 0)
        }
        result = validate_weather(raw)
        assert result is not None
        assert isinstance(result, WeatherData)

    def test_invalid_temperature_returns_none(self):
        """Data with temperature out of range should return None."""
        raw = {
            "city": "Paris",
            "country": "FR",
            "temperature": 999.0,
            "feels_like": 22.85,
            "humidity": 45,
            "pressure": 1012,
            "weather_description": "clear sky",
            "wind_speed": 5.66,
            "recorded_at": datetime(2026, 5, 12, 14, 0, 0)
        }
        result = validate_weather(raw)
        assert result is None

    def test_missing_required_field_returns_none(self):
        """Data missing a required field should return None."""
        raw = {
            "country": "FR",
            "temperature": 23.29,
            "feels_like": 22.85,
            "humidity": 45,
            "pressure": 1012,
            "weather_description": "clear sky",
            "wind_speed": 5.66,
            "recorded_at": datetime(2026, 5, 12, 14, 0, 0)
        }
        result = validate_weather(raw)
        assert result is None


# ─────────────────────────────────────────────────────────────
# produce_message tests
# ─────────────────────────────────────────────────────────────
class TestProduceMessage:

    def test_successful_produce_returns_true(self, valid_weather):
        """Successful Kafka produce should return True."""
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.partition = 0
        mock_metadata.offset = 42
        mock_future.get.return_value = mock_metadata
        mock_producer.send.return_value = mock_future

        result = produce_message(
            mock_producer, "weather_stream", valid_weather, "weather_stream_dlq"
        )
        assert result is True
        mock_producer.send.assert_called_once()

    def test_kafka_error_returns_false(self, valid_weather):
        """Kafka error during produce should return False."""
        from kafka.errors import KafkaError
        mock_producer = MagicMock()
        mock_producer.send.side_effect = KafkaError("Broker unavailable")

        result = produce_message(
            mock_producer, "weather_stream", valid_weather, "weather_stream_dlq"
        )
        assert result is False