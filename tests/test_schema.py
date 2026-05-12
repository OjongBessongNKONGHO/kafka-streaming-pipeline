import pytest
from datetime import datetime
from pydantic import ValidationError
from src.validation.schema import WeatherData, WeatherDataDB


# ─────────────────────────────────────────────────────────────
# Valid data fixture — reused across multiple tests
# ─────────────────────────────────────────────────────────────
@pytest.fixture
def valid_weather_data():
    return {
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


# ─────────────────────────────────────────────────────────────
# Happy path tests — valid data should pass
# ─────────────────────────────────────────────────────────────
class TestWeatherDataValid:

    def test_valid_data_passes(self, valid_weather_data):
        """Valid weather data should create a WeatherData object without errors."""
        weather = WeatherData(**valid_weather_data)
        assert weather.city == "Paris"
        assert weather.temperature == 23.29
        assert weather.humidity == 45

    def test_city_is_title_cased(self, valid_weather_data):
        """City names should be title-cased automatically."""
        valid_weather_data["city"] = "new york"
        weather = WeatherData(**valid_weather_data)
        assert weather.city == "New York"

    def test_description_is_lowercased(self, valid_weather_data):
        """Weather descriptions should be lowercased automatically."""
        valid_weather_data["weather_description"] = "CLEAR SKY"
        weather = WeatherData(**valid_weather_data)
        assert weather.weather_description == "clear sky"

    def test_temperature_is_rounded(self, valid_weather_data):
        """Temperature should be rounded to 2 decimal places."""
        valid_weather_data["temperature"] = 23.298765
        weather = WeatherData(**valid_weather_data)
        assert weather.temperature == 23.3

    def test_visibility_defaults_to_zero(self, valid_weather_data):
        """Visibility should default to 0 if not provided."""
        valid_weather_data.pop("visibility")
        weather = WeatherData(**valid_weather_data)
        assert weather.visibility == 0

    def test_weather_data_db_has_inserted_at(self, valid_weather_data):
        """WeatherDataDB should automatically set inserted_at."""
        weather = WeatherDataDB(**valid_weather_data)
        assert weather.inserted_at is not None
        assert isinstance(weather.inserted_at, datetime)


# ─────────────────────────────────────────────────────────────
# Sad path tests — invalid data should raise ValidationError
# ─────────────────────────────────────────────────────────────
class TestWeatherDataInvalid:

    def test_missing_city_raises_error(self, valid_weather_data):
        """Missing city should raise ValidationError."""
        valid_weather_data.pop("city")
        with pytest.raises(ValidationError):
            WeatherData(**valid_weather_data)

    def test_temperature_too_high_raises_error(self, valid_weather_data):
        """Temperature above 60°C should raise ValidationError."""
        valid_weather_data["temperature"] = 99.0
        with pytest.raises(ValidationError):
            WeatherData(**valid_weather_data)

    def test_temperature_too_low_raises_error(self, valid_weather_data):
        """Temperature below -90°C should raise ValidationError."""
        valid_weather_data["temperature"] = -100.0
        with pytest.raises(ValidationError):
            WeatherData(**valid_weather_data)

    def test_humidity_above_100_raises_error(self, valid_weather_data):
        """Humidity above 100 should raise ValidationError."""
        valid_weather_data["humidity"] = 150
        with pytest.raises(ValidationError):
            WeatherData(**valid_weather_data)

    def test_humidity_below_zero_raises_error(self, valid_weather_data):
        """Humidity below 0 should raise ValidationError."""
        valid_weather_data["humidity"] = -5
        with pytest.raises(ValidationError):
            WeatherData(**valid_weather_data)

    def test_negative_wind_speed_raises_error(self, valid_weather_data):
        """Negative wind speed should raise ValidationError."""
        valid_weather_data["wind_speed"] = -1.0
        with pytest.raises(ValidationError):
            WeatherData(**valid_weather_data)

    def test_empty_city_raises_error(self, valid_weather_data):
        """Empty or whitespace city should raise ValidationError."""
        valid_weather_data["city"] = "   "
        with pytest.raises(ValidationError):
            WeatherData(**valid_weather_data)

    def test_empty_description_raises_error(self, valid_weather_data):
        """Empty weather description should raise ValidationError."""
        valid_weather_data["weather_description"] = ""
        with pytest.raises(ValidationError):
            WeatherData(**valid_weather_data)

    def test_pressure_too_low_raises_error(self, valid_weather_data):
        """Pressure below 800 hPa should raise ValidationError."""
        valid_weather_data["pressure"] = 500
        with pytest.raises(ValidationError):
            WeatherData(**valid_weather_data)

    def test_missing_recorded_at_raises_error(self, valid_weather_data):
        """Missing recorded_at should raise ValidationError."""
        valid_weather_data.pop("recorded_at")
        with pytest.raises(ValidationError):
            WeatherData(**valid_weather_data)