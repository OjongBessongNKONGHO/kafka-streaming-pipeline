"""
Tests for the WeatherData data contract.

The contract validates messages at the Kafka pipeline boundary — before
they reach the consumer and the database. These tests prove:

- Valid messages pass without violations
- Structural violations (missing fields, wrong types) are caught in pass 1
- Business rule violations (implausible values) are caught in pass 2
- The feels_like coherence rule catches sensor faults
- The future timestamp rule catches clock skew bugs
- Batch validation returns correct counts
- The contract version is stable and accessible

Every test uses a minimal valid message fixture and mutates only the
field under test — so failures are unambiguous.
"""
import pytest
from datetime import datetime, timedelta
from src.contracts.weather_contract import WeatherDataContract, CONTRACT_VERSION


@pytest.fixture
def valid_message():
    """Minimal valid weather message — passes all contract rules."""
    return {
        "city": "Paris",
        "country": "FR",
        "temperature": 22.5,
        "feels_like": 21.0,
        "humidity": 65,
        "pressure": 1013,
        "weather_description": "clear sky",
        "wind_speed": 5.2,
        "visibility": 10000,
        "recorded_at": datetime(2026, 6, 16, 10, 0, 0),
    }


@pytest.fixture
def contract():
    return WeatherDataContract()


class TestValidMessage:
    def test_valid_message_passes(self, contract, valid_message):
        """A well-formed message must pass without violations."""
        result = contract.validate(valid_message)
        assert result.valid is True
        assert result.violations == []

    def test_contract_version_in_result(self, contract, valid_message):
        """Every result must carry the contract version."""
        result = contract.validate(valid_message)
        assert result.contract_version == CONTRACT_VERSION

    def test_contract_version_is_semver(self, contract):
        """Version must be a semver string — signals intentional versioning."""
        parts = WeatherDataContract.VERSION.split(".")
        assert len(parts) == 3
        assert all(p.isdigit() for p in parts)


class TestStructuralViolations:
    def test_missing_required_field_fails(self, contract, valid_message):
        """Removing a required field must produce a structural violation."""
        del valid_message["temperature"]
        result = contract.validate(valid_message)
        assert result.valid is False
        fields = [v.field for v in result.violations]
        assert "temperature" in fields

    def test_wrong_type_fails(self, contract, valid_message):
        """Passing a string where a number is expected must fail."""
        valid_message["humidity"] = "not-a-number"
        result = contract.validate(valid_message)
        assert result.valid is False

    def test_multiple_missing_fields_all_reported(self, contract, valid_message):
        """All structural violations must be reported, not just the first."""
        del valid_message["temperature"]
        del valid_message["humidity"]
        result = contract.validate(valid_message)
        assert result.valid is False
        assert len(result.violations) >= 2


class TestBusinessRules:
    def test_temperature_above_max_fails(self, contract, valid_message):
        """Temperature above 60°C must fail — physically impossible on Earth."""
        valid_message["temperature"] = 61.0
        valid_message["feels_like"] = 61.0
        result = contract.validate(valid_message)
        assert result.valid is False
        fields = [v.field for v in result.violations]
        assert "temperature" in fields

    def test_temperature_below_min_fails(self, contract, valid_message):
        """Temperature below -90°C must fail."""
        valid_message["temperature"] = -91.0
        valid_message["feels_like"] = -91.0
        result = contract.validate(valid_message)
        assert result.valid is False
        fields = [v.field for v in result.violations]
        assert "temperature" in fields

    def test_humidity_above_100_fails(self, contract, valid_message):
        """Humidity above 100% is physically impossible."""
        valid_message["humidity"] = 101
        result = contract.validate(valid_message)
        assert result.valid is False

    def test_humidity_below_0_fails(self, contract, valid_message):
        """Negative humidity is impossible."""
        valid_message["humidity"] = -1
        result = contract.validate(valid_message)
        assert result.valid is False

    def test_pressure_above_max_fails(self, contract, valid_message):
        """Pressure above 1100 hPa suggests sensor fault."""
        valid_message["pressure"] = 1101
        result = contract.validate(valid_message)
        assert result.valid is False

    def test_pressure_below_min_fails(self, contract, valid_message):
        """Pressure below 800 hPa is outside earthly range."""
        valid_message["pressure"] = 799
        result = contract.validate(valid_message)
        assert result.valid is False

    def test_negative_wind_speed_fails(self, contract, valid_message):
        """Wind speed cannot be negative."""
        valid_message["wind_speed"] = -1.0
        result = contract.validate(valid_message)
        assert result.valid is False

    def test_feels_like_too_far_from_temperature_fails(self, contract, valid_message):
        """
        feels_like more than 30°C from temperature suggests a sensor fault
        or API default value — not real meteorological data.
        """
        valid_message["temperature"] = 20.0
        valid_message["feels_like"] = 55.0  # 35°C delta
        result = contract.validate(valid_message)
        assert result.valid is False
        fields = [v.field for v in result.violations]
        assert "feels_like" in fields

    def test_future_timestamp_fails(self, contract, valid_message):
        """
        A recorded_at timestamp in the future almost always indicates
        a clock skew bug or API default value.
        """
        valid_message["recorded_at"] = datetime.utcnow() + timedelta(hours=1)
        result = contract.validate(valid_message)
        assert result.valid is False
        fields = [v.field for v in result.violations]
        assert "recorded_at" in fields

    def test_boundary_temperature_accepted(self, contract, valid_message):
        """Exact boundary values must pass — ge/le, not gt/lt."""
        valid_message["temperature"] = 60.0
        valid_message["feels_like"] = 60.0
        result = contract.validate(valid_message)
        assert result.valid is True


class TestBatchValidation:
    def test_batch_counts_correct(self, contract, valid_message):
        """Batch result must report correct valid/invalid counts."""
        bad_message = dict(valid_message)
        bad_message["humidity"] = 150
        results = contract.validate_batch([valid_message, bad_message, valid_message])
        assert results["total"] == 3
        assert results["valid"] == 2
        assert results["invalid"] == 1

    def test_batch_violations_include_index(self, contract, valid_message):
        """Batch violations must include the index of the failed message."""
        bad_message = dict(valid_message)
        bad_message["humidity"] = 150
        results = contract.validate_batch([valid_message, bad_message])
        assert results["violations"][0][0] == 1

    def test_empty_batch_returns_zero_counts(self, contract):
        """Empty batch must return zeros without error."""
        results = contract.validate_batch([])
        assert results["total"] == 0
        assert results["valid"] == 0
        assert results["invalid"] == 0