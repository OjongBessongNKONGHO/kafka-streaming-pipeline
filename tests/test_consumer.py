import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from consumer.processor import process_message, save_to_dlq


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────
@pytest.fixture
def valid_message():
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
        "recorded_at": "2026-05-12T14:00:00"
    }


@pytest.fixture
def invalid_message():
    return {
        "city": "Paris",
        "country": "FR",
        "temperature": 999.0,  # Invalid — above 60°C
        "feels_like": 22.85,
        "humidity": 45,
        "pressure": 1012,
        "weather_description": "clear sky",
        "wind_speed": 5.66,
        "recorded_at": "2026-05-12T14:00:00"
    }


# ─────────────────────────────────────────────────────────────
# process_message tests
# ─────────────────────────────────────────────────────────────
class TestProcessMessage:

    def test_valid_message_returns_true(self, valid_message):
        """Valid message with no duplicate should return True."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate no duplicate found
        mock_conn.execute.return_value.fetchone.return_value = None

        result = process_message(
            raw_message=valid_message,
            kafka_offset=1,
            kafka_partition=0,
            engine=mock_engine
        )
        assert result is True

    def test_invalid_message_returns_false(self, invalid_message):
        """Message failing Pydantic validation should return False."""
        mock_engine = MagicMock()

        result = process_message(
            raw_message=invalid_message,
            kafka_offset=2,
            kafka_partition=0,
            engine=mock_engine
        )
        assert result is False

    def test_duplicate_message_returns_true(self, valid_message):
        """Duplicate message should be skipped and return True."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate duplicate found
        mock_conn.execute.return_value.fetchone.return_value = (1,)

        result = process_message(
            raw_message=valid_message,
            kafka_offset=3,
            kafka_partition=0,
            engine=mock_engine
        )
        assert result is True

    def test_db_error_returns_false(self, valid_message):
        """Database error during insert should return False."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate no duplicate but DB error on insert
        mock_conn.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=None)),
            Exception("DB connection lost")
        ]

        result = process_message(
            raw_message=valid_message,
            kafka_offset=4,
            kafka_partition=0,
            engine=mock_engine
        )
        assert result is False


# ─────────────────────────────────────────────────────────────
# save_to_dlq tests
# ─────────────────────────────────────────────────────────────
class TestSaveToDLQ:

    def test_dlq_save_succeeds(self):
        """Failed message should be saved to DLQ without raising."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        # Should not raise any exception
        save_to_dlq(
            raw_message='{"city": "Paris"}',
            error_type="ValidationError",
            error_detail="Temperature out of range",
            kafka_topic="weather_stream",
            kafka_offset=10,
            engine=mock_engine
        )
        mock_conn.execute.assert_called_once()

    def test_dlq_save_handles_db_error(self):
        """DLQ save should handle DB errors without crashing the pipeline."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.side_effect = Exception("DB unavailable")

        # Should not raise — pipeline must continue even if DLQ save fails
        save_to_dlq(
            raw_message='{"city": "Paris"}',
            error_type="ValidationError",
            error_detail="Temperature out of range",
            kafka_topic="weather_stream",
            kafka_offset=10,
            engine=mock_engine
        )