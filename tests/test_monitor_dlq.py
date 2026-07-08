from datetime import datetime, timedelta, UTC
from unittest.mock import MagicMock, patch

import pytest

from scripts.monitor_dlq import fetch_dlq_summary, compute_age_minutes, monitor_dlq

# ── fetch_dlq_summary tests ──────────────────────────────────────────


class TestFetchDlqSummary:

    def test_returns_correct_structure_when_dlq_has_messages(self):
        """fetch_dlq_summary must aggregate all three queries into one dict
        with the expected keys and values."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        summary_row = MagicMock(
            total=3,
            oldest_failure=datetime(2026, 7, 8, 10, 0, 0),
            latest_failure=datetime(2026, 7, 8, 12, 0, 0),
        )
        error_row_1 = MagicMock(error_type="ConnectionError", count=2)
        error_row_2 = MagicMock(error_type="ValidationError", count=1)
        city_row_1 = MagicMock(city="Paris", count=2)
        city_row_2 = MagicMock(city="Tokyo", count=1)

        summary_result = MagicMock()
        summary_result.fetchone.return_value = summary_row
        by_error_result = MagicMock()
        by_error_result.fetchall.return_value = [error_row_1, error_row_2]
        by_city_result = MagicMock()
        by_city_result.fetchall.return_value = [city_row_1, city_row_2]

        mock_conn.execute.side_effect = [
            summary_result,
            by_error_result,
            by_city_result,
        ]

        result = fetch_dlq_summary(mock_engine)

        assert result["total"] == 3
        assert result["oldest_failure"] == datetime(2026, 7, 8, 10, 0, 0)
        assert result["latest_failure"] == datetime(2026, 7, 8, 12, 0, 0)
        assert result["by_error"] == [
            {"error_type": "ConnectionError", "count": 2},
            {"error_type": "ValidationError", "count": 1},
        ]
        assert result["by_city"] == [
            {"city": "Paris", "count": 2},
            {"city": "Tokyo", "count": 1},
        ]

    def test_returns_zero_total_when_dlq_is_empty(self):
        """An empty DLQ must return total=0 and empty breakdown lists,
        not raise or return None for the whole summary."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        summary_row = MagicMock(total=0, oldest_failure=None, latest_failure=None)
        summary_result = MagicMock()
        summary_result.fetchone.return_value = summary_row
        by_error_result = MagicMock()
        by_error_result.fetchall.return_value = []
        by_city_result = MagicMock()
        by_city_result.fetchall.return_value = []

        mock_conn.execute.side_effect = [
            summary_result,
            by_error_result,
            by_city_result,
        ]

        result = fetch_dlq_summary(mock_engine)

        assert result["total"] == 0
        assert result["oldest_failure"] is None
        assert result["by_error"] == []
        assert result["by_city"] == []

    def test_handles_missing_city_gracefully(self):
        """A DLQ row whose raw_message has no city key produces a None
        city in the breakdown, not a KeyError."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        summary_row = MagicMock(
            total=1,
            oldest_failure=datetime(2026, 7, 8),
            latest_failure=datetime(2026, 7, 8),
        )
        summary_result = MagicMock()
        summary_result.fetchone.return_value = summary_row
        by_error_result = MagicMock()
        by_error_result.fetchall.return_value = [
            MagicMock(error_type="ValidationError", count=1)
        ]
        by_city_result = MagicMock()
        by_city_result.fetchall.return_value = [MagicMock(city=None, count=1)]

        mock_conn.execute.side_effect = [
            summary_result,
            by_error_result,
            by_city_result,
        ]

        result = fetch_dlq_summary(mock_engine)

        assert result["by_city"] == [{"city": None, "count": 1}]


# ── compute_age_minutes tests ────────────────────────────────────────


class TestComputeAgeMinutes:

    def test_returns_none_when_timestamp_is_none(self):
        """No timestamp means no age to compute - must not raise."""
        assert compute_age_minutes(None) is None

    def test_computes_positive_minutes_for_past_timestamp(self):
        """A timestamp 90 minutes in the past must return ~90.0."""
        past = datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=90)
        age = compute_age_minutes(past)
        assert 89.0 <= age <= 91.0

    def test_computes_near_zero_for_recent_timestamp(self):
        """A timestamp from a few seconds ago must round to ~0.0 minutes."""
        recent = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=5)
        age = compute_age_minutes(recent)
        assert 0.0 <= age <= 0.2


# ── monitor_dlq tests ─────────────────────────────────────────────────


class TestMonitorDlq:

    @patch("scripts.monitor_dlq.get_engine")
    @patch("scripts.monitor_dlq.fetch_dlq_summary")
    def test_exits_zero_when_dlq_is_empty(self, mock_fetch, mock_get_engine):
        """An empty DLQ is a healthy pipeline - exit code 0."""
        mock_fetch.return_value = {
            "total": 0,
            "oldest_failure": None,
            "latest_failure": None,
            "by_error": [],
            "by_city": [],
        }

        with pytest.raises(SystemExit) as exc_info:
            monitor_dlq()

        assert exc_info.value.code == 0

    @patch("scripts.monitor_dlq.get_engine")
    @patch("scripts.monitor_dlq.fetch_dlq_summary")
    def test_exits_one_when_dlq_has_messages(self, mock_fetch, mock_get_engine):
        """A non-empty DLQ requires investigation - exit code 1."""
        mock_fetch.return_value = {
            "total": 5,
            "oldest_failure": datetime(2026, 7, 8, 10, 0, 0),
            "latest_failure": datetime(2026, 7, 8, 12, 0, 0),
            "by_error": [{"error_type": "ConnectionError", "count": 5}],
            "by_city": [{"city": "Paris", "count": 5}],
        }

        with pytest.raises(SystemExit) as exc_info:
            monitor_dlq()

        assert exc_info.value.code == 1

    @patch("scripts.monitor_dlq.get_engine")
    @patch("scripts.monitor_dlq.fetch_dlq_summary")
    def test_logs_warning_breakdown_when_dlq_has_messages(
        self, mock_fetch, mock_get_engine, caplog
    ):
        """When the DLQ has messages, the error-type and city breakdowns
        must actually appear in the log output - a report nobody can
        read is as useless as no report at all."""
        mock_fetch.return_value = {
            "total": 2,
            "oldest_failure": datetime(2026, 7, 8, 10, 0, 0),
            "latest_failure": datetime(2026, 7, 8, 12, 0, 0),
            "by_error": [{"error_type": "ConnectionError", "count": 2}],
            "by_city": [{"city": "Paris", "count": 2}],
        }

        with caplog.at_level("WARNING"):
            with pytest.raises(SystemExit):
                monitor_dlq()

        assert "ConnectionError" in caplog.text
        assert "Paris" in caplog.text
