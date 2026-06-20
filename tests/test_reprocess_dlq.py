import json
import pytest
from unittest.mock import MagicMock, patch
from scripts.reprocess_dlq import fetch_dlq_rows, delete_dlq_row, reprocess_dlq


# ──────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────
@pytest.fixture
def valid_dlq_row():
    """A DLQ row whose raw_message is valid and should reprocess successfully."""
    return {
        "id": 1,
        "raw_message": json.dumps({
            "city": "Paris",
            "country": "FR",
            "temperature": 23.29,
            "feels_like": 22.85,
            "humidity": 45,
            "pressure": 1012,
            "weather_description": "clear sky",
            "wind_speed": 5.66,
            "visibility": 10000,
            "recorded_at": "2026-05-12T14:00:00",
        }),
        "error_type": "ConnectionError",
        "error_detail": "DB unavailable at time of original processing",
        "kafka_topic": "weather_stream",
        "kafka_offset": 42,
        "failed_at": "2026-06-12T10:00:00",
    }


@pytest.fixture
def malformed_dlq_row():
    """A DLQ row whose raw_message is not valid JSON."""
    return {
        "id": 2,
        "raw_message": "{not valid json",
        "error_type": "ValidationError",
        "error_detail": "Temperature out of range",
        "kafka_topic": "weather_stream",
        "kafka_offset": 43,
        "failed_at": "2026-06-12T10:05:00",
    }


# ──────────────────────────────────────────────────────────────────
# fetch_dlq_rows tests
# ──────────────────────────────────────────────────────────────────
class TestFetchDlqRows:

    def test_returns_list_of_dicts(self, valid_dlq_row):
        """fetch_dlq_rows must convert SQLAlchemy Row objects to plain dicts."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row._mapping = valid_dlq_row
        mock_conn.execute.return_value = [mock_row]

        rows = fetch_dlq_rows(mock_engine)

        assert rows == [valid_dlq_row]

    def test_returns_empty_list_when_dlq_is_empty(self):
        """An empty DLQ table must return an empty list, not raise."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value = []

        rows = fetch_dlq_rows(mock_engine)

        assert rows == []


# ──────────────────────────────────────────────────────────────────
# delete_dlq_row tests
# ──────────────────────────────────────────────────────────────────
class TestDeleteDlqRow:

    def test_executes_delete_with_correct_id(self):
        """delete_dlq_row must issue a DELETE scoped to the given id."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        delete_dlq_row(mock_engine, dlq_id=7)

        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert call_args[0][1] == {"id": 7}


# ──────────────────────────────────────────────────────────────────
# reprocess_dlq tests
# ──────────────────────────────────────────────────────────────────
class TestReprocessDlq:

    @patch("scripts.reprocess_dlq.get_engine")
    @patch("scripts.reprocess_dlq.fetch_dlq_rows")
    def test_empty_dlq_does_nothing(self, mock_fetch, mock_get_engine):
        """An empty DLQ should log and return without calling process_message."""
        mock_fetch.return_value = []

        with patch("scripts.reprocess_dlq.process_message") as mock_process:
            reprocess_dlq()
            mock_process.assert_not_called()

    @patch("scripts.reprocess_dlq.get_engine")
    @patch("scripts.reprocess_dlq.fetch_dlq_rows")
    @patch("scripts.reprocess_dlq.process_message")
    @patch("scripts.reprocess_dlq.delete_dlq_row")
    def test_successful_reprocess_deletes_dlq_row(
        self, mock_delete, mock_process, mock_fetch, mock_get_engine, valid_dlq_row
    ):
        """
        A message that reprocesses successfully must be removed from
        the DLQ — the row should no longer represent an unresolved
        failure once process_message confirms it succeeded.
        """
        mock_fetch.return_value = [valid_dlq_row]
        mock_process.return_value = True

        reprocess_dlq()

        mock_process.assert_called_once()
        mock_delete.assert_called_once_with(mock_get_engine.return_value, 1)

    @patch("scripts.reprocess_dlq.get_engine")
    @patch("scripts.reprocess_dlq.fetch_dlq_rows")
    @patch("scripts.reprocess_dlq.process_message")
    @patch("scripts.reprocess_dlq.delete_dlq_row")
    def test_failed_reprocess_keeps_dlq_row(
        self, mock_delete, mock_process, mock_fetch, mock_get_engine, valid_dlq_row
    ):
        """
        A message that fails again on reprocessing must remain in the
        DLQ — delete_dlq_row should never be called for it.
        """
        mock_fetch.return_value = [valid_dlq_row]
        mock_process.return_value = False

        reprocess_dlq()

        mock_delete.assert_not_called()

    @patch("scripts.reprocess_dlq.get_engine")
    @patch("scripts.reprocess_dlq.fetch_dlq_rows")
    @patch("scripts.reprocess_dlq.process_message")
    @patch("scripts.reprocess_dlq.delete_dlq_row")
    def test_malformed_raw_message_is_skipped_not_crashed(
        self, mock_delete, mock_process, mock_fetch, mock_get_engine, malformed_dlq_row
    ):
        """
        A DLQ row with unparseable JSON must be skipped gracefully —
        process_message is never called for it, and the script does
        not raise, since one corrupt row should never halt reprocessing
        of the rest of the queue.
        """
        mock_fetch.return_value = [malformed_dlq_row]

        reprocess_dlq()

        mock_process.assert_not_called()
        mock_delete.assert_not_called()

    @patch("scripts.reprocess_dlq.get_engine")
    @patch("scripts.reprocess_dlq.fetch_dlq_rows")
    @patch("scripts.reprocess_dlq.process_message")
    @patch("scripts.reprocess_dlq.delete_dlq_row")
    def test_kafka_offset_and_topic_passed_through_for_traceability(
        self, mock_delete, mock_process, mock_fetch, mock_get_engine, valid_dlq_row
    ):
        """
        The original kafka_offset must be passed to process_message so
        the reprocessed row in weather_events keeps its original
        traceability link back to the Kafka message that produced it.
        """
        mock_fetch.return_value = [valid_dlq_row]
        mock_process.return_value = True

        reprocess_dlq()

        call_kwargs = mock_process.call_args.kwargs
        assert call_kwargs["kafka_offset"] == 42

    @patch("scripts.reprocess_dlq.get_engine")
    @patch("scripts.reprocess_dlq.fetch_dlq_rows")
    @patch("scripts.reprocess_dlq.process_message")
    @patch("scripts.reprocess_dlq.delete_dlq_row")
    def test_mixed_batch_processes_each_row_independently(
        self, mock_delete, mock_process, mock_fetch, mock_get_engine, valid_dlq_row
    ):
        """
        A batch with multiple rows must attempt every row even if an
        earlier one fails — one failure must not stop the rest of the
        batch from being attempted.
        """
        row_a = dict(valid_dlq_row, id=1, kafka_offset=42)
        row_b = dict(valid_dlq_row, id=2, kafka_offset=43)
        mock_fetch.return_value = [row_a, row_b]
        mock_process.side_effect = [False, True]

        reprocess_dlq()

        assert mock_process.call_count == 2
        mock_delete.assert_called_once_with(mock_get_engine.return_value, 2)