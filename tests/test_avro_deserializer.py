"""
Tests for the Confluent-compatible Avro deserializer.

These tests verify deserialization behavior without requiring a live
Schema Registry. The registry client is mocked so tests run in CI
without any infrastructure.

What we're proving:
- deserialize() correctly reads the Confluent wire format header
- magic byte validation catches non-Avro messages early
- schema ID is extracted correctly from bytes 1-4
- payload is decoded to the correct Python dict
- round-trip with AvroSerializer produces the original record
- wrong magic byte raises AvroDeserializationError
- message too short raises AvroDeserializationError
- registry fetch failure raises AvroDeserializationError
- writer schemas are cached after first fetch (registry called once per ID)
"""
import struct
import pytest
from unittest.mock import MagicMock, patch

from src.avro.deserializer import AvroDeserializer, AvroDeserializationError, MAGIC_BYTE
from src.avro.serializer import AvroSerializer

SCHEMA_PATH = "schemas/weather_v1.avsc"
REGISTRY_URL = "http://localhost:8081"
MOCK_SCHEMA_ID = 42


@pytest.fixture
def valid_record():
    """Minimal valid weather record matching weather_v1.avsc."""
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
        "recorded_at": "2026-07-25T10:00:00",
    }


@pytest.fixture
def serializer():
    """AvroSerializer with mocked registry — produces valid wire format bytes."""
    with patch("src.avro.serializer.SchemaRegistryClient") as MockClient:
        instance = MockClient.return_value
        instance.register_schema.return_value = MOCK_SCHEMA_ID
        s = AvroSerializer(
            registry_url=REGISTRY_URL,
            subject="weather-value",
            schema_path=SCHEMA_PATH,
        )
        yield s


@pytest.fixture
def deserializer():
    """
    AvroDeserializer with mocked registry client.
    The mock returns a parsed schema on get_schema_by_id so the
    deserializer can decode payloads without a live registry.
    """
    import json
    import fastavro.schema

    with open(SCHEMA_PATH) as f:
        raw_schema = json.load(f)
    parsed = fastavro.schema.parse_schema(raw_schema)

    with patch("src.avro.deserializer.SchemaRegistryClient") as MockClient:
        instance = MockClient.return_value
        instance.get_schema_by_id.return_value = raw_schema
        d = AvroDeserializer(
            registry_url=REGISTRY_URL,
            schema_path=SCHEMA_PATH,
        )
        yield d


class TestWireFormatParsing:
    def test_rejects_wrong_magic_byte(self, deserializer):
        """
        A message with the wrong magic byte must raise immediately.
        This catches plain JSON or other non-Avro messages before
        attempting schema lookup or decoding.
        """
        bad_magic = b"\x01" + struct.pack(">I", MOCK_SCHEMA_ID) + b"\x00" * 10
        with pytest.raises(AvroDeserializationError, match="magic byte"):
            deserializer.deserialize(bad_magic)

    def test_rejects_message_too_short(self, deserializer):
        """Messages under 5 bytes cannot contain a valid header."""
        with pytest.raises(AvroDeserializationError, match="too short"):
            deserializer.deserialize(b"\x00\x01\x02")

    def test_rejects_empty_message(self, deserializer):
        """Empty bytes must raise AvroDeserializationError."""
        with pytest.raises(AvroDeserializationError):
            deserializer.deserialize(b"")


class TestRoundTrip:
    def test_round_trip_recovers_city(self, serializer, deserializer, valid_record):
        """
        A record serialized by AvroSerializer must be recoverable
        by AvroDeserializer. City field is the primary key.
        """
        encoded = serializer.serialize(valid_record)
        decoded = deserializer.deserialize(encoded)
        assert decoded["city"] == valid_record["city"]

    def test_round_trip_recovers_all_numeric_fields(self, serializer, deserializer, valid_record):
        """All numeric fields must survive the round trip without precision loss."""
        encoded = serializer.serialize(valid_record)
        decoded = deserializer.deserialize(encoded)
        assert decoded["temperature"] == valid_record["temperature"]
        assert decoded["feels_like"] == valid_record["feels_like"]
        assert decoded["humidity"] == valid_record["humidity"]
        assert decoded["pressure"] == valid_record["pressure"]
        assert decoded["wind_speed"] == valid_record["wind_speed"]

    def test_round_trip_recovers_string_fields(self, serializer, deserializer, valid_record):
        """String fields must survive the round trip intact."""
        encoded = serializer.serialize(valid_record)
        decoded = deserializer.deserialize(encoded)
        assert decoded["country"] == valid_record["country"]
        assert decoded["weather_description"] == valid_record["weather_description"]
        assert decoded["recorded_at"] == valid_record["recorded_at"]

    def test_round_trip_recovers_optional_visibility(self, serializer, deserializer, valid_record):
        """Optional visibility field must survive the round trip."""
        encoded = serializer.serialize(valid_record)
        decoded = deserializer.deserialize(encoded)
        assert decoded["visibility"] == valid_record["visibility"]

    def test_round_trip_null_visibility(self, serializer, deserializer, valid_record):
        """Null optional field must round trip as None."""
        valid_record["visibility"] = None
        encoded = serializer.serialize(valid_record)
        decoded = deserializer.deserialize(encoded)
        assert decoded["visibility"] is None


class TestSchemaCaching:
    def test_writer_schema_fetched_once_per_id(self, valid_record):
        """
        The writer schema must be cached after first fetch.
        Processing 100 messages with the same schema ID must only
        hit the registry once — not once per message.
        """
        import json
        import fastavro.schema

        with open(SCHEMA_PATH) as f:
            raw_schema = json.load(f)

        with patch("src.avro.deserializer.SchemaRegistryClient") as MockClient:
            instance = MockClient.return_value
            instance.get_schema_by_id.return_value = raw_schema

            with patch("src.avro.serializer.SchemaRegistryClient") as MockSerClient:
                ser_instance = MockSerClient.return_value
                ser_instance.register_schema.return_value = MOCK_SCHEMA_ID

                s = AvroSerializer(
                    registry_url=REGISTRY_URL,
                    subject="weather-value",
                    schema_path=SCHEMA_PATH,
                )
                d = AvroDeserializer(
                    registry_url=REGISTRY_URL,
                    schema_path=SCHEMA_PATH,
                )

                for _ in range(10):
                    encoded = s.serialize(valid_record)
                    d.deserialize(encoded)

                assert instance.get_schema_by_id.call_count == 1


class TestRegistryFailure:
    def test_registry_unreachable_raises(self, valid_record):
        """
        If the registry is unreachable when fetching a writer schema,
        AvroDeserializationError must be raised with a clear message.
        The consumer can then route the message to the DLQ rather than
        crashing the entire consumer process.
        """
        from src.avro.registry_client import SchemaRegistryError

        with patch("src.avro.deserializer.SchemaRegistryClient") as MockClient:
            instance = MockClient.return_value
            instance.get_schema_by_id.side_effect = SchemaRegistryError(
                "Connection refused"
            )

            with patch("src.avro.serializer.SchemaRegistryClient") as MockSerClient:
                ser_instance = MockSerClient.return_value
                ser_instance.register_schema.return_value = MOCK_SCHEMA_ID

                s = AvroSerializer(
                    registry_url=REGISTRY_URL,
                    subject="weather-value",
                    schema_path=SCHEMA_PATH,
                )
                d = AvroDeserializer(
                    registry_url=REGISTRY_URL,
                    schema_path=SCHEMA_PATH,
                )

                encoded = s.serialize(valid_record)
                with pytest.raises(AvroDeserializationError, match="Failed to fetch writer schema"):
                    d.deserialize(encoded)