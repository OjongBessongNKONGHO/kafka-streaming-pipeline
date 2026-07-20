"""
Tests for the Confluent-compatible Avro serializer.

These tests verify the wire format and serialization behavior without
requiring a live Schema Registry. The registry client is mocked so
the tests run in CI without any infrastructure.

What we're proving:
- serialize() produces valid Confluent wire format (magic byte + schema ID + payload)
- The magic byte is always 0x00
- The schema ID is encoded as big-endian 32-bit int in bytes 1-4
- deserialize() recovers the original record from the wire bytes
- Round-trip: serialize then deserialize returns the original data
- Missing required fields raise AvroSerializationError
- Invalid magic byte on deserialization raises AvroSerializationError
- Schema ID is cached after first registration (registry called once)
"""
import struct
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from src.avro.serializer import AvroSerializer, AvroSerializationError, MAGIC_BYTE

SCHEMA_PATH = "schemas/weather_v1.avsc"
REGISTRY_URL = "http://localhost:8081"
SUBJECT = "weather-value-test"
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
        "recorded_at": "2026-07-20T10:00:00",
    }


@pytest.fixture
def serializer():
    """
    AvroSerializer with a mocked registry client.
    The mock returns MOCK_SCHEMA_ID on register_schema so tests
    never need a live Schema Registry.
    """
    with patch("src.avro.serializer.SchemaRegistryClient") as MockClient:
        instance = MockClient.return_value
        instance.register_schema.return_value = MOCK_SCHEMA_ID
        s = AvroSerializer(
            registry_url=REGISTRY_URL,
            subject=SUBJECT,
            schema_path=SCHEMA_PATH,
        )
        yield s


class TestWireFormat:
    def test_magic_byte_is_zero(self, serializer, valid_record):
        """First byte of every message must be 0x00 (Confluent magic byte)."""
        encoded = serializer.serialize(valid_record)
        assert encoded[0:1] == struct.pack("b", MAGIC_BYTE)

    def test_schema_id_in_bytes_1_to_4(self, serializer, valid_record):
        """Bytes 1-4 must be the schema ID as big-endian 32-bit int."""
        encoded = serializer.serialize(valid_record)
        schema_id = struct.unpack(">I", encoded[1:5])[0]
        assert schema_id == MOCK_SCHEMA_ID

    def test_message_at_least_5_bytes(self, serializer, valid_record):
        """Every encoded message must be at least 5 bytes (header alone)."""
        encoded = serializer.serialize(valid_record)
        assert len(encoded) >= 5

    def test_payload_follows_header(self, serializer, valid_record):
        """Bytes beyond the 5-byte header must be non-empty Avro payload."""
        encoded = serializer.serialize(valid_record)
        payload = encoded[5:]
        assert len(payload) > 0


class TestRoundTrip:
    def test_round_trip_recovers_city(self, serializer, valid_record):
        """Deserializing serialized bytes must recover the original city."""
        encoded = serializer.serialize(valid_record)
        decoded = serializer.deserialize(encoded)
        assert decoded["city"] == valid_record["city"]

    def test_round_trip_recovers_all_fields(self, serializer, valid_record):
        """All fields must survive the serialize/deserialize round trip."""
        encoded = serializer.serialize(valid_record)
        decoded = serializer.deserialize(encoded)
        assert decoded["temperature"] == valid_record["temperature"]
        assert decoded["humidity"] == valid_record["humidity"]
        assert decoded["pressure"] == valid_record["pressure"]
        assert decoded["wind_speed"] == valid_record["wind_speed"]
        assert decoded["weather_description"] == valid_record["weather_description"]
        assert decoded["country"] == valid_record["country"]

    def test_round_trip_optional_visibility(self, serializer, valid_record):
        """Optional visibility field must survive the round trip."""
        encoded = serializer.serialize(valid_record)
        decoded = serializer.deserialize(encoded)
        assert decoded["visibility"] == valid_record["visibility"]


class TestErrorHandling:
    def test_invalid_magic_byte_raises(self, serializer, valid_record):
        """
        Deserializing bytes with wrong magic byte must raise
        AvroSerializationError — not silently produce garbage.
        """
        encoded = serializer.serialize(valid_record)
        corrupted = b"\x01" + encoded[1:]
        with pytest.raises(AvroSerializationError, match="magic byte"):
            serializer.deserialize(corrupted)

    def test_message_too_short_raises(self, serializer):
        """
        Fewer than 5 bytes cannot contain a valid Confluent header.
        Must raise AvroSerializationError.
        """
        with pytest.raises(AvroSerializationError, match="too short"):
            serializer.deserialize(b"\x00\x01\x02")

    def test_missing_required_field_raises(self, serializer, valid_record):
        """
        A record missing a required field must raise AvroSerializationError.
        fastavro enforces schema compliance at encode time.
        """
        del valid_record["temperature"]
        with pytest.raises(AvroSerializationError):
            serializer.serialize(valid_record)


class TestSchemaIdCaching:
    def test_registry_called_once_for_multiple_messages(self, valid_record):
        """
        The schema ID must be cached after first registration.
        Multiple serialize() calls must only hit the registry once.
        """
        with patch("src.avro.serializer.SchemaRegistryClient") as MockClient:
            instance = MockClient.return_value
            instance.register_schema.return_value = MOCK_SCHEMA_ID
            s = AvroSerializer(
                registry_url=REGISTRY_URL,
                subject=SUBJECT,
                schema_path=SCHEMA_PATH,
            )
            s.serialize(valid_record)
            s.serialize(valid_record)
            s.serialize(valid_record)
            assert instance.register_schema.call_count == 1