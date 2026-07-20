"""
Confluent-compatible Avro serializer for Kafka producers.

Implements the Confluent wire format so messages produced here can be
consumed by any Confluent-compatible consumer (Kafka Streams, ksqlDB,
Spark Structured Streaming with the Confluent deserializer, etc.).

Confluent wire format (5-byte magic header):
    byte 0:   magic byte = 0x00
    bytes 1-4: schema ID as a big-endian 32-bit integer
    bytes 5+: Avro-encoded payload

Why the magic header matters:
    Without it, a consumer has no way to know which schema was used to
    encode the message. The schema ID tells the consumer exactly which
    version to fetch from the registry for deserialization — this is
    what makes schema evolution safe. A consumer running schema v1 can
    still deserialize messages encoded with v2 as long as the schemas
    are BACKWARD compatible, because it fetches v2's schema and uses
    it to decode the bytes into the v1 shape it expects.

Relationship to the Schema Registry client (src/avro/registry_client.py):
    The registry client handles HTTP — registering schemas, checking
    compatibility, fetching by ID. This serializer handles the wire
    format — turning a Python dict into the bytes that go on the wire.
    The producer uses both: registry client to get the schema ID, this
    serializer to encode the payload.
"""
import io
import json
import struct
import logging
from typing import Any

import fastavro

from src.avro.registry_client import SchemaRegistryClient, SchemaRegistryError

logger = logging.getLogger(__name__)

MAGIC_BYTE = 0x00


class AvroSerializationError(Exception):
    """Raised when a message cannot be serialized to Avro."""
    pass


class AvroSerializer:
    """
    Serializes Python dicts to Confluent wire format using Avro.

    Usage:
        serializer = AvroSerializer(
            registry_url="http://localhost:8081",
            subject="weather-value",
            schema_path="schemas/weather_v1.avsc",
        )
        encoded = serializer.serialize({"city": "Paris", ...})
        # encoded is bytes ready to pass to KafkaProducer as the value

    The schema is registered on first use and the ID is cached — every
    subsequent call uses the cached ID without hitting the registry.
    This means one registry roundtrip per process lifetime, not per
    message.
    """

    def __init__(
        self,
        registry_url: str,
        subject: str,
        schema_path: str,
    ):
        """
        Args:
            registry_url: Confluent Schema Registry base URL.
            subject: Schema subject name (convention: "<topic>-value").
            schema_path: Path to the .avsc schema file.
        """
        self.subject = subject
        self._client = SchemaRegistryClient(registry_url)
        self._schema_id: int | None = None
        self._parsed_schema: dict | None = None
        self._schema_path = schema_path

    def _load_schema(self) -> dict:
        """Load and parse the Avro schema from disk."""
        import fastavro.schema
        return fastavro.schema.load_schema(self._schema_path)

    def _ensure_registered(self) -> tuple[int, dict]:
        """
        Register the schema if not already registered and cache the ID.

        Returns (schema_id, parsed_schema).
        Raises AvroSerializationError if registration fails.
        """
        if self._schema_id is not None:
            return self._schema_id, self._parsed_schema

        try:
            schema = self._load_schema()
            # load_schema returns a parsed schema dict; we need the raw
            # dict for registry registration (JSON-serializable form).
            with open(self._schema_path) as f:
                import json as _json
                raw_schema = _json.load(f)

            schema_id = self._client.register_schema(self.subject, raw_schema)
            self._schema_id = schema_id
            self._parsed_schema = schema
            logger.info(
                "Schema registered under subject '%s' with ID %d",
                self.subject,
                schema_id,
            )
            return schema_id, schema
        except SchemaRegistryError as e:
            raise AvroSerializationError(
                f"Failed to register schema under subject '{self.subject}': {e}"
            ) from e

    def serialize(self, record: dict) -> bytes:
        """
        Serialize a Python dict to Confluent wire format.

        Args:
            record: The message payload as a Python dict. Must match
                    the schema fields — extra fields are ignored by
                    fastavro; missing required fields raise an error.

        Returns:
            bytes in Confluent wire format:
            [0x00][schema_id: 4 bytes big-endian][avro payload]

        Raises:
            AvroSerializationError: if serialization fails for any reason.
        """
        try:
            schema_id, parsed_schema = self._ensure_registered()

            buf = io.BytesIO()
            # Magic byte
            buf.write(struct.pack("b", MAGIC_BYTE))
            # Schema ID as big-endian 32-bit int
            buf.write(struct.pack(">I", schema_id))
            # Avro-encoded payload
            fastavro.schemaless_writer(buf, parsed_schema, record)

            return buf.getvalue()

        except AvroSerializationError:
            raise
        except Exception as e:
            raise AvroSerializationError(
                f"Failed to serialize record for city "
                f"'{record.get('city', 'unknown')}': {e}"
            ) from e

    def deserialize(self, data: bytes) -> dict:
        """
        Deserialize Confluent wire format bytes back to a Python dict.

        Validates the magic byte and extracts the schema ID, then
        deserializes the Avro payload. Useful for testing round-trips.

        Args:
            data: bytes in Confluent wire format.

        Returns:
            The deserialized Python dict.

        Raises:
            AvroSerializationError: if the magic byte is wrong or
                deserialization fails.
        """
        try:
            if len(data) < 5:
                raise AvroSerializationError(
                    f"Message too short for Confluent wire format: {len(data)} bytes"
                )

            magic = struct.unpack("b", data[0:1])[0]
            if magic != MAGIC_BYTE:
                raise AvroSerializationError(
                    f"Invalid magic byte: expected {MAGIC_BYTE}, got {magic}"
                )

            schema_id = struct.unpack(">I", data[1:5])[0]
            payload = data[5:]

            if self._parsed_schema is None:
                self._parsed_schema = self._load_schema()

            buf = io.BytesIO(payload)
            return fastavro.schemaless_reader(buf, self._parsed_schema)

        except AvroSerializationError:
            raise
        except Exception as e:
            raise AvroSerializationError(
                f"Failed to deserialize message: {e}"
            ) from e