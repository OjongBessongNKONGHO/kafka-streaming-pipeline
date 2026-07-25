"""
Confluent-compatible Avro deserializer for Kafka consumers.

Mirrors the AvroSerializer in src/avro/serializer.py — reads the same
Confluent wire format that the producer writes:

    byte 0:   magic byte = 0x00
    bytes 1-4: schema ID as a big-endian 32-bit integer
    bytes 5+: Avro-encoded payload

Why the schema ID matters for consumers:
    The producer embeds the schema ID used to encode each message.
    The consumer fetches that exact schema version from the registry
    and uses it to decode the bytes. This is what makes schema
    evolution safe: a consumer running schema v1 can receive messages
    encoded with v2 as long as the schemas are BACKWARD compatible,
    because it fetches v2's schema and decodes the bytes correctly
    into the shape it expects.

Schema caching:
    Parsed schemas are cached by ID after first fetch. In a pipeline
    processing thousands of messages per second, hitting the registry
    on every message would add significant latency. The cache means
    one HTTP roundtrip per schema version per process lifetime.

Relationship to AvroSerializer:
    The serializer encodes Python dicts to wire format bytes.
    The deserializer decodes wire format bytes back to Python dicts.
    Both understand the same 5-byte header format.
"""
import io
import struct
import logging
from typing import Any

import fastavro

from src.avro.registry_client import SchemaRegistryClient, SchemaRegistryError

logger = logging.getLogger(__name__)

MAGIC_BYTE = 0x00


class AvroDeserializationError(Exception):
    """Raised when a message cannot be deserialized from Avro wire format."""
    pass


class AvroDeserializer:
    """
    Deserializes Confluent wire format bytes back to Python dicts.

    Usage:
        deserializer = AvroDeserializer(
            registry_url="http://localhost:8081",
            schema_path="schemas/weather_v1.avsc",
        )
        record = deserializer.deserialize(raw_bytes)
        # record is a Python dict ready for processing

    The schema path is used as the reader schema — the schema the
    consumer expects. The writer schema (identified by the schema ID
    in the message) is fetched from the registry. fastavro handles
    schema evolution between writer and reader schemas automatically,
    filling in defaults for fields present in the reader but absent
    in the writer, and ignoring fields in the writer not in the reader.

    Schema IDs are cached after first fetch — one registry roundtrip
    per schema version per process lifetime.
    """

    def __init__(
        self,
        registry_url: str,
        schema_path: str,
    ):
        """
        Args:
            registry_url: Confluent Schema Registry base URL.
            schema_path: Path to the reader schema .avsc file.
                         This is the schema the consumer expects —
                         may differ from the writer schema if the
                         producer has published a newer version.
        """
        self._client = SchemaRegistryClient(registry_url)
        self._schema_path = schema_path
        self._reader_schema = None
        self._writer_schema_cache: dict[int, dict] = {}

    def _get_reader_schema(self) -> dict:
        """Load and cache the reader schema from disk."""
        if self._reader_schema is None:
            import fastavro.schema
            self._reader_schema = fastavro.schema.load_schema(self._schema_path)
        return self._reader_schema

    def _get_writer_schema(self, schema_id: int) -> dict:
        """
        Fetch and cache the writer schema by ID from the registry.

        The writer schema is the schema the producer used to encode
        the message. It may be a different version than the reader
        schema if the producer has published schema updates.

        Raises AvroDeserializationError if the registry is unreachable
        or the schema ID does not exist.
        """
        if schema_id not in self._writer_schema_cache:
            try:
                raw_schema = self._client.get_schema_by_id(schema_id)
                parsed = fastavro.schema.parse_schema(raw_schema)
                self._writer_schema_cache[schema_id] = parsed
                logger.debug("Cached writer schema for ID %d", schema_id)
            except SchemaRegistryError as e:
                raise AvroDeserializationError(
                    f"Failed to fetch writer schema for ID {schema_id}: {e}"
                ) from e
        return self._writer_schema_cache[schema_id]

    def deserialize(self, data: bytes) -> dict:
        """
        Deserialize Confluent wire format bytes to a Python dict.

        Args:
            data: Raw bytes from Kafka message value, in Confluent
                  wire format: [0x00][schema_id: 4 bytes][avro payload]

        Returns:
            Deserialized Python dict matching the reader schema.

        Raises:
            AvroDeserializationError: if the magic byte is wrong,
                the message is too short, the schema cannot be fetched,
                or the Avro payload cannot be decoded.
        """
        try:
            if len(data) < 5:
                raise AvroDeserializationError(
                    f"Message too short for Confluent wire format: "
                    f"{len(data)} bytes, minimum 5 required"
                )

            magic = struct.unpack("b", data[0:1])[0]
            if magic != MAGIC_BYTE:
                raise AvroDeserializationError(
                    f"Invalid magic byte: expected {MAGIC_BYTE}, got {magic}. "
                    f"Message may not be in Confluent wire format — "
                    f"check if the producer uses AvroSerializer."
                )

            schema_id = struct.unpack(">I", data[1:5])[0]
            payload = data[5:]

            writer_schema = self._get_writer_schema(schema_id)
            reader_schema = self._get_reader_schema()

            buf = io.BytesIO(payload)
            record = fastavro.schemaless_reader(buf, writer_schema, reader_schema)

            return record

        except AvroDeserializationError:
            raise
        except Exception as e:
            raise AvroDeserializationError(
                f"Failed to deserialize message: {e}"
            ) from e