"""
Confluent Schema Registry client for Avro schema management.

Wraps the Schema Registry REST API to handle:
- Schema registration under a subject (topic + "-value" convention)
- BACKWARD compatibility checking before registration
- Schema retrieval by ID for deserialization

BACKWARD compatibility means a new schema can read data written with
the previous schema. Concretely:
- Adding a field with a default value: COMPATIBLE (old data lacks the
  field; the default fills it in when read by new consumers)
- Removing a required field: INCOMPATIBLE (old data has the field;
  old consumers expect it but new producers won't send it)

The registry enforces this automatically when compatibility is set —
we don't implement the logic ourselves, we let the registry decide
and surface the result clearly.
"""
import json
import requests


class SchemaRegistryError(Exception):
    """Raised when the Schema Registry returns an error response."""
    pass


class SchemaRegistryClient:
    """
    Thin client for the Confluent Schema Registry REST API.

    The registry is addressed by base URL. Every schema is registered
    under a subject — by convention, "<topic-name>-value" for the
    value side of a Kafka topic. The subject is where compatibility
    rules are enforced: the registry checks the new schema against
    every previously registered schema under that subject.
    """

    def __init__(self, base_url: str):
        """
        Args:
            base_url: Schema Registry URL, e.g. http://localhost:8081
        """
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        )

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def set_compatibility(self, subject: str, level: str) -> dict:
        """
        Set the compatibility level for a subject.

        Args:
            subject: Schema subject name (e.g. "weather-value")
            level: Compatibility level — BACKWARD, FORWARD, FULL, or NONE

        BACKWARD is the default and the safest choice for most pipelines:
        new schema can read old data, so consumers can be upgraded before
        producers without breaking anything.
        """
        response = self.session.put(
            self._url(f"/config/{subject}"),
            data=json.dumps({"compatibility": level}),
        )
        if response.status_code not in (200, 201):
            raise SchemaRegistryError(
                f"Failed to set compatibility: {response.status_code} {response.text}"
            )
        return response.json()

    def check_compatibility(self, subject: str, schema: dict) -> bool:
        """
        Check whether a schema is compatible with the latest registered
        schema under a subject, without registering it.

        Returns True if compatible, False if not.
        Raises SchemaRegistryError if the request itself fails.
        """
        response = self.session.post(
            self._url(f"/compatibility/subjects/{subject}/versions/latest"),
            data=json.dumps({"schema": json.dumps(schema)}),
        )
        if response.status_code == 404:
            # No schema registered yet — any schema is compatible
            return True
        if response.status_code not in (200, 201):
            raise SchemaRegistryError(
                f"Compatibility check failed: {response.status_code} {response.text}"
            )
        return response.json().get("is_compatible", False)

    def register_schema(self, subject: str, schema: dict) -> int:
        """
        Register a schema under a subject.

        Returns the schema ID assigned by the registry.
        Raises SchemaRegistryError if registration fails (including
        compatibility violations — the registry enforces the rule and
        returns 409 Conflict).
        """
        response = self.session.post(
            self._url(f"/subjects/{subject}/versions"),
            data=json.dumps({"schema": json.dumps(schema)}),
        )
        if response.status_code not in (200, 201):
            raise SchemaRegistryError(
                f"Schema registration failed: {response.status_code} {response.text}"
            )
        return response.json()["id"]

    def get_schema_by_id(self, schema_id: int) -> dict:
        """
        Fetch a schema by its registry ID.

        Used by consumers to deserialize messages: the producer embeds
        the schema ID in the message wire format, and the consumer fetches
        the schema to deserialize the payload.
        """
        response = self.session.get(self._url(f"/schemas/ids/{schema_id}"))
        if response.status_code != 200:
            raise SchemaRegistryError(
                f"Schema fetch failed: {response.status_code} {response.text}"
            )
        return json.loads(response.json()["schema"])

    def list_subjects(self) -> list:
        """List all subjects registered in the registry."""
        response = self.session.get(self._url("/subjects"))
        if response.status_code != 200:
            raise SchemaRegistryError(
                f"Failed to list subjects: {response.status_code} {response.text}"
            )
        return response.json()

    def is_healthy(self) -> bool:
        """Check whether the registry is reachable."""
        try:
            response = self.session.get(self._url("/subjects"), timeout=5)
            return response.status_code == 200
        except requests.exceptions.ConnectionError:
            return False