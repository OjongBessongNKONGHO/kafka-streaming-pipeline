"""
Avro schema compatibility tests against a live Confluent Schema Registry.

These tests require the Schema Registry to be running:
    docker compose up schema-registry -d

They are skipped automatically when the registry is unreachable, so they
never block CI (which runs without Docker). The skip is explicit and
labelled — not a silent pass — so the absence of the registry is visible
in the test output rather than hidden.

What we're proving:
- v1 schema registers cleanly under BACKWARD compatibility
- v2_compatible (adds optional field with default) is accepted
- v2_breaking (removes a required field) is rejected
- Schema IDs are stable — fetching by ID returns the original schema
- The registry enforces BACKWARD compatibility, not us
"""
import json
import pytest
from src.avro.registry_client import SchemaRegistryClient, SchemaRegistryError

REGISTRY_URL = "http://localhost:8081"
SUBJECT = "weather-value-test"


def load_schema(filename: str) -> dict:
    with open(f"schemas/{filename}") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def registry():
    """
    Returns a SchemaRegistryClient if the registry is reachable.
    Skips the entire module otherwise — explicit, not silent.
    """
    client = SchemaRegistryClient(REGISTRY_URL)
    if not client.is_healthy():
        pytest.skip(
            "Schema Registry not reachable at http://localhost:8081 — "
            "start it with: docker compose up schema-registry -d"
        )
    return client


@pytest.fixture(autouse=True, scope="module")
def cleanup(registry):
    """
    Deletes the test subject before and after the module runs.
    Keeps the registry clean between test runs — without this,
    re-running tests against a live registry would accumulate stale
    schema versions and interfere with compatibility checks.
    """
    _delete_subject(registry, SUBJECT)
    yield
    _delete_subject(registry, SUBJECT)


def _delete_subject(client: SchemaRegistryClient, subject: str):
    """Best-effort subject deletion — ignores 404 (subject didn't exist)."""
    try:
        import requests
        response = client.session.delete(
            f"{client.base_url}/subjects/{subject}"
        )
        if response.status_code not in (200, 404):
            pass  # non-fatal — test isolation is best-effort
    except Exception:
        pass


class TestSchemaRegistration:
    def test_v1_registers_successfully(self, registry):
        """
        The baseline schema must register without error.
        This is the contract the rest of the pipeline depends on.
        """
        schema = load_schema("weather_v1.avsc")
        registry.set_compatibility(SUBJECT, "BACKWARD")
        schema_id = registry.register_schema(SUBJECT, schema)
        assert isinstance(schema_id, int)
        assert schema_id > 0

    def test_registered_schema_retrievable_by_id(self, registry):
        """
        Schema IDs must be stable and the registry must return the
        exact schema that was registered. Consumers embed the schema ID
        in every message — if retrieval is broken, deserialization breaks.
        """
        schema = load_schema("weather_v1.avsc")
        schema_id = registry.register_schema(SUBJECT, schema)
        retrieved = registry.get_schema_by_id(schema_id)
        assert retrieved["name"] == "WeatherData"
        assert retrieved["namespace"] == "com.weatherapi.streaming"
        assert any(f["name"] == "humidity" for f in retrieved["fields"])


class TestBackwardCompatibility:
    def test_compatible_schema_is_accepted(self, registry):
        """
        Adding an optional field with a default is BACKWARD compatible.
        The registry must accept it — old consumers can read new messages
        by using the default for the missing field.
        """
        v1 = load_schema("weather_v1.avsc")
        registry.set_compatibility(SUBJECT, "BACKWARD")
        registry.register_schema(SUBJECT, v1)

        v2_compatible = load_schema("weather_v2_compatible.avsc")
        is_compatible = registry.check_compatibility(SUBJECT, v2_compatible)
        assert is_compatible is True

        schema_id = registry.register_schema(SUBJECT, v2_compatible)
        assert schema_id > 0

    def test_breaking_schema_is_rejected(self, registry):
        """
        Changing a field's type is NOT backward compatible.
        The registry must reject it — old consumers expecting humidity
        as int will fail to deserialize messages where it is a string.

        This is the core value of a schema registry: catching breaking
        changes before they reach production consumers.
        """
        v1 = load_schema("weather_v1.avsc")
        registry.set_compatibility(SUBJECT, "BACKWARD")
        registry.register_schema(SUBJECT, v1)

        v2_breaking = load_schema("weather_v2_breaking.avsc")
        is_compatible = registry.check_compatibility(SUBJECT, v2_breaking)
        assert is_compatible is False

    def test_breaking_schema_registration_raises(self, registry):
        """
        Attempting to register a schema with a type change must raise
        SchemaRegistryError — the registry enforces BACKWARD compatibility
        and returns 409 Conflict, which our client surfaces as an exception.
        """
        v1 = load_schema("weather_v1.avsc")
        registry.set_compatibility(SUBJECT, "BACKWARD")
        registry.register_schema(SUBJECT, v1)

        v2_breaking = load_schema("weather_v2_breaking.avsc")
        with pytest.raises(SchemaRegistryError):
            registry.register_schema(SUBJECT, v2_breaking)


class TestRegistryHealth:
    def test_registry_is_healthy(self, registry):
        """Baseline — registry must be reachable for all other tests."""
        assert registry.is_healthy() is True

    def test_subject_appears_in_registry(self, registry):
        """
        After registration, the subject must appear in the registry's
        subject list — proves the schema is durably stored, not just
        acknowledged and discarded.
        """
        schema = load_schema("weather_v1.avsc")
        registry.set_compatibility(SUBJECT, "BACKWARD")
        registry.register_schema(SUBJECT, schema)
        subjects = registry.list_subjects()
        assert SUBJECT in subjects