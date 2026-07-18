"""
Data contract for weather events flowing through the Kafka pipeline.

A data contract is a formal agreement between the producer and consumer
about the structure, types, and business rules of data. It sits at the
pipeline boundary and enforces three things:

1. Schema compliance — required fields are present, types are correct.
   Handled by the existing Pydantic WeatherData model.

2. Business rule compliance — values are physically plausible (temperature
   within earthly range, humidity a percentage, pressure within atmosphere).
   These rules exist independently of serialization format: Avro enforces
   the wire schema; the contract enforces meaning.

3. Version tracking — the contract has a version number. When a breaking
   change is introduced (removing a field, tightening a constraint), the
   version increments and consumers can detect the change before it
   causes a silent failure.

Why this matters in production:
Without a contract, a producer can silently send humidity=150 (sensor
fault) or temperature=999 (API error default). The consumer stores it,
dbt models aggregate it, dashboards show impossible averages, and nobody
knows why until a user reports that Paris is showing 999°C. The contract
catches this at the pipeline entry point — before it touches the database.

Relationship to the Avro schema registry:
The Avro schema (schemas/weather_v1.avsc) enforces structural compatibility
across schema versions. The data contract enforces business rules within a
single version. They are complementary: Avro says "this field is a double",
the contract says "this double must be between -90 and 60".
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pydantic import ValidationError

from src.validation.schema import WeatherData

logger = logging.getLogger(__name__)

CONTRACT_VERSION = "1.0.0"


@dataclass
class ContractViolation:
    """
    Represents a single rule violation found during contract validation.

    field: the name of the field that violated the rule, or "structure"
           for missing/extra fields.
    rule: human-readable description of the rule that was violated.
    value: the actual value that caused the violation.
    """

    field: str
    rule: str
    value: Any


@dataclass
class ContractResult:
    """
    The outcome of validating a message against the contract.

    valid: True if all rules passed, False if any failed.
    violations: list of ContractViolation for every rule that failed.
                Empty when valid=True.
    contract_version: the version of the contract that was applied.
                      Consumers can use this to detect breaking changes.
    """

    valid: bool
    violations: list = field(default_factory=list)
    contract_version: str = CONTRACT_VERSION


class WeatherDataContract:
    """
    Enforces the data contract for weather events entering the Kafka pipeline.

    Usage:
        contract = WeatherDataContract()
        result = contract.validate(message_dict)
        if not result.valid:
            for v in result.violations:
                logger.error("Contract violation: %s — %s (got %r)", v.field, v.rule, v.value)

    The contract validates in two passes:
    1. Pydantic validation — catches type errors and missing fields
    2. Business rule validation — catches physically implausible values
       that Pydantic accepts (e.g. temperature=59.99 is valid Pydantic
       but temperature=999 would be caught by Pydantic's ge/le constraints
       only if they're set correctly; explicit rules here make the intent clear)
    """

    VERSION = CONTRACT_VERSION

    # Business rule thresholds — defined as class constants so they
    # can be referenced in tests without magic numbers.
    TEMP_MIN = -90.0
    TEMP_MAX = 60.0
    HUMIDITY_MIN = 0
    HUMIDITY_MAX = 100
    PRESSURE_MIN = 800
    PRESSURE_MAX = 1100
    WIND_SPEED_MIN = 0.0
    CITY_MAX_LENGTH = 100
    COUNTRY_MAX_LENGTH = 10

    def validate(self, message: dict) -> ContractResult:
        """
        Validate a raw message dict against the contract.

        Returns a ContractResult — never raises. The caller decides
        whether to reject, quarantine, or DLQ the message.
        """
        violations = []

        # Pass 1: Pydantic structural validation.
        # Catches missing required fields, wrong types, and the
        # basic range constraints already defined on the model.
        try:
            data = WeatherData(**message)
        except ValidationError as e:
            for err in e.errors():
                field_name = ".".join(str(loc) for loc in err["loc"])
                violations.append(
                    ContractViolation(
                        field=field_name,
                        rule=err["msg"],
                        value=message.get(field_name, "<missing>"),
                    )
                )
            # Structural validation failed — business rules can't run
            # without a valid object, so return early.
            return ContractResult(valid=False, violations=violations)

        # Pass 2: Business rule validation.
        # These rules express domain knowledge that goes beyond type
        # checking — what values are physically meaningful for weather data.
        violations.extend(self._check_business_rules(data, message))

        return ContractResult(
            valid=len(violations) == 0,
            violations=violations,
        )

    def _check_business_rules(
        self, data: WeatherData, raw: dict
    ) -> list:
        """
        Domain-specific rules that Pydantic constraints already enforce
        but are made explicit here for documentation and testability.

        Also catches rules that Pydantic can't express, like:
        - feels_like should be within a plausible delta of temperature
        - recorded_at should not be in the future
        """
        violations = []

        # Temperature sanity — Pydantic enforces ge/le but we make
        # the business intent explicit.
        if not (self.TEMP_MIN <= data.temperature <= self.TEMP_MAX):
            violations.append(
                ContractViolation(
                    field="temperature",
                    rule=f"must be between {self.TEMP_MIN} and {self.TEMP_MAX}°C",
                    value=data.temperature,
                )
            )

        # feels_like vs temperature coherence — a feels_like that differs
        # from temperature by more than 30°C is almost certainly a sensor
        # fault or API default value, not real meteorological data.
        feels_delta = abs(data.feels_like - data.temperature)
        if feels_delta > 30:
            violations.append(
                ContractViolation(
                    field="feels_like",
                    rule="must be within 30°C of temperature (large delta suggests sensor fault)",
                    value=data.feels_like,
                )
            )

        # Humidity is a percentage.
        if not (self.HUMIDITY_MIN <= data.humidity <= self.HUMIDITY_MAX):
            violations.append(
                ContractViolation(
                    field="humidity",
                    rule=f"must be between {self.HUMIDITY_MIN} and {self.HUMIDITY_MAX}%",
                    value=data.humidity,
                )
            )

        # Pressure within earthly atmosphere range.
        if not (self.PRESSURE_MIN <= data.pressure <= self.PRESSURE_MAX):
            violations.append(
                ContractViolation(
                    field="pressure",
                    rule=f"must be between {self.PRESSURE_MIN} and {self.PRESSURE_MAX} hPa",
                    value=data.pressure,
                )
            )

        # Wind speed cannot be negative.
        if data.wind_speed < self.WIND_SPEED_MIN:
            violations.append(
                ContractViolation(
                    field="wind_speed",
                    rule="must be non-negative",
                    value=data.wind_speed,
                )
            )

        # recorded_at should not be in the future — a future timestamp
        # almost always indicates a clock skew or default value bug.
        if data.recorded_at > datetime.utcnow():
            violations.append(
                ContractViolation(
                    field="recorded_at",
                    rule="must not be in the future",
                    value=data.recorded_at.isoformat(),
                )
            )

        return violations

    def validate_batch(self, messages: list[dict]) -> dict:
        """
        Validate a batch of messages and return a summary.

        Returns a dict with:
        - total: number of messages validated
        - valid: number that passed
        - invalid: number that failed
        - violations: list of (index, ContractResult) for failed messages
        """
        results = [self.validate(m) for m in messages]
        invalid = [(i, r) for i, r in enumerate(results) if not r.valid]

        return {
            "total": len(messages),
            "valid": len(results) - len(invalid),
            "invalid": len(invalid),
            "violations": invalid,
            "contract_version": self.VERSION,
        }