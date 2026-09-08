from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import cast

from aegis_alpha.metadata.records import (
    freeze_json_metadata,
    reject_credential_metadata,
    validated_json_copy,
)

_SHA256_HEX_LENGTH = 64
_NUMERIC_24_6_MAX_INTEGER_DIGITS = 18


class CollectionMode(StrEnum):
    PROBE = "probe"
    INCREMENTAL = "incremental"
    BACKFILL = "backfill"


class RunEventType(StrEnum):
    ATTEMPT_STARTED = "attempt_started"
    ATTEMPT_SUCCEEDED = "attempt_succeeded"
    ATTEMPT_FAILED = "attempt_failed"
    RUN_SUCCEEDED = "run_succeeded"
    RUN_FAILED = "run_failed"
    RUN_CANCELLED = "run_cancelled"


TERMINAL_EVENT_TYPES = frozenset(
    {RunEventType.RUN_SUCCEEDED, RunEventType.RUN_FAILED, RunEventType.RUN_CANCELLED}
)
ATTEMPT_EVENT_TYPES = frozenset(
    {
        RunEventType.ATTEMPT_STARTED,
        RunEventType.ATTEMPT_SUCCEEDED,
        RunEventType.ATTEMPT_FAILED,
    }
)


def collection_plan_digest(plan: CollectionRunPlan) -> str:
    projection = {
        "dataset": plan.dataset,
        "mode": plan.mode.value,
        "parameters": validated_json_copy(plan.parameters),
        "provider": plan.provider,
        "requested_window_end": _window_literal(plan.requested_window_end),
        "requested_window_start": _window_literal(plan.requested_window_start),
        "schema_version": plan.schema_version,
    }
    canonical = json.dumps(
        projection,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


def _window_literal(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _require_nonempty(field_name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must be nonempty")


def _require_timezone_aware(field_name: str, value: datetime) -> None:
    if value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _frozen_json_object(field_name: str, value: object) -> Mapping[str, object]:
    frozen = freeze_json_metadata(value)
    reject_credential_metadata(frozen)
    if not isinstance(frozen, Mapping):
        raise TypeError(f"{field_name} must be a JSON object")
    return cast("Mapping[str, object]", frozen)


def _optional_window(
    field_name: str,
    start: datetime | None,
    end: datetime | None,
) -> None:
    if (start is None) != (end is None):
        raise ValueError(f"{field_name} window must be paired")
    if start is None or end is None:
        return
    _require_timezone_aware(f"{field_name} window start", start)
    _require_timezone_aware(f"{field_name} window end", end)
    if end < start:
        raise ValueError(f"{field_name} window end cannot precede its start")


@dataclass(frozen=True, slots=True)
class CollectionRunPlan:
    plan_id: str
    schema_version: int
    provider: str
    dataset: str
    mode: CollectionMode
    requested_window_start: datetime | None
    requested_window_end: datetime | None
    parameters: Mapping[str, object]
    created_at_utc: datetime
    plan_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonempty("plan_id", self.plan_id)
        _require_nonempty("provider", self.provider)
        _require_nonempty("dataset", self.dataset)
        if self.schema_version < 1:
            raise ValueError("schema_version must be positive")
        try:
            mode = CollectionMode(self.mode)
        except ValueError:
            allowed = ", ".join(member.value for member in CollectionMode)
            raise ValueError(f"mode must be one of: {allowed}") from None
        object.__setattr__(self, "mode", mode)
        _optional_window("requested", self.requested_window_start, self.requested_window_end)
        _require_timezone_aware("created_at_utc", self.created_at_utc)
        frozen_parameters = _frozen_json_object("plan parameters", self.parameters)
        object.__setattr__(self, "parameters", frozen_parameters)
        object.__setattr__(self, "plan_sha256", collection_plan_digest(self))


@dataclass(frozen=True, slots=True)
class CollectionRun:
    run_id: str
    plan_id: str
    created_at_utc: datetime

    def __post_init__(self) -> None:
        _require_nonempty("run_id", self.run_id)
        _require_nonempty("plan_id", self.plan_id)
        _require_timezone_aware("created_at_utc", self.created_at_utc)


@dataclass(frozen=True, slots=True)
class CollectionRunEvent:
    run_id: str
    event_type: RunEventType
    occurred_at_utc: datetime
    attempt_number: int | None = None
    retry_of_attempt: int | None = None
    error_class: str | None = None
    error_message: str | None = None
    details: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_nonempty("run_id", self.run_id)
        try:
            event_type = RunEventType(self.event_type)
        except ValueError:
            allowed = ", ".join(member.value for member in RunEventType)
            raise ValueError(f"event_type must be one of: {allowed}") from None
        object.__setattr__(self, "event_type", event_type)
        _require_timezone_aware("occurred_at_utc", self.occurred_at_utc)
        if event_type in ATTEMPT_EVENT_TYPES:
            if self.attempt_number is None or self.attempt_number < 1:
                raise ValueError("attempt_number is required for attempt events")
        elif self.attempt_number is not None:
            raise ValueError("attempt_number is forbidden on run-level events")
        if self.retry_of_attempt is not None and (
            event_type is not RunEventType.ATTEMPT_STARTED or self.retry_of_attempt < 1
        ):
            raise ValueError("retry_of_attempt is only allowed when an attempt starts")
        failed = event_type in {RunEventType.ATTEMPT_FAILED, RunEventType.RUN_FAILED}
        if failed and (self.error_class is None or not self.error_class.strip()):
            raise ValueError("error_class is required for failed events")
        if not failed and self.error_class is not None:
            raise ValueError("error_class is only allowed on failed events")
        if self.error_message is not None and self.error_class is None:
            raise ValueError("error_message requires an error_class")
        if self.error_message is not None:
            reject_credential_metadata(self.error_message)
        object.__setattr__(self, "details", _frozen_json_object("event details", self.details))


@dataclass(frozen=True, slots=True)
class CollectionRunState:
    run_id: str
    plan_id: str
    state: RunEventType | None
    terminal: bool
    attempt_count: int
    last_event_seq: int | None
    last_occurred_at_utc: datetime | None


@dataclass(frozen=True, slots=True)
class CollectionReceipt:
    run_id: str
    attempt_number: int
    source_snapshot_id: str
    observed_window_start: datetime | None = None
    observed_window_end: datetime | None = None
    row_count: int | None = None
    byte_count: int | None = None
    receipt_sha256: str | None = None

    def __post_init__(self) -> None:
        _require_nonempty("run_id", self.run_id)
        _require_nonempty("source_snapshot_id", self.source_snapshot_id)
        if self.attempt_number < 1:
            raise ValueError("attempt_number must be positive")
        _optional_window("observed", self.observed_window_start, self.observed_window_end)
        if self.row_count is not None and self.row_count < 0:
            raise ValueError("row_count cannot be negative")
        if self.byte_count is not None and self.byte_count < 0:
            raise ValueError("byte_count cannot be negative")
        if self.receipt_sha256 is not None and (
            len(self.receipt_sha256) != _SHA256_HEX_LENGTH
            or any(character not in "0123456789abcdef" for character in self.receipt_sha256)
        ):
            raise ValueError("receipt_sha256 must be lowercase SHA-256 hexadecimal")


@dataclass(frozen=True, slots=True)
class WatermarkAdvance:
    provider: str
    dataset: str
    stream: str
    run_id: str
    watermark_value: str
    watermark_position: datetime

    def __post_init__(self) -> None:
        _require_nonempty("provider", self.provider)
        _require_nonempty("dataset", self.dataset)
        _require_nonempty("stream", self.stream)
        _require_nonempty("run_id", self.run_id)
        _require_nonempty("watermark_value", self.watermark_value)
        _require_timezone_aware("watermark_position", self.watermark_position)


@dataclass(frozen=True, slots=True)
class CurrentWatermark:
    provider: str
    dataset: str
    stream: str
    watermark_seq: int
    run_id: str
    watermark_value: str
    watermark_position: datetime
    recorded_at_utc: datetime


@dataclass(frozen=True, slots=True)
class CollectionUsageRecord:
    run_id: str
    usage_seq: int
    metric: str
    quantity: Decimal
    unit: str
    recorded_at_utc: datetime
    evidence: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_nonempty("run_id", self.run_id)
        _require_nonempty("metric", self.metric)
        _require_nonempty("unit", self.unit)
        if self.usage_seq < 1:
            raise ValueError("usage_seq must be positive")
        if not isinstance(self.quantity, Decimal):
            raise TypeError("quantity must be a Decimal")
        if not self.quantity.is_finite() or self.quantity < 0:
            raise ValueError("quantity must be a finite nonnegative Decimal")
        try:
            quantized = self.quantity.quantize(Decimal("0.000001"))
        except InvalidOperation:
            raise ValueError("quantity must fit NUMERIC(24,6)") from None
        if (
            quantized != self.quantity
            or self.quantity.copy_abs().adjusted() >= _NUMERIC_24_6_MAX_INTEGER_DIGITS
        ):
            raise ValueError("quantity must fit NUMERIC(24,6)")
        _require_timezone_aware("recorded_at_utc", self.recorded_at_utc)
        object.__setattr__(self, "evidence", _frozen_json_object("usage evidence", self.evidence))
