from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Final

from aegis_alpha.collection.usage_checkpoint_errors import (
    CheckpointContractError,
    UnsupportedContractError,
)
from aegis_alpha.data.serialization import canonical_json_bytes

SCHEMA_VERSION: Final = 1
SIGNATURE_ALGORITHM: Final = "ed25519"
SIGNATURE_VERSION: Final = 1
ORDERING_VERSION: Final = 1
USAGE_RECORD_ORDERING: Final = (
    "recorded_at_utc,run_id,usage_seq,plan_id,plan_sha256,provider,metric,quantity,unit"
)
_ROOT_DOMAIN: Final = b"aegis-alpha/collection/usage-record-root/v1\x00"
_PAYLOAD_DOMAIN: Final = "aegis-alpha/collection/usage-checkpoint/v1"
_LEAF_DOMAIN: Final = "aegis-alpha/collection/usage-record-leaf/v1"
_SHA256_PATTERN: Final = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")
_PROVIDER_PATTERN: Final = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_QUANTUM: Final = Decimal("0.000001")
_PROVIDER_MAX_LENGTH: Final = 100
_NUMERIC_MAX_INTEGER_DIGITS: Final = 18


def _reject(field: str, requirement: str) -> None:
    raise CheckpointContractError(field=field, requirement=requirement)


def _identifier(field: str, value: str, maximum: int) -> None:
    if (
        not isinstance(value, str)
        or len(value) > maximum
        or not _IDENTIFIER_PATTERN.fullmatch(value)
    ):
        _reject(field, "must be a normalized ASCII identifier")


def _provider(value: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) > _PROVIDER_MAX_LENGTH
        or not _PROVIDER_PATTERN.fullmatch(value)
    ):
        _reject("provider", "must be a normalized lowercase provider identifier")


def _sha256(field: str, value: str) -> None:
    if not isinstance(value, str) or not _SHA256_PATTERN.fullmatch(value):
        _reject(field, "must be lowercase SHA-256 hexadecimal")


def _utc(field: str, value: datetime) -> datetime:
    if not isinstance(value, datetime):
        _reject(field, "must be a UTC timestamp")
    offset = value.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        _reject(field, "must be a UTC timestamp")
    return value.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _quantity_text(value: Decimal) -> str:
    if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
        _reject("quantity", "must be a finite nonnegative Decimal")
    try:
        quantized = value.quantize(_QUANTUM)
    except InvalidOperation:
        _reject("quantity", "must fit NUMERIC(24,6)")
    if quantized != value or (
        quantized != 0 and quantized.adjusted() >= _NUMERIC_MAX_INTEGER_DIGITS
    ):
        _reject("quantity", "must fit NUMERIC(24,6)")
    return f"{quantized:.6f}"


@dataclass(frozen=True, slots=True)
class UsageRecordLeaf:
    run_id: str
    usage_seq: int
    plan_id: str
    plan_sha256: str
    provider: str
    metric: str
    quantity: Decimal
    unit: str
    recorded_at_utc: datetime

    def __post_init__(self) -> None:
        _identifier("run_id", self.run_id, 255)
        _identifier("plan_id", self.plan_id, 255)
        _sha256("plan_sha256", self.plan_sha256)
        _provider(self.provider)
        _identifier("metric", self.metric, 100)
        _identifier("unit", self.unit, 50)
        if type(self.usage_seq) is not int or self.usage_seq < 1:
            _reject("usage_seq", "must be a positive integer")
        _quantity_text(self.quantity)
        object.__setattr__(self, "recorded_at_utc", _utc("recorded_at_utc", self.recorded_at_utc))

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(
            {
                "contract": _LEAF_DOMAIN,
                "metric": self.metric,
                "plan_id": self.plan_id,
                "plan_sha256": self.plan_sha256,
                "provider": self.provider,
                "quantity": _quantity_text(self.quantity),
                "recorded_at_utc": _utc_text(self.recorded_at_utc),
                "run_id": self.run_id,
                "unit": self.unit,
                "usage_seq": self.usage_seq,
            }
        )

    def ordering_key(self) -> tuple[str, str, int, str, str, str, str, str, str]:
        return (
            _utc_text(self.recorded_at_utc),
            self.run_id,
            self.usage_seq,
            self.plan_id,
            self.plan_sha256,
            self.provider,
            self.metric,
            _quantity_text(self.quantity),
            self.unit,
        )


def usage_records_root(leaves: Sequence[UsageRecordLeaf]) -> str:
    digest = hashlib.sha256(_ROOT_DOMAIN)
    previous_key: tuple[str, str, int, str, str, str, str, str, str] | None = None
    identities: set[tuple[str, int]] = set()
    for leaf in leaves:
        identity = (leaf.run_id, leaf.usage_seq)
        if identity in identities:
            _reject("usage leaves", "must not contain duplicate run_id/usage_seq identities")
        key = leaf.ordering_key()
        if previous_key is not None and key <= previous_key:
            _reject("usage leaves", f"must be ordered by {USAGE_RECORD_ORDERING}")
        encoded = leaf.canonical_bytes()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        identities.add(identity)
        previous_key = key
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class UsageCheckpoint:
    checkpoint_id: str
    provider: str
    coverage_start_utc: datetime
    coverage_end_utc: datetime
    usage_record_count: int
    usage_records_root_sha256: str
    authority_id: str
    key_id: str
    generated_at_utc: datetime
    schema_version: int = SCHEMA_VERSION
    signature_algorithm: str = SIGNATURE_ALGORITHM
    signature_version: int = SIGNATURE_VERSION
    ordering_version: int = ORDERING_VERSION

    def __post_init__(self) -> None:
        _identifier("checkpoint_id", self.checkpoint_id, 255)
        _provider(self.provider)
        _identifier("authority_id", self.authority_id, 255)
        _identifier("key_id", self.key_id, 255)
        _sha256("usage_records_root_sha256", self.usage_records_root_sha256)
        start = _utc("coverage_start_utc", self.coverage_start_utc)
        end = _utc("coverage_end_utc", self.coverage_end_utc)
        generated = _utc("generated_at_utc", self.generated_at_utc)
        if end <= start:
            _reject("coverage interval", "must be nonempty and increasing")
        if generated < end:
            _reject("generated_at_utc", "must not precede coverage_end_utc")
        if type(self.usage_record_count) is not int or self.usage_record_count < 0:
            _reject("usage_record_count", "must be a nonnegative integer")
        versions = (
            ("schema_version", self.schema_version, SCHEMA_VERSION),
            ("signature_version", self.signature_version, SIGNATURE_VERSION),
            ("ordering_version", self.ordering_version, ORDERING_VERSION),
        )
        for field, observed, supported in versions:
            if type(observed) is not int or observed != supported:
                raise UnsupportedContractError(field=field, requirement="is unsupported")
        if self.signature_algorithm != SIGNATURE_ALGORITHM:
            raise UnsupportedContractError(
                field="signature_algorithm", requirement="is unsupported"
            )
        object.__setattr__(self, "coverage_start_utc", start)
        object.__setattr__(self, "coverage_end_utc", end)
        object.__setattr__(self, "generated_at_utc", generated)

    @classmethod
    def build(  # noqa: PLR0913 - fields mirror the signed schema contract
        cls,
        *,
        checkpoint_id: str,
        provider: str,
        coverage_start_utc: datetime,
        coverage_end_utc: datetime,
        authority_id: str,
        key_id: str,
        generated_at_utc: datetime,
        leaves: Sequence[UsageRecordLeaf],
    ) -> UsageCheckpoint:
        leaf_tuple = tuple(leaves)
        start = _utc("coverage_start_utc", coverage_start_utc)
        end = _utc("coverage_end_utc", coverage_end_utc)
        _provider(provider)
        _validate_leaf_scope(provider, start, end, leaf_tuple)
        return cls(
            checkpoint_id=checkpoint_id,
            provider=provider,
            coverage_start_utc=start,
            coverage_end_utc=end,
            usage_record_count=len(leaf_tuple),
            usage_records_root_sha256=usage_records_root(leaf_tuple),
            authority_id=authority_id,
            key_id=key_id,
            generated_at_utc=generated_at_utc,
        )

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(
            {
                "authority_id": self.authority_id,
                "checkpoint_id": self.checkpoint_id,
                "contract": _PAYLOAD_DOMAIN,
                "coverage_end_utc": _utc_text(self.coverage_end_utc),
                "coverage_start_utc": _utc_text(self.coverage_start_utc),
                "generated_at_utc": _utc_text(self.generated_at_utc),
                "key_id": self.key_id,
                "ordering_version": self.ordering_version,
                "provider": self.provider,
                "schema_version": self.schema_version,
                "signature_algorithm": self.signature_algorithm,
                "signature_version": self.signature_version,
                "usage_record_count": self.usage_record_count,
                "usage_record_ordering": USAGE_RECORD_ORDERING,
                "usage_records_root_sha256": self.usage_records_root_sha256,
            }
        )


def _validate_leaf_scope(
    provider: str,
    coverage_start_utc: datetime,
    coverage_end_utc: datetime,
    leaves: Sequence[UsageRecordLeaf],
) -> None:
    for leaf in leaves:
        if leaf.provider != provider:
            _reject("usage leaf provider", "must match the checkpoint provider")
        if not coverage_start_utc <= leaf.recorded_at_utc < coverage_end_utc:
            _reject("usage leaf timestamp", "must be inside checkpoint coverage [start,end)")
