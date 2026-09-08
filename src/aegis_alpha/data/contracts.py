from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from urllib.parse import parse_qsl, urlsplit

_SHA256_HEX_LENGTH = 64


class ValidationStatus(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    BLOCKED = "BLOCKED"


class AdjustmentBasis(StrEnum):
    RAW = "RAW"
    SPLIT_ADJUSTED = "SPLIT_ADJUSTED"
    TOTAL_RETURN = "TOTAL_RETURN"


class SourceRole(StrEnum):
    PRIMARY = "PRIMARY"
    HISTORICAL_BACKTEST_REFERENCE = "HISTORICAL_BACKTEST_REFERENCE"
    OFFICIAL_VERIFIER = "OFFICIAL_VERIFIER"
    REFERENCE_IDENTITY = "REFERENCE_IDENTITY"
    EMERGENCY_PAPER_ONLY = "EMERGENCY_PAPER_ONLY"
    MANUAL_EVIDENCE = "MANUAL_EVIDENCE"
    NO_SAFE_FALLBACK = "NO_SAFE_FALLBACK"


class QualityStatus(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    BLOCKED = "BLOCKED"


class MappingState(StrEnum):
    VERIFIED = "VERIFIED"
    AMBIGUOUS = "AMBIGUOUS"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    """Credential-free provenance declared for one immutable raw capture."""

    snapshot_id: str
    schema_version: int
    provider: str
    dataset: str
    source_uri: str
    request_fingerprint: str
    parameters: Mapping[str, str]
    requested_at_utc: datetime
    retrieved_at_utc: datetime
    content_type: str
    encoding: str | None
    compression: str | None
    raw_byte_length: int
    content_sha256: str
    parser_name: str
    parser_version: str
    validation_status: ValidationStatus
    provider_published_at: datetime | None = None
    provider_watermark: str | None = None
    observation_date: str | None = None
    row_count: int | None = None
    coverage_start: str | None = None
    coverage_end: str | None = None
    license_classification: str = "UNKNOWN"
    retention_classification: str = "UNKNOWN"

    def __post_init__(self) -> None:
        required_text = {
            "snapshot_id": self.snapshot_id,
            "provider": self.provider,
            "dataset": self.dataset,
            "source_uri": self.source_uri,
            "request_fingerprint": self.request_fingerprint,
            "content_type": self.content_type,
            "parser_name": self.parser_name,
            "parser_version": self.parser_version,
        }
        if missing := [name for name, value in required_text.items() if not value.strip()]:
            raise ValueError(f"required fields are empty: {', '.join(sorted(missing))}")
        if self.schema_version < 1:
            raise ValueError("schema_version must be positive")
        if self.raw_byte_length < 0:
            raise ValueError("raw_byte_length cannot be negative")
        _require_sha256("content_sha256", self.content_sha256)
        fingerprint_algorithm, separator, fingerprint_digest = self.request_fingerprint.partition(
            ":"
        )
        if separator != ":" or fingerprint_algorithm != "sha256":
            raise ValueError("request_fingerprint must use sha256:<lowercase hex>")
        _require_sha256("request_fingerprint digest", fingerprint_digest)
        if self.requested_at_utc.tzinfo is None or self.retrieved_at_utc.tzinfo is None:
            raise ValueError("snapshot timestamps must be timezone-aware")
        if self.retrieved_at_utc < self.requested_at_utc:
            raise ValueError("retrieved_at_utc cannot precede requested_at_utc")

        if forbidden := [name for name in self.parameters if _is_credential_name(name)]:
            raise ValueError(f"credential parameters are forbidden: {', '.join(sorted(forbidden))}")
        source = urlsplit(self.source_uri)
        if source.username is not None or source.password is not None:
            raise ValueError("credential userinfo is forbidden in source_uri")
        uri_keys = (
            name
            for component in (source.query, source.fragment)
            for name, _value in parse_qsl(component, keep_blank_values=True)
        )
        if any(_is_credential_name(name) for name in uri_keys):
            raise ValueError("credential query parameters are forbidden in source_uri")
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))


@dataclass(frozen=True, slots=True)
class DataObservation:
    """A value with stable identity, availability time, and raw lineage."""

    observation_id: str
    schema_version: int
    instrument_id: str | None
    series_id: str | None
    value: Decimal
    unit: str
    currency: str | None
    observed_at: datetime
    available_at: datetime | None
    retrieved_at_utc: datetime
    source_snapshot_id: str
    adjustment_basis: AdjustmentBasis | None
    backtest_eligible: bool

    def __post_init__(self) -> None:
        if not self.instrument_id and not self.series_id:
            raise ValueError("a stable instrument_id or series_id is required")
        if not self.source_snapshot_id:
            raise ValueError("source_snapshot_id is required")
        _require_aware("observed_at", self.observed_at)
        _require_aware("retrieved_at_utc", self.retrieved_at_utc)
        if self.available_at is not None:
            _require_aware("available_at", self.available_at)
            if self.available_at < self.observed_at:
                raise ValueError("available_at cannot precede observed_at")
            if self.available_at > self.retrieved_at_utc:
                raise ValueError("available_at cannot follow retrieved_at_utc")
        if self.backtest_eligible and self.available_at is None:
            raise ValueError("backtest-eligible observations require available_at")

    def is_available_at(self, decision_cutoff: datetime) -> bool:
        _require_aware("decision_cutoff", decision_cutoff)
        return self.available_at is not None and self.available_at <= decision_cutoff


@dataclass(frozen=True, slots=True)
class SourcePolicy:
    """Field/domain-level permissions for one provider role."""

    policy_id: str
    provider: str
    role: SourceRole
    domains: tuple[str, ...]
    fields: tuple[str, ...]
    semantic_compatibility: str
    scheduled_collection_allowed: bool
    canonical_write_allowed: bool
    backtest_eligible: bool
    paper_eligible: bool
    order_eligible: bool
    license_classification: str
    retention_classification: str

    def __post_init__(self) -> None:
        if not self.domains or not self.fields:
            raise ValueError("source policy requires explicit domains and fields")
        if self.scheduled_collection_allowed and self.license_classification == "UNKNOWN":
            raise ValueError("scheduled collection requires a known license classification")
        if self.role is SourceRole.EMERGENCY_PAPER_ONLY and self.order_eligible:
            raise ValueError("emergency paper-only source cannot be order eligible")
        if self.role is SourceRole.HISTORICAL_BACKTEST_REFERENCE and (
            self.scheduled_collection_allowed or self.paper_eligible or self.order_eligible
        ):
            raise ValueError(
                "historical backtest reference cannot feed scheduled, paper, or order paths"
            )
        if self.order_eligible and not self.paper_eligible:
            raise ValueError("order eligibility requires paper eligibility")


@dataclass(frozen=True, slots=True)
class Eligibility:
    canonical: bool
    backtest: bool
    paper: bool
    order: bool

    def __post_init__(self) -> None:
        if self.order and not self.paper:
            raise ValueError("order eligibility requires paper eligibility")

    @classmethod
    def blocked(cls) -> Eligibility:
        return cls(canonical=False, backtest=False, paper=False, order=False)


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    dataset_id: str
    dataset_version: str
    schema_version: int
    source_snapshot_ids: tuple[str, ...]
    row_count: int
    coverage_start: date | None
    coverage_end: date | None
    identity_coverage: float
    freshness_status: QualityStatus
    quality_result_ids: tuple[str, ...]
    transformation_version: str
    content_sha256: str
    created_at_utc: datetime
    eligibility: Eligibility

    def __post_init__(self) -> None:
        if not self.source_snapshot_ids:
            raise ValueError("dataset manifest requires at least one source snapshot")
        if self.row_count < 0:
            raise ValueError("row_count cannot be negative")
        if not 0.0 <= self.identity_coverage <= 1.0:
            raise ValueError("identity_coverage must be between zero and one")
        if self.coverage_start and self.coverage_end and self.coverage_end < self.coverage_start:
            raise ValueError("coverage_end cannot precede coverage_start")
        _require_sha256("content_sha256", self.content_sha256)
        _require_aware("created_at_utc", self.created_at_utc)
        if self.eligibility != Eligibility.blocked():
            if not self.quality_result_ids:
                raise ValueError("eligible dataset requires at least one quality result")
            if self.freshness_status is not QualityStatus.PASS:
                raise ValueError("eligible dataset requires PASS freshness")


@dataclass(frozen=True, slots=True)
class DataQualityResult:
    result_id: str
    check_id: str
    check_version: str
    subject_id: str
    status: QualityStatus
    dimensions: tuple[str, ...]
    details: tuple[str, ...]
    safe_next_action: str
    checked_at_utc: datetime

    def __post_init__(self) -> None:
        if not self.dimensions:
            raise ValueError("quality result requires at least one dimension")
        _require_aware("checked_at_utc", self.checked_at_utc)

    def eligibility(self) -> Eligibility:
        """A single check can block promotion but cannot grant it."""

        return Eligibility.blocked()


@dataclass(frozen=True, slots=True)
class TickerMapping:
    ticker: str
    mic: str
    effective_from: date
    effective_to: date | None
    source_snapshot_id: str
    state: MappingState

    def __post_init__(self) -> None:
        if self.effective_to is not None and self.effective_to < self.effective_from:
            raise ValueError("ticker effective_to cannot precede effective_from")
        if not self.source_snapshot_id:
            raise ValueError("ticker mapping requires source snapshot lineage")

    def is_effective_at(self, as_of: date) -> bool:
        return self.effective_from <= as_of and (
            self.effective_to is None or as_of <= self.effective_to
        )


@dataclass(frozen=True, slots=True)
class SecurityIdentity:
    issuer_id: str
    instrument_id: str
    asset_id: str | None
    ticker_history: tuple[TickerMapping, ...]
    cik: str | None = None
    figi: str | None = None
    composite_figi: str | None = None
    share_class_figi: str | None = None
    cusip: str | None = None
    isin: str | None = None
    lei: str | None = None

    def __post_init__(self) -> None:
        if not self.issuer_id or not self.instrument_id:
            raise ValueError("issuer_id and instrument_id are required and distinct concepts")

    def ticker_at(self, as_of: date) -> str | None:
        candidates = tuple(
            mapping for mapping in self.ticker_history if mapping.is_effective_at(as_of)
        )
        if any(mapping.state is MappingState.AMBIGUOUS for mapping in candidates):
            raise ValueError("ticker mapping is ambiguous")
        verified = tuple(
            mapping for mapping in candidates if mapping.state is MappingState.VERIFIED
        )
        if len(verified) > 1:
            raise ValueError("multiple ticker mappings are effective")
        return verified[0].ticker if verified else None


@dataclass(frozen=True, slots=True)
class DerivedFeatureSet:
    feature_set_id: str
    feature_set_version: str
    input_canonical_dataset_ids: tuple[str, ...]
    calculation_contract: str
    parameters: Mapping[str, int | float | str | bool]
    decision_cutoff: datetime
    code_version: str
    output_sha256: str
    eligibility: Eligibility

    def __post_init__(self) -> None:
        if not self.input_canonical_dataset_ids:
            raise ValueError("derived feature set requires canonical input lineage")
        if not self.calculation_contract or not self.code_version:
            raise ValueError("calculation contract and code version are required")
        _require_aware("decision_cutoff", self.decision_cutoff)
        _require_sha256("output_sha256", self.output_sha256)
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))


@dataclass(frozen=True, slots=True)
class FallbackDecision:
    decision_id: str
    primary_provider: str
    primary_snapshot_id: str | None
    primary_error_code: str
    fallback_provider: str
    fallback_role: SourceRole
    semantic_compatibility: bool
    reason: str
    owner_approval_required: bool

    def __post_init__(self) -> None:
        if not self.primary_error_code or not self.reason:
            raise ValueError("fallback decision requires failure evidence and reason")
        if not self.semantic_compatibility and not self.owner_approval_required:
            raise ValueError("non-equivalent fallback requires owner approval")


@dataclass(frozen=True, slots=True)
class FallbackReceipt:
    receipt_id: str
    decision_id: str
    fallback_snapshot_id: str
    content_sha256: str
    retrieved_at_utc: datetime
    freshness_status: QualityStatus
    eligibility: Eligibility
    degraded: bool

    @classmethod
    def from_decision(
        cls,
        decision: FallbackDecision,
        *,
        fallback_snapshot_id: str,
        content_sha256: str,
        retrieved_at_utc: datetime,
        freshness_status: QualityStatus,
    ) -> FallbackReceipt:
        can_use_emergency_paper = (
            decision.semantic_compatibility
            and not decision.owner_approval_required
            and freshness_status is QualityStatus.PASS
            and decision.fallback_role is SourceRole.EMERGENCY_PAPER_ONLY
        )
        if can_use_emergency_paper:
            eligibility = Eligibility(canonical=False, backtest=False, paper=True, order=False)
        else:
            eligibility = Eligibility.blocked()
        return cls(
            receipt_id=f"receipt-{decision.decision_id}",
            decision_id=decision.decision_id,
            fallback_snapshot_id=fallback_snapshot_id,
            content_sha256=content_sha256,
            retrieved_at_utc=retrieved_at_utc,
            freshness_status=freshness_status,
            eligibility=eligibility,
            degraded=True,
        )

    def __post_init__(self) -> None:
        if not self.fallback_snapshot_id:
            raise ValueError("fallback receipt requires snapshot lineage")
        _require_sha256("content_sha256", self.content_sha256)
        _require_aware("retrieved_at_utc", self.retrieved_at_utc)


def _require_aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _require_sha256(name: str, value: str) -> None:
    if len(value) != _SHA256_HEX_LENGTH or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")


def _is_credential_name(name: str) -> bool:
    text = unicodedata.normalize("NFKC", name).strip()
    text = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", text)
    normalized = "".join(character for character in text.casefold() if character.isalnum())
    return normalized == "authorization" or normalized.endswith(
        ("apikey", "accesskey", "privatekey", "credential", "password", "secret", "token")
    )
