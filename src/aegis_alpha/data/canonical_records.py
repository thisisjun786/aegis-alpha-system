"""Versioned canonical market-data records (AAS-DATA-009).

Every record here is immutable, carries explicit source lineage, and states
when its value became knowable. The validation in this module is deliberately
fail-closed: a canonical row that cannot prove its identity, lineage, basis, or
availability time is rejected rather than published with an assumption.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Final

from aegis_alpha.data.contracts import AdjustmentBasis, Eligibility, QualityStatus
from aegis_alpha.data.serialization import canonical_json_bytes

#: Identity of what a canonical row *means*. An incompatible change to record
#: semantics must change this value so a new build can never reuse an existing
#: canonical dataset version, exactly as ``006C`` did for its output contract.
CANONICAL_CONTRACT_VERSION: Final = "aas-data-009.canonical-v1"

#: Record schema version. Readers reject an unknown version instead of coercing.
CANONICAL_SCHEMA_VERSION: Final = 1

_SHA256_HEX_LENGTH: Final = 64
_HEX_DIGITS: Final = frozenset("0123456789abcdef")

#: The only adjustment bases a Norgate canonical price build may declare. They
#: are kept apart everywhere: separate partitions, separate rows, no converter.
CANONICAL_PRICE_BASES: Final = (AdjustmentBasis.SPLIT_ADJUSTED, AdjustmentBasis.TOTAL_RETURN)

#: Provider basis label -> canonical basis. The mapping is explicit so a new or
#: renamed provider label fails closed instead of silently selecting a basis.
NORGATE_BASIS_LABELS: Final[Mapping[str, AdjustmentBasis]] = {
    "CAPITAL": AdjustmentBasis.SPLIT_ADJUSTED,
    "TOTALRETURN": AdjustmentBasis.TOTAL_RETURN,
}

#: Stable directory label per basis. Derived from the canonical enum rather than
#: the provider label so the published layout never depends on one provider.
BASIS_PARTITION_LABELS: Final[Mapping[AdjustmentBasis, str]] = {
    AdjustmentBasis.RAW: "RAW",
    AdjustmentBasis.SPLIT_ADJUSTED: "SPLIT_ADJUSTED",
    AdjustmentBasis.TOTAL_RETURN: "TOTAL_RETURN",
}


class CanonicalErrorCode(StrEnum):
    """Typed reasons a canonical build refuses to continue."""

    UNRESOLVED_IDENTITY = "unresolved_identity"
    AMBIGUOUS_IDENTITY = "ambiguous_identity"
    TICKER_ONLY_JOIN = "ticker_only_join"
    MISSING_LINEAGE = "missing_lineage"
    FUTURE_AVAILABILITY = "future_availability"
    SCHEMA_DRIFT = "schema_drift"
    INELIGIBLE_SOURCE = "ineligible_source"
    BASIS_CONFLICT = "basis_conflict"
    UNKNOWN_BASIS = "unknown_basis"
    ELIGIBILITY_PROMOTION = "eligibility_promotion"
    NON_DETERMINISTIC = "non_deterministic"
    PUBLICATION_CONFLICT = "publication_conflict"
    RESOURCE_LIMIT = "resource_limit"


class CanonicalBuildError(RuntimeError):
    """A canonical build failed closed with an exact typed reason."""

    def __init__(self, code: CanonicalErrorCode, message: str) -> None:
        super().__init__(f"{code.value}: {message}")
        self.code = code


class IdentityResolutionState(StrEnum):
    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"
    AMBIGUOUS = "ambiguous"


class CorporateActionType(StrEnum):
    DIVIDEND = "dividend"
    SPLIT = "split"


class DisagreementResolution(StrEnum):
    """Reconciliation outcome.

    There is exactly one member on purpose. A disagreement between an eligible
    and an ineligible provider is evidence, never an instruction to overwrite a
    canonical value, so no "winner" outcome exists to be selected by accident.
    """

    DIAGNOSTIC_ONLY = "DIAGNOSTIC_ONLY"


def require_sha256(name: str, value: str) -> None:
    if len(value) != _SHA256_HEX_LENGTH or any(character not in _HEX_DIGITS for character in value):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")


def require_aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def require_schema_version(value: int) -> None:
    if value != CANONICAL_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported canonical schema_version {value}; "
            f"this reader accepts only {CANONICAL_SCHEMA_VERSION}"
        )


def require_finite(name: str, value: float) -> float:
    """Reject NaN and infinities before they can become canonical evidence.

    ``float.hex`` happily serializes ``nan`` and ``inf`` as ordinary-looking
    digest strings, so a non-finite value would otherwise flow into a content
    digest and a published column as if it were a real measurement. Numeric
    validity is therefore checked at record construction, which is the last
    boundary before a value becomes canonical.
    """

    number = float(value)
    if not math.isfinite(number):
        raise CanonicalBuildError(
            CanonicalErrorCode.SCHEMA_DRIFT,
            f"{name} must be a finite number, received {number}",
        )
    return number


def encode_float(value: float) -> str:
    """Encode a float losslessly for digest input.

    ``float.hex`` round-trips exactly and does not depend on text formatting,
    which is the same convention the ``006C`` anomaly evidence already uses.

    Non-finite values are refused here as well, so no digest can ever be taken
    over ``nan`` or ``inf`` even if a caller bypasses record construction.
    """

    return float(require_finite("digest value", value)).hex()


@dataclass(frozen=True, slots=True)
class SourceLineage:
    """Immutable byte-level provenance carried by every canonical row.

    A canonical value with no way back to the exact registered bytes it came
    from is not evidence, so each field here is required.
    """

    provider: str
    dataset_id: str
    dataset_version: str
    source_snapshot_id: str
    artifact_relative_path: str
    artifact_sha256: str
    row_ordinal: int

    def __post_init__(self) -> None:
        required = {
            "provider": self.provider,
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "source_snapshot_id": self.source_snapshot_id,
            "artifact_relative_path": self.artifact_relative_path,
        }
        missing = sorted(name for name, value in required.items() if not value.strip())
        if missing:
            raise CanonicalBuildError(
                CanonicalErrorCode.MISSING_LINEAGE,
                f"source lineage is incomplete: {', '.join(missing)}",
            )
        require_sha256("artifact_sha256", self.artifact_sha256)
        if self.row_ordinal < 0:
            raise ValueError("row_ordinal cannot be negative")

    def digest_projection(self) -> Mapping[str, object]:
        return {
            "artifact_relative_path": self.artifact_relative_path,
            "artifact_sha256": self.artifact_sha256,
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "provider": self.provider,
            "row_ordinal": self.row_ordinal,
            "source_snapshot_id": self.source_snapshot_id,
        }


@dataclass(frozen=True, slots=True)
class CanonicalPriceObservation:
    """One instrument's price values for one session date and one basis."""

    instrument_id: str
    observation_date: date
    adjustment_basis: AdjustmentBasis
    open: float
    high: float
    low: float
    close: float
    volume: float
    unadjusted_close: float
    dividend: float
    currency: str
    observed_at: datetime
    available_at: datetime
    lineage: SourceLineage
    issuer_id: str | None = None
    quality_flags: tuple[str, ...] = ()
    schema_version: int = CANONICAL_SCHEMA_VERSION
    observation_id: str = field(init=False)

    def __post_init__(self) -> None:
        require_schema_version(self.schema_version)
        if not self.instrument_id.strip():
            raise CanonicalBuildError(
                CanonicalErrorCode.UNRESOLVED_IDENTITY,
                "canonical price requires a resolved instrument_id",
            )
        if self.adjustment_basis not in CANONICAL_PRICE_BASES:
            raise CanonicalBuildError(
                CanonicalErrorCode.UNKNOWN_BASIS,
                f"unsupported canonical price basis: {self.adjustment_basis}",
            )
        if not self.currency.strip():
            raise ValueError("canonical price requires a currency")
        require_aware("observed_at", self.observed_at)
        require_aware("available_at", self.available_at)
        if self.available_at < self.observed_at:
            raise ValueError("available_at cannot precede observed_at")
        for name in ("open", "high", "low", "close", "volume", "unadjusted_close", "dividend"):
            object.__setattr__(self, name, require_finite(name, getattr(self, name)))
        if self.volume < 0:
            raise ValueError("volume cannot be negative")
        if tuple(sorted(self.quality_flags)) != tuple(self.quality_flags):
            raise ValueError("quality_flags must be sorted for deterministic output")
        object.__setattr__(self, "observation_id", self._derive_observation_id())

    def _derive_observation_id(self) -> str:
        digest = hashlib.sha256(
            canonical_json_bytes(
                {
                    "adjustment_basis": self.adjustment_basis.value,
                    "dataset_version": self.lineage.dataset_version,
                    "instrument_id": self.instrument_id,
                    "observation_date": self.observation_date.isoformat(),
                    "provider": self.lineage.provider,
                }
            )
        ).hexdigest()
        return f"cprice-{digest}"

    def is_available_at(self, decision_cutoff: datetime) -> bool:
        require_aware("decision_cutoff", decision_cutoff)
        return self.available_at <= decision_cutoff

    def digest_projection(self) -> Mapping[str, object]:
        """Deterministic projection used for content digests.

        Float values are hex-encoded so the digest is lossless, and instants are
        normalized to UTC so an equivalent instant in another offset produces
        the same bytes.
        """

        return {
            "adjustment_basis": self.adjustment_basis.value,
            "available_at": self.available_at.astimezone(UTC).isoformat(),
            "close": encode_float(self.close),
            "currency": self.currency,
            "dividend": encode_float(self.dividend),
            "high": encode_float(self.high),
            "instrument_id": self.instrument_id,
            "issuer_id": self.issuer_id,
            "lineage": dict(self.lineage.digest_projection()),
            "low": encode_float(self.low),
            "observation_date": self.observation_date.isoformat(),
            "observation_id": self.observation_id,
            "observed_at": self.observed_at.astimezone(UTC).isoformat(),
            "open": encode_float(self.open),
            "quality_flags": list(self.quality_flags),
            "schema_version": self.schema_version,
            "unadjusted_close": encode_float(self.unadjusted_close),
            "volume": encode_float(self.volume),
        }


@dataclass(frozen=True, slots=True)
class CanonicalCorporateAction:
    """A dividend or split with the same lineage and availability discipline."""

    instrument_id: str
    action_type: CorporateActionType
    effective_date: date
    value: float
    currency: str | None
    observed_at: datetime
    available_at: datetime
    lineage: SourceLineage
    derived_from: str
    schema_version: int = CANONICAL_SCHEMA_VERSION
    action_id: str = field(init=False)

    def __post_init__(self) -> None:
        require_schema_version(self.schema_version)
        if not self.instrument_id.strip():
            raise CanonicalBuildError(
                CanonicalErrorCode.UNRESOLVED_IDENTITY,
                "canonical corporate action requires a resolved instrument_id",
            )
        if not self.derived_from.strip():
            raise CanonicalBuildError(
                CanonicalErrorCode.MISSING_LINEAGE,
                "corporate action must name the exact source field it derives from",
            )
        require_aware("observed_at", self.observed_at)
        require_aware("available_at", self.available_at)
        if self.available_at < self.observed_at:
            raise ValueError("available_at cannot precede observed_at")
        object.__setattr__(self, "value", require_finite("corporate action value", self.value))
        if self.action_type is CorporateActionType.DIVIDEND and self.value <= 0:
            raise ValueError("a recorded dividend must be positive")
        if self.action_type is CorporateActionType.DIVIDEND and not (self.currency or "").strip():
            raise ValueError("a dividend requires a currency")
        object.__setattr__(self, "action_id", self._derive_action_id())

    def _derive_action_id(self) -> str:
        digest = hashlib.sha256(
            canonical_json_bytes(
                {
                    "action_type": self.action_type.value,
                    "dataset_version": self.lineage.dataset_version,
                    "effective_date": self.effective_date.isoformat(),
                    "instrument_id": self.instrument_id,
                    "provider": self.lineage.provider,
                }
            )
        ).hexdigest()
        return f"caction-{digest}"

    def digest_projection(self) -> Mapping[str, object]:
        return {
            "action_id": self.action_id,
            "action_type": self.action_type.value,
            "available_at": self.available_at.astimezone(UTC).isoformat(),
            "currency": self.currency,
            "derived_from": self.derived_from,
            "effective_date": self.effective_date.isoformat(),
            "instrument_id": self.instrument_id,
            "lineage": dict(self.lineage.digest_projection()),
            "observed_at": self.observed_at.astimezone(UTC).isoformat(),
            "schema_version": self.schema_version,
            "value": encode_float(self.value),
        }


@dataclass(frozen=True, slots=True)
class IdentityMappingEvidence:
    """The exact ``007`` mapping row that justified one canonical binding.

    A binding that records only an instrument ID is an unverifiable claim: a
    later reader cannot tell which dated, source-backed mapping produced it, nor
    detect that the mapping was replaced. Carrying the mapping's own identity,
    digest, snapshot lineage, and effective interval makes the binding auditable
    and makes a changed mapping change the canonical dataset identity.

    The provider key and instrument are carried too, so the evidence can be
    checked against the identity it vouches for. Without them a mapping row for
    one instrument could be presented as proof for another.
    """

    mapping_id: str
    mapping_sha256: str
    source_snapshot_id: str
    effective_start: datetime
    effective_end: datetime | None
    provider: str
    namespace: str
    provider_identifier: str
    instrument_id: str
    #: The remaining immutable fields of the 007 mapping row. They are carried so
    #: `mapping_sha256` can be *recomputed* rather than merely copied: a corrupted
    #: row could otherwise pair arbitrary fields with a syntactically valid digest.
    asserted_at_utc: datetime | None = None
    mapping_schema_version: int | None = None
    evidence: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name, value in (
            ("mapping_id", self.mapping_id),
            ("source_snapshot_id", self.source_snapshot_id),
            ("provider", self.provider),
            ("namespace", self.namespace),
            ("provider_identifier", self.provider_identifier),
            ("instrument_id", self.instrument_id),
        ):
            if not value.strip():
                raise CanonicalBuildError(
                    CanonicalErrorCode.MISSING_LINEAGE,
                    f"identity mapping evidence requires {name}",
                )
        require_sha256("mapping_sha256", self.mapping_sha256)
        require_aware("effective_start", self.effective_start)
        if self.effective_end is not None:
            require_aware("effective_end", self.effective_end)
            if self.effective_end <= self.effective_start:
                raise ValueError("effective_end must be strictly after effective_start")

    def covers(self, moment: datetime) -> bool:
        """Half-open ``[start, end)`` containment, matching the ``007`` contract."""

        require_aware("as_of", moment)
        return self.effective_start <= moment and (
            self.effective_end is None or moment < self.effective_end
        )

    def digest_projection(self) -> Mapping[str, object]:
        return {
            "asserted_at_utc": (
                None
                if self.asserted_at_utc is None
                else self.asserted_at_utc.astimezone(UTC).isoformat()
            ),
            "effective_end": (
                None
                if self.effective_end is None
                else self.effective_end.astimezone(UTC).isoformat()
            ),
            "effective_start": self.effective_start.astimezone(UTC).isoformat(),
            "evidence": dict(self.evidence),
            "instrument_id": self.instrument_id,
            "mapping_id": self.mapping_id,
            "mapping_schema_version": self.mapping_schema_version,
            "mapping_sha256": self.mapping_sha256,
            "namespace": self.namespace,
            "provider": self.provider,
            "provider_identifier": self.provider_identifier,
            "source_snapshot_id": self.source_snapshot_id,
        }


@dataclass(frozen=True, slots=True)
class CanonicalIdentityBinding:
    """How one provider key resolved to one instrument at one instant."""

    provider: str
    namespace: str
    provider_identifier: str
    as_of: datetime
    state: IdentityResolutionState
    instrument_id: str | None
    issuer_id: str | None
    mapping_evidence: IdentityMappingEvidence | None = None
    schema_version: int = CANONICAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        require_schema_version(self.schema_version)
        require_aware("as_of", self.as_of)
        for name, value in (
            ("provider", self.provider),
            ("namespace", self.namespace),
            ("provider_identifier", self.provider_identifier),
        ):
            if not value.strip():
                raise ValueError(f"identity binding requires {name}")
        if self.state is IdentityResolutionState.RESOLVED and not self.instrument_id:
            raise CanonicalBuildError(
                CanonicalErrorCode.UNRESOLVED_IDENTITY,
                "a resolved binding requires an instrument_id",
            )
        if self.state is not IdentityResolutionState.RESOLVED and self.instrument_id:
            raise ValueError("an unresolved or ambiguous binding cannot carry an instrument_id")
        if self.state is IdentityResolutionState.RESOLVED:
            self._require_bound_evidence()
        elif self.mapping_evidence is not None:
            raise ValueError("an unresolved or ambiguous binding cannot carry mapping evidence")

    def _require_bound_evidence(self) -> None:
        """Prove the mapping evidence describes this exact identity.

        Evidence for another provider key or another instrument would let one
        mapping row vouch for a binding it never justified.
        """

        if self.mapping_evidence is None:
            raise CanonicalBuildError(
                CanonicalErrorCode.MISSING_LINEAGE,
                "a resolved binding requires its source-backed 007 mapping evidence",
            )
        evidence_key = (
            self.mapping_evidence.provider,
            self.mapping_evidence.namespace,
            self.mapping_evidence.provider_identifier,
        )
        if evidence_key != (self.provider, self.namespace, self.provider_identifier):
            raise CanonicalBuildError(
                CanonicalErrorCode.MISSING_LINEAGE,
                f"mapping {self.mapping_evidence.mapping_id} belongs to provider key "
                f"{':'.join(evidence_key)}, not {self.provider}:{self.namespace}:"
                f"{self.provider_identifier}",
            )
        if self.mapping_evidence.instrument_id != self.instrument_id:
            raise CanonicalBuildError(
                CanonicalErrorCode.MISSING_LINEAGE,
                f"mapping {self.mapping_evidence.mapping_id} resolves "
                f"{self.mapping_evidence.instrument_id}, not {self.instrument_id}",
            )
        if not self.mapping_evidence.covers(self.as_of):
            raise CanonicalBuildError(
                CanonicalErrorCode.UNRESOLVED_IDENTITY,
                f"mapping {self.mapping_evidence.mapping_id} is not effective at "
                f"{self.as_of.astimezone(UTC).isoformat()}",
            )

    def digest_projection(self) -> Mapping[str, object]:
        return {
            "as_of": self.as_of.astimezone(UTC).isoformat(),
            "instrument_id": self.instrument_id,
            "issuer_id": self.issuer_id,
            "mapping_evidence": (
                None
                if self.mapping_evidence is None
                else dict(self.mapping_evidence.digest_projection())
            ),
            "namespace": self.namespace,
            "provider": self.provider,
            "provider_identifier": self.provider_identifier,
            "schema_version": self.schema_version,
            "state": self.state.value,
        }


@dataclass(frozen=True, slots=True)
class CanonicalQualityDiagnostic:
    """A typed, non-authoritative observation about canonical data quality."""

    check_id: str
    check_version: str
    status: QualityStatus
    subject: str
    details: tuple[str, ...]
    safe_next_action: str
    schema_version: int = CANONICAL_SCHEMA_VERSION
    result_id: str = field(init=False)

    def __post_init__(self) -> None:
        require_schema_version(self.schema_version)
        for name, value in (
            ("check_id", self.check_id),
            ("check_version", self.check_version),
            ("subject", self.subject),
            ("safe_next_action", self.safe_next_action),
        ):
            if not value.strip():
                raise ValueError(f"quality diagnostic requires {name}")
        if tuple(sorted(self.details)) != tuple(self.details):
            raise ValueError("diagnostic details must be sorted for deterministic output")
        digest = hashlib.sha256(
            canonical_json_bytes(
                {
                    "check_id": self.check_id,
                    "check_version": self.check_version,
                    "details": list(self.details),
                    "status": self.status.value,
                    "subject": self.subject,
                }
            )
        ).hexdigest()
        object.__setattr__(self, "result_id", f"cq-{digest}")

    def eligibility(self) -> Eligibility:
        """A diagnostic can block promotion but can never grant it."""

        return Eligibility.blocked()

    def digest_projection(self) -> Mapping[str, object]:
        return {
            "check_id": self.check_id,
            "check_version": self.check_version,
            "details": list(self.details),
            "result_id": self.result_id,
            "safe_next_action": self.safe_next_action,
            "schema_version": self.schema_version,
            "status": self.status.value,
            "subject": self.subject,
        }


@dataclass(frozen=True, slots=True)
class CanonicalDisagreement:
    """Two providers reported different values for the same observation."""

    instrument_id: str
    observation_date: date
    adjustment_basis: AdjustmentBasis
    field_name: str
    canonical_provider: str
    canonical_value: float
    comparison_provider: str
    comparison_value: float
    canonical_lineage: SourceLineage
    comparison_lineage: SourceLineage
    schema_version: int = CANONICAL_SCHEMA_VERSION
    resolution: DisagreementResolution = DisagreementResolution.DIAGNOSTIC_ONLY
    absolute_difference: float = field(init=False)
    relative_difference: float | None = field(init=False)

    def __post_init__(self) -> None:
        require_schema_version(self.schema_version)
        if not self.instrument_id.strip() or not self.field_name.strip():
            raise ValueError("a disagreement requires an instrument and a field name")
        if self.canonical_provider == self.comparison_provider:
            raise ValueError("a disagreement requires two distinct providers")
        object.__setattr__(
            self, "canonical_value", require_finite("canonical_value", self.canonical_value)
        )
        object.__setattr__(
            self, "comparison_value", require_finite("comparison_value", self.comparison_value)
        )
        absolute = abs(self.canonical_value - self.comparison_value)
        object.__setattr__(self, "absolute_difference", absolute)
        relative = absolute / abs(self.canonical_value) if self.canonical_value else None
        object.__setattr__(self, "relative_difference", relative)

    def digest_projection(self) -> Mapping[str, object]:
        return {
            "absolute_difference": encode_float(self.absolute_difference),
            "adjustment_basis": self.adjustment_basis.value,
            "canonical_lineage": dict(self.canonical_lineage.digest_projection()),
            "canonical_provider": self.canonical_provider,
            "canonical_value": encode_float(self.canonical_value),
            "comparison_lineage": dict(self.comparison_lineage.digest_projection()),
            "comparison_provider": self.comparison_provider,
            "comparison_value": encode_float(self.comparison_value),
            "field_name": self.field_name,
            "instrument_id": self.instrument_id,
            "observation_date": self.observation_date.isoformat(),
            "relative_difference": (
                None if self.relative_difference is None else encode_float(self.relative_difference)
            ),
            "resolution": self.resolution.value,
            "schema_version": self.schema_version,
        }


def assert_eligibility_blocked(eligibility: Eligibility) -> None:
    """Refuse any eligibility other than fully blocked.

    ``009`` builds canonical data; it never promotes it. Promotion is a separate
    owner gate, so a non-blocked request fails closed here rather than reaching
    publication.
    """

    if eligibility != Eligibility.blocked():
        raise CanonicalBuildError(
            CanonicalErrorCode.ELIGIBILITY_PROMOTION,
            "AAS-DATA-009 cannot publish a non-blocked eligibility; promotion is a separate gate",
        )


def sequence_digest(projections: Sequence[Mapping[str, object]]) -> str:
    """Digest an ordered sequence of record projections."""

    return hashlib.sha256(canonical_json_bytes(list(projections))).hexdigest()
