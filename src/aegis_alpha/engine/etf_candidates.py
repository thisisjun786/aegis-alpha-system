"""Compare explicitly supplied ETF evidence without resolving or replacing assets.

Instrument IDs are stable caller identities, never ticker matches. Liquidity is
average daily traded notional in profile currency over the tracking window.
Dates and hashes are supplied evidence labels, not independently verified facts.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Literal

from aegis_alpha.engine.errors import ContractDefinitionError
from aegis_alpha.engine.numbers import require_finite

_SHA256 = re.compile(r"[0-9a-f]{64}")


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value or not value.isprintable():
        raise ContractDefinitionError(f"{name} must be nonempty exact printable text")
    return value


def _day(value: object, name: str) -> date:
    if type(value) is not date:
        raise ContractDefinitionError(f"{name} must be a date, not datetime")
    return value


def _hash(value: object) -> None:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise ContractDefinitionError("source_hash must be lowercase SHA256 hex")


def _nonnegative(value: object, name: str) -> float:
    number = require_finite(value, field=name)
    if number < 0:
        raise ContractDefinitionError(f"{name} must be nonnegative")
    return number


@dataclass(frozen=True, slots=True)
class TrackingMeasure:
    """Daily tracking-error fraction: sample SD of ETF minus exposure returns.

    Both return series use net total returns on the same supplied sessions;
    the measure is not annualized. Source truth remains caller-owned.
    """

    start: date
    end: date
    value: float
    source_hash: str
    basis: str = "net_total_return"

    def __post_init__(self) -> None:
        if _day(self.start, "tracking.start") >= _day(self.end, "tracking.end"):
            raise ContractDefinitionError("tracking start must precede end")
        object.__setattr__(self, "value", _nonnegative(self.value, "tracking error"))
        _hash(self.source_hash)
        _text(self.basis, "tracking.basis")


@dataclass(frozen=True, slots=True)
class ETFProfile:
    instrument_id: str
    exposure_id: str
    currency: str
    hedged: bool
    leverage: float
    reset: str
    fee_bps: float | None
    inception: date | None
    as_of: date | None
    source_hash: str | None
    tracking: TrackingMeasure | None
    liquidity: float | None

    def __post_init__(self) -> None:
        for name in ("instrument_id", "exposure_id", "currency", "reset"):
            _text(getattr(self, name), name)
        if type(self.hedged) is not bool:
            raise ContractDefinitionError("hedged must be boolean")
        leverage = require_finite(self.leverage, field="leverage")
        if leverage == 0:
            raise ContractDefinitionError("leverage must be nonzero")
        object.__setattr__(self, "leverage", leverage)
        for name in ("fee_bps", "liquidity"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _nonnegative(value, name))
        for name in ("inception", "as_of"):
            value = getattr(self, name)
            if value is not None:
                _day(value, name)
        if self.source_hash is not None:
            _hash(self.source_hash)
        if self.inception is not None and self.as_of is not None and self.inception > self.as_of:
            raise ContractDefinitionError("inception must not follow profile as_of")
        self._validate_tracking()

    def _validate_tracking(self) -> None:
        if self.tracking is None:
            return
        if not isinstance(self.tracking, TrackingMeasure):
            raise ContractDefinitionError("tracking must be TrackingMeasure or None")
        if self.as_of is not None and self.tracking.end > self.as_of:
            raise ContractDefinitionError("tracking must end by profile as_of")
        if self.inception is not None and self.inception > self.tracking.start:
            raise ContractDefinitionError("tracking must start after inception")

    @property
    def exposure_key(self) -> tuple[str, str, bool, float, str]:
        return self.exposure_id, self.currency, self.hedged, self.leverage, self.reset


@dataclass(frozen=True, slots=True)
class ComparisonPolicy:
    as_of: date
    max_profile_age_days: int
    min_liquidity: float
    min_fee_saving_bps: float
    max_tracking_error: float
    tracking_start: date
    tracking_end: date
    tracking_basis: str = "net_total_return"

    def __post_init__(self) -> None:
        for name in ("as_of", "tracking_start", "tracking_end"):
            _day(getattr(self, name), name)
        if not self.tracking_start < self.tracking_end <= self.as_of:
            raise ContractDefinitionError("policy requires tracking_start < tracking_end <= as_of")
        if type(self.max_profile_age_days) is not int or self.max_profile_age_days < 0:
            raise ContractDefinitionError("max_profile_age_days must be a nonnegative integer")
        for name in ("min_liquidity", "min_fee_saving_bps", "max_tracking_error"):
            object.__setattr__(self, name, _nonnegative(getattr(self, name), name))
        _text(self.tracking_basis, "tracking_basis")


@dataclass(frozen=True, slots=True)
class ComparisonDecision:
    instrument_id: str
    status: Literal["matched", "excluded", "insufficient_evidence"]
    reason: str
    fee_saving_bps: float | None


@dataclass(frozen=True, slots=True)
class ComparisonReport:
    current: ETFProfile
    candidates: tuple[ETFProfile, ...]
    policy: ComparisonPolicy
    reference_status: str
    decisions: tuple[ComparisonDecision, ...]
    research_only: bool = field(default=True, init=False)
    automatic_replacement: bool = field(default=False, init=False)
    source_pins_verified: bool = field(default=False, init=False)


def _evidence_gap(profile: ETFProfile, policy: ComparisonPolicy) -> str | None:
    for name in ("fee_bps", "inception", "as_of", "source_hash", "tracking", "liquidity"):
        if getattr(profile, name) is None:
            return f"missing_{name}"
    # These assertions narrow the validated optional evidence fields.
    if profile.as_of is None or profile.tracking is None:
        raise ContractDefinitionError("profile evidence is incomplete")
    if profile.as_of > policy.as_of:
        return "future_profile"
    if (policy.as_of - profile.as_of).days > policy.max_profile_age_days:
        return "stale_profile"
    if (profile.tracking.start, profile.tracking.end) != (
        policy.tracking_start,
        policy.tracking_end,
    ):
        return "tracking_window_mismatch"
    if profile.tracking.basis != policy.tracking_basis:
        return "tracking_basis_mismatch"
    return None


def _threshold_reason(
    current: ETFProfile, candidate: ETFProfile, policy: ComparisonPolicy
) -> tuple[str | None, float]:
    if (
        current.fee_bps is None
        or candidate.fee_bps is None
        or current.liquidity is None
        or candidate.liquidity is None
        or current.tracking is None
        or candidate.tracking is None
    ):
        raise ContractDefinitionError("complete profile evidence required for thresholds")
    saving = current.fee_bps - candidate.fee_bps
    if current.liquidity < policy.min_liquidity:
        return "reference_liquidity_below_minimum", saving
    if candidate.liquidity < policy.min_liquidity:
        return "candidate_liquidity_below_minimum", saving
    if saving <= 0 or saving < policy.min_fee_saving_bps:
        return "fee_saving_below_minimum", saving
    if candidate.tracking.value > policy.max_tracking_error:
        return "tracking_error_above_maximum", saving
    if candidate.tracking.value > current.tracking.value:
        return "tracking_worse_than_reference", saving
    return None, saving


def compare_etfs(  # noqa: C901 -- one explicit identity/evidence/ranking boundary
    current: ETFProfile, candidates: Sequence[ETFProfile], policy: ComparisonPolicy
) -> ComparisonReport:
    """Account for every candidate; rank only equivalent, complete evidence."""
    if not isinstance(current, ETFProfile) or not isinstance(policy, ComparisonPolicy):
        raise ContractDefinitionError("validated ETFProfile and ComparisonPolicy required")
    if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes, bytearray)):
        raise ContractDefinitionError("candidates must be a sequence of ETFProfile")
    supplied = tuple(candidates)
    if any(not isinstance(item, ETFProfile) for item in supplied):
        raise ContractDefinitionError("candidates must contain only ETFProfile")
    identities = [current.instrument_id, *(item.instrument_id for item in supplied)]
    if len(identities) != len(set(identities)):
        raise ContractDefinitionError("duplicate stable instrument identity is ambiguous")
    reference_gap = _evidence_gap(current, policy)
    matched: list[tuple[float, float, str, ComparisonDecision]] = []
    remaining: list[ComparisonDecision] = []
    for candidate in supplied:
        identity = candidate.instrument_id
        if candidate.exposure_key != current.exposure_key:
            remaining.append(ComparisonDecision(identity, "excluded", "exposure_mismatch", None))
            continue
        gap = f"reference_{reference_gap}" if reference_gap else None
        candidate_gap = _evidence_gap(candidate, policy)
        if gap is None and candidate_gap is not None:
            gap = f"candidate_{candidate_gap}"
        if gap is not None:
            remaining.append(ComparisonDecision(identity, "insufficient_evidence", gap, None))
            continue
        reason, saving = _threshold_reason(current, candidate, policy)
        if reason:
            remaining.append(ComparisonDecision(identity, "excluded", reason, saving))
            continue
        if candidate.fee_bps is None or candidate.tracking is None:
            raise ContractDefinitionError("complete candidate evidence required for ranking")
        decision = ComparisonDecision(identity, "matched", "comparable_lower_cost", saving)
        matched.append((candidate.fee_bps, candidate.tracking.value, identity, decision))
    decisions = tuple(item[3] for item in sorted(matched)) + tuple(
        sorted(remaining, key=lambda item: item.instrument_id)
    )
    return ComparisonReport(current, supplied, policy, reference_gap or "complete", decisions)
