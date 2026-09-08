"""Replay orchestrator: full receipt or a single block, never both."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType

from aegis_alpha.engine.allocation import allocate
from aegis_alpha.engine.bundle import EngineBundle, canonical_contract_hash
from aegis_alpha.engine.calendar import (
    SIGNAL_DATE_PRIOR_CALENDAR_MONTH_END,
    prior_calendar_month_end,
)
from aegis_alpha.engine.derived import derive_history
from aegis_alpha.engine.ensemble import EnsembleMembership, combine_ensemble
from aegis_alpha.engine.errors import BlockReason, ReplayBlockedError
from aegis_alpha.engine.features import FeatureBuildRequest, PricePoint, build_feature_matrix
from aegis_alpha.engine.pit import reject_as_of_mismatch
from aegis_alpha.engine.signals import MacroPoint, evaluate_signals


@dataclass(frozen=True, slots=True)
class ReplayRequest:
    as_of: date
    fixture_as_of: date
    prices: Mapping[str, Sequence[PricePoint]]
    macro: Mapping[str, Sequence[MacroPoint]]
    derived_inputs: Mapping[str, Mapping[str, object]]
    membership: EnsembleMembership


@dataclass(frozen=True, slots=True)
class ReplayReceipt:
    bundle_id: str
    bundle_version: str
    source_sha256: str
    contract_sha256: str
    as_of: date
    per_strategy: Mapping[str, Mapping[str, float]]
    ensemble: Mapping[str, float]
    signals: Mapping[str, Mapping[str, bool]]
    master_switch: Mapping[str, bool]


def replay(bundle: EngineBundle, request: ReplayRequest) -> ReplayReceipt:
    """Execute Score→Select→Switch→Dynamic→Ratio or raise ReplayBlockedError first."""
    reject_as_of_mismatch(replay_as_of=request.as_of, fixture_as_of=request.fixture_as_of)
    contract = bundle.contract
    if {spec.series_id for spec in contract.derived_series} & set(request.macro):
        raise ReplayBlockedError(
            BlockReason.MISSING_CONFIG, "derived series cannot also be supplied as macro inputs"
        )
    if contract.calendar.signal_date != SIGNAL_DATE_PRIOR_CALENDAR_MONTH_END:
        raise ReplayBlockedError(
            BlockReason.UNSUPPORTED_SIGNAL_DATE,
            f"unsupported calendar.signal_date {contract.calendar.signal_date!r}",
        )
    signal_date = prior_calendar_month_end(request.as_of)
    derived = derive_history(
        contract.derived_series,
        inputs=request.derived_inputs,
        as_of=request.as_of,
        gates=contract.stale_gates,
        signal_date=signal_date,
    )
    merged_macro = {**request.macro}
    for name, points in derived.items():
        supplied = tuple(request.macro.get(name, ()))
        by_date = {point.as_of: point for point in (*supplied, *points)}
        merged_macro[name] = tuple(by_date[when] for when in sorted(by_date))
    features = build_feature_matrix(
        request.prices,
        FeatureBuildRequest(
            spec=contract.feature_matrix,
            evaluation_date=request.as_of,
            gates=contract.stale_gates,
            drop_before_day=contract.calendar.current_month_drop_before_day,
            history_observations=contract.calendar.history_observations,
        ),
    )
    per_strategy: dict[str, Mapping[str, float]] = {}
    flags: dict[str, Mapping[str, bool]] = {}
    switches: dict[str, bool] = {}
    for strategy in contract.pack:
        snapshot = evaluate_signals(
            strategy=strategy,
            macro=merged_macro,
            features=features,
            signal_date=signal_date,
            specs=contract.macro_signals,
            as_of=request.as_of,
            gates=contract.stale_gates,
        )
        per_strategy[strategy.name] = allocate(strategy, features, snapshot)
        flags[strategy.name] = MappingProxyType(dict(snapshot.flags))
        switches[strategy.name] = snapshot.master_switch
    _require_membership_identity(contract.ensemble_membership_reference, request.membership)
    ensemble = combine_ensemble(per_strategy, request.membership)
    return ReplayReceipt(
        bundle_id=bundle.bundle_id,
        bundle_version=bundle.bundle_version,
        source_sha256=bundle.source_sha256,
        contract_sha256=canonical_contract_hash(contract),
        as_of=request.as_of,
        per_strategy=MappingProxyType(per_strategy),
        ensemble=ensemble,
        signals=MappingProxyType(flags),
        master_switch=MappingProxyType(switches),
    )


def _require_membership_identity(reference: str, membership: EnsembleMembership) -> None:
    prefix, separator, embedded = reference.partition(":")
    if prefix != "ensemble" or separator != ":" or not embedded:
        raise ReplayBlockedError(
            BlockReason.RECEIPT_MISMATCH,
            f"unparseable ensemble_membership_reference {reference}",
        )
    observed = membership.membership_sha256
    if observed != embedded or observed != membership.expected_membership_sha256:
        raise ReplayBlockedError(
            BlockReason.RECEIPT_MISMATCH,
            "membership hash must equal the declared digest and the contract reference",
        )
