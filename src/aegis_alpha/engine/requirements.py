"""Pure execution-input projection of a validated bundle, not readiness admission."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import cast

from aegis_alpha.engine.bundle import EngineBundle
from aegis_alpha.engine.models import (
    CalendarConventions,
    DerivedInputBinding,
    DerivedSeriesSpec,
    FeatureMatrixSpec,
    MacroSignalSpec,
    StaleGateSpec,
)

type LegacyRequirementRow = tuple[str, str, str, int, str, str, str, int, str, str]


@dataclass(frozen=True, slots=True)
class InputRequirement:
    """One injected input role; lag buckets are relative to calendar.signal_date.

    Minimum observations applies to feature prices, not sparse macro/cashflow
    series. Macro OR permits any declared lag; derived replay computes every lag.
    Named derived bindings remain exact dataset/version/series/field references.
    """

    role: str
    identifiers: tuple[str, ...]
    minimum_observations: int = 0
    lag_months: tuple[int, ...] = ()
    lag_combination: str | None = None
    trailing_months: int = 0
    input_bindings: tuple[DerivedInputBinding, ...] = ()
    basis: str | None = None


@dataclass(frozen=True, slots=True)
class ExecutionDefinition:
    """Requirements, with external convention admission explicitly unresolved.

    asset_ids includes defensive/output assets; price_asset_ids contains only
    feature consumers in replay. Cash is separate unless also used as a feature.
    Calendar history is the actual replay cap, not an invented horizon unit.
    A v1 bundle supplies no basis, cost, execution or registered calendar pin.
    """

    bundle_id: str
    bundle_version: str
    source_sha256: str
    contract_sha256: str
    asset_ids: tuple[str, ...]
    price_asset_ids: tuple[str, ...]
    cash_asset_ids: tuple[str, ...]
    input_requirements: tuple[InputRequirement, ...]
    feature_matrix: FeatureMatrixSpec
    macro_signals: tuple[MacroSignalSpec, ...]
    derived_series: tuple[DerivedSeriesSpec, ...]
    calendar: CalendarConventions
    stale_gates: StaleGateSpec
    ensemble_membership_reference: str
    required_convention_roles: tuple[str, ...]
    unresolved_convention_roles: tuple[str, ...]
    executable: bool


def legacy_requirement_rows(definition: ExecutionDefinition) -> Iterator[LegacyRequirementRow]:
    """Project immutable v1 rows, not the richer execution-input requirements.

    V1 stores the configured history cap and all macro signals (including derived
    ones). Its explicit-input basis label does not resolve an execution basis.
    """
    yield (
        definition.bundle_id,
        definition.bundle_version,
        "prices",
        1,
        "engine-price-v1",
        "close",
        "prices",
        definition.calendar.history_observations,
        "explicit-input",
        "calendar_month_end",
    )
    for ordinal, signal in enumerate(definition.macro_signals, 1):
        yield (
            definition.bundle_id,
            definition.bundle_version,
            "macro",
            ordinal,
            "engine-macro-v1",
            signal.series_id,
            "macro_observations",
            max(signal.lag_months, default=0),
            "not_applicable",
            "calendar_month_end",
        )


def derive_execution_definition(bundle: EngineBundle) -> ExecutionDefinition:
    """Derive deterministically without storage access or changing v1 stored rows.

    The caller owns ingress through load_bundle and any subsequent binding
    validation. In particular, this function never resolves a convention pin.
    """
    contract = bundle.contract
    prices: set[str] = set()
    assets: set[str] = set()
    cash: set[str] = set()
    for strategy in contract.pack:
        # StrategyRecord validates and freezes these nested values at ingress.
        prices.update(cast("tuple[str, ...]", strategy.offensive_config["assets"]))
        prices.add(cast("str", strategy.offensive_config["reference_asset"]))
        prices.update(
            asset
            for asset, enabled in zip(
                cast("tuple[str, ...]", strategy.canary_config["assets"]),
                cast("tuple[bool, ...]", strategy.canary_config["enabled"]),
                strict=True,
            )
            if enabled
        )
        assets.update(cast("tuple[str, ...]", strategy.defensive_config["assets"]))
        cash.add(strategy.cash_asset)
    assets.update(prices)
    features = contract.feature_matrix
    minimum = max(
        (
            1,
            *(months + 1 for months in features.return_months),
            *(
                months + (not features.ma_window_includes_current_month)
                for months in features.moving_average_months
            ),
        )
    )
    requirements = [InputRequirement("prices", tuple(sorted(prices)), minimum)]
    derived_ids = {spec.series_id for spec in contract.derived_series}
    requirements.extend(
        InputRequirement(
            "macro",
            (signal.series_id,),
            lag_months=signal.lag_months,
            lag_combination=signal.lag_combination,
        )
        for signal in contract.macro_signals
        if signal.series_id not in derived_ids
    )
    requirements.extend(
        InputRequirement(
            "derived",
            (spec.series_id,),
            lag_months=spec.signal_lag_months,
            trailing_months=spec.trailing_months,
            input_bindings=spec.input_bindings,
            basis="capital",
        )
        for spec in contract.derived_series
    )
    requirements.append(InputRequirement("membership", (contract.ensemble_membership_reference,)))
    roles = ("calendar", "basis", "cost", "execution")
    return ExecutionDefinition(
        bundle_id=bundle.bundle_id,
        bundle_version=bundle.bundle_version,
        source_sha256=bundle.source_sha256,
        contract_sha256=bundle.contract_sha256,
        asset_ids=tuple(sorted(assets)),
        price_asset_ids=tuple(sorted(prices)),
        cash_asset_ids=tuple(sorted(cash)),
        input_requirements=tuple(requirements),
        feature_matrix=features,
        macro_signals=contract.macro_signals,
        derived_series=contract.derived_series,
        calendar=contract.calendar,
        stale_gates=contract.stale_gates,
        ensemble_membership_reference=contract.ensemble_membership_reference,
        required_convention_roles=roles,
        unresolved_convention_roles=roles,
        executable=False,
    )
