"""Immutable public engine contract. Callers supply every recipe field."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Final

from aegis_alpha.data.canonical_records import encode_float
from aegis_alpha.data.serialization import content_sha256
from aegis_alpha.engine.calendar import (
    EVALUATION_SNAP_CALENDAR_MONTH_END,
    SIGNAL_DATE_PRIOR_CALENDAR_MONTH_END,
)
from aegis_alpha.engine.errors import (
    ContractDefinitionError,
    ContractVersionError,
    StrategyPackLineageError,
)
from aegis_alpha.engine.membership import require_sha256_hex
from aegis_alpha.engine.strategy_config import (
    freeze_config,
    require_nonempty,
    validate_strategy_config,
)

ENGINE_CONTRACT_VERSION_V1: Final = "aas-engine-v1"
_MAX_DAY_OF_MONTH: Final = 31
_LAG_COMBINATIONS: Final = frozenset({"EXACT", "OR"})
_COMPARISONS: Final = frozenset({"LT", "LTE", "LT_SOFT_HARD"})
_THRESHOLD_COUNTS: Final = {"LT": 1, "LTE": 1, "LT_SOFT_HARD": 2}
_DERIVED_OPERATIONS: Final = frozenset(
    {
        "trailing_sum_over_price",
        "trailing_sum_plus_trailing_sum_over_price",
    }
)
_NON_EMPTY_FIELDS: Final = ("name", "cash_asset", "role")
_CONFIG_FIELDS: Final = (
    "offensive_config",
    "defensive_config",
    "canary_config",
    "signals_config",
)
_RECEIPT_REFERENCE_PATTERN: Final = re.compile(r"^ensemble:[0-9a-f]{64}$")


def _require_positive_ints(values: Sequence[int], field_name: str) -> tuple[int, ...]:
    unique: list[int] = []
    seen: set[int] = set()
    for item in values:
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            raise ContractDefinitionError(f"{field_name} values must be positive")
        if item in seen:
            raise ContractDefinitionError(f"{field_name} values must be unique")
        seen.add(item)
        unique.append(item)
    return tuple(unique)


def _require_finite_thresholds(values: Sequence[float], field_name: str) -> tuple[float, ...]:
    converted: list[float] = []
    for item in values:
        number = float(item)
        if not math.isfinite(number):
            raise ContractDefinitionError(f"{field_name} must contain only finite numbers")
        converted.append(number)
    return tuple(converted)


@dataclass(frozen=True, slots=True)
class StrategyRecord:
    name: str
    description: str
    cash_asset: str
    role: str
    similarity_group: str | None
    offensive_config: Mapping[str, object]
    defensive_config: Mapping[str, object]
    canary_config: Mapping[str, object]
    signals_config: Mapping[str, object]
    variant_of: str | None
    variant_spec: Mapping[str, object] | None

    def __post_init__(self) -> None:
        for field_name in _NON_EMPTY_FIELDS:
            value = getattr(self, field_name)
            if not isinstance(value, str):
                raise ContractDefinitionError(f"{field_name} must be a nonempty string")
            require_nonempty(value, field_name)
        if not isinstance(self.description, str):
            raise ContractDefinitionError("description must be a string")
        for name, optional in (
            ("similarity_group", self.similarity_group),
            ("variant_of", self.variant_of),
        ):
            if optional is not None and (not isinstance(optional, str) or not optional.strip()):
                raise ContractDefinitionError(f"{name} must be a nonempty string or null")
        validate_strategy_config(
            offensive_config=self.offensive_config,
            defensive_config=self.defensive_config,
            canary_config=self.canary_config,
            signals_config=self.signals_config,
            variant_spec=self.variant_spec,
        )
        for field_name in _CONFIG_FIELDS:
            object.__setattr__(self, field_name, freeze_config(getattr(self, field_name)))
        if self.variant_spec is not None:
            object.__setattr__(self, "variant_spec", freeze_config(self.variant_spec))


@dataclass(frozen=True, slots=True)
class MomentumScoreSpec:
    name: str
    return_months: tuple[int, ...]
    weights: tuple[float, ...]
    divisor: float

    def __post_init__(self) -> None:
        require_nonempty(self.name, "momentum score name")
        if self.divisor <= 0 or not math.isfinite(self.divisor):
            raise ContractDefinitionError("momentum score divisor must be a finite positive number")
        if len(self.weights) != len(self.return_months):
            raise ContractDefinitionError(
                "momentum score requires exactly one weight per return month"
            )
        if not self.return_months:
            raise ContractDefinitionError("momentum score requires at least one return month")
        for month in self.return_months:
            if month <= 0:
                raise ContractDefinitionError("momentum score return months must be positive")
        for weight in self.weights:
            if not math.isfinite(weight):
                raise ContractDefinitionError("momentum score weights must be finite")


@dataclass(frozen=True, slots=True)
class FeatureMatrixSpec:
    momentum_scores: tuple[MomentumScoreSpec, ...]
    moving_average_months: tuple[int, ...]
    ma_window_includes_current_month: bool
    return_months: tuple[int, ...]
    includes_latest_price: bool

    def __post_init__(self) -> None:
        names = [score.name for score in self.momentum_scores]
        if len(set(names)) != len(names):
            raise ContractDefinitionError("momentum score names must be unique")
        moving_average_months = _require_positive_ints(
            self.moving_average_months, "moving_average_months"
        )
        return_months = _require_positive_ints(self.return_months, "return_months")
        object.__setattr__(self, "moving_average_months", moving_average_months)
        object.__setattr__(self, "return_months", return_months)
        available = set(return_months)
        for score in self.momentum_scores:
            if any(month not in available for month in score.return_months):
                raise ContractDefinitionError(
                    f"momentum score {score.name} requires return horizons present in return_months"
                )
        if not self.includes_latest_price:
            raise ContractDefinitionError("feature_matrix requires the latest price row")


@dataclass(frozen=True, slots=True)
class MacroSignalSpec:
    series_id: str
    lag_months: tuple[int, ...]
    lag_combination: str
    comparison: str
    thresholds: tuple[float, ...]
    threshold_mode_key: str | None = None

    def __post_init__(self) -> None:
        require_nonempty(self.series_id, "macro signal series_id")
        if self.lag_combination not in _LAG_COMBINATIONS:
            raise ContractDefinitionError("lag_combination must be EXACT or OR")
        if self.lag_combination == "EXACT" and len(self.lag_months) != 1:
            raise ContractDefinitionError("EXACT lag_combination requires exactly one lag month")
        if self.lag_combination == "OR" and not self.lag_months:
            raise ContractDefinitionError("OR lag_combination requires at least one lag month")
        if any(lag < 0 for lag in self.lag_months):
            raise ContractDefinitionError("lag_months must be nonnegative")
        if self.comparison not in _COMPARISONS:
            raise ContractDefinitionError(
                "comparison must be one of " + ", ".join(sorted(_COMPARISONS))
            )
        thresholds = _require_finite_thresholds(self.thresholds, "thresholds")
        object.__setattr__(self, "thresholds", thresholds)
        expected = _THRESHOLD_COUNTS[self.comparison]
        if len(thresholds) != expected:
            raise ContractDefinitionError(
                f"comparison {self.comparison} requires exactly {expected} threshold(s)"
            )
        if self.comparison == "LT_SOFT_HARD":
            if self.threshold_mode_key is None or not self.threshold_mode_key.strip():
                raise ContractDefinitionError(
                    "LT_SOFT_HARD comparison requires explicit threshold_mode_key"
                )
        elif self.threshold_mode_key is not None:
            raise ContractDefinitionError(
                "threshold_mode_key is only valid for LT_SOFT_HARD comparisons"
            )


@dataclass(frozen=True, slots=True)
class CalendarConventions:
    evaluation_snap: str
    signal_date: str
    current_month_drop_before_day: int
    history_observations: int
    canonical_selection_authority: str
    evaluation_authority: str
    signal_authority: str
    history_authority: str

    def __post_init__(self) -> None:
        if self.evaluation_snap != EVALUATION_SNAP_CALENDAR_MONTH_END:
            raise ContractDefinitionError('evaluation_snap must be "calendar_month_end"')
        if self.signal_date != SIGNAL_DATE_PRIOR_CALENDAR_MONTH_END:
            raise ContractDefinitionError('signal_date must be "prior_calendar_month_end"')
        if not 0 < self.current_month_drop_before_day <= _MAX_DAY_OF_MONTH:
            raise ContractDefinitionError("current_month_drop_before_day must be within 1..31")
        if self.history_observations <= 0:
            raise ContractDefinitionError("history_observations must be positive")
        for field_name in (
            "canonical_selection_authority",
            "evaluation_authority",
            "signal_authority",
            "history_authority",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str):
                raise ContractDefinitionError(f"{field_name} must be a nonempty string")
            require_nonempty(value, field_name)


@dataclass(frozen=True, slots=True)
class StaleGateSpec:
    price_stale_after_days: int
    macro_stale_after_days: int

    def __post_init__(self) -> None:
        if self.price_stale_after_days <= 0 or self.macro_stale_after_days <= 0:
            raise ContractDefinitionError("stale gate thresholds must be positive day counts")


@dataclass(frozen=True, slots=True)
class DerivedInputBinding:
    dataset_id: str
    dataset_version: str
    series: str
    field: str

    def __post_init__(self) -> None:
        for field_name in ("dataset_id", "dataset_version", "series", "field"):
            value = getattr(self, field_name)
            if not isinstance(value, str):
                raise ContractDefinitionError(f"{field_name} must be a nonempty string")
            require_nonempty(value, field_name)


@dataclass(frozen=True, slots=True)
class DerivedSeriesSpec:
    series_id: str
    operation: str
    trailing_months: int
    consumes_capital: bool
    consumes_totalreturn: bool
    input_bindings: tuple[DerivedInputBinding, ...]
    signal_lag_months: tuple[int, ...]
    signal_thresholds: tuple[float, ...]
    reference_provenance: str
    canonical_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        require_nonempty(self.series_id, "derived series_id")
        if self.operation not in _DERIVED_OPERATIONS:
            raise ContractDefinitionError(
                "derived operation must be one of " + ", ".join(sorted(_DERIVED_OPERATIONS))
            )
        if self.trailing_months <= 0:
            raise ContractDefinitionError("derived trailing_months must be positive")
        if not self.consumes_capital or self.consumes_totalreturn:
            raise ContractDefinitionError("derived series must consume capital prices only")
        if not self.input_bindings:
            raise ContractDefinitionError("derived series require named dataset-version inputs")
        fields = [binding.field for binding in self.input_bindings]
        if len(set(fields)) != len(fields):
            raise ContractDefinitionError("derived input binding fields must be unique")
        required = {"price", "addend_a"}
        if self.operation == "trailing_sum_plus_trailing_sum_over_price":
            required.add("addend_b")
        if set(fields) != required:
            raise ContractDefinitionError(
                f"derived operation {self.operation} requires fields {sorted(required)}"
            )
        if any(lag < 0 for lag in self.signal_lag_months):
            raise ContractDefinitionError("derived signal_lag_months must be nonnegative")
        thresholds = _require_finite_thresholds(self.signal_thresholds, "signal_thresholds")
        object.__setattr__(self, "signal_thresholds", thresholds)
        require_nonempty(self.reference_provenance, "reference_provenance")
        projection = {
            "consumes_capital": self.consumes_capital,
            "consumes_totalreturn": self.consumes_totalreturn,
            "input_bindings": self.input_bindings,
            "operation": self.operation,
            "reference_provenance": self.reference_provenance,
            "series_id": self.series_id,
            "signal_lag_months": self.signal_lag_months,
            "signal_thresholds": tuple(encode_float(value) for value in thresholds),
            "trailing_months": self.trailing_months,
        }
        object.__setattr__(self, "canonical_sha256", content_sha256(projection))


def validate_strategy_lineage(records: Sequence[StrategyRecord]) -> None:
    names = [record.name for record in records]
    if len(set(names)) != len(names):
        raise StrategyPackLineageError("duplicate strategy names are forbidden")
    base_names = {record.name for record in records if record.variant_of is None}
    for variant in (record for record in records if record.variant_of is not None):
        if variant.variant_of not in base_names:
            raise StrategyPackLineageError(
                f"variant {variant.name} has unresolved or non-base parent {variant.variant_of}"
            )
        if variant.variant_spec is None or "axis" not in variant.variant_spec:
            raise StrategyPackLineageError(f"variant {variant.name} must declare an explicit axis")
        axis = variant.variant_spec["axis"]
        if not isinstance(axis, str) or not axis.strip():
            raise StrategyPackLineageError(f"variant {variant.name} axis must be a nonempty string")


def _validate_signals_and_derived(  # noqa: C901 -- cross-record contract invariants
    *,
    macro_signals: tuple[MacroSignalSpec, ...],
    derived_series: tuple[DerivedSeriesSpec, ...],
    pack: tuple[StrategyRecord, ...],
) -> None:
    validate_strategy_lineage(pack)
    signal_ids = [signal.series_id for signal in macro_signals]
    if len(set(signal_ids)) != len(signal_ids):
        raise StrategyPackLineageError("duplicate macro signal series ids are forbidden")
    derived_ids = [item.series_id for item in derived_series]
    if len(set(derived_ids)) != len(derived_ids):
        raise ContractDefinitionError("duplicate derived series ids are forbidden")
    signals = {signal.series_id: signal for signal in macro_signals}
    for record in pack:
        if set(record.signals_config) & set(signals):
            raise ContractDefinitionError("strategy and macro signal identifiers must not collide")
    for item in derived_series:
        signal = signals.get(item.series_id)
        if signal is None:
            raise ContractDefinitionError(
                f"derived series {item.series_id} must link to a matching signal slot"
            )
        if (
            signal.lag_months != item.signal_lag_months
            or signal.thresholds != item.signal_thresholds
        ):
            raise ContractDefinitionError(
                f"derived series {item.series_id} must link to matching signal slots"
            )
    for signal in macro_signals:
        if signal.comparison != "LT_SOFT_HARD":
            continue
        key = signal.threshold_mode_key
        if key is None:
            raise ContractDefinitionError(
                f"macro signal {signal.series_id} requires explicit threshold_mode_key"
            )
        for record in pack:
            raw = record.signals_config.get(key)
            if not isinstance(raw, Mapping) or raw.get("kind") != "threshold_mode":
                raise ContractDefinitionError(
                    f"strategy {record.name} must declare threshold_mode {key}"
                )


@dataclass(frozen=True, slots=True)
class EngineContract:
    contract_version: str
    pack: tuple[StrategyRecord, ...]
    feature_matrix: FeatureMatrixSpec
    macro_signals: tuple[MacroSignalSpec, ...]
    calendar: CalendarConventions
    stale_gates: StaleGateSpec
    ensemble_membership_reference: str
    derived_series: tuple[DerivedSeriesSpec, ...]

    def __post_init__(self) -> None:
        if self.contract_version != ENGINE_CONTRACT_VERSION_V1:
            raise ContractVersionError(self.contract_version)
        if not self.pack:
            raise ContractDefinitionError("engine contract requires at least one strategy")
        _validate_signals_and_derived(
            macro_signals=self.macro_signals,
            derived_series=self.derived_series,
            pack=self.pack,
        )
        if not _RECEIPT_REFERENCE_PATTERN.fullmatch(self.ensemble_membership_reference):
            raise ContractDefinitionError(
                'ensemble_membership_reference must match "ensemble:<64-hex membership sha256>"'
            )
        require_sha256_hex(
            self.ensemble_membership_reference.split(":", 1)[1],
            field="ensemble_membership_reference",
        )
