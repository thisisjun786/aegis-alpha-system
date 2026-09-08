"""Regular signals, canary evaluation, and master switch."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from types import MappingProxyType
from typing import assert_never, cast

from aegis_alpha.engine.calendar import lag_month
from aegis_alpha.engine.errors import (
    BlockReason,
    ObservationKind,
    ReplayBlockedError,
)
from aegis_alpha.engine.features import AssetFeatures
from aegis_alpha.engine.models import MacroSignalSpec, StaleGateSpec, StrategyRecord
from aegis_alpha.engine.numbers import require_finite_observation
from aegis_alpha.engine.pit import reject_stale


class LagCombination(StrEnum):
    EXACT = "EXACT"
    OR = "OR"


class Comparison(StrEnum):
    LT = "LT"
    LTE = "LTE"
    LT_SOFT_HARD = "LT_SOFT_HARD"


class CanaryMode(StrEnum):
    AND = "AND"
    OR = "OR"


class ScoringMethod(StrEnum):
    MOVING_AVERAGE = "moving_average"
    RETURN_RATE = "return_rate"
    MOMENTUM_SCORE = "momentum_score"


@dataclass(frozen=True, slots=True)
class MacroPoint:
    as_of: date
    value: float
    observed_on: date

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "value",
            require_finite_observation(self.value, field="macro value"),
        )


@dataclass(frozen=True, slots=True)
class SignalSnapshot:
    flags: Mapping[str, bool]
    canary: bool
    master_switch: bool


@dataclass(frozen=True, slots=True)
class ScoringSpec:
    method: ScoringMethod
    horizon: int
    score_name: str | None


def evaluate_signals(  # noqa: PLR0913 -- explicit replay inputs
    *,
    strategy: StrategyRecord,
    macro: Mapping[str, Sequence[MacroPoint]],
    features: Mapping[str, AssetFeatures],
    signal_date: date,
    specs: Sequence[MacroSignalSpec],
    as_of: date,
    gates: StaleGateSpec,
) -> SignalSnapshot:
    flags: dict[str, bool] = {
        spec.series_id: _regular_signal(
            spec,
            macro.get(spec.series_id, ()),
            signal_date,
            as_of=as_of,
            gates=gates,
            strategy=strategy,
        )
        for spec in specs
    }
    for name, raw in strategy.signals_config.items():
        if isinstance(raw, Mapping) and raw.get("kind") == "negative_abs_momentum":
            if name in flags:
                raise ReplayBlockedError(BlockReason.MISSING_CONFIG, "duplicate signal identifier")
            # StrategyRecord validates and freezes these nested string-keyed configs.
            flags[name] = _negative_abs_momentum(
                cast("Mapping[str, object]", raw), strategy, features
            )
    canary = _eval_canary(strategy, features)
    regular = any(flags.values())
    return SignalSnapshot(MappingProxyType(flags), canary, regular or canary)


def configured_score(row: AssetFeatures, scoring: ScoringSpec) -> float | None:
    match scoring.method:
        case ScoringMethod.MOVING_AVERAGE:
            return row.ma_ratios.get(scoring.horizon)
        case ScoringMethod.RETURN_RATE:
            return row.returns.get(scoring.horizon)
        case ScoringMethod.MOMENTUM_SCORE:
            if scoring.score_name is None:
                return None
            return row.momentum.get(scoring.score_name)
        case unreachable:
            assert_never(unreachable)


def is_bad_score(score: float, method: ScoringMethod) -> bool:
    if method is ScoringMethod.MOVING_AVERAGE:
        return score < 1.0
    return score < 0.0


def require_scoring(raw: object, *, field_name: str) -> ScoringSpec:
    if not isinstance(raw, Mapping):
        raise ReplayBlockedError(
            BlockReason.MISSING_CONFIG,
            f"{field_name} requires an explicit scoring object",
        )
    method_raw = raw.get("method")
    horizon_raw = raw.get("horizon")
    if not isinstance(method_raw, str):
        raise ReplayBlockedError(BlockReason.MISSING_CONFIG, f"{field_name}.method is required")
    try:
        method = ScoringMethod(method_raw)
    except ValueError as error:
        raise ReplayBlockedError(
            BlockReason.UNSUPPORTED_OPERATION,
            f"{field_name}.method {method_raw!r} is unsupported",
        ) from error
    if not isinstance(horizon_raw, int) or isinstance(horizon_raw, bool) or horizon_raw <= 0:
        raise ReplayBlockedError(
            BlockReason.MISSING_CONFIG,
            f"{field_name}.horizon must be a positive integer",
        )
    score_name_raw = raw.get("score_name")
    score_name: str | None
    if method is ScoringMethod.MOMENTUM_SCORE:
        if not isinstance(score_name_raw, str) or not score_name_raw.strip():
            raise ReplayBlockedError(
                BlockReason.MISSING_CONFIG,
                f"{field_name}.score_name is required for momentum_score",
            )
        score_name = score_name_raw
    else:
        score_name = None
    return ScoringSpec(method=method, horizon=horizon_raw, score_name=score_name)


def _regular_signal(  # noqa: PLR0913 -- one signal and its point-in-time context
    spec: MacroSignalSpec,
    series: Sequence[MacroPoint],
    signal_date: date,
    *,
    as_of: date,
    gates: StaleGateSpec,
    strategy: StrategyRecord,
) -> bool:
    by_date = {point.as_of: point for point in series}
    match LagCombination(spec.lag_combination):
        case LagCombination.EXACT:
            lag = spec.lag_months[0]
            bucket = lag_month(signal_date, lag)
            if bucket not in by_date:
                raise ReplayBlockedError(
                    BlockReason.LAG_BUCKET_MISS,
                    f"{spec.series_id} missing lag {lag} bucket {bucket.isoformat()}",
                )
            value = _known_macro_value(by_date[bucket], as_of=as_of, gates=gates)
            return _compare(value, spec, strategy)
        case LagCombination.OR:
            values = []
            for lag in spec.lag_months:
                bucket = lag_month(signal_date, lag)
                point = by_date.get(bucket)
                if point is None:
                    continue
                values.append(_known_macro_value(point, as_of=as_of, gates=gates))
            if not values:
                raise ReplayBlockedError(
                    BlockReason.LAG_BUCKET_MISS,
                    f"{spec.series_id} missing all OR lag buckets",
                )
            return any(_compare(value, spec, strategy) for value in values)
        case unreachable:
            assert_never(unreachable)


def _known_macro_value(point: MacroPoint, *, as_of: date, gates: StaleGateSpec) -> float:
    reject_stale(
        observation_date=point.observed_on,
        as_of=as_of,
        gates=gates,
        kind=ObservationKind.MACRO,
    )
    return point.value


def _compare(value: float, spec: MacroSignalSpec, strategy: StrategyRecord) -> bool:
    match Comparison(spec.comparison):
        case Comparison.LT:
            return value < spec.thresholds[0]
        case Comparison.LTE:
            return value <= spec.thresholds[0]
        case Comparison.LT_SOFT_HARD:
            return value < _selected_yield_threshold(spec, strategy)
        case unreachable:
            assert_never(unreachable)


def _selected_yield_threshold(spec: MacroSignalSpec, strategy: StrategyRecord) -> float:
    soft, hard = spec.thresholds
    if spec.threshold_mode_key is None:
        raise ReplayBlockedError(
            BlockReason.MISSING_CONFIG,
            f"{spec.series_id} requires an explicit threshold_mode_key",
        )
    raw = strategy.signals_config.get(spec.threshold_mode_key)
    if not isinstance(raw, Mapping):
        raise ReplayBlockedError(
            BlockReason.MISSING_CONFIG,
            f"{spec.series_id} missing signals_config.{spec.threshold_mode_key}",
        )
    mode = raw.get("mode")
    if mode == "HARD":
        return hard
    if mode == "SOFT":
        return soft
    raise ReplayBlockedError(
        BlockReason.MISSING_CONFIG,
        f"{spec.series_id} signals_config.{spec.threshold_mode_key}.mode must be SOFT or HARD",
    )


def _negative_abs_momentum(
    raw: Mapping[str, object],
    strategy: StrategyRecord,
    features: Mapping[str, AssetFeatures],
) -> bool:
    if raw.get("enabled") is not True:
        return False
    threshold = raw.get("threshold")
    if not isinstance(threshold, int) or isinstance(threshold, bool) or threshold <= 0:
        raise ReplayBlockedError(
            BlockReason.MISSING_CONFIG,
            "negative_abs_momentum.threshold must be a positive integer",
        )
    scoring = require_scoring(raw.get("scoring"), field_name="negative_abs_momentum.scoring")
    offensive = _string_sequence(
        strategy.offensive_config.get("assets"), field_name="offensive assets"
    )
    hits = 0
    for asset_id in offensive:
        row = features.get(asset_id)
        if row is None:
            raise ReplayBlockedError(
                BlockReason.MISSING_HISTORY, f"missing signal asset {asset_id}"
            )
        score = configured_score(row, scoring)
        if score is None:
            raise ReplayBlockedError(
                BlockReason.MISSING_HISTORY, f"missing signal score {asset_id}"
            )
        if is_bad_score(score, scoring.method):
            hits += 1
    return hits >= threshold


def _eval_canary(strategy: StrategyRecord, features: Mapping[str, AssetFeatures]) -> bool:
    config = strategy.canary_config
    assets = _string_sequence(config.get("assets"), field_name="canary assets")
    enabled = tuple(bool(item) for item in _as_sequence(config.get("enabled")))
    if len(enabled) != len(assets):
        raise ReplayBlockedError(
            BlockReason.MISSING_CONFIG,
            "canary enabled flags must match assets length",
        )
    active = tuple(asset_id for asset_id, flag in zip(assets, enabled, strict=True) if flag)
    if not active:
        return False
    scoring = require_scoring(config.get("scoring"), field_name="canary_config.scoring")
    hits = []
    for asset_id in active:
        row = features.get(asset_id)
        if row is None:
            raise ReplayBlockedError(
                BlockReason.MISSING_HISTORY, f"missing canary asset {asset_id}"
            )
        score = configured_score(row, scoring)
        if score is None:
            raise ReplayBlockedError(
                BlockReason.MISSING_HISTORY, f"missing canary score {asset_id}"
            )
        hits.append(is_bad_score(score, scoring.method))
    mode = str(config["canary_mode"])
    match CanaryMode(mode):
        case CanaryMode.AND:
            return all(hits)
        case CanaryMode.OR:
            return any(hits)
        case unreachable:
            assert_never(unreachable)


def _string_sequence(raw: object, *, field_name: str) -> tuple[str, ...]:
    values = _as_sequence(raw)
    names: list[str] = []
    for item in values:
        if not isinstance(item, str) or not item.strip():
            raise ReplayBlockedError(
                BlockReason.MISSING_CONFIG,
                f"{field_name} must contain nonempty strings",
            )
        names.append(item)
    return tuple(names)


def _as_sequence(value: object) -> Sequence[object]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    return ()
