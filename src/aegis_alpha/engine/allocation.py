"""Score → Select → Switch → Dynamic → Ratio for the seven inventoried types."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from types import MappingProxyType
from typing import assert_never

from aegis_alpha.engine.errors import BlockReason, ReplayBlockedError
from aegis_alpha.engine.features import AssetFeatures
from aegis_alpha.engine.models import StrategyRecord
from aegis_alpha.engine.signals import (
    ScoringSpec,
    SignalSnapshot,
    configured_score,
    require_scoring,
)


class StrategyType(StrEnum):
    EQUAL_WEIGHT = "equal_weight"
    RELATIVE = "relative"
    ABSOLUTE = "absolute"
    ABSOLUTE_DEFENSIVE = "absolute_defensive"
    RELATIVE_ABSOLUTE = "relative_absolute"
    RELATIVE_ABSOLUTE_DEFENSIVE = "relative_absolute_defensive"
    RELATIVE_ABSOLUTE_CASH = "relative_absolute_cash"


def allocate(
    strategy: StrategyRecord,
    features: Mapping[str, AssetFeatures],
    signals: SignalSnapshot,
) -> Mapping[str, float]:
    """Return asset weights including cash. Missing prices raise instead of inventing cash."""
    offensive = _assets(strategy.offensive_config, field_name="offensive_config.assets")
    defensive = _assets(strategy.defensive_config, field_name="defensive_config.assets")
    cash = strategy.cash_asset
    top_n = _require_top_n(strategy.offensive_config.get("top_n"))
    kind = _require_strategy_type(strategy.offensive_config.get("strategy_type"))
    reference = strategy.offensive_config.get("reference_asset")
    if not isinstance(reference, str) or not reference.strip():
        raise ReplayBlockedError(
            BlockReason.MISSING_CONFIG,
            "offensive_config.reference_asset is required",
        )
    if any(asset_id not in features for asset_id in offensive) or reference not in features:
        missing = [asset_id for asset_id in (*offensive, reference) if asset_id not in features]
        raise ReplayBlockedError(
            BlockReason.MISSING_HISTORY,
            "missing feature rows for " + ", ".join(missing),
        )
    scoring = require_scoring(
        strategy.offensive_config.get("scoring"),
        field_name="offensive_config.scoring",
    )
    scores = {
        asset_id: _require_configured_score(features[asset_id], scoring)
        for asset_id in (*offensive, reference)
    }
    selected = _select(kind, offensive, scores, top_n, reference=reference)
    switched = _switch(
        selected, master_switch=signals.master_switch, defensive=defensive, kind=kind
    )
    residual_defensive = () if kind is StrategyType.RELATIVE_ABSOLUTE_CASH else defensive
    return _ratio(switched, residual_defensive, cash)


def _require_top_n(raw: object) -> int:
    if not isinstance(raw, int) or isinstance(raw, bool) or raw <= 0:
        raise ReplayBlockedError(
            BlockReason.INVALID_TOP_N,
            f"top_n must be a positive integer, got {raw!r}",
        )
    return raw


def _require_strategy_type(raw: object) -> StrategyType:
    if not isinstance(raw, str):
        raise ReplayBlockedError(
            BlockReason.MISSING_CONFIG,
            "offensive_config.strategy_type is required",
        )
    try:
        return StrategyType(raw)
    except ValueError as error:
        raise ReplayBlockedError(
            BlockReason.UNSUPPORTED_OPERATION,
            f"unsupported strategy_type {raw!r}",
        ) from error


def _require_configured_score(row: AssetFeatures, scoring: ScoringSpec) -> float:
    score = configured_score(row, scoring)
    if score is None:
        raise ReplayBlockedError(
            BlockReason.MISSING_HISTORY,
            f"missing {row.asset_id} score for {scoring}",
        )
    return float(score)


def _select(
    kind: StrategyType,
    offensive: tuple[str, ...],
    scores: Mapping[str, float],
    top_n: int,
    *,
    reference: str,
) -> dict[str, float]:
    ranked = _top(offensive, scores, top_n)
    floor = scores[reference]
    match kind:
        case StrategyType.EQUAL_WEIGHT:
            share = 1.0 / len(offensive)
            return dict.fromkeys(offensive, share)
        case StrategyType.RELATIVE:
            share = 1.0 / len(ranked)
            return dict.fromkeys(ranked, share)
        case StrategyType.ABSOLUTE | StrategyType.ABSOLUTE_DEFENSIVE:
            qualified = tuple(asset_id for asset_id in offensive if scores[asset_id] >= floor)
            if not qualified:
                return {}
            share = 1.0 / len(qualified)
            return dict.fromkeys(qualified, share)
        case (
            StrategyType.RELATIVE_ABSOLUTE
            | StrategyType.RELATIVE_ABSOLUTE_DEFENSIVE
            | StrategyType.RELATIVE_ABSOLUTE_CASH
        ):
            qualified = tuple(asset_id for asset_id in offensive if scores[asset_id] >= floor)
            chosen = _top(qualified, scores, top_n)
            share = 1.0 / top_n
            return dict.fromkeys(chosen, share)
        case unreachable:
            assert_never(unreachable)


def _switch(
    selected: Mapping[str, float],
    *,
    master_switch: bool,
    defensive: tuple[str, ...],
    kind: StrategyType,
) -> dict[str, float]:
    if not master_switch:
        return dict(selected)
    match kind:
        case StrategyType.RELATIVE_ABSOLUTE_CASH:
            return {}
        case (
            StrategyType.ABSOLUTE_DEFENSIVE
            | StrategyType.RELATIVE_ABSOLUTE_DEFENSIVE
            | StrategyType.EQUAL_WEIGHT
            | StrategyType.RELATIVE
            | StrategyType.ABSOLUTE
            | StrategyType.RELATIVE_ABSOLUTE
        ):
            if not defensive:
                return {}
            share = sum(selected.values()) / len(defensive)
            return dict.fromkeys(defensive, share)
        case unreachable:
            assert_never(unreachable)


def _ratio(
    switched: Mapping[str, float],
    defensive: tuple[str, ...],
    cash: str,
) -> Mapping[str, float]:
    return _residual(switched, defensive, cash)


def _residual(
    offensive: Mapping[str, float],
    defensive: tuple[str, ...],
    cash: str,
) -> Mapping[str, float]:
    weights: dict[str, float] = {
        asset_id: weight for asset_id, weight in offensive.items() if weight > 0
    }
    used = sum(weights.values())
    leftover = max(0.0, 1.0 - used)
    if leftover and defensive and not any(asset_id in weights for asset_id in defensive):
        share = leftover / len(defensive)
        for asset_id in defensive:
            weights[asset_id] = share
        leftover = 0.0
    if leftover:
        weights[cash] = weights.get(cash, 0.0) + leftover
    return MappingProxyType(weights)


def _top(assets: tuple[str, ...], scores: Mapping[str, float], top_n: int) -> tuple[str, ...]:
    ordered = sorted(assets, key=lambda asset_id: (-scores[asset_id], asset_id))
    return tuple(ordered[:top_n])


def _assets(config: Mapping[str, object], *, field_name: str) -> tuple[str, ...]:
    raw = config.get("assets")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raise ReplayBlockedError(
            BlockReason.MISSING_CONFIG,
            f"{field_name} must be an array of strings",
        )
    names: list[str] = []
    for item in raw:
        if not isinstance(item, str) or not item.strip():
            raise ReplayBlockedError(
                BlockReason.MISSING_CONFIG,
                f"{field_name} must contain nonempty strings",
            )
        names.append(item)
    return tuple(names)
