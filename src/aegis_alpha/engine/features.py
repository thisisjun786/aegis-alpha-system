"""Month-end feature matrix from injected prices. Horizons are keyed by month count."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from itertools import pairwise
from types import MappingProxyType

from aegis_alpha.engine.errors import BlockReason, ObservationKind, ReplayBlockedError
from aegis_alpha.engine.models import FeatureMatrixSpec, MomentumScoreSpec, StaleGateSpec
from aegis_alpha.engine.numbers import require_finite_observation, require_positive_observation
from aegis_alpha.engine.pit import reject_post_cutoff, reject_stale


@dataclass(frozen=True, slots=True)
class PricePoint:
    as_of: date
    close: float
    observed_on: date

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "close",
            require_positive_observation(self.close, field="price close"),
        )


@dataclass(frozen=True, slots=True)
class FeatureBuildRequest:
    spec: FeatureMatrixSpec
    evaluation_date: date
    gates: StaleGateSpec
    drop_before_day: int
    history_observations: int


@dataclass(frozen=True, slots=True)
class AssetFeatures:
    asset_id: str
    as_of: date
    latest_price: float
    returns: Mapping[int, float]
    ma_ratios: Mapping[int, float]
    momentum: Mapping[str, float]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "latest_price",
            require_positive_observation(self.latest_price, field="latest_price"),
        )
        for name in ("returns", "ma_ratios", "momentum"):
            values = getattr(self, name)
            object.__setattr__(
                self,
                name,
                MappingProxyType(
                    {
                        key: require_finite_observation(value, field=name)
                        for key, value in values.items()
                    }
                ),
            )


def build_feature_matrix(
    prices: Mapping[str, Sequence[PricePoint]],
    request: FeatureBuildRequest,
    *,
    knowledge_as_of: date | None = None,
) -> dict[str, AssetFeatures]:
    """Select economic buckets at evaluation_date; guard evidence at knowledge_as_of."""
    cutoff = knowledge_as_of or request.evaluation_date
    window = _history_window(
        prices,
        evaluation_date=request.evaluation_date,
        knowledge_as_of=cutoff,
        length=request.history_observations,
        drop_before_day=request.drop_before_day,
    )
    rows: dict[str, AssetFeatures] = {}
    for asset_id, series in window.items():
        if not series:
            continue
        latest = series[-1]
        expected_month = request.evaluation_date.year * 12 + request.evaluation_date.month
        if request.evaluation_date.day < request.drop_before_day:
            expected_month -= 1
        if latest.as_of.year * 12 + latest.as_of.month != expected_month:
            raise ReplayBlockedError(
                BlockReason.MISSING_HISTORY, f"missing current price bucket for {asset_id}"
            )
        reject_stale(
            observation_date=latest.observed_on,
            as_of=cutoff,
            gates=request.gates,
            kind=ObservationKind.PRICE,
        )
        months = tuple(point.as_of.year * 12 + point.as_of.month for point in series)
        if any(right != left + 1 for left, right in pairwise(months)):
            raise ReplayBlockedError(
                BlockReason.MISSING_HISTORY, f"missing calendar month for {asset_id}"
            )
        closes = tuple(point.close for point in series)
        returns = {months: _nth_return(closes, months) for months in request.spec.return_months}
        ma_ratios = {
            months: _price_ma_ratio(
                closes,
                months,
                includes_current=request.spec.ma_window_includes_current_month,
            )
            for months in request.spec.moving_average_months
        }
        momentum = {score.name: _momentum(returns, score) for score in request.spec.momentum_scores}
        rows[asset_id] = AssetFeatures(
            asset_id=asset_id,
            as_of=latest.as_of,
            latest_price=latest.close,
            returns=MappingProxyType(returns),
            ma_ratios=MappingProxyType(ma_ratios),
            momentum=MappingProxyType(momentum),
        )
    return rows


def _history_window(
    prices: Mapping[str, Sequence[PricePoint]],
    *,
    evaluation_date: date,
    knowledge_as_of: date,
    length: int,
    drop_before_day: int,
) -> dict[str, tuple[PricePoint, ...]]:
    drop_current = evaluation_date.day < drop_before_day
    windowed: dict[str, tuple[PricePoint, ...]] = {}
    for asset_id, series in prices.items():
        eligible: list[PricePoint] = []
        for point in sorted(series, key=lambda item: item.as_of):
            if point.as_of > evaluation_date:
                continue
            reject_post_cutoff(observation_date=point.observed_on, as_of=knowledge_as_of)
            eligible.append(point)
        monthly = _month_end_snap(eligible)
        if (
            drop_current
            and monthly
            and (monthly[-1].as_of.year, monthly[-1].as_of.month)
            == (evaluation_date.year, evaluation_date.month)
        ):
            monthly = monthly[:-1]
        windowed[asset_id] = monthly[-length:]
    return windowed


def _month_end_snap(points: Sequence[PricePoint]) -> tuple[PricePoint, ...]:
    by_month: dict[tuple[int, int], PricePoint] = {}
    for point in points:
        key = (point.as_of.year, point.as_of.month)
        current = by_month.get(key)
        if current is None or point.as_of > current.as_of:
            by_month[key] = point
    return tuple(by_month[key] for key in sorted(by_month))


def _nth_return(closes: Sequence[float], months: int) -> float:
    if len(closes) <= months or closes[-1 - months] == 0:
        raise ReplayBlockedError(
            BlockReason.MISSING_HISTORY,
            f"need {months} month-end observations to compute return",
        )
    return closes[-1] / closes[-1 - months] - 1.0


def _price_ma_ratio(closes: Sequence[float], months: int, *, includes_current: bool) -> float:
    window = closes[-months:] if includes_current else closes[-months - 1 : -1]
    needed = months if includes_current else months + 1
    if len(closes) < needed or len(window) != months or any(value <= 0 for value in window):
        raise ReplayBlockedError(
            BlockReason.MISSING_HISTORY,
            f"need {months} month-end observations to compute moving-average ratio",
        )
    average = sum(window) / len(window)
    if average == 0:
        raise ReplayBlockedError(
            BlockReason.MISSING_HISTORY,
            "moving-average window sums to zero",
        )
    return closes[-1] / average


def _momentum(returns: Mapping[int, float], score: MomentumScoreSpec) -> float:
    total = 0.0
    for month, weight in zip(score.return_months, score.weights, strict=True):
        if month not in returns:
            raise ReplayBlockedError(
                BlockReason.MISSING_HISTORY,
                f"momentum score {score.name} needs return horizon {month}",
            )
        total += returns[month] * weight
    return total / score.divisor
