"""Pure risk-window arithmetic: returns, covariance, weights, drawdown guards.

These are explicit calculation APIs over caller-supplied numbers. They do not
certify source rules, call providers, evaluate real strategies, or infer a
trading calendar. Cadence remains a private evaluator decision, not something
implied by this module.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from itertools import pairwise
from types import MappingProxyType
from typing import Literal

from aegis_alpha.engine.errors import ContractDefinitionError
from aegis_alpha.engine.numbers import require_finite, require_positive

_MIN_OBSERVATIONS = 2


def _date(value: object, name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ContractDefinitionError(f"{name} must be a date, not datetime")
    return value


def _sequence(value: object, name: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ContractDefinitionError(f"{name} must be a non-string sequence")
    return value


def _dates(value: object, name: str) -> tuple[date, ...]:
    dates = tuple(_date(item, name) for item in _sequence(value, name))
    if not dates or any(left >= right for left, right in pairwise(dates)):
        raise ContractDefinitionError(f"{name} must be nonempty and strictly increasing")
    return dates


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ContractDefinitionError(f"{name} must be a positive integer")
    return value


def _asset_id(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or not value.isprintable():
        raise ContractDefinitionError(f"{name} must be a nonempty printable string")
    return value


@dataclass(frozen=True, slots=True)
class RiskWindow:
    """Trailing ``lookback_sessions + 1`` aligned closes ending at decision_date.

    The caller may supply a longer calendar; only the trailing window survives
    construction. No date may exceed ``decision_date``.
    """

    decision_date: date
    sessions: tuple[date, ...]
    closes: Mapping[str, tuple[float, ...]]
    lookback_sessions: int

    def __post_init__(self) -> None:
        decision_date = _date(self.decision_date, "decision_date")
        sessions = _dates(self.sessions, "sessions")
        if sessions[-1] > decision_date:
            raise ContractDefinitionError("sessions must not exceed decision_date")
        lookback_sessions = _positive_int(self.lookback_sessions, "lookback_sessions")
        required = lookback_sessions + 1
        if len(sessions) < required:
            raise ContractDefinitionError(
                f"sessions must supply at least lookback_sessions+1={required} entries"
            )
        if not isinstance(self.closes, Mapping) or not self.closes:
            raise ContractDefinitionError("closes must be a nonempty mapping of asset closes")
        trimmed_closes: dict[str, tuple[float, ...]] = {}
        for asset_id in self.closes:
            _asset_id(asset_id, "closes key")
        for asset_id in sorted(self.closes):
            raw = _sequence(self.closes[asset_id], f"closes[{asset_id}]")
            if len(raw) != len(sessions):
                raise ContractDefinitionError(f"closes[{asset_id}] must align with sessions")
            values = tuple(require_positive(value, field=f"closes[{asset_id}]") for value in raw)
            trimmed_closes[asset_id] = values[-required:]
        object.__setattr__(self, "decision_date", decision_date)
        object.__setattr__(self, "sessions", sessions[-required:])
        object.__setattr__(self, "closes", MappingProxyType(trimmed_closes))
        object.__setattr__(self, "lookback_sessions", lookback_sessions)

    @property
    def asset_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self.closes))


def period_returns(window: RiskWindow) -> Mapping[str, tuple[float, ...]]:
    """Simple per-session returns for every asset, in the window's asset order."""

    if not isinstance(window, RiskWindow):
        raise ContractDefinitionError("a validated RiskWindow is required")
    result: dict[str, tuple[float, ...]] = {}
    for asset_id in window.asset_ids:
        closes = window.closes[asset_id]
        result[asset_id] = tuple(
            require_finite(closes[index] / closes[index - 1] - 1, field=f"{asset_id} period return")
            for index in range(1, len(closes))
        )
    return MappingProxyType(result)


def sample_covariance(returns: Mapping[str, Sequence[float]]) -> tuple[tuple[float, ...], ...]:
    """Sample covariance matrix (``ddof=1``) in sorted asset-id order.

    Every asset's return series must supply the same number of observations
    (at least two).
    """

    if not isinstance(returns, Mapping) or not returns:
        raise ContractDefinitionError("returns must be a nonempty mapping of per-asset returns")
    asset_ids = sorted(_asset_id(key, "returns key") for key in returns)
    series: list[tuple[float, ...]] = []
    observations: int | None = None
    for asset_id in asset_ids:
        values = tuple(
            require_finite(value, field=f"returns[{asset_id}]")
            for value in _sequence(returns[asset_id], f"returns[{asset_id}]")
        )
        if observations is None:
            observations = len(values)
        elif len(values) != observations:
            raise ContractDefinitionError("all asset return series must share equal length")
        series.append(values)
    if observations is None or observations < _MIN_OBSERVATIONS:
        raise ContractDefinitionError("returns require at least two aligned observations")
    means = [sum(values) / observations for values in series]
    covariance: list[tuple[float, ...]] = []
    for row_index, row_values in enumerate(series):
        row: list[float] = []
        for col_index, col_values in enumerate(series):
            total = sum(
                (a - means[row_index]) * (b - means[col_index])
                for a, b in zip(row_values, col_values, strict=True)
            )
            row.append(require_finite(total / (observations - 1), field="sample covariance"))
        covariance.append(tuple(row))
    return tuple(covariance)


def inverse_volatility(variances: Mapping[str, float]) -> Mapping[str, float]:
    """Weights proportional to ``1 / sqrt(variance)``, normalized to sum to one."""

    if not isinstance(variances, Mapping) or not variances:
        raise ContractDefinitionError("variances must be a nonempty mapping")
    asset_ids = sorted(_asset_id(key, "variances key") for key in variances)
    inverse: dict[str, float] = {}
    for asset_id in asset_ids:
        variance = require_positive(variances[asset_id], field=f"variances[{asset_id}]")
        inverse[asset_id] = 1 / (variance**0.5)
    total = sum(inverse.values())
    weights = {
        asset_id: require_finite(value / total, field=f"inverse_volatility weight[{asset_id}]")
        for asset_id, value in inverse.items()
    }
    return MappingProxyType(weights)


def annualized_volatility(returns: Sequence[float], *, periods_per_year: int) -> float:
    """Sample standard deviation (``ddof=1``) scaled by ``sqrt(periods_per_year)``."""

    values = tuple(
        require_finite(value, field="returns") for value in _sequence(returns, "returns")
    )
    if len(values) < _MIN_OBSERVATIONS:
        raise ContractDefinitionError("annualized_volatility requires at least two returns")
    periods = require_finite(
        _positive_int(periods_per_year, "periods_per_year"), field="periods_per_year"
    )
    mean = sum(values) / len(values)
    variance = sum((value - mean) * (value - mean) for value in values) / (len(values) - 1)
    return require_finite(variance**0.5 * periods**0.5, field="annualized_volatility")


def target_volatility_scale(
    realized_annual: float, target_annual: float, leverage_ceiling: float
) -> float:
    """Scale factor in ``[0, leverage_ceiling]`` mapping realized to target volatility."""

    realized = require_positive(realized_annual, field="realized_annual")
    target = require_positive(target_annual, field="target_annual")
    ceiling = require_positive(leverage_ceiling, field="leverage_ceiling")
    if ceiling > 1:
        raise ContractDefinitionError("leverage_ceiling must be at most 1")
    scale = target / realized
    return require_finite(min(max(scale, 0.0), ceiling), field="target_volatility_scale")


def two_asset_minimum_variance(
    var_a: float, var_b: float, cov_ab: float, lower_a: float, upper_a: float
) -> tuple[float, float]:
    """Closed-form two-asset minimum-variance weights, clamped to feasible bounds.

    Only malformed bounds (outside ``0 <= lower_a <= upper_a <= 1``) and a
    degenerate or non-positive-definite covariance are rejected; an
    unconstrained optimum outside ``[lower_a, upper_a]`` is clamped to the
    nearest feasible bound rather than treated as infeasible.
    """

    variance_a = require_positive(var_a, field="var_a")
    variance_b = require_positive(var_b, field="var_b")
    covariance = require_finite(cov_ab, field="cov_ab")
    lower = require_finite(lower_a, field="lower_a")
    upper = require_finite(upper_a, field="upper_a")
    if not 0 <= lower <= upper <= 1:
        raise ContractDefinitionError(
            "lower_a and upper_a must satisfy 0 <= lower_a <= upper_a <= 1"
        )
    determinant = require_finite(
        variance_a * variance_b - covariance * covariance, field="two-asset determinant"
    )
    if determinant <= 0:
        raise ContractDefinitionError(
            "var_a, var_b, cov_ab must form a positive-definite covariance"
        )
    denominator = require_finite(
        variance_a + variance_b - 2 * covariance, field="two-asset denominator"
    )
    if denominator <= 0:
        raise ContractDefinitionError("two-asset minimum variance is degenerate for these inputs")
    unconstrained = require_finite(
        (variance_b - covariance) / denominator, field="unconstrained weight_a"
    )
    weight_a = min(max(unconstrained, lower), upper)
    weight_b = require_finite(1 - weight_a, field="weight_b")
    return weight_a, weight_b


@dataclass(frozen=True, slots=True)
class DrawdownPolicy:
    threshold: float
    high_convention: Literal["rolling", "running"]
    lookback_sessions: int
    reset_on_reentry: bool

    def __post_init__(self) -> None:
        threshold = require_finite(self.threshold, field="threshold")
        if not 0 < threshold < 1:
            raise ContractDefinitionError("threshold must be strictly between 0 and 1")
        if self.high_convention not in ("rolling", "running"):
            raise ContractDefinitionError("high_convention must be rolling or running")
        _positive_int(self.lookback_sessions, "lookback_sessions")
        if not isinstance(self.reset_on_reentry, bool):
            raise ContractDefinitionError("reset_on_reentry must be boolean")
        object.__setattr__(self, "threshold", threshold)


@dataclass(frozen=True, slots=True)
class DrawdownPoint:
    date: date
    high: float
    drawdown: float
    invested: bool
    reentered: bool
    source_semantics_certified: bool = field(default=False, init=False)


def apply_drawdown_guard(
    sessions: Sequence[date], closes: Sequence[float], policy: DrawdownPolicy
) -> tuple[DrawdownPoint, ...]:
    """Explicit dated drawdown exit/reentry over a caller-supplied close path.

    ``rolling`` bounds the high-water window to ``lookback_sessions`` trailing
    closes; ``running`` accumulates from the start (or the last reset). A
    reentry point itself always reports the pre-reset high/drawdown that
    triggered it; only later points observe a reset high-water mark.

    The exit/reentry boundary compares ``close`` against ``high * (1 -
    threshold)`` directly, not the derived ``drawdown`` ratio, so an exact
    threshold crossing is never missed to floating-point division noise.
    """

    if not isinstance(policy, DrawdownPolicy):
        raise ContractDefinitionError("a validated DrawdownPolicy is required")
    dates = _dates(sessions, "sessions")
    raw_closes = _sequence(closes, "closes")
    if len(raw_closes) != len(dates):
        raise ContractDefinitionError("closes must align with sessions")
    values = tuple(require_positive(value, field="closes") for value in raw_closes)
    points: list[DrawdownPoint] = []
    invested = True
    reset_index = 0
    for index, (when, close) in enumerate(zip(dates, values, strict=True)):
        if policy.high_convention == "rolling":
            window_start = max(index - policy.lookback_sessions + 1, reset_index)
        else:
            window_start = reset_index
        high = require_positive(max(values[window_start : index + 1]), field="drawdown high")
        drawdown = require_finite(1 - close / high, field="drawdown")
        threshold_close = require_finite(
            high * (1 - policy.threshold), field="drawdown threshold close"
        )
        reentered = False
        if invested:
            if close <= threshold_close:
                invested = False
        elif close > threshold_close:
            invested = True
            reentered = True
            if policy.reset_on_reentry:
                reset_index = index
        points.append(DrawdownPoint(when, high, drawdown, invested, reentered))
    return tuple(points)
