"""Fractional long-only replay over an explicitly supplied exchange calendar.

The caller supplies observed, consistently adjusted OHLC and every session in
order. Signal targets belong to that day's close and fill at the following
session's actual open. These functions do not fetch prices or infer strategies.

Provenance: AAS-authored research/next_open.py
commit 35dcab9c sha256 2c2128dcd4e3a0b8eaaa65912bcad379c7de09cf3d56a3a4ed99242555cba357
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from itertools import pairwise

_REBALANCE_BISECTIONS = 64
_VALUE_RELATIVE_TOLERANCE = 1e-12
_MINIMUM_SESSIONS = 2


@dataclass(frozen=True, slots=True)
class NavPoint:
    """Daily closing equity, including remaining cash and marked holdings."""

    date: date
    equity: float
    cash: float
    fee: float


@dataclass(frozen=True, slots=True)
class Fill:
    """Net symbol fill with the preceding close's decision date."""

    decision_date: date
    execution_date: date
    symbol: str
    shares: float
    price: float
    fee: float


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Immutable equity and fill evidence; positions remain marked at the end."""

    nav: tuple[NavPoint, ...]
    fills: tuple[Fill, ...]


def _finite(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def _as_finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        if not math.isfinite(value):
            return None
    except OverflowError:
        return None
    return float(value)


def _calendar_date(value: object) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        msg = "calendar values must be datetime.date, not datetime or bool"
        raise ValueError(msg)  # noqa: TRY004 -- public contract rejects mistyped dates as ValueError
    return value


def _require_symbol(symbol: object) -> str:
    if (
        not isinstance(symbol, str)
        or not symbol.isprintable()
        or symbol != symbol.strip()
        or not symbol
    ):
        msg = "traded symbols must be printable nonempty identity strings"
        raise ValueError(msg)
    if symbol == "CASH":
        msg = "cash is the unallocated fraction, not a traded symbol"
        raise ValueError(msg)
    return symbol


def _require_mapping(value: object, *, field: str) -> Mapping[object, object]:
    if not isinstance(value, Mapping):
        msg = f"{field} must be a mapping"
        raise ValueError(msg)  # noqa: TRY004 -- keep a single ValueError boundary for callers
    return value


def _require_sequence(value: object, *, field: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        msg = f"{field} must be a non-string sequence"
        raise ValueError(msg)  # noqa: TRY004 -- keep a single ValueError boundary for callers
    return value


def _validate(  # noqa: PLR0913, PLR0917 -- one explicit research input boundary.
    dates: Sequence[date],
    opens: Sequence[Mapping[str, float]],
    closes: Sequence[Mapping[str, float]],
    targets: Mapping[date, Mapping[str, float]],
    initial_cash: float,
    cost: float,
) -> None:
    calendar = _require_sequence(dates, field="dates")
    opening = _require_sequence(opens, field="opens")
    closing = _require_sequence(closes, field="closes")
    if (
        len(calendar) < _MINIMUM_SESSIONS
        or len(opening) != len(calendar)
        or len(closing) != len(calendar)
    ):
        msg = "equal price/calendar lengths and at least two sessions required"
        raise ValueError(msg)
    sessions = [_calendar_date(day) for day in calendar]
    if any(left >= right for left, right in pairwise(sessions)):
        msg = "sessions must be strictly increasing and unique"
        raise ValueError(msg)
    for row in (*opening, *closing):
        _require_mapping(row, field="open and close rows")
    if not _finite(initial_cash) or initial_cash <= 0 or not _finite(cost) or not 0 <= cost < 1:
        msg = "initial cash must be positive and cost must be in [0, 1)"
        raise ValueError(msg)
    _validate_targets(targets, decision_days=set(sessions[:-1]))


def _validate_targets(
    targets: object,
    *,
    decision_days: set[date],
) -> None:
    weights_by_day = _require_mapping(targets, field="targets")
    for day, weights in weights_by_day.items():
        decision = _calendar_date(day)
        if decision not in decision_days:
            msg = "each decision must have a following supplied session"
            raise ValueError(msg)
        mapping = _require_mapping(weights, field="target weights")
        values: list[float] = []
        for symbol, weight in mapping.items():
            _require_symbol(symbol)
            parsed = _as_finite(weight)
            if parsed is None or not 0 <= parsed <= 1:
                msg = "long-only target weights must sum to at most one"
                raise ValueError(msg)
            values.append(parsed)
        if math.fsum(values) > 1:
            msg = "long-only target weights must sum to at most one"
            raise ValueError(msg)


def _require_prices(prices: Mapping[str, float], symbols: set[str]) -> None:
    table = _require_mapping(prices, field="open and close rows")
    for symbol in symbols:
        parsed = _as_finite(table[symbol]) if symbol in table else None
        if parsed is None or parsed <= 0:
            msg = "positive finite observed open/close prices required for held or traded symbols"
            raise ValueError(msg)


def _rebalance(
    cash: float,
    units: dict[str, float],
    prices: Mapping[str, float],
    weights: Mapping[str, float],
    cost: float,
) -> tuple[float, dict[str, float], float]:
    symbols = units.keys() | {symbol for symbol, weight in weights.items() if weight > 0}
    old = {symbol: units.get(symbol, 0.0) * prices[symbol] for symbol in symbols}
    pre = cash + sum(old.values())
    if not _finite(pre) or pre <= 0:
        msg = "nonfinite or nonpositive portfolio value"
        raise ArithmeticError(msg)
    tolerance = max(1.0, pre) * _VALUE_RELATIVE_TOLERANCE
    if all(abs(weights.get(symbol, 0.0) * pre - old[symbol]) <= tolerance for symbol in symbols):
        return cash, units.copy(), 0.0
    low, high = 0.0, pre
    # Unique root: derivative >= 1-cost*sum(weights) > 0.
    for _ in range(_REBALANCE_BISECTIONS):
        post = (low + high) / 2
        fee = cost * sum(abs(weights.get(symbol, 0.0) * post - old[symbol]) for symbol in symbols)
        if post + fee > pre:
            high = post
        else:
            low = post
    post = (low + high) / 2
    new = {symbol: weights.get(symbol, 0.0) * post for symbol in symbols}
    fee = cost * sum(abs(new[symbol] - old[symbol]) for symbol in symbols)
    new_cash = pre - sum(new.values()) - fee
    if new_cash < -tolerance or abs(new_cash + sum(new.values()) + fee - pre) > tolerance:
        msg = "rebalance violated self financing"
        raise ArithmeticError(msg)
    return (
        max(0.0, new_cash),
        {symbol: value / prices[symbol] for symbol, value in new.items() if value > 0},
        fee,
    )


def replay_next_open(  # noqa: PLR0913, PLR0917 -- explicit calendar, prices, targets and accounting contract.
    dates: Sequence[date],
    opens: Sequence[Mapping[str, float]],
    closes: Sequence[Mapping[str, float]],
    targets: Mapping[date, Mapping[str, float]],
    initial_cash: float,
    cost: float,
) -> ReplayResult:
    """Replay supplied close decisions at the adjacent session's real open.

    The first date is a cash baseline. Unallocated cash earns zero. Fees apply
    to gross traded notional, including sales; no terminal liquidation occurs.
    Calendar completeness and price provenance are caller-owned input contracts.
    """
    _validate(dates, opens, closes, targets, initial_cash, cost)
    cash = initial_cash
    units: dict[str, float] = {}
    points = [NavPoint(dates[0], initial_cash, initial_cash, 0.0)]
    fills: list[Fill] = []
    for index in range(1, len(dates)):
        day, decision_day = dates[index], dates[index - 1]
        fee = 0.0
        if decision_day in targets:
            needed = units.keys() | {
                symbol for symbol, weight in targets[decision_day].items() if weight > 0
            }
            _require_prices(opens[index], needed)
            previous = units
            cash, units, fee = _rebalance(cash, units, opens[index], targets[decision_day], cost)
            for symbol in sorted(previous.keys() | units.keys()):
                delta = units.get(symbol, 0.0) - previous.get(symbol, 0.0)
                if delta != 0:
                    price = opens[index][symbol]
                    fills.append(
                        Fill(decision_day, day, symbol, delta, price, abs(delta * price) * cost)
                    )
        _require_prices(closes[index], set(units))
        equity = cash + sum(quantity * closes[index][symbol] for symbol, quantity in units.items())
        if not _finite(equity) or equity <= 0:
            msg = "nonfinite or nonpositive closing equity"
            raise ArithmeticError(msg)
        points.append(NavPoint(day, equity, cash, fee))
    return ReplayResult(tuple(points), tuple(fills))
