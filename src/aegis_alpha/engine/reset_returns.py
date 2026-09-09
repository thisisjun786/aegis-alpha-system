"""Explicit reset-period accounting, never ETF prices or verified financing.

Cost amounts are fractions of starting NAV; callers pre-scale financing notional.
The supplied calendar and source/basis labels are assertions, not certification.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from itertools import pairwise
from typing import Literal

from aegis_alpha.engine.errors import ContractDefinitionError
from aegis_alpha.engine.numbers import require_finite, require_positive
from aegis_alpha.engine.proxy import ReturnSeries

_BASE_INDEX = 100.0
_SHA256 = re.compile(r"[0-9a-f]{64}")


def _sequence(value: object, name: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ContractDefinitionError(f"{name} must be a non-string sequence")
    return value


def _date(value: object, name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ContractDefinitionError(f"{name} must be a date, not datetime")
    return value


def _dates(value: object, name: str) -> tuple[date, ...]:
    dates = tuple(_date(item, name) for item in _sequence(value, name))
    if not dates or any(left >= right for left, right in pairwise(dates)):
        raise ContractDefinitionError(f"{name} must be nonempty, unique and increasing")
    return dates


@dataclass(frozen=True, slots=True)
class ResetCosts:
    anchor_date: date
    dates: tuple[date, ...]
    financing_drag: tuple[float, ...]
    collateral_return: tuple[float, ...]
    expense_drag: tuple[float, ...]
    source_sha256: str
    convention: Literal["fraction_of_starting_nav"]
    basis: Literal["observed_inputs", "explicit_assumptions", "zero_sensitivity"]

    def __post_init__(self) -> None:
        anchor = _date(self.anchor_date, "anchor_date")
        dates = _dates(self.dates, "dates")
        if anchor >= dates[0]:
            raise ContractDefinitionError("anchor_date must precede the first end date")
        if self.convention != "fraction_of_starting_nav":
            raise ContractDefinitionError("unsupported cost convention")
        if self.basis not in ("observed_inputs", "explicit_assumptions", "zero_sensitivity"):
            raise ContractDefinitionError("unsupported cost basis")
        if not isinstance(self.source_sha256, str) or not _SHA256.fullmatch(self.source_sha256):
            raise ContractDefinitionError("source_sha256 must be lowercase SHA256 hex")
        object.__setattr__(self, "dates", dates)
        for name in ("financing_drag", "collateral_return", "expense_drag"):
            values = tuple(
                require_finite(value, field=name) for value in _sequence(getattr(self, name), name)
            )
            if len(values) != len(dates):
                raise ContractDefinitionError(f"{name} must align with dates")
            if name != "collateral_return" and any(value < 0 for value in values):
                raise ContractDefinitionError(f"{name} must be nonnegative")
            if self.basis == "zero_sensitivity" and any(value != 0 for value in values):
                raise ContractDefinitionError("zero_sensitivity requires all cost terms zero")
            object.__setattr__(self, name, values)


@dataclass(frozen=True, slots=True)
class ResetRecipe:
    multiplier: float
    reason: str

    def __post_init__(self) -> None:
        multiplier = require_finite(self.multiplier, field="multiplier")
        if multiplier == 0:
            raise ContractDefinitionError("multiplier must be nonzero")
        reason = self.reason
        if (
            not isinstance(reason, str)
            or not reason
            or not reason.isprintable()
            or reason != reason.strip()
        ):
            raise ContractDefinitionError("reason must be nonempty exact printable text")
        object.__setattr__(self, "multiplier", multiplier)


@dataclass(frozen=True, slots=True)
class ResetPoint:
    date: date
    index_value: float
    period_return: float


@dataclass(frozen=True, slots=True)
class ResetResult:
    underlying: ReturnSeries
    costs: ResetCosts
    recipe: ResetRecipe
    points: tuple[ResetPoint, ...]
    research_only: bool = field(default=True, init=False)
    non_executable: bool = field(default=True, init=False)
    point_in_time_verified: bool = field(default=False, init=False)
    source_pins_verified: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "points", tuple(self.points))


def build_reset_returns(
    underlying: ReturnSeries,
    costs: ResetCosts,
    recipe: ResetRecipe,
    *,
    expected_sessions: Sequence[date],
) -> ResetResult:
    """Reset the multiplier each supplied period and compound explicit NAV drags.

    Financing is the total starting-NAV drag: the caller pre-scales notional.
    Reject even zero-expense cases when underlying fees are already included.
    No annual-rate conversion, liquidation, calendar inference or source checks.
    """
    if not isinstance(underlying, ReturnSeries) or not isinstance(costs, ResetCosts):
        raise ContractDefinitionError("validated ReturnSeries and ResetCosts are required")
    if not isinstance(recipe, ResetRecipe):
        raise ContractDefinitionError("validated ResetRecipe is required")
    sessions = _dates(expected_sessions, "expected_sessions")
    if sessions != (underlying.anchor_date, *underlying.dates):
        raise ContractDefinitionError("underlying dates must exactly match expected_sessions")
    if sessions != (costs.anchor_date, *costs.dates):
        raise ContractDefinitionError("cost dates must exactly match expected_sessions")
    if underlying.net_of_fees:
        raise ContractDefinitionError("underlying must exclude fees, even with zero expense")
    points = [ResetPoint(underlying.anchor_date, _BASE_INDEX, 0.0)]
    periods = zip(
        underlying.dates,
        underlying.returns,
        costs.financing_drag,
        costs.collateral_return,
        costs.expense_drag,
        strict=True,
    )
    for end, raw_return, financing, collateral, expense in periods:
        period_return = require_finite(
            recipe.multiplier * raw_return - financing + collateral - expense,
            field=f"reset return at {end}",
        )
        factor = require_positive(1 + period_return, field=f"reset factor at {end}")
        value = require_positive(points[-1].index_value * factor, field=f"reset index at {end}")
        points.append(ResetPoint(end, value, period_return))
    return ResetResult(underlying, costs, recipe, tuple(points))
