"""Pure research return splicing over explicit periods, never executable prices.

Labels and source hashes are caller assertions, not verified market provenance.
Each return spans the preceding end (or anchor) to its dated end. Both inputs
must contain the switch boundary; overlapping history is selected, not scaled.
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

_SHA256 = re.compile(r"[0-9a-f]{64}")
_BASE_INDEX = 100.0
_CALENDAR_DAYS_PER_YEAR = 365.25


def _exact_text(value: object, name: str) -> None:
    if not isinstance(value, str) or not value or not value.isprintable() or value != value.strip():
        raise ContractDefinitionError(f"{name} must be nonempty exact printable text")


def _date(value: object, name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ContractDefinitionError(f"{name} must be a date, not datetime or another type")
    return value


def _sequence(value: object, name: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ContractDefinitionError(f"{name} must be a non-string sequence")
    return value


@dataclass(frozen=True, slots=True)
class ReturnSeries:
    """Validated period returns with an explicit start boundary and source label."""

    instrument_id: str
    currency: str
    return_kind: str
    close_convention: str
    net_of_fees: bool
    anchor_date: date
    dates: tuple[date, ...]
    returns: tuple[float, ...]
    source_sha256: str

    def __post_init__(self) -> None:
        for name in ("instrument_id", "currency", "return_kind", "close_convention"):
            _exact_text(getattr(self, name), name)
        if self.return_kind not in ("price_return", "total_return"):
            raise ContractDefinitionError("return_kind must be price_return or total_return")
        if not isinstance(self.net_of_fees, bool):
            raise ContractDefinitionError("net_of_fees must be boolean")
        if not isinstance(self.source_sha256, str) or not _SHA256.fullmatch(self.source_sha256):
            raise ContractDefinitionError("source_sha256 must be lowercase SHA256 hex")
        anchor = _date(self.anchor_date, "anchor_date")
        dates = tuple(_date(day, "dates") for day in _sequence(self.dates, "dates"))
        returns = tuple(
            require_finite(value, field="returns") for value in _sequence(self.returns, "returns")
        )
        if not dates or len(dates) != len(returns):
            raise ContractDefinitionError("dates and returns must be nonempty and aligned")
        if anchor >= dates[0]:
            raise ContractDefinitionError("anchor_date must precede the first end date")
        if any(left >= right for left, right in pairwise(dates)):
            raise ContractDefinitionError("dates must be strictly increasing and unique")
        if any(value <= -1 for value in returns):
            raise ContractDefinitionError("returns must be greater than -1")
        object.__setattr__(self, "dates", dates)
        object.__setattr__(self, "returns", returns)


@dataclass(frozen=True, slots=True)
class ProxyRecipe:
    target_id: str
    donor_id: str
    switch_date: date
    annual_fee: float
    fee_model: str
    reason: str

    def __post_init__(self) -> None:
        for name in ("target_id", "donor_id", "fee_model", "reason"):
            _exact_text(getattr(self, name), name)
        _date(self.switch_date, "switch_date")
        fee = require_finite(self.annual_fee, field="annual_fee")
        if self.fee_model not in ("already_net", "annual_expense", "zero_expense_sensitivity"):
            raise ContractDefinitionError("unsupported fee_model")
        if self.fee_model == "annual_expense":
            if not 0 < fee < 1:
                raise ContractDefinitionError("annual_expense requires 0 < annual_fee < 1")
        elif fee != 0:
            raise ContractDefinitionError("already_net and zero_expense_sensitivity require fee 0")
        object.__setattr__(self, "annual_fee", fee)


@dataclass(frozen=True, slots=True)
class ProxyPoint:
    date: date
    index_value: float
    period_return: float
    source_kind: Literal["anchor", "donor", "target"]


@dataclass(frozen=True, slots=True)
class ProxyResult:
    target_id: str
    donor_id: str
    donor_source_sha256: str
    target_source_sha256: str
    switch_date: date
    currency: str
    return_kind: str
    close_convention: str
    annual_fee: float
    fee_model: str
    reason: str
    points: tuple[ProxyPoint, ...]
    research_only: bool = field(default=True, init=False)
    non_executable: bool = field(default=True, init=False)
    point_in_time_verified: bool = field(default=False, init=False)
    source_pins_verified: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "points", tuple(self.points))


def _validate_splice(donor: ReturnSeries, target: ReturnSeries, recipe: ProxyRecipe) -> None:
    if donor.instrument_id != recipe.donor_id or target.instrument_id != recipe.target_id:
        raise ContractDefinitionError("recipe IDs must match donor and target instrument IDs")
    for name in ("currency", "return_kind", "close_convention"):
        if getattr(donor, name) != getattr(target, name):
            raise ContractDefinitionError(f"donor and target {name} must match")
    if not target.net_of_fees:
        raise ContractDefinitionError("target must be net_of_fees")
    if donor.net_of_fees != (recipe.fee_model == "already_net"):
        raise ContractDefinitionError("fee_model is incompatible with donor net_of_fees")
    for series in (donor, target):
        if recipe.switch_date != series.anchor_date and recipe.switch_date not in series.dates:
            raise ContractDefinitionError(
                "both series must contain the switch boundary; no seam gap"
            )
    if donor.anchor_date >= recipe.switch_date or target.dates[-1] <= recipe.switch_date:
        raise ContractDefinitionError("at least one donor period and one target period required")


def _append_periods(
    points: list[ProxyPoint],
    series: ReturnSeries,
    recipe: ProxyRecipe,
    source_kind: Literal["donor", "target"],
) -> None:
    previous = series.anchor_date
    for end, raw_return in zip(series.dates, series.returns, strict=True):
        start, previous = previous, end
        if source_kind == "donor" and end > recipe.switch_date:
            break
        if source_kind == "target" and end <= recipe.switch_date:
            continue
        period_return = raw_return
        if source_kind == "donor" and recipe.fee_model == "annual_expense":
            drag = (1 - recipe.annual_fee) ** ((end - start).days / _CALENDAR_DAYS_PER_YEAR)
            period_return = (1 + raw_return) * drag - 1
        value = require_positive(
            points[-1].index_value * (1 + period_return), field="proxy index_value"
        )
        points.append(ProxyPoint(end, value, period_return, source_kind))


def build_proxy_returns(
    donor: ReturnSeries, target: ReturnSeries, recipe: ProxyRecipe
) -> ProxyResult:
    """Compound donor through switch and net target after it from a base of 100.

    No future level calibration, calendar inference, source-pin verification,
    trading, provider access or storage takes place here.
    """
    _validate_splice(donor, target, recipe)
    points = [ProxyPoint(donor.anchor_date, _BASE_INDEX, 0.0, "anchor")]
    _append_periods(points, donor, recipe, "donor")
    _append_periods(points, target, recipe, "target")
    return ProxyResult(
        target_id=recipe.target_id,
        donor_id=recipe.donor_id,
        donor_source_sha256=donor.source_sha256,
        target_source_sha256=target.source_sha256,
        switch_date=recipe.switch_date,
        currency=target.currency,
        return_kind=target.return_kind,
        close_convention=target.close_convention,
        annual_fee=recipe.annual_fee,
        fee_model=recipe.fee_model,
        reason=recipe.reason,
        points=tuple(points),
    )
