"""Explicit calendar operations used by feature, signal, and derived paths."""

from __future__ import annotations

from datetime import date
from typing import Final

from aegis_alpha.engine.errors import BlockReason, ReplayBlockedError

_MONTHS_IN_YEAR: Final = 12

EVALUATION_SNAP_CALENDAR_MONTH_END: Final = "calendar_month_end"
SIGNAL_DATE_PRIOR_CALENDAR_MONTH_END: Final = "prior_calendar_month_end"


def calendar_month_end(value: date) -> date:
    if value.year == date.max.year and value.month == _MONTHS_IN_YEAR:
        return date.max
    if value.month == _MONTHS_IN_YEAR:
        following = date(value.year + 1, 1, 1)
    else:
        following = date(value.year, value.month + 1, 1)
    return date.fromordinal(following.toordinal() - 1)


def prior_calendar_month_end(as_of: date) -> date:
    first = date(as_of.year, as_of.month, 1)
    return date.fromordinal(first.toordinal() - 1)


def _month_start(value: date, offset: int) -> date:
    ordinal = (value.year - 1) * _MONTHS_IN_YEAR + value.month - 1 + offset
    if not 0 <= ordinal < date.max.year * _MONTHS_IN_YEAR:
        raise ReplayBlockedError(
            BlockReason.MISSING_HISTORY, "calendar horizon exceeds supported dates"
        )
    year, month = divmod(ordinal, _MONTHS_IN_YEAR)
    return date(year + 1, month + 1, 1)


def lag_month(signal_date: date, lag: int) -> date:
    return calendar_month_end(_month_start(signal_date, -lag))


def trailing_window_start(as_of: date, months: int) -> date:
    return _month_start(as_of, 1 - months)
