"""Monthly close decisions paired with explicit pinned next-session opens.

The caller owns calendar completeness and availability. No holiday, timezone,
clock, history, price, or storage discovery occurs here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from itertools import pairwise
from typing import Literal, cast

from aegis_alpha.engine.models import CalendarConventions

_INT64_MAX = 9_223_372_036_854_775_807


def _date(value: object) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise TypeError("schedule dates must be datetime.date, not datetime or other values")
    return value


def _microseconds(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _INT64_MAX:
        raise ValueError("schedule microseconds must be nonnegative int64 integers, not bool")
    return value


def _identity(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or not value.isprintable():
        raise ValueError("calendar identity must be nonempty trimmed printable text")
    return value


@dataclass(frozen=True, slots=True)
class Session:
    """One supplied daily calendar value, not a revision-bearing storage row."""

    calendar_id: str
    venue: str
    session_date: date
    open_at_us: int | None
    close_at_us: int | None
    status: Literal["open", "closed"]
    timezone_version: str

    def __post_init__(self) -> None:
        for value in (self.calendar_id, self.venue, self.timezone_version):
            _identity(value)
        _date(self.session_date)
        if self.status == "open":
            opening = _microseconds(self.open_at_us)
            closing = _microseconds(self.close_at_us)
            if opening >= closing:
                raise ValueError("open session requires open_at_us < close_at_us")
        elif self.status == "closed":
            if self.open_at_us is not None or self.close_at_us is not None:
                raise ValueError("closed session requires both times null")
        else:
            raise ValueError("session status must be open or closed")


@dataclass(frozen=True, slots=True)
class DecisionSlot:
    """Actual economic decision and execution dates with a bounded knowledge cutoff."""

    decision_date: date
    execution_date: date
    cutoff_us: int

    def __post_init__(self) -> None:
        if _date(self.decision_date) >= _date(self.execution_date):
            raise ValueError("execution_date must follow decision_date")
        _microseconds(self.cutoff_us)


def _open_sessions(
    sessions: tuple[Session, ...],
    calendar_id: str,
    venue: str,
    timezone_version: str,
) -> tuple[Session, ...]:
    if not isinstance(sessions, tuple):
        raise TypeError("sessions must be a tuple of Session values")
    identity = tuple(_identity(value) for value in (calendar_id, venue, timezone_version))
    previous_date: date | None = None
    previous_close = -1
    opened: list[Session] = []
    for row in sessions:
        if not isinstance(row, Session):
            raise TypeError("sessions must contain Session values")
        if (row.calendar_id, row.venue, row.timezone_version) != identity:
            raise ValueError("session calendar identity must match the requested identity")
        if previous_date is not None and row.session_date <= previous_date:
            raise ValueError("session dates must be unique and strictly increasing")
        previous_date = row.session_date
        if row.status == "open":
            # Session construction guarantees nonnull int64 times for open rows.
            if cast("int", row.open_at_us) <= previous_close:
                raise ValueError("session times must be ordered without overlap")
            previous_close = cast("int", row.close_at_us)
            opened.append(row)
    return tuple(opened)


def _selected_dates(
    explicit: tuple[date, ...] | None,
    eligible: dict[date, tuple[Session, Session]],
) -> tuple[date, ...]:
    if explicit is None:
        return tuple(eligible)
    if not isinstance(explicit, tuple):
        raise TypeError("explicit decision dates must be a tuple or None")
    previous: date | None = None
    for value in explicit:
        day = _date(value)
        if previous is not None and day <= previous:
            raise ValueError("explicit decision dates must be unique and strictly increasing")
        if day not in eligible:
            raise ValueError("explicit decision needs a month-end open and its next open in period")
        previous = day
    return explicit


@dataclass(frozen=True, slots=True)
class ScheduleRequest:
    """Explicit calendar, period and cutoff inputs validated together with sessions."""

    calendar: CalendarConventions
    calendar_id: str
    venue: str
    timezone_version: str
    period_start: date
    period_end: date
    decision_latency_us: int
    request_cutoff_us: int
    explicit_decision_dates: tuple[date, ...] | None


def decision_slots(
    sessions: tuple[Session, ...],
    *,
    request: ScheduleRequest,
) -> tuple[DecisionSlot, ...]:
    """Schedule last-open monthly decisions within inclusive close valuation bounds.

    Both bounds must be supplied open dates, with start before end. Automatic
    terminal decisions are omitted; explicit unfillable targets reject. An empty
    explicit tuple means no decisions. Cutoffs are min(ceiling, close + latency),
    never clamped to the next open. The actual decision date is replay's as_of:
    its prior-calendar-month signal convention is not advanced to the fill month.
    """
    if not isinstance(request, ScheduleRequest):
        raise TypeError("request must be a ScheduleRequest")
    if not isinstance(request.calendar, CalendarConventions):
        raise TypeError("calendar must be validated CalendarConventions")
    # Reuse the existing cadence/convention validator, not a parallel parser.
    request.calendar.__post_init__()
    start, end = _date(request.period_start), _date(request.period_end)
    latency = _microseconds(request.decision_latency_us)
    ceiling = _microseconds(request.request_cutoff_us)
    opened = _open_sessions(sessions, request.calendar_id, request.venue, request.timezone_version)
    open_dates = {row.session_date for row in opened}
    if start >= end or start not in open_dates or end not in open_dates:
        raise ValueError("period requires two ordered supplied open-session date bounds")
    eligible = {
        left.session_date: (left, right)
        for left, right in pairwise(opened)
        if start <= left.session_date < right.session_date <= end
        and (left.session_date.year, left.session_date.month)
        != (right.session_date.year, right.session_date.month)
    }
    slots: list[DecisionSlot] = []
    for day in _selected_dates(request.explicit_decision_dates, eligible):
        decision, execution = eligible[day]
        close = cast("int", decision.close_at_us)
        delayed = close + latency
        if delayed > _INT64_MAX:
            raise ValueError("decision close plus latency overflows int64")
        cutoff = min(ceiling, delayed)
        if not close <= cutoff < cast("int", execution.open_at_us):
            raise ValueError("cutoff must satisfy decision close <= cutoff < next open")
        slots.append(DecisionSlot(day, execution.session_date, cutoff))
    return tuple(slots)
