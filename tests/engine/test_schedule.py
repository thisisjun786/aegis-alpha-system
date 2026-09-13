"""Literal supplied-calendar and timing oracles; no exchange discovery or prices."""

from __future__ import annotations

import json
from dataclasses import MISSING, FrozenInstanceError, asdict, fields, replace
from datetime import UTC, date, datetime
from typing import cast

import pytest

from aegis_alpha.engine import load_bundle, sha256_bytes
from aegis_alpha.engine.calendar import prior_calendar_month_end
from aegis_alpha.engine.schedule import DecisionSlot, ScheduleRequest, Session, decision_slots
from tests.engine.engine_support import bundle, contract, raw_bundle

INT64_MAX = 9_223_372_036_854_775_807


def _us(instant: str) -> int:
    delta = datetime.fromisoformat(instant) - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _open(day: str, opening: str, closing: str) -> Session:
    return Session(
        "synthetic-calendar",
        "SYN",
        date.fromisoformat(day),
        _us(f"{day}T{opening}+00:00"),
        _us(f"{day}T{closing}+00:00"),
        "open",
        "synthetic-tz-v1",
    )


def _closed(day: str) -> Session:
    return Session(
        "synthetic-calendar",
        "SYN",
        date.fromisoformat(day),
        None,
        None,
        "closed",
        "synthetic-tz-v1",
    )


# Declared synthetic holidays, NOT a claim about a real exchange.
# Every UTC open/close is supplied, including February's short close and March's offset.
SESSIONS = (
    _open("2026-01-29", "14:30", "21:00"),
    _closed("2026-01-30"),
    _closed("2026-01-31"),
    _open("2026-02-02", "14:30", "21:00"),
    _open("2026-02-26", "14:30", "18:00"),
    _closed("2026-02-27"),
    _closed("2026-02-28"),
    _open("2026-03-02", "14:30", "21:00"),
    _open("2026-03-30", "13:30", "20:00"),
    _open("2026-04-01", "13:30", "20:00"),
)


def _request() -> ScheduleRequest:
    return ScheduleRequest(
        calendar=bundle(contract()).contract.calendar,
        calendar_id="synthetic-calendar",
        venue="SYN",
        timezone_version="synthetic-tz-v1",
        period_start=date(2026, 1, 29),
        period_end=date(2026, 3, 30),
        decision_latency_us=4_000_000,
        request_cutoff_us=_us("2026-04-01T20:00:00+00:00"),
        explicit_decision_dates=None,
    )


def _schedule(
    sessions: tuple[Session, ...] = SESSIONS,
    /,
    **changes: object,
) -> tuple[DecisionSlot, ...]:
    # Malformed fixture values still cross the real request/function validation boundary.
    return decision_slots(sessions, request=replace(_request(), **changes))


def test_request_is_frozen_slotted_and_requires_all_explicit_fields() -> None:
    request = _request()
    assert tuple(field.name for field in fields(request)) == (
        "calendar",
        "calendar_id",
        "venue",
        "timezone_version",
        "period_start",
        "period_end",
        "decision_latency_us",
        "request_cutoff_us",
        "explicit_decision_dates",
    )
    for field in fields(request):
        assert field.default is MISSING
        assert field.default_factory is MISSING
        with pytest.raises(FrozenInstanceError):
            setattr(request, field.name, None)
    assert not hasattr(request, "__dict__")
    assert decision_slots(SESSIONS, request=request) == _schedule()
    assert replace(request, explicit_decision_dates=()).explicit_decision_dates == ()
    assert request.explicit_decision_dates is None


@pytest.mark.parametrize("bad", [None, True, (), {}])
def test_schedule_requires_grouped_request_type(bad: object) -> None:
    with pytest.raises(TypeError, match="ScheduleRequest"):
        decision_slots(SESSIONS, request=cast("ScheduleRequest", bad))


def test_three_month_holidays_preserve_actual_decisions_and_prior_signal_month() -> None:
    slots = _schedule()
    assert slots == (
        DecisionSlot(date(2026, 1, 29), date(2026, 2, 2), _us("2026-01-29T21:00:04+00:00")),
        DecisionSlot(date(2026, 2, 26), date(2026, 3, 2), _us("2026-02-26T18:00:04+00:00")),
    )
    assert tuple(prior_calendar_month_end(slot.decision_date) for slot in slots) == (
        date(2025, 12, 31),
        date(2026, 1, 31),
    )
    by_date = {row.session_date: row for row in SESSIONS}
    assert tuple(by_date[slot.execution_date].open_at_us for slot in slots) == (
        _us("2026-02-02T14:30:00+00:00"),
        _us("2026-03-02T14:30:00+00:00"),
    )
    assert _schedule(explicit_decision_dates=(date(2026, 1, 29), date(2026, 2, 26))) == slots


def test_context_and_final_valuation_do_not_create_terminal_targets() -> None:
    assert _schedule(SESSIONS[:-1]) == _schedule()
    assert _schedule(period_end=date(2026, 3, 2)) == _schedule()
    assert _schedule(period_start=date(2026, 2, 2)) == (_schedule()[1],)
    assert _schedule(period_end=date(2026, 4, 1))[-1] == DecisionSlot(
        date(2026, 3, 30),
        date(2026, 4, 1),
        _us("2026-03-30T20:00:04+00:00"),
    )
    assert _schedule(period_start=date(2026, 3, 2)) == ()


def test_empty_explicit_tuple_is_not_automatic_and_results_are_repeatable() -> None:
    before = tuple(asdict(row) for row in SESSIONS)
    slots = _schedule()
    assert _schedule(explicit_decision_dates=()) == ()
    assert _schedule() == slots
    assert tuple(asdict(row) for row in SESSIONS) == before


@pytest.mark.parametrize("sessions", [SESSIONS, SESSIONS[:-1]])
def test_explicit_terminal_rejects_with_or_without_context(sessions: tuple[Session, ...]) -> None:
    with pytest.raises(ValueError, match="explicit decision"):
        _schedule(sessions, explicit_decision_dates=(date(2026, 3, 30),))


@pytest.mark.parametrize(
    "dates",
    [
        (date(2026, 2, 2),),
        (date(2026, 1, 30),),
        (date(2026, 1, 28),),
        (date(2026, 4, 1),),
        (date(2026, 1, 29), date(2026, 1, 29)),
        (date(2026, 2, 26), date(2026, 1, 29)),
        (datetime(2026, 1, 29, tzinfo=UTC),),
        (True,),
        ("2026-01-29",),
        [date(2026, 1, 29)],
    ],
)
def test_invalid_explicit_selection_rejects(dates: object) -> None:
    with pytest.raises((ValueError, TypeError), match=r"explicit decision|schedule dates"):
        _schedule(explicit_decision_dates=dates)


def test_explicit_decision_before_baseline_rejects_even_with_in_period_fill() -> None:
    with pytest.raises(ValueError, match="explicit decision"):
        _schedule(period_start=date(2026, 2, 2), explicit_decision_dates=(date(2026, 1, 29),))


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (date(2026, 1, 28), date(2026, 3, 30)),
        (date(2026, 1, 30), date(2026, 3, 30)),
        (date(2026, 1, 29), date(2026, 3, 31)),
        (date(2026, 1, 29), date(2026, 2, 28)),
        (date(2026, 1, 29), date(2026, 1, 29)),
        (date(2026, 3, 30), date(2026, 1, 29)),
        (datetime(2026, 1, 29, tzinfo=UTC), date(2026, 3, 30)),
        (date(2026, 1, 29), True),
        ("2026-01-29", date(2026, 3, 30)),
    ],
)
def test_period_requires_two_ordered_open_date_bounds(start: object, end: object) -> None:
    with pytest.raises((ValueError, TypeError), match=r"period|schedule dates"):
        _schedule(period_start=start, period_end=end)


def _tiny_sessions() -> tuple[Session, ...]:
    return (
        replace(SESSIONS[0], open_at_us=10, close_at_us=20),
        replace(SESSIONS[3], open_at_us=30, close_at_us=40),
    )


@pytest.mark.parametrize(
    ("latency", "ceiling", "expected"),
    [
        (4, 100, 24),
        (12, 27, 27),
        (0, 100, 20),
        (9, 100, 29),
        (12, 20, 20),
    ],
)
def test_cutoff_literal_half_open_bounds(latency: int, ceiling: int, expected: int) -> None:
    assert _schedule(
        _tiny_sessions(),
        period_end=date(2026, 2, 2),
        decision_latency_us=latency,
        request_cutoff_us=ceiling,
    ) == (DecisionSlot(date(2026, 1, 29), date(2026, 2, 2), expected),)


@pytest.mark.parametrize(
    ("latency", "ceiling"),
    [
        (4, 19),
        (10, 100),
        (12, 100),
        (12, 30),
        (INT64_MAX, 27),
    ],
)
def test_invalid_cutoff_is_rejected_not_clamped(latency: int, ceiling: int) -> None:
    with pytest.raises(ValueError, match=r"cutoff|overflow"):
        _schedule(
            _tiny_sessions(),
            period_end=date(2026, 2, 2),
            decision_latency_us=latency,
            request_cutoff_us=ceiling,
        )


@pytest.mark.parametrize("field", ["decision_latency_us", "request_cutoff_us"])
@pytest.mark.parametrize("bad", [-1, True, False, 1.0, "4", None, INT64_MAX + 1])
def test_request_microseconds_require_nonnegative_int64(field: str, bad: object) -> None:
    with pytest.raises(ValueError, match="microseconds"):
        _schedule(**{field: bad, "explicit_decision_dates": ()})


@pytest.mark.parametrize("field", ["calendar_id", "venue", "timezone_version"])
def test_every_session_must_match_requested_identity_even_outside_period(field: str) -> None:
    rows = (*SESSIONS[:-1], replace(SESSIONS[-1], **{field: "other"}))
    with pytest.raises(ValueError, match="identity"):
        _schedule(rows, explicit_decision_dates=())
    with pytest.raises(ValueError, match="identity"):
        _schedule(**{field: "other"})


@pytest.mark.parametrize("field", ["calendar_id", "venue", "timezone_version"])
@pytest.mark.parametrize("bad", ["", " padded", "padded ", "bad\n", "\ud800", True, None])
def test_session_and_request_identity_scalars_reject(field: str, bad: object) -> None:
    with pytest.raises(ValueError, match="identity"):
        replace(SESSIONS[0], **{field: bad})
    with pytest.raises(ValueError, match="identity"):
        _schedule(**{field: bad, "explicit_decision_dates": ()})


@pytest.mark.parametrize("field", ["open_at_us", "close_at_us"])
@pytest.mark.parametrize("bad", [None, -1, True, False, 1.0, "20", INT64_MAX + 1])
def test_open_session_time_scalars_reject(field: str, bad: object) -> None:
    with pytest.raises(ValueError, match="microseconds"):
        replace(SESSIONS[0], **{field: bad})


@pytest.mark.parametrize(
    "changes",
    [
        {"open_at_us": 20, "close_at_us": 20},
        {"open_at_us": 21, "close_at_us": 20},
        {"status": "closed"},
        {"status": "holiday"},
        {"status": True},
        {"session_date": datetime(2026, 1, 29, tzinfo=UTC)},
        {"session_date": True},
        {"session_date": "2026-01-29"},
    ],
)
def test_session_local_invariants_reject(changes: dict[str, object]) -> None:
    with pytest.raises((ValueError, TypeError), match=r"session|schedule dates"):
        replace(SESSIONS[0], **changes)


@pytest.mark.parametrize("field", ["open_at_us", "close_at_us"])
def test_closed_session_requires_both_null_times(field: str) -> None:
    with pytest.raises(ValueError, match="both times null"):
        replace(SESSIONS[1], **{field: 0})


@pytest.mark.parametrize(
    "rows",
    [
        (),
        (SESSIONS[0],),
        (SESSIONS[0], SESSIONS[0]),
        tuple(reversed(SESSIONS)),
        (SESSIONS[0], SESSIONS[2], SESSIONS[1], *SESSIONS[3:]),
        (*SESSIONS, SESSIONS[-1]),
        (None,),
        list(SESSIONS),
    ],
)
def test_all_rows_and_order_are_validated_even_for_empty_selection(rows: object) -> None:
    with pytest.raises((ValueError, TypeError), match=r"sessions|session dates|period"):
        _schedule(cast("tuple[Session, ...]", rows), explicit_decision_dates=())


@pytest.mark.parametrize("opening", [19, 20])
def test_overlapping_or_touching_sessions_reject_across_closed_rows(opening: int) -> None:
    first, second = _tiny_sessions()
    rows = (first, SESSIONS[1], SESSIONS[2], replace(second, open_at_us=opening))
    with pytest.raises(ValueError, match="overlap"):
        _schedule(rows, period_end=date(2026, 2, 2), explicit_decision_dates=())


def test_outside_period_time_reversal_rejects() -> None:
    rows = (*SESSIONS[:-1], replace(SESSIONS[-1], open_at_us=0, close_at_us=1))
    with pytest.raises(ValueError, match="overlap"):
        _schedule(rows, explicit_decision_dates=())


def test_zero_and_max_int64_session_times_and_year_boundary_are_valid() -> None:
    rows = (
        replace(SESSIONS[0], session_date=date(2025, 12, 31), open_at_us=0, close_at_us=1),
        replace(SESSIONS[3], session_date=date(2026, 1, 2), open_at_us=2, close_at_us=INT64_MAX),
    )
    assert _schedule(
        rows,
        period_start=date(2025, 12, 31),
        period_end=date(2026, 1, 2),
        decision_latency_us=0,
        request_cutoff_us=INT64_MAX,
    ) == (DecisionSlot(date(2025, 12, 31), date(2026, 1, 2), 1),)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("evaluation_snap", "weekly"),
        ("signal_date", "same_day"),
        ("current_month_drop_before_day", True),
        ("history_observations", 3.0),
    ],
)
def test_schedule_calendar_comes_through_existing_strict_parser(field: str, bad: object) -> None:
    payload = json.loads(raw_bundle(contract()))
    payload["contract"]["calendar"][field] = bad
    raw = json.dumps(payload).encode()
    with pytest.raises(ValueError, match=field):
        load_bundle(raw, sha256_bytes(raw), "synthetic-probe", "1")


def test_calendar_requires_existing_model_not_duck_typed_mapping() -> None:
    with pytest.raises(TypeError, match="CalendarConventions"):
        _schedule(calendar=asdict(contract().calendar))


def test_models_are_frozen_slotted_and_have_exact_value_fields() -> None:
    row, slot = SESSIONS[0], _schedule()[0]
    assert tuple(asdict(row)) == (
        "calendar_id",
        "venue",
        "session_date",
        "open_at_us",
        "close_at_us",
        "status",
        "timezone_version",
    )
    assert asdict(slot) == {
        "decision_date": date(2026, 1, 29),
        "execution_date": date(2026, 2, 2),
        "cutoff_us": _us("2026-01-29T21:00:04+00:00"),
    }
    for value, field in ((row, "status"), (slot, "cutoff_us")):
        assert not hasattr(value, "__dict__")
        with pytest.raises(FrozenInstanceError):
            setattr(value, field, None)


@pytest.mark.parametrize(
    "changes",
    [
        {"cutoff_us": True},
        {"cutoff_us": -1},
        {"cutoff_us": INT64_MAX + 1},
        {"decision_date": datetime(2026, 1, 29, tzinfo=UTC)},
        {"execution_date": "2026-02-02"},
        {"execution_date": date(2026, 1, 29)},
    ],
)
def test_decision_slot_rejects_malformed_values(changes: dict[str, object]) -> None:
    slot = DecisionSlot(date(2026, 1, 29), date(2026, 2, 2), 24)
    with pytest.raises((ValueError, TypeError), match=r"microseconds|schedule dates|follow"):
        replace(slot, **changes)
