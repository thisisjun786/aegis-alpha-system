"""Decision-local calendar obligations across real revision-head projections."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from typing import cast

import pytest

from aegis_alpha.application import backtest_prepare as preparation
from aegis_alpha.engine.schedule import ScheduleRequest
from tests.application.test_backtest_prepare import DAYS, Document, micros, session_rows
from tests.engine.engine_support import bundle, contract


def schedule(rows: list[Document], **changes: object) -> tuple[date, ...]:
    request = ScheduleRequest(
        bundle(contract()).contract.calendar,
        "synthetic-calendar",
        "SYN",
        "synthetic-utc-1",
        DAYS[2],
        DAYS[6],
        0,
        micros(DAYS[-1]),
        None,
    )
    request = replace(request, **changes)
    visibility = preparation._Visibility(  # noqa: SLF001
        "observed_snapshot_research", request.request_cutoff_us, None, DAYS[0], DAYS[4]
    )
    return tuple(
        slot.decision_date
        for slot in preparation._schedule(tuple(rows), visibility, request)  # noqa: SLF001
    )


def revise(rows: list[Document], day: date, known: int, **changes: object) -> None:
    previous = next(row for row in reversed(rows) if row["session_date"] == day.isoformat())
    rows.append(
        {
            **previous,
            "revision_id": f"revision-{len(rows)}",
            "supersedes_revision_id": previous["revision_id"],
            "op": "SUPERSEDE",
            "revision_known_at_us": known,
            "available_at_us": known,
            **changes,
        }
    )


def close(rows: list[Document], day: date, known: int) -> None:
    revise(rows, day, known, status="closed", open_at_us=None, close_at_us=None)


def reopen(rows: list[Document], day: date, known: int) -> None:
    revise(rows, day, known, status="open", open_at_us=micros(day, 9), close_at_us=micros(day))


@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_next_open_availability_at_decision_cutoff(delta: int) -> None:
    rows = session_rows()
    # Remove all later declarations at the February decision when publication is late.
    for row in tuple(rows):
        if row["session_date"] > DAYS[4].isoformat():
            revise(
                rows,
                date.fromisoformat(row["session_date"]),
                micros(date(2026, 2, 25)),
                available_at_us=micros(DAYS[4]) + delta,
            )
    if delta > 0:
        with pytest.raises(ValueError, match="incomplete decision-local calendar"):
            schedule(rows)
    else:
        assert schedule(rows) == (DAYS[2], DAYS[4])


def test_known_unavailable_superseder_cannot_resurrect_old_open() -> None:
    rows = session_rows()
    for day in (DAYS[5], DAYS[6]):
        revise(rows, day, micros(DAYS[4]) - 1, available_at_us=micros(DAYS[5]))
    with pytest.raises(ValueError, match="incomplete decision-local calendar"):
        schedule(rows)


def test_future_close_revision_does_not_invalidate_an_admitted_slot() -> None:
    rows = session_rows()
    revise(rows, DAYS[4], micros(DAYS[6]), close_at_us=micros(DAYS[4], 17))
    assert schedule(rows) == (DAYS[2], DAYS[4])


@pytest.mark.parametrize("successor", [True, False])
def test_complete_terminal_calendar_preserves_omission(*, successor: bool) -> None:
    rows = session_rows()
    close(rows, DAYS[5], 1)
    close(rows, DAYS[6], micros(date(2026, 2, 25)))
    if not successor:
        close(rows, DAYS[-1], 1)
    # Reopening the endpoint later cannot invent a February slot at its own cutoff.
    reopen(rows, DAYS[6], micros(date(2026, 3, 15)))
    assert schedule(rows) == (DAYS[2],)


def test_later_admitted_open_invalidates_earlier_terminal_evidence() -> None:
    rows = session_rows()
    for day in (DAYS[4], DAYS[5], DAYS[6]):
        close(rows, day, 1)
    reopen(rows, DAYS[4], micros(date(2026, 2, 25)))
    reopen(rows, DAYS[5], micros(date(2026, 2, 25)))
    rows[-1]["available_at_us"] = micros(DAYS[5])
    reopen(rows, DAYS[6], micros(date(2026, 3, 15)))
    with pytest.raises(ValueError, match="incomplete decision-local calendar"):
        schedule(rows, period_start=DAYS[3])


def test_later_admitted_open_invalidates_earlier_slot_completeness() -> None:
    rows = session_rows()
    inserted = date(2026, 1, 30)
    reopen(rows, inserted, micros(inserted, 9))
    revise(rows, DAYS[3], micros(inserted, 9), available_at_us=micros(DAYS[3]))
    close(rows, inserted, micros(date(2026, 2, 1)))
    with pytest.raises(ValueError, match="incomplete decision-local calendar"):
        schedule(rows, period_end=DAYS[3])


def test_explicit_empty_and_single_month_do_not_create_monthly_obligations() -> None:
    rows = session_rows()
    for row in rows:
        row["available_at_us"] = micros(date.fromisoformat(cast("str", row["session_date"])))
    assert schedule(rows, explicit_decision_dates=()) == ()
    assert schedule(rows, period_start=DAYS[3], period_end=DAYS[4]) == ()


def test_entirely_closed_month_is_not_a_missing_decision() -> None:
    rows = session_rows()
    for day in (DAYS[3], DAYS[4]):
        close(rows, day, 1)
    assert schedule(rows) == (DAYS[2],)
