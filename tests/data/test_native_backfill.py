from __future__ import annotations

from datetime import date, datetime

import pytest

from aegis_alpha.data.native_backfill import plan_gaps


def test_missing_fields_use_sessions_and_inception() -> None:
    days = tuple(date(2026, 9, day) for day in (3, 4, 7, 8))
    gaps = plan_gaps(
        "synthetic-etf",
        days[1],
        days,
        ("open", "close"),
        {
            days[1]: frozenset({"close"}),
            days[2]: frozenset({"open", "close"}),
            days[3]: frozenset({"close"}),
        },
    )
    assert [(gap.field, gap.sessions) for gap in gaps] == [
        ("open", (days[1],)),
        ("open", (days[3],)),
    ]
    assert all(date(2026, 9, 5) not in gap.sessions for gap in gaps)


def test_complete_coverage_has_no_work() -> None:
    day = date(2026, 9, 8)
    assert plan_gaps("synthetic-etf", day, (day,), ("open",), {day: frozenset({"open"})}) == ()


def test_unknown_or_duplicate_calendar_rejected() -> None:
    day = date(2026, 9, 8)
    with pytest.raises(ValueError, match="unique"):
        plan_gaps("synthetic-etf", day, (day, day), ("open",), {})
    with pytest.raises(ValueError, match="date values"):
        plan_gaps("synthetic-etf", datetime(2026, 9, 8), (day,), ("open",), {})  # noqa: DTZ001 -- rejected datetime boundary
