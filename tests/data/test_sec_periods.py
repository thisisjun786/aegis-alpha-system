from __future__ import annotations

from datetime import date, timedelta

import pytest

from aegis_alpha.data.sec_periods import PeriodKind, classify_frame, frame_key, matches_cadence


def test_quarter_and_ytd_same_end_are_distinct() -> None:
    quarter = {"start": "2025-07-01", "end": "2025-09-30"}
    ytd = {"start": "2025-01-01", "end": "2025-09-30"}
    assert frame_key(quarter) != frame_key(ytd)
    assert matches_cadence(quarter, "quarter")
    assert not matches_cadence(ytd, "quarter")
    assert classify_frame(ytd) is PeriodKind.YTD_OR_STUB


@pytest.mark.parametrize(
    ("days", "expected"),
    [
        (59, "ytd_or_stub"),
        (60, "quarter"),
        (120, "quarter"),
        (121, "ytd_or_stub"),
        (329, "ytd_or_stub"),
        (330, "annual"),
        (380, "annual"),
        (381, "ytd_or_stub"),
    ],
)
def test_duration_boundaries(days: int, expected: str) -> None:
    start = date(2024, 1, 1)
    assert (
        classify_frame(
            {"start": start.isoformat(), "end": (start + timedelta(days=days)).isoformat()}
        )
        == expected
    )


@pytest.mark.parametrize("start", ["", "invalid", "2026-01-01", "20250101", 0])
def test_invalid_present_start_is_not_instant(start: object) -> None:
    with pytest.raises((ValueError, TypeError)):
        classify_frame({"start": start, "end": "2025-12-31"})


def test_instant_requires_valid_end_and_cadence() -> None:
    assert classify_frame({"end": "2025-12-31"}) is PeriodKind.INSTANT
    assert matches_cadence({"end": "2025-12-31"}, "annual")
    with pytest.raises((ValueError, TypeError), match="date"):
        classify_frame({})
    with pytest.raises(ValueError, match="cadence"):
        matches_cadence({"end": "2025-12-31"}, "monthly")
