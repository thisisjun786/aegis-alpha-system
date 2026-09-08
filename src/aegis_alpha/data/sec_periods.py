"""SEC reporting-period identity, adapted from Vibe-Trading sec_frames.py.

Upstream fb5013c2e37ff992ce2e76f0e19219d631eebc2b, v0.1.14.
Changes: stdlib dates; invalid dates and reversed intervals raise instead of
being classified as instant; cadence is explicit. No financial values computed.

MIT License
Copyright (c) 2026 Vibe-Trading Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from enum import StrEnum

QUARTER_SPAN_DAYS = (60, 120)
ANNUAL_SPAN_DAYS = (330, 380)


class PeriodKind(StrEnum):
    INSTANT = "instant"
    QUARTER = "quarter"
    ANNUAL = "annual"
    YTD_OR_STUB = "ytd_or_stub"


def _date(value: object) -> date:
    if not isinstance(value, str):
        raise TypeError("SEC period date must be ISO date text")
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("SEC period date must be canonical ISO date text")
    return parsed


def frame_key(row: Mapping[str, object]) -> tuple[date | None, date]:
    """Start and end distinguish quarter/YTD rows ending on the same date.

    This is only the reporting-period key. A complete fact identity also needs
    CIK, concept, unit and filing/revision provenance.
    """
    end = _date(row.get("end"))
    start = None if row.get("start") is None else _date(row["start"])
    if start is not None and start > end:
        raise ValueError("SEC reporting period is reversed")
    return start, end


def classify_frame(row: Mapping[str, object]) -> PeriodKind:
    start, end = frame_key(row)
    if start is None:
        return PeriodKind.INSTANT
    days = (end - start).days
    if QUARTER_SPAN_DAYS[0] <= days <= QUARTER_SPAN_DAYS[1]:
        return PeriodKind.QUARTER
    if ANNUAL_SPAN_DAYS[0] <= days <= ANNUAL_SPAN_DAYS[1]:
        return PeriodKind.ANNUAL
    return PeriodKind.YTD_OR_STUB


def matches_cadence(row: Mapping[str, object], period: str) -> bool:
    if period not in {"quarter", "annual"}:
        raise ValueError("SEC cadence must be quarter or annual")
    kind = classify_frame(row)
    return kind is PeriodKind.INSTANT or kind.value == period
