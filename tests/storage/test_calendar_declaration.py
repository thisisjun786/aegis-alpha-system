# ruff: noqa: PLR2004 -- session counts and dates are the expected values
"""``aas-calendar-declaration-v1``: the packaged XNYS/XKRX declarations and the parser.

Expected sessions are spelled from the venues' published schedules, not from the
declaration code: holidays, early closes and late opens are written out by date.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, time

import pytest

from aegis_alpha.storage.calendar_declaration import (
    DeclaredDay,
    packaged_declaration,
    parse_declaration,
    source_rows,
)


def _packaged(calendar_id: str) -> dict[date, DeclaredDay]:
    raw = packaged_declaration(calendar_id)
    declaration = parse_declaration(raw, hashlib.sha256(raw).hexdigest())
    return {item.session_date: item for item in declaration.days()}


def _hours(day: DeclaredDay) -> tuple[str, str] | None:
    if day.open_local is None or day.close_local is None:
        return None
    return day.open_local.strftime("%H:%M"), day.close_local.strftime("%H:%M")


def test_packaged_declarations_cover_the_next_year() -> None:
    for calendar_id in ("XNYS", "XKRX"):
        raw = packaged_declaration(calendar_id)
        declaration = parse_declaration(raw, hashlib.sha256(raw).hexdigest())
        assert declaration.dataset_id == "sessions." + calendar_id.lower()
        # Declared on 2026-10-03, so the next year is 2027 and every date through it.
        assert declaration.declared_at == datetime(2026, 10, 3, tzinfo=UTC)
        assert (declaration.start, declaration.end) == (date(1990, 1, 1), date(2028, 1, 1))
    nyse, krx = _packaged("XNYS"), _packaged("XKRX")
    assert len(nyse) == len(krx) == (date(2028, 1, 1) - date(1990, 1, 1)).days
    nyse_2027 = [day for day, item in nyse.items() if day.year == 2027 and item.status == "open"]
    krx_2027 = [day for day, item in krx.items() if day.year == 2027 and item.status == "open"]
    assert (len(nyse_2027), len(krx_2027)) == (251, 247)
    # NYSE 2027: published holidays and the early close after Thanksgiving.
    for text in (
        "2027-01-01",
        "2027-01-18",
        "2027-02-15",
        "2027-03-26",
        "2027-05-31",
        "2027-06-18",
        "2027-07-05",
        "2027-09-06",
        "2027-11-25",
        "2027-12-24",
    ):
        assert nyse[date.fromisoformat(text)].status == "closed", text
    assert _hours(nyse[date(2027, 11, 26)]) == ("09:30", "13:00")
    assert _hours(nyse[date(2027, 12, 23)]) == ("09:30", "16:00")
    # KRX: the 2026 election and Constitution Day closures, the CSAT day and year end.
    assert krx[date(2026, 6, 3)].status == "closed"
    assert krx[date(2026, 7, 17)].status == "closed"
    assert _hours(krx[date(2026, 11, 19)]) == ("10:00", "16:30")
    assert _hours(krx[date(2026, 12, 30)]) == ("09:00", "15:30")
    assert krx[date(2026, 12, 31)].status == "closed"
    assert _hours(krx[date(2027, 1, 4)]) == ("10:00", "15:30")
    for text in ("2027-02-08", "2027-02-09", "2027-05-13", "2027-09-15", "2027-12-27"):
        assert krx[date.fromisoformat(text)].status == "closed", text
    # Saturday sessions end with the week of 1998-12-07, and so does the 09:30 open.
    assert _hours(krx[date(1998, 12, 5)]) == ("09:30", "15:00")
    assert krx[date(1998, 12, 12)].status == "closed"
    assert _hours(krx[date(1998, 12, 7)]) == ("09:00", "15:00")
    assert _hours(krx[date(2016, 8, 1)]) == ("09:00", "15:30")


def _document(**changes: object) -> dict[str, object]:
    document: dict[str, object] = {
        "schema": "aas-calendar-declaration-v1",
        "calendar_id": "XTST",
        "venue": "XTST",
        "timezone": "Asia/Seoul",
        "declared_at": "2025-01-01T00:00:00Z",
        "from": "2025-01-06",
        "to": "2025-01-20",
        "sources": ["synthetic"],
        "regimes": [
            {
                "from": "2025-01-06",
                "to": "2025-01-20",
                "hours": {name: ["09:00", "15:30"] for name in ("mon", "tue", "wed", "thu", "fri")},
            }
        ],
        "closed": ["2025-01-08"],
        "sessions": [
            {"date": "2025-01-09", "open": "10:00", "close": "15:30"},
            {"date": "2025-01-11", "open": "09:00", "close": "12:00"},
        ],
    }
    document.update(changes)
    return document


def _parse(document: dict[str, object]) -> object:
    raw = json.dumps(document).encode()
    return parse_declaration(raw, hashlib.sha256(raw).hexdigest())


def test_source_rows_tabulate_every_date() -> None:
    raw = json.dumps(_document()).encode()
    declaration = parse_declaration(raw, hashlib.sha256(raw).hexdigest())
    declared = datetime(2025, 1, 1, tzinfo=UTC)

    def opened(day: int, start: time, end: time) -> tuple[object, ...]:
        moment = date(2025, 1, day)
        return (
            "XTST",
            "XTST",
            "Asia/Seoul",
            moment,
            "open",
            datetime.combine(moment, start),
            datetime.combine(moment, end),
            declared,
        )

    def closed(day: int) -> tuple[object, ...]:
        return ("XTST", "XTST", "Asia/Seoul", date(2025, 1, day), "closed", None, None, declared)

    regular = (time(9), time(15, 30))
    assert list(source_rows(declaration)) == [
        opened(6, *regular),
        opened(7, *regular),
        closed(8),
        opened(9, time(10), time(15, 30)),
        opened(10, *regular),
        opened(11, time(9), time(12)),
        closed(12),
        *(opened(day, *regular) for day in (13, 14, 15, 16, 17)),
        closed(18),
        closed(19),
    ]


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"extra": 1}, "needs exactly"),
        ({"calendar_id": "xtst"}, "MIC"),
        ({"timezone": "Mars/Base"}, "IANA"),
        ({"declared_at": "2025-01-01T00:00:00+00:00"}, "ending in Z"),
        ({"to": "2025-01-27"}, "cover"),
        ({"closed": ["2025-01-11"]}, "not a regime weekday"),
        ({"closed": ["2025-01-08", "2025-01-08"]}, "strictly increasing"),
        ({"sessions": [{"date": "2025-01-09", "open": "09:00", "close": "15:30"}]}, "repeats"),
        ({"sessions": [{"date": "2025-01-08", "open": "10:00", "close": "15:30"}]}, "also closed"),
        ({"sessions": [{"date": "2025-01-09", "open": "15:30", "close": "10:00"}]}, "before"),
        ({"sessions": [{"date": "2025-01-09", "open": "9:00", "close": "15:30"}]}, "HH:MM"),
        ({"sources": []}, "source"),
    ],
)
def test_declaration_has_one_spelling(changes: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _parse(_document(**changes))


def test_declaration_bytes_are_exact() -> None:
    raw = json.dumps(_document()).encode()
    with pytest.raises(ValueError, match="SHA-256"):
        parse_declaration(raw, hashlib.sha256(raw + b" ").hexdigest())
    duplicated = raw.replace(b'"venue": "XTST"', b'"venue": "XTST", "venue": "XTST"')
    with pytest.raises(ValueError, match="strict JSON"):
        parse_declaration(duplicated, hashlib.sha256(duplicated).hexdigest())
