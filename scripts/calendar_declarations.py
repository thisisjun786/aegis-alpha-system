"""Regenerate the packaged ``aas-calendar-declaration-v1`` documents for XNYS and XKRX.

The packaged declarations are data, reviewed as data: this script records how they were
derived so the next revision (a new year, a published closure) is reproduced rather than
hand-edited. It reads the session schedules of the ``exchange_calendars`` package, which
is not an AAS dependency, and applies the AAS corrections below, each with its evidence::

    uv run --no-sync --with exchange-calendars==4.13.2 \
        python -m scripts.calendar_declarations --declared-at 2026-10-03T00:00:00Z

Every hour regime is written out here and checked against the library's schedule, so a
library change that moves regular hours fails instead of silently becoming a new regime.
Sessions whose hours differ from their regime and closures on regime weekdays are listed
explicitly; nothing else is inferred. The output replaces the files under
``src/aegis_alpha/storage/calendar_declarations/``.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Final

EXCHANGE_CALENDARS: Final = "4.13.2"
FIRST: Final = date(1990, 1, 1)
END: Final = date(2028, 1, 1)
_WEEK: Final = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_OUTPUT: Final = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "aegis_alpha"
    / "storage"
    / "calendar_declarations"
)

# (from, to, weekdays, open, close) in local time; weekdays index _WEEK.
_REGIMES: Final = {
    "XNYS": ((FIRST, END, range(5), "09:30", "16:00"),),
    "XKRX": (
        (FIRST, date(1995, 1, 1), range(6), "09:40", "15:20"),
        (date(1995, 1, 1), date(1998, 12, 7), range(6), "09:30", "15:00"),
        (date(1998, 12, 7), date(2016, 8, 1), range(5), "09:00", "15:00"),
        (date(2016, 8, 1), END, range(5), "09:00", "15:30"),
    ),
}
_TIMEZONES: Final = {"XNYS": "America/New_York", "XKRX": "Asia/Seoul"}

# Corrections to the library schedule. Evidence names the AAS-retained source that
# decides each date; the KR bar history is the EODHD daily history in the source library
# and the KRX holiday list is the KRX global site's holiday table captured 2026-09-10.
_CLOSED: Final = {
    "XKRX": {
        "1995-06-27": "first nationwide local election, a public holiday; no KR history bar",
        "1996-04-11": "15th National Assembly election, a public holiday; no KR history bar",
        "1998-06-04": "second nationwide local election, a public holiday; no KR history bar",
        "2026-06-03": "ninth nationwide local election; KRX holiday list; no KR history bar",
        "2026-07-17": "Constitution Day; KRX holiday list; no KR history bar",
    },
}
_OPENED: Final = {
    "XKRX": {
        "1997-12-29": "KR history bars carry volume for 492 of the 507 instruments that day",
    },
}
# Hours that replace the library's for one session. The college entrance examination
# (CSAT) day opens an hour late and closes an hour late; the library stops listing it
# after 2020. A later declared close only delays a rule time, so it stays an upper bound.
_HOURS: Final = {
    "XKRX": {
        "2021-11-18": ("10:00", "16:30"),
        "2022-11-17": ("10:00", "16:30"),
        "2023-11-16": ("10:00", "16:30"),
        "2024-11-14": ("10:00", "16:30"),
        "2025-11-13": ("10:00", "16:30"),
        "2026-11-19": ("10:00", "16:30"),
        # The library opens this session late without a published basis; regular hours.
        "2027-12-30": ("09:00", "15:30"),
    },
}
_SOURCES: Final = {
    "XNYS": [
        f"exchange_calendars {EXCHANGE_CALENDARS} XNYS schedule (Apache-2.0)",
        "NYSE holidays and trading hours, https://www.nyse.com/trade/hours-calendars",
        (
            "Norgate US daily history in the AAS source library: identical session dates "
            "1990-01-02..2026-07-28"
        ),
    ],
    "XKRX": [
        f"exchange_calendars {EXCHANGE_CALENDARS} XKRX schedule (Apache-2.0)",
        "KRX holiday list 2021-2026, https://global.krx.co.kr (captured 2026-09-10)",
        "EODHD KR daily history in the AAS source library (sessions with traded volume)",
        (
            "Saturday sessions before 1998-12-07 carry the weekday close: the half-day close "
            "is unverified and the weekday close bounds it from above"
        ),
        (
            "2027 holidays follow the public holiday law as of the declaration; KRX publishes "
            "the year's list in December and a change is a new declaration"
        ),
    ],
}


def _hours(stamp: object, zone: str) -> str:
    return stamp.tz_convert(zone).strftime("%H:%M")  # type: ignore[attr-defined] -- pandas


def _schedule(calendar_id: str) -> dict[date, tuple[str, str]]:
    import exchange_calendars  # noqa: PLC0415 -- generator-only dependency

    if exchange_calendars.__version__ != EXCHANGE_CALENDARS:
        raise SystemExit(f"exchange_calendars {EXCHANGE_CALENDARS} is required")
    calendar = exchange_calendars.get_calendar(
        calendar_id, start=FIRST.isoformat(), end=(END - timedelta(days=1)).isoformat()
    )
    zone = _TIMEZONES[calendar_id]
    return {
        day.date(): (_hours(row["open"], zone), _hours(row["close"], zone))
        for day, row in calendar.schedule.iterrows()
    }


def _corrected(calendar_id: str) -> dict[date, tuple[str, str]]:
    sessions = _schedule(calendar_id)
    for text in _CLOSED.get(calendar_id, {}):
        day = date.fromisoformat(text)
        if day not in sessions:
            raise SystemExit(f"{calendar_id} {text} is already closed; drop the correction")
        del sessions[day]
    for text in _OPENED.get(calendar_id, {}):
        day = date.fromisoformat(text)
        if day in sessions:
            raise SystemExit(f"{calendar_id} {text} is already open; drop the correction")
        sessions[day] = _regime(calendar_id, day)[1]
    for text, hours in _HOURS.get(calendar_id, {}).items():
        day = date.fromisoformat(text)
        if day not in sessions or sessions[day] == hours:
            raise SystemExit(f"{calendar_id} {text} needs no hours correction")
        sessions[day] = hours
    return sessions


def _regime(calendar_id: str, day: date) -> tuple[range, tuple[str, str]]:
    for start, end, weekdays, opened, closed in _REGIMES[calendar_id]:
        if start <= day < end:
            return weekdays, (opened, closed)
    raise SystemExit(f"{day} is outside every {calendar_id} regime")


def declaration(calendar_id: str, declared_at: str) -> dict[str, object]:
    sessions = _corrected(calendar_id)
    closed: list[str] = []
    special: list[dict[str, str]] = []
    day = FIRST
    while day < END:
        weekdays, hours = _regime(calendar_id, day)
        found = sessions.get(day)
        if found is None and day.weekday() in weekdays:
            closed.append(day.isoformat())
        elif found is not None and (day.weekday() not in weekdays or found != hours):
            special.append({"date": day.isoformat(), "open": found[0], "close": found[1]})
        day += timedelta(days=1)
    # Each regime's hours must be what most of its sessions use, or the regime is wrong.
    for start, end, weekdays, opened, closed_at in _REGIMES[calendar_id]:
        inside = [
            hours
            for value, hours in sessions.items()
            if start <= value < end and value.weekday() in weekdays
        ]
        if max(set(inside), key=inside.count) != (opened, closed_at):
            raise SystemExit(f"{calendar_id} regime from {start} does not match the schedule")
    return {
        "schema": "aas-calendar-declaration-v1",
        "calendar_id": calendar_id,
        "venue": calendar_id,
        "timezone": _TIMEZONES[calendar_id],
        "declared_at": declared_at,
        "from": FIRST.isoformat(),
        "to": END.isoformat(),
        "sources": _SOURCES[calendar_id],
        "regimes": [
            {
                "from": start.isoformat(),
                "to": end.isoformat(),
                "hours": {_WEEK[index]: [opened, closed_at] for index in weekdays},
            }
            for start, end, weekdays, opened, closed_at in _REGIMES[calendar_id]
        ],
        "closed": closed,
        "sessions": special,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--declared-at", required=True, help="UTC instant, YYYY-MM-DDTHH:MM:SSZ")
    parser.add_argument("--output", type=Path, default=_OUTPUT)
    args = parser.parse_args()
    for calendar_id in ("XNYS", "XKRX"):
        text = json.dumps(declaration(calendar_id, args.declared_at), indent=1) + "\n"
        (args.output / f"{calendar_id.lower()}.json").write_text(text, encoding="ascii")


if __name__ == "__main__":
    main()
