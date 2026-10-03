"""``aas-calendar-declaration-v1``: one venue's declared sessions and their source table.

A declaration states, for every calendar date in ``[from, to)``, whether the venue is open
and its local hours. It is a compact document, strict UTF-8 JSON of at most 8 MiB with no
unknown, missing or duplicate keys::

    {
      "schema": "aas-calendar-declaration-v1",
      "calendar_id": "XKRX", "venue": "XKRX", "timezone": "Asia/Seoul",
      "declared_at": "2026-10-03T00:00:00Z",
      "from": "1990-01-01", "to": "2028-01-01",
      "sources": ["..."],
      "regimes": [{"from": "...", "to": "...", "hours": {"mon": ["09:00", "15:30"], ...}}],
      "closed": ["2027-01-01", ...],
      "sessions": [{"date": "2027-01-04", "open": "10:00", "close": "15:30"}, ...]
    }

- ``regimes`` cover ``[from, to)`` contiguously; a regime's ``hours`` name the weekdays it
  opens and their regular local hours (``HH:MM``, open before close, within the day).
- ``closed`` lists the regime weekdays the venue does not open; ``sessions`` lists the
  open dates whose hours differ from their regime, or that fall on a weekday the regime
  does not open. Both are increasing and disjoint, and neither repeats what the regime
  already says, so one schedule has one spelling.
- ``declared_at`` is the instant the declaration was made, in UTC. A later declaration
  of the same calendar supersedes an earlier one; a correction is a new declaration.

The packaged declarations for XNYS and XKRX live beside this module and are regenerated
by ``scripts/calendar_declarations.py``. ``aas calendar refresh`` retains the document
bytes in ``raw/`` and commits one source table from them (``source_table``): one row per
calendar date, with local wall times and no time zone arithmetic, so the rows depend on
the document alone. ``calendar.declared@1`` maps that table to ``calendar_sessions``.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from importlib import resources
from itertools import pairwise
from typing import TYPE_CHECKING, Final, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aegis_alpha.engine.codec import decode_json

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    import pyarrow as pa

DECLARATION_SCHEMA: Final = "aas-calendar-declaration-v1"
MAX_DECLARATION_BYTES: Final = 8 * 1024 * 1024
SOURCE_PROVIDER: Final = "calendar"
SOURCE_SHAPE: Final = "declared-sessions"
SOURCE_MAJOR: Final = 1
SOURCE_TABLE: Final = "sessions"
PACKAGED: Final = ("XKRX", "XNYS")
WEEKDAYS: Final = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_ROOT: Final = frozenset(
    {
        "schema",
        "calendar_id",
        "venue",
        "timezone",
        "declared_at",
        "from",
        "to",
        "sources",
        "regimes",
        "closed",
        "sessions",
    }
)
_MIC: Final = re.compile(r"[A-Z0-9]{4}")
_ZONE: Final = re.compile(r"[A-Za-z][A-Za-z0-9_+-]*(?:/[A-Za-z0-9_+-]+)*")
_CLOCK: Final = re.compile(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]")
_INSTANT: Final = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{6})?Z")
_SHA256: Final = re.compile(r"[0-9a-f]{64}")
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND: Final = timedelta(microseconds=1)

type Hours = tuple[time, time]


@dataclass(frozen=True, slots=True)
class Regime:
    start: date
    end: date
    hours: Mapping[int, Hours]


@dataclass(frozen=True, slots=True)
class DeclaredDay:
    """One calendar date as the declaration states it, in local wall time."""

    session_date: date
    status: Literal["open", "closed"]
    open_local: datetime | None
    close_local: datetime | None


@dataclass(frozen=True, slots=True)
class Declaration:
    raw: bytes
    sha256: str
    calendar_id: str
    venue: str
    timezone: str
    declared_at: datetime
    start: date
    end: date
    sources: tuple[str, ...]
    regimes: tuple[Regime, ...]
    closed: frozenset[date]
    sessions: Mapping[date, Hours]

    @property
    def dataset_id(self) -> str:
        """The calendar's dataset: ``sessions.<calendar_id lowercased>``."""
        return "sessions." + self.calendar_id.lower()

    @property
    def declared_at_us(self) -> int:
        return (self.declared_at - _EPOCH) // _MICROSECOND

    def regime(self, day: date) -> Regime:
        for regime in self.regimes:
            if regime.start <= day < regime.end:
                return regime
        raise ValueError(f"{day} is outside the declaration")

    def days(self) -> Iterator[DeclaredDay]:
        """Every date of ``[from, to)`` in order, open with its hours or closed."""
        day = self.start
        while day < self.end:
            hours = self.sessions.get(day)
            if hours is None and day not in self.closed:
                hours = self.regime(day).hours.get(day.weekday())
            if hours is None:
                yield DeclaredDay(day, "closed", None, None)
            else:
                yield DeclaredDay(
                    day,
                    "open",
                    datetime.combine(day, hours[0]),
                    datetime.combine(day, hours[1]),
                )
            day += timedelta(days=1)

    def summary(self) -> dict[str, object]:
        years: dict[str, dict[str, int]] = {}
        for item in self.days():
            counts = years.setdefault(str(item.session_date.year), {"open": 0, "closed": 0})
            counts[item.status] += 1
        return {
            "schema": DECLARATION_SCHEMA,
            "sha256": self.sha256,
            "calendar_id": self.calendar_id,
            "venue": self.venue,
            "timezone": self.timezone,
            "declared_at": _instant_text(self.declared_at),
            "from": self.start.isoformat(),
            "to": self.end.isoformat(),
            "regimes": len(self.regimes),
            "closed": len(self.closed),
            "special_sessions": len(self.sessions),
            "years": years,
        }


def _instant_text(moment: datetime) -> str:
    text = moment.astimezone(UTC).replace(tzinfo=None).isoformat()
    return text + "Z"


def _object(value: object, keys: frozenset[str], name: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError(f"calendar declaration {name} needs exactly {sorted(keys)}")
    return value


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
        raise ValueError(f"calendar declaration {name} must be exact nonempty text")
    return value


def _day(value: object, name: str) -> date:
    text = _text(value, name)
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        raise ValueError(f"calendar declaration {name} must be an ISO date") from None
    if parsed.isoformat() != text:
        raise ValueError(f"calendar declaration {name} must be an ISO date")
    return parsed


def _clock(value: object, name: str) -> time:
    text = _text(value, name)
    if _CLOCK.fullmatch(text) is None:
        raise ValueError(f"calendar declaration {name} must be a local HH:MM time")
    return time.fromisoformat(text)


def _hours(value: object, name: str) -> Hours:
    if not isinstance(value, list) or len(value) != 2:  # noqa: PLR2004 -- open and close
        raise ValueError(f"calendar declaration {name} must be [open, close]")
    opened, closed = _clock(value[0], name), _clock(value[1], name)
    if opened >= closed:
        raise ValueError(f"calendar declaration {name} must open before it closes")
    return opened, closed


def _instant(value: object) -> datetime:
    text = _text(value, "declared_at")
    if _INSTANT.fullmatch(text) is None:
        raise ValueError("calendar declaration declared_at must be a UTC instant ending in Z")
    try:
        return datetime.fromisoformat(text.removesuffix("Z")).replace(tzinfo=UTC)
    except ValueError:
        raise ValueError("calendar declaration declared_at must be a valid instant") from None


def _regimes(value: object, start: date, end: date) -> tuple[Regime, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError("calendar declaration needs at least one regime")
    regimes = []
    cursor = start
    for entry in value:
        item = _object(entry, frozenset({"from", "to", "hours"}), "regime")
        begin, finish = _day(item["from"], "regime from"), _day(item["to"], "regime to")
        if begin != cursor or begin >= finish:
            raise ValueError("calendar declaration regimes must cover [from, to) in order")
        cursor = finish
        hours = item["hours"]
        if not isinstance(hours, dict) or not hours or not set(hours) <= set(WEEKDAYS):
            raise ValueError("a calendar regime opens on named weekdays mon..sun")
        regimes.append(
            Regime(
                begin,
                finish,
                {
                    WEEKDAYS.index(name): _hours(hours[name], f"{name} hours")
                    for name in sorted(hours, key=WEEKDAYS.index)
                },
            )
        )
    if cursor != end:
        raise ValueError("calendar declaration regimes must cover [from, to) in order")
    return tuple(regimes)


def _increasing(days: list[date], name: str) -> None:
    if any(later <= earlier for earlier, later in pairwise(days)):
        raise ValueError(f"calendar declaration {name} must be strictly increasing")


def _decode(raw: bytes, sha256: str) -> dict[str, object]:
    if not isinstance(sha256, str) or _SHA256.fullmatch(sha256) is None:
        raise ValueError("calendar declaration hash must be lowercase SHA-256 hex")
    if len(raw) > MAX_DECLARATION_BYTES:
        raise ValueError("calendar declaration exceeds 8 MiB")
    if hashlib.sha256(raw).hexdigest() != sha256:
        raise ValueError("calendar declaration bytes do not match the expected SHA-256")
    if raw.startswith(b"\xef\xbb\xbf") or b"\x00" in raw:
        raise ValueError("calendar declaration must be UTF-8 JSON without BOM or NUL")
    try:
        raw.decode("utf-8", errors="strict")
        document = decode_json(raw)
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise ValueError(f"calendar declaration is not strict JSON: {error}") from None
    body = _object(document, _ROOT, "document")
    if body["schema"] != DECLARATION_SCHEMA:
        raise ValueError("unsupported calendar declaration schema")
    return body


def _venue(body: Mapping[str, object]) -> tuple[str, str, str]:
    calendar_id, venue = _text(body["calendar_id"], "calendar_id"), _text(body["venue"], "venue")
    if _MIC.fullmatch(calendar_id) is None or _MIC.fullmatch(venue) is None:
        raise ValueError("calendar_id and venue must be four-character uppercase MICs")
    zone = _text(body["timezone"], "timezone")
    if _ZONE.fullmatch(zone) is None:
        raise ValueError("calendar declaration timezone must be an IANA zone name")
    try:
        ZoneInfo(zone)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError("calendar declaration timezone must be an IANA zone name") from None
    return calendar_id, venue, zone


def _exceptions(body: Mapping[str, object]) -> tuple[list[date], dict[date, Hours]]:
    listed_closed, listed_sessions = body["closed"], body["sessions"]
    if not isinstance(listed_closed, list) or not isinstance(listed_sessions, list):
        raise TypeError("calendar declaration closed and sessions must be lists")
    closed = [_day(item, "closed date") for item in listed_closed]
    _increasing(closed, "closed")
    sessions: dict[date, Hours] = {}
    for entry in listed_sessions:
        item = _object(entry, frozenset({"date", "open", "close"}), "session")
        sessions[_day(item["date"], "session date")] = _hours(
            [item["open"], item["close"]], f"{item['date']} hours"
        )
    if len(sessions) != len(listed_sessions):
        raise ValueError("calendar declaration sessions must be strictly increasing")
    _increasing(list(sessions), "sessions")
    return closed, sessions


def _check_exceptions(declaration: Declaration) -> None:
    """Refuse exceptions outside the declaration or that repeat what a regime says."""
    start, end = declaration.start, declaration.end
    for day in sorted(declaration.closed):
        if not start <= day < end or day.weekday() not in declaration.regime(day).hours:
            raise ValueError(f"closed date {day} is not a regime weekday inside the declaration")
    for day, hours in declaration.sessions.items():
        if not start <= day < end or day in declaration.closed:
            raise ValueError(f"session {day} is outside the declaration or also closed")
        if declaration.regime(day).hours.get(day.weekday()) == hours:
            raise ValueError(f"session {day} repeats its regime hours")


def parse_declaration(raw: bytes, sha256: str) -> Declaration:
    """Validate an exact declaration document against its SHA-256; nothing is stored."""
    body = _decode(raw, sha256)
    calendar_id, venue, zone = _venue(body)
    start, end = _day(body["from"], "from"), _day(body["to"], "to")
    if start >= end:
        raise ValueError("calendar declaration dates must be increasing")
    sources = body["sources"]
    if not isinstance(sources, list) or not sources:
        raise ValueError("calendar declaration names at least one source")
    closed, sessions = _exceptions(body)
    declaration = Declaration(
        raw=raw,
        sha256=sha256,
        calendar_id=calendar_id,
        venue=venue,
        timezone=zone,
        declared_at=_instant(body["declared_at"]),
        start=start,
        end=end,
        sources=tuple(_text(item, "source") for item in sources),
        regimes=_regimes(body["regimes"], start, end),
        closed=frozenset(closed),
        sessions=sessions,
    )
    _check_exceptions(declaration)
    return declaration


def packaged_declaration(calendar_id: str) -> bytes:
    """The exact bytes of the declaration this package ships for ``calendar_id``."""
    if calendar_id not in PACKAGED:
        raise ValueError(f"no packaged declaration for {calendar_id}; pass --declaration")
    package = resources.files("aegis_alpha.storage") / "calendar_declarations"
    return (package / f"{calendar_id.lower()}.json").read_bytes()


def source_rows(declaration: Declaration) -> Iterator[tuple[object, ...]]:
    """The source table's rows in column order (``SOURCE_COLUMNS``)."""
    for item in declaration.days():
        yield (
            declaration.calendar_id,
            declaration.venue,
            declaration.timezone,
            item.session_date,
            item.status,
            item.open_local,
            item.close_local,
            declaration.declared_at,
        )


SOURCE_COLUMNS: Final = (
    "calendar_id",
    "venue",
    "timezone",
    "session_date",
    "status",
    "open_local",
    "close_local",
    "declared_at",
)


def source_table(declaration: Declaration) -> pa.Table:
    """The one source table committed from a declaration (requires ``pyarrow``)."""
    import pyarrow as pa  # noqa: PLC0415 -- the source library's Arrow loaders need the legacy extra

    schema = pa.schema(
        [
            ("calendar_id", pa.string()),
            ("venue", pa.string()),
            ("timezone", pa.string()),
            ("session_date", pa.date32()),
            ("status", pa.string()),
            ("open_local", pa.timestamp("us")),
            ("close_local", pa.timestamp("us")),
            ("declared_at", pa.timestamp("us", tz="UTC")),
        ]
    )
    columns = list(zip(*source_rows(declaration), strict=True))
    return pa.table(
        {name: list(column) for name, column in zip(SOURCE_COLUMNS, columns, strict=True)},
        schema=schema,
    )
