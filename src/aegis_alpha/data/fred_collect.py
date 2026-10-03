"""FRED and ALFRED requests, answers and the daily plan of the native FRED collector.

Endpoints (``Request.provider`` is ``fred``):

- ``observations`` (``fred/series/observations``): one page of a series' ALFRED rows for
  one real-time window, ``series_id``, ``realtime_start``, ``realtime_end``, ``limit``
  (100000), ``offset`` and ``file_type=json``.
- ``vintage_dates`` (``fred/series/vintagedates``): the vintage dates of a series inside a
  real-time window, ``series_id``, ``realtime_start``, ``realtime_end``, ``limit`` (10000),
  ``offset`` and ``file_type=json``.
- ``series_csv`` (``fredgraph.csv``): the current values of one series as the
  ``observation_date,<SERIES>`` download, parameter ``id``. It needs no key.

ALFRED reports a row's real-time period clipped to the query window: a vintage that began
before the query's ``realtime_start`` is reported as starting on it, and a period that ends
after ``realtime_end`` as ending on it. FRED also refuses an observations window that spans
more than 2000 vintage dates; the collector asks windows of at most 1990, counting a window's
start when it is a vintage date, so the window stays inside the limit however FRED counts
that start. So, for each series:

1. ``vintage_dates`` lists the vintage dates after the latest vintage day already collected
   (``known``), or all of them from the ALFRED origin (1776-07-04) when nothing is known, up
   to the last ended FRED (St. Louis, ``America/Chicago``) day. None means the series is
   current.
2. The observations windows run from ``known`` (or the origin) to that day, each spanning at
   most 1990 vintage dates and each starting on the last vintage date of the window before
   it (``observation_windows``).
3. In every window that does not start at the origin, a row starting on the window's start
   restates a period an earlier window or collection already holds (clipped, or genuinely
   starting there); every other row is a vintage as FRED dated it (``split_rows``). The
   collector keeps those rows and counts the restated ones.

Because the windows end on an ended day, no vintage day is collected half published, and
because windows are asked in order and a series stops at its first incomplete window, every
vintage up to the latest collected one has been collected.
"""

from __future__ import annotations

import hashlib
import json
import re
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Final, cast
from zoneinfo import ZoneInfo

from aegis_alpha.data.fred_alfred_series import MACRO_SERIES_IDS
from aegis_alpha.data.opendart import Clock, Transport, TransportError, canonical, utc_now
from aegis_alpha.data.provider_request import COMPLETED, FAILED, NO_DATA, Request, Response

PROVIDER: Final = "fred"
OBSERVATIONS: Final = "observations"
VINTAGE_DATES: Final = "vintage_dates"
SERIES_CSV: Final = "series_csv"
URLS: Final[Mapping[str, str]] = {
    OBSERVATIONS: "https://api.stlouisfed.org/fred/series/observations",
    VINTAGE_DATES: "https://api.stlouisfed.org/fred/series/vintagedates",
    SERIES_CSV: "https://fred.stlouisfed.org/graph/fredgraph.csv",
}
KEYED: Final = frozenset({OBSERVATIONS, VINTAGE_DATES})
FRED_ZONE: Final = ZoneInfo("America/Chicago")
ORIGIN: Final = date(1776, 7, 4)
PAGE_LIMIT: Final = 100_000
VINTAGE_LIMIT: Final = 10_000
# FRED refuses an observations window that spans more than 2000 vintage dates; a window
# holds at most this many, its start included, which leaves a margin below that limit.
MAX_WINDOW_VINTAGES: Final = 1_990
DEFAULT_ALFRED_SERIES: Final = (*MACRO_SERIES_IDS, "DEXKOUS")
DEFAULT_CSV_SERIES: Final[Mapping[str, str]] = {"DEXKOUS": "fx.usdkrw.fred"}
ALFRED_DATASET: Final = "macro.us.alfred"
# A refused key and the rate limit end the run: no later call in it can be answered.
STOP_HTTP: Final = frozenset({401, 403, 429})
MAX_RESPONSE_BYTES: Final = 128 * 1024 * 1024
_SERIES: Final = re.compile(r"[A-Z0-9][A-Z0-9_]{0,63}")
_DAY: Final = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_COUNT: Final = re.compile(r"0|[1-9][0-9]{0,8}")
_KEY: Final = re.compile(r"[a-z0-9]{32}")
_OBSERVATION_KEYS: Final = frozenset({"realtime_start", "realtime_end", "date", "value"})
_HTTP_OK: Final = 200
_HTTP_BAD_REQUEST: Final = 400


def _day(text: object, name: str) -> date:
    if not isinstance(text, str) or _DAY.fullmatch(text) is None:
        raise ValueError(f"{name} must be YYYY-MM-DD")
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise ValueError(f"{name} must be a calendar date") from None


def fred_day(moment: datetime) -> date:
    """The FRED (St. Louis) calendar date of an instant."""
    return moment.astimezone(FRED_ZONE).date()


def series_id(value: str) -> str:
    if not isinstance(value, str) or _SERIES.fullmatch(value) is None:
        raise ValueError(f"FRED series IDs are upper-case letters and digits: {value!r}")
    return value


# --- requests ---------------------------------------------------------------------------------


def _window(series: str, start: date, end: date, limit: int, offset: int) -> dict[str, str]:
    if not ORIGIN <= start <= end:
        raise ValueError("a FRED real-time window runs forward from 1776-07-04")
    if offset < 0 or offset % limit:
        raise ValueError("FRED page offsets are whole pages")
    return {
        "file_type": "json",
        "limit": str(limit),
        "offset": str(offset),
        "realtime_end": end.isoformat(),
        "realtime_start": start.isoformat(),
        "series_id": series_id(series),
    }


def observations(series: str, start: date, end: date, offset: int = 0) -> Request:
    return Request.of(PROVIDER, OBSERVATIONS, _window(series, start, end, PAGE_LIMIT, offset))


def vintage_dates(series: str, start: date, end: date, offset: int = 0) -> Request:
    return Request.of(PROVIDER, VINTAGE_DATES, _window(series, start, end, VINTAGE_LIMIT, offset))


def series_csv(series: str) -> Request:
    return Request.of(PROVIDER, SERIES_CSV, {"id": series_id(series)})


@dataclass(frozen=True, slots=True)
class Window:
    """The ALFRED window an ``observations`` or ``vintage_dates`` request asks."""

    series_id: str
    start: date
    end: date
    limit: int
    offset: int

    @property
    def query(self) -> tuple[str, str, str]:
        """The pages of one query share it: endpoint-independent series and window."""
        return (self.series_id, self.start.isoformat(), self.end.isoformat())


def window(request: Request) -> Window:
    """The checked window of a keyed request; a request of another shape is refused."""
    if request.provider != PROVIDER or request.endpoint not in KEYED:
        raise ValueError("only ALFRED requests have a real-time window")
    parameters = request.parameters
    limit = PAGE_LIMIT if request.endpoint == OBSERVATIONS else VINTAGE_LIMIT
    offset_text = parameters.get("offset", "")
    if _COUNT.fullmatch(offset_text) is None:
        raise ValueError("FRED offsets are decimal counts")
    expected = _window(
        parameters.get("series_id", ""),
        _day(parameters.get("realtime_start"), "realtime_start"),
        _day(parameters.get("realtime_end"), "realtime_end"),
        limit,
        int(offset_text),
    )
    if parameters != expected:
        raise ValueError(f"FRED {request.endpoint} request parameters are not canonical")
    return Window(
        expected["series_id"],
        date.fromisoformat(expected["realtime_start"]),
        date.fromisoformat(expected["realtime_end"]),
        limit,
        int(offset_text),
    )


def check(request: Request) -> None:
    """Refuse a request the collector does not make."""
    if request.provider != PROVIDER or request.endpoint not in URLS:
        raise ValueError(f"unsupported FRED request {request.endpoint!r}")
    if request.endpoint in KEYED:
        window(request)
    elif set(request.parameters) != {"id"}:
        raise ValueError("series_csv takes exactly id")
    else:
        series_id(request.parameters["id"])


# --- answers ----------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Page:
    """A checked ``observations`` page: the query's total and its rows in FRED's order."""

    count: int
    rows: tuple[tuple[date, date, date, str], ...]  # observation, realtime start, end, value


def _json(body: bytes) -> dict[str, object]:
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("FRED answer is not UTF-8 JSON") from None
    if not isinstance(value, dict):
        raise ValueError("FRED answer is not a JSON object")  # noqa: TRY004 -- malformed-content ValueError contract
    return cast("dict[str, object]", value)


def _paging(document: Mapping[str, object], asked: Window) -> int:
    for name, expected in (
        ("realtime_start", asked.start.isoformat()),
        ("realtime_end", asked.end.isoformat()),
    ):
        if document.get(name) != expected:
            raise ValueError(f"FRED answer {name} is not the asked window")
    count, offset, limit = document.get("count"), document.get("offset"), document.get("limit")
    for value in (count, offset, limit):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("FRED answer count, offset and limit are counts")
    if (offset, limit) != (asked.offset, asked.limit):
        raise ValueError("FRED answer is not the asked page")
    return cast("int", count)


def _expected_rows(count: int, asked: Window) -> int:
    return max(0, min(asked.limit, count - asked.offset))


def parse_page(request: Request, body: bytes) -> Page:
    """The rows of an answered ``observations`` page, checked against its request.

    Every row starts and ends inside the asked window, the page holds the rows its
    offset and the query count call for, and every value is text; anything else refuses
    the page.
    """
    asked = window(request)
    document = _json(body)
    count = _paging(document, asked)
    items = document.get("observations")
    if not isinstance(items, list) or len(items) != _expected_rows(count, asked):
        raise ValueError("FRED observations page does not hold its share of the count")
    rows: list[tuple[date, date, date, str]] = []
    for item in items:
        if not isinstance(item, dict) or set(item) != _OBSERVATION_KEYS:
            raise ValueError("FRED observations rows hold exactly date, value and real time")
        value = item["value"]
        if not isinstance(value, str):
            raise ValueError("FRED observation values are text")  # noqa: TRY004 -- malformed-content ValueError contract
        start = _day(item["realtime_start"], "realtime_start")
        end = _day(item["realtime_end"], "realtime_end")
        if not asked.start <= start <= end <= asked.end:
            raise ValueError("an ALFRED row lies outside the asked real-time window")
        rows.append((_day(item["date"], "date"), start, end, value))
    return Page(count, tuple(rows))


def parse_vintage_dates(request: Request, body: bytes) -> tuple[int, tuple[date, ...]]:
    """The query count and the vintage dates of an answered ``vintage_dates`` page."""
    asked = window(request)
    document = _json(body)
    count = _paging(document, asked)
    items = document.get("vintage_dates")
    if not isinstance(items, list) or len(items) != _expected_rows(count, asked):
        raise ValueError("FRED vintage dates page does not hold its share of the count")
    days = tuple(_day(item, "vintage date") for item in items)
    if any(not asked.start <= day <= asked.end for day in days):
        raise ValueError("a FRED vintage date lies outside the asked window")
    return count, days


def csv_header(request: Request) -> bytes:
    return f"observation_date,{request.parameters['id']}".encode()


def classify(request: Request, response: Response) -> tuple[str, str | None]:
    """The outcome and FRED's error text: the outcome only routes the collector.

    A checked answer is ``COMPLETED``; a series FRED says does not exist is ``NO_DATA``;
    anything else (an error answer, an unexpected shape) is ``FAILED``.
    """
    if request.endpoint == SERIES_CSV:
        first = response.body.split(b"\n", 1)[0].removesuffix(b"\r")
        ok = response.status == _HTTP_OK and first == csv_header(request)
        return (COMPLETED if ok else FAILED), None
    if response.status != _HTTP_OK:
        message = _error_message(response.body)
        if response.status == _HTTP_BAD_REQUEST and message and "does not exist" in message:
            return NO_DATA, message
        return FAILED, message
    try:
        if request.endpoint == OBSERVATIONS:
            parse_page(request, response.body)
        else:
            parse_vintage_dates(request, response.body)
    except ValueError as error:
        return FAILED, f"unreadable answer: {error}"
    return COMPLETED, None


def _error_message(body: bytes) -> str | None:
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    message = value.get("error_message") if isinstance(value, dict) else None
    return message[:500] if isinstance(message, str) else None


def stops_run(response: Response) -> bool:
    """Whether no later call in this run can be answered: a refused key or the rate limit."""
    if response.status in STOP_HTTP:
        return True
    message = _error_message(response.body) if response.status == _HTTP_BAD_REQUEST else None
    return message is not None and "api_key" in message


# --- client -----------------------------------------------------------------------------------


class FredClient:
    """Calls FRED with the key it holds; the key reaches only the provider URL."""

    def __init__(self, key: str, transport: Transport, clock: Clock = utc_now) -> None:
        if not isinstance(key, str) or _KEY.fullmatch(key) is None:
            raise ValueError("a FRED API key is 32 lowercase letters and digits")
        self._key = key
        self._transport = transport
        self._clock = clock

    def request(self, request: Request) -> Response:
        check(request)
        query = dict(request.parameters)
        if request.endpoint in KEYED:
            query["api_key"] = self._key
        url = URLS[request.endpoint] + "?" + urllib.parse.urlencode(sorted(query.items()))
        started = self._clock()
        answer = self._transport("GET", url, None, {"Accept": "*/*"})
        finished = self._clock()
        key = self._key.encode()
        if key in answer.body or any(key in value.encode() for _, value in answer.headers):
            raise TransportError("provider answer echoed the credential; it is not retained")
        if len(answer.body) > MAX_RESPONSE_BYTES:
            raise TransportError("provider answer exceeds its bound")
        return Response(answer.status, answer.headers, answer.body, started, finished)


# --- plan -------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FredPolicy:
    """The series a run keeps current; its hash is the ledger jobs' ``policy_hash``."""

    alfred_series: tuple[str, ...] = DEFAULT_ALFRED_SERIES
    csv_series: Mapping[str, str] = field(default_factory=lambda: dict(DEFAULT_CSV_SERIES))

    def __post_init__(self) -> None:
        for name in (*self.alfred_series, *self.csv_series):
            series_id(name)
        if len(set(self.alfred_series)) != len(self.alfred_series):
            raise ValueError("ALFRED series repeat")
        if any(not isinstance(value, str) or not value for value in self.csv_series.values()):
            raise ValueError("every CSV series names its dataset")

    @property
    def sha256(self) -> str:
        document = {
            "schema": "aas-fred-policy-v1",
            "alfred_series": list(self.alfred_series),
            "csv_series": dict(sorted(self.csv_series.items())),
        }
        return hashlib.sha256(canonical(document).encode()).hexdigest()


@dataclass(slots=True)
class FredKnowledge:
    """What committed sources say: each series' latest collected vintage day and CSV day."""

    vintages: dict[str, date] = field(default_factory=dict)
    csv_days: dict[str, date] = field(default_factory=dict)

    def vintage(self, series: str, realtime_start: date) -> None:
        if realtime_start > self.vintages.get(series, ORIGIN - timedelta(days=1)):
            self.vintages[series] = realtime_start

    def csv(self, series: str, day: date) -> None:
        if day > self.csv_days.get(series, date.min):
            self.csv_days[series] = day


@dataclass(frozen=True, slots=True)
class Planned:
    request: Request
    reason: str


def last_ended_day(today: date) -> date:
    return today - timedelta(days=1)


def plan_alfred(knowledge: FredKnowledge, today: date, policy: FredPolicy) -> list[Planned]:
    """The first ``vintage_dates`` page of each ALFRED series that may have a new vintage."""
    end = last_ended_day(today)
    planned: list[Planned] = []
    for series in policy.alfred_series:
        known = knowledge.vintages.get(series)
        if known is None:
            planned.append(Planned(vintage_dates(series, ORIGIN, end), "origin"))
        elif known < end:
            planned.append(Planned(vintage_dates(series, known + timedelta(days=1), end),
                                   "vintage_check"))  # fmt: skip
    return planned


def next_vintage_page(request: Request, count: int) -> Request | None:
    asked = window(request)
    following = asked.offset + asked.limit
    if following >= count:
        return None
    return vintage_dates(asked.series_id, asked.start, asked.end, following)


def observation_windows(
    start: date, vintages: Sequence[date], end: date
) -> list[tuple[date, date]]:
    """Windows from ``start`` to ``end`` over ``vintages``, each spanning at most 1990.

    Each window after the first starts on the last vintage date of the one before it, so a
    period starting on a window's start was already read whole by the window before. The
    start counts as a vintage date unless it is the origin.
    """
    later = sorted({day for day in vintages if start < day <= end})
    windows: list[tuple[date, date]] = []
    first = start
    while later:
        take = MAX_WINDOW_VINTAGES - (first != ORIGIN)
        chunk, later = later[:take], later[take:]
        last = chunk[-1] if later else end
        windows.append((first, last))
        first = chunk[-1]
    return windows


def next_page(request: Request, page: Page) -> Request | None:
    asked = window(request)
    following = asked.offset + asked.limit
    if following >= page.count:
        return None
    return observations(asked.series_id, asked.start, asked.end, following)


def plan_csv(knowledge: FredKnowledge, today: date, policy: FredPolicy) -> list[Planned]:
    """Each CSV series once a FRED day."""
    return [
        Planned(series_csv(series), "daily")
        for series in sorted(policy.csv_series)
        if knowledge.csv_days.get(series, date.min) < today
    ]


def split_rows(
    request: Request, rows: Sequence[tuple[date, date, date, str]]
) -> tuple[list[tuple[date, date, date, str]], int]:
    """The rows that are vintages as FRED dated them, and the count restated at the start.

    A window from the origin restates nothing; otherwise a row starting on the window's
    start restates a period the window before it, or an earlier collection, holds.
    """
    start = window(request).start
    if start == ORIGIN:
        return list(rows), 0
    kept = [row for row in rows if row[1] > start]
    return kept, len(rows) - len(kept)
