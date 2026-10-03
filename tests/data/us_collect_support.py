"""A synthetic FRED/ALFRED and SEC EDGAR for collector tests: answers, a call log and history.

Every series, value, CIK, company and accession here is made up. The fakes answer in the
providers' shapes, including ALFRED's clipping of a real-time period to the asked window
and a limit on the vintage dates of one observations window. FRED's own limit is 2000; the
fake refuses a window over the collector's margin of 1990 (its start included), so a
collector that relied on unclipped periods, one unbounded window or no margin fails.
"""

from __future__ import annotations

import json
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Final

from aegis_alpha.data.opendart import HttpAnswer, TransportError

FRED_KEY: Final = "0123456789abcdef0123456789abcdef"  # 32 letters and digits, not a key
USER_AGENT: Final = "Synthetic Collector ops@example.invalid"
JSON: Final = (("content-type", "application/json; charset=UTF-8"),)
TEXT: Final = (("content-type", "text/plain"),)
OPEN: Final = date(9999, 12, 31)
ORIGIN: Final = date(1776, 7, 4)


@dataclass(slots=True)
class Period:
    """One ALFRED real-time period of one observation, as FRED dates it (unclipped)."""

    observation: date
    value: str
    start: date
    end: date = OPEN


def _fred_error(code: int, message: str) -> HttpAnswer:
    return HttpAnswer(code, JSON, json.dumps({"error_code": code, "error_message": message})
                      .encode())  # fmt: skip


@dataclass(slots=True)
class FakeFred:
    """ALFRED histories per series; ``today`` is FRED's (Chicago) date."""

    today: date
    history: dict[str, list[Period]] = field(default_factory=dict)
    fail: set[str] = field(default_factory=set)
    # Observations windows (by realtime_start) whose calls fail in transport.
    fail_starts: set[str] = field(default_factory=set)
    answers: dict[str, HttpAnswer] = field(default_factory=dict)
    calls: list[tuple[str, dict[str, str]]] = field(default_factory=list)
    crash_after: int | None = None

    def publish(self, series: str, observation: date, value: str, on: date) -> None:
        """A first release, or a revision closing the observation's current period."""
        periods = self.history.setdefault(series, [])
        for period in periods:
            if period.observation == observation and period.end == OPEN:
                period.end = on - timedelta(days=1)
        periods.append(Period(observation, value, on))

    def vintages(self, series: str) -> list[date]:
        return sorted({period.start for period in self.history.get(series, [])})

    def __call__(  # noqa: PLR0911 -- one answer per route
        self, method: str, url: str, body: bytes | None, headers: Mapping[str, str]
    ) -> HttpAnswer:
        del body, headers
        assert method == "GET"
        path, _, query = url.partition("?")
        parameters = dict(urllib.parse.parse_qsl(query))
        endpoint = path.rsplit("/", 1)[-1]
        if self.crash_after is not None and len(self.calls) >= self.crash_after:
            raise KeyboardInterrupt  # the process dies mid-run
        self.calls.append((endpoint, parameters))
        if endpoint in self.fail or (
            endpoint == "observations" and parameters.get("realtime_start") in self.fail_starts
        ):
            raise TransportError("synthetic transport failure")
        if endpoint in self.answers:
            return self.answers[endpoint]
        if endpoint == "fredgraph.csv":
            return self._csv(parameters["id"])
        assert parameters.pop("api_key") == FRED_KEY
        series = parameters["series_id"]
        if series not in self.history:
            return _fred_error(400, "Bad Request.  The series does not exist.")
        start = date.fromisoformat(parameters["realtime_start"])
        end = date.fromisoformat(parameters["realtime_end"])
        if end > self.today and end != OPEN:
            return _fred_error(400, "Bad Request.  Variable realtime_end can not be after today.")
        limit, offset = int(parameters["limit"]), int(parameters["offset"])
        if endpoint == "vintagedates":
            days = [day.isoformat() for day in self.vintages(series) if start <= day <= end]
            return self._page(parameters, "vintage_dates", days, limit, offset)
        inside = [day for day in self.vintages(series) if start <= day <= end]
        if len(inside) > 1990:  # noqa: PLR2004 -- the collector's margin below FRED's 2000
            return _fred_error(400, f"Bad Request.  There are {len(inside)} vintage dates.")
        rows = sorted(
            (
                {
                    "realtime_start": max(period.start, start).isoformat(),
                    "realtime_end": min(period.end, end).isoformat(),
                    "date": period.observation.isoformat(),
                    "value": period.value,
                }
                for period in self.history[series]
                if period.start <= end and period.end >= start
            ),
            key=lambda row: (row["date"], row["realtime_start"]),
        )
        return self._page(parameters, "observations", rows, limit, offset)

    @staticmethod
    def _page(
        parameters: Mapping[str, str], key: str, items: Sequence[object], limit: int, offset: int
    ) -> HttpAnswer:
        document = {
            "realtime_start": parameters["realtime_start"],
            "realtime_end": parameters["realtime_end"],
            "count": len(items),
            "offset": offset,
            "limit": limit,
            key: list(items[offset : offset + limit]),
        }
        return HttpAnswer(200, JSON, json.dumps(document).encode())

    def _csv(self, series: str) -> HttpAnswer:
        current = sorted(
            (period.observation, period.value)
            for period in self.history.get(series, [])
            if period.end == OPEN and period.start <= self.today
        )
        lines = [f"observation_date,{series}"]
        lines.extend(f"{day.isoformat()},{'' if value == '.' else value}" for day, value in current)
        return HttpAnswer(200, (("content-type", "text/csv"),), ("\n".join(lines) + "\n").encode())

    def asked(self, endpoint: str) -> list[dict[str, str]]:
        return [parameters for name, parameters in self.calls if name == endpoint]


# --- SEC --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SecFiling:
    cik: str
    accession: str
    form: str
    filed: date
    accepted: str
    report: str = ""


@dataclass(frozen=True, slots=True)
class SecFact:
    accession: str
    tag: str
    value: str  # the JSON number's text
    end: date
    filed: date
    form: str = "10-Q"
    start: date | None = None


def index_bytes(day: date, filings: list[SecFiling], extra: tuple[str, ...] = ()) -> bytes:
    """A daily ``master`` index laid out line for line as EDGAR's, with synthetic entries."""
    lines = [
        "Description:           Daily Index of EDGAR Dissemination Feed",
        f"Last Data Received:    {day:%B} {day.day}, {day.year}",
        "Comments:              webmaster@sec.gov",
        "Anonymous FTP:         ftp://ftp.sec.gov/edgar/",
        " ",
        "CIK|Company Name|Form Type|Date Filed|File Name",
        "-" * 80,
    ]
    lines.extend(
        f"{int(f.cik)}|SYNTHETIC CO {int(f.cik)}|{f.form}|{f.filed:%Y%m%d}|"
        f"edgar/data/{int(f.cik)}/{f.accession}.txt"
        for f in filings
    )
    lines.extend(extra)
    return ("\n".join(lines) + "\n").encode()


def submissions_bytes(cik: str, filings: list[SecFiling]) -> bytes:
    recent: dict[str, list[object]] = {
        "accessionNumber": [], "filingDate": [], "reportDate": [], "acceptanceDateTime": [],
        "act": [], "form": [], "fileNumber": [], "filmNumber": [], "items": [], "core_type": [],
        "size": [], "isXBRL": [], "isInlineXBRL": [], "primaryDocument": [],
        "primaryDocDescription": [],
    }  # fmt: skip
    for filing in sorted(filings, key=lambda item: item.filed, reverse=True):
        for name, value in (
            ("accessionNumber", filing.accession), ("filingDate", filing.filed.isoformat()),
            ("reportDate", filing.report), ("acceptanceDateTime", filing.accepted),
            ("act", "34"), ("form", filing.form), ("fileNumber", "001-00001"),
            ("filmNumber", "26000001"), ("items", ""), ("core_type", filing.form),
            ("size", 1234), ("isXBRL", 1), ("isInlineXBRL", 1), ("primaryDocument", "d.htm"),
            ("primaryDocDescription", filing.form),
        ):  # fmt: skip
            recent[name].append(value)
    document = {
        "cik": str(int(cik)),
        "entityType": "operating",
        "name": f"SYNTHETIC CO {int(cik)}",
        "filings": {"recent": recent, "files": []},
    }
    return json.dumps(document).encode()


def companyfacts_bytes(cik: str, facts: list[SecFact]) -> bytes:
    concepts: dict[str, dict[str, object]] = {}
    for fact in facts:
        concept = concepts.setdefault(
            fact.tag, {"label": fact.tag, "description": None, "units": {"USD": []}}
        )
        item = {
            "end": fact.end.isoformat(),
            "val": "@@" + fact.value + "@@",
            "accn": fact.accession,
            "fy": fact.end.year,
            "fp": "Q2",
            "form": fact.form,
            "filed": fact.filed.isoformat(),
        }
        if fact.start is not None:
            item = {"start": fact.start.isoformat(), **item}
        concept["units"]["USD"].append(item)  # ty: ignore[not-subscriptable]
    document = {"cik": int(cik), "entityName": f"SYNTHETIC CO {int(cik)}",
                "facts": {"us-gaap": concepts}}  # fmt: skip
    # The value placeholders keep each number's exact text, as SEC writes it.
    return json.dumps(document).replace('"@@', "").replace('@@"', "").encode()


@dataclass(slots=True)
class FakeSec:
    """EDGAR indexes per day (a missing day answers 404) and each CIK's documents."""

    filings: list[SecFiling] = field(default_factory=list)
    facts: dict[str, list[SecFact]] = field(default_factory=dict)
    holidays: set[date] = field(default_factory=set)
    # Accessions EDGAR has not yet added to their filer's submissions document.
    unlisted: set[str] = field(default_factory=set)
    refuse: int | None = None
    # Endpoints whose calls fail in transport after they are logged.
    fail: set[str] = field(default_factory=set)
    calls: list[tuple[str, str]] = field(default_factory=list)

    def __call__(  # noqa: PLR0911 -- one answer per route
        self, method: str, url: str, body: bytes | None, headers: Mapping[str, str]
    ) -> HttpAnswer:
        del body
        assert method == "GET"
        assert headers["User-Agent"] == USER_AGENT
        if self.refuse is not None:
            self.calls.append(("refused", url))
            return HttpAnswer(self.refuse, TEXT, b"Request Rate Threshold Exceeded")
        if url.startswith("https://www.sec.gov/Archives/edgar/daily-index/"):
            stamp = url.rsplit(".", 2)[-2]
            day = date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:]))
            self.calls.append(("daily_index", day.isoformat()))
            if "daily_index" in self.fail:
                raise TransportError("synthetic transport failure")
            listed = [f for f in self.filings if f.filed == day]
            if day in self.holidays or day.weekday() >= 5:  # noqa: PLR2004 -- weekend
                return HttpAnswer(404, TEXT, b"Not Found")
            return HttpAnswer(200, TEXT, index_bytes(day, listed))
        cik = url.rsplit("CIK", 1)[-1].removesuffix(".json")
        if url.startswith("https://data.sec.gov/submissions/"):
            self.calls.append(("submissions", cik))
            if "submissions" in self.fail:
                raise TransportError("synthetic transport failure")
            mine = [f for f in self.filings if f.cik == cik and f.accession not in self.unlisted]
            if not mine:
                return HttpAnswer(404, TEXT, b"Not Found")
            return HttpAnswer(200, JSON, submissions_bytes(cik, mine))
        assert url.startswith("https://data.sec.gov/api/xbrl/companyfacts/")
        self.calls.append(("companyfacts", cik))
        if "companyfacts" in self.fail:
            raise TransportError("synthetic transport failure")
        if cik not in self.facts:
            return HttpAnswer(404, TEXT, b"Not Found")
        return HttpAnswer(200, JSON, companyfacts_bytes(cik, self.facts[cik]))

    def asked(self, endpoint: str) -> list[str]:
        return [key for name, key in self.calls if name == endpoint]
