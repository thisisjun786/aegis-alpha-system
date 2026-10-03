"""SEC EDGAR requests, answers and the daily plan of the native SEC collector.

Endpoints (``Request.provider`` is ``sec``):

- ``daily_index`` (``Archives/edgar/daily-index/<year>/QTR<n>/master.<YYYYMMDD>.idx``): the
  filings EDGAR disseminated on one business day, parameter ``day``. A day without an index
  (a weekend or EDGAR holiday) answers 404, which is ``NO_DATA``.
- ``submissions`` (``data.sec.gov/submissions/CIK##########.json``): a filer's submissions
  document, parameter ``cik`` (ten digits). Its ``filings.recent`` arrays carry each
  filing's acceptance instant.
- ``companyfacts`` (``data.sec.gov/api/xbrl/companyfacts/CIK##########.json``): every XBRL
  fact SEC holds for a company, parameter ``cik``. A company without facts answers 404.

The day's index names the filings; the collector then asks each filer's documents once a
run and keeps only the rows of the filings it wants from them (the receipt records that
selection), since the documents restate every earlier filing. Requests carry the
operator's ``User-Agent`` (SEC's fair-access contact), which is never retained.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Final, cast
from zoneinfo import ZoneInfo

from aegis_alpha.data.opendart import Clock, Transport, TransportError, canonical, utc_now
from aegis_alpha.data.provider_request import COMPLETED, FAILED, NO_DATA, Request, Response
from aegis_alpha.data.sec_transport import (
    UserAgentError,
    configured_user_agent_needles,
    validate_user_agent,
)

PROVIDER: Final = "sec"
DAILY_INDEX: Final = "daily_index"
SUBMISSIONS: Final = "submissions"
COMPANYFACTS: Final = "companyfacts"
ENDPOINTS: Final = (DAILY_INDEX, SUBMISSIONS, COMPANYFACTS)
DATASETS: Final[Mapping[str, str]] = {
    DAILY_INDEX: "filings.us.sec",
    SUBMISSIONS: "filings.us.sec",
    COMPANYFACTS: "fundamentals.us.sec",
}
EDGAR_ZONE: Final = ZoneInfo("America/New_York")
# SEC answers a request rate it refuses, or a missing contact, with 403; 429 is a limit.
STOP_HTTP: Final = frozenset({403, 429})
MAX_RESPONSE_BYTES: Final = 256 * 1024 * 1024
# Periodic, current and foreign issuer reports and their amendments: the filings whose
# acceptance instant the fundamentals and event readers need.
DEFAULT_FILING_FORMS: Final = frozenset(
    {
        "10-K", "10-K/A", "10-KT", "10-KT/A", "10-Q", "10-Q/A", "10-QT", "10-QT/A",
        "8-K", "8-K/A", "20-F", "20-F/A", "40-F", "40-F/A", "6-K", "6-K/A",
    }
)  # fmt: skip
# The reports whose XBRL financial statements reach companyfacts.
DEFAULT_FACT_FORMS: Final = frozenset(
    {
        "10-K", "10-K/A", "10-KT", "10-KT/A", "10-Q", "10-Q/A", "10-QT", "10-QT/A",
        "20-F", "20-F/A", "40-F", "40-F/A",
    }
)  # fmt: skip
_CIK: Final = re.compile(r"[0-9]{10}")
_DAY: Final = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_ACCESSION: Final = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_INDEX_HEADER: Final = ("cik", "company name", "form type", "date filed")
_INDEX_FILE_COLUMNS: Final = frozenset({"file name", "filename"})
_FILE_NAME: Final = re.compile(r"edgar/data/([0-9]{1,10})/([0-9]{10}-[0-9]{2}-[0-9]{6})\.txt")
_INDEX_DAY: Final = re.compile(r"([0-9]{4})-?([0-9]{2})-?([0-9]{2})")
_FACT_KEYS: Final = frozenset({"start", "end", "val", "accn", "fy", "fp", "form", "filed", "frame"})
_FACT_REQUIRED: Final = frozenset({"end", "val", "accn", "form", "filed"})
_DECIMAL: Final = re.compile(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?")
_HTTP_OK: Final = 200
_HTTP_NOT_FOUND: Final = 404
_WEEKEND: Final = 5
_QUARTER_MONTHS: Final = 3


def edgar_day(moment: datetime) -> date:
    """The EDGAR (New York) calendar date of an instant."""
    return moment.astimezone(EDGAR_ZONE).date()


def cik10(value: object) -> str:
    """A ten-digit CIK from SEC's decimal CIK (text or integer)."""
    if isinstance(value, bool):
        raise ValueError("a CIK is decimal digits")  # noqa: TRY004 -- malformed-content ValueError contract
    text = str(value) if isinstance(value, int) else value
    if not isinstance(text, str) or not text.isdigit() or not text.isascii() or len(text) > 10:  # noqa: PLR2004 -- ten digits
        raise ValueError("a CIK is at most ten decimal digits")
    return text.zfill(10)


# --- requests ---------------------------------------------------------------------------------


def daily_index(day: date) -> Request:
    return Request.of(PROVIDER, DAILY_INDEX, {"day": day.isoformat()})


def submissions(cik: str) -> Request:
    return Request.of(PROVIDER, SUBMISSIONS, {"cik": cik10(cik)})


def companyfacts(cik: str) -> Request:
    return Request.of(PROVIDER, COMPANYFACTS, {"cik": cik10(cik)})


def check(request: Request) -> None:
    """Refuse a request the collector does not make."""
    if request.provider != PROVIDER or request.endpoint not in ENDPOINTS:
        raise ValueError(f"unsupported SEC request {request.endpoint!r}")
    parameters = request.parameters
    if request.endpoint == DAILY_INDEX:
        if set(parameters) != {"day"} or _DAY.fullmatch(parameters["day"]) is None:
            raise ValueError("daily_index takes exactly day as YYYY-MM-DD")
        date.fromisoformat(parameters["day"])
    elif set(parameters) != {"cik"} or _CIK.fullmatch(parameters["cik"]) is None:
        raise ValueError(f"{request.endpoint} takes exactly cik as ten digits")


def url(request: Request) -> str:
    check(request)
    parameters = request.parameters
    if request.endpoint == DAILY_INDEX:
        day = date.fromisoformat(parameters["day"])
        quarter = (day.month - 1) // _QUARTER_MONTHS + 1
        return (
            f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/QTR{quarter}/"
            f"master.{day:%Y%m%d}.idx"
        )
    if request.endpoint == SUBMISSIONS:
        return f"https://data.sec.gov/submissions/CIK{parameters['cik']}.json"
    return f"https://data.sec.gov/api/xbrl/companyfacts/CIK{parameters['cik']}.json"


def request_day(request: Request) -> date:
    return date.fromisoformat(request.parameters["day"])


# --- answers ----------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class IndexLine:
    """One line of a daily index; the parsed fields are None when the line is unreadable."""

    line: str
    cik: str | None = None
    company: str | None = None
    form: str | None = None
    filed: date | None = None
    accession: str | None = None


def _text(body: bytes) -> str:
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError:
        return body.decode("latin-1")


def _index_line(line: str) -> IndexLine:
    fields = line.split("|")
    if len(fields) != 5:  # noqa: PLR2004 -- the index's five columns
        return IndexLine(line)
    cik, company, form, filed, name = fields
    stamp = _INDEX_DAY.fullmatch(filed)
    named = _FILE_NAME.fullmatch(name)
    if not cik.isdigit() or len(cik) > 10 or stamp is None or named is None or not form:  # noqa: PLR2004 -- ten digits
        return IndexLine(line)
    if int(named.group(1)) != int(cik):  # the file lies under another filer
        return IndexLine(line)
    try:
        day = date(int(stamp.group(1)), int(stamp.group(2)), int(stamp.group(3)))
    except ValueError:
        return IndexLine(line)
    return IndexLine(line, cik.zfill(10), company, form, day, named.group(2))


def parse_index(body: bytes) -> list[IndexLine]:
    """Every entry line of a daily ``master`` index, in the index's order.

    The entries follow the ``CIK|Company Name|Form Type|Date Filed|File Name`` header and
    its dashed rule; an index without them is refused. The header is matched by its column
    names, ignoring case and surrounding whitespace, and the last may be ``Filename`` as in
    EDGAR's full-index files. A line that does not read as five fields naming a CIK, a form,
    a filing date and an ``edgar/data`` accession file under that CIK stays as its text
    with no fields.
    """
    lines = _text(body).splitlines()
    header = next((index for index, line in enumerate(lines) if _is_index_header(line)), None)
    if header is None:
        raise ValueError("SEC daily index has no CIK|Company Name|... header")
    rule = lines[header + 1].strip() if header + 1 < len(lines) else ""
    if len(rule) < 10 or set(rule) != {"-"}:  # noqa: PLR2004 -- the dashed rule under the header
        raise ValueError("SEC daily index header is not followed by its dashed rule")
    return [_index_line(line) for line in lines[header + 2 :] if line]


def _is_index_header(line: str) -> bool:
    names = [name.strip().casefold() for name in line.split("|")]
    return tuple(names[:-1]) == _INDEX_HEADER and names[-1] in _INDEX_FILE_COLUMNS


class _Number(str):
    """A JSON number's exact text."""

    __slots__ = ()


def _json(body: bytes, *, exact_numbers: bool = False) -> dict[str, object]:
    hooks: dict[str, Callable[[str], object]] = (
        {"parse_int": _Number, "parse_float": _Number} if exact_numbers else {}
    )

    def refuse(name: str) -> object:
        raise ValueError(f"SEC JSON holds {name}")

    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        document = dict(pairs)
        if len(document) != len(pairs):
            raise ValueError("SEC JSON repeats a key")
        return document

    try:
        value = json.loads(
            body.decode("utf-8"),
            parse_constant=refuse,
            object_pairs_hook=unique,
            **hooks,  # ty: ignore[invalid-argument-type]
        )
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("SEC answer is not UTF-8 JSON") from None
    if not isinstance(value, dict):
        raise ValueError("SEC answer is not a JSON object")  # noqa: TRY004 -- malformed-content ValueError contract
    return cast("dict[str, object]", value)


def submissions_document(body: bytes, cik: str) -> dict[str, object]:
    """A submissions answer as a filer document of ``cik``; another filer's is refused."""
    document = _json(body)
    if cik10(cast("str", document.get("cik"))) != cik:
        raise ValueError("SEC submissions answer names another CIK")
    return document


@dataclass(frozen=True, slots=True)
class Fact:
    """One companyfacts fact as text: SEC's dates, the number's exact text and labels."""

    taxonomy: str
    tag: str
    unit: str
    start: date | None
    end: date
    accession: str
    form: str
    filed: date
    value: str
    fy: str | None
    fp: str | None
    frame: str | None


def _fact_day(value: object, name: str) -> date:
    if not isinstance(value, str) or _DAY.fullmatch(value) is None:
        raise ValueError(f"SEC fact {name} must be YYYY-MM-DD")
    return date.fromisoformat(value)


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"SEC fact {name} must be text")  # noqa: TRY004 -- malformed-content ValueError contract
    return str(value)


def _fact(taxonomy: str, tag: str, unit: str, item: object) -> Fact:
    if not isinstance(item, dict) or not _FACT_REQUIRED <= set(item) <= _FACT_KEYS:
        raise ValueError(f"SEC fact of {taxonomy}:{tag} has unknown or missing fields")
    value, accession = item["val"], item["accn"]
    if not isinstance(value, _Number) or _DECIMAL.fullmatch(value) is None:
        raise ValueError(f"SEC fact of {taxonomy}:{tag} has no decimal value")
    if not isinstance(accession, str) or _ACCESSION.fullmatch(accession) is None:
        raise ValueError(f"SEC fact of {taxonomy}:{tag} has no accession number")
    form = item["form"]
    if not isinstance(form, str) or isinstance(form, _Number):
        raise ValueError(f"SEC fact of {taxonomy}:{tag} has no form")  # noqa: TRY004 -- malformed-content ValueError contract
    fy = item.get("fy")
    if fy is not None and not isinstance(fy, _Number):
        raise ValueError(f"SEC fact of {taxonomy}:{tag} has a fiscal year that is no number")
    return Fact(
        taxonomy,
        tag,
        unit,
        None if item.get("start") is None else _fact_day(item["start"], "start"),
        _fact_day(item["end"], "end"),
        accession,
        str(form),
        _fact_day(item["filed"], "filed"),
        str(value),
        None if fy is None else str(fy),
        _optional_text(item.get("fp"), "fp"),
        _optional_text(item.get("frame"), "frame"),
    )


def companyfacts_facts(body: bytes, cik: str) -> Iterator[Fact]:
    """Every fact of a companyfacts answer of ``cik``, in SEC's order.

    Numbers keep their JSON text. A fact without an end, value, accession, form or filing
    date, an unknown field or a document of another CIK refuses the answer.
    """
    document = _json(body, exact_numbers=True)
    if not set(document) <= {"cik", "entityName", "facts"}:
        raise ValueError("SEC companyfacts answer has unknown fields")
    if cik10(document.get("cik")) != cik:
        raise ValueError("SEC companyfacts answer names another CIK")
    facts = document.get("facts", {})
    if not isinstance(facts, dict):
        raise ValueError("SEC companyfacts facts must be an object")  # noqa: TRY004 -- malformed-content ValueError contract
    for taxonomy, tags in facts.items():
        if not isinstance(tags, dict):
            raise ValueError("SEC companyfacts taxonomies map tags")  # noqa: TRY004 -- malformed-content ValueError contract
        for tag, concept in tags.items():
            yield from _concept_facts(str(taxonomy), str(tag), concept)


def _concept_facts(taxonomy: str, tag: str, concept: object) -> Iterator[Fact]:
    units = concept.get("units") if isinstance(concept, dict) else None
    if not isinstance(units, dict):
        raise ValueError(f"SEC companyfacts {taxonomy}:{tag} has no units")  # noqa: TRY004 -- malformed-content ValueError contract
    for unit, items in units.items():
        if not isinstance(items, list):
            raise ValueError(f"SEC companyfacts {taxonomy}:{tag} {unit} is no list")  # noqa: TRY004 -- malformed-content ValueError contract
        for item in items:
            yield _fact(taxonomy, tag, str(unit), item)


def classify(request: Request, response: Response) -> tuple[str, str | None]:
    """The outcome and a reason: the outcome only routes the collector.

    A readable answer is ``COMPLETED``; 404 (no index that day, no document for the CIK)
    is ``NO_DATA``; anything else is ``FAILED`` with the reason.
    """
    if response.status == _HTTP_NOT_FOUND:
        return NO_DATA, None
    if response.status != _HTTP_OK:
        return FAILED, f"http {response.status}"
    try:
        if request.endpoint == DAILY_INDEX:
            parse_index(response.body)
        elif request.endpoint == SUBMISSIONS:
            submissions_document(response.body, request.parameters["cik"])
        else:
            for _ in companyfacts_facts(response.body, request.parameters["cik"]):
                pass
    except ValueError as error:
        return FAILED, f"unreadable answer: {error}"
    return COMPLETED, None


def stops_run(response: Response) -> bool:
    """Whether no later call in this run can be answered: SEC refused the rate or contact."""
    return response.status in STOP_HTTP


# --- client -----------------------------------------------------------------------------------


class SecClient:
    """Calls SEC with the operator's contact ``User-Agent``, which reaches no receipt."""

    def __init__(self, user_agent: str, transport: Transport, clock: Clock = utc_now) -> None:
        try:
            self._user_agent = validate_user_agent(user_agent)
        except UserAgentError:
            raise ValueError("the SEC User-Agent must name a contact address") from None
        self._transport = transport
        self._clock = clock

    def request(self, request: Request) -> Response:
        started = self._clock()
        answer = self._transport(
            "GET", url(request), None, {"Accept": "*/*", "User-Agent": self._user_agent}
        )
        finished = self._clock()
        # The whole User-Agent and the contact address in it.
        for contact in configured_user_agent_needles(self._user_agent):
            if contact in answer.body or any(contact in v.encode() for _, v in answer.headers):
                raise TransportError("provider answer echoed the contact; it is not retained")
        if len(answer.body) > MAX_RESPONSE_BYTES:
            raise TransportError("provider answer exceeds its bound")
        return Response(answer.status, answer.headers, answer.body, started, finished)


# --- plan -------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SecPolicy:
    """What a run collects and how long it waits; its hash is the jobs' ``policy_hash``.

    ``issuers`` is ``registered`` (only filers whose ``mint_issuer('sec_cik', cik)`` the
    installation's identity holds) or ``all``. ``lookback_days`` is how far before the
    collection day the first run starts reading indexes. A wanted filing a filer's
    document does not list yet is asked again a day later, for ``listing_days`` after its
    index was read; its facts, which reach companyfacts after XBRL processing, for
    ``fact_days``.
    """

    filing_forms: frozenset[str] = DEFAULT_FILING_FORMS
    fact_forms: frozenset[str] = DEFAULT_FACT_FORMS
    issuers: str = "registered"
    lookback_days: int = 30
    listing_days: int = 7
    fact_days: int = 14

    def __post_init__(self) -> None:
        if self.issuers not in {"registered", "all"}:
            raise ValueError("SEC issuers are registered or all")
        if not self.fact_forms <= self.filing_forms:
            raise ValueError("fact forms are filing forms")
        for value in (self.lookback_days, self.listing_days, self.fact_days):
            if type(value) is not int or value < 1:
                raise ValueError("SEC policy windows are positive day counts")

    @property
    def sha256(self) -> str:
        document = {
            "schema": "aas-sec-policy-v1",
            "filing_forms": sorted(self.filing_forms),
            "fact_forms": sorted(self.fact_forms),
            "issuers": self.issuers,
            "lookback_days": self.lookback_days,
            "listing_days": self.listing_days,
            "fact_days": self.fact_days,
        }
        return hashlib.sha256(canonical(document).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Filing:
    """A filing a read index names: its filer, form, filing date and when it was read."""

    cik: str
    accession: str
    form: str
    filed: date
    indexed_at: datetime


@dataclass(slots=True)
class SecKnowledge:
    """What committed SEC answers say."""

    # Index day -> (outcome, retrieval instant) of its newest answer.
    days: dict[date, tuple[str, datetime]] = field(default_factory=dict)
    filings: dict[str, Filing] = field(default_factory=dict)
    listed: set[str] = field(default_factory=set)
    reported: set[str] = field(default_factory=set)
    # (endpoint, cik) -> newest ask (answered or not).
    asked: dict[tuple[str, str], datetime] = field(default_factory=dict)

    def index(self, day: date, outcome: str, at: datetime) -> None:
        if day not in self.days or at > self.days[day][1]:
            self.days[day] = (outcome, at)

    def file(self, line: IndexLine, at: datetime) -> None:
        if line.cik is None or line.accession is None or line.form is None or line.filed is None:
            return
        key = f"{line.cik}/{line.accession}"
        known = self.filings.get(key)
        if known is None or at < known.indexed_at:
            self.filings[key] = Filing(line.cik, line.accession, line.form, line.filed, at)

    def ask(self, endpoint: str, cik: str, at: datetime) -> None:
        key = (endpoint, cik)
        if key not in self.asked or at > self.asked[key]:
            self.asked[key] = at


@dataclass(frozen=True, slots=True)
class Planned:
    request: Request
    reason: str
    wanted: tuple[str, ...] = ()


def weekdays(first: date, last: date) -> Iterator[date]:
    day = first
    while day <= last:
        if day.weekday() < _WEEKEND:
            yield day
        day += timedelta(days=1)


def covered(knowledge: SecKnowledge, day: date) -> bool:
    """An index day is covered by an answered index, or a 404 read after the next day ended."""
    answer = knowledge.days.get(day)
    if answer is None:
        return False
    outcome, at = answer
    return outcome == COMPLETED or (outcome == NO_DATA and edgar_day(at) >= day + timedelta(days=2))


def first_day(knowledge: SecKnowledge, today: date, policy: SecPolicy) -> date:
    if knowledge.days:
        return min(knowledge.days)
    return today - timedelta(days=policy.lookback_days)


def plan_indexes(
    knowledge: SecKnowledge, today: date, policy: SecPolicy, since: date | None = None
) -> list[Planned]:
    """Every uncovered weekday from the first known (or ``since``) day to yesterday."""
    start = since if since is not None else first_day(knowledge, today, policy)
    return [
        Planned(daily_index(day), "index_gap")
        for day in weekdays(start, today - timedelta(days=1))
        if not covered(knowledge, day)
    ]


def _due(
    knowledge: SecKnowledge,
    endpoint: str,
    filing: Filing,
    window_days: int,
    now: datetime,
) -> str | None:
    asked = knowledge.asked.get((endpoint, filing.cik))
    if asked is None or asked < filing.indexed_at:
        return "new_filing"
    if now - asked >= timedelta(days=1) and now - filing.indexed_at <= timedelta(days=window_days):
        return "listing_retry" if endpoint == SUBMISSIONS else "fact_retry"
    return None


def _plan_documents(  # noqa: PLR0913 -- one planner for both document endpoints
    knowledge: SecKnowledge,
    *,
    endpoint: str,
    forms: frozenset[str],
    done: set[str],
    window_days: int,
    now: datetime,
    in_universe: Callable[[str], bool],
) -> tuple[list[Planned], dict[str, int]]:
    by_cik: dict[str, list[Filing]] = {}
    counts = {"wanted": 0, "done": 0, "outside_universe": 0, "abandoned": 0}
    for filing in knowledge.filings.values():
        if filing.form not in forms:
            continue
        if not in_universe(filing.cik):
            counts["outside_universe"] += 1
            continue
        counts["wanted"] += 1
        if filing.accession in done:
            counts["done"] += 1
            continue
        by_cik.setdefault(filing.cik, []).append(filing)
    planned: list[Planned] = []
    for cik, filings in sorted(by_cik.items()):
        reasons = {_due(knowledge, endpoint, filing, window_days, now) for filing in filings}
        reasons.discard(None)
        if not reasons:
            counts["abandoned"] += sum(
                now - filing.indexed_at > timedelta(days=window_days) for filing in filings
            )
            continue
        request = submissions(cik) if endpoint == SUBMISSIONS else companyfacts(cik)
        reason = "new_filing" if "new_filing" in reasons else min(cast("set[str]", reasons))
        planned.append(Planned(request, reason, tuple(sorted(f.accession for f in filings))))
    return planned, counts


def plan_documents(
    knowledge: SecKnowledge,
    now: datetime,
    policy: SecPolicy,
    in_universe: Callable[[str], bool],
) -> tuple[list[Planned], list[Planned], dict[str, dict[str, int]]]:
    """Submissions and companyfacts requests, one per filer, with the filings each wants."""
    filings, filing_counts = _plan_documents(
        knowledge, endpoint=SUBMISSIONS, forms=policy.filing_forms, done=knowledge.listed,
        window_days=policy.listing_days, now=now, in_universe=in_universe,
    )  # fmt: skip
    facts, fact_counts = _plan_documents(
        knowledge, endpoint=COMPANYFACTS, forms=policy.fact_forms, done=knowledge.reported,
        window_days=policy.fact_days, now=now, in_universe=in_universe,
    )  # fmt: skip
    return filings, facts, {SUBMISSIONS: filing_counts, COMPANYFACTS: fact_counts}


def summarize(planned: Sequence[Planned]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in planned:
        key = f"{item.request.endpoint}:{item.reason}"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))
