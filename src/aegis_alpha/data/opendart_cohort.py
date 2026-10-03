"""The rolling OpenDART cohort: what to ask today from what is already known.

There is no fixed job list. Every plan is derived from four inputs and the Seoul date:

- the companies with a stock code in the newest completed corp code list (narrowed to
  KIND's KOSPI and KOSDAQ lists when those are known),
- every retained receipt (request, outcome, retrieval instant), whoever collected it,
- the periodic-report filings the retained ``list.json`` pages name,
- failed or uncertain attempts the collection ledger recorded without a receipt.

A request is the endpoint and parameters alone (``opendart.DartRequest``), so a
re-asked question is a new attempt of the same request and a ``NO_DATA`` answer is never
terminal. For each listed company, business year from ``FIRST_YEAR`` and report whose
period has ended (December fiscal year), the consolidated (``CFS``) request is planned
when it was never asked, when a filing for that report was filed on or after the day of
its latest retrieval (a late filing or an amendment), when its latest answer was
``NO_DATA`` and the retry interval has passed (``season_retry_days`` until the filing
deadline plus ``season_grace_days``; after it ``no_data_retry_days``, for business years
from ``no_data_retry_years`` before the current one, since an older report filed late
shows as a filing first), or when its latest attempt failed ``failed_retry_days`` ago.
The separate (``OFS``) request follows the same rules while the consolidated request's
latest answer is ``NO_DATA``.

The disclosure list is read day by day: a Seoul date before today is covered when its
first page and every page that page counts were answered after the day ended. The
corp code list is refreshed every ``corp_codes_refresh_days``.
"""

from __future__ import annotations

import calendar
import hashlib
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Final
from zoneinfo import ZoneInfo

from aegis_alpha.data.opendart import (
    COMPLETED,
    CORP_CODES,
    FAILED,
    FINANCIALS,
    FIRST_YEAR,
    LIST,
    NO_DATA,
    REPORT_END_MONTH,
    DartRequest,
    Filing,
    canonical,
)

POLICY_FORMAT: Final = "aas-opendart-cohort-v1"
SEOUL: Final = ZoneInfo("Asia/Seoul")
# Statutory filing deadlines after the period end: 45 days for quarterly and half-year
# reports, 90 days for the annual report.
DEADLINE_DAYS: Final[Mapping[str, int]] = {"11013": 45, "11012": 45, "11014": 45, "11011": 90}
# ``corp_cls`` of a filing by a KOSPI (Y) or KOSDAQ (K) company.
LISTED_MARKETS: Final = frozenset({"Y", "K"})
# Reasons in the order a run spends its calls on them.
REASONS: Final = (
    "corp_codes_refresh",
    "list_page",
    "new_filing",
    "never_asked",
    "season_retry",
    "failed_retry",
    "no_data_retry",
)


@dataclass(frozen=True, slots=True)
class CohortPolicy:
    """The retry intervals of the rolling cohort; its hash is every job's ``policy_hash``."""

    first_year: int = FIRST_YEAR
    season_grace_days: int = 30
    season_retry_days: int = 7
    no_data_retry_days: int = 90
    no_data_retry_years: int = 1
    failed_retry_days: int = 1
    corp_codes_refresh_days: int = 7
    list_lookback_days: int = 90

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            least = {"first_year": FIRST_YEAR, "no_data_retry_years": 0}.get(name, 1)
            if type(value) is not int or value < least:
                raise ValueError(f"cohort policy {name} must be a positive integer")

    @property
    def sha256(self) -> str:
        return hashlib.sha256(canonical([POLICY_FORMAT, asdict(self)]).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Observation:
    """One answered or failed ask of a request: its outcome and when it was retrieved."""

    request: DartRequest
    outcome: str
    retrieved_at: datetime
    total_pages: int | None = None


@dataclass(frozen=True, slots=True)
class Planned:
    request: DartRequest
    reason: str


def seoul_day(moment: datetime) -> date:
    return moment.astimezone(SEOUL).date()


def period_end(year: int, report: str) -> date:
    month = REPORT_END_MONTH[report]
    return date(year, month, calendar.monthrange(year, month)[1])


def period_window(year: int, report: str) -> tuple[date, date]:
    """First and last day of the period a report's year-to-date statements measure."""
    return date(year, 1, 1), period_end(year, report)


def _end_of(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=SEOUL) + timedelta(days=1)


@dataclass(slots=True)
class Knowledge:
    """What the retained receipts, list filings and ledger say has been asked and answered."""

    latest: dict[str, Observation] = field(default_factory=dict)
    filings: dict[tuple[str, int, str], date] = field(default_factory=dict)
    filing_corps: set[str] = field(default_factory=set)
    stock_codes: dict[str, str] = field(default_factory=dict)
    kind_codes: frozenset[str] | None = None
    corp_codes_at: datetime | None = None
    list_pages: dict[date, dict[int, Observation]] = field(default_factory=dict)
    unanswered: dict[str, datetime] = field(default_factory=dict)

    def seen(self, request: DartRequest) -> Observation | None:
        """The newest ask of a request; an attempt with no retained answer counts as failed."""
        known = self.latest.get(request.fingerprint)
        at = self.unanswered.get(request.fingerprint)
        if at is not None and (known is None or at > known.retrieved_at):
            return Observation(request, FAILED, at)
        return known

    def observe(self, observation: Observation) -> None:
        """Keep the newest observation of each request (the later of equal instants wins)."""
        key = observation.request.fingerprint
        known = self.latest.get(key)
        if known is None or observation.retrieved_at >= known.retrieved_at:
            self.latest[key] = observation
        request = observation.request
        if (
            request.endpoint == CORP_CODES
            and observation.outcome == COMPLETED
            and (self.corp_codes_at is None or observation.retrieved_at > self.corp_codes_at)
        ):
            self.corp_codes_at = observation.retrieved_at
        if request.endpoint == LIST:
            parameters = request.parameters
            if parameters["bgn_de"] == parameters["end_de"]:
                stamp = parameters["bgn_de"]
                day = date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:]))
                pages = self.list_pages.setdefault(day, {})
                page = int(parameters["page_no"])
                if page not in pages or observation.retrieved_at >= pages[page].retrieved_at:
                    pages[page] = observation

    def file(self, filings: Iterable[Filing]) -> None:
        for filing in filings:
            key = (filing.corp_code, filing.bsns_year, filing.reprt_code)
            known = self.filings.get(key)
            if known is None or filing.filed_on > known:
                self.filings[key] = filing.filed_on
            if filing.market in LISTED_MARKETS:
                self.filing_corps.add(filing.corp_code)

    def listed(self, stock_codes: Mapping[str, str], retrieved_at: datetime) -> None:
        """Adopt the corp -> stock code map of the newest completed corp code answer."""
        if self.corp_codes_at is None or retrieved_at >= self.corp_codes_at:
            self.stock_codes = dict(stock_codes)
            self.corp_codes_at = retrieved_at

    @property
    def corps(self) -> tuple[str, ...]:
        """Corps with a stock code, narrowed to KIND's KOSPI and KOSDAQ lists when known,
        and every corp a KOSPI or KOSDAQ periodic filing names."""
        codes = self.kind_codes
        listed = {
            corp for corp, stock in self.stock_codes.items() if codes is None or stock in codes
        }
        return tuple(sorted(listed | self.filing_corps))


def plan_corp_codes(knowledge: Knowledge, today: date, policy: CohortPolicy) -> list[Planned]:
    known = knowledge.corp_codes_at
    if known is not None and (today - seoul_day(known)).days < policy.corp_codes_refresh_days:
        return []
    return [Planned(DartRequest.of(CORP_CODES), "corp_codes_refresh")]


def _first_list_day(knowledge: Knowledge, today: date, policy: CohortPolicy) -> date:
    return min([today - timedelta(days=policy.list_lookback_days), *knowledge.list_pages])


def list_gaps(knowledge: Knowledge, today: date, policy: CohortPolicy) -> list[Planned]:
    """Pages of the days before today whose list is not fully answered after the day ended.

    A page whose last ask failed or got no answer waits ``failed_retry_days`` like a
    statement.
    """
    planned: list[Planned] = []
    day = _first_list_day(knowledge, today, policy)
    while day < today:
        for page in missing_pages(knowledge, day):
            request = DartRequest.list_page(day, page)
            seen = knowledge.seen(request)
            if (
                seen is None
                or seen.outcome != FAILED
                or ((today - seoul_day(seen.retrieved_at)).days >= policy.failed_retry_days)
            ):
                planned.append(Planned(request, "list_page"))
        day += timedelta(days=1)
    return planned


def missing_pages(knowledge: Knowledge, day: date) -> list[int]:
    ended = _end_of(day)
    pages = {
        number: seen
        for number, seen in knowledge.list_pages.get(day, {}).items()
        if seen.retrieved_at >= ended and seen.outcome in {COMPLETED, NO_DATA}
    }
    first = pages.get(1)
    if first is None:
        return [1]
    if first.outcome == NO_DATA:
        return []
    return [page for page in range(2, (first.total_pages or 1) + 1) if page not in pages]


@dataclass(frozen=True, slots=True)
class _Period:
    """When a report's requests are asked again: in season, recent, and today."""

    in_season: bool
    recent: bool
    today: date
    policy: CohortPolicy

    def reason(self, observation: Observation | None, filed: date | None) -> str | None:
        if observation is None:
            return "never_asked"
        asked = seoul_day(observation.retrieved_at)
        if filed is not None and filed >= asked and asked < self.today:
            return "new_filing"
        waited = (self.today - asked).days
        policy = self.policy
        if observation.outcome == COMPLETED:
            reason = None
        elif observation.outcome != NO_DATA:
            reason = "failed_retry" if waited >= policy.failed_retry_days else None
        elif self.in_season:
            reason = "season_retry" if waited >= policy.season_retry_days else None
        else:
            due = self.recent and waited >= policy.no_data_retry_days
            reason = "no_data_retry" if due else None
        return reason


def plan_financials(knowledge: Knowledge, today: date, policy: CohortPolicy) -> list[Planned]:
    """Every statement request due today, ordered by reason and then newest period first."""
    corps = knowledge.corps
    periods = [
        (year, report)
        for year in range(today.year, policy.first_year - 1, -1)
        for report in sorted(REPORT_END_MONTH, key=REPORT_END_MONTH.__getitem__, reverse=True)
        if period_end(year, report) < today
    ]
    by_reason: dict[str, list[Planned]] = defaultdict(list)
    for year, report in periods:
        deadline = period_end(year, report) + timedelta(
            days=DEADLINE_DAYS[report] + policy.season_grace_days
        )
        period = _Period(
            in_season=today <= deadline,
            recent=year >= today.year - policy.no_data_retry_years,
            today=today,
            policy=policy,
        )
        for corp in corps:
            filed = knowledge.filings.get((corp, year, report))
            consolidated = DartRequest.financials(corp, year, report, "CFS")
            seen = knowledge.seen(consolidated)
            reason = period.reason(seen, filed)
            if reason is not None:
                by_reason[reason].append(Planned(consolidated, reason))
            if seen is None or seen.outcome != NO_DATA:
                continue
            separate = DartRequest.financials(corp, year, report, "OFS")
            reason = period.reason(knowledge.seen(separate), filed)
            if reason is not None:
                by_reason[reason].append(Planned(separate, reason))
    return [item for reason in REASONS for item in by_reason.get(reason, [])]


def financial_window(request: DartRequest) -> tuple[date, date] | None:
    if request.endpoint != FINANCIALS:
        return None
    parameters = request.parameters
    return period_window(int(parameters["bsns_year"]), parameters["reprt_code"])


def summarize(planned: Iterable[Planned]) -> dict[str, object]:
    """Counts by reason and by (business year, report, fs_div) for statement requests."""
    reasons: dict[str, int] = defaultdict(int)
    periods: dict[str, int] = defaultdict(int)
    for item in planned:
        reasons[item.reason] += 1
        if item.request.endpoint == FINANCIALS:
            parameters = item.request.parameters
            label = f"{parameters['bsns_year']}/{parameters['reprt_code']}/{parameters['fs_div']}"
            periods[label] += 1
    return {"by_reason": dict(sorted(reasons.items())), "by_period": dict(sorted(periods.items()))}
