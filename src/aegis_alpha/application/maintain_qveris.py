"""Qveris requests of ``aas maintain``: planned from the raw root, asked under per-run caps.

A request is what a job asks the provider (tool, upstream provider, market, dataset and
parameters); the observation date and job ID are not part of it. A job is one ask of a
request on one observation date, so asking a request again is a new job with the day's
observation date. The raw collection root is the record of every ask; each ask's state
is read from its evidence:

- ``completed`` (``RAW_ACQUIRED``) or ``warned`` (``RAW_ACQUIRED_WITH_WARNINGS``): a
  ``complete.json`` exists. A warned ask is a partial success, counted apart from a
  completion: its ``gateway_warnings`` and each warned page's settled usage
  (``provider_status_code``, ``raw_result_count``, ``valid_result_count``) are read as
  recorded, never interpreted, and its rows are judged by the clean response path's value
  checks (``qveris_native.warned_value_failures``).
- ``failed``: every page that has an intent also has a settled billing record and the job
  has no completion. The provider answered and the answer is kept, but it was not usable.
- ``quarantined``: a page without billing that the operator quarantined (the page itself
  or its parallel group). Its outcome is unknown; the quarantine kept the worst-case
  reservation.
- ``unsettled``: a page without billing that is not quarantined. The next Qveris run
  settles it before any new call, or the operator quarantines it.

A warned ask is asked again (``partial_retry``) only when it held rows (a row failed its
value checks) or a page's ``raw_result_count`` differs from its ``valid_result_count``;
any other warned ask covers its request as ``partial``, so a warned answer whose rows all
pass is not asked again forever. A warned ask whose rows cannot be read for judging
(``unjudged``) is not asked again either, the cheaper choice, and the report counts it.

Planning, for a request that is not covered: an ``unsettled`` ask holds it (``held``);
otherwise the latest ask decides. A held ask's page whose execute answered a 5xx status
without an ``execution_id``, and whose retained settlement reads (``audits/``) found no
usage event for its search, is a transient error: it is reported as a quarantine
candidate (job and page) and is never quarantined here. An ask of an earlier observation
date that ``failed`` is asked again (``failed_retry``) 1, 2, 4, ... up to 32 days after
it, doubling with each settled failure in a row (``failing`` lists requests with more
than one); a ``partial_retry`` waits the same way, a warned ask to ask again counting as
one of that row, and one that is ``quarantined`` is asked again the next day
(``uncertain_retry``): the quarantined page never runs again, the request is asked as a
new job. An ask of the current
observation date, or a failure still backing off, waits (``waiting``).
A request never asked is ``new``. KR symbol lists are covered for ``symbol_list_days`` after
the observation date of their latest completion and are then asked again (``refresh``); the
US lists are covered only on their observation date, so they are asked every day.

Requests, in the order a run asks them while its caps last:

1. exchange-wide daily ``prices``, ``splits``, ``dividends`` downloads for every open
   session of the exchange's declared calendar (``US`` on XNYS, ``KO``/``KQ`` on XKRX)
   from the exchange's ``since`` date (no earlier than 366 days before the observation
   date) through the day before the observation date, and the exchange's
   ``extra_sessions`` (explicit earlier gaps), by date and then exchange;
2. exchange symbol lists: ``KO``/``KQ`` listed and delisted, the identity input of new KR
   listings, and ``US`` listed, the dated observation US ticker claims continue from
   (``storage.us_symbols``), and delisted, the input of US delisting events
   (``status.us.eodhd``, ``eodhd.delisted@1``);
3. FX pair histories (``<PAIR>.FOREX``) from ``forex_lookback_days`` before the
   observation date.

Retries (``failed_retry``, ``partial_retry``, ``uncertain_retry``) come after every first
ask and refresh, in the same order, so they spend only what the caps leave.

With ``continue_past_transient`` (off by default) a run whose only unsettled pages are
quarantine candidates still asks its planned requests: those pages are resumed last,
their quotes stay reserved against the balance, and the run still reports ``stopped``.

Every ask of a run is a ledger attempt (provider ``qveris``) of the request's job
(``qveris:<request SHA-256>``): ``reserved`` with the ask's job fingerprint as the
reservation's receipt before the cohort runs, ``started`` immediately before its paid
execution, then settled from the raw evidence: ``succeeded`` with a retained
``aas-qveris-attempt-v1`` receipt for a completion or a settled failure, ``uncertain`` for a
quarantined page (or a ``started`` attempt without an intent), and ``failed`` with a
``released`` event for an ask the run never executed (its caps or the account's balance
stopped it first). An attempt whose ask is ``unsettled`` stays open until a resume or a
quarantine decides it. An interrupted run's attempts are settled the same way at the
start of the next run, so no ask is executed twice: the raw root reuses a completion,
refuses a settled failure's job, and the planner never re-plans an unsettled ask. A run
that still holds an unsettled ask after its resume reports ``stopped``.
"""

from __future__ import annotations

# ruff: noqa: PLC0415 -- provider clients and storage modules load with the stage that uses them.
import hashlib
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.application.qveris_daily import EXCHANGES, daily_job, declaration_for
from aegis_alpha.data.descriptor_tree import DescriptorTree, DescriptorTreeError
from aegis_alpha.data.qveris_contracts import (
    EOD_HISTORY_JSON_TOOL,
    EOD_TOOL,
    EOD_UNIVERSE_TOOL,
    FX_DATASET,
    FX_EXCHANGE,
    FX_MARKET,
    MAX_PAGES,
    QverisJob,
    load_json,
    object_value,
)
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage import collection_ledger as ledger
from aegis_alpha.storage.collection_ledger import Job

if TYPE_CHECKING:
    from aegis_alpha.data.qveris import InvocationBudget
    from aegis_alpha.data.qveris_billing import QverisPort
    from aegis_alpha.data.qveris_client import QverisResponse
    from aegis_alpha.storage.calendar_declaration import Declaration
    from aegis_alpha.storage.workspace import Workspace

PROVIDER: Final = "qveris"
DAILY_DATASETS: Final = ("prices", "splits", "dividends")
SYMBOL_EXCHANGES: Final = ("KO", "KQ", "US")
US_SYMBOL_LIST: Final = "US"
MAX_LOOKBACK_DAYS: Final = 366
ATTEMPT_SCHEMA: Final = "aas-qveris-attempt-v1"
REQUEST_SCHEMA: Final = "aas-qveris-request-v1"
REASONS: Final = ("new", "failed_retry", "partial_retry", "uncertain_retry", "refresh")
_MAX_DOCUMENT: Final = 1024 * 1024
_TERMINAL: Final = frozenset({"completed", "warned"})
RETRY_DOUBLINGS: Final = 5
_RETRIES: Final = frozenset({"failed_retry", "partial_retry", "uncertain_retry"})
UNCLASSIFIED: Final = "unclassified"
_SETTLEMENT_READ: Final = "/auth/usage/history/v2"
# A settlement read that found nothing for one search is a few hundred bytes; larger audits
# hold other answers and are not read when looking for those.
_MAX_EMPTY_READ: Final = 64 * 1024
_SERVER_ERRORS: Final = range(500, 600)

type Request = tuple[str, str, str, str, str]


def request_of(job: QverisJob) -> Request:
    return (job.tool_id, job.upstream, job.market, job.dataset, job.parameters_json)


def request_sha256(request: Request) -> str:
    return hashlib.sha256(canonical_json_bytes([REQUEST_SCHEMA, *request])).hexdigest()


# --- policy ------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QverisPolicy:
    """What a maintenance run asks Qveris for; its SHA-256 is the ledger jobs' policy hash."""

    exchanges: tuple[str, ...] = ("US", "KO", "KQ")
    datasets: tuple[str, ...] = DAILY_DATASETS
    since: Mapping[str, date] = field(default_factory=dict)
    extra_sessions: Mapping[str, tuple[date, ...]] = field(default_factory=dict)
    symbol_lists: tuple[str, ...] = ()
    symbol_list_days: int = 7
    forex: tuple[str, ...] = ()
    forex_lookback_days: int = 10

    def __post_init__(self) -> None:
        if not set(self.exchanges) <= EXCHANGES.keys() or len(set(self.exchanges)) != len(
            self.exchanges
        ):
            raise ValueError("qveris exchanges must be distinct among " + ", ".join(EXCHANGES))
        if not set(self.datasets) <= set(DAILY_DATASETS) or len(set(self.datasets)) != len(
            self.datasets
        ):
            raise ValueError("qveris datasets must be distinct among " + ", ".join(DAILY_DATASETS))
        if set(self.since) != set(self.exchanges) or not all(
            type(value) is date for value in self.since.values()
        ):
            raise ValueError("qveris since must give one first date for every exchange")
        if not set(self.extra_sessions) <= set(self.exchanges) or not all(
            isinstance(days, tuple) and all(type(day) is date for day in days)
            for days in self.extra_sessions.values()
        ):
            raise ValueError("qveris extra_sessions are dates of configured exchanges")
        if not set(self.symbol_lists) <= set(SYMBOL_EXCHANGES):
            raise ValueError("qveris symbol lists are exchanges among KO, KQ and US")
        for name, value in (
            ("symbol_list_days", self.symbol_list_days),
            ("forex_lookback_days", self.forex_lookback_days),
        ):
            if type(value) is not int or not 1 <= value <= MAX_LOOKBACK_DAYS:
                raise ValueError(f"qveris {name} must be from 1 to {MAX_LOOKBACK_DAYS}")
        for pair in self.forex:
            if len(pair) != 6 or not pair.isalpha() or not pair.isupper():  # noqa: PLR2004 -- ISO pair
                raise ValueError("qveris forex pairs are six-letter currency pairs")

    def document(self) -> dict[str, object]:
        return {
            "exchanges": list(self.exchanges),
            "datasets": list(self.datasets),
            "since": {key: value.isoformat() for key, value in sorted(self.since.items())},
            "extra_sessions": {
                key: [day.isoformat() for day in sorted(days)]
                for key, days in sorted(self.extra_sessions.items())
            },
            "symbol_lists": list(self.symbol_lists),
            "symbol_list_days": self.symbol_list_days,
            "forex": list(self.forex),
            "forex_lookback_days": self.forex_lookback_days,
        }

    @property
    def sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.document())).hexdigest()


# --- raw evidence ------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PageWarning:
    """One warned page as recorded: the gateway's warning and the page's settled usage.

    Values are kept as the raw root holds them; nothing here says what a code means.
    """

    page: object
    reason_code: object
    provider_status_code: object
    raw_result_count: object
    valid_result_count: object

    @property
    def counts(self) -> str:
        """``equal``/``differ`` for the page's raw and valid result counts, else ``unknown``."""
        raw, valid = self.raw_result_count, self.valid_result_count
        if type(raw) is not int or type(valid) is not int:
            return "unknown"
        return "equal" if raw == valid else "differ"


@dataclass(frozen=True, slots=True)
class TransientPage:
    """An unsettled page whose execute answered 5xx without an execution ID."""

    page: str
    http_status: int
    search_id: str
    empty_reads: int = 0  # retained settlement reads that found no usage event
    other_reads: int = 0  # retained settlement reads that found one or more

    @property
    def candidate(self) -> bool:
        """A quarantine candidate: every retained settlement read found no usage event."""
        return self.empty_reads > 0 and self.other_reads == 0


@dataclass(frozen=True, slots=True)
class RawAsk:
    """One ask (job directory) of a request in the raw root and its evidence state."""

    job: QverisJob
    state: str
    evidence: tuple[tuple[str, str], ...]  # (relative path, SHA-256) of the deciding files
    warnings: tuple[PageWarning, ...] = ()  # a warned ask's pages, as recorded
    transient: tuple[TransientPage, ...] = ()  # an unsettled ask's 5xx pages

    @property
    def fingerprint(self) -> str:
        return self.job.fingerprint

    @property
    def observation_date(self) -> date:
        return self.job.observation_date


def _sha(tree: DescriptorTree, path: str) -> str:
    return hashlib.sha256(tree.read_bytes(path, max_bytes=_MAX_DOCUMENT)).hexdigest()


def _document(tree: DescriptorTree, path: str) -> dict[str, object]:
    return object_value(load_json(tree.read_bytes(path, max_bytes=_MAX_DOCUMENT)))


def _quarantined_batches(tree: DescriptorTree) -> frozenset[str]:
    if not tree.exists("parallel-batches"):
        return frozenset()
    return frozenset(
        name
        for name in tree.listdir("parallel-batches")
        if tree.exists(f"parallel-batches/{name}/quarantine.json")
        and not tree.exists(f"parallel-batches/{name}/complete.json")
    )


def _page_warnings(
    tree: DescriptorTree, base: str, body: Mapping[str, object]
) -> tuple[PageWarning, ...]:
    """Each ``gateway_warnings`` entry of a warned completion with its page's usage."""
    listed = body.get("gateway_warnings")
    entries = (
        [item for item in listed if isinstance(item, dict)] if isinstance(listed, list) else []
    )
    if not entries:
        return (PageWarning(None, None, None, None, None),)
    found = []
    for entry in entries:
        page = entry.get("page")
        usage: object = None
        if type(page) is int and 0 <= page < MAX_PAGES:
            billing = f"{base}/{page:04d}.billing.json"
            if tree.exists(billing):
                usage = _document(tree, billing).get("usage")
        recorded = cast("dict[str, object]", usage) if isinstance(usage, dict) else {}
        found.append(
            PageWarning(
                page,
                entry.get("reason_code"),
                recorded.get("provider_status_code"),
                recorded.get("raw_result_count"),
                recorded.get("valid_result_count"),
            )
        )
    return tuple(found)


def _transient(tree: DescriptorTree, page: str) -> TransientPage | None:
    """The page when its execute answered 5xx without an execution ID, else ``None``."""
    if not tree.exists(f"{page}.response.json"):
        return None
    response = _document(tree, f"{page}.response.json")
    status, execution = response.get("status"), response.get("execution_id")
    if (
        type(status) is not int
        or status not in _SERVER_ERRORS
        or (isinstance(execution, str) and execution)
    ):
        return None
    search = _document(tree, f"{page}.intent.json").get("search_id")
    return TransientPage(page, status, search) if isinstance(search, str) and search else None


def _settlement_reads(tree: DescriptorTree, searches: set[str]) -> dict[str, tuple[int, int]]:
    """(reads without, reads with a usage event) the raw root retains for each search."""
    counts = {search: [0, 0] for search in searches}
    if not searches or not tree.exists("audits"):
        return {}
    for name in tree.listdir("audits"):
        path = f"audits/{name}"
        if not name.endswith(".json") or tree.stat(path).st_size > _MAX_EMPTY_READ:
            continue
        document = _document(tree, path)
        query, response = document.get("query"), document.get("response")
        if document.get("path") != _SETTLEMENT_READ or not isinstance(query, dict):
            continue
        search = query.get("search_id")
        data = response.get("data") if isinstance(response, dict) else None
        total = data.get("total") if isinstance(data, dict) else None
        if search in counts and type(total) is int:
            counts[search][0 if total == 0 else 1] += 1
    return {search: (empty, other) for search, (empty, other) in counts.items()}


def _ask(tree: DescriptorTree, name: str, batches: frozenset[str]) -> tuple[QverisJob, RawAsk]:
    base = f"jobs/{name}"
    if tree.exists(f"{base}/complete.json"):
        body = _document(tree, f"{base}/complete.json")
        job = QverisJob.from_document(body.get("job"))
        state = "warned" if body.get("status") == "RAW_ACQUIRED_WITH_WARNINGS" else "completed"
        evidence = ((f"{base}/complete.json", _sha(tree, f"{base}/complete.json")),)
        warnings = _page_warnings(tree, base, body) if state == "warned" else ()
        return job, RawAsk(job, state, evidence, warnings)
    intents = sorted(item for item in tree.listdir(base) if item.endswith(".intent.json"))
    if not intents:
        raise ValueError(f"Qveris job {name} has neither a completion nor an intent")
    job = QverisJob.from_document(_document(tree, f"{base}/{intents[0]}").get("job"))
    states: list[str] = []
    evidence: list[tuple[str, str]] = []
    transient: list[TransientPage] = []
    for intent in intents:
        page = f"{base}/{intent.removesuffix('.intent.json')}"
        billing = f"{page}.billing.json"
        if tree.exists(billing) and _document(tree, billing).get("over_quote") is False:
            states.append("billed")
            evidence.append((billing, _sha(tree, billing)))
        elif not tree.exists(billing) and (
            tree.exists(f"{page}.quarantine.json")
            or _document(tree, f"{page}.intent.json").get("batch_id") in batches
        ):
            states.append("quarantined")
        else:
            states.append("unsettled")
            found = _transient(tree, page)
            if found is not None:
                transient.append(found)
    state = (
        "unsettled"
        if "unsettled" in states
        else "quarantined"
        if "quarantined" in states
        else "failed"
    )
    return job, RawAsk(job, state, tuple(evidence), transient=tuple(transient))


def raw_asks(raw_root: Path | None) -> dict[Request, list[RawAsk]]:
    """Every ask of the raw root by request, oldest observation date first."""
    found: dict[Request, list[RawAsk]] = {}
    if raw_root is None or not raw_root.exists():
        return found
    try:
        with DescriptorTree.open_path(raw_root) as tree:
            batches = _quarantined_batches(tree)
            names = sorted(tree.listdir("jobs")) if tree.exists("jobs") else []
            for name in names:
                job, ask = _ask(tree, name, batches)
                if job.fingerprint != name:
                    raise ValueError(f"Qveris job {name} is filed under another fingerprint")
                found.setdefault(request_of(job), []).append(ask)
            searches = {page.search_id for asks in found.values() for ask in asks
                        for page in ask.transient}  # fmt: skip
            if searches:
                reads = _settlement_reads(tree, searches)
                for asks in found.values():
                    asks[:] = [
                        replace(ask, transient=tuple(
                            replace(page, empty_reads=reads[page.search_id][0],
                                    other_reads=reads[page.search_id][1])
                            for page in ask.transient))
                        if ask.transient and reads else ask
                        for ask in asks
                    ]  # fmt: skip
    except (OSError, DescriptorTreeError) as error:
        raise ValueError("cannot read the Qveris raw collection root") from error
    for asks in found.values():
        asks.sort(key=lambda item: (item.observation_date, item.fingerprint))
    return found


# --- planning ----------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Planned:
    job: QverisJob
    reason: str
    dataset_id: str
    day: date | None = None

    @property
    def request(self) -> Request:
        return request_of(self.job)


@dataclass(slots=True)
class QverisPlan:
    planned: list[Planned] = field(default_factory=list)
    resumable: list[QverisJob] = field(default_factory=list)
    covered: int = 0
    partial: int = 0
    held: list[dict[str, object]] = field(default_factory=list)
    candidates: list[dict[str, object]] = field(default_factory=list)
    warned: list[tuple[RawAsk, str, int | None]] = field(default_factory=list)
    waiting: int = 0
    failing: list[dict[str, object]] = field(default_factory=list)
    calendars: dict[str, str] = field(default_factory=dict)
    windows: dict[str, dict[str, str] | None] = field(default_factory=dict)
    undeclared: dict[str, int] = field(default_factory=dict)

    def report(self) -> dict[str, object]:
        reasons = dict.fromkeys(REASONS, 0)
        for item in self.planned:
            reasons[item.reason] += 1
        by_dataset: dict[str, int] = {}
        for item in self.planned:
            key = f"{item.job.market}:{item.job.dataset}"
            by_dataset[key] = by_dataset.get(key, 0) + 1
        return {
            "planned": len(self.planned),
            "resumable": len(self.resumable),
            "reasons": reasons,
            "by_dataset": dict(sorted(by_dataset.items())),
            "covered": self.covered,
            "partial": self.partial,
            "warnings": warning_summary(self.warned),
            "held": self.held,
            "quarantine_candidates": self.candidates,
            "waiting": self.waiting,
            "failing": self.failing,
            "calendars": self.calendars,
            "windows": self.windows,
            "undeclared_days": self.undeclared,
        }


def _label(value: object) -> str:
    """A recorded code as report text: text or an integer as given, anything else unclassified."""
    if isinstance(value, str) and value:
        return value
    return str(value) if type(value) is int else UNCLASSIFIED


def warning_summary(warned: Sequence[tuple[RawAsk, str, int | None]]) -> dict[str, object]:
    """Warned asks that decide their request: decisions, held rows and pages by recorded code.

    Pages are counted by (``reason_code``, ``provider_status_code``) as recorded, a missing
    or non-scalar code as ``unclassified``, with whether the page's raw and valid result
    counts are equal, differ or are unknown. ``unjudged`` asks are those whose rows could
    not be read for the value checks.
    """
    pages: dict[tuple[str, str], dict[str, int]] = {}
    decisions: dict[str, int] = {}
    for ask, decision, _ in warned:
        decisions[decision] = decisions.get(decision, 0) + 1
        for warning in ask.warnings:
            key = (_label(warning.reason_code), _label(warning.provider_status_code))
            counts = pages.setdefault(
                key, {"pages": 0, "counts_equal": 0, "counts_differ": 0, "counts_unknown": 0}
            )
            counts["pages"] += 1
            counts["counts_" + warning.counts] += 1
    return {
        "asks": len(warned),
        "decisions": dict(sorted(decisions.items())),
        "held_rows": sum(held or 0 for _, _, held in warned),
        "unjudged": sum(held is None for _, _, held in warned),
        "by_reason": [
            {"reason_code": reason, "provider_status_code": code, **counts}
            for (reason, code), counts in sorted(pages.items())
        ],
    }


def _never(ask: RawAsk) -> bool:
    del ask
    return False


def _decide(  # noqa: PLR0911 -- one decision per evidence state
    asks: Sequence[RawAsk],
    today: date,
    *,
    fresh_after: date | None = None,
    again: Callable[[RawAsk], bool] = _never,
) -> tuple[str, RawAsk | None]:
    """(``covered``/``partial``/``held``/``waiting``/a planning reason, the deciding ask).

    ``again`` tells a warned ask to ask again: it covers nothing and waits like a failure.
    """
    done = [ask for ask in asks if ask.state in _TERMINAL and not again(ask)]
    if done and (fresh_after is None or done[-1].observation_date > fresh_after):
        return ("partial" if done[-1].state == "warned" else "covered"), done[-1]
    unsettled = [ask for ask in asks if ask.state == "unsettled"]
    if unsettled:
        return "held", unsettled[-1]
    if not asks:
        return "new", None
    latest = asks[-1]
    if latest.observation_date >= today:
        return "waiting", latest
    if latest.state in _TERMINAL and again(latest):
        due = latest.observation_date + timedelta(days=retry_after(asks, again))
        return ("backoff" if today < due else "partial_retry"), latest
    if latest.state in _TERMINAL or latest.state == "quarantined":
        return ("refresh" if latest.state in _TERMINAL else "uncertain_retry"), latest
    due = latest.observation_date + timedelta(days=retry_after(asks, again))
    return ("backoff" if today < due else "failed_retry"), latest


def failed_streak(asks: Sequence[RawAsk], again: Callable[[RawAsk], bool] = _never) -> int:
    """The number of settled failures (and warned asks to ask again) that end the asks."""
    count = 0
    for ask in reversed(asks):
        if ask.state != "failed" and not (ask.state == "warned" and again(ask)):
            break
        count += 1
    return count


def retry_after(asks: Sequence[RawAsk], again: Callable[[RawAsk], bool] = _never) -> int:
    """Days after the latest settled failure before a paid retry: 1, 2, 4, ... up to 32."""
    return 2 ** min(max(failed_streak(asks, again) - 1, 0), RETRY_DOUBLINGS)


def held_rows(raw_root: Path | None, ask: RawAsk) -> int | None:
    """Rows of a warned ask that fail their value checks, or ``None`` when unreadable."""
    from aegis_alpha.data.qveris_native import read_completed_job, warned_value_failures

    if raw_root is None or ask.state != "warned" or not ask.evidence:
        return None
    try:
        history = read_completed_job(raw_root, ask.fingerprint, ask.evidence[0][1])
    except (ValueError, TypeError, KeyError, OSError, RuntimeError, DescriptorTreeError):
        return None  # reported as unjudged, never asked again on its account
    return len(warned_value_failures(history))


def _ledger_dataset(job: QverisJob) -> str:
    if job.dataset == FX_DATASET:
        symbol = str(job.parameters["symbol"])
        return f"fx.{symbol.removesuffix('.' + FX_EXCHANGE).lower()}.eodhd"
    if job.dataset == "universe":
        return f"identity.{job.market.lower()}.eodhd"
    market = job.market.lower()
    return f"prices.{market}.eodhd" if job.dataset == "prices" else f"actions.{market}.eodhd"


def symbol_list_job(exchange: str, delisted: str, today: date) -> QverisJob:
    return QverisJob(
        f"maintain-symbols-{exchange}-{delisted}",
        EOD_UNIVERSE_TOOL,
        "eodhd",
        "US" if exchange == US_SYMBOL_LIST else "KR",
        "universe",
        canonical_json_bytes({"EXCHANGE_CODE": exchange, "delisted": delisted, "fmt": "json"})
        .decode(),
        today,
    )  # fmt: skip


def forex_job(pair: str, today: date, lookback_days: int) -> QverisJob:
    start = today - timedelta(days=lookback_days)
    parameters = {"symbol": f"{pair}.{FX_EXCHANGE}", "order": "a", "from": start.isoformat(),
                  "fmt": "json"}  # fmt: skip
    return QverisJob(
        f"maintain-fx-{pair}",
        EOD_HISTORY_JSON_TOOL,
        "eodhd",
        FX_MARKET,
        FX_DATASET,
        canonical_json_bytes(parameters).decode(),
        today,
    )


def _sessions(
    policy: QverisPolicy, today: date, overrides: Mapping[str, Declaration], plan: QverisPlan
) -> list[tuple[date, str]]:
    found: list[tuple[date, str]] = []
    through = today - timedelta(days=1)
    for exchange in policy.exchanges:
        calendar = EXCHANGES[exchange][1]
        declaration = declaration_for(calendar, overrides.get(calendar))
        plan.calendars[calendar] = declaration.sha256
        start = max(policy.since[exchange], today - timedelta(days=MAX_LOOKBACK_DAYS - 1))
        last = min(through, declaration.end - timedelta(days=1))
        start = max(start, declaration.start)
        if through > last:
            plan.undeclared[exchange] = (through - last).days
        extra = set(policy.extra_sessions.get(exchange, ()))
        plan.windows[exchange] = (
            None if start > last else {"from": start.isoformat(), "through": last.isoformat()}
        )
        found.extend(
            (item.session_date, exchange)
            for item in declaration.days()
            if item.status == "open"
            and item.session_date <= last
            and (start <= item.session_date or item.session_date in extra)
        )
    order = {exchange: index for index, exchange in enumerate(policy.exchanges)}
    return sorted(found, key=lambda item: (item[0], order[item[1]]))


def plan_requests(  # noqa: C901 -- one pass deciding and reporting every candidate request
    raw_root: Path | None,
    policy: QverisPolicy,
    *,
    today: date,
    declarations: Sequence[Declaration] = (),
    asks: Mapping[Request, list[RawAsk]] | None = None,
) -> QverisPlan:
    """The jobs a run on ``today`` asks, in order, and what it leaves out; reads no key."""
    asks = raw_asks(raw_root) if asks is None else asks
    overrides = {item.calendar_id: item for item in declarations}
    plan = QverisPlan()
    verdicts: dict[str, int | None] = {}

    def held(ask: RawAsk) -> int | None:
        if ask.fingerprint not in verdicts:
            verdicts[ask.fingerprint] = held_rows(raw_root, ask)
        return verdicts[ask.fingerprint]

    def again(ask: RawAsk) -> bool:
        """A warned ask is asked again when it held rows or its result counts differ."""
        if ask.state != "warned":
            return False
        return any(item.counts == "differ" for item in ask.warnings) or (held(ask) or 0) > 0

    sessions = _sessions(policy, today, overrides, plan)
    candidates: list[tuple[QverisJob, date | None, date | None]] = [
        (_rename(daily_job(exchange, dataset, day, today)), day, None)
        for day, exchange in sessions
        for dataset in policy.datasets
    ]
    candidates += [
        (
            symbol_list_job(exchange, delisted, today),
            None,
            today - timedelta(days=1 if exchange == US_SYMBOL_LIST else policy.symbol_list_days),
        )
        for exchange in policy.symbol_lists
        for delisted in ("0", "1")
    ]
    candidates += [
        (forex_job(pair, today, policy.forex_lookback_days), None, None) for pair in policy.forex
    ]
    for job, day, fresh_after in candidates:
        found = asks.get(request_of(job), [])
        if found and found[-1].state in {"unsettled", "failed"}:
            plan.resumable.append(found[-1].job)
        decision, ask = _decide(found, today, fresh_after=fresh_after, again=again)
        streak = failed_streak(found, again)
        if streak > 1 and decision in {"backoff", "failed_retry", "partial_retry"}:
            plan.failing.append({
                "job_id": job.job_id, "failed_asks": streak,
                "retry_on": (cast("RawAsk", ask).observation_date
                             + timedelta(days=retry_after(found, again))).isoformat(),
            })  # fmt: skip
        if ask is not None and ask.state == "warned":
            plan.warned.append((ask, decision, held(ask)))
        if decision == "covered":
            plan.covered += 1
        elif decision == "partial":
            plan.partial += 1
        elif decision == "held":
            unsettled = cast("RawAsk", ask)
            plan.held.append({"job_id": job.job_id, "unsettled_fingerprint": unsettled
                              .fingerprint})  # fmt: skip
            plan.candidates.extend(
                {"job_id": unsettled.job.job_id, "page": page.page,
                 "http_status": page.http_status, "settlement_reads_without_usage":
                 page.empty_reads}
                for page in unsettled.transient if page.candidate
            )  # fmt: skip
        elif decision in {"waiting", "backoff"}:
            plan.waiting += 1
        else:
            plan.planned.append(Planned(job, decision, _ledger_dataset(job), day))
    # Retries take what the caps leave after every first ask and refresh.
    plan.planned.sort(key=lambda item: item.reason in _RETRIES)
    return plan


def _rename(job: QverisJob) -> QverisJob:
    """A daily job under the maintenance job ID; the fingerprint ignores the ID."""
    return QverisJob(
        job.job_id.replace("daily-", "maintain-", 1),
        job.tool_id,
        job.upstream,
        job.market,
        job.dataset,
        job.parameters_json,
        job.observation_date,
    )


# --- ledger ------------------------------------------------------------------------------------


def _us(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def ledger_job(item: Planned, policy_hash: str) -> Job:
    window: tuple[int | None, int | None] = (None, None)
    if item.day is not None:
        start = _us(datetime.combine(item.day, datetime.min.time(), tzinfo=UTC))
        window = (start, start + 86_400_000_000 - 1)
    return Job(PROVIDER, item.dataset_id, request_sha256(item.request), policy_hash, *window)


def _attempt_receipt(workspace: Workspace, request: Request, ask: RawAsk) -> str:
    """Retain the ``aas-qveris-attempt-v1`` receipt naming one ask's deciding evidence."""
    from aegis_alpha.storage.raw import put_raw

    document = {
        "schema": ATTEMPT_SCHEMA,
        "request": dict(zip(("tool_id", "upstream", "market", "dataset", "parameters_json"),
                            request, strict=True)),
        "fingerprint": ask.fingerprint,
        "observation_date": ask.observation_date.isoformat(),
        "state": ask.state,
        "evidence": [{"path": path, "sha256": digest} for path, digest in ask.evidence],
    }  # fmt: skip
    return put_raw(workspace.paths.raw, canonical_json_bytes(document))[1]


def settle(
    workspace: Workspace,
    asks: Mapping[Request, list[RawAsk]],
    *,
    clock: Callable[[], datetime],
) -> dict[str, int]:
    """Settle every ``reserved``/``started`` Qveris attempt from the raw evidence of its ask."""
    by_fingerprint = {
        ask.fingerprint: (request, ask) for request, items in asks.items() for ask in items
    }
    state = workspace.state
    rows = state.execute(
        "SELECT a.job_id, a.attempt, a.status, u.receipt_hash FROM collection_attempts a "
        "JOIN collection_jobs j ON j.job_id=a.job_id JOIN usage_events u ON "
        "u.job_id=a.job_id AND u.attempt=a.attempt AND u.kind='reserved' "
        "WHERE j.provider=? AND a.status IN ('reserved','started') ORDER BY a.job_id, a.attempt",
        (PROVIDER,),
    ).fetchall()
    settled = {"succeeded": 0, "uncertain": 0, "released": 0}
    for job_id, number, status, fingerprint in rows:
        attempt = ledger.Attempt(str(job_id), int(number))
        found = by_fingerprint.get(str(fingerprint))
        at_us = _us(clock())
        if found is None:
            # No intent: the ask never reached the provider. A started attempt without an
            # intent cannot be shown not to have, so it stays uncertain.
            if status == "reserved":
                ledger.release(state, attempt, at_us=at_us)
                settled["released"] += 1
            else:
                ledger.uncertain(state, attempt, at_us=at_us)
                settled["uncertain"] += 1
            continue
        request, ask = found
        if ask.state == "unsettled":
            # Its page may have run: it stays open until a resume or the operator's
            # quarantine decides the ask, then it is settled from that evidence.
            continue
        if status == "reserved":
            ledger.start(state, attempt, at_us=at_us)
        if ask.state in {"completed", "warned", "failed"}:
            receipt = _attempt_receipt(workspace, request, ask)
            ledger.succeed(state, attempt, receipt_sha256=receipt, outcome=ask.state, at_us=at_us)
            settled["succeeded"] += 1
        else:
            ledger.uncertain(state, attempt, at_us=at_us)
            settled["uncertain"] += 1
    return settled


class LedgerPort:
    """A Qveris port that marks the ask's attempt ``started`` just before its paid execution."""

    def __init__(self, inner: QverisPort, on_execute: Callable[[str, str], None]) -> None:
        self._inner = inner
        self._on_execute = on_execute

    @property
    def account_key(self) -> str:
        return self._inner.account_key

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        if path == "/tools/execute" and body is not None and query is not None:
            parameters = {
                key: value
                for key, value in object_value(body.get("parameters")).items()
                if key != "offset"
            }
            self._on_execute(str(query.get("tool_id")), canonical_json_bytes(parameters).decode())
        return self._inner.request(path, body=body, query=query)


# --- run ---------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QverisCaps:
    """Per-run limits; the account's server balance is checked again before every page."""

    max_calls: int
    max_credits: str
    max_http_requests: int | None = None
    time_limit_seconds: float = 3600.0
    request_interval: float | None = None
    continue_past_transient: bool = False
    """Ask the planned requests next to unsettled pages that are only quarantine candidates."""

    def __post_init__(self) -> None:
        from aegis_alpha.data.qveris_contracts import credit_value

        if type(self.max_calls) is not int or self.max_calls < 0:
            raise ValueError("qveris max_calls must be a nonnegative integer")
        if type(self.continue_past_transient) is not bool:
            raise TypeError("qveris continue_past_transient must be true or false")
        credit_value(self.max_credits)


def _resume_budget() -> InvocationBudget:
    """A budget that refuses every new page before its intent: a resume executes nothing."""
    from aegis_alpha.data.qveris import InvocationBudget

    class NoNewCalls(InvocationBudget):
        def reserve(self, quote: Decimal) -> None:
            del quote
            raise RuntimeError("INVOCATION_CALL_LIMIT: resuming asks makes no new call")

    return NoNewCalls(1, Decimal(0))


class _Session:
    """One run's Qveris client, pacing and ledger attempts."""

    def __init__(
        self,
        workspace: Workspace,
        raw_root: Path,
        caps: QverisCaps,
        port: Callable[[], QverisPort],
        clock: Callable[[], datetime],
    ) -> None:
        from aegis_alpha.data.qveris_pacing import DEFAULT_REQUEST_INTERVAL, RequestAdmission

        self.workspace, self.raw_root, self.clock, self._port = workspace, raw_root, clock, port
        limit = caps.max_http_requests or max(32, caps.max_calls * 16)
        self.admission = RequestAdmission(limit, caps.time_limit_seconds)
        self.interval = (
            DEFAULT_REQUEST_INTERVAL if caps.request_interval is None else caps.request_interval
        )
        self.attempts: dict[tuple[str, str], ledger.Attempt] = {}
        self._client: QverisPort | None = None

    def client(self) -> QverisPort:
        from aegis_alpha.data.qveris_pacing import PacedQverisClient, RequestPacer

        if self._client is None:
            paced = PacedQverisClient(self._port(), RequestPacer(self.interval), self.admission)
            self._client = LedgerPort(paced, self.started)
        return self._client

    def started(self, tool_id: str, parameters_json: str) -> None:
        attempt = self.attempts.get((tool_id, parameters_json))
        if attempt is None:
            return
        row = self.workspace.state.execute(
            "SELECT status FROM collection_attempts WHERE job_id=? AND attempt=?",
            (attempt.job_id, attempt.attempt),
        ).fetchone()
        if row is not None and row[0] == "reserved":
            ledger.start(self.workspace.state, attempt, at_us=_us(self.clock()))

    def reserve(self, planned: Sequence[Planned], policy_hash: str) -> None:
        for item in planned:
            self.attempts[(item.job.tool_id, item.job.parameters_json)] = ledger.reserve(
                self.workspace.state,
                ledger_job(item, policy_hash),
                at_us=_us(self.clock()),
                receipt_sha256=item.job.fingerprint,
            )

    def finish(self) -> dict[str, int]:
        asks = raw_asks(self.raw_root)
        settled = settle(self.workspace, asks, clock=self.clock)
        open_asks = {ask.fingerprint for items in asks.values() for ask in items
                     if ask.state == "unsettled"}  # fmt: skip
        # Asks the cohort never reached hold no intent; they are released, never called.
        for row in self.workspace.state.execute(
            "SELECT a.job_id, a.attempt, u.receipt_hash FROM collection_attempts a JOIN "
            "collection_jobs j ON j.job_id=a.job_id JOIN usage_events u ON u.job_id=a.job_id AND "
            "u.attempt=a.attempt AND u.kind='reserved' WHERE j.provider=? "
            "AND a.status='reserved'",
            (PROVIDER,),
        ).fetchall():
            if str(row[2]) in open_asks:
                continue
            attempt = ledger.Attempt(str(row[0]), int(row[1]))
            ledger.release(self.workspace.state, attempt, at_us=_us(self.clock()))
            settled["released"] += 1
        return settled

    def calls(self) -> dict[str, int]:
        return {
            "provider_calls": self.admission.paid_executions,
            "http_requests": self.admission.http_requests,
        }


def _tolerated(asks: Mapping[Request, list[RawAsk]]) -> frozenset[str]:
    """The unsettled pages that are quarantine candidates (``continue_past_transient``)."""
    return frozenset(
        page.page
        for items in asks.values()
        for ask in items
        if ask.state == "unsettled"
        for page in ask.transient
        if page.candidate
    )


def _status(cohort: Mapping[str, object]) -> str:
    from aegis_alpha.data.qveris_batch import BUDGET_STOPS

    stopped = cohort.get("stopped")
    if stopped and str(stopped) not in BUDGET_STOPS:
        return "stopped"
    if cohort.get("status") == "PARTIAL":
        return "partial"
    return "budget_exhausted" if stopped else "succeeded"


def collect(  # noqa: PLR0913 -- the run's explicit inputs
    workspace: Workspace,
    raw_root: Path,
    policy: QverisPolicy,
    caps: QverisCaps,
    port: Callable[[], QverisPort],
    *,
    today: date,
    clock: Callable[[], datetime],
    declarations: Sequence[Declaration] = (),
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    """Resume and settle earlier asks, plan, reserve, ask the cohort in order, settle again."""
    from aegis_alpha.data import qveris
    from aegis_alpha.data.qveris_batch import collect_cohort
    from aegis_alpha.data.qveris_contracts import credit_value

    session = _Session(workspace, raw_root, caps, port, clock)
    # An ask an earlier run left unsettled, or billed without a completion, is finished on
    # its own job first: its pages are settled and verified, and no page is executed.
    asks = raw_asks(raw_root)
    resumable = plan_requests(
        raw_root, policy, today=today, declarations=declarations, asks=asks
    ).resumable
    tolerate = _tolerated(asks) if caps.continue_past_transient else frozenset()
    # Tolerated pages' jobs are resumed last, so the others are settled first.
    resumable.sort(key=lambda job: any(page.split("/")[1] == job.fingerprint
                                       for page in tolerate))  # fmt: skip
    resumed: dict[str, object] | None = None
    if resumable:
        resumed = collect_cohort(
            tuple(resumable), raw_root, session.client(), budget=_resume_budget(), sleep=sleep,
            tolerate=tolerate,
        )  # fmt: skip
        asks = raw_asks(raw_root)
        if tolerate:
            tolerate = _tolerated(asks)
    before = settle(workspace, asks, clock=clock)
    plan = plan_requests(raw_root, policy, today=today, declarations=declarations, asks=asks)
    report: dict[str, object] = {
        "settled_before": before,
        "resumed": None
        if resumed is None
        else {
            key: resumed[key]
            for key in ("requested", "completed", "warned", "failed", "empty", "stopped")
        },
        "continue_past_transient": caps.continue_past_transient,
        **plan.report(),
    }
    # A paid page still unsettled after the resume needs settlement or quarantine.
    unsettled = bool(plan.held) or (resumed is not None and _status(resumed) == "stopped")
    if not plan.planned or caps.max_calls == 0:
        status = "budget_exhausted" if plan.planned else "succeeded"
        return {**report, "status": "stopped" if unsettled else status, **session.calls()}
    session.reserve(plan.planned, policy.sha256)
    budget = qveris.InvocationBudget(caps.max_calls, credit_value(caps.max_credits))
    # An interrupted process (KeyboardInterrupt, SystemExit) settles nothing here: its
    # started attempts stay started until the next run has resumed their asks.
    try:
        cohort = collect_cohort(
            tuple(item.job for item in plan.planned),
            raw_root,
            session.client(),
            budget=budget,
            sleep=sleep,
            tolerate=tolerate,
        )
    except Exception:
        session.finish()
        raise
    after = session.finish()
    return {
        **report,
        "status": "stopped" if unsettled else _status(cohort),
        "cohort": {key: cohort[key] for key in sorted(cohort) if key != "billing_authority"},
        "settled_after": after,
        **session.calls(),
        "max_calls": caps.max_calls,
        "max_credits": caps.max_credits,
        "reserved_credits": str(budget.reserved_credits),
    }


# --- import ------------------------------------------------------------------------------------


def _imported_fingerprints(workspace: Workspace) -> frozenset[str]:
    """Job fingerprints the committed ``qveris-*`` sources record in their lineage."""
    import json

    from aegis_alpha.storage import source_library_schema as schema
    from aegis_alpha.storage.source_library import list_sources

    visible = {
        str(row["source_id"])
        for row in list_sources(workspace)
        if str(row["source_id"]).startswith(PROVIDER + "-")
    }
    found: set[str] = set()
    for conn in schema.connections(workspace).values():
        for source_id, manifest in conn.execute(
            "SELECT source_id, manifest_json FROM source_library_commits "
            "WHERE substr(source_id, 1, ?) = ?",
            [len(PROVIDER) + 1, PROVIDER + "-"],
        ).fetchall():
            if str(source_id) not in visible:
                continue
            metadata = json.loads(str(manifest)).get("metadata")
            lineage = metadata.get("lineage") if isinstance(metadata, dict) else None
            if isinstance(lineage, dict) and isinstance(lineage.get("fingerprint"), str):
                found.add(str(lineage["fingerprint"]))
    return frozenset(found)


def _completed(
    asks: Mapping[Request, list[RawAsk]], imported: frozenset[str]
) -> tuple[list[str], list[str], list[str]]:
    """(FX jobs, daily jobs that need the identity document, symbol lists) to import."""
    ready: list[str] = []
    needs_identity: list[str] = []
    symbols: list[str] = []
    for request, items in asks.items():
        tool, _, market, dataset, _ = request
        for ask in items:
            if ask.state not in _TERMINAL:
                continue
            if tool == EOD_UNIVERSE_TOOL and market in {"KR", "US"}:
                symbols.append(ask.fingerprint)
            elif ask.fingerprint in imported:
                continue
            elif tool == EOD_TOOL and dataset in DAILY_DATASETS:
                needs_identity.append(ask.fingerprint)
            elif dataset == FX_DATASET:
                ready.append(ask.fingerprint)
    return ready, needs_identity, symbols


def import_completed(
    workspace: Workspace,
    raw_root: Path,
    identity_path: Path | None,
    *,
    asks: Mapping[Request, list[RawAsk]] | None = None,
) -> dict[str, object]:
    """Commit the completed daily, FX and symbol-list jobs no source holds yet."""
    from aegis_alpha.data.sec_evidence import read_bytes
    from aegis_alpha.storage import kr_identity, qveris_import
    from aegis_alpha.storage.source_library import list_sources

    if not raw_root.exists():
        return {"status": "no_raw_root", "built": 0, "reused": 0, "failed": 0, "missing": [],
                "provider_calls": 0}  # fmt: skip
    asks = raw_asks(raw_root) if asks is None else asks
    ready, needs_identity, symbols = _completed(asks, _imported_fingerprints(workspace))
    identity = None
    if identity_path is not None:
        identity = qveris_import.parse_identity(
            read_bytes(identity_path, maximum=qveris_import.MAX_IDENTITY_BYTES)
        )
    if identity is not None:
        ready += needs_identity
    selected, missing = qveris_import.completions(raw_root, fingerprints=sorted(ready))
    committed = frozenset(str(row["source_id"]) for row in list_sources(workspace))
    result = qveris_import.import_completions(
        raw_root, selected, identity, workspace=workspace, committed=committed
    )
    units = []
    for fingerprint in sorted(symbols):
        unit = kr_identity.read_eodhd_job(raw_root / "jobs" / fingerprint)
        if unit.content.source_id in committed:
            continue
        units.append(kr_identity.import_unit(workspace, unit))
    return {
        "built": result["built"],
        "reused": result["reused"],
        "failed": result["failed"],
        "rows": result["rows"],
        "held_rows": result["held_rows"],
        "provider_warnings": result["provider_warnings"],
        "units": result["units"],
        "failures": result["failures"],
        "missing": missing,
        "identity_sha256": None if identity is None else identity.sha256,
        "waiting_for_identity": [] if identity is not None else sorted(needs_identity),
        "symbol_lists": units,
        "provider_calls": 0,
    }
