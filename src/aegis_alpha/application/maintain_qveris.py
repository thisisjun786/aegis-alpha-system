"""Qveris requests of ``aas maintain``: planned from the raw root, asked under per-run caps.

A request is what a job asks the provider (tool, upstream provider, market, dataset and
parameters); the observation date and job ID are not part of it. A job is one ask of a
request on one observation date, so asking a request again is a new job with the day's
observation date. The raw collection root is the record of every ask; each ask's state
is read from its evidence:

- ``completed`` (``RAW_ACQUIRED``) or ``warned`` (``RAW_ACQUIRED_WITH_WARNINGS``): a
  ``complete.json`` exists. The request is covered; a provider warning is a completion.
- ``failed``: every page that has an intent also has a settled billing record and the job
  has no completion. The provider answered and the answer is kept, but it was not usable.
- ``quarantined``: a page without billing that the operator quarantined (the page itself
  or its parallel group). Its outcome is unknown; the quarantine kept the worst-case
  reservation.
- ``unsettled``: a page without billing that is not quarantined. The next Qveris run
  settles it before any new call, or the operator quarantines it.

Planning, for a request that is not covered: an ``unsettled`` ask holds it (``held``);
otherwise the latest ask decides. An ask of an earlier observation date that ``failed``
is asked again (``failed_retry``) and one that is ``quarantined`` likewise
(``uncertain_retry``): the quarantined page never runs again, the request is asked as a
new job. An ask of the current observation date waits for the next day (``waiting``).
A request never asked is ``new``. Symbol lists are covered for ``symbol_list_days`` after
the observation date of their latest completion and are then asked again (``refresh``).

Requests, in the order a run asks them while its caps last:

1. exchange-wide daily ``prices``, ``splits``, ``dividends`` downloads for every open
   session of the exchange's declared calendar (``US`` on XNYS, ``KO``/``KQ`` on XKRX)
   from the exchange's ``since`` date (no earlier than 366 days before the observation
   date) through the day before the observation date, and the exchange's
   ``extra_sessions`` (explicit earlier gaps), by date and then exchange;
2. KR exchange symbol lists (``KO``/``KQ``, listed and delisted), the identity input of
   new KR listings;
3. FX pair histories (``<PAIR>.FOREX``) from ``forex_lookback_days`` before the
   observation date.

Every ask of a run is a ledger attempt (provider ``qveris``) of the request's job
(``qveris:<request SHA-256>``): ``reserved`` with the ask's job fingerprint as the
reservation's receipt before the cohort runs, ``started`` immediately before its paid
execution, then settled from the raw evidence: ``succeeded`` with a retained
``aas-qveris-attempt-v1`` receipt for a completion or a settled failure, ``uncertain`` for a
page without billing, and ``failed`` with a ``released`` event for an ask the run never
executed (its caps or the account's balance stopped it first). An interrupted run's
attempts are settled the same way at the start of the next run, so no ask is executed
twice: the raw root reuses a completion, refuses a settled failure's job, and the planner
never re-plans an unsettled ask.
"""

from __future__ import annotations

# ruff: noqa: PLC0415 -- provider clients and storage modules load with the stage that uses them.
import hashlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
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
SYMBOL_EXCHANGES: Final = ("KO", "KQ")
MAX_LOOKBACK_DAYS: Final = 366
ATTEMPT_SCHEMA: Final = "aas-qveris-attempt-v1"
REQUEST_SCHEMA: Final = "aas-qveris-request-v1"
REASONS: Final = ("new", "failed_retry", "uncertain_retry", "refresh")
_MAX_DOCUMENT: Final = 1024 * 1024
_TERMINAL: Final = frozenset({"completed", "warned"})

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
            raise ValueError("qveris symbol lists are KR exchanges (KO, KQ)")
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
class RawAsk:
    """One ask (job directory) of a request in the raw root and its evidence state."""

    job: QverisJob
    state: str
    evidence: tuple[tuple[str, str], ...]  # (relative path, SHA-256) of the deciding files

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


def _ask(tree: DescriptorTree, name: str, batches: frozenset[str]) -> tuple[QverisJob, RawAsk]:
    base = f"jobs/{name}"
    if tree.exists(f"{base}/complete.json"):
        body = _document(tree, f"{base}/complete.json")
        job = QverisJob.from_document(body.get("job"))
        state = "warned" if body.get("status") == "RAW_ACQUIRED_WITH_WARNINGS" else "completed"
        evidence = ((f"{base}/complete.json", _sha(tree, f"{base}/complete.json")),)
        return job, RawAsk(job, state, evidence)
    intents = sorted(item for item in tree.listdir(base) if item.endswith(".intent.json"))
    if not intents:
        raise ValueError(f"Qveris job {name} has neither a completion nor an intent")
    job = QverisJob.from_document(_document(tree, f"{base}/{intents[0]}").get("job"))
    states: list[str] = []
    evidence: list[tuple[str, str]] = []
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
    state = (
        "unsettled"
        if "unsettled" in states
        else "quarantined"
        if "quarantined" in states
        else "failed"
    )
    return job, RawAsk(job, state, tuple(evidence))


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
    held: list[dict[str, object]] = field(default_factory=list)
    waiting: int = 0
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
            "held": self.held,
            "waiting": self.waiting,
            "calendars": self.calendars,
            "windows": self.windows,
            "undeclared_days": self.undeclared,
        }


def _decide(
    asks: Sequence[RawAsk], today: date, *, fresh_after: date | None = None
) -> tuple[str, RawAsk | None]:
    """(``covered``/``held``/``waiting``/a planning reason, the deciding ask)."""
    done = [ask for ask in asks if ask.state in _TERMINAL]
    if done and (fresh_after is None or done[-1].observation_date > fresh_after):
        return "covered", done[-1]
    unsettled = [ask for ask in asks if ask.state == "unsettled"]
    if unsettled:
        return "held", unsettled[-1]
    if not asks:
        return "new", None
    latest = asks[-1]
    if latest.observation_date >= today:
        return "waiting", latest
    if latest.state in _TERMINAL:
        return "refresh", latest
    return ("uncertain_retry" if latest.state == "quarantined" else "failed_retry"), latest


def _ledger_dataset(job: QverisJob) -> str:
    if job.dataset == FX_DATASET:
        symbol = str(job.parameters["symbol"])
        return f"fx.{symbol.removesuffix('.' + FX_EXCHANGE).lower()}.eodhd"
    if job.dataset == "universe":
        return "identity.kr.eodhd"
    market = job.market.lower()
    return f"prices.{market}.eodhd" if job.dataset == "prices" else f"actions.{market}.eodhd"


def symbol_list_job(exchange: str, delisted: str, today: date) -> QverisJob:
    return QverisJob(
        f"maintain-symbols-{exchange}-{delisted}",
        EOD_UNIVERSE_TOOL,
        "eodhd",
        "KR",
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


def plan_requests(
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
    sessions = _sessions(policy, today, overrides, plan)
    candidates: list[tuple[QverisJob, date | None, date | None]] = [
        (_rename(daily_job(exchange, dataset, day, today)), day, None)
        for day, exchange in sessions
        for dataset in policy.datasets
    ]
    stale = today - timedelta(days=policy.symbol_list_days)
    candidates += [
        (symbol_list_job(exchange, delisted, today), None, stale)
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
        decision, ask = _decide(found, today, fresh_after=fresh_after)
        if decision == "covered":
            plan.covered += 1
        elif decision == "held":
            plan.held.append({"job_id": job.job_id, "unsettled_fingerprint": cast("RawAsk", ask)
                              .fingerprint})  # fmt: skip
        elif decision == "waiting":
            plan.waiting += 1
        else:
            plan.planned.append(Planned(job, decision, _ledger_dataset(job), day))
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
        if ask.state == "unsettled" and status == "reserved":
            continue  # the cohort was stopped before this page; the next run settles it
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

    def __post_init__(self) -> None:
        from aegis_alpha.data.qveris_contracts import credit_value

        if type(self.max_calls) is not int or self.max_calls < 0:
            raise ValueError("qveris max_calls must be a nonnegative integer")
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
        settled = settle(self.workspace, raw_asks(self.raw_root), clock=self.clock)
        # Asks the cohort never reached hold no intent; they are released, never called.
        for row in self.workspace.state.execute(
            "SELECT a.job_id, a.attempt FROM collection_attempts a JOIN collection_jobs j ON "
            "j.job_id=a.job_id WHERE j.provider=? AND a.status='reserved'",
            (PROVIDER,),
        ).fetchall():
            attempt = ledger.Attempt(str(row[0]), int(row[1]))
            ledger.release(self.workspace.state, attempt, at_us=_us(self.clock()))
            settled["released"] += 1
        return settled

    def calls(self) -> dict[str, int]:
        return {
            "provider_calls": self.admission.paid_executions,
            "http_requests": self.admission.http_requests,
        }


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
    resumed: dict[str, object] | None = None
    if resumable:
        resumed = collect_cohort(
            tuple(resumable), raw_root, session.client(), budget=_resume_budget()
        )
        asks = raw_asks(raw_root)
    before = settle(workspace, asks, clock=clock)
    plan = plan_requests(raw_root, policy, today=today, declarations=declarations, asks=asks)
    report: dict[str, object] = {
        "settled_before": before,
        "resumed": None
        if resumed is None
        else {key: resumed[key] for key in ("requested", "completed", "failed", "stopped")},
        **plan.report(),
    }
    if not plan.planned or caps.max_calls == 0:
        status = "budget_exhausted" if plan.planned else "succeeded"
        return {**report, "status": status, **session.calls()}
    session.reserve(plan.planned, policy.sha256)
    budget = qveris.InvocationBudget(caps.max_calls, credit_value(caps.max_credits))
    # An interrupted process (KeyboardInterrupt, SystemExit) settles nothing here: its
    # started attempts stay started until the next run has resumed their asks.
    try:
        cohort = collect_cohort(
            tuple(item.job for item in plan.planned), raw_root, session.client(), budget=budget
        )
    except Exception:
        session.finish()
        raise
    after = session.finish()
    return {
        **report,
        "status": _status(cohort),
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
    """(FX jobs, daily jobs that need the identity document, KR symbol lists) to import."""
    ready: list[str] = []
    needs_identity: list[str] = []
    symbols: list[str] = []
    for request, items in asks.items():
        tool, _, market, dataset, _ = request
        for ask in items:
            if ask.state not in _TERMINAL:
                continue
            if tool == EOD_UNIVERSE_TOOL and market == "KR":
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
    """Commit the completed daily, FX and KR symbol-list jobs no source holds yet."""
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
