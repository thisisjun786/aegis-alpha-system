"""KR collection into the installation: the rolling OpenDART cohort and KIND listings.

``collect_dart`` runs one bounded OpenDART collection on an admitted writable workspace:

1. settle attempts an interrupted run left (``collection_ledger.recover``);
2. read what is known: every committed ``opendart-*`` receipts table (the legacy imports
   and this collector's batches), the newest KIND lists, and attempts with no answer;
3. spend at most ``max_calls`` and what the 24-hour quota (usage events) leaves, in
   phases: the corp code list when due, the disclosure-list pages each day still lacks
   (a first page adds the pages it counts), then the statement requests the rolling
   cohort (``data.opendart_cohort``) plans with what the earlier phases just learned;
4. commit what it retained as ``opendart-receipts`` content sources.

Each call is a ledger attempt (reserved, started, then succeeded or uncertain). The
response bytes and a canonical receipt (``aas-opendart-receipt-v1``) go to ``raw/``
before the attempt succeeds. A batch document (``aas-opendart-batch-v1``) lists the
receipts of one commit in collection order, so the batch, its receipts and their
responses are one complete unit whose boundary the bytes fix. A run interrupted before
its commit leaves succeeded attempts whose receipts no committed batch lists; the next
run commits them first. Rows keep the receipt's fields as text and the response as
base64, the receipts shape ``dart.fnltt@1``, ``dart.fnltt_filings@1`` and
``dart.corp_codes@1`` read.

``collect_kind`` fetches KIND's KOSPI and KOSDAQ listed-company lists and commits each
answer with its receipt as a ``kind-listings`` content source (``kr_identity.kind_unit``).
"""

from __future__ import annotations

import base64
import hashlib
import json
import time
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data import kind as kind_lists
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.opendart import (
    COMPLETED,
    CORP_CODES,
    FINANCIALS,
    LIST,
    NO_DATA,
    DartRequest,
    DartResponse,
    TransportError,
    canonical,
    classify,
    instant,
    listed_corp_codes,
    parse_list,
    stops_run,
)
from aegis_alpha.data.opendart_cohort import (
    SEOUL,
    CohortPolicy,
    Knowledge,
    Observation,
    Planned,
    financial_window,
    list_gaps,
    plan_corp_codes,
    plan_financials,
    seoul_day,
    summarize,
)
from aegis_alpha.storage import collection_ledger as ledger
from aegis_alpha.storage import source_library_schema as schema
from aegis_alpha.storage.kr_identity import KIND_TABLE, import_unit, kind_unit
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import SourceContent, SourceFile
from aegis_alpha.storage.source_library import list_sources, list_tables

if TYPE_CHECKING:
    import duckdb

    from aegis_alpha.data.opendart import Clock, OpenDartClient, Transport
    from aegis_alpha.storage.workspace import Workspace

DART_PROVIDER: Final = "opendart"
KIND_PROVIDER: Final = "kind"
SHAPE: Final = "receipts"
TABLE: Final = "receipts"
SOURCE_MAJOR: Final = 1
SOURCE_PREFIX: Final = "opendart-"
KIND_PREFIX: Final = "kind-listings-"
RECEIPT_FORMAT: Final = "aas-opendart-receipt-v1"
BATCH_FORMAT: Final = "aas-opendart-batch-v1"
KIND_REQUEST_FORMAT: Final = "aas-kind-request-v1"
COLUMNS: Final = (
    "fingerprint",
    "endpoint",
    "outcome",
    "provider_status",
    "request_json",
    "receipt_json",
    "receipt_sha256",
    "raw_base64",
    "raw_sha256",
    "retrieved_at_utc",
)
# OpenDART allows 20,000 calls a day per key; the collector keeps a margin under it.
DAILY_QUOTA: Final = 19_000
DEFAULT_MAX_CALLS: Final = 2_000
DEFAULT_BATCH: Final = 500
MIN_INTERVAL_SECONDS: Final = 0.5
MAX_TRANSPORT_FAILURES: Final = 3
DATASETS: Final = {
    CORP_CODES: "identity.kr.dart",
    LIST: "filings.kr.dart",
    FINANCIALS: "fundamentals.kr.dart",
}
_DAY_US: Final = 86_400_000_000
_KNOWN_COLUMNS: Final = ("endpoint", "outcome", "request_json", "retrieved_at_utc")
_MAX_RAW_BYTES: Final = 64 * 1024 * 1024


def _us(moment: datetime) -> int:
    return (moment.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def _parse_instant(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.endswith("Z"):
        return None
    try:
        return datetime.fromisoformat(value[:-1]).replace(tzinfo=UTC)
    except ValueError:
        return None


# --- receipts and batches -------------------------------------------------------------------


def receipt_bytes(
    request: DartRequest,
    response: DartResponse,
    *,
    outcome: str,
    provider_status: str | None,
    attempt: ledger.Attempt,
) -> bytes:
    """The canonical receipt of one answered call; the response is named by size and hash."""
    return canonical(
        {
            "schema_version": RECEIPT_FORMAT,
            "request": request.document,
            "fingerprint": request.fingerprint,
            "job_id": attempt.job_id,
            "attempt": attempt.attempt,
            "http_status": response.status,
            "headers": [list(pair) for pair in response.headers],
            "requested_at_utc": instant(response.requested_at),
            "retrieved_at_utc": instant(response.retrieved_at),
            "outcome": outcome,
            "provider_status": provider_status,
            "raw": {"sha256": hashlib.sha256(response.body).hexdigest(),
                    "size": len(response.body)},
        }
    ).encode()  # fmt: skip


@dataclass(frozen=True, slots=True)
class Retained:
    """A receipt and the response it names, both in ``raw/``."""

    receipt: bytes
    response: bytes

    @property
    def document(self) -> dict[str, object]:
        return cast("dict[str, object]", json.loads(self.receipt))

    def row(self) -> tuple[str | None, ...]:
        body = self.document
        request = DartRequest.from_document(body["request"])
        raw = cast("dict[str, object]", body["raw"])
        if raw.get("sha256") != hashlib.sha256(self.response).hexdigest():
            raise ValueError("a retained OpenDART response differs from its receipt")
        status = body.get("provider_status")
        return (
            request.fingerprint,
            request.endpoint,
            cast("str", body["outcome"]),
            status if isinstance(status, str) else None,
            canonical(request.document),
            self.receipt.decode(),
            hashlib.sha256(self.receipt).hexdigest(),
            base64.b64encode(self.response).decode(),
            hashlib.sha256(self.response).hexdigest(),
            cast("str", body["retrieved_at_utc"]),
        )


@dataclass(frozen=True, slots=True)
class Batch:
    """One commit: the batch document, then each receipt and its response."""

    manifest: bytes
    retained: tuple[Retained, ...]

    @classmethod
    def of(cls, retained: Sequence[Retained]) -> Batch:
        manifest = canonical(
            {
                "schema_version": BATCH_FORMAT,
                "receipts": [
                    {"sha256": hashlib.sha256(item.receipt).hexdigest(),
                     "size": len(item.receipt)}
                    for item in retained
                ],
            }
        ).encode()  # fmt: skip
        return cls(manifest, tuple(retained))

    @property
    def content(self) -> SourceContent:
        files = [self.manifest]
        for item in self.retained:
            files.extend((item.receipt, item.response))
        return SourceContent(
            DART_PROVIDER,
            SHAPE,
            SOURCE_MAJOR,
            tuple(SourceFile(hashlib.sha256(raw).hexdigest(), len(raw)) for raw in files),
        )


def commit_batch(workspace: Workspace, batch: Batch) -> dict[str, object]:
    """Retain the batch document and commit its rows as one ``opendart-receipts`` source."""
    import pyarrow as pa  # noqa: PLC0415 -- the legacy extra commits source tables

    from aegis_alpha.storage.source_library import import_content_arrow  # noqa: PLC0415

    put_raw(workspace.paths.raw, batch.manifest)
    for item in batch.retained:
        put_raw(workspace.paths.raw, item.receipt)
        put_raw(workspace.paths.raw, item.response)
    rows = [item.row() for item in batch.retained]
    table = pa.table(
        {name: [row[index] for row in rows] for index, name in enumerate(COLUMNS)},
        schema=pa.schema([(name, pa.string()) for name in COLUMNS]),
    )
    content = batch.content
    result = import_content_arrow(
        workspace,
        content,
        TABLE,
        table.to_reader(),
        lineage={"loader": "aas collect dart", "format": BATCH_FORMAT},
    )
    (committed,) = cast("list[dict[str, object]]", result["tables"])
    return {
        "source_id": content.source_id,
        "rows": committed["rows"],
        "digest": committed["digest"],
        "reused": bool(result.get("reused", False)),
    }


def _read_raw(workspace: Workspace, digest: str) -> bytes:
    with DescriptorTree.open_path(workspace.paths.raw) as tree:
        raw = tree.read_bytes(digest[:2] + "/" + digest, max_bytes=_MAX_RAW_BYTES)
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError(f"raw object {digest} differs from its address")
    return raw


# --- what is known --------------------------------------------------------------------------


def _receipt_tables(
    workspace: Workspace, prefix: str, table: str
) -> Iterator[tuple[str, dict[str, object]]]:
    for source in list_sources(workspace):
        source_id = str(source["source_id"])
        if not source_id.startswith(prefix):
            continue
        for entry in list_tables(workspace, source_id):
            if entry["name"] == table:
                yield str(source["store"]), entry


def _select(
    workspace: Workspace,
    store: str,
    entry: dict[str, object],
    columns: Sequence[str],
    where: str = "",
) -> list[tuple[object, ...]]:
    connection = cast("duckdb.DuckDBPyConnection", schema.connections(workspace)[store])
    names = {*cast("list[str]", entry["columns"]), "_aas_ordinal"}
    selected = ",".join(schema.quoted(name) if name in names else "NULL" for name in columns)
    query = f"SELECT {selected} FROM {schema.quoted(str(entry['target']))} {where}"  # noqa: S608 -- quoted manifest identifiers
    return [tuple(row) for row in connection.execute(query).fetchall()]


@dataclass(slots=True)
class Known:
    """Knowledge for the planner plus the receipt hashes committed batches already hold."""

    knowledge: Knowledge = field(default_factory=Knowledge)
    committed_receipts: set[str] = field(default_factory=set)
    sources: int = 0
    rows: int = 0
    unreadable: int = 0


def observe_row(knowledge: Knowledge, row: Sequence[object], raw: bytes | None) -> bool:
    """Add one receipts row (endpoint, outcome, request JSON, retrieval instant).

    False when the row names no request or retrieval instant it can be read by.
    """
    endpoint, outcome, request_json, retrieved = row
    at = _parse_instant(retrieved)
    if at is None or not isinstance(request_json, str) or outcome not in {COMPLETED, NO_DATA,
                                                                          "FAILED"}:  # fmt: skip
        return False
    try:
        request = DartRequest.from_document(json.loads(request_json))
    except (ValueError, TypeError):
        return False
    if request.endpoint != endpoint:
        return False
    total = None
    if request.endpoint == LIST and outcome == COMPLETED and raw is not None:
        page = parse_list(raw)
        total = page.total_pages
        knowledge.file(page.filings)
    if request.endpoint == CORP_CODES and outcome == COMPLETED and raw is not None:
        knowledge.listed(listed_corp_codes(raw), at)
    knowledge.observe(Observation(request, cast("str", outcome), at, total))
    return True


def load_known(workspace: Workspace) -> Known:
    """Read every committed OpenDART receipts table, the newest KIND lists and the ledger."""
    known = Known()
    newest_corp_codes: tuple[datetime, str, dict[str, object], int] | None = None
    for store, entry in _receipt_tables(workspace, SOURCE_PREFIX, TABLE):
        known.sources += 1
        columns = (*_KNOWN_COLUMNS, "receipt_sha256", "_aas_ordinal")
        rows = _select(workspace, store, entry, columns)
        for endpoint, outcome, request_json, retrieved, receipt, ordinal in rows:
            known.rows += 1
            if isinstance(receipt, str):
                known.committed_receipts.add(receipt)
            if endpoint == LIST:
                continue  # read with its page below
            ok = observe_row(known.knowledge, (endpoint, outcome, request_json, retrieved), None)
            known.unreadable += not ok
            at = _parse_instant(retrieved)
            if ok and endpoint == CORP_CODES and outcome == COMPLETED and at is not None and (
                newest_corp_codes is None or at > newest_corp_codes[0]
            ):  # fmt: skip
                newest_corp_codes = (at, store, entry, cast("int", ordinal))
        pages = _select(
            workspace, store, entry, (*_KNOWN_COLUMNS, "raw_base64"), "WHERE endpoint='list'"
        )
        for *fields, encoded in pages:
            raw = base64.b64decode(encoded) if isinstance(encoded, str) else None
            ok = observe_row(known.knowledge, fields, raw)
            known.unreadable += not ok
    if newest_corp_codes is not None:
        at, store, entry, ordinal = newest_corp_codes
        ((encoded,),) = _select(
            workspace, store, entry, ("raw_base64",), f"WHERE _aas_ordinal={int(ordinal)}"
        )
        known.knowledge.listed(listed_corp_codes(base64.b64decode(cast("str", encoded))), at)
    known.knowledge.kind_codes = kind_codes(workspace)
    for fingerprint, at_us in ledger.unanswered(workspace.state, DART_PROVIDER).items():
        known.knowledge.unanswered[fingerprint] = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
            microseconds=at_us
        )
    return known


def kind_codes(workspace: Workspace) -> frozenset[str] | None:
    """Short codes of the newest committed KOSPI and KOSDAQ lists, None unless both exist."""
    newest: dict[str, tuple[datetime, set[str]]] = {}
    for store, entry in _receipt_tables(workspace, KIND_PREFIX, KIND_TABLE):
        rows = _select(workspace, store, entry, ("list_id", "short_code", "retrieved_at_utc"))
        lists: dict[tuple[str, datetime], set[str]] = {}
        for list_id, code, retrieved in rows:
            at = _parse_instant(retrieved)
            if isinstance(list_id, str) and isinstance(code, str) and at is not None:
                lists.setdefault((list_id, at), set()).add(code)
        for (list_id, at), codes in lists.items():
            if list_id not in newest or at > newest[list_id][0]:
                newest[list_id] = (at, codes)
    if set(newest) != set(kind_lists.LISTS):
        return None
    return frozenset().union(*(codes for _, codes in newest.values()))


def orphans(workspace: Workspace, committed: set[str]) -> list[Retained]:
    """Receipts succeeded attempts retained that no committed batch lists, oldest first."""
    found: list[Retained] = []
    for digest in ledger.charged_receipts(workspace.state, DART_PROVIDER):
        if digest in committed:
            continue
        receipt = _read_raw(workspace, digest)
        raw = cast("dict[str, object]", json.loads(receipt)["raw"])
        found.append(Retained(receipt, _read_raw(workspace, cast("str", raw["sha256"]))))
    return found


# --- one run ---------------------------------------------------------------------------------


@dataclass(slots=True)
class Run:
    workspace: Workspace
    client: OpenDartClient
    policy: CohortPolicy
    clock: Clock
    sleep: Callable[[float], None]
    budget: int
    batch_size: int
    min_interval: float
    knowledge: Knowledge
    pending: list[Retained] = field(default_factory=list)
    committed: list[dict[str, object]] = field(default_factory=list)
    outcomes: Counter[str] = field(default_factory=Counter)
    asked: Counter[str] = field(default_factory=Counter)
    calls: int = 0
    uncertain: int = 0
    stopped: str | None = None
    _failures: int = 0
    _last_call: float | None = None

    def flush(self, *, final: bool = False) -> None:
        while self.pending and (final or len(self.pending) >= self.batch_size):
            chunk, self.pending = (
                self.pending[: self.batch_size],
                self.pending[self.batch_size :],
            )
            self.committed.append(commit_batch(self.workspace, Batch.of(chunk)))

    def _now_us(self) -> int:
        return _us(self.clock())

    def _job(self, request: DartRequest) -> ledger.Job:
        window = financial_window(request)
        if request.endpoint == LIST:
            stamp = request.parameters["bgn_de"]
            day = date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:]))
            window = (day, day)
        bounds = (None, None)
        if window is not None:
            start = datetime(window[0].year, window[0].month, window[0].day, tzinfo=SEOUL)
            end = datetime(window[1].year, window[1].month, window[1].day, tzinfo=SEOUL)
            bounds = (_us(start), _us(end + timedelta(days=1)) - 1)
        return ledger.Job(
            DART_PROVIDER,
            DATASETS[request.endpoint],
            request.fingerprint,
            self.policy.sha256,
            *bounds,
        )

    def _pace(self) -> None:
        if self._last_call is not None:
            wait = self.min_interval - (time.monotonic() - self._last_call)
            if wait > 0:
                self.sleep(wait)
        self._last_call = time.monotonic()

    def ask(self, planned: Planned) -> DartResponse | None:
        """One ledgered call; None when the run is out of budget or stopped."""
        if self.stopped is not None:
            return None
        if self.calls >= self.budget:
            self.stopped = "budget"
            return None
        request = planned.request
        state = self.workspace.state
        attempt = ledger.reserve(state, self._job(request), at_us=self._now_us())
        self._pace()
        ledger.start(state, attempt, at_us=self._now_us())
        self.calls += 1
        self.asked[planned.reason] += 1
        try:
            response = self.client.request(request)
        except TransportError:
            ledger.uncertain(state, attempt, at_us=self._now_us())
            self.uncertain += 1
            self.knowledge.unanswered[request.fingerprint] = self.clock()
            self._failures += 1
            if self._failures >= MAX_TRANSPORT_FAILURES:
                self.stopped = "transport_failures"
            return None
        self._failures = 0
        outcome, status = classify(request, response)
        receipt = receipt_bytes(
            request, response, outcome=outcome, provider_status=status, attempt=attempt
        )
        put_raw(self.workspace.paths.raw, response.body)
        put_raw(self.workspace.paths.raw, receipt)
        ledger.succeed(
            state,
            attempt,
            receipt_sha256=hashlib.sha256(receipt).hexdigest(),
            outcome=outcome,
            at_us=self._now_us(),
        )
        self.outcomes[outcome] += 1
        observe_row(
            self.knowledge,
            (request.endpoint, outcome, canonical(request.document),
             instant(response.retrieved_at)),
            response.body if outcome == COMPLETED else None,
        )  # fmt: skip
        self.pending.append(Retained(receipt, response.body))
        self.flush()
        if stops_run(response):
            self.stopped = f"provider_refused:{status or response.status}"
        return response

    def ask_all(self, planned: Sequence[Planned]) -> None:
        for item in planned:
            if self.stopped is not None:
                return
            self.ask(item)

    def ask_list(self, planned: Sequence[Planned]) -> None:
        queue = list(planned)
        while queue and self.stopped is None:
            item = queue.pop(0)
            response = self.ask(item)
            parameters = item.request.parameters
            if response is None or parameters["page_no"] != "1":
                continue
            if classify(item.request, response)[0] != COMPLETED:
                continue
            day = date(
                int(parameters["bgn_de"][:4]),
                int(parameters["bgn_de"][4:6]),
                int(parameters["bgn_de"][6:]),
            )
            queue[:0] = [
                Planned(DartRequest.list_page(day, page), "list_page")
                for page in range(2, parse_list(response.body).total_pages + 1)
            ]


def collect_dart(  # noqa: PLR0913 -- every bound of one run is explicit
    workspace: Workspace,
    client: OpenDartClient,
    *,
    policy: CohortPolicy | None = None,
    clock: Clock,
    sleep: Callable[[float], None] = time.sleep,
    max_calls: int = DEFAULT_MAX_CALLS,
    daily_quota: int = DAILY_QUOTA,
    batch_size: int = DEFAULT_BATCH,
    min_interval: float = MIN_INTERVAL_SECONDS,
) -> dict[str, object]:
    """One bounded OpenDART collection; see the module documentation for the phases."""
    policy = policy or CohortPolicy()
    for name, value in (("max_calls", max_calls), ("daily_quota", daily_quota),
                        ("batch_size", batch_size)):  # fmt: skip
        if type(value) is not int or value < (0 if name == "max_calls" else 1):
            raise ValueError(f"{name} must be a positive integer")
    now = clock()
    settled = ledger.recover(workspace.state, DART_PROVIDER, at_us=_us(now))
    known = load_known(workspace)
    used = ledger.used(workspace.state, DART_PROVIDER, since_us=_us(now) - _DAY_US)
    run = Run(
        workspace,
        client,
        policy,
        clock,
        sleep,
        max(0, min(max_calls, daily_quota - used)),
        batch_size,
        min_interval,
        known.knowledge,
    )
    run.pending.extend(orphans(workspace, known.committed_receipts))
    recovered = len(run.pending)
    today = seoul_day(now)
    run.ask_all(plan_corp_codes(run.knowledge, today, policy))
    run.ask_list(list_gaps(run.knowledge, today, policy))
    planned = plan_financials(run.knowledge, today, policy)
    run.ask_all(planned)
    run.flush(final=True)
    remaining = list_gaps(run.knowledge, today, policy) + plan_financials(
        run.knowledge, today, policy
    )
    return {
        "provider": DART_PROVIDER,
        "seoul_date": today.isoformat(),
        "policy_sha256": policy.sha256,
        "recovered_attempts": settled,
        "recovered_receipts": recovered,
        "known": {
            "sources": known.sources,
            "rows": known.rows,
            "unreadable_rows": known.unreadable,
            "listed_corps": len(run.knowledge.corps),
            "kind_filter": run.knowledge.kind_codes is not None,
        },
        "quota": {"daily": daily_quota, "used_before": used, "budget": run.budget},
        "planned_statements": summarize(planned),
        "provider_calls": run.calls,
        "asked": dict(sorted(run.asked.items())),
        "outcomes": dict(sorted(run.outcomes.items())),
        "uncertain": run.uncertain,
        "stopped": run.stopped,
        "pending": summarize(remaining),
        "sources": run.committed,
    }


def plan_dart(workspace: Workspace, *, today: date, policy: CohortPolicy) -> dict[str, object]:
    """What a run on ``today`` would ask first, from committed knowledge; writes nothing."""
    known = load_known(workspace)
    return plan_report(known.knowledge, today=today, policy=policy) | {
        "known": {
            "sources": known.sources,
            "rows": known.rows,
            "unreadable_rows": known.unreadable,
            "uncommitted_receipts": len(
                [
                    digest
                    for digest in ledger.charged_receipts(workspace.state, DART_PROVIDER)
                    if digest not in known.committed_receipts
                ]
            ),
        }
    }


def plan_report(knowledge: Knowledge, *, today: date, policy: CohortPolicy) -> dict[str, object]:
    corp_codes = plan_corp_codes(knowledge, today, policy)
    pages = list_gaps(knowledge, today, policy)
    statements = plan_financials(knowledge, today, policy)
    return {
        "mode": "plan",
        "provider_calls": 0,
        "seoul_date": today.isoformat(),
        "policy_sha256": policy.sha256,
        "listed_corps": len(knowledge.corps),
        "kind_filter": knowledge.kind_codes is not None,
        "corp_codes_due": bool(corp_codes),
        "list_pages_due": len(pages),
        "list_days_due": len({item.request.parameters["bgn_de"] for item in pages}),
        "statements": summarize(statements),
        "statement_requests": len(statements),
    }


# --- KIND -------------------------------------------------------------------------------------


def _kind_job(list_id: str) -> ledger.Job:
    fingerprint = hashlib.sha256(canonical([KIND_REQUEST_FORMAT, list_id]).encode()).hexdigest()
    policy = hashlib.sha256(canonical([KIND_REQUEST_FORMAT, "on-demand"]).encode()).hexdigest()
    return ledger.Job(KIND_PROVIDER, "identity.kr.kind", fingerprint, policy)


def collect_kind(
    workspace: Workspace,
    transport: Transport,
    *,
    clock: Clock,
    lists: Sequence[str] = kind_lists.LISTS,
) -> dict[str, object]:
    """Fetch KIND lists and commit each answer as a ``kind-listings`` content source."""
    state = workspace.state
    settled = ledger.recover(state, KIND_PROVIDER, at_us=_us(clock()))
    results: list[dict[str, object]] = []
    for list_id in lists:
        attempt = ledger.reserve(state, _kind_job(list_id), at_us=_us(clock()))
        ledger.start(state, attempt, at_us=_us(clock()))
        try:
            response = kind_lists.fetch(list_id, transport, clock)
        except TransportError:
            ledger.uncertain(state, attempt, at_us=_us(clock()))
            results.append({"list": list_id, "status": "uncertain"})
            continue
        receipt = response.receipt()
        put_raw(workspace.paths.raw, response.answer.body)
        put_raw(workspace.paths.raw, receipt)
        entry: dict[str, object] = {"list": list_id, "http_status": response.answer.status}
        unit = None
        if response.answer.status != 200:  # noqa: PLR2004 -- HTTP OK
            entry["status"] = "failed"
        else:
            try:
                unit = kind_unit(receipt, response.answer.body)
            except ValueError as error:
                entry |= {"status": "refused", "reason": str(error)}
        ledger.succeed(
            state,
            attempt,
            receipt_sha256=hashlib.sha256(receipt).hexdigest(),
            outcome="FAILED" if unit is None else COMPLETED,
            at_us=_us(clock()),
        )
        if unit is not None:
            entry |= {"status": "committed", **import_unit(workspace, unit)}
        results.append(entry)
    return {
        "provider": KIND_PROVIDER,
        "provider_calls": len(results),
        "recovered_attempts": settled,
        "lists": results,
    }
