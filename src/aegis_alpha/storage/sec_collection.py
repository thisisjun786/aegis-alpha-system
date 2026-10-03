"""SEC EDGAR collection into the installation (``aas collect sec``).

``collect_sec`` runs one bounded collection on an admitted writable workspace:

1. settle attempts an interrupted run left (``collection_ledger.recover``) and commit the
   receipts it retained but did not commit;
2. read what is known (``load_known``) from this collector's committed batches: the index
   days answered, the filings those indexes name, the filings submissions answers listed
   and the filings companyfacts answers reported facts of;
3. read every uncovered weekday's daily index up to yesterday (New York), then ask each
   filer's submissions document for its wanted filings that are not listed yet, and its
   companyfacts for its wanted reports that have no facts yet (``data.sec_collect``);
4. commit the batch as content sources of its files:

   - ``sec-collect-receipts``: every receipt, with the selection a document ask recorded;
   - ``sec-daily-index-entries``: every line of every answered index;
   - ``sec-submissions-filings``: the wanted filings each submissions document lists, in
     the columns of ``sec.submissions_filings@1``, which ``sec.submissions@1`` reads;
   - ``sec-companyfacts-facts``: the facts of the wanted reports each companyfacts answer
     holds, the columns ``sec.companyfacts@1`` reads plus SEC's ``fy``, ``fp``, ``frame``.

A document restates every earlier filing of its filer, so only the rows of the filings the
ask wanted become rows; the receipt records that selection and the bytes stay in ``raw/``.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data import sec_collect as sec
from aegis_alpha.data.provider_request import COMPLETED, FAILED, Request, Response
from aegis_alpha.storage import collection_ledger as ledger
from aegis_alpha.storage import provider_collection as collection
from aegis_alpha.storage.identity import mint_issuer
from aegis_alpha.storage.legacy_import.public import (
    SUBMISSION_FILINGS,
    filer_arrays,
    filing_rows,
)

if TYPE_CHECKING:
    import pyarrow as pa

    from aegis_alpha.data.opendart import Clock
    from aegis_alpha.storage.workspace import Workspace

PROVIDER: Final = sec.PROVIDER
LOADER: Final = "aas collect sec"
ENTRIES_SHAPE: Final = "daily-index-entries"
ENTRIES_TABLE: Final = "entries"
FILINGS_SHAPE: Final = SUBMISSION_FILINGS.shape
FILINGS_TABLE: Final = SUBMISSION_FILINGS.name
FACTS_SHAPE: Final = "companyfacts-facts"
FACTS_TABLE: Final = "facts"
DEFAULT_MAX_CALLS: Final = 2_000
# SEC's fair-access limit is ten requests a second.
MIN_INTERVAL_SECONDS: Final = 0.125
_ENTRY_COLUMNS: Final = (
    ("day", "date"),
    ("line", "string"),
    ("cik", "string"),
    ("company", "string"),
    ("form", "string"),
    ("date_filed", "date"),
    ("accession", "string"),
    ("retrieved_at_utc", "string"),
    ("receipt_sha256", "string"),
)
_FACT_COLUMNS: Final = (
    ("cik", "string"),
    ("taxonomy", "string"),
    ("tag", "string"),
    ("unit", "string"),
    ("period_start", "date"),
    ("period_end", "date"),
    ("accession_number", "string"),
    ("form", "string"),
    ("filed", "date"),
    ("value", "string"),
    ("fy", "string"),
    ("fp", "string"),
    ("frame", "string"),
    ("retrieved_at", "timestamp"),
    ("receipt_sha256", "string"),
)


def _schema(columns: tuple[tuple[str, str], ...]) -> pa.Schema:
    import pyarrow as pa  # noqa: PLC0415 -- the legacy extra commits source tables

    kinds = {"string": pa.string(), "date": pa.date32(),
             "timestamp": pa.timestamp("us", tz="UTC")}  # fmt: skip
    return pa.schema([(name, kinds[kind]) for name, kind in columns])


def _table(schema: pa.Schema, rows: list[tuple[object, ...]]) -> pa.Table:
    import pyarrow as pa  # noqa: PLC0415 -- the legacy extra commits source tables

    columns = list(zip(*rows, strict=True)) if rows else [() for _ in schema]
    return pa.table(
        {name: list(values) for name, values in zip(schema.names, columns, strict=True)},
        schema=schema,
    )


def _wanted(item: collection.Retained) -> frozenset[str]:
    selection = item.selection
    accessions = selection.get("accessions") if isinstance(selection, dict) else None
    if not isinstance(accessions, list) or not all(isinstance(a, str) for a in accessions):
        raise ValueError("a document receipt records the accessions it wants")
    return frozenset(cast("list[str]", accessions))


def submission_rows(item: collection.Retained) -> list[tuple[object, ...]]:
    """The wanted filings a submissions answer lists, as ``sec.submissions_filings@1`` rows."""
    cik = item.request.parameters["cik"]
    member = f"CIK{cik}.json"
    document = sec.submissions_document(item.response, cik)
    arrays = filer_arrays(document, member, cik, {})
    wanted = _wanted(item)
    position = [name for name, _ in SUBMISSION_FILINGS.columns].index("accessionNumber")
    return [row for row in filing_rows(arrays, member, cik) if row[position] in wanted]


def fact_rows(item: collection.Retained) -> list[tuple[object, ...]]:
    """The facts of the wanted reports a companyfacts answer holds, in SEC's order."""
    cik = item.request.parameters["cik"]
    wanted = _wanted(item)
    retrieved = item.retrieved_at
    return [
        (cik, fact.taxonomy, fact.tag, fact.unit, fact.start, fact.end, fact.accession,
         fact.form, fact.filed, fact.value, fact.fy, fact.fp, fact.frame, retrieved, item.sha256)
        for fact in sec.companyfacts_facts(item.response, cik)
        if fact.accession in wanted
    ]  # fmt: skip


def entry_rows(item: collection.Retained) -> list[tuple[object, ...]]:
    day = sec.request_day(item.request)
    retrieved = cast("str", item.document["retrieved_at_utc"])
    return [
        (day, line.line, line.cik, line.company, line.form, line.filed, line.accession,
         retrieved, item.sha256)
        for line in sec.parse_index(item.response)
    ]  # fmt: skip


def commit_sec_batch(workspace: Workspace, batch: collection.Batch) -> list[dict[str, object]]:
    entries: list[tuple[object, ...]] = []
    filings: list[tuple[object, ...]] = []
    facts: list[tuple[object, ...]] = []
    for item in batch.retained:
        if item.outcome != COMPLETED:
            continue
        endpoint = item.request.endpoint
        if endpoint == sec.DAILY_INDEX:
            entries.extend(entry_rows(item))
        elif endpoint == sec.SUBMISSIONS:
            filings.extend(submission_rows(item))
        else:
            facts.extend(fact_rows(item))
    return collection.commit_batch(
        workspace,
        batch,
        [
            (ENTRIES_SHAPE, ENTRIES_TABLE, _table(_schema(_ENTRY_COLUMNS), entries)),
            (FILINGS_SHAPE, FILINGS_TABLE, _table(SUBMISSION_FILINGS.schema(), filings)),
            (FACTS_SHAPE, FACTS_TABLE, _table(_schema(_FACT_COLUMNS), facts)),
        ],
        loader=LOADER,
    )


def classify(request: Request, response: Response) -> tuple[str, str | None]:
    """``sec.classify``, and a submissions answer is readable only if its rows are."""
    outcome, status = sec.classify(request, response)
    if outcome == COMPLETED and request.endpoint == sec.SUBMISSIONS:
        cik = request.parameters["cik"]
        member = f"CIK{cik}.json"
        try:
            filing_rows(filer_arrays(sec.submissions_document(response.body, cik), member,
                                     cik, {}), member, cik)  # fmt: skip
        except ValueError as error:
            return FAILED, f"unreadable answer: {error}"
    return outcome, status


# --- what is known ------------------------------------------------------------------------------


@dataclass(slots=True)
class Known:
    knowledge: sec.SecKnowledge = field(default_factory=sec.SecKnowledge)
    committed_receipts: set[str] = field(default_factory=set)
    batches: int = 0
    receipt_rows: int = 0


def _distinct(
    workspace: Workspace, prefix: str, name: str, column: str, batches: set[str]
) -> set[str]:
    found: set[str] = set()
    for table in collection.committed_tables(workspace, prefix):
        if table.entry["name"] == name and table.hex in batches:
            rows = collection.select(workspace, table, (column,))
            found.update(value for (value,) in rows if isinstance(value, str))
    return found


def _read_receipts(workspace: Workspace, known: Known) -> set[str]:
    """Index answers and document asks from committed receipts; the batches they are in."""
    batches: set[str] = set()
    for table, row in collection.receipt_rows(workspace, PROVIDER):
        batches.add(table.hex)
        known.receipt_rows += 1
        known.committed_receipts.add(cast("str", row[8]))
        endpoint, request_json, outcome, retrieved = row[1], row[2], row[3], row[11]
        at = collection.parse_instant(retrieved)
        if at is None:
            continue
        request = Request.from_document(PROVIDER, json.loads(cast("str", request_json)))
        if endpoint == sec.DAILY_INDEX:
            known.knowledge.index(sec.request_day(request), cast("str", outcome), at)
        else:
            known.knowledge.ask(cast("str", endpoint), request.parameters["cik"], at)
    return batches


def _read_entries(workspace: Workspace, knowledge: sec.SecKnowledge, batches: set[str]) -> None:
    columns = ("cik", "company", "form", "date_filed", "accession", "retrieved_at_utc")
    for table in collection.committed_tables(workspace, f"{PROVIDER}-{ENTRIES_SHAPE}-"):
        if table.entry["name"] != ENTRIES_TABLE or table.hex not in batches:
            continue
        rows = collection.select(workspace, table, columns, "WHERE accession IS NOT NULL")
        for cik, company, form, filed, accession, retrieved in rows:
            at = collection.parse_instant(retrieved)
            if at is not None:
                line = sec.IndexLine(
                    "", cast("str", cik), cast("str", company), cast("str", form),
                    cast("date", filed), cast("str", accession),
                )  # fmt: skip
                knowledge.file(line, at)


def load_known(workspace: Workspace) -> Known:
    """Index days, indexed filings and what was listed and reported, from this collector."""
    known = Known()
    knowledge = known.knowledge
    batches = _read_receipts(workspace, known)
    known.batches = len(batches)
    _read_entries(workspace, knowledge, batches)
    knowledge.listed = _distinct(
        workspace, f"{PROVIDER}-{FILINGS_SHAPE}-", FILINGS_TABLE, "accessionNumber", batches
    )
    knowledge.reported = _distinct(
        workspace, f"{PROVIDER}-{FACTS_SHAPE}-", FACTS_TABLE, "accession_number", batches
    )
    unanswered = ledger.unanswered(workspace.state, PROVIDER)
    for cik in {filing.cik for filing in knowledge.filings.values()} if unanswered else ():
        for endpoint, make in ((sec.SUBMISSIONS, sec.submissions),
                               (sec.COMPANYFACTS, sec.companyfacts)):  # fmt: skip
            at_us = unanswered.get(make(cik).fingerprint)
            if at_us is not None:
                knowledge.ask(endpoint, cik, collection.from_us(at_us))
    return known


def universe(workspace: Workspace, policy: sec.SecPolicy) -> Callable[[str], bool]:
    """Whether a CIK is collected: every CIK, or those whose SEC issuer is registered."""
    if policy.issuers == "all":
        return lambda _: True
    registered = {str(row[0]) for row in workspace.state.execute("SELECT issuer_id FROM issuers")}
    cache: dict[str, bool] = {}

    def contains(cik: str) -> bool:
        if cik not in cache:
            cache[cik] = mint_issuer("sec_cik", cik) in registered
        return cache[cik]

    return contains


# --- one run ------------------------------------------------------------------------------------


@dataclass(slots=True)
class Run:
    workspace: Workspace
    caller: collection.Caller
    knowledge: sec.SecKnowledge
    batch_size: int
    batch_bytes: int
    pending: list[collection.Retained] = field(default_factory=list)
    committed: list[dict[str, object]] = field(default_factory=list)

    def flush(self, *, final: bool = False) -> None:
        size = sum(len(item.response) for item in self.pending)
        if self.pending and (
            final or len(self.pending) >= self.batch_size or size >= self.batch_bytes
        ):
            batch = collection.Batch.of(PROVIDER, self.pending)
            self.pending = []
            self.committed.extend(commit_sec_batch(self.workspace, batch))

    def ask(self, planned: sec.Planned) -> None:
        selection = {"accessions": list(planned.wanted)} if planned.wanted else None
        answered = self.caller.ask(planned.request, planned.reason, selection)
        if answered is None:
            return
        retained, outcome, response = answered
        self.pending.append(retained)
        request, at = planned.request, response.retrieved_at
        if request.endpoint == sec.DAILY_INDEX:
            self.knowledge.index(sec.request_day(request), outcome, at)
            if outcome == COMPLETED:
                for line in sec.parse_index(response.body):
                    self.knowledge.file(line, at)
        else:
            self.knowledge.ask(request.endpoint, request.parameters["cik"], at)
            if outcome == COMPLETED:
                done = (
                    {row[2] for row in submission_rows(retained)}
                    if request.endpoint == sec.SUBMISSIONS
                    else {row[6] for row in fact_rows(retained)}
                )
                target = (
                    self.knowledge.listed
                    if request.endpoint == sec.SUBMISSIONS
                    else self.knowledge.reported
                )
                target.update(cast("set[str]", done))
        self.flush()


def collect_sec(  # noqa: PLR0913 -- every bound of one run is explicit
    workspace: Workspace,
    client: sec.SecClient,
    *,
    policy: sec.SecPolicy | None = None,
    clock: Clock,
    since: date | None = None,
    sleep: Callable[[float], None] = time.sleep,
    max_calls: int = DEFAULT_MAX_CALLS,
    batch_size: int = collection.DEFAULT_BATCH,
    batch_bytes: int = collection.DEFAULT_BATCH_BYTES,
    min_interval: float = MIN_INTERVAL_SECONDS,
) -> dict[str, object]:
    """One bounded SEC collection; see the module documentation for the phases."""
    policy = policy or sec.SecPolicy()
    for name, value in (("max_calls", max_calls), ("batch_size", batch_size),
                        ("batch_bytes", batch_bytes)):  # fmt: skip
        if type(value) is not int or value < (0 if name == "max_calls" else 1):
            raise ValueError(f"{name} must be a positive integer")
    now = clock()
    settled = ledger.recover(workspace.state, PROVIDER, at_us=collection.epoch_us(now))
    before = load_known(workspace)
    recovered = collection.orphans(workspace, PROVIDER, before.committed_receipts)
    committed: list[dict[str, object]] = []
    if recovered:
        committed.extend(commit_sec_batch(workspace, collection.Batch.of(PROVIDER, recovered)))
    known = load_known(workspace) if recovered else before
    caller = collection.Caller(
        workspace, PROVIDER, policy.sha256, client.request, classify, sec.stops_run,
        lambda request: sec.DATASETS[request.endpoint], clock, sleep, max_calls, min_interval,
    )  # fmt: skip
    run = Run(workspace, caller, known.knowledge, batch_size, batch_bytes, committed=committed)
    today = sec.edgar_day(now)
    for planned in sec.plan_indexes(run.knowledge, today, policy, since):
        run.ask(planned)
    contains = universe(workspace, policy)
    filings, facts, _ = sec.plan_documents(run.knowledge, clock(), policy, contains)
    for planned in (*filings, *facts):
        if caller.stopped is not None:
            break
        run.ask(planned)
    run.flush(final=True)
    remaining = _plan_report(
        run.knowledge, today=today, now=clock(), policy=policy, contains=contains, since=since
    )
    return {
        "provider": PROVIDER,
        "edgar_date": today.isoformat(),
        "policy_sha256": policy.sha256,
        "recovered_attempts": settled,
        "recovered_receipts": len(recovered),
        "known": {"batches": known.batches, "receipt_rows": known.receipt_rows},
        "provider_calls": caller.calls,
        "asked": dict(sorted(caller.asked.items())),
        "outcomes": dict(sorted(caller.outcomes.items())),
        "uncertain": caller.uncertain,
        "stopped": caller.stopped,
        "covered_through": _covered_through(run.knowledge, today, policy, since),
        "pending": remaining,
        "sources": run.committed,
    }


def _covered_through(
    knowledge: sec.SecKnowledge, today: date, policy: sec.SecPolicy, since: date | None
) -> str | None:
    """The last weekday before which every weekday since the first one is covered."""
    start = since if since is not None else sec.first_day(knowledge, today, policy)
    through = None
    for day in sec.weekdays(start, today - timedelta(days=1)):
        if not sec.covered(knowledge, day):
            break
        through = day.isoformat()
    return through


def _plan_report(  # noqa: PLR0913 -- the inputs of one plan
    knowledge: sec.SecKnowledge,
    *,
    today: date,
    now: datetime,
    policy: sec.SecPolicy,
    contains: Callable[[str], bool],
    since: date | None,
) -> dict[str, object]:
    indexes = sec.plan_indexes(knowledge, today, policy, since)
    filings, facts, counts = sec.plan_documents(knowledge, now, policy, contains)
    return {
        "index_days": len(indexes),
        "requests": sec.summarize([*indexes, *filings, *facts]),
        "filings": counts,
    }


def plan_sec(
    workspace: Workspace,
    *,
    now: datetime,
    policy: sec.SecPolicy,
    since: date | None = None,
) -> dict[str, object]:
    """What a run at ``now`` would ask first, from committed answers; writes nothing.

    Documents are planned from the indexes already committed; the indexes the run reads
    first add their filings to that plan.
    """
    known = load_known(workspace)
    today = sec.edgar_day(now)
    uncommitted = [
        digest
        for digest in ledger.charged_receipts(workspace.state, PROVIDER)
        if digest not in known.committed_receipts
    ]
    contains = universe(workspace, policy)
    return {
        "mode": "plan",
        "provider_calls": 0,
        "edgar_date": today.isoformat(),
        "policy_sha256": policy.sha256,
        "known": {
            "batches": known.batches,
            "receipt_rows": known.receipt_rows,
            "index_days": len(known.knowledge.days),
            "indexed_filings": len(known.knowledge.filings),
            "uncommitted_receipts": len(uncommitted),
        },
        "first_day": (since or sec.first_day(known.knowledge, today, policy)).isoformat(),
        "covered_through": _covered_through(known.knowledge, today, policy, since),
        **_plan_report(
            known.knowledge, today=today, now=now, policy=policy, contains=contains, since=since
        ),
    }
