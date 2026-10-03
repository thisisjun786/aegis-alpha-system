"""FRED/ALFRED and FRED CSV collection into the installation (``aas collect fred``).

``collect_fred`` runs one bounded collection on an admitted writable workspace:

1. settle attempts an interrupted run left (``collection_ledger.recover``) and commit the
   receipts it retained but did not commit;
2. read what is known (``load_known``): each series' latest collected vintage day from
   every committed table ``fred.alfred@1`` reads, counting only vintages that started
   before the FRED day they were retrieved on, and each CSV series' latest download day;
3. download each CSV series once a FRED day (``series_csv``), then for each ALFRED series
   ask from the origin when nothing is known, or ask ``vintage_dates`` after the known day
   and, when a vintage exists, every page of the observations query from the known day
   to the last ended FRED day (``data.fred_collect``);
4. commit the batch: a ``fred-collect-receipts`` table of every receipt and a
   ``fred-alfred-observations`` table of the rows of every complete query that are
   vintages as FRED dated them; and each completed CSV download as a ``fred-series-csv``
   source of its bytes alone, the shape ``fred.series_csv@1`` imports and
   ``fred.fx_series@1`` reads, so the same bytes are the same source.

A query is complete when every page from offset 0 answered ``COMPLETED`` with the same
count and the pages hold that many rows. Rows of an incomplete query are not committed;
the next run asks the query again from the same known day.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data import fred_collect as fred
from aegis_alpha.data.provider_request import COMPLETED, Request
from aegis_alpha.storage import collection_ledger as ledger
from aegis_alpha.storage import provider_collection as collection
from aegis_alpha.storage import source_library_schema as schema
from aegis_alpha.storage.legacy_import.loaders import csv_table
from aegis_alpha.storage.legacy_import.public import FRED as SERIES_CSV_TABLE
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import SourceContent, SourceFile

if TYPE_CHECKING:
    import pyarrow as pa

    from aegis_alpha.data.opendart import Clock
    from aegis_alpha.storage.workspace import Workspace

PROVIDER: Final = fred.PROVIDER
OBSERVATIONS_SHAPE: Final = "alfred-observations"
OBSERVATIONS_TABLE: Final = "observations"
LOADER: Final = "aas collect fred"
DEFAULT_MAX_CALLS: Final = 500
# A batch's rows are built in memory, so a batch closes at the end of the window that
# brings its answers past this many bytes.
BATCH_BYTES: Final = 64 * 1024 * 1024
# FRED allows 120 requests a minute per key.
MIN_INTERVAL_SECONDS: Final = 0.5
ALFRED_COLUMNS: Final[dict[str, str]] = {
    "series_id": "VARCHAR",
    "observation_date": "DATE",
    "realtime_start": "DATE",
    "value": "VARCHAR",
    "retrieved_at_utc": "TIMESTAMP WITH TIME ZONE",
}
_ROW_COLUMNS: Final = (
    ("series_id", "string"),
    ("observation_date", "date"),
    ("realtime_start", "date"),
    ("realtime_end", "date"),
    ("value", "string"),
    ("query_realtime_start", "date"),
    ("query_realtime_end", "date"),
    ("retrieved_at_utc", "timestamp"),
    ("receipt_sha256", "string"),
)


def observations_schema() -> pa.Schema:
    import pyarrow as pa  # noqa: PLC0415 -- the legacy extra commits source tables

    kinds = {"string": pa.string(), "date": pa.date32(),
             "timestamp": pa.timestamp("us", tz="UTC")}  # fmt: skip
    return pa.schema([(name, kinds[kind]) for name, kind in _ROW_COLUMNS])


def _dataset(policy: fred.FredPolicy) -> Callable[[Request], str]:
    def dataset(request: Request) -> str:
        if request.endpoint == fred.SERIES_CSV:
            return policy.csv_series.get(request.parameters["id"], "fred.series_csv")
        return fred.ALFRED_DATASET

    return dataset


# --- batch tables -----------------------------------------------------------------------------


def _complete_queries(
    batch: collection.Batch,
) -> list[tuple[Request, list[tuple[collection.Retained, fred.Page]]]]:
    """Each complete observations query of the batch: its first request and pages in order."""
    queries: dict[tuple[str, str, str], dict[int, tuple[collection.Retained, fred.Page]]] = {}
    firsts: dict[tuple[str, str, str], Request] = {}
    for item in batch.retained:
        request = item.request
        if request.endpoint != fred.OBSERVATIONS or item.outcome != COMPLETED:
            continue
        asked = fred.window(request)
        page = fred.parse_page(request, item.response)
        queries.setdefault(asked.query, {})[asked.offset] = (item, page)
        if asked.offset == 0:
            firsts[asked.query] = request
    complete = []
    for key, pages in queries.items():
        if key not in firsts:
            continue
        count = pages[0][1].count
        offsets = range(0, max(count, 1), fred.PAGE_LIMIT)
        if all(offset in pages and pages[offset][1].count == count for offset in offsets):
            ordered = [pages[offset] for offset in offsets]
            if sum(len(page.rows) for _, page in ordered) == count:
                complete.append((firsts[key], ordered))
    return complete


def observations_table(batch: collection.Batch) -> tuple[pa.Table, int]:
    """The vintage rows of the batch's complete windows and the count restated at a start."""
    import pyarrow as pa  # noqa: PLC0415 -- the legacy extra commits source tables

    rows: list[tuple[object, ...]] = []
    restated = 0
    for first, pages in _complete_queries(batch):
        asked = fred.window(first)
        for item, page in pages:
            kept, dropped = fred.split_rows(first, page.rows)
            restated += dropped
            retrieved = item.retrieved_at
            rows.extend(
                (asked.series_id, observation, start, end, value, asked.start, asked.end,
                 retrieved, item.sha256)
                for observation, start, end, value in kept
            )  # fmt: skip
    schema = observations_schema()
    columns = list(zip(*rows, strict=True)) if rows else [() for _ in schema]
    table = pa.table(
        {name: list(values) for name, values in zip(schema.names, columns, strict=True)},
        schema=schema,
    )
    return table, restated


def csv_source(payload: bytes, series: str) -> tuple[SourceContent, pa.Table]:
    """A CSV download as the ``fred-series-csv`` source ``fred.series_csv@1`` would import."""
    import pyarrow as pa  # noqa: PLC0415 -- the legacy extra commits source tables

    header = ("observation_date", series)
    parsed = csv_table(payload, header, f"{series}.csv")
    rows = len(parsed[0])
    table = pa.Table.from_batches(
        [pa.record_batch([pa.repeat(pa.scalar(series, pa.string()), rows), *parsed],
                         schema=SERIES_CSV_TABLE.schema())]
    )  # fmt: skip
    content = SourceContent(
        SERIES_CSV_TABLE.provider,
        SERIES_CSV_TABLE.shape,
        collection.SOURCE_MAJOR,
        (SourceFile(hashlib.sha256(payload).hexdigest(), len(payload)),),
    )
    return content, table


def commit_fred_batch(workspace: Workspace, batch: collection.Batch) -> list[dict[str, object]]:
    observations, restated = observations_table(batch)
    committed = collection.commit_batch(
        workspace, batch, [(OBSERVATIONS_SHAPE, OBSERVATIONS_TABLE, observations)], loader=LOADER
    )
    for entry in committed:
        if entry["table"] == OBSERVATIONS_TABLE:
            entry["restated_rows"] = restated
    for item in batch.retained:
        request = item.request
        if request.endpoint != fred.SERIES_CSV or item.outcome != COMPLETED:
            continue
        try:
            content, table = csv_source(item.response, request.parameters["id"])
        except ValueError as error:
            committed.append({"series": request.parameters["id"], "refused": str(error)})
            continue
        put_raw(workspace.paths.raw, item.response)
        committed.append(
            collection.commit(workspace, content, SERIES_CSV_TABLE.name, table, loader=LOADER)
        )
    return committed


# --- what is known ------------------------------------------------------------------------------


@dataclass(slots=True)
class Known:
    knowledge: fred.FredKnowledge = field(default_factory=fred.FredKnowledge)
    committed_receipts: set[str] = field(default_factory=set)
    alfred_tables: int = 0
    receipt_rows: int = 0


def _alfred_tables(workspace: Workspace) -> list[collection.Committed]:
    tables = []
    for table in collection.committed_tables(workspace):
        if not set(ALFRED_COLUMNS) <= set(table.columns):
            continue
        types = collection.column_types(workspace, table)
        if all(types.get(name) == kind for name, kind in ALFRED_COLUMNS.items()):
            tables.append(table)
    return tables


def load_known(workspace: Workspace) -> Known:
    """Each series' latest collected vintage day and CSV day, from committed sources."""
    known = Known()
    for table in _alfred_tables(workspace):
        known.alfred_tables += 1
        # A vintage that started on the FRED day it was retrieved may still have been
        # publishing; only earlier vintage days count as collected.
        rows = (
            collection.connection(workspace, table)
            .execute(
                "SELECT series_id, max(realtime_start) FROM "  # noqa: S608 -- quoted manifest identifier
                f"{schema.quoted(str(table.entry['target']))} WHERE realtime_start < "
                "CAST(timezone('America/Chicago', retrieved_at_utc) AS DATE) GROUP BY series_id"
            )
            .fetchall()
        )
        for series, start in rows:
            if isinstance(series, str) and isinstance(start, date):
                known.knowledge.vintage(series, start)
    for _, row in collection.receipt_rows(workspace, PROVIDER):
        known.receipt_rows += 1
        known.committed_receipts.add(cast("str", row[8]))
        endpoint, request_json, outcome, retrieved = row[1], row[2], row[3], row[11]
        at = collection.parse_instant(retrieved)
        if endpoint != fred.SERIES_CSV or outcome != COMPLETED or at is None:
            continue
        request = Request.from_document(PROVIDER, json.loads(cast("str", request_json)))
        known.knowledge.csv(request.parameters["id"], fred.fred_day(at))
    return known


# --- one run ------------------------------------------------------------------------------------


@dataclass(slots=True)
class Run:
    workspace: Workspace
    caller: collection.Caller
    knowledge: fred.FredKnowledge
    batch_size: int
    pending: list[collection.Retained] = field(default_factory=list)
    committed: list[dict[str, object]] = field(default_factory=list)
    incomplete: Counter[str] = field(default_factory=Counter)

    def flush(self, *, final: bool = False) -> None:
        size = sum(len(item.response) for item in self.pending)
        if self.pending and (final or len(self.pending) >= self.batch_size or size >= BATCH_BYTES):
            batch = collection.Batch.of(PROVIDER, self.pending)
            self.pending = []
            self.committed.extend(commit_fred_batch(self.workspace, batch))

    def _answer(self, request: Request, reason: str) -> bytes | None:
        """One call's body when it answered ``COMPLETED``; every answer is kept for commit."""
        answered = self.caller.ask(request, reason)
        if answered is None:
            return None
        retained, outcome, response = answered
        self.pending.append(retained)
        return response.body if outcome == COMPLETED else None

    def query(self, request: Request) -> bool:
        """Every page of one observations window; False when the window did not complete."""
        current: Request | None = request
        starts: list[date] = []
        while current is not None:
            body = self._answer(current, "window")
            if body is None:
                self.incomplete[fred.window(request).series_id] += 1
                return False
            page = fred.parse_page(current, body)
            kept, _ = fred.split_rows(request, page.rows)
            starts.extend(start for _, start, _, _ in kept)
            current = fred.next_page(current, page)
        for start in starts:
            self.knowledge.vintage(fred.window(request).series_id, start)
        self.flush()
        return True

    def series(self, planned: fred.Planned) -> None:
        """Vintage dates, then each observations window in order up to the first incomplete."""
        current: Request | None = planned.request
        vintages: list[date] = []
        while current is not None:
            body = self._answer(current, planned.reason)
            if body is None:
                return
            count, days = fred.parse_vintage_dates(current, body)
            vintages.extend(days)
            current = fred.next_vintage_page(current, count)
        asked = fred.window(planned.request)
        start = self.knowledge.vintages.get(asked.series_id, fred.ORIGIN)
        for first, last in fred.observation_windows(start, vintages, asked.end):
            if not self.query(fred.observations(asked.series_id, first, last)):
                return


def collect_fred(  # noqa: PLR0913 -- every bound of one run is explicit
    workspace: Workspace,
    client: fred.FredClient,
    *,
    policy: fred.FredPolicy | None = None,
    clock: Clock,
    sleep: Callable[[float], None] = time.sleep,
    max_calls: int = DEFAULT_MAX_CALLS,
    batch_size: int = collection.DEFAULT_BATCH,
    min_interval: float = MIN_INTERVAL_SECONDS,
) -> dict[str, object]:
    """One bounded FRED collection; see the module documentation for the phases."""
    policy = policy or fred.FredPolicy()
    for name, value in (("max_calls", max_calls), ("batch_size", batch_size)):
        if type(value) is not int or value < (0 if name == "max_calls" else 1):
            raise ValueError(f"{name} must be a positive integer")
    now = clock()
    settled = ledger.recover(workspace.state, PROVIDER, at_us=collection.epoch_us(now))
    before = load_known(workspace)
    recovered = collection.orphans(workspace, PROVIDER, before.committed_receipts)
    committed: list[dict[str, object]] = []
    if recovered:
        committed.extend(commit_fred_batch(workspace, collection.Batch.of(PROVIDER, recovered)))
    known = load_known(workspace) if recovered else before
    caller = collection.Caller(
        workspace, PROVIDER, policy.sha256, client.request, fred.classify, fred.stops_run,
        _dataset(policy), clock, sleep, max_calls, min_interval,
    )  # fmt: skip
    run = Run(workspace, caller, known.knowledge, batch_size, committed=committed)
    today = fred.fred_day(now)
    for planned in fred.plan_csv(run.knowledge, today, policy):
        answered = caller.ask(planned.request, planned.reason)
        if answered is not None:
            run.pending.append(answered[0])
    for planned in fred.plan_alfred(run.knowledge, today, policy):
        if caller.stopped is not None:
            break
        run.series(planned)
    run.flush(final=True)
    return {
        "provider": PROVIDER,
        "fred_date": today.isoformat(),
        "policy_sha256": policy.sha256,
        "recovered_attempts": settled,
        "recovered_receipts": len(recovered),
        "known": _known_report(known, policy),
        "provider_calls": caller.calls,
        "asked": dict(sorted(caller.asked.items())),
        "outcomes": dict(sorted(caller.outcomes.items())),
        "uncertain": caller.uncertain,
        "incomplete_queries": dict(sorted(run.incomplete.items())),
        "stopped": caller.stopped,
        "vintages_through": {
            series: day.isoformat() for series, day in sorted(run.knowledge.vintages.items())
        },
        "sources": run.committed,
    }


def _known_report(known: Known, policy: fred.FredPolicy) -> dict[str, object]:
    return {
        "alfred_tables": known.alfred_tables,
        "receipt_rows": known.receipt_rows,
        "series_with_vintages": len(set(policy.alfred_series) & set(known.knowledge.vintages)),
    }


def plan_fred(workspace: Workspace, *, today: date, policy: fred.FredPolicy) -> dict[str, object]:
    """What a run on ``today`` (a FRED date) would ask first; writes nothing."""
    known = load_known(workspace)
    alfred = fred.plan_alfred(known.knowledge, today, policy)
    csv = fred.plan_csv(known.knowledge, today, policy)
    uncommitted = [
        digest
        for digest in ledger.charged_receipts(workspace.state, PROVIDER)
        if digest not in known.committed_receipts
    ]
    return {
        "mode": "plan",
        "provider_calls": 0,
        "fred_date": today.isoformat(),
        "realtime_end": fred.last_ended_day(today).isoformat(),
        "policy_sha256": policy.sha256,
        "known": _known_report(known, policy) | {"uncommitted_receipts": len(uncommitted)},
        "series": {
            series: {
                "known_vintage": (
                    None if series not in known.knowledge.vintages
                    else known.knowledge.vintages[series].isoformat()
                ),
            }
            for series in policy.alfred_series
        },
        "requests": dict(
            sorted(Counter(f"{i.request.endpoint}:{i.reason}" for i in alfred + csv).items())
        ),
        "due": [
            {"series": item.request.parameters.get("series_id", item.request.parameters.get("id")),
             "endpoint": item.request.endpoint, "reason": item.reason}
            for item in csv + alfred
        ],
    }  # fmt: skip
