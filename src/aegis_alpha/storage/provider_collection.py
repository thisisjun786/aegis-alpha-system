"""Ledgered calls, receipts and receipt batches shared by the native US collectors.

``Caller.ask`` makes one provider call as a ledger attempt (``collection_ledger``): it is
``reserved`` with a usage event before the call and ``started`` just before it. After an
answer, the response bytes and a canonical receipt (``aas-<provider>-receipt-v1``) go to
``raw/`` and the attempt ``succeeded`` with a ``charged`` event naming the receipt; a call
without an answer is ``uncertain``. A refused key, rate or contact stops the run, and so do
three transport failures in a row.

A batch document (``aas-<provider>-batch-v1``) lists the receipts of one commit in
collection order, so the batch, its receipts and their responses are one complete unit
whose boundary the bytes fix; every table a collector derives from it is a content source
of those files and shares their ``hex``. Receipts a run retained but did not commit (it
stopped before its commit) are committed by the next run first.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.opendart import Clock, TransportError, canonical, instant
from aegis_alpha.data.provider_request import Request, Response
from aegis_alpha.storage import collection_ledger as ledger
from aegis_alpha.storage import source_library_schema as schema
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import SourceContent, SourceFile
from aegis_alpha.storage.source_library import list_sources

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

    from aegis_alpha.storage.workspace import Workspace

SOURCE_MAJOR: Final = 1
RECEIPTS_SHAPE: Final = "collect-receipts"
RECEIPTS_TABLE: Final = "receipts"
RECEIPT_COLUMNS: Final = (
    "fingerprint",
    "endpoint",
    "request_json",
    "outcome",
    "provider_status",
    "http_status",
    "selection_json",
    "receipt_json",
    "receipt_sha256",
    "raw_sha256",
    "raw_size",
    "retrieved_at_utc",
)
MAX_TRANSPORT_FAILURES: Final = 3
DEFAULT_BATCH: Final = 500
DEFAULT_BATCH_BYTES: Final = 256 * 1024 * 1024
_MAX_RAW_BYTES: Final = 512 * 1024 * 1024
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)


def epoch_us(moment: datetime) -> int:
    return (moment.astimezone(UTC) - _EPOCH) // timedelta(microseconds=1)


def from_us(value: int) -> datetime:
    return _EPOCH + timedelta(microseconds=value)


def parse_instant(value: object) -> datetime | None:
    """A receipt instant (``YYYY-MM-DDTHH:MM:SS.ffffffZ``) or None."""
    if not isinstance(value, str) or not value.endswith("Z"):
        return None
    try:
        return datetime.fromisoformat(value[:-1]).replace(tzinfo=UTC)
    except ValueError:
        return None


def receipt_bytes(  # noqa: PLR0913 -- every fact of one answered call is explicit
    request: Request,
    response: Response,
    *,
    outcome: str,
    provider_status: str | None,
    attempt: ledger.Attempt,
    selection: object = None,
) -> bytes:
    """The canonical receipt of one answered call; the response is named by size and hash."""
    return canonical(
        {
            "schema_version": f"aas-{request.provider}-receipt-v1",
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
            "selection": selection,
            "raw": {"sha256": hashlib.sha256(response.body).hexdigest(),
                    "size": len(response.body)},
        }
    ).encode()  # fmt: skip


@dataclass(frozen=True, slots=True)
class Retained:
    """A receipt and the response it names, both in ``raw/``."""

    provider: str
    receipt: bytes
    response: bytes

    @property
    def document(self) -> dict[str, object]:
        return cast("dict[str, object]", json.loads(self.receipt))

    @property
    def request(self) -> Request:
        return Request.from_document(self.provider, self.document["request"])

    @property
    def outcome(self) -> str:
        return cast("str", self.document["outcome"])

    @property
    def selection(self) -> object:
        return self.document.get("selection")

    @property
    def retrieved_at(self) -> datetime:
        moment = parse_instant(self.document["retrieved_at_utc"])
        if moment is None:
            raise ValueError("a receipt names no retrieval instant")
        return moment

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.receipt).hexdigest()

    def row(self) -> tuple[str | None, ...]:
        body = self.document
        raw = cast("dict[str, object]", body["raw"])
        if raw.get("sha256") != hashlib.sha256(self.response).hexdigest():
            raise ValueError("a retained response differs from its receipt")
        status = body.get("provider_status")
        selection = body.get("selection")
        request = self.request
        return (
            request.fingerprint,
            request.endpoint,
            canonical(request.document),
            self.outcome,
            status if isinstance(status, str) else None,
            str(body["http_status"]),
            None if selection is None else canonical(selection),
            self.receipt.decode(),
            self.sha256,
            cast("str", raw["sha256"]),
            str(raw["size"]),
            cast("str", body["retrieved_at_utc"]),
        )


@dataclass(frozen=True, slots=True)
class Batch:
    """One commit: the batch document, then each receipt and its response."""

    provider: str
    manifest: bytes
    retained: tuple[Retained, ...]

    @classmethod
    def of(cls, provider: str, retained: Sequence[Retained]) -> Batch:
        manifest = canonical(
            {
                "schema_version": f"aas-{provider}-batch-v1",
                "receipts": [{"sha256": item.sha256, "size": len(item.receipt)}
                             for item in retained],
            }
        ).encode()  # fmt: skip
        return cls(provider, manifest, tuple(retained))

    def content(self, shape: str) -> SourceContent:
        files = [self.manifest]
        for item in self.retained:
            files.extend((item.receipt, item.response))
        return SourceContent(
            self.provider,
            shape,
            SOURCE_MAJOR,
            tuple(SourceFile(hashlib.sha256(raw).hexdigest(), len(raw)) for raw in files),
        )


def receipts_table(batch: Batch) -> pa.Table:
    import pyarrow as pa  # noqa: PLC0415 -- the legacy extra commits source tables

    rows = [item.row() for item in batch.retained]
    return pa.table(
        {name: [row[index] for row in rows] for index, name in enumerate(RECEIPT_COLUMNS)},
        schema=pa.schema([(name, pa.string()) for name in RECEIPT_COLUMNS]),
    )


def commit(
    workspace: Workspace,
    content: SourceContent,
    table_name: str,
    table: pa.Table,
    *,
    loader: str,
) -> dict[str, object]:
    """Commit one table as the content source ``content``; its files are already in ``raw/``."""
    from aegis_alpha.storage.source_library import import_content_arrow  # noqa: PLC0415

    result = import_content_arrow(
        workspace, content, table_name, table.to_reader(), lineage={"loader": loader}
    )
    (committed,) = cast("list[dict[str, object]]", result["tables"])
    return {
        "source_id": content.source_id,
        "table": table_name,
        "rows": committed["rows"],
        "digest": committed["digest"],
        "reused": bool(result.get("reused", False)),
    }


def commit_batch(
    workspace: Workspace,
    batch: Batch,
    tables: Sequence[tuple[str, str, pa.Table]],
    *,
    loader: str,
) -> list[dict[str, object]]:
    """Retain the batch and commit its receipts and every nonempty derived table.

    ``tables`` are ``(shape, table name, rows)``; each is a source of the batch's files.
    """
    put_raw(workspace.paths.raw, batch.manifest)
    for item in batch.retained:
        put_raw(workspace.paths.raw, item.receipt)
        put_raw(workspace.paths.raw, item.response)
    committed = [
        commit(workspace, batch.content(RECEIPTS_SHAPE), RECEIPTS_TABLE, receipts_table(batch),
               loader=loader)
    ]  # fmt: skip
    committed.extend(
        commit(workspace, batch.content(shape), name, table, loader=loader)
        for shape, name, table in tables
        if table.num_rows
    )
    return committed


def read_raw(workspace: Workspace, digest: str) -> bytes:
    with DescriptorTree.open_path(workspace.paths.raw) as tree:
        raw = tree.read_bytes(digest[:2] + "/" + digest, max_bytes=_MAX_RAW_BYTES)
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError(f"raw object {digest} differs from its address")
    return raw


# --- committed tables ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Committed:
    """One committed source table: its store, source ID and manifest entry."""

    store: str
    source_id: str
    entry: dict[str, object]

    @property
    def hex(self) -> str:
        return self.source_id.rsplit("-", 1)[-1]

    @property
    def columns(self) -> list[str]:
        return cast("list[str]", self.entry["columns"])


def committed_tables(workspace: Workspace, prefix: str = "") -> Iterator[Committed]:
    """Every table of every completed source whose ID starts with ``prefix``.

    The completed sources are listed once and their commit manifests read in one query per
    store, so a scan of every source stays linear in the number of sources.
    """
    visible = {
        str(source["source_id"]): str(source["store"])
        for source in list_sources(workspace)
        if str(source["source_id"]).startswith(prefix)
    }
    for store, conn in schema.connections(workspace).items():
        if store not in set(visible.values()):
            continue
        rows = conn.execute(
            "SELECT source_id, manifest_json FROM source_library_commits "
            "WHERE substr(source_id, 1, ?) = ? ORDER BY source_id",
            [len(prefix), prefix],
        ).fetchall()
        for source_id, manifest in rows:
            if visible.get(str(source_id)) != store:
                continue
            for entry in cast("list[dict[str, object]]", json.loads(str(manifest))["tables"]):
                yield Committed(store, str(source_id), entry)


def connection(workspace: Workspace, table: Committed) -> duckdb.DuckDBPyConnection:
    return cast("duckdb.DuckDBPyConnection", schema.connections(workspace)[table.store])


def column_types(workspace: Workspace, table: Committed) -> dict[str, str]:
    rows = (
        connection(workspace, table)
        .execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name=?",
            [str(table.entry["target"])],
        )
        .fetchall()
    )
    return {str(name): str(kind) for name, kind in rows}


def select(
    workspace: Workspace,
    table: Committed,
    columns: Sequence[str],
    where: str = "",
    parameters: Sequence[object] = (),
) -> list[tuple[object, ...]]:
    names = {*table.columns, "_aas_ordinal"}
    selected = ",".join(schema.quoted(name) if name in names else "NULL" for name in columns)
    query = f"SELECT {selected} FROM {schema.quoted(str(table.entry['target']))} {where}"  # noqa: S608 -- quoted manifest identifiers
    return [tuple(row) for row in connection(workspace, table).execute(query, parameters)
            .fetchall()]  # fmt: skip


def receipt_rows(workspace: Workspace, provider: str) -> Iterator[tuple[Committed, tuple]]:
    """Every committed receipts row of ``provider`` with the table it is in."""
    for table in committed_tables(workspace, f"{provider}-{RECEIPTS_SHAPE}-"):
        if table.entry["name"] != RECEIPTS_TABLE:
            continue
        for row in select(workspace, table, RECEIPT_COLUMNS, "ORDER BY _aas_ordinal"):
            yield table, row


def orphans(workspace: Workspace, provider: str, committed: set[str]) -> list[Retained]:
    """Receipts succeeded attempts retained that no committed batch lists, oldest first."""
    found: list[Retained] = []
    for digest in ledger.charged_receipts(workspace.state, provider):
        if digest in committed:
            continue
        receipt = read_raw(workspace, digest)
        raw = cast("dict[str, object]", json.loads(receipt)["raw"])
        found.append(Retained(provider, receipt, read_raw(workspace, cast("str", raw["sha256"]))))
    return found


# --- calls -------------------------------------------------------------------------------------


@dataclass(slots=True)
class Caller:
    """The ledgered, paced and budgeted calls of one run."""

    workspace: Workspace
    provider: str
    policy_hash: str
    call: Callable[[Request], Response]
    classify: Callable[[Request, Response], tuple[str, str | None]]
    stops: Callable[[Response], bool]
    dataset: Callable[[Request], str]
    clock: Clock
    sleep: Callable[[float], None]
    budget: int
    min_interval: float
    calls: int = 0
    uncertain: int = 0
    stopped: str | None = None
    outcomes: Counter[str] = field(default_factory=Counter)
    asked: Counter[str] = field(default_factory=Counter)
    _failures: int = 0
    _last_call: float | None = None

    def _now_us(self) -> int:
        return epoch_us(self.clock())

    def _pace(self) -> None:
        if self._last_call is not None:
            wait = self.min_interval - (time.monotonic() - self._last_call)
            if wait > 0:
                self.sleep(wait)
        self._last_call = time.monotonic()

    def ask(
        self, request: Request, reason: str, selection: object = None
    ) -> tuple[Retained, str, Response] | None:
        """One ledgered call; None when the run is stopped, out of budget or got no answer."""
        if self.stopped is not None:
            return None
        if self.calls >= self.budget:
            self.stopped = "budget"
            return None
        state = self.workspace.state
        job = ledger.Job(self.provider, self.dataset(request), request.fingerprint,
                         self.policy_hash)  # fmt: skip
        attempt = ledger.reserve(state, job, at_us=self._now_us())
        self._pace()
        ledger.start(state, attempt, at_us=self._now_us())
        self.calls += 1
        self.asked[f"{request.endpoint}:{reason}"] += 1
        try:
            response = self.call(request)
        except TransportError:
            ledger.uncertain(state, attempt, at_us=self._now_us())
            self.uncertain += 1
            self._failures += 1
            if self._failures >= MAX_TRANSPORT_FAILURES:
                self.stopped = "transport_failures"
            return None
        self._failures = 0
        outcome, status = self.classify(request, response)
        receipt = receipt_bytes(
            request, response, outcome=outcome, provider_status=status, attempt=attempt,
            selection=selection,
        )  # fmt: skip
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
        if self.stops(response):
            self.stopped = f"provider_refused:{response.status}"
        return Retained(self.provider, receipt, response.body), outcome, response
