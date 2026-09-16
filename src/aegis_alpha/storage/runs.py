"""Formal run records: durable intent, sealed evidence, and resumable commit.

The caller owns calculation. This module records what was run, stores the result
once, and reads it back by run identity. It never imports application code and
never recomputes: recovery finishes a storage commit that already has its target
marker, or terminates a run that does not.
"""

from __future__ import annotations

import calendar
import hashlib
import json
import math
import os
import time
import uuid
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_EVEN, Context, Decimal
from typing import TYPE_CHECKING

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.storage.rowset import rowset_hash
from aegis_alpha.storage.run_schema import require_run_schema
from aegis_alpha.storage.state import (
    atomic,
    complete_operation,
    get_operation,
    prepare_operation,
    quarantine_operation,
)

if TYPE_CHECKING:
    import sqlite3

    from aegis_alpha.storage.workspace import Workspace

RUN_OPERATION_KIND = "run_commit"
METRIC_DEFINITION_VERSION = "aas-run-metrics-v1"
_MANIFEST_SCHEMA = "aas-run-manifest-v1"
_ENVELOPE = "envelope.json"
_PREPARATION = "preparation.json"
_BACKTEST = "backtest.json"
_ARTIFACTS = (_ENVELOPE, _PREPARATION, _BACKTEST)
_MEDIA_TYPE = "application/json"
_SHA_LENGTH = 64
# A sealed document is decoded whole, so it is charged at the expansion the state
# stores already use for their own documents, before anything is read.
_DOCUMENT_OVERHEAD = 2048
_DOCUMENT_EXPANSION = 128
# An explicit context: an ambient one can carry traps or exponent limits that turn
# an ordinary projection into an exception.
_DECIMAL = Context(prec=38, rounding=ROUND_HALF_EVEN, Emin=-999999, Emax=999999, clamp=0, traps=[])
_QUANTUM = Decimal("1e-12")
_DECIMAL_LIMIT = Decimal(10) ** 26
# Hashed row shapes exclude run_id and ordinal: the first is fresh per run and the
# second is a storage ordering detail, so including either would make the same
# calculation hash differently.
_ROW_SCHEMAS: dict[str, tuple[tuple[str, str], ...]] = {
    "signals": (
        ("module", "text"),
        ("at_us", "utc_us"),
        ("instrument_id", "text"),
        ("signal_id", "text"),
        ("value", "float"),
        ("value_state", "text"),
    ),
    "target_weights": (
        ("module", "text"),
        ("at_us", "utc_us"),
        ("instrument_id", "text"),
        ("weight", "decimal"),
    ),
    "simulated_trades": (
        ("module", "text"),
        ("at_us", "utc_us"),
        ("instrument_id", "text"),
        ("quantity", "decimal"),
        ("price", "decimal"),
        ("cost", "decimal"),
    ),
    "positions": (
        ("module", "text"),
        ("at_us", "utc_us"),
        ("instrument_id", "text"),
        ("quantity", "decimal"),
        ("value", "decimal"),
    ),
    "equity_points": (
        ("module", "text"),
        ("at_us", "utc_us"),
        ("equity", "decimal"),
        ("cash", "decimal"),
    ),
}
# Sort keys give every table a deterministic zero-based ordinal within its module.
_ORDER_KEYS: dict[str, tuple[str, ...]] = {
    "signals": ("at_us", "instrument_id", "signal_id"),
    "target_weights": ("at_us", "instrument_id"),
    "simulated_trades": ("at_us", "instrument_id"),
    "positions": ("at_us", "instrument_id"),
    "equity_points": ("at_us",),
}


class RunStorageError(ValueError):
    """A run record conflicts with its stored evidence."""


@dataclass(frozen=True, slots=True)
class RunStrategyPin:
    """A storage-owned strategy pin; application converts its own shape into this."""

    module: str
    ordinal: int
    store_id: str
    strategy_id: str
    version: str
    raw_hash: str
    contract_hash: str


@dataclass(frozen=True, slots=True)
class RunIntent:
    """Everything known before calculation, including the sealed inputs."""

    request_hash: str
    bundle_id: str
    engine_hash: str
    environment_hash: str
    reason: str
    envelope_bytes: bytes
    preparation_bytes: bytes
    strategy_pins: tuple[RunStrategyPin, ...] = ()
    prior_run_id: str | None = None
    run_id: str | None = None


@dataclass(frozen=True, slots=True)
class RunHandle:
    """An opened run and the evidence sealed while opening it."""

    run_id: str
    operation_id: str
    request_hash: str
    envelope_sha256: str
    preparation_sha256: str


@dataclass(frozen=True, slots=True)
class RunResult:
    """The calculated result. Metrics are derived from these bytes, not supplied."""

    backtest_bytes: bytes


def _decimal12(value: object) -> Decimal:
    number = _finite(value)
    exact = Decimal(repr(number))
    if exact.copy_abs() >= _DECIMAL_LIMIT:
        raise RunStorageError("result value exceeds DECIMAL(38,12)")
    return _DECIMAL.quantize(exact, _QUANTUM)


def _finite(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RunStorageError("result values must be numbers")
    number = float(value)
    if not math.isfinite(number):
        raise RunStorageError("result values must be finite")
    return number


def _session_us(value: object) -> int:
    if not isinstance(value, str):
        raise RunStorageError("result dates must be ISO text")
    return calendar.timegm(date.fromisoformat(value).timetuple()) * 1_000_000


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RunStorageError(f"{field} must be nonempty text")
    return value


def _digest(value: str, field: str) -> str:
    if len(value) != _SHA_LENGTH or value.strip("0123456789abcdef"):
        raise RunStorageError(f"{field} must be a lowercase SHA-256")
    return value


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RunStorageError(f"{field} must be an object")
    return value


def project_result_rows(backtest_bytes: bytes, envelope_bytes: bytes) -> dict[str, list[dict]]:
    """Derive stored result rows from the sealed bytes, deterministically.

    Both commit and recovery call this, so a resumed commit reproduces exactly the
    rows the interrupted one would have written.
    """
    document = _mapping(json.loads(backtest_bytes), "backtest result")
    envelope = _mapping(json.loads(envelope_bytes), "envelope")
    module = _text(document.get("module"), "module")
    result = _mapping(document.get("result"), "result")
    equity: list[dict[str, object]] = []
    for row in _sequence(result.get("nav"), "nav"):
        entry = _mapping(row, "nav entry")
        equity.append(
            {
                "module": module,
                "at_us": _session_us(entry.get("date")),
                "equity": _decimal12(entry.get("equity")),
                "cash": _decimal12(entry.get("cash")),
            }
        )
    trades: list[dict[str, object]] = []
    for row in _sequence(result.get("fills"), "fills"):
        fill = _mapping(row, "fill")
        trades.append(
            {
                "module": module,
                "at_us": _session_us(fill.get("execution_date")),
                "instrument_id": _text(fill.get("symbol"), "fill symbol"),
                "quantity": _decimal12(fill.get("shares")),
                "price": _decimal12(fill.get("price")),
                "cost": _decimal12(fill.get("fee")),
            }
        )
    weights: list[dict[str, object]] = []
    for day, targets in sorted(_mapping(envelope.get("targets"), "targets").items()):
        for symbol, weight in sorted(_mapping(targets, "target weights").items()):
            weights.append(
                {
                    "module": module,
                    "at_us": _session_us(day),
                    "instrument_id": _text(symbol, "target symbol"),
                    "weight": _decimal12(weight),
                }
            )
    return {
        "signals": [],
        "target_weights": weights,
        "simulated_trades": trades,
        "positions": [],
        "equity_points": equity,
    }


def project_metrics(backtest_bytes: bytes) -> dict[str, tuple[Decimal | None, str]]:
    """Derive v1 metrics from the sealed result bytes, recording absence as a state.

    A metric this version cannot resolve is written down as unresolved rather than
    left out, so a reader can tell an absent metric from an absent reason. sharpe and
    sortino need a pinned risk-free reference that the run does not carry, so they are
    always recorded not collected.
    """
    result = _mapping(
        _mapping(json.loads(backtest_bytes), "backtest result").get("result"), "result"
    )
    nav = _sequence(result.get("nav"), "nav")
    metrics: dict[str, tuple[Decimal | None, str]] = {
        "sharpe": (None, "not_collected"),
        "sortino": (None, "not_collected"),
    }
    if not nav:
        return {**metrics, "final_equity": (None, "missing"), "total_return": (None, "missing")}
    opening = _decimal12(_mapping(nav[0], "nav entry").get("equity"))
    final = _decimal12(_mapping(nav[-1], "nav entry").get("equity"))
    metrics["final_equity"] = (final, "present")
    if opening == 0:
        # A return measured against a zero opening equity is undefined, not zero.
        metrics["total_return"] = (None, "unsupported")
    else:
        # Every step stays inside the explicit context. An ambient subtraction could
        # round at a different precision and make recovery disagree with commit.
        metrics["total_return"] = (
            _DECIMAL.quantize(
                _DECIMAL.subtract(_DECIMAL.divide(final, opening), Decimal(1)), _QUANTUM
            ),
            "present",
        )
    return metrics


def _sequence(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise RunStorageError(f"{field} must be an array")
    return value


def table_receipts(rows: dict[str, list[dict]]) -> tuple[dict[str, str], dict[str, int]]:
    """Hash each table's content independently of run identity and row order."""
    hashes = {name: rowset_hash(_ROW_SCHEMAS[name], rows[name]) for name in sorted(_ROW_SCHEMAS)}
    counts = {name: len(rows[name]) for name in sorted(_ROW_SCHEMAS)}
    return hashes, counts


def manifest_hash(request_hash: str, artifacts: dict[str, str], receipts: tuple) -> str:
    hashes, counts = receipts
    return content_sha256(
        {
            "schema": _MANIFEST_SCHEMA,
            "request_hash": request_hash,
            "artifacts": artifacts,
            "table_hashes": hashes,
            "table_counts": counts,
        }
    )


def _seal(workspace: Workspace, run_id: str, name: str, raw: bytes) -> str:
    """Write one artifact exclusively, fsync it and its directory, then re-read it."""
    relative = run_id + "/" + name
    with DescriptorTree.open_path(workspace.paths.runs) as tree:
        tree.mkdir(run_id, exist_ok=True)
        with tree.binary_writer(relative, exclusive=True) as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        tree.fsync_directory(run_id)
        with tree.binary_reader(relative, require_single_link=True) as handle:
            stored = handle.read(len(raw) + 1)
    if stored != raw:
        raise RunStorageError("sealed run artifact changed while being written")
    return hashlib.sha256(raw).hexdigest()


def _artifact_digest(workspace: Workspace, run_id: str, name: str) -> tuple[str, int]:
    relative = run_id + "/" + name
    with (
        DescriptorTree.open_path(workspace.paths.runs) as tree,
        tree.binary_reader(relative, require_single_link=True) as handle,
    ):
        digest = hashlib.sha256()
        size = 0
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def open_run(workspace: Workspace, intent: RunIntent) -> RunHandle:
    """Record a durable intent, then seal the inputs. Nothing is calculated here."""
    require_run_schema(workspace)
    if workspace.state.in_transaction:
        # atomic() degrades to a savepoint inside an open transaction, so an outer
        # rollback would erase this run while its sealed files stayed on disk.
        raise RunStorageError("open_run cannot run inside another state transaction")
    request_hash = _digest(_text(intent.request_hash, "request_hash"), "request_hash")
    stored = workspace.state.execute(
        "SELECT request_hash FROM backtest_requests WHERE bundle_id=?", (intent.bundle_id,)
    ).fetchone()
    if stored is None or stored[0] != request_hash:
        raise RunStorageError("run requires a registered request for its bundle")
    run_id = intent.run_id or "run-" + uuid.uuid4().hex
    _text(run_id, "run_id")
    envelope_sha256 = hashlib.sha256(intent.envelope_bytes).hexdigest()
    operation_id = "run:" + run_id
    now = time.time_ns() // 1000
    with atomic(workspace.state):
        workspace.state.execute(
            "INSERT INTO runs(run_id,prior_run_id,bundle_id,engine_hash,environment_hash,"
            "seed,reason,status,created_at_us,completed_at_us,result_hash) "
            "VALUES (?,?,?,?,?,NULL,?,'RUNNING',?,NULL,NULL)",
            (
                run_id,
                intent.prior_run_id,
                intent.bundle_id,
                _text(intent.engine_hash, "engine_hash"),
                _text(intent.environment_hash, "environment_hash"),
                _text(intent.reason, "reason"),
                now,
            ),
        )
        workspace.state.execute(
            "INSERT INTO run_details(run_id,request_hash,prior_run_id,request_schema) "
            "VALUES (?,?,?,'aas-backtest-request-v1')",
            (run_id, request_hash, intent.prior_run_id),
        )
        for pin in intent.strategy_pins:
            workspace.state.execute(
                "INSERT INTO run_strategies(run_id,module,ordinal,strategy_store_id,strategy_id,"
                "version,raw_hash,contract_hash) VALUES (?,?,?,?,?,?,?,?)",
                (
                    run_id,
                    _text(pin.module, "pin module"),
                    pin.ordinal,
                    _text(pin.store_id, "pin store_id"),
                    _text(pin.strategy_id, "pin strategy_id"),
                    _text(pin.version, "pin version"),
                    _digest(pin.raw_hash, "pin raw_hash"),
                    _digest(pin.contract_hash, "pin contract_hash"),
                ),
            )
        workspace.state.execute(
            "INSERT INTO run_events(run_id,sequence,known_at_us,kind,reason) VALUES (?,1,?,?,?)",
            (run_id, now, "started", intent.reason),
        )
        prepare_operation(
            workspace.state,
            operation_id=operation_id,
            kind=RUN_OPERATION_KIND,
            request_hash=request_hash,
            target_id=run_id,
            expected_parent=intent.prior_run_id,
            payload_hash=envelope_sha256,
        )
    # The intent is durable now, so a crash during sealing leaves a discoverable run.
    sealed_envelope = _seal(workspace, run_id, _ENVELOPE, intent.envelope_bytes)
    sealed_preparation = _seal(workspace, run_id, _PREPARATION, intent.preparation_bytes)
    return RunHandle(
        run_id=run_id,
        operation_id=operation_id,
        request_hash=request_hash,
        envelope_sha256=sealed_envelope,
        preparation_sha256=sealed_preparation,
    )


def _marker(workspace: Workspace, run_id: str) -> dict[str, object] | None:
    row = workspace.market.execute(
        "SELECT run_id,operation_id,request_hash,manifest_hash,table_hashes,table_counts "
        "FROM result_commits WHERE run_id=?",
        [run_id],
    ).fetchone()
    if row is None:
        return None
    return {
        "run_id": row[0],
        "operation_id": row[1],
        "request_hash": row[2],
        "manifest_hash": row[3],
        "table_hashes": json.loads(row[4]),
        "table_counts": json.loads(row[5]),
    }


def _admit(budget: ComputeBudget | None, needed: int, message: str) -> None:
    """Charge a materialization before it happens, never after it is in memory."""
    if budget is not None and needed > budget.available_bytes:
        raise ComputeResourceError(message)


def _read_sealed(workspace: Workspace, run_id: str, name: str) -> bytes:
    with (
        DescriptorTree.open_path(workspace.paths.runs) as tree,
        tree.binary_reader(run_id + "/" + name, require_single_link=True) as handle,
    ):
        return handle.read()


def _comparable(value: object) -> tuple[int, Decimal, str]:
    """Give every stored field one total order across its own type."""
    if value is None:
        return (0, Decimal(0), "")
    if isinstance(value, Decimal):
        return (1, value, "")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (1, Decimal(str(value)), "")
    return (2, Decimal(0), str(value))


def _ordered(name: str, rows: list[dict[str, object]]) -> list[tuple[int, dict[str, object]]]:
    """Number rows zero-based inside each module under a total order.

    The declared keys are not unique on their own: signals repeat a date and an
    instrument. Every remaining field breaks the tie, so shuffling the caller's
    input cannot move an ordinal.
    """
    fields = (*_ORDER_KEYS[name], *(field for field, _kind in _ROW_SCHEMAS[name]))
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        grouped.setdefault(_text(row["module"], "row module"), []).append(row)
    numbered: list[tuple[int, dict[str, object]]] = []
    for module in sorted(grouped):
        ranked = sorted(
            grouped[module], key=lambda row: tuple(_comparable(row[field]) for field in fields)
        )
        numbered.extend(enumerate(ranked))
    return numbered


@dataclass(frozen=True, slots=True)
class _Derived:
    """Everything a commit needs, derived only from evidence already on disk."""

    module: str
    artifacts: dict[str, str]
    sizes: dict[str, int]
    ordered: dict[str, list[tuple[int, dict[str, object]]]]
    hashes: dict[str, str]
    counts: dict[str, int]
    manifest: str
    metrics: dict[str, tuple[Decimal | None, str]]


def _derive(
    workspace: Workspace, run_id: str, request_hash: str, budget: ComputeBudget | None
) -> _Derived:
    """Reproduce the commit from the sealed files alone, with no handle and no caller input."""
    measured = {name: _artifact_digest(workspace, run_id, name) for name in _ARTIFACTS}
    sizes = {name: size for name, (_hash, size) in measured.items()}
    artifacts = {name: digest for name, (digest, _size) in measured.items()}
    _admit(
        budget,
        _DOCUMENT_OVERHEAD + _DOCUMENT_EXPANSION * sum(sizes.values()),
        "run artifacts exceed materialization budget",
    )
    envelope_bytes = _read_sealed(workspace, run_id, _ENVELOPE)
    backtest_bytes = _read_sealed(workspace, run_id, _BACKTEST)
    rows = project_result_rows(backtest_bytes, envelope_bytes)
    hashes, counts = table_receipts(rows)
    document = _mapping(json.loads(backtest_bytes), "backtest result")
    return _Derived(
        module=_text(document.get("module"), "module"),
        artifacts=artifacts,
        sizes=sizes,
        ordered={name: _ordered(name, rows[name]) for name in sorted(_ROW_SCHEMAS)},
        hashes=hashes,
        counts=counts,
        manifest=manifest_hash(request_hash, artifacts, (hashes, counts)),
        metrics=project_metrics(backtest_bytes),
    )


def _normalized(value: object) -> object:
    """Re-quantize a stored decimal so its scale cannot depend on the driver."""
    return _DECIMAL.quantize(value, _QUANTUM) if isinstance(value, Decimal) else value


def _stored_rows(workspace: Workspace, run_id: str) -> dict[str, list[dict[str, object]]]:
    stored: dict[str, list[dict[str, object]]] = {}
    for name in sorted(_ROW_SCHEMAS):
        fields = tuple(field for field, _kind in _ROW_SCHEMAS[name])
        columns = ",".join('"' + field + '"' for field in fields)
        # The names come from this module's own schema map, never from input, and the
        # receipt hash sorts rows itself so no stored order is relied on here.
        statement = f'SELECT {columns} FROM "{name}" WHERE run_id=?'  # noqa: S608
        stored[name] = [
            {field: _normalized(row[index]) for index, field in enumerate(fields)}
            for row in workspace.market.execute(statement, [run_id]).fetchall()
        ]
    return stored


def _require_open(workspace: Workspace, handle: RunHandle) -> None:
    row = workspace.state.execute(
        "SELECT r.status,d.request_hash FROM runs r JOIN run_details d ON d.run_id=r.run_id "
        "WHERE r.run_id=?",
        (handle.run_id,),
    ).fetchone()
    if row is None or row[0] != "RUNNING" or row[1] != handle.request_hash:
        raise RunStorageError("run is not open under this handle")
    operation = get_operation(workspace.state, handle.operation_id)
    if (
        operation is None
        or operation["phase"] != "PREPARED"
        or operation["kind"] != RUN_OPERATION_KIND
        or operation["target_id"] != handle.run_id
        or operation["request_hash"] != handle.request_hash
    ):
        raise RunStorageError("run has no prepared intent under this handle")


def _require_marker_match(
    marker: dict[str, object], operation_id: str, request_hash: str, derived: _Derived
) -> None:
    if (
        marker["operation_id"] != operation_id
        or marker["request_hash"] != request_hash
        or marker["manifest_hash"] != derived.manifest
        or marker["table_hashes"] != derived.hashes
        or marker["table_counts"] != derived.counts
    ):
        raise RunStorageError("stored result marker disagrees with the sealed evidence")


def _write_marker(
    workspace: Workspace, run_id: str, operation_id: str, request_hash: str, derived: _Derived
) -> None:
    """Commit the verifiable target in one market transaction, or leave nothing."""
    existing = _marker(workspace, run_id)
    if existing is not None:
        _require_marker_match(existing, operation_id, request_hash, derived)
        return
    workspace.market.execute("BEGIN TRANSACTION")
    try:
        workspace.market.execute(
            "INSERT INTO result_commits(run_id,operation_id,request_hash,manifest_hash,"
            "table_hashes,table_counts) VALUES (?,?,?,?,?,?)",
            [
                run_id,
                operation_id,
                request_hash,
                derived.manifest,
                canonical_json_bytes(derived.hashes).decode(),
                canonical_json_bytes(derived.counts).decode(),
            ],
        )
        for name in sorted(_ROW_SCHEMAS):
            payload = derived.ordered[name]
            if not payload:
                continue
            fields = tuple(field for field, _kind in _ROW_SCHEMAS[name])
            named = ("run_id", "ordinal", *fields)
            columns = ",".join('"' + field + '"' for field in named)
            marks = ",".join("?" * (2 + len(fields)))
            statement = f'INSERT INTO "{name}"({columns}) VALUES ({marks})'  # noqa: S608
            workspace.market.executemany(
                statement,
                [[run_id, ordinal, *(row[field] for field in fields)] for ordinal, row in payload],
            )
        workspace.market.execute("COMMIT")
    except BaseException:
        workspace.market.execute("ROLLBACK")
        raise


def _record_event(workspace: Workspace, run_id: str, kind: str, reason: str, now: int) -> None:
    sequence = workspace.state.execute(
        "SELECT coalesce(max(sequence),0)+1 FROM run_events WHERE run_id=?", (run_id,)
    ).fetchone()[0]
    workspace.state.execute(
        "INSERT INTO run_events(run_id,sequence,known_at_us,kind,reason) VALUES (?,?,?,?,?)",
        (run_id, sequence, now, kind, reason),
    )


def _metric_payload(
    metrics: dict[str, tuple[Decimal | None, str]],
) -> dict[str, dict[str, object]]:
    """Report every metric with its state, so an absent number keeps its reason."""
    return {
        name: {"value": None if value is None else format(value, "f"), "value_state": state}
        for name, (value, state) in sorted(metrics.items())
    }


def _finish(
    workspace: Workspace, run_id: str, operation_id: str, request_hash: str, derived: _Derived
) -> dict[str, object]:
    """Record the receipts and end the run SUCCESS, only after the marker verifies."""
    hashes, counts = table_receipts(_stored_rows(workspace, run_id))
    if hashes != derived.hashes or counts != derived.counts:
        raise RunStorageError("stored result rows disagree with the sealed evidence")
    now = time.time_ns() // 1000
    with atomic(workspace.state):
        for name in _ARTIFACTS:
            workspace.state.execute(
                "INSERT INTO artifacts(run_id,relative_path,media_type,size_bytes,content_hash) "
                "VALUES (?,?,?,?,?)",
                (run_id, name, _MEDIA_TYPE, derived.sizes[name], derived.artifacts[name]),
            )
        workspace.state.execute(
            "INSERT INTO module_manifests(run_id,module,output_schema,content_hash,row_count) "
            "VALUES (?,?,?,?,?)",
            (run_id, derived.module, _MANIFEST_SCHEMA, derived.manifest, sum(counts.values())),
        )
        for metric in sorted(derived.metrics):
            value, state = derived.metrics[metric]
            workspace.state.execute(
                "INSERT INTO run_metrics(run_id,metric,definition_version,value,value_state) "
                "VALUES (?,?,?,?,?)",
                (
                    run_id,
                    metric,
                    METRIC_DEFINITION_VERSION,
                    None if value is None else format(value, "f"),
                    state,
                ),
            )
        _record_event(workspace, run_id, "committed", "result recorded", now)
        updated = workspace.state.execute(
            "UPDATE runs SET status='SUCCESS', completed_at_us=?, result_hash=? "
            "WHERE run_id=? AND status='RUNNING'",
            (now, derived.manifest, run_id),
        ).rowcount
        if updated != 1:
            raise RunStorageError("run was no longer open when its result was recorded")
        complete_operation(workspace.state, operation_id, request_hash)
    return {
        "run_id": run_id,
        "status": "SUCCESS",
        "result_hash": derived.manifest,
        "module": derived.module,
        "artifacts": derived.artifacts,
        "table_hashes": derived.hashes,
        "table_counts": derived.counts,
        "metrics": _metric_payload(derived.metrics),
        # Stored research evidence. Nothing here admits a strategy to execution.
        "research_only": True,
    }


def _terminate(
    workspace: Workspace,
    run_id: str,
    operation_id: str,
    *,
    status: str,
    reason: str,
) -> dict[str, object]:
    """End a run and its intent together so neither can outlive the other."""
    now = time.time_ns() // 1000
    with atomic(workspace.state):
        current = workspace.state.execute(
            "SELECT status FROM runs WHERE run_id=?", (run_id,)
        ).fetchone()
        if current is None:
            raise RunStorageError("run record is missing")
        final = current[0]
        if final == "RUNNING":
            _record_event(workspace, run_id, status.lower(), reason, now)
            workspace.state.execute(
                "UPDATE runs SET status=?, completed_at_us=? WHERE run_id=? AND status='RUNNING'",
                (status, now, run_id),
            )
            final = status
        operation = get_operation(workspace.state, operation_id)
        if operation is not None and operation["phase"] == "PREPARED":
            # A generic quarantine would leave the run RUNNING and invisible to the
            # PREPARED-only scan, so the run and the intent end in the same transaction.
            quarantine_operation(workspace.state, operation_id, reason)
    return {"run_id": run_id, "status": final, "reason": reason}


def commit_run(
    workspace: Workspace,
    handle: RunHandle,
    result: RunResult,
    *,
    budget: ComputeBudget | None = None,
) -> dict[str, object]:
    """Seal the result, commit the market marker, then finish the state record."""
    require_run_schema(workspace)
    if workspace.state.in_transaction:
        raise RunStorageError("commit_run cannot run inside another state transaction")
    _require_open(workspace, handle)
    _seal(workspace, handle.run_id, _BACKTEST, result.backtest_bytes)
    derived = _derive(workspace, handle.run_id, handle.request_hash, budget)
    if (
        derived.artifacts[_ENVELOPE] != handle.envelope_sha256
        or derived.artifacts[_PREPARATION] != handle.preparation_sha256
    ):
        raise RunStorageError("sealed run inputs changed after the run was opened")
    _write_marker(workspace, handle.run_id, handle.operation_id, handle.request_hash, derived)
    return _finish(workspace, handle.run_id, handle.operation_id, handle.request_hash, derived)


def fail_run(workspace: Workspace, handle: RunHandle, reason: str) -> dict[str, object]:
    """End a calculation that never produced a result. A committed run is refused."""
    require_run_schema(workspace)
    if workspace.state.in_transaction:
        raise RunStorageError("fail_run cannot run inside another state transaction")
    if _marker(workspace, handle.run_id) is not None:
        # Ending it here would strand transaction B: recovery can only finish a run
        # that is still RUNNING.
        raise RunStorageError("a run with a committed marker must be recovered, not failed")
    return _terminate(
        workspace,
        handle.run_id,
        handle.operation_id,
        status="FAILED",
        reason=_text(reason, "reason"),
    )


def read_run(
    workspace: Workspace, run_id: str, *, budget: ComputeBudget | None = None
) -> dict[str, object]:
    """Return a result only when the record, the marker and the files still agree."""
    require_run_schema(workspace)
    row = workspace.state.execute(
        "SELECT r.run_id,r.prior_run_id,r.bundle_id,r.engine_hash,r.environment_hash,r.reason,"
        "r.status,r.created_at_us,r.completed_at_us,r.result_hash,d.request_hash FROM runs r "
        "JOIN run_details d ON d.run_id=r.run_id WHERE r.run_id=?",
        (run_id,),
    ).fetchone()
    if row is None:
        raise RunStorageError("no such run")
    if row["status"] != "SUCCESS":
        raise RunStorageError("run has no readable result: " + row["status"])
    operation = workspace.state.execute(
        "SELECT operation_id,phase FROM storage_operations WHERE kind=? AND target_id=?",
        (RUN_OPERATION_KIND, run_id),
    ).fetchone()
    if operation is None or operation["phase"] != "COMPLETED":
        raise RunStorageError("successful run has no completed intent")
    marker = _marker(workspace, run_id)
    if marker is None:
        raise RunStorageError("successful run has no result marker")
    derived = _derive(workspace, run_id, row["request_hash"], budget)
    _require_marker_match(marker, operation["operation_id"], row["request_hash"], derived)
    if derived.manifest != row["result_hash"]:
        raise RunStorageError("recorded result hash disagrees with the sealed evidence")
    hashes, counts = table_receipts(_stored_rows(workspace, run_id))
    if hashes != derived.hashes or counts != derived.counts:
        raise RunStorageError("stored result rows disagree with the sealed evidence")
    recorded = {
        artifact["relative_path"]: (artifact["size_bytes"], artifact["content_hash"])
        for artifact in workspace.state.execute(
            "SELECT relative_path,size_bytes,content_hash FROM artifacts WHERE run_id=?",
            (run_id,),
        )
    }
    if recorded != {name: (derived.sizes[name], derived.artifacts[name]) for name in _ARTIFACTS}:
        raise RunStorageError("recorded artifacts disagree with the files on disk")
    metrics = {
        row["metric"]: {
            "value": row["value"],
            "value_state": row["value_state"],
            "benchmark_ref": row["benchmark_ref"],
            "risk_free_ref": row["risk_free_ref"],
            "cost_ref": row["cost_ref"],
            "comparison_condition_hash": row["comparison_condition_hash"],
        }
        for row in workspace.state.execute(
            "SELECT metric,value,value_state,benchmark_ref,risk_free_ref,cost_ref,"
            "comparison_condition_hash FROM run_metrics WHERE run_id=? AND definition_version=? "
            "ORDER BY metric",
            (run_id, METRIC_DEFINITION_VERSION),
        )
    }
    if {
        name: {"value": entry["value"], "value_state": entry["value_state"]}
        for name, entry in metrics.items()
    } != _metric_payload(derived.metrics):
        raise RunStorageError("recorded metrics disagree with the sealed evidence")
    return {
        "run_id": run_id,
        "status": row["status"],
        "bundle_id": row["bundle_id"],
        "prior_run_id": row["prior_run_id"],
        "request_hash": row["request_hash"],
        "engine_hash": row["engine_hash"],
        "environment_hash": row["environment_hash"],
        "reason": row["reason"],
        "created_at_us": row["created_at_us"],
        "completed_at_us": row["completed_at_us"],
        "result_hash": row["result_hash"],
        "module": derived.module,
        "artifacts": derived.artifacts,
        "artifact_sizes": derived.sizes,
        "table_hashes": derived.hashes,
        "table_counts": derived.counts,
        "metrics": metrics,
        # Stored research evidence. Nothing here admits a strategy to execution.
        "research_only": True,
        "strategy_pins": [
            dict(pin)
            for pin in workspace.state.execute(
                "SELECT module,ordinal,strategy_store_id,strategy_id,version,raw_hash,"
                "contract_hash FROM run_strategies WHERE run_id=? ORDER BY module,ordinal",
                (run_id,),
            )
        ],
    }


def list_runs(workspace: Workspace) -> list[dict[str, object]]:
    """List every recorded run, whatever its outcome."""
    return [
        dict(row)
        for row in workspace.state.execute(
            "SELECT run_id,bundle_id,status,created_at_us,completed_at_us,result_hash FROM runs "
            "ORDER BY created_at_us,run_id"
        )
    ]


def recover_run(
    workspace: Workspace, operation: sqlite3.Row, *, budget: ComputeBudget | None = None
) -> bool:
    """Finish or end one interrupted run. It never recalculates anything."""
    run_id = str(operation["target_id"])
    operation_id = str(operation["operation_id"])
    request_hash = str(operation["request_hash"])
    row = workspace.state.execute("SELECT status FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        return False
    if row[0] != "RUNNING":
        _terminate(
            workspace,
            run_id,
            operation_id,
            status=row[0],
            reason="run already ended",
        )
        return True
    if _marker(workspace, run_id) is None:
        # Absence cannot separate an unfinished calculation from a rolled-back commit,
        # so the run ends conservatively and nothing on disk is reused or overwritten.
        _terminate(
            workspace,
            run_id,
            operation_id,
            status="INTERRUPTED",
            reason="no result marker",
        )
        return True
    try:
        derived = _derive(workspace, run_id, request_hash, budget)
        marker = _marker(workspace, run_id)
        if marker is None:
            raise RunStorageError("result marker disappeared during recovery")
        _require_marker_match(marker, operation_id, request_hash, derived)
    except (ValueError, OSError):
        _terminate(
            workspace,
            run_id,
            operation_id,
            status="QUARANTINED",
            reason="result marker disagrees with the sealed evidence",
        )
        return True
    _finish(workspace, run_id, operation_id, request_hash, derived)
    return True
