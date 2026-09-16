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
import re
import time
import uuid
from dataclasses import dataclass, replace
from datetime import date
from decimal import ROUND_HALF_EVEN, Context, Decimal
from fractions import Fraction
from itertools import pairwise
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
_INPUTS_SCHEMA = "aas-run-inputs-v1"
_HASH_FORMAT = "aas-canonical-json-sha256-v1"
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
# Fields the frozen result row cannot hold, kept in the run add-on on the same ordinal.
_ADDON_FIELDS = frozenset({"decision_at_us"})
# Every entry point falls back to this when a caller names no budget, so no path
# decodes caller-controlled documents with the checks switched off.
_DEFAULT_MEMORY_BYTES = 512 * 1024 * 1024
# A run identifier becomes a directory name under the runs root.
# A reason is stored twice and read back on every verification, so it is bounded
# rather than left to the caller.
_MAX_REASON_BYTES = 4096
# The first supplied session is the account baseline, so a decision and the session it
# executes on need two. The exporter holds the same minimum on the other side of the seal.
_MINIMUM_SESSIONS = 2
_RUN_ID = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}")
# One stored result row becomes a Python dict, a tagged rowset encoding and a sort
# key, all live at once, on top of its own measured text.
_ROW_OVERHEAD = 512
_ROW_COPIES = 4
# An explicit context: an ambient one can carry traps or exponent limits that turn
# an ordinary projection into an exception.
_DECIMAL = Context(prec=38, rounding=ROUND_HALF_EVEN, Emin=-999999, Emax=999999, clamp=0, traps=[])
_QUANTUM = Decimal("1e-12")
# A literal, not arithmetic: an exponentiation here would be evaluated in whatever
# context the importing process happens to have installed.
_DECIMAL_LIMIT = Decimal("1e26")
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
        ("decision_at_us", "utc_us"),
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
    exact = _exact(value)
    if exact.copy_abs() >= _DECIMAL_LIMIT:
        raise RunStorageError("result value exceeds DECIMAL(38,12)")
    projected = _DECIMAL.quantize(exact, _QUANTUM)
    if not projected.is_finite():
        # The context traps nothing, so an unrepresentable result arrives as NaN
        # rather than as an exception. Refuse it before anything durable is written.
        raise RunStorageError("result value has no DECIMAL(38,12) projection")
    return projected


def _exact(value: object) -> Decimal:
    """Take the number at full width.

    An integer goes straight to Decimal. Routing it through binary64 first would
    silently round every integer above 2**53, and the stored row would then disagree
    with the sealed artifact while reproducing the same wrong value on every read.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RunStorageError("result values must be numbers")
    if isinstance(value, int):
        return Decimal(value)
    if not math.isfinite(value):
        raise RunStorageError("result values must be finite")
    return Decimal(repr(value))


def _session_us(value: object, label: str = "result dates") -> int:
    """Encode a calendar session date in the at_us column.

    The result contract supplies a session date and no instant, and the frozen market
    DDL gives these tables one BIGINT column for it. Midnight UTC is therefore an
    encoding of the date, not a claim about when the session opened: a fill dated
    2024-01-02 on a US venue did not execute at 00:00Z. Ordering and joining these rows
    against real intraday UTC events is not supported. Resolving an actual instant needs
    the pinned session calendar and a column that can hold it, which is a schema change.
    """
    if not isinstance(value, str):
        raise RunStorageError(f"{label} must be ISO text")
    try:
        day = date.fromisoformat(value)
    except ValueError as error:
        # fromisoformat reports the offending text but not the field it came from, and
        # the envelope and the result both reach the column through here.
        raise RunStorageError(f"{label} must be ISO text") from error
    stamp = calendar.timegm(day.timetuple()) * 1_000_000
    if stamp < 0:
        # The stored columns are constrained non-negative, and that constraint must be
        # met before sealing rather than at the insert, when the artifact is immutable.
        raise RunStorageError(f"{label} before 1970 are not storable")
    return stamp


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


def _envelope_module(envelope: dict[str, object]) -> str:
    """The module the sealed envelope was exported for; every run row carries it."""
    return _text(envelope.get("module"), "envelope module")


def account_evidence(document: dict[str, object]) -> tuple[dict[str, object], list[object] | None]:
    """Locate the account evidence under either replay contract, or refuse the document.

    replay_next_open returns nav and fills directly. The cashflow replay nests the same
    account under "account" and adds contribution-neutral unit values. Anything else is
    an unsupported result contract and is refused before a durable write.
    """
    result = _mapping(document.get("result"), "result")
    if "account" in result:
        return _mapping(result["account"], "account"), _sequence(result.get("unit_nav"), "unit_nav")
    if "nav" in result:
        return result, None
    raise RunStorageError("unsupported backtest result contract")


def _by_date(rows: list[object], field: str, label: str) -> list[dict[str, object]]:
    """Order dated entries by their session, then by their own content.

    The supplied array order decides nothing: a caller that reverses its nav must not
    change which entry opens and which closes the period.
    """
    entries = [_mapping(row, label) for row in rows]
    return sorted(
        entries,
        key=lambda entry: (
            _comparable(_session_us(entry.get(field))),
            tuple(_comparable(entry[key]) for key in sorted(entry)),
        ),
    )


def _envelope_sessions(envelope: dict[str, object]) -> tuple[int, ...]:
    """The sessions the envelope was exported from, in the stored date encoding.

    The engine fills a decision at the next supplied session's open, so this list is
    what decides which decision and execution pairs the sealed input could have
    produced. It is required rather than reconstructed: a system calendar or a current
    market lookup answers for today's venue rather than for the period this run was
    prepared from, and would keep answering after the two stopped agreeing.
    """
    listed = envelope.get("dates")
    if listed is None:
        raise RunStorageError("envelope does not name the sessions it was prepared from")
    sessions = tuple(
        _session_us(day, "envelope sessions") for day in _sequence(listed, "envelope dates")
    )
    if len(sessions) < _MINIMUM_SESSIONS:
        raise RunStorageError("envelope must supply at least two sessions")
    if any(earlier >= later for earlier, later in pairwise(sessions)):
        # A repeated or reordered session would make adjacency ambiguous, and the
        # exporter never produces one.
        raise RunStorageError("envelope sessions must increase without repeating")
    return sessions


def _require_paired_sessions(envelope: dict[str, object], fills: list[dict[str, object]]) -> None:
    """Refuse fills the sealed envelope could not have produced.

    The engine decides on one supplied session's close and fills at the next supplied
    session's open, so a fill is possible only when its decision is a session carrying
    targets and its execution is the session immediately after it. Whether that gap is
    a calendar day or a weekend is never asked: adjacency belongs to the supplied list.

    Only that relationship is judged here. Symbols, quantities, prices and fees would
    take the replay itself to check, and demanding a fill for every target day would
    refuse an unchanged allocation, an all-cash target, or a sale of a symbol the new
    positive weights no longer name, all of which the engine produces legitimately.
    """
    sessions = _envelope_sessions(envelope)
    position = {session: index for index, session in enumerate(sessions)}
    decisions = {
        _session_us(day, "envelope target dates")
        for day in _mapping(envelope.get("targets"), "targets")
    }
    for fill in fills:
        decision = _session_us(fill.get("decision_date"))
        execution = _session_us(fill.get("execution_date"))
        if decision >= execution:
            # The engine decides on a close and fills on a later open. A fill that does
            # not follow its decision would corrupt the trade chronology.
            raise RunStorageError("a fill must execute after the decision that produced it")
        index = position.get(decision)
        if index is None:
            raise RunStorageError("a fill decision is not one of the envelope sessions")
        if decision not in decisions:
            raise RunStorageError("a fill decision carries no target weights in the envelope")
        if index + 1 == len(sessions) or sessions[index + 1] != execution:
            raise RunStorageError("a fill must execute on the session after its decision")


def project_result_rows(backtest_bytes: bytes, envelope_bytes: bytes) -> dict[str, list[dict]]:
    """Derive stored result rows from the sealed bytes, deterministically.

    Both commit and recovery call this, so a resumed commit reproduces exactly the
    rows the interrupted one would have written.
    """
    document = _mapping(json.loads(backtest_bytes), "backtest result")
    envelope = _mapping(json.loads(envelope_bytes), "envelope")
    module = _text(document.get("module"), "module")
    if _envelope_module(envelope) != module:
        # The envelope supplies the target weights. A result produced by a different
        # module must not be recorded against them.
        raise RunStorageError("result module conflicts with the envelope module")
    account, _unit_nav = account_evidence(document)
    equity: list[dict[str, object]] = [
        {
            "module": module,
            "at_us": _session_us(entry.get("date")),
            "equity": _decimal12(entry.get("equity")),
            "cash": _decimal12(entry.get("cash")),
        }
        for entry in _by_date(_sequence(account.get("nav"), "nav"), "date", "nav entry")
    ]
    _require_paired_sessions(
        envelope, _by_date(_sequence(account.get("fills"), "fills"), "execution_date", "fill")
    )
    trades: list[dict[str, object]] = [
        {
            "module": module,
            "at_us": _session_us(fill.get("execution_date")),
            "decision_at_us": _session_us(fill.get("decision_date")),
            "instrument_id": _text(fill.get("symbol"), "fill symbol"),
            "quantity": _decimal12(fill.get("shares")),
            "price": _decimal12(fill.get("price")),
            "cost": _decimal12(fill.get("fee")),
        }
        for fill in _by_date(_sequence(account.get("fills"), "fills"), "execution_date", "fill")
    ]
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

    When the document carries external cashflows the account return is not the
    strategy return, so total_return reads the engine contribution-neutral unit values
    whenever they are present.
    """
    document = _mapping(json.loads(backtest_bytes), "backtest result")
    account, unit_nav = account_evidence(document)
    metrics: dict[str, tuple[Decimal | None, str]] = {
        "sharpe": (None, "not_collected"),
        "sortino": (None, "not_collected"),
    }
    nav = _by_date(_sequence(account.get("nav"), "nav"), "date", "nav entry")
    units = None if unit_nav is None else _by_date(unit_nav, "date", "unit nav entry")
    if units is not None and [e.get("date") for e in units] != [e.get("date") for e in nav]:
        # The engine emits one unit value per account session. Checked before the empty
        # case, so contradictory unit evidence cannot pass as merely missing metrics.
        raise RunStorageError("unit nav sessions do not match the account nav")
    if not nav:
        return {**metrics, "final_equity": (None, "missing"), "total_return": (None, "missing")}
    metrics["final_equity"] = (_decimal12(nav[-1].get("equity")), "present")
    if units is None:
        metrics["total_return"] = _change([entry.get("equity") for entry in nav])
    else:
        metrics["total_return"] = _change([entry.get("unit_value") for entry in units])
    return metrics


def _change(series: list[object]) -> tuple[Decimal | None, str]:
    """Return the closing-over-opening change, or the reason it is not defined."""
    if not series:
        return (None, "missing")
    opening, final = _decimal12(series[0]), _decimal12(series[-1])
    if opening == 0:
        # A change measured against a zero opening value is undefined, not zero.
        return (None, "unsupported")
    # Every step stays inside the explicit context. An ambient subtraction could round
    # at a different precision and make recovery disagree with commit.
    change = _DECIMAL.quantize(
        _DECIMAL.subtract(_DECIMAL.divide(final, opening), Decimal(1)), _QUANTUM
    )
    # The context traps nothing, so an unrepresentable ratio arrives as NaN, not as an
    # exception, and would otherwise be committed as a successful metric.
    return (change, "present") if change.is_finite() else (None, "unsupported")


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
    """Write one artifact exclusively, fsync it and its directory, then re-read it.

    A repeat with identical bytes is the same seal, not a conflict, so an interrupted
    open or commit is resumed by comparison. Different bytes under a name that is
    already sealed are refused; a sealed artifact is never replaced.
    """
    relative = run_id + "/" + name
    with DescriptorTree.open_path(workspace.paths.runs) as tree:
        tree.mkdir(run_id, exist_ok=True)
        # The new directory entry itself lives in the parent, so the parent is synced
        # too. Syncing only the new directory would leave it unreachable after a loss.
        tree.fsync_directory()
        if not tree.exists(relative):
            with tree.binary_writer(relative, exclusive=True) as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            tree.fsync_directory(run_id)
        with tree.binary_reader(relative, require_single_link=True) as handle:
            stored = handle.read(len(raw) + 1)
    if stored != raw:
        raise RunStorageError("a sealed run artifact already holds different bytes")
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


def _pin_rows(pins: tuple[RunStrategyPin, ...]) -> list[tuple[object, ...]]:
    return [
        (
            _text(pin.module, "pin module"),
            pin.ordinal,
            _text(pin.store_id, "pin store_id"),
            _text(pin.strategy_id, "pin strategy_id"),
            _text(pin.version, "pin version"),
            _digest(pin.raw_hash, "pin raw_hash"),
            _digest(pin.contract_hash, "pin contract_hash"),
        )
        for pin in sorted(pins, key=lambda pin: (pin.module, pin.ordinal))
    ]


def _sealed_request(
    workspace: Workspace, bundle_id: str, request_hash: str, budget: ComputeBudget | None
) -> dict[str, object]:
    """Read the registered request this run must match, charged before it is decoded."""
    _admit(
        budget,
        int(
            workspace.state.execute(
                "SELECT coalesce(max(2048 + 128*length(CAST(request_bytes AS BLOB))),0) "
                "FROM backtest_requests WHERE bundle_id=?",
                (bundle_id,),
            ).fetchone()[0]
        ),
        "backtest request exceeds materialization budget",
    )
    row = workspace.state.execute(
        "SELECT request_bytes,request_hash FROM backtest_requests WHERE bundle_id=?", (bundle_id,)
    ).fetchone()
    if row is None or row[1] != request_hash:
        raise RunStorageError("run requires a registered request for its bundle")
    if hashlib.sha256(row[0]).hexdigest() != request_hash:
        raise RunStorageError("registered request bytes do not match their hash")
    return _mapping(json.loads(row[0]), "backtest request")


def _sealed_identities(request: dict[str, object]) -> tuple[str, str, tuple[str, ...]]:
    """The engine, environment and strategy identities the request already seals."""
    sealed = _mapping(request.get("strategy"), "request strategy")
    return (
        content_sha256(_mapping(request.get("engine"), "request engine")),
        content_sha256(_mapping(request.get("environment"), "request environment")),
        (
            _text(sealed.get("strategy_store_id"), "request strategy store"),
            _text(sealed.get("strategy_id"), "request strategy_id"),
            _text(sealed.get("version"), "request strategy version"),
            _digest(_text(sealed.get("raw_sha256"), "request raw_sha256"), "request raw_sha256"),
            _digest(
                _text(sealed.get("contract_sha256"), "request contract_sha256"),
                "request contract_sha256",
            ),
        ),
    )


def _inputs_hash(bundle_id: str, envelope_sha256: str, preparation_sha256: str) -> str:
    """The durable intent names the bundle and both sealed inputs.

    storage_operations guards its identity columns with a trigger, so this is the only
    immutable copy of the bundle a run was opened against: runs.bundle_id stays writable
    while the run is RUNNING and two bundles can register the same canonical request.
    """
    return content_sha256(
        {
            "schema": _INPUTS_SCHEMA,
            "hash_format": _HASH_FORMAT,
            "bundle_id": bundle_id,
            "envelope_sha256": envelope_sha256,
            "preparation_sha256": preparation_sha256,
        }
    )


def _require_recorded_provenance(
    workspace: Workspace, derived: _Derived, budget: ComputeBudget | None
) -> None:
    """Re-authenticate the stored provenance against the request that sealed it.

    completed_run only freezes a run once it stops being RUNNING, so the identity
    columns can be rewritten between open_run and commit_run. Verification therefore
    compares what is stored rather than trusting what was validated at open time.
    """
    row = workspace.state.execute(
        "SELECT bundle_id,engine_hash,environment_hash,result_hash FROM runs WHERE run_id=?",
        (derived.run_id,),
    ).fetchone()
    if row is None:
        raise RunStorageError("run record is missing")
    if row["result_hash"] != derived.manifest:
        raise RunStorageError("recorded result hash disagrees with the sealed evidence")
    operation = workspace.state.execute(
        "SELECT payload_hash FROM storage_operations WHERE kind=? AND target_id=?",
        (RUN_OPERATION_KIND, derived.run_id),
    ).fetchone()
    expected_inputs = _inputs_hash(
        row["bundle_id"], derived.artifacts[_ENVELOPE], derived.artifacts[_PREPARATION]
    )
    if operation is None or operation["payload_hash"] != expected_inputs:
        raise RunStorageError("recorded bundle or sealed inputs disagree with the durable intent")
    # The projection stays live while the request is decoded beside it.
    engine, environment, strategy = _sealed_identities(
        _sealed_request(
            workspace, row["bundle_id"], derived.request_hash, _reserved(budget, derived)
        )
    )
    if (row["engine_hash"], row["environment_hash"]) != (engine, environment):
        raise RunStorageError("recorded engine identity disagrees with the registered request")
    pins = [
        tuple(pin)
        for pin in workspace.state.execute(
            "SELECT module,ordinal,strategy_store_id,strategy_id,version,raw_hash,contract_hash "
            "FROM run_strategies WHERE run_id=? ORDER BY module,ordinal",
            (derived.run_id,),
        )
    ]
    if pins != [(derived.module, 0, *strategy)]:
        raise RunStorageError("recorded strategy pins disagree with the registered request")
    _require_recorded_metadata(workspace, derived.run_id)


def _require_recorded_metadata(workspace: Workspace, run_id: str) -> None:
    """Compare the columns completed_run leaves writable with their immutable copies.

    prior_run_id, reason and created_at_us can all be rewritten while a run is RUNNING.
    run_details and the started event recorded the same values under immutable triggers
    at open time, so read_run cannot return lineage, a reason or a time nobody recorded.
    """
    seeded = workspace.state.execute(
        "SELECT 1 FROM runs WHERE run_id=? AND seed IS NOT NULL", (run_id,)
    ).fetchone()
    if seeded is not None:
        # open_run always records NULL and no sealed evidence supplies a seed, so a
        # non-null value can only have been written after the fact.
        raise RunStorageError("run seed was recorded without any sealed evidence")
    row = workspace.state.execute(
        "SELECT r.prior_run_id,r.reason,r.created_at_us,d.prior_run_id,e.reason,e.known_at_us "
        "FROM runs r JOIN run_details d ON d.run_id=r.run_id "
        "JOIN run_events e ON e.run_id=r.run_id AND e.sequence=1 WHERE r.run_id=?",
        (run_id,),
    ).fetchone()
    if row is None:
        raise RunStorageError("run record is missing its opening evidence")
    if tuple(row)[:3] != tuple(row)[3:]:
        raise RunStorageError("recorded run metadata disagrees with its opening evidence")


def _require_sealed_provenance(intent: RunIntent, request: dict[str, object], module: str) -> None:
    """Refuse provenance the registered request does not already seal.

    read_run returns the engine, environment and strategy identities as the run
    immutable provenance. The request already seals all three, so accepting whatever a
    caller passes would let a successful run describe a calculation nobody performed.
    """
    engine, environment, expected = _sealed_identities(request)
    if intent.engine_hash != engine:
        raise RunStorageError("engine_hash does not match the registered request")
    if intent.environment_hash != environment:
        raise RunStorageError("environment_hash does not match the registered request")
    pins = _pin_rows(intent.strategy_pins)
    if len(pins) != 1 or tuple(pins[0][2:]) != expected:
        raise RunStorageError("strategy pins do not match the registered request")
    if (pins[0][0], pins[0][1]) != (module, 0):
        # run_strategies is keyed on (run_id, module, ordinal). A pin filed under
        # another module would make read_run report provenance the manifest contradicts.
        raise RunStorageError("strategy pin is not placed on the run module")


def _require_admitted_pins(workspace: Workspace, pins: tuple[RunStrategyPin, ...]) -> None:
    """Refuse a pin the private strategy store cannot confirm.

    read_run returns these hashes as the run's immutable provenance, so a caller that
    supplies a fabricated or stale pin would make a successful run report a strategy it
    never used. Only a version the store actually admitted is recorded.
    """
    if not pins:
        return
    if workspace.strategies is None:
        raise RunStorageError("strategy pins require the private strategy store")
    store_id = workspace.strategies.execute("SELECT store_id FROM store_info").fetchone()[0]
    for pin in pins:
        if pin.store_id != store_id:
            raise RunStorageError("strategy pin names a different strategy store")
        admitted = workspace.strategies.execute(
            "SELECT 1 FROM strategy_versions WHERE strategy_id=? AND version=? "
            "AND raw_sha256=? AND contract_sha256=?",
            (pin.strategy_id, pin.version, pin.raw_hash, pin.contract_hash),
        ).fetchone()
        if admitted is None:
            raise RunStorageError("strategy pin does not match an admitted strategy version")


def _require_same_intent(
    workspace: Workspace, intent: RunIntent, run_id: str, request_hash: str
) -> None:
    """Accept an identical reopen; refuse a different request under the same name.

    The state rows are immutable, so a retry can only be a comparison. A run that has
    already ended is not reopened at all.
    """
    existing = workspace.state.execute(
        "SELECT r.bundle_id,r.engine_hash,r.environment_hash,r.reason,r.status,r.prior_run_id,"
        "d.request_hash FROM runs r LEFT JOIN run_details d ON d.run_id=r.run_id "
        "WHERE r.run_id=?",
        (run_id,),
    ).fetchone()
    stored_pins = [
        tuple(row)
        for row in workspace.state.execute(
            "SELECT module,ordinal,strategy_store_id,strategy_id,version,raw_hash,contract_hash "
            "FROM run_strategies WHERE run_id=? ORDER BY module,ordinal",
            (run_id,),
        )
    ]
    if (
        existing["status"] != "RUNNING"
        or existing["bundle_id"] != intent.bundle_id
        or existing["engine_hash"] != intent.engine_hash
        or existing["environment_hash"] != intent.environment_hash
        or existing["reason"] != intent.reason
        or existing["prior_run_id"] != intent.prior_run_id
        or existing["request_hash"] != request_hash
        or stored_pins != _pin_rows(intent.strategy_pins)
    ):
        raise RunStorageError("run ID already identifies a different or finished run")


@dataclass(frozen=True, slots=True)
class _DurableIntent:
    """The run intent exactly as prepare_operation records it."""

    operation_id: str
    request_hash: str
    target_id: str
    expected_parent: str | None
    payload_hash: str

    def prepare(self, connection: sqlite3.Connection) -> None:
        prepare_operation(
            connection,
            operation_id=self.operation_id,
            kind=RUN_OPERATION_KIND,
            request_hash=self.request_hash,
            target_id=self.target_id,
            expected_parent=self.expected_parent,
            payload_hash=self.payload_hash,
        )


def open_run(
    workspace: Workspace, intent: RunIntent, *, budget: ComputeBudget | None = None
) -> RunHandle:
    """Record a durable intent, then seal the inputs. Nothing is calculated here."""
    require_run_schema(workspace)
    if workspace.state.in_transaction:
        # atomic() degrades to a savepoint inside an open transaction, so an outer
        # rollback would erase this run while its sealed files stayed on disk.
        raise RunStorageError("open_run cannot run inside another state transaction")
    request_hash = _digest(_text(intent.request_hash, "request_hash"), "request_hash")
    # Both sidecars are decoded below and stay live while the request is decoded
    # beside them, so they are charged first and then reserved rather than compared
    # twice against the same allowance.
    inputs = _DOCUMENT_OVERHEAD + _DOCUMENT_EXPANSION * (
        len(intent.envelope_bytes) + len(intent.preparation_bytes)
    )
    _admit(budget, inputs, "run inputs exceed materialization budget")
    held = _allowance(budget)
    held = replace(held, reserved_bytes=held.reserved_bytes + inputs)
    request = _sealed_request(workspace, intent.bundle_id, request_hash, held)
    envelope = _mapping(json.loads(intent.envelope_bytes), "envelope")
    module = _envelope_module(envelope)
    _require_sealed_provenance(intent, request, module)
    # Every later check reads the result's fills against this list, so an envelope
    # without one is refused before it becomes an artifact nothing can replace.
    _envelope_sessions(envelope)
    # Checked before anything durable happens, so a preparation that belongs to another
    # request or another envelope is never sealed under this run.
    _require_linked_inputs(intent.envelope_bytes, intent.preparation_bytes, request_hash)
    _require_admitted_pins(workspace, intent.strategy_pins)
    run_id = intent.run_id or "run-" + uuid.uuid4().hex
    if len(_text(intent.reason, "reason").encode()) > _MAX_REASON_BYTES:
        raise RunStorageError("reason is too large to record")
    if intent.prior_run_id == run_id:
        # The self-referencing foreign key would accept the new row as its own parent.
        raise RunStorageError("a run cannot be its own predecessor")
    if not _RUN_ID.fullmatch(_text(run_id, "run_id")):
        # This becomes a directory name under the runs root.
        raise RunStorageError("run_id must be a plain identifier")
    envelope_sha256 = hashlib.sha256(intent.envelope_bytes).hexdigest()
    preparation_sha256 = hashlib.sha256(intent.preparation_bytes).hexdigest()
    operation_id = "run:" + run_id
    durable = _DurableIntent(
        operation_id=operation_id,
        request_hash=request_hash,
        target_id=run_id,
        expected_parent=intent.prior_run_id,
        # Both sealed inputs are named in the immutable intent. Committing only the
        # envelope would let a retry after an interrupted seal substitute a different
        # preparation under the same accepted run.
        payload_hash=_inputs_hash(intent.bundle_id, envelope_sha256, preparation_sha256),
    )
    if workspace.state.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,)).fetchone():
        # A resumed open: prepare_operation refuses a different or quarantined intent
        # under the same ID, and the run rows are compared rather than rewritten.
        _require_same_intent(workspace, intent, run_id, request_hash)
        durable.prepare(workspace.state)
    else:
        _open_intent(workspace, intent, durable)
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


def _open_intent(workspace: Workspace, intent: RunIntent, durable: _DurableIntent) -> None:
    """Transaction A: the run, its request, its pins and its intent, or none of them."""
    run_id = durable.target_id
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
                _digest(_text(intent.engine_hash, "engine_hash"), "engine_hash"),
                _digest(_text(intent.environment_hash, "environment_hash"), "environment_hash"),
                _text(intent.reason, "reason"),
                now,
            ),
        )
        workspace.state.execute(
            "INSERT INTO run_details(run_id,request_hash,prior_run_id,request_schema) "
            "VALUES (?,?,?,'aas-backtest-request-v1')",
            (run_id, durable.request_hash, intent.prior_run_id),
        )
        for pin in _pin_rows(intent.strategy_pins):
            workspace.state.execute(
                "INSERT INTO run_strategies(run_id,module,ordinal,strategy_store_id,strategy_id,"
                "version,raw_hash,contract_hash) VALUES (?,?,?,?,?,?,?,?)",
                (run_id, *pin),
            )
        workspace.state.execute(
            "INSERT INTO run_events(run_id,sequence,known_at_us,kind,reason) VALUES (?,1,?,?,?)",
            (run_id, now, "started", intent.reason),
        )
        durable.prepare(workspace.state)


def _admit(budget: ComputeBudget | None, needed: int, message: str) -> None:
    """Charge a materialization before it happens, never after it is in memory."""
    if needed > _allowance(budget).available_bytes:
        raise ComputeResourceError(message)


def _allowance(budget: ComputeBudget | None) -> ComputeBudget:
    """Fall back to the serial default rather than treating no budget as no limit."""
    return budget or ComputeBudget(Fraction(1), _DEFAULT_MEMORY_BYTES)


def _marker(
    workspace: Workspace, run_id: str, budget: ComputeBudget | None = None
) -> dict[str, object] | None:
    """Read the market commit marker, charged for its own variable width first."""
    measured = workspace.market.execute(
        "SELECT coalesce(sum(coalesce(octet_length(encode(operation_id)),0)"
        "+coalesce(octet_length(encode(request_hash)),0)"
        "+coalesce(octet_length(encode(manifest_hash)),0)"
        "+coalesce(octet_length(encode(table_hashes)),0)"
        "+coalesce(octet_length(encode(table_counts)),0)),0) "
        "FROM result_commits WHERE run_id=?",
        [run_id],
    ).fetchone()
    _admit(
        budget,
        _DOCUMENT_OVERHEAD + _DOCUMENT_EXPANSION * (0 if measured is None else int(measured[0])),
        "result marker exceeds materialization budget",
    )
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
    instrument. Every remaining field breaks the tie, so shuffling the caller's input
    cannot move an ordinal.
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

    run_id: str
    request_hash: str
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
    preparation_bytes = _read_sealed(workspace, run_id, _PREPARATION)
    backtest_bytes = _read_sealed(workspace, run_id, _BACKTEST)
    projected = _project(envelope_bytes, preparation_bytes, backtest_bytes, request_hash)
    return _Derived(
        run_id=run_id,
        request_hash=request_hash,
        module=projected.module,
        artifacts=artifacts,
        sizes=sizes,
        ordered=projected.ordered,
        hashes=projected.hashes,
        counts=projected.counts,
        manifest=manifest_hash(request_hash, artifacts, (projected.hashes, projected.counts)),
        metrics=projected.metrics,
    )


def _normalized(value: object) -> object:
    """Re-quantize a stored decimal so its scale cannot depend on the driver."""
    return _DECIMAL.quantize(value, _QUANTUM) if isinstance(value, Decimal) else value


def _require_link(document: dict[str, object], field: str, expected: str, label: str) -> None:
    """Require the document to name the evidence it was produced from."""
    recorded = document.get(field)
    if recorded is None:
        raise RunStorageError(label + " does not name the evidence it used")
    if recorded != expected:
        raise RunStorageError(label + " names different evidence")


def _require_linked_inputs(
    envelope_bytes: bytes, preparation_bytes: bytes, request_hash: str
) -> str:
    """Refuse artifacts that do not name this run request and envelope.

    The preparation document records the request it was prepared for and the envelope
    it produced, and the backtest response records the envelope it consumed. These are
    required rather than compared only when present: a document that omits its link
    proves nothing about which calculation produced it, and the manifest built over it
    would be internally consistent while certifying unrelated evidence.
    """
    envelope_sha256 = hashlib.sha256(envelope_bytes).hexdigest()
    preparation = _mapping(json.loads(preparation_bytes), "preparation")
    _require_link(preparation, "request_hash", request_hash, "preparation request")
    _require_link(preparation, "envelope_sha256", envelope_sha256, "preparation envelope")
    return envelope_sha256


def _require_linked_evidence(
    envelope_bytes: bytes, preparation_bytes: bytes, backtest_bytes: bytes, request_hash: str
) -> None:
    envelope_sha256 = _require_linked_inputs(envelope_bytes, preparation_bytes, request_hash)
    document = _mapping(json.loads(backtest_bytes), "backtest result")
    _require_link(document, "input_sha256", envelope_sha256, "backtest envelope")


@dataclass(frozen=True, slots=True)
class _Projection:
    """Everything derived from the three documents, with no file identity in it."""

    module: str
    ordered: dict[str, list[tuple[int, dict[str, object]]]]
    hashes: dict[str, str]
    counts: dict[str, int]
    metrics: dict[str, tuple[Decimal | None, str]]


def _project(
    envelope_bytes: bytes, preparation_bytes: bytes, backtest_bytes: bytes, request_hash: str
) -> _Projection:
    """Validate the evidence links, then derive every stored row, receipt and metric.

    Commit runs this against the candidate bytes before sealing them and recovery runs
    it again from disk, so a document that fails any check never becomes an artifact
    that a corrected retry could not replace.
    """
    _require_linked_evidence(envelope_bytes, preparation_bytes, backtest_bytes, request_hash)
    rows = project_result_rows(backtest_bytes, envelope_bytes)
    hashes, counts = table_receipts(rows)
    document = _mapping(json.loads(backtest_bytes), "backtest result")
    return _Projection(
        module=_text(document.get("module"), "module"),
        ordered={name: _ordered(name, rows[name]) for name in sorted(_ROW_SCHEMAS)},
        hashes=hashes,
        counts=counts,
        metrics=project_metrics(backtest_bytes),
    )


def _row_charge(workspace: Workspace, run_id: str) -> int:
    """Measure what the stored rows will occupy in Python, before fetching any of them."""
    total = 0
    for name in sorted(_ROW_SCHEMAS):
        widths = " + ".join(
            'coalesce(octet_length(encode("' + field + '")),0)'
            for field, kind in _ROW_SCHEMAS[name]
            if kind == "text"
        )
        measure = (
            f"SELECT count(*)*{_ROW_OVERHEAD} + coalesce(sum({widths}),0) "  # noqa: S608
            f'FROM "{name}" WHERE run_id=?'
        )
        measured = workspace.market.execute(measure, [run_id]).fetchone()
        total += 0 if measured is None else int(measured[0])
    # The rows, their rowset encodings and the sort storage are all live at once.
    return total * _ROW_COPIES


def _stored_rows(
    workspace: Workspace, run_id: str, budget: ComputeBudget | None = None
) -> dict[str, list[dict[str, object]]]:
    _admit(
        budget, _row_charge(workspace, run_id), "stored result rows exceed materialization budget"
    )
    stored: dict[str, list[dict[str, object]]] = {}
    for name in sorted(_ROW_SCHEMAS):
        fields = tuple(field for field, _kind in _ROW_SCHEMAS[name])
        columns = ",".join(
            ("d.decision_at_us" if field in _ADDON_FIELDS else 't."' + field + '"')
            for field in fields
        )
        # The names come from this module's own schema map, never from input, and the
        # receipt hash sorts rows itself so no stored order is relied on here. The
        # decision date lives in the run add-on, joined back on the shared ordinal.
        source = f'"{name}" t'
        if any(field in _ADDON_FIELDS for field, _kind in _ROW_SCHEMAS[name]):
            source += (
                " JOIN result_trade_decisions d ON d.run_id=t.run_id "
                "AND d.module=t.module AND d.ordinal=t.ordinal"
            )
        statement = f"SELECT {columns} FROM {source} WHERE t.run_id=?"  # noqa: S608
        stored[name] = [
            {field: _normalized(row[index]) for index, field in enumerate(fields)}
            for row in workspace.market.execute(statement, [run_id]).fetchall()
        ]
    # An inner join hides an add-on row whose ordinal has no trade, so the add-on is
    # counted against the table it annotates rather than only followed into it.
    orphans = workspace.market.execute(
        "SELECT count(*) FROM result_trade_decisions WHERE run_id=?", [run_id]
    ).fetchone()
    if orphans is not None and int(orphans[0]) != len(stored["simulated_trades"]):
        raise RunStorageError("trade decision rows disagree with the stored trades")
    return stored


def _require_open(workspace: Workspace, handle: RunHandle) -> None:
    row = workspace.state.execute(
        "SELECT r.status,d.request_hash FROM runs r JOIN run_details d ON d.run_id=r.run_id "
        "WHERE r.run_id=?",
        (handle.run_id,),
    ).fetchone()
    if row is None or row[0] != "RUNNING" or row[1] != handle.request_hash:
        raise RunStorageError("run is not open under this handle")
    _require_own_intent(workspace, handle.run_id, handle.operation_id, phase="PREPARED")
    operation = get_operation(workspace.state, handle.operation_id)
    if operation is None or operation["request_hash"] != handle.request_hash:
        raise RunStorageError("run has no prepared intent under this handle")


def _require_own_intent(
    workspace: Workspace, run_id: str, operation_id: str, *, phase: str | None = None
) -> dict[str, object] | None:
    """Refuse an intent that names a different run.

    Without this a handle assembled from one run's ID and another run's operation would
    end the wrong intent, leaving that run RUNNING and invisible to the recovery scan.
    """
    operation = get_operation(workspace.state, operation_id)
    if operation is None:
        if phase is not None:
            raise RunStorageError("run has no prepared intent under this handle")
        return None
    if operation["kind"] != RUN_OPERATION_KIND or operation["target_id"] != run_id:
        raise RunStorageError("operation does not belong to this run")
    if phase is not None and operation["phase"] != phase:
        raise RunStorageError("run intent is not in phase " + phase)
    return operation


def _require_marker_match(marker: dict[str, object], operation_id: str, derived: _Derived) -> None:
    if (
        marker["run_id"] != derived.run_id
        or marker["operation_id"] != operation_id
        or marker["request_hash"] != derived.request_hash
        or marker["manifest_hash"] != derived.manifest
        or marker["table_hashes"] != derived.hashes
        or marker["table_counts"] != derived.counts
    ):
        raise RunStorageError("stored result marker disagrees with the sealed evidence")


def _write_marker(workspace: Workspace, derived: _Derived, operation_id: str) -> None:
    """Commit the verifiable target in one market transaction, or leave nothing."""
    existing = _marker(workspace, derived.run_id)
    if existing is not None:
        _require_marker_match(existing, operation_id, derived)
        return
    workspace.market.execute("BEGIN TRANSACTION")
    try:
        workspace.market.execute(
            "INSERT INTO result_commits(run_id,operation_id,request_hash,manifest_hash,"
            "table_hashes,table_counts) VALUES (?,?,?,?,?,?)",
            [
                derived.run_id,
                operation_id,
                derived.request_hash,
                derived.manifest,
                canonical_json_bytes(derived.hashes).decode(),
                canonical_json_bytes(derived.counts).decode(),
            ],
        )
        for name in sorted(_ROW_SCHEMAS):
            payload = derived.ordered[name]
            if not payload:
                continue
            fields = tuple(
                field for field, _kind in _ROW_SCHEMAS[name] if field not in _ADDON_FIELDS
            )
            named = ("run_id", "ordinal", *fields)
            columns = ",".join('"' + field + '"' for field in named)
            marks = ",".join("?" * (2 + len(fields)))
            statement = f'INSERT INTO "{name}"({columns}) VALUES ({marks})'  # noqa: S608
            workspace.market.executemany(
                statement,
                [
                    [derived.run_id, ordinal, *(row[field] for field in fields)]
                    for ordinal, row in payload
                ],
            )
        # The frozen simulated_trades row holds one date, so the decision that produced
        # each fill lives in the run add-on table made for it, under the same ordinal.
        decisions = derived.ordered["simulated_trades"]
        if decisions:
            workspace.market.executemany(
                "INSERT INTO result_trade_decisions(run_id,module,ordinal,decision_at_us) "
                "VALUES (?,?,?,?)",
                [
                    [derived.run_id, row["module"], ordinal, row["decision_at_us"]]
                    for ordinal, row in decisions
                ],
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


def _metric_records(
    metrics: dict[str, tuple[Decimal | None, str]],
) -> dict[str, dict[str, object]]:
    """The full stored shape. v1 pins no comparison references, so all four are null."""
    return {
        name: {
            **entry,
            "benchmark_ref": None,
            "risk_free_ref": None,
            "cost_ref": None,
            "comparison_condition_hash": None,
        }
        for name, entry in _metric_payload(metrics).items()
    }


def _reserved(budget: ComputeBudget | None, derived: _Derived) -> ComputeBudget:
    """Hold the live projection against the allowance before a second copy is read.

    _derive leaves every projected row resident. Admitting the stored rows against the
    unchanged allowance would let two individually acceptable materializations exceed
    the allowance together. An omitted budget resolves to the same fallback _admit uses,
    so the reservation is not discarded on the default path.
    """
    held = _allowance(budget)
    text_bytes = sum(
        len(value.encode())
        for rows in derived.ordered.values()
        for _ordinal, row in rows
        for value in row.values()
        if isinstance(value, str)
    )
    charge = sum(derived.counts.values()) * _ROW_OVERHEAD + text_bytes
    return replace(held, reserved_bytes=held.reserved_bytes + charge * _ROW_COPIES)


def _finish(
    workspace: Workspace,
    derived: _Derived,
    operation_id: str,
    *,
    budget: ComputeBudget | None = None,
) -> dict[str, object]:
    """Record the receipts and end the run SUCCESS, only after the marker verifies."""
    run_id = derived.run_id
    hashes, counts = table_receipts(_stored_rows(workspace, run_id, _reserved(budget, derived)))
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
        complete_operation(workspace.state, operation_id, derived.request_hash)
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
    workspace: Workspace, run_id: str, operation_id: str, *, status: str, reason: str
) -> dict[str, object]:
    """End a run and its intent together so neither can outlive the other."""
    now = time.time_ns() // 1000
    with atomic(workspace.state):
        operation = _require_own_intent(workspace, run_id, operation_id)
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
        if operation is not None and operation["phase"] == "PREPARED":
            # A generic quarantine would leave the run RUNNING and invisible to the
            # PREPARED-only scan, so the run and the intent end in the same transaction.
            quarantine_operation(workspace.state, operation_id, reason)
    return {"run_id": run_id, "status": final, "reason": reason}


def _check_candidate(
    workspace: Workspace,
    handle: RunHandle,
    backtest_bytes: bytes,
    budget: ComputeBudget | None,
) -> None:
    """Derive the whole result from the candidate bytes before any of it is sealed."""
    measured = [
        _artifact_digest(workspace, handle.run_id, name) for name in (_ENVELOPE, _PREPARATION)
    ]
    _admit(
        budget,
        _DOCUMENT_OVERHEAD
        + _DOCUMENT_EXPANSION * (sum(size for _hash, size in measured) + len(backtest_bytes)),
        "run artifacts exceed materialization budget",
    )
    sealed = dict(zip((_ENVELOPE, _PREPARATION), measured, strict=True))
    if (sealed[_ENVELOPE][0], sealed[_PREPARATION][0]) != (
        handle.envelope_sha256,
        handle.preparation_sha256,
    ):
        # Checked before sealing: a stale handle must not leave an artifact behind that
        # a corrected commit could never replace.
        raise RunStorageError("sealed run inputs changed after the run was opened")
    _project(
        _read_sealed(workspace, handle.run_id, _ENVELOPE),
        _read_sealed(workspace, handle.run_id, _PREPARATION),
        backtest_bytes,
        handle.request_hash,
    )


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
    # A sealed artifact is never replaced, so everything the commit will check runs
    # against the candidate bytes first. A rejected result leaves the run open for a
    # corrected retry instead of stranding it behind an unreplaceable file.
    _check_candidate(workspace, handle, result.backtest_bytes, budget)
    _seal(workspace, handle.run_id, _BACKTEST, result.backtest_bytes)
    derived = _derive(workspace, handle.run_id, handle.request_hash, budget)
    if (
        derived.artifacts[_ENVELOPE] != handle.envelope_sha256
        or derived.artifacts[_PREPARATION] != handle.preparation_sha256
    ):
        raise RunStorageError("sealed run inputs changed after the run was opened")
    _write_marker(workspace, derived, handle.operation_id)
    return _finish(workspace, derived, handle.operation_id, budget=budget)


def fail_run(workspace: Workspace, handle: RunHandle, reason: str) -> dict[str, object]:
    """End a calculation that never produced a result. A committed run is refused."""
    require_run_schema(workspace)
    if workspace.state.in_transaction:
        raise RunStorageError("fail_run cannot run inside another state transaction")
    _require_open(workspace, handle)
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


def _recorded_metrics(workspace: Workspace, run_id: str) -> dict[str, dict[str, object]]:
    return {
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


def verify_run(
    workspace: Workspace, run_id: str, request_hash: str, *, budget: ComputeBudget | None = None
) -> _Derived:
    """Re-derive one successful run and refuse it unless every record still agrees."""
    operation = workspace.state.execute(
        "SELECT operation_id,phase,target_id,kind,request_hash FROM storage_operations "
        "WHERE kind=? AND target_id=?",
        (RUN_OPERATION_KIND, run_id),
    ).fetchone()
    if operation is None or operation["phase"] != "COMPLETED":
        raise RunStorageError("successful run has no completed intent")
    if operation["request_hash"] != request_hash:
        raise RunStorageError("run intent names a different request")
    marker = _marker(workspace, run_id, budget)
    if marker is None:
        raise RunStorageError("successful run has no result marker")
    derived = _derive(workspace, run_id, request_hash, budget)
    _require_marker_match(marker, operation["operation_id"], derived)
    hashes, counts = table_receipts(_stored_rows(workspace, run_id, _reserved(budget, derived)))
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
    if _recorded_metrics(workspace, run_id) != _metric_records(derived.metrics):
        raise RunStorageError("recorded metrics disagree with the sealed evidence")
    manifests = [
        tuple(row)
        for row in workspace.state.execute(
            "SELECT module,output_schema,content_hash,row_count FROM module_manifests "
            "WHERE run_id=?",
            (run_id,),
        )
    ]
    if manifests != [
        (derived.module, _MANIFEST_SCHEMA, derived.manifest, sum(derived.counts.values()))
    ]:
        raise RunStorageError("recorded module manifest disagrees with the sealed evidence")
    _require_recorded_provenance(workspace, derived, budget)
    return derived


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
    derived = verify_run(workspace, run_id, row["request_hash"], budget=budget)
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
        "metrics": _recorded_metrics(workspace, run_id),
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


def _quarantine_run(workspace: Workspace, run_id: str, operation_id: str, reason: str) -> bool:
    _terminate(workspace, run_id, operation_id, status="QUARANTINED", reason=reason)
    return True


def recover_run(
    workspace: Workspace, operation: sqlite3.Row, *, budget: ComputeBudget | None = None
) -> bool:
    """Finish or end one interrupted run. It never recalculates anything."""
    # Recovery runs from the CLI, which has no budget to hand down. Falling back to
    # the serial default keeps the materialization checks on rather than disabling
    # them on exactly the path that reads unverified evidence.
    budget = budget or ComputeBudget(Fraction(1), 512 * 1024 * 1024)
    run_id = str(operation["target_id"])
    operation_id = str(operation["operation_id"])
    request_hash = str(operation["request_hash"])
    row = workspace.state.execute("SELECT status FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        return False
    if row[0] != "RUNNING":
        _terminate(workspace, run_id, operation_id, status=row[0], reason="run already ended")
        return True
    try:
        marker = _marker(workspace, run_id, budget)
        derived = None if marker is None else _derive(workspace, run_id, request_hash, budget)
        if marker is not None and derived is not None:
            _require_marker_match(marker, operation_id, derived)
    except ComputeResourceError:
        # Refusing to materialize describes this machine, not the stored result. The
        # run stays open so a recovery with room to work can still finish it.
        raise
    except (ValueError, OSError) as error:
        # The quarantine reason is the only record an operator gets, so it carries the
        # actual failure rather than a fixed sentence.
        return _quarantine_run(
            workspace,
            run_id,
            operation_id,
            "result marker disagrees with the sealed evidence: " + str(error),
        )
    if derived is None:
        # Absence cannot separate an unfinished calculation from a rolled-back commit,
        # so the run ends conservatively and nothing on disk is reused or overwritten.
        _terminate(workspace, run_id, operation_id, status="INTERRUPTED", reason="no result marker")
        return True
    try:
        _finish(workspace, derived, operation_id, budget=budget)
    except ComputeResourceError:
        raise
    except (ValueError, OSError) as error:
        return _quarantine_run(
            workspace,
            run_id,
            operation_id,
            "stored result rows disagree with the sealed evidence: " + str(error),
        )
    return True
