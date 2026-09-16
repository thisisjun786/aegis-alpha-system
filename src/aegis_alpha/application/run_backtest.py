"""One integrated run: prepare stored inputs, record the intent, calculate, commit.

`run_backtest`, `read_backtest_run` and `list_backtest_runs` are the public Python
surface; `run_cli` is their argument adapter and holds no other logic, so a Python
call and a CLI call of the same request differ only in run identity.

Nothing here calculates or persists on its own. Preparation belongs to
`backtest_prepare`, accounting to `backtest_cli.run_document` and the run record to
`storage.runs`. The calculation holds no storage lock and no state transaction, so a
long replay never blocks another reader of the same installation.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import dataclass, replace
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, cast

from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.application.backtest_prepare import prepare_backtest
from aegis_alpha.application.compute_cli import price_compute
from aegis_alpha.application.prepare_cli import (
    admitted_path,
    read_prepare_request,
    require_new_outputs,
    seal_outputs,
)
from aegis_alpha.compute_resources import ComputeResourceError
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.backtest_requests import read_backtest_request, register_backtest_request
from aegis_alpha.storage.input_pins import InputBundleRef, register_input_bundle
from aegis_alpha.storage.locks import private_directory, storage_lock_targets
from aegis_alpha.storage.paths import load_paths, resolve_home
from aegis_alpha.storage.run_schema import require_run_schema
from aegis_alpha.storage.runs import (
    RunHandle,
    RunIntent,
    RunResult,
    RunStorageError,
    RunStrategyPin,
    commit_run,
    fail_run,
    list_runs,
    open_run,
    read_run,
)
from aegis_alpha.storage.workspace import Workspace, open_workspace

if TYPE_CHECKING:
    from aegis_alpha.application.backtest_prepare import (
        PreparedBacktest,
        PrepareRequest,
        StrategyPin,
    )
    from aegis_alpha.compute_resources import ComputeBudget

__all__ = [
    "DEFAULT_REASON",
    "RunBacktestRequest",
    "list_backtest_runs",
    "read_backtest_run",
    "run_backtest",
]

DEFAULT_REASON = "integrated prepared backtest"
_BUNDLE_SCHEMA = "aas-input-bundle-v1"
_BUNDLE_NAME_SCHEMA = "aas-run-input-bundle-name-v1"
_HASH_FORMAT = "aas-canonical-json-sha256-v1"
# A failure reason is stored and read back on every verification, so what an arbitrary
# exception carries is bounded here rather than written through at whatever length.
_MAX_REASON_CHARS = 512
# A sealed document is decoded whole before it is used, so it is charged at the same
# expansion the run store already charges for decoding one of its own artifacts.
_DOCUMENT_EXPANSION = 128


@dataclass(frozen=True, slots=True)
class RunBacktestRequest:
    """One integrated execution request.

    Only the inputs a caller legitimately owns are accepted. Every recorded identity
    (engine, environment, strategy pin, module) is derived from the registered request
    instead, so a run can never describe a calculation nobody performed.
    """

    request: Path
    request_sha256: str
    home: Path | None = None
    reason: str = DEFAULT_REASON
    bundle_id: str | None = None
    prior_run_id: str | None = None
    run_id: str | None = None
    envelope_output: Path | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.request, Path):
            raise TypeError("request must be a path to the exact prepare-request document")
        for name in ("request_sha256", "reason"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(name + " must be nonempty text")
        for name in ("bundle_id", "prior_run_id", "run_id"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(name + " must be nonempty text when supplied")


@dataclass(frozen=True, slots=True)
class _Opened:
    """What stage A produced: a durable run, its bundle and the installation it ran on."""

    handle: RunHandle
    bundle: InputBundleRef
    installation: tuple[str, str, str, str | None]


@dataclass(frozen=True, slots=True)
class _Carried:
    """What outlives the whole preparation: the receipt's evidence and the calculation input."""

    request_hash: str
    envelope_bytes: bytes
    envelope_sha256: str
    preparation_sha256: str


@dataclass(frozen=True, slots=True)
class _Opening:
    """Exactly what stage A reads, so nothing else stays resident while it materializes.

    The decisions, features, slots, inputs and definition are the preparation's bulk and
    no later stage reads any of them. Extracting these serialized documents first lets
    the whole `PreparedBacktest` graph be released before a single run is opened.
    """

    request_hash: str
    projection_bytes: bytes
    envelope_bytes: bytes
    provenance_bytes: bytes
    strategy: StrategyPin


def _carry(prepared: PreparedBacktest) -> tuple[_Opening, _Carried]:
    """Take every byte the later stages need, so the preparation can be dropped at once."""
    opening = _Opening(
        request_hash=prepared.request_hash,
        projection_bytes=prepared.projection.canonical_bytes,
        envelope_bytes=prepared.envelope.canonical_bytes,
        provenance_bytes=prepared.provenance,
        strategy=prepared.request.strategy,
    )
    carried = _Carried(
        request_hash=prepared.request_hash,
        envelope_bytes=prepared.envelope.canonical_bytes,
        envelope_sha256=prepared.envelope.envelope_sha256,
        preparation_sha256=hashlib.sha256(prepared.provenance).hexdigest(),
    )
    return opening, carried


def _sealed_targets(envelope_bytes: bytes) -> dict[str, dict[str, object]]:
    """Read the decision weights back from the sealed envelope.

    The preparation holds the same mapping, but keeping either a copy or a reference to
    it would carry the decoded graph through every later stage for one receipt field.
    The envelope is retained and reserved anyway, and it is the authority the result was
    projected against, so the reported weights are by construction the ones that ran.
    """
    envelope = _object(decode_json(envelope_bytes), "envelope")
    targets = _object(envelope.get("targets"), "envelope targets")
    return {
        _text(day, "target date"): dict(sorted(_object(weights, "target weights").items()))
        for day, weights in sorted(targets.items())
    }


def _object(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(field + " must be a JSON object")
    return cast("dict[str, object]", value)


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(field + " must be nonempty text")
    return value


def _installation(workspace: Workspace) -> tuple[str, str, str, str | None]:
    """The store identities a reopened workspace must still present before a commit."""
    return (
        workspace.installation_id,
        workspace.state.execute("SELECT store_id FROM store_info").fetchone()[0],
        workspace.market.execute("SELECT store_id FROM store_info").fetchall()[0][0],
        None
        if workspace.strategies is None
        else workspace.strategies.execute("SELECT store_id FROM store_info").fetchone()[0],
    )


def _bundle_id(explicit: str | None, bindings: object, request_hash: str) -> str:
    """Name the input bundle after its content unless the caller names it.

    The request hash is part of the name, not only the bindings. Storage keeps exactly
    one immutable request per bundle, so two requests that pin the same inputs while
    differing in period, account or schedule need two bundle names; sharing one would
    make the second run fail on a stored-request mismatch instead of running.
    """
    if explicit is not None:
        return explicit
    return "inputs-" + content_sha256(
        {
            "schema": _BUNDLE_NAME_SCHEMA,
            "hash_format": _HASH_FORMAT,
            "bindings": bindings,
            "request_hash": request_hash,
        }
    )


def _intent(opening: _Opening, request: RunBacktestRequest, bundle_id: str) -> RunIntent:
    """Fill every RunIntent field from the request this run is about to register.

    The engine and environment hashes come from the projected canonical request rather
    than from the identity objects themselves: storage hashes exactly those decoded
    members, and a differently normalized copy would be refused at open time.
    """
    projected = _object(decode_json(opening.projection_bytes), "projected request")
    envelope = _object(decode_json(opening.envelope_bytes), "envelope")
    strategy = opening.strategy
    return RunIntent(
        request_hash=opening.request_hash,
        bundle_id=bundle_id,
        engine_hash=content_sha256(_object(projected.get("engine"), "request engine")),
        environment_hash=content_sha256(
            _object(projected.get("environment"), "request environment")
        ),
        reason=request.reason,
        envelope_bytes=opening.envelope_bytes,
        preparation_bytes=opening.provenance_bytes,
        strategy_pins=(
            RunStrategyPin(
                # Read from the sealed envelope: storage places the single pin on the
                # envelope module and refuses a result produced for another one.
                module=_text(envelope.get("module"), "envelope module"),
                ordinal=0,
                store_id=strategy.strategy_store_id,
                strategy_id=strategy.strategy_id,
                version=strategy.version,
                raw_hash=strategy.raw_sha256,
                contract_hash=strategy.contract_sha256,
            ),
        ),
        prior_run_id=request.prior_run_id,
        run_id=request.run_id,
    )


def _prepare(home: Path, request: PrepareRequest, budget: ComputeBudget) -> PreparedBacktest:
    """Stage 0: SELECT-only preparation, after refusing an installation without the add-on."""
    with open_workspace(home) as workspace:
        # Checked before any calculation, so a missing add-on costs nothing and names
        # its install command instead of failing at the first durable write.
        require_run_schema(workspace)
        return prepare_backtest(workspace, request, budget=budget)


def _retaining(budget: ComputeBudget, *held: bytes) -> ComputeBudget:
    """Charge what this command keeps live against what a later stage may materialize.

    The prepared envelope and provenance stay referenced until the receipt is built, and
    the accounting response stays referenced until the result is sealed. Handing a later
    stage the unchanged allowance would let two materializations that each fit exceed the
    allocation together, which is the same reservation storage makes when one projection
    stays live beside another. `ComputeBudget` refuses a reservation that leaves no
    allowance at all, so a run too large for this host stops before that stage runs.
    """
    return replace(budget, reserved_bytes=budget.reserved_bytes + sum(len(item) for item in held))


def _admit(budget: ComputeBudget, size: int) -> None:
    """Charge a materialization before it happens, never after it is in memory."""
    if size * _DOCUMENT_EXPANSION > budget.available_bytes:
        raise ComputeResourceError("accounting exceeds the remaining materialization budget")


def _require_exportable(home: Path, output: Path) -> None:
    """Refuse an export that could collide with the run's own immutable artifacts.

    The run store accepts identical bytes under a sealed name as a resumed seal, so an
    export written into the managed runs directory can become the artifact itself.
    Removing the export afterwards would then make the run unreadable.
    """
    runs = Path(os.path.realpath(load_paths(home).runs))
    parent = Path(os.path.realpath(output.parent))
    if parent == runs or runs in parent.parents:
        raise ValueError("an envelope export cannot be written inside the managed runs directory")


def _open(
    home: Path, opening: _Opening, request: RunBacktestRequest, budget: ComputeBudget
) -> _Opened:
    """Stage A: one short write that registers the inputs and commits the run intent."""
    projected = _object(decode_json(opening.projection_bytes), "projected request")
    bindings = projected.get("bindings")
    document = canonical_json_bytes(
        {
            "schema": _BUNDLE_SCHEMA,
            "hash_format": _HASH_FORMAT,
            "bundle_id": _bundle_id(request.bundle_id, bindings, opening.request_hash),
            "bindings": bindings,
        }
    )
    with open_workspace(home, writable=True) as workspace:
        bundle = register_input_bundle(
            workspace,
            document,
            expected_file_sha256=hashlib.sha256(document).hexdigest(),
            budget=budget,
        )
        register_backtest_request(
            workspace,
            bundle,
            opening.projection_bytes,
            expected_request_hash=opening.request_hash,
            budget=budget,
        )
        handle = open_run(workspace, _intent(opening, request, bundle.bundle_id), budget=budget)
        return _Opened(handle, bundle, _installation(workspace))


def _record(
    home: Path, opened: _Opened, result: RunResult, request_hash: str, budget: ComputeBudget
) -> dict[str, object]:
    """Stage C: reopen briefly, re-authenticate installation and pins, then commit."""
    with open_workspace(home, writable=True) as workspace:
        if _installation(workspace) != opened.installation:
            raise ValueError("installation identity changed after the inputs were prepared")
        # Re-reading the registered request re-verifies every binding, so a generation
        # or pinned document that moved during the calculation is refused before a
        # result is recorded rather than after.
        read_backtest_request(
            workspace, opened.bundle, expected_request_hash=request_hash, budget=budget
        )
        return commit_run(workspace, opened.handle, result, budget=budget)


def _abandon(home: Path, handle: RunHandle, reason: str) -> str:
    """End a run that recorded no result and report what happened to it.

    The caller must still see its own failure, so nothing here is raised over it. What
    this attempt did is returned instead of discarded: a refusal and an unexpected
    storage fault look identical to a suppressing caller, and only one of them is
    normal. A run whose market marker is already committed belongs to `recover_run`,
    and `fail_run` refuses it by design.
    """
    try:
        with open_workspace(home, writable=True) as workspace:
            fail_run(workspace, handle, reason[:_MAX_REASON_CHARS])
    except RunStorageError as refused:
        return handle.run_id + " was left for 'aas db recover' (" + _describe(refused) + ")"
    except BaseException as failure:  # noqa: BLE001 -- the caller's own failure must survive
        # Deliberately widest. Lock contention, a driver fault, an interrupt arriving
        # during cleanup and an outright bug in this path all land here, and any of them
        # raised onward would replace the error the caller actually needs. The command is
        # already failing, so the run's fate is reported and the original error is what
        # propagates. Nothing is hidden: every outcome is named in the returned text.
        return (
            handle.run_id + " could not be ended (" + _describe(failure) + "); run 'aas db recover'"
        )
    return handle.run_id + " ended FAILED"


def _describe(error: BaseException) -> str:
    """Name a failure without trusting its own formatting.

    This runs while another exception is already in flight, so an error whose `__str__`
    raises must not become the failure the caller sees instead of its own.
    """
    try:
        return type(error).__name__ + ": " + str(error)
    except BaseException:  # noqa: BLE001 -- an unprintable failure is still a failure
        return type(error).__name__ + ": <unprintable>"


@dataclass(frozen=True, slots=True)
class _Outcome:
    """What the calculation and the commit produced, before it is shaped for a caller."""

    response: dict[str, object]
    recorded: dict[str, object]
    exported: dict[str, dict[str, str]] | None


def _receipt(
    request: RunBacktestRequest,
    carried: _Carried,
    opened: _Opened,
    outcome: _Outcome,
) -> dict[str, object]:
    """Returned only after the result is stored. Only `run.run_id` is execution specific."""
    return {
        "executed": True,
        "request_sha256": request.request_sha256,
        "request_hash": carried.request_hash,
        "bundle_id": opened.bundle.bundle_id,
        "envelope": {"sha256": carried.envelope_sha256},
        "preparation": {"sha256": carried.preparation_sha256},
        "exported": outcome.exported,
        # Read back from the sealed envelope: nothing of the preparation is retained.
        "target_weights": _sealed_targets(carried.envelope_bytes),
        "backtest": outcome.response,
        "run": outcome.recorded,
        "certified": False,
    }


def _staged(
    home: Path,
    parsed: PrepareRequest,
    request: RunBacktestRequest,
    budget: ComputeBudget,
    output: Path | None,
) -> dict[str, object]:
    """The four stages. Only A and C hold a writable workspace, and neither calculates."""
    if output is None:
        prepared = _prepare(home, parsed, budget)
        exported = None
    else:
        with DescriptorTree.open_path(output.parent) as tree:
            require_new_outputs(tree, output.name)
            prepared = _prepare(home, parsed, budget)
            exported = seal_outputs(
                tree, output.name, prepared.envelope.canonical_bytes, prepared.provenance
            )
    # Take what the later stages read, then release the preparation before anything
    # else materializes. No stage after this runs beside the decisions and features.
    opening, carried = _carry(prepared)
    del prepared
    retained = _retaining(
        budget, opening.projection_bytes, opening.envelope_bytes, opening.provenance_bytes
    )
    opened = _open(home, opening, request, retained)
    try:
        # The run is durable now, so every failure from here ends it rather than
        # stranding it, including one raised while narrowing what is still held.
        del opening
        retained = _retaining(budget, carried.envelope_bytes)
        # Stage B. No storage lock and no state transaction is held across this call.
        # run_document takes no budget of its own, so the decode it is about to perform
        # is admitted here against what this command still holds live. A refusal ends the
        # run cleanly instead of letting the host kill a process mid-calculation.
        _admit(retained, len(carried.envelope_bytes))
        response = run_document(carried.envelope_bytes, carried.envelope_sha256)
        result = RunResult(canonical_json_bytes(response))
        # The sealed bytes are the response. Dropping the decoded copy keeps it from
        # spanning the commit, which decodes and projects those same bytes again; the
        # receipt reads it back afterwards, from exactly what was stored.
        del response
    except BaseException as error:
        error.add_note(
            "run "
            + _abandon(home, opened.handle, "calculation produced no result: " + _describe(error))
        )
        raise
    try:
        recorded = _record(
            home,
            opened,
            result,
            carried.request_hash,
            _retaining(retained, result.backtest_bytes),
        )
    except BaseException as error:
        error.add_note(
            "run " + _abandon(home, opened.handle, "result was not recorded: " + _describe(error))
        )
        raise
    sealed = _object(decode_json(result.backtest_bytes), "sealed backtest result")
    return _receipt(request, carried, opened, _Outcome(sealed, recorded, exported))


def _admitted(home: Path | None) -> tuple[Path, tuple[Path, ...], type[Exception]]:
    """Resolve the installation and load the optional driver, as every native command does."""
    resolved = resolve_home(home)
    private_directory(resolved)
    targets = storage_lock_targets(resolved, load_paths(resolved).stores())
    return resolved, targets, cast("type[Exception]", import_module("duckdb").Error)


def run_backtest(request: RunBacktestRequest) -> dict[str, object]:
    """Prepare, register, calculate and record one run of a registered strategy.

    Returns only after the result is durably committed, so a receipt always implies a
    stored SUCCESS. A failure returns no receipt. It can still leave evidence behind on
    purpose: an `--envelope-output` export is written before the run is opened, and a
    run interrupted while its inputs are being sealed stays RUNNING for `aas db recover`
    rather than being force-failed, exactly as the run lifecycle defines.
    """
    parsed = read_prepare_request(request.request, request.request_sha256)
    output = None if request.envelope_output is None else admitted_path(request.envelope_output)
    home, targets, database_error = _admitted(request.home)
    if output is not None:
        _require_exportable(home, output)
    try:
        with price_compute(excluded_locks=targets) as budget:
            if budget is None:
                raise ValueError("a run requires the explicit AAS compute budget environment")
            return _staged(home, parsed, request, budget, output)
    except (sqlite3.Error, database_error) as error:
        raise _translated(error) from None


def _translated(error: Exception) -> ValueError:
    """Replace a driver fault with its operator message, keeping what happened to the run."""
    translated = ValueError("local database operation failed; run aas db verify")
    for note in getattr(error, "__notes__", ()):
        translated.add_note(note)
    return translated


def read_backtest_run(run_id: str, *, home: Path | None = None) -> dict[str, object]:
    """Re-derive one recorded run from its sealed evidence and return what still agrees."""
    resolved, targets, database_error = _admitted(home)
    try:
        with (
            price_compute(excluded_locks=targets) as budget,
            open_workspace(resolved) as workspace,
        ):
            return {
                "run": read_run(workspace, _text(run_id, "run_id"), budget=budget),
                "research_only": True,
                "certified": False,
            }
    except (sqlite3.Error, database_error) as error:
        raise _translated(error) from None


def list_backtest_runs(*, home: Path | None = None) -> dict[str, object]:
    """List every recorded run, whatever its outcome. This verifies no result."""
    resolved, _targets, database_error = _admitted(home)
    try:
        with open_workspace(resolved) as workspace:
            # Named explicitly, so an installation without the add-on reports what to
            # install rather than an opaque missing-table failure.
            require_run_schema(workspace)
            return {"runs": list_runs(workspace), "research_only": True, "certified": False}
    except (sqlite3.Error, database_error) as error:
        raise _translated(error) from None
