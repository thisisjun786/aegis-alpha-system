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
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.application.backtest_prepare import (
    PrepareRequest,
    parse_prepare_request,
    prepare_backtest,
)
from aegis_alpha.application.compute_cli import price_compute
from aegis_alpha.application.prepare_cli import (
    admitted_path,
    read_request_bytes,
    require_new_outputs,
    seal_outputs,
)
from aegis_alpha.application.run_staging import (
    MAX_REASON_BYTES,
    RUN_ID,
    Opened,
    abandon,
    admit,
    bundle_name,
    describe,
    installation_identity,
    json_object,
    json_text,
    record,
    require_exportable,
    resolve_installation,
    retaining,
    sealed_targets,
    translated,
)
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.backtest_requests import register_backtest_request
from aegis_alpha.storage.input_pins import BUNDLE_SCHEMA, HASH_FORMAT, register_input_bundle
from aegis_alpha.storage.run_schema import require_run_schema
from aegis_alpha.storage.runs import (
    RunIntent,
    RunResult,
    RunStrategyPin,
    list_runs,
    open_run,
    read_run,
)
from aegis_alpha.storage.workspace import open_workspace

if TYPE_CHECKING:
    from aegis_alpha.application.backtest_prepare import PreparedBacktest, StrategyPin
    from aegis_alpha.compute_resources import ComputeBudget

__all__ = [
    "DEFAULT_REASON",
    "RunBacktestRequest",
    "list_backtest_runs",
    "read_backtest_run",
    "run_backtest",
]

DEFAULT_REASON = "integrated prepared backtest"
# The bundle name a certified run derives when the caller does not supply one.
_BUNDLE_PREFIX = "inputs-"


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
        if len(self.reason.encode()) > MAX_REASON_BYTES:
            raise ValueError("reason is too large to record")
        if self.run_id is not None and not RUN_ID.fullmatch(self.run_id):
            raise ValueError("run_id must be a plain identifier")
        if self.run_id is not None and self.run_id == self.prior_run_id:
            # The run store refuses a self-referencing predecessor too, but only once
            # stage A has registered; refused here so a bad intent strands nothing.
            raise ValueError("a run cannot be its own predecessor")


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


def _intent(
    opening: _Opening,
    request: RunBacktestRequest,
    bundle_id: str,
    projected: dict[str, object],
) -> RunIntent:
    """Fill every RunIntent field from the request this run is about to register.

    The engine and environment hashes come from the projected canonical request rather
    than from the identity objects themselves: storage hashes exactly those decoded
    members, and a differently normalized copy would be refused at open time. The
    projection is decoded once by the caller and passed in, never decoded twice.
    """
    envelope = json_object(decode_json(opening.envelope_bytes), "envelope")
    strategy = opening.strategy
    return RunIntent(
        request_hash=opening.request_hash,
        bundle_id=bundle_id,
        engine_hash=content_sha256(json_object(projected.get("engine"), "request engine")),
        environment_hash=content_sha256(
            json_object(projected.get("environment"), "request environment")
        ),
        reason=request.reason,
        envelope_bytes=opening.envelope_bytes,
        preparation_bytes=opening.provenance_bytes,
        strategy_pins=(
            RunStrategyPin(
                # Read from the sealed envelope: storage places the single pin on the
                # envelope module and refuses a result produced for another one.
                module=json_text(envelope.get("module"), "envelope module"),
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


def _prepare(home: Path, raw: bytes, budget: ComputeBudget) -> PreparedBacktest:
    """Stage 0: SELECT-only preparation, after refusing an installation without the add-on.

    The request document is decoded here rather than by the caller, so the object graph
    it expands into is built inside the compute lease instead of beside it, where
    concurrent runs would each materialize one with nothing serializing them.
    """
    with open_workspace(home) as workspace:
        # Checked before any calculation, so a missing add-on costs nothing and names
        # its install command instead of failing at the first durable write.
        require_run_schema(workspace)
        request = PrepareRequest(parse_prepare_request(raw))
        # The bytes and the graph they expand into stay live for the whole preparation,
        # so the readers below see an allowance that already excludes them. Measured on
        # the serialized form, which is what can be measured; the expansion is not.
        return prepare_backtest(workspace, request, budget=retaining(budget, raw))


def _open(
    home: Path, opening: _Opening, request: RunBacktestRequest, budget: ComputeBudget
) -> Opened:
    """Stage A: one short write that registers the inputs and commits the run intent."""
    projected = json_object(decode_json(opening.projection_bytes), "projected request")
    bindings = projected.get("bindings")
    document = canonical_json_bytes(
        {
            "schema": BUNDLE_SCHEMA,
            "hash_format": HASH_FORMAT,
            "bundle_id": bundle_name(
                request.bundle_id, bindings, opening.request_hash, prefix=_BUNDLE_PREFIX
            ),
            "bindings": bindings,
        }
    )
    with open_workspace(home, writable=True) as workspace:
        # Registration is durable and a bundle name binds one request for good, so the
        # run-only fields are checked first. open_run cannot run inside another
        # transaction, so stage A cannot be one atomic write; refusing a predecessor
        # nobody recorded is what keeps a bad run field from stranding a registration.
        if (
            request.prior_run_id is not None
            and not workspace.state.execute(
                "SELECT 1 FROM runs WHERE run_id=?", (request.prior_run_id,)
            ).fetchone()
        ):
            raise ValueError("prior_run_id names no recorded run")
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
        intent = _intent(opening, request, bundle.bundle_id, projected)
        handle = open_run(workspace, intent, budget=budget)
        return Opened(handle, bundle, installation_identity(workspace))


@dataclass(frozen=True, slots=True)
class _Outcome:
    """What the calculation and the commit produced, before it is shaped for a caller."""

    response: dict[str, object]
    recorded: dict[str, object]
    exported: dict[str, dict[str, str]] | None


def _receipt(
    request: RunBacktestRequest,
    carried: _Carried,
    opened: Opened,
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
        "target_weights": sealed_targets(carried.envelope_bytes),
        "backtest": outcome.response,
        "run": outcome.recorded,
        "certified": False,
    }


def _staged(
    home: Path,
    carrier: list[bytes],
    request: RunBacktestRequest,
    budget: ComputeBudget,
    output: Path | None,
) -> dict[str, object]:
    """The four stages. Only A and C hold a writable workspace, and neither calculates.

    The request bytes arrive in a one-slot carrier that stage 0 empties as it takes them,
    so no frame here keeps them or the document they decode into alive beside a later
    stage. The decoded form lives inside PreparedBacktest until del prepared drops it.
    """
    if output is None:
        prepared = _prepare(home, carrier.pop(), budget)
        exported = None
    else:
        with DescriptorTree.open_path(output.parent) as tree:
            require_new_outputs(tree, output.name)
            prepared = _prepare(home, carrier.pop(), budget)
            exported = seal_outputs(
                tree, output.name, prepared.envelope.canonical_bytes, prepared.provenance
            )
    # Take what the later stages read, then release the preparation before anything
    # else materializes. No stage after this runs beside the decisions and features.
    opening, carried = _carry(prepared)
    del prepared
    retained = retaining(
        budget, opening.projection_bytes, opening.envelope_bytes, opening.provenance_bytes
    )
    opened = _open(home, opening, request, retained)
    try:
        # The run is durable now, so every failure from here ends it rather than
        # stranding it, including one raised while narrowing what is still held.
        del opening
        retained = retaining(budget, carried.envelope_bytes)
        # Stage B. No storage lock and no state transaction is held across this call.
        # run_document takes no budget of its own, so the decode it is about to perform
        # is admitted here against what this command still holds live. A refusal ends the
        # run cleanly instead of letting the host kill a process mid-calculation.
        admit(retained, len(carried.envelope_bytes))
        response = run_document(carried.envelope_bytes, carried.envelope_sha256)
        result = RunResult(canonical_json_bytes(response))
        # The sealed bytes are the response. Dropping the decoded copy keeps it from
        # spanning the commit, which decodes and projects those same bytes again; the
        # receipt reads it back afterwards, from exactly what was stored.
        del response
    except BaseException as error:
        error.add_note(
            "run "
            + abandon(home, opened.handle, "calculation produced no result: " + describe(error))
        )
        raise
    try:
        recorded = record(
            home,
            opened,
            result,
            carried.request_hash,
            retaining(retained, result.backtest_bytes),
        )
    except BaseException as error:
        error.add_note(
            "run " + abandon(home, opened.handle, "result was not recorded: " + describe(error))
        )
        raise
    # The run is stored. Both documents the receipt reports are decoded from the sealed
    # bytes, so the charge is made before either is expanded; a refusal here leaves a
    # readable SUCCESS run rather than a wrong receipt.
    admit(retained, len(result.backtest_bytes) + len(carried.envelope_bytes))
    sealed = json_object(decode_json(result.backtest_bytes), "sealed backtest result")
    return _receipt(request, carried, opened, _Outcome(sealed, recorded, exported))


def run_backtest(request: RunBacktestRequest) -> dict[str, object]:
    """Prepare, register, calculate and record one run of a registered strategy.

    Returns only after the result is durably committed, so a receipt always implies a
    stored SUCCESS. A failure returns no receipt. It can still leave evidence behind on
    purpose: an `--envelope-output` export is written before the run is opened, and a
    run interrupted while its inputs are being sealed stays RUNNING for `aas db recover`
    rather than being force-failed, exactly as the run lifecycle defines.
    """
    # Read and hash-checked here because that is bounded and cheap, then handed over in
    # a one-slot carrier. Stage 0 decodes it under the lease and this frame stops
    # referencing it, rather than expanding it outside the lease and holding it.
    carrier = [read_request_bytes(request.request, request.request_sha256)]
    output = None if request.envelope_output is None else admitted_path(request.envelope_output)
    home, targets, database_error = resolve_installation(request.home)
    if output is not None:
        require_exportable(home, output)
    try:
        with price_compute(excluded_locks=targets) as budget:
            if budget is None:
                raise ValueError("a run requires the explicit AAS compute budget environment")
            return _staged(home, carrier, request, budget, output)
    except (sqlite3.Error, database_error) as error:
        raise translated(error) from None


def read_backtest_run(run_id: str, *, home: Path | None = None) -> dict[str, object]:
    """Re-derive one recorded run from its sealed evidence and return what still agrees."""
    resolved, targets, database_error = resolve_installation(home)
    try:
        with (
            price_compute(excluded_locks=targets) as budget,
            open_workspace(resolved) as workspace,
        ):
            return {
                "run": read_run(workspace, json_text(run_id, "run_id"), budget=budget),
                "research_only": True,
                "certified": False,
            }
    except (sqlite3.Error, database_error) as error:
        raise translated(error) from None


def list_backtest_runs(*, home: Path | None = None) -> dict[str, object]:
    """List every recorded run, whatever its outcome. This verifies no result."""
    resolved, _targets, database_error = resolve_installation(home)
    try:
        with open_workspace(resolved) as workspace:
            # Named explicitly, so an installation without the add-on reports what to
            # install rather than an opaque missing-table failure.
            require_run_schema(workspace)
            return {"runs": list_runs(workspace), "research_only": True, "certified": False}
    except (sqlite3.Error, database_error) as error:
        raise translated(error) from None
