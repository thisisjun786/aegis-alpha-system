"""One integrated declared research run: prepare, register, calculate, commit, requery.

`run_research` and `rerun_research_run` are the installed Python surface of the declared
uncertified path and `run_cli` is their argument adapter, so a CLI run and a Python run of
one declaration differ in nothing. Before this module the declared lifecycle existed only
as functions a test could call in sequence: nothing an operator installs could record a
declared run, which left its durable record unreachable from the product it belongs to.

Requery, listing and verification are not repeated here. `aas run show` already re-derives
any recorded run from its sealed evidence and its payload names the contract the run was
opened under, so a second reader would be a second answer to one question.

Nothing here makes an observation executable. No admission the strict path owns is
called: the preparation reads the panel through `load_pinned_observations`, the accounting
runs under the one mode whose response states in its own bytes that it is uncertified and
non-executable, and the run store pairs a declaration with its own sealed preparation
rather than with a certified request. The refusals `admit_native_input`, the price reader
and `_reject_observation_contract` put on this data are untouched by this path.
"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.application.backtest_prepare import prepare_research_run
from aegis_alpha.application.compute_cli import price_compute
from aegis_alpha.application.prepare_cli import admitted_path, require_new_outputs, seal_outputs
from aegis_alpha.application.research_run import (
    EXECUTION_MODE,
    RESEARCH_COMPOSITION_SCHEMA,
    RESEARCH_RUN_SCHEMA,
    parse_declared_request,
)
from aegis_alpha.application.run_staging import (
    MAX_REASON_BYTES,
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
from aegis_alpha.storage.backtest_requests import register_backtest_request, research_bindings
from aegis_alpha.storage.input_pins import BUNDLE_SCHEMA, HASH_FORMAT, register_input_bundle
from aegis_alpha.storage.run_schema import require_request_schema, require_run_schema
from aegis_alpha.storage.runs import (
    RunIntent,
    RunResult,
    RunStrategyPin,
    open_run,
    read_run,
    read_run_evidence,
)
from aegis_alpha.storage.workspace import open_workspace

if TYPE_CHECKING:
    from aegis_alpha.application.backtest_prepare import PreparedResearchRun
    from aegis_alpha.application.research_run import ResearchRunRequest
    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.runs import RunEvidence

__all__ = [
    "DEFAULT_REASON",
    "RunResearchRequest",
    "rerun_research_run",
    "run_research",
]

DEFAULT_REASON = "declared uncertified research run"
# Named apart from a certified run's bundles, because the two are registered in the same
# table and an operator reading it should not have to resolve a hash to tell them apart.
_BUNDLE_PREFIX = "research-inputs-"
# The request store refuses a registered request above this size, so a declaration too
# large to record is refused before anything is prepared rather than after.
_MAX_DECLARATION_BYTES = 1024 * 1024
# The two contracts a declaration can be, as the run records them.
_DECLARED_SCHEMAS = frozenset({RESEARCH_RUN_SCHEMA, RESEARCH_COMPOSITION_SCHEMA})


@dataclass(frozen=True, slots=True)
class RunResearchRequest:
    """One integrated declared-run request.

    There is no run_id field, and that is the point. A declared run's identity is derived
    from the declaration by the preparation, so the same declaration always names the same
    run and a caller cannot file one calculation under a name of its own choosing. Every
    other recorded identity is taken from the declaration or its sealed preparation for
    the same reason.
    """

    declaration: Path
    declaration_sha256: str
    home: Path | None = None
    reason: str = DEFAULT_REASON
    bundle_id: str | None = None
    prior_run_id: str | None = None
    envelope_output: Path | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.declaration, Path):
            raise TypeError("declaration must be a path to the exact declared-run document")
        for name in ("declaration_sha256", "reason"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(name + " must be nonempty text")
        for name in ("bundle_id", "prior_run_id"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(name + " must be nonempty text when supplied")
        if len(self.reason.encode()) > MAX_REASON_BYTES:
            raise ValueError("reason is too large to record")


@dataclass(frozen=True, slots=True)
class _Declared:
    """The declaration in the three forms the stages need, none of them the file's bytes.

    The file may be formatted however its author left it. What gets registered, hashed
    and bound is the canonical form, which is the only one the request store accepts and
    the only one the declaration's own content hash covers.
    """

    request: ResearchRunRequest
    canonical: bytes
    bindings: list[dict[str, object]]


@dataclass(frozen=True, slots=True)
class _Carried:
    """Everything the later stages read, so the preparation can be released at once.

    The decisions, panels and slots are the preparation's bulk and no stage after the
    envelope reads any of them. Taking these first lets the whole PreparedResearchRun
    graph be dropped before a run is opened.
    """

    run_id: str
    request_hash: str
    schema_version: str
    scope: str
    envelope_bytes: bytes
    envelope_sha256: str
    provenance_bytes: bytes
    preparation_sha256: str
    engine_hash: str
    environment_hash: str
    pins: tuple[RunStrategyPin, ...]
    composition: dict[str, object] | None


@dataclass(frozen=True, slots=True)
class _Outcome:
    """What the calculation and the commit produced, before it is shaped for a caller."""

    response: dict[str, object]
    recorded: dict[str, object]
    exported: dict[str, dict[str, str]] | None


def _read_declaration(path: Path, expected_sha256: str) -> bytes:
    """Read the exact declaration document, refusing anything but its own bytes."""
    document = admitted_path(path)
    with DescriptorTree.open_path(document.parent) as tree:
        raw = tree.read_bytes(document.name, max_bytes=_MAX_DECLARATION_BYTES)
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("declaration SHA-256 mismatch")
    return raw


def _declared(raw: bytes) -> _Declared:
    """Parse the declaration, then derive the bundle it must be registered against.

    Parsed before the bindings are derived: the contract parser refuses anything that is
    not one of the two declared documents, so the binding rule is never handed an
    executable request whose root it would read as a declaration missing fields.
    """
    body = json_object(decode_json(raw), "declaration")
    canonical = canonical_json_bytes(body)
    request = parse_declared_request(canonical)
    return _Declared(request, canonical, research_bindings(body))


def _binding_report(declared: _Declared) -> dict[str, object]:
    """What the bundle binds, and what only the declaration's own hash covers.

    A sleeve run binds its membership, and that binding is the whole bundle. A
    composition pins one membership per sleeve while the binding vocabulary holds a
    single membership, so binding either one would leave run.bundle_id describing half
    the run while looking complete; the store binds nothing for a composition instead.
    That is a weaker tie and it is reported as one rather than smoothed over: whatever
    stays unbound is covered by the declaration's content hash, which the run records as
    its request_hash, and by nothing else.
    """
    composed = declared.request.composition is not None
    return {
        "bound_roles": [str(binding.get("role")) for binding in declared.bindings],
        "membership_bound": not composed,
        "covered_by_declaration_hash_only": [
            "calendar",
            "observations",
            *(
                ["composition.sleeves.defense.membership", "composition.sleeves.offense.membership"]
                if composed
                else []
            ),
        ],
    }


def _pins(declaration: ResearchRunRequest, module: str) -> tuple[RunStrategyPin, ...]:
    """The sleeves this run records, in the order the run store reads them back.

    A sleeve run records one. A composition records two, because the switch chooses
    between them decision by decision and a record naming only the offense would describe
    a calculation the defense also took part in. Nothing but the ordinal tells the two
    apart in the run's own rows, so the offense is always zero.
    """
    offense = RunStrategyPin(
        module=module,
        ordinal=0,
        store_id=declaration.strategy_store_id,
        strategy_id=declaration.strategy_id,
        version=declaration.strategy_version,
        raw_hash=declaration.strategy_raw_sha256,
        contract_hash=declaration.strategy_contract_sha256,
    )
    if declaration.composition is None:
        return (offense,)
    defense = declaration.composition.defense
    return (
        offense,
        RunStrategyPin(
            module=module,
            ordinal=1,
            store_id=defense.strategy_store_id,
            strategy_id=defense.strategy_id,
            version=defense.version,
            raw_hash=defense.raw_sha256,
            contract_hash=defense.contract_sha256,
        ),
    )


def _composition(sealed: dict[str, object]) -> dict[str, object] | None:
    """What the sealed preparation says the switch actually did, for the receipt.

    Read back from the sealed document rather than recomputed, so the receipt reports
    the record instead of a second opinion about it.
    """
    block = sealed.get("composition")
    if block is None:
        return None
    body = json_object(block, "sealed composition")
    return {
        field: body.get(field)
        for field in ("sample_id", "switch", "defensive_decisions", "defensive_decision_count")
    }


def _carry(prepared: PreparedResearchRun) -> _Carried:
    """Take every byte and identity the later stages need from the preparation.

    A declaration names neither an engine nor an environment, because no certified
    request stands behind its calculation, so both are read from the document it sealed
    beside the envelope. That document is sealed under the same durable intent as the
    envelope, so it is no more replaceable than a registered request would be.
    """
    sealed = json_object(decode_json(prepared.provenance), "sealed preparation")
    envelope = json_object(decode_json(prepared.envelope.canonical_bytes), "envelope")
    declaration = prepared.declaration
    return _Carried(
        run_id=prepared.run_id,
        request_hash=declaration.request_sha256,
        schema_version=declaration.schema_version,
        scope=json_text(sealed.get("scope"), "sealed scope"),
        envelope_bytes=prepared.envelope.canonical_bytes,
        envelope_sha256=prepared.envelope.envelope_sha256,
        provenance_bytes=prepared.provenance,
        preparation_sha256=hashlib.sha256(prepared.provenance).hexdigest(),
        engine_hash=content_sha256(json_object(sealed.get("engine"), "sealed engine")),
        environment_hash=content_sha256(
            json_object(sealed.get("environment"), "sealed environment")
        ),
        # Storage places every pin on the envelope's own module and refuses a result
        # produced for another one, so the module is read from what was sealed.
        pins=_pins(declaration, json_text(envelope.get("module"), "envelope module")),
        composition=_composition(sealed),
    )


def _prepare(home: Path, declared: _Declared, budget: ComputeBudget) -> PreparedResearchRun:
    """Stage 0: SELECT-only preparation, after refusing an installation that cannot record it.

    Both checks run before anything is calculated. A missing add-on names its install
    command, and an add-on older than this declaration's contract names the migration,
    instead of a full replay ending at a constraint the installation could have reported
    in the first second.
    """
    with open_workspace(home) as workspace:
        require_run_schema(workspace)
        require_request_schema(workspace, declared.request.schema_version)
        return prepare_research_run(
            workspace, declared.request, budget=retaining(budget, declared.canonical)
        )


def _intent(carried: _Carried, request: RunResearchRequest, bundle_id: str) -> RunIntent:
    """Fill every RunIntent field from the declaration and the document it sealed."""
    return RunIntent(
        request_hash=carried.request_hash,
        bundle_id=bundle_id,
        engine_hash=carried.engine_hash,
        environment_hash=carried.environment_hash,
        reason=request.reason,
        envelope_bytes=carried.envelope_bytes,
        preparation_bytes=carried.provenance_bytes,
        strategy_pins=carried.pins,
        prior_run_id=request.prior_run_id,
        run_id=carried.run_id,
    )


def _open(
    home: Path,
    declared: _Declared,
    carried: _Carried,
    request: RunResearchRequest,
    budget: ComputeBudget,
) -> Opened:
    """Stage A: one short write that registers the declaration and commits the run intent.

    The registered request is the declaration itself, byte for byte. Nothing converts it
    into an executable request shape on the way in, and the store keeps the two roots
    apart rather than translating between them.
    """
    document = canonical_json_bytes(
        {
            "schema": BUNDLE_SCHEMA,
            "hash_format": HASH_FORMAT,
            "bundle_id": bundle_name(
                request.bundle_id, declared.bindings, carried.request_hash, prefix=_BUNDLE_PREFIX
            ),
            "bindings": declared.bindings,
        }
    )
    with open_workspace(home, writable=True) as workspace:
        # Registration is durable and a bundle name binds one request for good, so the
        # run-only fields are checked first. open_run cannot run inside another
        # transaction, so stage A cannot be one atomic write; refusing a predecessor
        # nobody recorded, or one that is this run itself, is what keeps a bad run field
        # from stranding a registration. The run store refuses both too, but only after
        # the declaration is already filed.
        if request.prior_run_id == carried.run_id:
            raise ValueError("a run cannot be its own predecessor")
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
            declared.canonical,
            expected_request_hash=carried.request_hash,
            budget=budget,
        )
        handle = open_run(workspace, _intent(carried, request, bundle.bundle_id), budget=budget)
        return Opened(handle, bundle, installation_identity(workspace))


def _receipt(
    request: RunResearchRequest,
    declared: _Declared,
    carried: _Carried,
    opened: Opened,
    outcome: _Outcome,
) -> dict[str, object]:
    """Returned only after the result is stored, and it claims nothing the run does not.

    The seven negative statements are the ones the sealed preparation and the sealed
    result already carry. They are repeated here because a receipt is what a caller
    reads first, and a receipt that reported only a run ID and a NAV would be the one
    place in this path that looks like a certified result.
    """
    return {
        "executed": True,
        "declaration_sha256": request.declaration_sha256,
        "request_hash": carried.request_hash,
        "request_schema": carried.schema_version,
        "scope": carried.scope,
        "bundle_id": opened.bundle.bundle_id,
        "bindings": _binding_report(declared),
        "envelope": {"sha256": carried.envelope_sha256},
        "preparation": {"sha256": carried.preparation_sha256},
        "exported": outcome.exported,
        # Read back from the sealed envelope: nothing of the preparation is retained.
        "target_weights": sealed_targets(carried.envelope_bytes),
        "composition": carried.composition,
        "backtest": outcome.response,
        "run": outcome.recorded,
        "execution_mode": EXECUTION_MODE,
        "research_only": True,
        "certified": False,
        "non_executable": True,
        "executable_prices": False,
        "point_in_time_certified": False,
        "observed_prices_verified": False,
        "source_parity": "unknown",
    }


def _staged(
    home: Path,
    declared: _Declared,
    request: RunResearchRequest,
    budget: ComputeBudget,
    output: Path | None,
) -> dict[str, object]:
    """The four stages. Only A and C hold a writable workspace, and neither calculates."""
    if output is None:
        prepared = _prepare(home, declared, budget)
        exported = None
    else:
        with DescriptorTree.open_path(output.parent) as tree:
            require_new_outputs(tree, output.name)
            prepared = _prepare(home, declared, budget)
            exported = seal_outputs(
                tree, output.name, prepared.envelope.canonical_bytes, prepared.provenance
            )
    # Take what the later stages read, then release the preparation before anything else
    # materializes. No stage after this runs beside the decisions or the panel.
    carried = _carry(prepared)
    del prepared
    retained = retaining(
        budget, declared.canonical, carried.envelope_bytes, carried.provenance_bytes
    )
    opened = _open(home, declared, carried, request, retained)
    try:
        # The run is durable now, so every failure from here ends it rather than
        # stranding it.
        #
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
            home, opened, result, carried.request_hash, retaining(retained, result.backtest_bytes)
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
    return _receipt(request, declared, carried, opened, _Outcome(sealed, recorded, exported))


def run_research(request: RunResearchRequest) -> dict[str, object]:
    """Prepare, register, calculate and record one declared uncertified research run.

    Returns only after the result is durably committed, so a receipt always implies a
    stored SUCCESS. A failure returns no receipt and can still leave evidence on purpose:
    an --envelope-output export is written before the run is opened, and a run
    interrupted while its inputs are being sealed stays RUNNING for aas db recover.

    Running the same declaration twice does not record it twice. The second attempt
    prepares to the same run identity and the run store refuses to reopen a finished run
    under it, which is what stops one calculation from appearing as two results.
    """
    declared = _declared(_read_declaration(request.declaration, request.declaration_sha256))
    output = None if request.envelope_output is None else admitted_path(request.envelope_output)
    home, targets, database_error = resolve_installation(request.home)
    if output is not None:
        require_exportable(home, output)
    try:
        with price_compute(excluded_locks=targets) as budget:
            if budget is None:
                raise ValueError("a run requires the explicit AAS compute budget environment")
            return _staged(home, declared, request, budget, output)
    except (sqlite3.Error, database_error) as error:
        raise translated(error) from None


def rerun_research_run(
    run_id: str,
    *,
    home: Path | None = None,
    declaration: Path | None = None,
    declaration_sha256: str | None = None,
) -> dict[str, object]:
    """Reproduce one recorded run from its own sealed evidence. Writes nothing.

    Determinism here is two separate claims, and each is reported on its own. The result
    claim is that the sealed envelope, run through the same accounting, still produces
    exactly the bytes the run committed. The preparation claim is that the declaration
    still prepares to the same run identity, envelope and sealed document, and only a
    caller holding the declaration can ask for it. Neither is inferred from the other: a
    result that reproduces from a replaced envelope would still be the wrong run.

    The result claim needs nothing of the declared contract, so it is made for any
    recorded run. The preparation claim is declared-path only, because re-preparing is
    what a declaration supports and a certified request has aas prepare for it.
    """
    if (declaration is None) != (declaration_sha256 is None):
        raise ValueError("a declaration and its exact SHA-256 are supplied together or not at all")
    declared = (
        None
        if declaration is None or declaration_sha256 is None
        else _declared(_read_declaration(declaration, declaration_sha256))
    )
    resolved, targets, database_error = resolve_installation(home)
    try:
        with price_compute(excluded_locks=targets) as budget:
            if budget is None:
                raise ValueError("a rerun requires the explicit AAS compute budget environment")
            return _reproduced(resolved, json_text(run_id, "run_id"), declared, budget)
    except (sqlite3.Error, database_error) as error:
        raise translated(error) from None


def _reproduced(
    home: Path, run_id: str, declared: _Declared | None, budget: ComputeBudget
) -> dict[str, object]:
    """Read the run and its evidence, recompute, and compare only what was asked for."""
    with open_workspace(home) as workspace:
        payload = read_run(workspace, run_id, budget=budget)
        evidence = read_run_evidence(workspace, run_id, budget=budget)
    if declared is not None and payload.get("request_schema") not in _DECLARED_SCHEMAS:
        # Re-preparing is something a declaration supports and a certified request does
        # not, so asking for it against another contract's run is refused rather than
        # answered with a comparison that would report false for the wrong reason.
        raise ValueError("only a declared research run can be re-prepared from its declaration")
    retained = retaining(budget, evidence.envelope, evidence.backtest, evidence.preparation)
    admit(retained, len(evidence.envelope))
    recomputed = canonical_json_bytes(
        run_document(evidence.envelope, hashlib.sha256(evidence.envelope).hexdigest())
    )
    checks: dict[str, object] = {
        "result": {
            "reproduced": recomputed == evidence.backtest,
            "recorded_sha256": hashlib.sha256(evidence.backtest).hexdigest(),
            "recomputed_sha256": hashlib.sha256(recomputed).hexdigest(),
        }
    }
    if declared is not None:
        checks["preparation"] = _repreparation(home, run_id, declared, evidence, retained)
    return {
        "run_id": run_id,
        "checked": sorted(checks),
        "reproduced": all(
            bool(check["reproduced"]) for check in checks.values() if isinstance(check, dict)
        ),
        "checks": checks,
        "run": payload,
        "research_only": True,
        "certified": False,
    }


def _repreparation(
    home: Path,
    run_id: str,
    declared: _Declared,
    evidence: RunEvidence,
    budget: ComputeBudget,
) -> dict[str, object]:
    """Prepare the declaration again and hold it against what the run actually sealed.

    Every field is compared, not only the identity. Two preparations agreeing on a run ID
    while differing in a byte of the envelope would be a determinism failure that a run
    ID comparison alone would report as success.
    """
    with open_workspace(home) as workspace:
        prepared = prepare_research_run(workspace, declared.request, budget=budget)
    matches = {
        "run_id": prepared.run_id == run_id,
        "envelope": prepared.envelope.canonical_bytes == evidence.envelope,
        "preparation": prepared.provenance == evidence.preparation,
    }
    return {
        "reproduced": all(matches.values()),
        "matches": matches,
        "declaration_sha256": hashlib.sha256(declared.canonical).hexdigest(),
        "request_hash": declared.request.request_sha256,
    }
