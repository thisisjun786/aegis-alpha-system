"""Versioned core schema migration of the state and market stores.

``aas db migrate --to N --backup-output DIR`` raises an installation one version at a
time up to ``N``. Each step from ``v - 1`` to ``v`` runs in a fixed order: installation
lock, a verified backup, a durable state intent, the market DDL in one DuckDB
transaction, the state DDL in one SQLite transaction, the installation receipt, then
completion. Every store keeps its old receipt rows and adds one per applied version, so
a store migrated from v1 records ``(1, v1), (2, v2)``.

Each step has its own intent, ``core-schema-migrate-v<v>``, whose identity is bound to
that step's versions and checksums rather than to ``CORE_VERSION``; ``v2`` is the
identity every installation migrated before later versions existed carries. An
installation stopped anywhere after a step's intent is migration-incomplete. Ordinary
admission refuses it, and repeating the command finishes that step without a second
backup. A step that has no intent yet always takes its own backup first, so an
installation stopped between one step's completion and the next step's intent needs a
new backup directory. The run add-on has its own versions and is independent of this.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from aegis_alpha.data.serialization import content_sha256
from aegis_alpha.storage.market import (
    MARKET_CHECKSUMS,
    budgeted,
    upgrade_market,
    validate_market,
)
from aegis_alpha.storage.paths import read_json
from aegis_alpha.storage.sqlite import schema_checksums
from aegis_alpha.storage.state import (
    complete_operation,
    get_operation,
    prepare_operation,
    state_version,
    upgrade_state,
)
from aegis_alpha.storage.state_schema import MIGRATIONS as STATE_MIGRATIONS

if TYPE_CHECKING:
    import sqlite3

    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace

# Both stores move together, so they always know the same number of versions.
CORE_VERSION = len(MARKET_CHECKSUMS)
STATE_CHECKSUMS = schema_checksums(STATE_MIGRATIONS)
if len(STATE_CHECKSUMS) != CORE_VERSION:
    raise RuntimeError("state and market core schemas must know the same versions")
MIGRATION_KIND = "core-schema-migrate"
_OPERATION_PREFIX = MIGRATION_KIND + "-v"
# The first step's operation; every later step has its own (``step_operation``).
MIGRATION_OPERATION = _OPERATION_PREFIX + "2"
_FIRST_STEP = 2
_HASH_FORMAT = "aas-canonical-json-sha256-v1"
_REQUEST_SCHEMA = "aas-core-schema-migrate-v1"
_STEPS = ("backup", "intent", "market", "state", "receipt", "complete")


class CoreSchemaError(ValueError):
    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        super().__init__(reason + (": " + detail if detail else ""))


@dataclass(frozen=True, slots=True)
class CoreSchemaStatus:
    """Where an installation stands, read from its stores, receipt and step intents.

    ``current`` is at the version this code installs, ``outdated`` is an older complete
    installation, and ``incomplete`` has a prepared step intent to finish.
    ``migration_operation`` names the step intent ``migration_phase`` is read from: the
    prepared one, or else the one that brought the stores to their version, if any.
    """

    state: Literal["current", "outdated", "incomplete"]
    state_version: int
    market_version: int
    receipt_state_version: int
    receipt_market_version: int
    migration_phase: str | None
    target_version: int
    migration_operation: str | None = None


def step_operation(to_version: int) -> str:
    """The operation ID of the step that raises an installation to ``to_version``."""
    return _OPERATION_PREFIX + str(to_version)


def _step(operation_id: str) -> int | None:
    """The step an operation ID names, or None when it is not one this code knows."""
    number = operation_id.removeprefix(_OPERATION_PREFIX)
    if number == operation_id or not number.isdecimal():
        return None
    step = int(number)
    return (
        step
        if step_operation(step) == operation_id and _FIRST_STEP <= step <= CORE_VERSION
        else None
    )


def _target(to_version: int) -> None:
    if type(to_version) is not int or not _FIRST_STEP <= to_version <= CORE_VERSION:
        known = (
            str(CORE_VERSION)
            if CORE_VERSION == _FIRST_STEP
            else f"{_FIRST_STEP} through {CORE_VERSION}"
        )
        raise CoreSchemaError(
            "core_schema_unknown_version",
            f"this code migrates to version {known} only, not {to_version}",
        )


def require_core_migration_finished(state: sqlite3.Connection) -> None:
    """Refuse ordinary admission while a core migration intent is not completed."""
    row = state.execute(
        "SELECT operation_id FROM storage_operations "
        "WHERE (kind=? OR operation_id=?) AND phase!='COMPLETED' ORDER BY operation_id",
        (MIGRATION_KIND, MIGRATION_OPERATION),
    ).fetchone()
    if row:
        step = _step(str(row[0])) or CORE_VERSION
        raise CoreSchemaError(
            "core_schema_migration_incomplete",
            f"the core schema migration is incomplete; repeat aas db migrate --to {step}",
        )


def _step_request(
    step: int, *, installation_id: str, state_store_id: str, market_store_id: str
) -> dict[str, object]:
    """The deterministic identity of one installation's step to version ``step``.

    It names the checksums of every version up to the step and nothing after it, so a
    step's request is the same whichever later versions this code knows.
    """
    return {
        "schema": _REQUEST_SCHEMA,
        "hash_format": _HASH_FORMAT,
        "from_version": step - 1,
        "to_version": step,
        "market_checksums": list(MARKET_CHECKSUMS[:step]),
        "state_checksums": list(STATE_CHECKSUMS[:step]),
        "installation_id": installation_id,
        "state_store_id": state_store_id,
        "market_store_id": market_store_id,
    }


def _request_hash(workspace: Workspace, step: int) -> str:
    return content_sha256(
        _step_request(
            step,
            installation_id=workspace.installation_id,
            state_store_id=workspace.state.execute("SELECT store_id FROM store_info").fetchone()[0],
            market_store_id=workspace.market.execute("SELECT store_id FROM store_info").fetchall()[
                0
            ][0],
        )
    )


def _expected_parent(step: int) -> str:
    """The exact stores a step was written against, by their checksums before it.

    A resumed attempt cannot apply to a schema it never inspected, because the intent
    names these checksums.
    """
    return content_sha256(
        {"market": MARKET_CHECKSUMS[step - 2], "state": STATE_CHECKSUMS[step - 2]}
    )


def _step_phases(workspace: Workspace) -> dict[int, str]:
    """Each recorded step intent's phase, refusing any intent that is not its own step."""
    phases: dict[int, str] = {}
    for row in workspace.state.execute(
        "SELECT operation_id FROM storage_operations WHERE kind=? OR operation_id LIKE ?",
        (MIGRATION_KIND, _OPERATION_PREFIX + "%"),
    ).fetchall():
        step = _step(str(row[0]))
        if step is None:
            raise CoreSchemaError(
                "core_schema_invalid", "a core migration intent names a step this code lacks"
            )
        operation = cast("dict[str, object]", get_operation(workspace.state, str(row[0])))
        expected = {
            "kind": MIGRATION_KIND,
            "request_hash": _request_hash(workspace, step),
            "target_id": workspace.installation_id,
            "expected_parent": _expected_parent(step),
        }
        if any(operation[key] != value for key, value in expected.items()):
            raise CoreSchemaError("core_schema_invalid", "migration intent identity mismatch")
        phases[step] = str(operation["phase"])
    return phases


def _receipt_versions(workspace: Workspace) -> tuple[int, int]:
    stores = cast("dict[str, dict[str, object]]", read_json(_receipt_path(workspace))["stores"])
    return (
        cast("int", stores["state"]["schema_version"]),
        cast("int", stores["market"]["schema_version"]),
    )


def _receipt_path(workspace: Workspace) -> Path:
    return workspace.paths.root / "installation.json"


def inspect_core_schema(workspace: Workspace) -> CoreSchemaStatus:
    """Read the installation's core schema position without changing anything."""
    market = validate_market(workspace.market, workspace.installation_id)
    state = state_version(workspace.state, workspace.installation_id)
    named_state, named_market = _receipt_versions(workspace)
    versions = {state, market, named_state, named_market}
    phases = _step_phases(workspace)
    ended = sorted(phase for phase in phases.values() if phase not in ("PREPARED", "COMPLETED"))
    if ended:
        raise CoreSchemaError(
            "core_schema_invalid", "the core migration intent is " + ended[0].lower()
        )
    prepared = [step for step, phase in phases.items() if phase == "PREPARED"]
    if len(prepared) > 1:
        raise CoreSchemaError(
            "core_schema_invalid", "more than one core migration step is prepared"
        )
    if prepared:
        (step,) = prepared
        # The stores and the receipt are each at the step's start or end, nowhere else,
        # and no later step can have completed before this one.
        if not versions <= {step - 1, step} or any(later > step for later in phases):
            raise CoreSchemaError("core_schema_invalid", "stores and receipt disagree")
        return CoreSchemaStatus(
            "incomplete",
            state,
            market,
            named_state,
            named_market,
            "PREPARED",
            CORE_VERSION,
            step_operation(step),
        )
    if len(versions) != 1:
        raise CoreSchemaError("core_schema_invalid", "stores and receipt disagree")
    if any(step > market for step in phases):
        raise CoreSchemaError("core_schema_invalid", "a completed migration left an old store")
    phase = phases.get(market)
    return CoreSchemaStatus(
        "current" if market == CORE_VERSION else "outdated",
        state,
        market,
        named_state,
        named_market,
        phase,
        CORE_VERSION,
        None if phase is None else step_operation(market),
    )


def _remaining(status: CoreSchemaStatus, step: int) -> list[str]:
    """The parts of ``step`` still to run: all of it unless its intent is prepared."""
    if status.state != "incomplete" or status.migration_operation != step_operation(step):
        return list(_STEPS)
    done = {
        "market": status.market_version == step,
        "state": status.state_version == step,
        "receipt": (status.receipt_state_version, status.receipt_market_version) == (step, step),
    }
    return [part for part in _STEPS[2:] if not done.get(part, False)]


def _pending_steps(status: CoreSchemaStatus, to_version: int) -> list[tuple[int, bool]]:
    """``(step, prepared)`` for each step still to run up to ``to_version``, oldest first.

    A prepared step is finished first, so a target before it is refused rather than
    leaving the installation half-migrated. A store already at or past the target is
    left alone; nothing is downgraded.
    """
    if status.state != "incomplete":
        return [(step, False) for step in range(status.market_version + 1, to_version + 1)]
    prepared = cast("int", _step(cast("str", status.migration_operation)))
    if prepared > to_version:
        raise CoreSchemaError(
            "core_schema_migration_incomplete",
            f"the step to version {prepared} is prepared; repeat aas db migrate --to {prepared}",
        )
    return [(prepared, True), *((step, False) for step in range(prepared + 1, to_version + 1))]


def _receipts(workspace: Workspace) -> dict[str, list[list[object]]]:
    """Every recorded schema_migrations row of both stores, oldest first."""
    query = "SELECT version,checksum FROM schema_migrations ORDER BY version"
    return {
        "state": [[int(row[0]), str(row[1])] for row in workspace.state.execute(query)],
        "market": [
            [int(row[0]), str(row[1])] for row in workspace.market.execute(query).fetchall()
        ],
    }


def _pending_operations(
    workspace: Workspace, *, excluding: str | None
) -> tuple[list[str], list[str]]:
    """``(carried, blocking)``: the other prepared operations, and the ones that stop a step.

    A migration carries only an untouched promotion intent, which keeps its intent and
    retained evidence and is published by ``aas db recover`` afterwards. Every other
    prepared operation, including a promotion whose generation is committed, blocks.
    """
    from aegis_alpha.storage.promotion.engine import (  # noqa: PLC0415 -- promotion owner
        untouched_promotion_refusal,
    )

    carried: list[str] = []
    blocking: list[str] = []
    for row in workspace.state.execute(
        "SELECT operation_id,kind,request_hash,target_id,expected_parent,payload_hash,phase "
        "FROM storage_operations WHERE phase='PREPARED' AND operation_id IS NOT ? "
        "ORDER BY operation_id",
        (excluding,),
    ).fetchall():
        operation = dict(row)
        refusal = untouched_promotion_refusal(workspace, operation)
        (blocking if refusal else carried).append(str(operation["operation_id"]))
    return carried, blocking


def _running(workspace: Workspace) -> bool:
    return (
        workspace.state.execute("SELECT 1 FROM runs WHERE status='RUNNING'").fetchone() is not None
    )


def _prepared_step(status: CoreSchemaStatus) -> str | None:
    return status.migration_operation if status.state == "incomplete" else None


def plan_core_migration(home: Path, *, to_version: int) -> dict[str, object]:
    """Report the installation's versions, recognised checksums and remaining steps.

    The stores are opened read-only and nothing is written, so this is safe beside a
    live installation's readers. An unrecognised checksum or version is refused here
    exactly as the migration would refuse it. ``steps`` is the next step's remaining
    parts and ``migrations`` lists every step up to ``to_version``; ``carried_operations``
    are the prepared promotions a step's backup would hold pending, and
    ``blocking_operations`` the prepared operations that refuse it.
    """
    from aegis_alpha.storage.workspace import open_workspace  # noqa: PLC0415

    _target(to_version)
    with open_workspace(home, migrating=True, require_strategies=False) as workspace:
        status = inspect_core_schema(workspace)
        pending = _pending_steps(status, to_version)
        carried, blocking = _pending_operations(workspace, excluding=_prepared_step(status))
        migrations = [
            {
                "operation_id": step_operation(step),
                "from_version": step - 1,
                "to_version": step,
                "phase": "PREPARED" if prepared else None,
                "steps": _remaining(status, step),
                "backup_required": not prepared,
            }
            for step, prepared in pending
        ]
        return {
            "plan": True,
            "writes": 0,
            **asdict(status),
            "recognized": _receipts(workspace),
            "steps": migrations[0]["steps"] if migrations else [],
            "migrations": migrations,
            "backup_required": any(not prepared for _, prepared in pending),
            "carried_operations": carried,
            "blocking_operations": blocking,
            "running_analyses": _running(workspace),
        }


def _quiet(workspace: Workspace, *, excluding: str | None) -> list[str]:
    """Refuse to migrate while anything else could be writing either store.

    Returns the untouched promotion intents the step carries through its backup.
    """
    carried, blocking = _pending_operations(workspace, excluding=excluding)
    if _running(workspace) or blocking:
        raise CoreSchemaError(
            "core_schema_busy",
            "stop running analyses and recover prepared operations first"
            + (" (" + ", ".join(blocking) + ")" if blocking else ""),
        )
    return carried


def _upgrade_market(workspace: Workspace, budget: ComputeBudget | None, step: int) -> None:
    # Admission, and the backup's reopen after its checkpoint, use the installation's
    # own limits; the lease's lower share is applied here, right before the transaction.
    with budgeted(workspace.market, budget, "the core schema migration"):
        upgrade_market(workspace.market, workspace.installation_id, step)


def _readmit_market(workspace: Workspace, step: int) -> None:
    """Let the admission name the market's version once its step has landed.

    Admission recorded the receipt's market, which lags the store when a run died after
    the market's COMMIT. Only the version may move, and only to this step's: the store
    and installation identity stay the admitted ones, so a later step's backup still
    refuses any other market.
    """
    from aegis_alpha.storage.workspace import store_info  # noqa: PLC0415

    observed = store_info(workspace.market)
    admitted = workspace._market_info  # noqa: SLF001 -- same admission
    if observed.keys() != admitted.keys() or any(
        observed[key] != admitted[key] for key in observed if key != "schema_version"
    ):
        raise ValueError("market identity changed during admitted maintenance")
    if observed["schema_version"] != step:
        raise CoreSchemaError("core_schema_migration_incomplete", "a step did not land")
    workspace._market_info = observed  # noqa: SLF001 -- same admission


def _upgrade_state(workspace: Workspace, step: int) -> None:
    upgrade_state(workspace.state, workspace.installation_id, step)


def _write_receipt(workspace: Workspace) -> None:
    """Name the stores' new version in the installation receipt, changing nothing else."""
    from aegis_alpha.storage.workspace import store_info, write_json  # noqa: PLC0415

    receipt = read_json(_receipt_path(workspace))
    stores = cast("dict[str, object]", receipt["stores"])
    stores["state"] = store_info(workspace.state)
    stores["market"] = store_info(workspace.market)
    write_json(_receipt_path(workspace), receipt)


def _finish(workspace: Workspace, step: int, budget: ComputeBudget | None) -> None:
    """Apply whatever the step's prepared intent still names, then complete it."""
    status = inspect_core_schema(workspace)
    if status.migration_operation != step_operation(step) or status.state != "incomplete":
        raise CoreSchemaError("core_schema_migration_incomplete", "the step has no prepared intent")
    if status.market_version < step:
        _upgrade_market(workspace, budget, step)
    # Also when an earlier run's COMMIT already moved the market: the next step's backup
    # compares the market with what admission recorded.
    _readmit_market(workspace, step)
    if status.state_version < step:
        _upgrade_state(workspace, step)
    if (status.receipt_state_version, status.receipt_market_version) != (step, step):
        _write_receipt(workspace)
    checked = inspect_core_schema(workspace)
    if _remaining(checked, step) != ["complete"]:
        raise CoreSchemaError("core_schema_migration_incomplete", "a step did not land")
    complete_operation(workspace.state, step_operation(step), _request_hash(workspace, step))


def _backup_targets(workspace: Workspace, output: Path | None, steps: list[int]) -> dict[int, Path]:
    """Where each step that needs one writes its backup.

    One backup goes to ``output`` itself. Several go to ``output/<operation ID>``, so
    each step keeps its own verified rollback snapshot. Either way ``output`` is admitted
    here as the backup would admit it, so a resumed step is not finished before a later
    one refuses its destination.
    """
    from aegis_alpha.storage.backup import backup_destination  # noqa: PLC0415

    if not steps:
        return {}
    if output is None:
        raise CoreSchemaError(
            "core_schema_backup_required", "pass --backup-output with a new directory"
        )
    root = backup_destination(workspace, output)
    if len(steps) == 1:
        return {steps[0]: output}
    return {step: root / step_operation(step) for step in steps}


def migrate_core_schema(
    home: Path,
    *,
    to_version: int,
    backup_output: Path | None,
    budget: ComputeBudget | None = None,
    deep: bool = False,
) -> dict[str, object]:
    """Migrate the installation's state and market stores to ``to_version``.

    An installation at or past the target is a no-op. Every step without an intent needs
    ``backup_output``: its backup is taken and verified before its intent, and the intent
    records the backup manifest's SHA-256. A prepared step is finished from its intent
    without a second backup. Nothing is written before the target, the backup
    destinations and the quiet installation are all checked.
    """
    from aegis_alpha.storage.backup import backup_workspace, manifest_sha256  # noqa: PLC0415
    from aegis_alpha.storage.workspace import open_workspace  # noqa: PLC0415

    _target(to_version)
    with open_workspace(home, writable=True, migrating=True) as workspace:
        status = inspect_core_schema(workspace)
        pending = _pending_steps(status, to_version)
        if not pending:
            return _report(workspace, performed=[])
        targets = _backup_targets(
            workspace, backup_output, [step for step, prepared in pending if not prepared]
        )
        _quiet(workspace, excluding=_prepared_step(status))
        performed: list[dict[str, object]] = []
        for step, prepared in pending:
            backup_root = None
            if not prepared:
                carried = _quiet(workspace, excluding=None)
                backup = backup_workspace(
                    workspace, targets[step], budget=budget, deep=deep, carry=carried
                )
                backup_root = str(backup["backup_root"])
                prepare_operation(
                    workspace.state,
                    operation_id=step_operation(step),
                    kind=MIGRATION_KIND,
                    request_hash=_request_hash(workspace, step),
                    target_id=workspace.installation_id,
                    expected_parent=_expected_parent(step),
                    payload_hash=manifest_sha256(Path(backup_root)),
                )
            _finish(workspace, step, budget)
            performed.append({"step": step, "resumed": prepared, "backup_root": backup_root})
        return _report(workspace, performed=performed)


def _report(workspace: Workspace, *, performed: list[dict[str, object]]) -> dict[str, object]:
    """The installation after the command, and each step this invocation finished.

    The top-level ``operation_id`` and backup fields name the last step finished, or
    the intent the stores are at when nothing was to do.
    """
    status = inspect_core_schema(workspace)
    migrations = []
    for entry in performed:
        operation = cast(
            "dict[str, object]",
            get_operation(workspace.state, step_operation(cast("int", entry["step"]))),
        )
        migrations.append(
            {
                "operation_id": operation["operation_id"],
                "from_version": cast("int", entry["step"]) - 1,
                "to_version": entry["step"],
                "resumed": entry["resumed"],
                "backup_root": entry["backup_root"],
                "backup_manifest_sha256": operation["payload_hash"],
            }
        )
    last = migrations[-1] if migrations else None
    named = status.migration_operation if last is None else str(last["operation_id"])
    operation = None if named is None else get_operation(workspace.state, named)
    return {
        "migrated": bool(performed),
        **asdict(status),
        "receipts": _receipts(workspace),
        "operation_id": None if operation is None else operation["operation_id"],
        "backup_root": None if last is None else last["backup_root"],
        "backup_manifest_sha256": None if operation is None else operation["payload_hash"],
        "migrations": migrations,
        # A carried promotion is still pending here; aas db recover publishes it.
        "pending_operations": [
            str(row[0])
            for row in workspace.state.execute(
                "SELECT operation_id FROM storage_operations WHERE phase='PREPARED' "
                "ORDER BY operation_id"
            )
        ],
    }
