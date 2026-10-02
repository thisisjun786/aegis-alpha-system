"""Versioned core schema migration of the state and market stores.

``aas db migrate --to 2 --backup-output DIR`` raises a v1 installation to v2. The order
is fixed: installation lock, a verified backup, a durable state intent, the market DDL
in one DuckDB transaction, the state DDL in one SQLite transaction, the installation
receipt, then completion. Every store keeps its old receipt rows and adds one per
applied version, so a migrated store records ``(1, v1), (2, v2)``.

An installation stopped anywhere after the intent is migration-incomplete. Ordinary
admission refuses it, and repeating the command finishes the remaining steps without a
second backup. The intent is ``core-schema-migrate-v2``, and its identity is bound to
the v1 to v2 step rather than to ``CORE_VERSION``. The run add-on has its own versions
and is independent of this one.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from aegis_alpha.data.serialization import content_sha256
from aegis_alpha.storage.market import MARKET_CHECKSUMS, upgrade_market, validate_market
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
MIGRATION_OPERATION = "core-schema-migrate-v2"
# The step this operation names. Its intent identity is bound to this step, not to
# CORE_VERSION, so a completed intent keeps matching after later versions exist.
_STEP_FROM = 1
_STEP_TO = 2
_HASH_FORMAT = "aas-canonical-json-sha256-v1"
_REQUEST_SCHEMA = "aas-core-schema-migrate-v1"
# The exact v1 stores this migration was written against. A resumed attempt cannot apply
# to a schema it never inspected, because the intent names these checksums.
_EXPECTED_PARENT = content_sha256(
    {"market": MARKET_CHECKSUMS[_STEP_FROM - 1], "state": STATE_CHECKSUMS[_STEP_FROM - 1]}
)
_STEPS = ("backup", "intent", "market", "state", "receipt", "complete")


class CoreSchemaError(ValueError):
    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        super().__init__(reason + (": " + detail if detail else ""))


@dataclass(frozen=True, slots=True)
class CoreSchemaStatus:
    """Where an installation stands, read from its stores, receipt and intent.

    ``current`` is at the version this code installs, ``outdated`` is an older complete
    installation, and ``incomplete`` has a prepared migration intent to finish.
    """

    state: Literal["current", "outdated", "incomplete"]
    state_version: int
    market_version: int
    receipt_state_version: int
    receipt_market_version: int
    migration_phase: str | None
    target_version: int = CORE_VERSION


def _target(to_version: int) -> None:
    if type(to_version) is not int or to_version != CORE_VERSION:
        raise CoreSchemaError(
            "core_schema_unknown_version",
            f"this code migrates to version {CORE_VERSION} only, not {to_version}",
        )


def require_core_migration_finished(state: sqlite3.Connection) -> None:
    """Refuse ordinary admission while a core migration intent is not completed."""
    if state.execute(
        "SELECT 1 FROM storage_operations WHERE (kind=? OR operation_id=?) AND phase!='COMPLETED'",
        (MIGRATION_KIND, MIGRATION_OPERATION),
    ).fetchone():
        raise CoreSchemaError(
            "core_schema_migration_incomplete",
            f"the core schema migration is incomplete; repeat aas db migrate --to {CORE_VERSION}",
        )


def _request(workspace: Workspace) -> dict[str, object]:
    """The deterministic identity of this installation's v1 to v2 migration step."""
    return {
        "schema": _REQUEST_SCHEMA,
        "hash_format": _HASH_FORMAT,
        "from_version": _STEP_FROM,
        "to_version": _STEP_TO,
        "market_checksums": list(MARKET_CHECKSUMS[:_STEP_TO]),
        "state_checksums": list(STATE_CHECKSUMS[:_STEP_TO]),
        "installation_id": workspace.installation_id,
        "state_store_id": workspace.state.execute("SELECT store_id FROM store_info").fetchone()[0],
        "market_store_id": workspace.market.execute("SELECT store_id FROM store_info").fetchall()[
            0
        ][0],
    }


def _phase(workspace: Workspace) -> str | None:
    """The v2 step intent's phase, refusing an intent that is not this step."""
    operation = get_operation(workspace.state, MIGRATION_OPERATION)
    if operation is None:
        return None
    expected = {
        "kind": MIGRATION_KIND,
        "request_hash": content_sha256(_request(workspace)),
        "target_id": workspace.installation_id,
        "expected_parent": _EXPECTED_PARENT,
    }
    if any(operation[key] != value for key, value in expected.items()):
        raise CoreSchemaError("core_schema_invalid", "migration intent identity mismatch")
    return str(operation["phase"])


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
    phase = _phase(workspace)
    status = CoreSchemaStatus(
        "incomplete", state, market, named_state, named_market, phase, CORE_VERSION
    )
    if phase == "PREPARED":
        return status
    if phase not in (None, "COMPLETED"):
        raise CoreSchemaError(
            "core_schema_invalid", "the core migration intent is " + str(phase).lower()
        )
    if len({state, market, named_state, named_market}) != 1:
        raise CoreSchemaError("core_schema_invalid", "stores and receipt disagree")
    if phase == "COMPLETED" and market != CORE_VERSION:
        raise CoreSchemaError("core_schema_invalid", "a completed migration left an old store")
    return CoreSchemaStatus(
        "current" if market == CORE_VERSION else "outdated",
        state,
        market,
        named_state,
        named_market,
        phase,
        CORE_VERSION,
    )


def _remaining(status: CoreSchemaStatus) -> list[str]:
    if status.state == "current":
        return []
    if status.state == "outdated":
        return list(_STEPS)
    done = {
        "market": status.market_version == CORE_VERSION,
        "state": status.state_version == CORE_VERSION,
        "receipt": (status.receipt_state_version, status.receipt_market_version)
        == (CORE_VERSION, CORE_VERSION),
    }
    return [step for step in _STEPS[2:] if not done.get(step, False)]


def _receipts(workspace: Workspace) -> dict[str, list[list[object]]]:
    """Every recorded schema_migrations row of both stores, oldest first."""
    query = "SELECT version,checksum FROM schema_migrations ORDER BY version"
    return {
        "state": [[int(row[0]), str(row[1])] for row in workspace.state.execute(query)],
        "market": [
            [int(row[0]), str(row[1])] for row in workspace.market.execute(query).fetchall()
        ],
    }


def plan_core_migration(home: Path, *, to_version: int) -> dict[str, object]:
    """Report the installation's versions, recognised checksums and remaining steps.

    The stores are opened read-only and nothing is written, so this is safe beside a
    live installation's readers. An unrecognised checksum or version is refused here
    exactly as the migration would refuse it.
    """
    from aegis_alpha.storage.workspace import open_workspace  # noqa: PLC0415

    _target(to_version)
    with open_workspace(home, migrating=True, require_strategies=False) as workspace:
        status = inspect_core_schema(workspace)
        return {
            "plan": True,
            "writes": 0,
            **asdict(status),
            "recognized": _receipts(workspace),
            "steps": _remaining(status),
            "backup_required": status.state == "outdated",
        }


def _quiet(workspace: Workspace, *, excluding: str | None) -> None:
    """Refuse to migrate while anything else could be writing either store."""
    if workspace.state.execute("SELECT 1 FROM runs WHERE status='RUNNING'").fetchone() or (
        workspace.state.execute(
            "SELECT 1 FROM storage_operations WHERE phase='PREPARED' AND operation_id IS NOT ?",
            (excluding,),
        ).fetchone()
    ):
        raise CoreSchemaError(
            "core_schema_busy", "stop running analyses and recover prepared operations first"
        )


def _upgrade_market(workspace: Workspace) -> None:
    from aegis_alpha.storage.workspace import store_info  # noqa: PLC0415

    upgrade_market(workspace.market, workspace.installation_id, CORE_VERSION)
    # The admitted identity is unchanged; only the version it records moved on.
    workspace._market_info = store_info(workspace.market)  # noqa: SLF001 -- same admission


def _upgrade_state(workspace: Workspace) -> None:
    upgrade_state(workspace.state, workspace.installation_id, CORE_VERSION)


def _write_receipt(workspace: Workspace) -> None:
    """Name the stores' new version in the installation receipt, changing nothing else."""
    from aegis_alpha.storage.workspace import store_info, write_json  # noqa: PLC0415

    receipt = read_json(_receipt_path(workspace))
    stores = cast("dict[str, object]", receipt["stores"])
    stores["state"] = store_info(workspace.state)
    stores["market"] = store_info(workspace.market)
    write_json(_receipt_path(workspace), receipt)


def _finish(workspace: Workspace, request_hash: str) -> None:
    """Apply whatever the prepared intent still names, then complete it."""
    status = inspect_core_schema(workspace)
    if status.market_version < CORE_VERSION:
        _upgrade_market(workspace)
    if status.state_version < CORE_VERSION:
        _upgrade_state(workspace)
    if (status.receipt_state_version, status.receipt_market_version) != (
        CORE_VERSION,
        CORE_VERSION,
    ):
        _write_receipt(workspace)
    checked = inspect_core_schema(workspace)
    if _remaining(checked) != ["complete"]:
        raise CoreSchemaError("core_schema_migration_incomplete", "a step did not land")
    complete_operation(workspace.state, MIGRATION_OPERATION, request_hash)


def _manifest_sha256(root: Path) -> str:
    from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415

    with DescriptorTree.open_path(root) as tree:
        return hashlib.sha256(
            tree.read_bytes("backup.json", max_bytes=64 * 1024 * 1024)
        ).hexdigest()


def migrate_core_schema(
    home: Path,
    *,
    to_version: int,
    backup_output: Path | None,
    budget: ComputeBudget | None = None,
) -> dict[str, object]:
    """Migrate the installation's state and market stores to ``to_version``.

    A current installation is a no-op. An outdated one needs ``backup_output``: the
    backup is taken and verified before the intent, and the intent records the backup
    manifest's SHA-256. A migration-incomplete installation is finished from its intent
    without a second backup, so ``backup_output`` is then not used.
    """
    from aegis_alpha.storage.backup import backup_workspace  # noqa: PLC0415
    from aegis_alpha.storage.workspace import open_workspace  # noqa: PLC0415

    _target(to_version)
    with open_workspace(home, writable=True, migrating=True) as workspace:
        status = inspect_core_schema(workspace)
        if status.state == "current":
            return _report(workspace, migrated=False, backup_root=None)
        request_hash = content_sha256(_request(workspace))
        backup_root = None
        if status.state == "outdated":
            if backup_output is None:
                raise CoreSchemaError(
                    "core_schema_backup_required", "pass --backup-output with a new directory"
                )
            _quiet(workspace, excluding=None)
            backup = backup_workspace(workspace, backup_output, budget=budget)
            backup_root = str(backup["backup_root"])
            prepare_operation(
                workspace.state,
                operation_id=MIGRATION_OPERATION,
                kind=MIGRATION_KIND,
                request_hash=request_hash,
                target_id=workspace.installation_id,
                expected_parent=_EXPECTED_PARENT,
                payload_hash=_manifest_sha256(Path(backup_root)),
            )
        else:
            _quiet(workspace, excluding=MIGRATION_OPERATION)
        _finish(workspace, request_hash)
        return _report(workspace, migrated=True, backup_root=backup_root)


def _report(workspace: Workspace, *, migrated: bool, backup_root: str | None) -> dict[str, object]:
    status = inspect_core_schema(workspace)
    operation = get_operation(workspace.state, MIGRATION_OPERATION)
    return {
        "migrated": migrated,
        **asdict(status),
        "receipts": _receipts(workspace),
        "operation_id": None if operation is None else MIGRATION_OPERATION,
        "backup_root": backup_root,
        "backup_manifest_sha256": None if operation is None else operation["payload_hash"],
    }
