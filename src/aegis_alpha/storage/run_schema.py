"""Explicit, resumable two-store run add-on. Ordinary inspection is SELECT-only."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Literal

from aegis_alpha.data.serialization import content_sha256
from aegis_alpha.storage.state import atomic, complete_operation, get_operation, prepare_operation

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace

_HASH_FORMAT = "aas-canonical-json-sha256-v1"
BACKTEST_REQUEST_SCHEMA = "aas-backtest-request-v1"
RESEARCH_REQUEST_SCHEMA = "aas-research-run-v2"
# The explicit allow-list run_details carries, in the order its CHECK spells them. A
# run is described by exactly one of these documents and the store never infers which:
# the schema is recorded from the request that was actually registered.
REQUEST_SCHEMAS = (BACKTEST_REQUEST_SCHEMA, RESEARCH_REQUEST_SCHEMA)
# The state add-on version whose CHECK actually holds each request schema. v1 named one
# schema as a literal, so recording a declared research run is a versioned migration of
# an installed add-on rather than a write the old CHECK would have accepted.
REQUEST_SCHEMA_VERSION = {BACKTEST_REQUEST_SCHEMA: 1, RESEARCH_REQUEST_SCHEMA: 2}
STATE_VERSION = 2
# The market add-on's DDL is untouched here, so its receipt stays at 1. Bumping it for
# symmetry would make a version number stop meaning that something actually changed.
MARKET_VERSION = 1
_STATE_TABLES = {
    "run_schema": (
        "CREATE TABLE run_schema(version INTEGER PRIMARY KEY, "
        "checksum TEXT NOT NULL CHECK(length(checksum)=64)) STRICT"
    ),
    "backtest_requests": (
        "CREATE TABLE backtest_requests(bundle_id TEXT NOT NULL PRIMARY KEY "
        "REFERENCES input_bundles(bundle_id), request_bytes BLOB NOT NULL, "
        "request_hash TEXT NOT NULL CHECK(length(request_hash)=64)) STRICT"
    ),
}
_RUN_DETAILS_HEAD = (
    "CREATE TABLE run_details(run_id TEXT NOT NULL PRIMARY KEY REFERENCES runs(run_id), "
    "request_hash TEXT NOT NULL CHECK(length(request_hash)=64), "
    "prior_run_id TEXT REFERENCES runs(run_id), request_schema TEXT NOT NULL "
)
# v1's single-value CHECK is kept verbatim. An installation that never migrated is
# recognised by these exact bytes, so this is stored history rather than dead code.
_RUN_DETAILS = {
    1: _RUN_DETAILS_HEAD + "CHECK(request_schema='" + BACKTEST_REQUEST_SCHEMA + "')) STRICT",
    2: (
        _RUN_DETAILS_HEAD
        + "CHECK(request_schema IN ("
        + ",".join("'" + schema + "'" for schema in REQUEST_SCHEMAS)
        + "))) STRICT"
    ),
}
_TABLE_NAMES = frozenset({*_STATE_TABLES, "run_details"})


def _state_objects(version: int) -> dict[tuple[str, str], str]:
    """Every object one add-on version owns, in the order its DDL checksum covers."""
    tables = {**_STATE_TABLES, "run_details": _RUN_DETAILS[version]}
    objects: dict[tuple[str, str], str] = {("table", key): value for key, value in tables.items()}
    objects.update(
        {
            (
                "trigger",
                f"immutable_{table}_{action.lower()}",
            ): (
                f"CREATE TRIGGER immutable_{table}_{action.lower()} BEFORE {action} ON {table} "
                "BEGIN SELECT RAISE(ABORT,'immutable run record'); END"
            )
            for table in tables
            for action in ("UPDATE", "DELETE")
        }
    )
    objects[("index", "run_details_request_hash")] = (
        "CREATE INDEX run_details_request_hash ON run_details(request_hash)"
    )
    return objects


_STATE_OBJECTS = {version: _state_objects(version) for version in _RUN_DETAILS}
_STATE_DDL = {
    version: ";\n".join(objects.values()) + ";\n" for version, objects in _STATE_OBJECTS.items()
}
STATE_CHECKSUMS = {
    version: hashlib.sha256(ddl.encode()).hexdigest() for version, ddl in _STATE_DDL.items()
}
STATE_DDL = _STATE_DDL[STATE_VERSION]
# The receipt table is append-only under its own immutable triggers, so a migration adds
# a row instead of rewriting one. These are the three histories an admitted state add-on
# can show: installed at v1, installed at v2, or installed at v1 and then migrated.
_STATE_HISTORIES = ((1,), (STATE_VERSION,), (1, STATE_VERSION))
# The name the rebuild renames the old run_details to. It lives only inside the rebuild
# transaction, so finding it afterwards means an add-on nobody finished.
_REBUILD_TABLE = "run_details_rebuild_v1"
# Named once so the rebuild copies exactly the columns both versions declare, in order.
_RUN_DETAIL_COLUMNS = "run_id,request_hash,prior_run_id,request_schema"
_MARKET_TABLES = {
    "run_schema": (
        "CREATE TABLE run_schema(version INTEGER PRIMARY KEY, "
        "checksum VARCHAR NOT NULL CHECK(length(checksum)=64))"
    ),
    "result_trade_decisions": (
        "CREATE TABLE result_trade_decisions(run_id VARCHAR NOT NULL REFERENCES "
        "result_commits(run_id), module VARCHAR NOT NULL, ordinal BIGINT NOT NULL "
        "CHECK(ordinal>=0), decision_at_us BIGINT NOT NULL CHECK(decision_at_us>=0), "
        "PRIMARY KEY(run_id,module,ordinal))"
    ),
}
MARKET_DDL = ";\n".join(_MARKET_TABLES.values()) + ";\n"
STATE_CHECKSUM = STATE_CHECKSUMS[STATE_VERSION]
MARKET_CHECKSUM = hashlib.sha256(MARKET_DDL.encode()).hexdigest()
# DuckDB's catalog prints PK-implied NOT NULL only for non-PK columns. These are
# its exact v1 catalog forms; columns/nullability are checked independently below.
_MARKET_CATALOG = {
    "run_schema": (
        'CREATE TABLE run_schema("version" INTEGER PRIMARY KEY, '
        "checksum VARCHAR NOT NULL, CHECK((length(checksum) = 64)));"
    ),
    "result_trade_decisions": (
        "CREATE TABLE result_trade_decisions(run_id VARCHAR, module VARCHAR, ordinal BIGINT, "
        "decision_at_us BIGINT NOT NULL, FOREIGN KEY (run_id) REFERENCES result_commits(run_id), "
        "CHECK((ordinal >= 0)), CHECK((decision_at_us >= 0)), "
        "PRIMARY KEY(run_id, module, ordinal));"
    ),
}
_MARKET_COLUMNS = {
    "run_schema": [("version", "INTEGER", False), ("checksum", "VARCHAR", False)],
    "result_trade_decisions": [
        ("run_id", "VARCHAR", False),
        ("module", "VARCHAR", False),
        ("ordinal", "BIGINT", False),
        ("decision_at_us", "BIGINT", False),
    ],
}
_OPERATION = "run-schema-install-v1"
MIGRATION_KIND = "run-schema-migrate"
MIGRATION_OPERATION = "run-schema-migrate-v2"


class RunSchemaError(ValueError):
    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        super().__init__(reason + (": " + detail if detail else ""))


@dataclass(frozen=True, slots=True)
class RunSchemaState:
    state: Literal["absent", "partial", "complete"]
    state_installed: bool
    market_installed: bool
    operation_id: str | None
    # The add-on version actually on disk, read from the append-only receipts rather
    # than assumed from the code that is running.
    state_version: int | None = None
    # PREPARED while a migration is unfinished, COMPLETED once it is recorded.
    migration_phase: str | None = None
    # The version the install intent was prepared against, which stays at 1 on an
    # installation that was migrated afterwards.
    install_version: int | None = None


def _sql(value: str) -> str:
    return value.strip().removesuffix(";")


def _receipt_version(workspace: Workspace) -> int:
    """Read the installed version from the receipts, refusing any other history."""
    receipts = [
        (int(row[0]), str(row[1]))
        for row in workspace.state.execute(
            "SELECT version,checksum FROM run_schema ORDER BY version"
        )
    ]
    history = tuple(version for version, _checksum in receipts)
    if history not in _STATE_HISTORIES or any(
        checksum != STATE_CHECKSUMS[version] for version, checksum in receipts
    ):
        raise RunSchemaError("run_schema_invalid", "state receipt mismatch")
    return history[-1]


def _local_state(workspace: Workspace) -> int | None:
    """Return the installed state add-on version, or None when it is absent."""
    # SQLite folds ASCII identifiers only; Python lower() also folds e.g. U+212A.
    expected = _STATE_OBJECTS[STATE_VERSION]
    owned = {
        (row[0], row[1]): row[2]
        for _, name in expected
        for row in workspace.state.execute(
            "SELECT type,name,sql FROM sqlite_schema WHERE sql IS NOT NULL AND "
            "(name = ? COLLATE NOCASE OR (? AND tbl_name = ? COLLATE NOCASE))",
            (name, name in _TABLE_NAMES, name),
        )
    }
    if not owned:
        return None
    if workspace.state.execute(
        "SELECT 1 FROM sqlite_schema WHERE name=? COLLATE NOCASE", (_REBUILD_TABLE,)
    ).fetchone():
        # The rebuild is one transaction, so this table cannot survive an interruption.
        # If it is here anyway, something edited the add-on outside this module.
        raise RunSchemaError("run_schema_invalid", "an unfinished add-on rebuild is present")
    # The object names are the same at every version, so a missing or extra object is
    # a mismatch before any receipt is trusted to say which DDL text to expect.
    if owned.keys() != expected.keys():
        raise RunSchemaError("run_schema_invalid", "state add-on actual schema mismatch")
    version = _receipt_version(workspace)
    if {key: _sql(value) for key, value in owned.items()} != {
        key: _sql(value) for key, value in _STATE_OBJECTS[version].items()
    }:
        raise RunSchemaError("run_schema_invalid", "state add-on actual schema mismatch")
    return version


def _local_market(workspace: Workspace) -> bool:
    rows = workspace.market.execute("SELECT type,name,tbl_name,sql FROM sqlite_master").fetchall()
    owned = {row[1]: row[3] for row in rows if row[2].lower() in _MARKET_TABLES}
    if not owned:
        return False
    if {key: _sql(value) for key, value in owned.items()} != {
        key: _sql(value) for key, value in _MARKET_CATALOG.items()
    }:
        raise RunSchemaError("run_schema_invalid", "market add-on actual schema mismatch")
    for table, expected in _MARKET_COLUMNS.items():
        columns = workspace.market.execute(
            "SELECT column_name,data_type,is_nullable FROM duckdb_columns() "
            "WHERE schema_name='main' AND table_name=? ORDER BY column_index",
            [table],
        ).fetchall()
        if columns != expected:
            raise RunSchemaError("run_schema_invalid", "market columns/nullability mismatch")
    if workspace.market.execute("SELECT version,checksum FROM run_schema").fetchall() != [
        (MARKET_VERSION, MARKET_CHECKSUM)
    ]:
        raise RunSchemaError("run_schema_invalid", "market receipt mismatch")
    return True


def _intent(workspace: Workspace, version: int = STATE_VERSION) -> dict[str, str | None]:
    """The install intent exactly as the named add-on version recorded it.

    An installation made before this migration committed v1's checksum, so inspection
    compares against the version that was installed rather than the one this code would
    install now. Recomputing it from current constants would reject every add-on on disk.
    """
    payload = {
        "schema": "aas-run-schema-install-v1",
        "hash_format": _HASH_FORMAT,
        "state_checksum": STATE_CHECKSUMS[version],
        "market_checksum": MARKET_CHECKSUM,
    }
    request = {
        **payload,
        "installation_id": workspace.installation_id,
        "state_store_id": workspace.state.execute("SELECT store_id FROM store_info").fetchone()[0],
        "market_store_id": workspace.market.execute("SELECT store_id FROM store_info").fetchall()[
            0
        ][0],
    }
    return {
        "operation_id": _OPERATION,
        "kind": "run-schema-install",
        "request_hash": content_sha256(request),
        "target_id": workspace.installation_id,
        "expected_parent": None,
        "payload_hash": content_sha256(payload),
    }


def _migration_intent(workspace: Workspace) -> dict[str, str | None]:
    """The durable intent of the one migration this module knows how to apply."""
    payload = {
        "schema": "aas-run-schema-migrate-v1",
        "hash_format": _HASH_FORMAT,
        "from_version": 1,
        "to_version": STATE_VERSION,
        "state_checksum": STATE_CHECKSUMS[STATE_VERSION],
        "request_schemas": list(REQUEST_SCHEMAS),
    }
    request = {
        **payload,
        "installation_id": workspace.installation_id,
        "state_store_id": workspace.state.execute("SELECT store_id FROM store_info").fetchone()[0],
    }
    return {
        "operation_id": MIGRATION_OPERATION,
        "kind": MIGRATION_KIND,
        "request_hash": content_sha256(request),
        "target_id": workspace.installation_id,
        # The exact add-on this migration was prepared against, so a resumed attempt
        # cannot apply to a state schema it never inspected.
        "expected_parent": STATE_CHECKSUMS[1],
        "payload_hash": content_sha256(payload),
    }


def _migration_phase(workspace: Workspace) -> str | None:
    """Report the migration intent's phase, refusing an intent that is not ours."""
    operations = workspace.state.execute(
        "SELECT operation_id FROM storage_operations WHERE kind=? OR operation_id=?",
        (MIGRATION_KIND, MIGRATION_OPERATION),
    ).fetchall()
    if not operations:
        return None
    operation = get_operation(workspace.state, MIGRATION_OPERATION)
    if (
        len(operations) != 1
        or operation is None
        or any(operation[key] != value for key, value in _migration_intent(workspace).items())
    ):
        raise RunSchemaError("run_schema_invalid", "migration intent identity mismatch")
    if operation["phase"] not in ("PREPARED", "COMPLETED"):
        raise RunSchemaError("run_schema_invalid", "the run add-on migration is quarantined")
    return str(operation["phase"])


def _install_version(workspace: Workspace, operation: dict[str, object] | None, found: int) -> int:
    """Name the one supported install intent this operation is, or refuse it."""
    matched = [
        version
        for version in _STATE_OBJECTS
        if operation is not None
        and not any(operation[key] != value for key, value in _intent(workspace, version).items())
    ]
    if found != 1 or len(matched) != 1:
        raise RunSchemaError("run_schema_invalid", "install intent identity mismatch")
    return matched[0]


def inspect_run_schema(workspace: Workspace) -> RunSchemaState:
    state, market = _local_state(workspace), _local_market(workspace)
    installed = state is not None
    migration = _migration_phase(workspace)
    operations = workspace.state.execute(
        "SELECT operation_id FROM storage_operations "
        "WHERE kind='run-schema-install' OR operation_id=?",
        (_OPERATION,),
    ).fetchall()
    if not operations:
        if installed or market or migration is not None:
            raise RunSchemaError("run_schema_invalid", "unowned add-on objects")
        return RunSchemaState(
            "absent", state_installed=False, market_installed=False, operation_id=None
        )
    operation = get_operation(workspace.state, _OPERATION)
    install_version = _install_version(workspace, operation, len(operations))
    if operation is None:
        raise RunSchemaError("run_schema_invalid", "install intent identity mismatch")
    if operation["phase"] == "PREPARED":
        return RunSchemaState(
            "partial", installed, market, _OPERATION, state, migration, install_version
        )
    if operation["phase"] != "COMPLETED" or not installed or not market:
        raise RunSchemaError("run_schema_invalid", "completed intent requires both exact add-ons")
    return RunSchemaState(
        "complete",
        state_installed=True,
        market_installed=True,
        operation_id=_OPERATION,
        state_version=state,
        migration_phase=migration,
        install_version=install_version,
    )


def require_run_schema(workspace: Workspace) -> None:
    status = inspect_run_schema(workspace)
    if status.state != "complete":
        raise RunSchemaError(
            "run_schema_required" if status.state == "absent" else "run_schema_incomplete",
            "repeat explicit aas db run-install",
        )


def require_request_schema(workspace: Workspace, schema: str) -> None:
    """Admit a request schema only where the installed CHECK would actually hold it.

    The refusal names the migration rather than letting the write reach a constraint
    error, so an operator on an unmigrated installation is told what to run.
    """
    if schema not in REQUEST_SCHEMA_VERSION:
        raise RunSchemaError("run_schema_unsupported_request", schema)
    status = inspect_run_schema(workspace)
    if status.state != "complete":
        raise RunSchemaError(
            "run_schema_required" if status.state == "absent" else "run_schema_incomplete",
            "repeat explicit aas db run-install",
        )
    needed = REQUEST_SCHEMA_VERSION[schema]
    if (status.state_version or 0) < needed:
        raise RunSchemaError(
            "run_schema_outdated",
            schema + " needs state add-on v" + str(needed) + "; run aas db run-migrate",
        )


def _install_state(workspace: Workspace, version: int) -> None:
    with atomic(workspace.state):
        for statement in _STATE_OBJECTS[version].values():
            workspace.state.execute(statement)
        workspace.state.execute(
            "INSERT INTO run_schema VALUES (?,?)", (version, STATE_CHECKSUMS[version])
        )
    _local_state(workspace)


def _install_market(workspace: Workspace) -> None:
    workspace.market.execute("BEGIN TRANSACTION")
    try:
        for statement in _MARKET_TABLES.values():
            workspace.market.execute(statement)
        workspace.market.execute(
            "INSERT INTO run_schema VALUES (?,?)", [MARKET_VERSION, MARKET_CHECKSUM]
        )
        workspace.market.execute("COMMIT")
    except BaseException:
        workspace.market.execute("ROLLBACK")
        raise
    _local_market(workspace)


def _rebuild_state(workspace: Workspace) -> None:
    """Rebuild run_details under the widened CHECK, carrying every recorded run.

    SQLite cannot alter a CHECK in place, so the table is rebuilt. The old table is
    renamed out of the way and the new one is created under its own name, because
    ALTER TABLE ... RENAME rewrites the stored DDL text and this add-on is recognised by
    exact DDL. Dropping a table does not fire its DELETE trigger, and the triggers are
    dropped first anyway, so the immutability rule is lifted deliberately for the rebuild
    rather than worked around row by row.

    The whole rebuild is one transaction. An interruption leaves the installed v1 add-on
    exactly as it was, with the prepared intent still naming the work to redo.
    """
    objects = _STATE_OBJECTS[STATE_VERSION]
    with atomic(workspace.state):
        for action in ("update", "delete"):
            workspace.state.execute("DROP TRIGGER immutable_run_details_" + action)
        workspace.state.execute("DROP INDEX run_details_request_hash")
        workspace.state.execute("ALTER TABLE run_details RENAME TO " + _REBUILD_TABLE)
        workspace.state.execute(objects[("table", "run_details")])
        workspace.state.execute(
            f"INSERT INTO run_details({_RUN_DETAIL_COLUMNS}) "  # noqa: S608 -- module constants
            f"SELECT {_RUN_DETAIL_COLUMNS} FROM {_REBUILD_TABLE}"
        )
        carried = workspace.state.execute(
            f"SELECT count(*) FROM {_REBUILD_TABLE}"  # noqa: S608 -- module constant
        ).fetchone()[0]
        if workspace.state.execute("SELECT count(*) FROM run_details").fetchone()[0] != carried:
            raise RunSchemaError("run_schema_invalid", "rebuild did not carry every run record")
        workspace.state.execute("DROP TABLE " + _REBUILD_TABLE)
        workspace.state.execute(objects[("index", "run_details_request_hash")])
        for action in ("update", "delete"):
            workspace.state.execute(objects[("trigger", "immutable_run_details_" + action)])
        workspace.state.execute(
            "INSERT INTO run_schema VALUES (?,?)", (STATE_VERSION, STATE_CHECKSUMS[STATE_VERSION])
        )


def _quiet(workspace: Workspace, *, excluding: str) -> None:
    """Refuse to touch the add-on while anything else could be writing runs."""
    if (
        workspace.state.execute("SELECT 1 FROM runs WHERE status='RUNNING'").fetchone()
        or workspace.state.execute(
            "SELECT 1 FROM storage_operations WHERE phase='PREPARED' AND operation_id!=?",
            (excluding,),
        ).fetchone()
    ):
        raise RunSchemaError(
            "run_schema_incomplete", "stop running analyses and recover unrelated operations"
        )


def install_run_schema(
    home: Path, *, backup_output: Path | None = None, budget: ComputeBudget | None = None
) -> dict[str, object]:
    from aegis_alpha.storage.backup import (  # noqa: PLC0415 -- backup verifies this schema
        backup_workspace,
    )
    from aegis_alpha.storage.workspace import open_workspace  # noqa: PLC0415

    with open_workspace(home, writable=True) as workspace:
        status = inspect_run_schema(workspace)
        _quiet(workspace, excluding=_OPERATION)
        # An interrupted install is resumed at the version its own intent named, so a
        # newer interpreter cannot finish it by installing a schema nobody prepared.
        version = status.install_version or STATE_VERSION
        if status.state == "absent":
            backup_workspace(workspace, backup_output, budget=budget)
            intent = _intent(workspace)
            prepare_operation(
                workspace.state,
                operation_id=_OPERATION,
                kind="run-schema-install",
                request_hash=str(intent["request_hash"]),
                target_id=workspace.installation_id,
                expected_parent=None,
                payload_hash=str(intent["payload_hash"]),
            )
        if not status.state_installed:
            _install_state(workspace, version)
        if not status.market_installed:
            _install_market(workspace)
        if status.state != "complete":
            checked = inspect_run_schema(workspace)
            if not checked.state_installed or not checked.market_installed:
                raise RunSchemaError("run_schema_incomplete")
            complete_operation(
                workspace.state, _OPERATION, str(_intent(workspace, version)["request_hash"])
            )
        require_run_schema(workspace)
        final = inspect_run_schema(workspace)
        return {
            **asdict(final),
            "state_checksum": STATE_CHECKSUMS[final.state_version or STATE_VERSION],
            "market_checksum": MARKET_CHECKSUM,
        }


def migrate_run_schema(
    home: Path, *, backup_output: Path | None = None, budget: ComputeBudget | None = None
) -> dict[str, object]:
    """Migrate an installed run add-on to the version that admits every request schema.

    Backup, then a durable intent, then one transactional rebuild, then verification,
    then completion. An installation already at the current version is a no-op, and an
    interrupted migration is finished by repeating this command or by aas db recover.
    Runs recorded under the old CHECK are carried across unchanged and stay readable.
    """
    from aegis_alpha.storage.backup import (  # noqa: PLC0415 -- backup verifies this schema
        backup_workspace,
    )
    from aegis_alpha.storage.workspace import open_workspace  # noqa: PLC0415

    with open_workspace(home, writable=True) as workspace:
        status = inspect_run_schema(workspace)
        if status.state != "complete":
            raise RunSchemaError(
                "run_schema_required" if status.state == "absent" else "run_schema_incomplete",
                "install the run add-on before migrating it",
            )
        if status.state_version == STATE_VERSION and status.migration_phase in (None, "COMPLETED"):
            return _migration_report(workspace, migrated=False)
        _quiet(workspace, excluding=MIGRATION_OPERATION)
        intent = _migration_intent(workspace)
        if status.migration_phase is None:
            # Before the intent and before the rebuild: this backup is the operator's
            # way back to the exact add-on the migration is about to replace.
            backup_workspace(workspace, backup_output, budget=budget)
        prepare_operation(
            workspace.state,
            operation_id=MIGRATION_OPERATION,
            kind=MIGRATION_KIND,
            request_hash=str(intent["request_hash"]),
            target_id=workspace.installation_id,
            expected_parent=intent["expected_parent"],
            payload_hash=str(intent["payload_hash"]),
        )
        if (inspect_run_schema(workspace).state_version or 0) < STATE_VERSION:
            _rebuild_state(workspace)
        checked = inspect_run_schema(workspace)
        if checked.state != "complete" or checked.state_version != STATE_VERSION:
            raise RunSchemaError("run_schema_incomplete", "the rebuild did not reach the version")
        complete_operation(workspace.state, MIGRATION_OPERATION, str(intent["request_hash"]))
        require_run_schema(workspace)
        return _migration_report(workspace, migrated=True)


def recover_run_schema_migration(workspace: Workspace, operation: sqlite3.Row) -> bool:
    """Complete a migration whose rebuild already landed, from stored content only.

    Recovery never rebuilds. An add-on still on the old version is left prepared so the
    operator repeats aas db run-migrate, which takes its own backup first.
    """
    if str(operation["operation_id"]) != MIGRATION_OPERATION:
        return False
    status = inspect_run_schema(workspace)
    if status.state != "complete" or status.state_version != STATE_VERSION:
        return False
    complete_operation(
        workspace.state, MIGRATION_OPERATION, str(_migration_intent(workspace)["request_hash"])
    )
    return True


def _migration_report(workspace: Workspace, *, migrated: bool) -> dict[str, object]:
    final = inspect_run_schema(workspace)
    return {
        **asdict(final),
        "migrated": migrated,
        "state_checksum": STATE_CHECKSUMS[final.state_version or STATE_VERSION],
        "market_checksum": MARKET_CHECKSUM,
        "request_schemas": list(REQUEST_SCHEMAS),
    }
