"""Explicit, resumable two-store run add-on. Ordinary inspection is SELECT-only."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Literal

from aegis_alpha.data.serialization import content_sha256
from aegis_alpha.storage.state import atomic, complete_operation, get_operation, prepare_operation

if TYPE_CHECKING:
    from pathlib import Path

    from aegis_alpha.storage.workspace import Workspace

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
    "run_details": (
        "CREATE TABLE run_details(run_id TEXT NOT NULL PRIMARY KEY REFERENCES runs(run_id), "
        "request_hash TEXT NOT NULL CHECK(length(request_hash)=64), "
        "prior_run_id TEXT REFERENCES runs(run_id), request_schema TEXT NOT NULL "
        "CHECK(request_schema='aas-backtest-request-v1')) STRICT"
    ),
}
_STATE_OBJECTS = {("table", key): value for key, value in _STATE_TABLES.items()}
_STATE_OBJECTS.update(
    {
        (
            "trigger",
            f"immutable_{table}_{action.lower()}",
        ): (
            f"CREATE TRIGGER immutable_{table}_{action.lower()} BEFORE {action} ON {table} "
            "BEGIN SELECT RAISE(ABORT,'immutable run record'); END"
        )
        for table in _STATE_TABLES
        for action in ("UPDATE", "DELETE")
    }
)
_STATE_OBJECTS[("index", "run_details_request_hash")] = (
    "CREATE INDEX run_details_request_hash ON run_details(request_hash)"
)
STATE_DDL = ";\n".join(_STATE_OBJECTS.values()) + ";\n"
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
STATE_CHECKSUM = hashlib.sha256(STATE_DDL.encode()).hexdigest()
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


def _sql(value: str) -> str:
    return value.strip().removesuffix(";")


def _local_state(workspace: Workspace) -> bool:
    rows = workspace.state.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE sql IS NOT NULL"
    ).fetchall()
    reserved_names = {name for _, name in _STATE_OBJECTS}
    owned = {
        (row[0], row[1]): row[3]
        for row in rows
        if row[2].lower() in _STATE_TABLES or row[1].lower() in reserved_names
    }
    if not owned:
        return False
    if {key: _sql(value) for key, value in owned.items()} != {
        key: _sql(value) for key, value in _STATE_OBJECTS.items()
    }:
        raise RunSchemaError("run_schema_invalid", "state add-on actual schema mismatch")
    receipts = workspace.state.execute("SELECT version,checksum FROM run_schema").fetchall()
    if [tuple(row) for row in receipts] != [(1, STATE_CHECKSUM)]:
        raise RunSchemaError("run_schema_invalid", "state receipt mismatch")
    return True


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
        (1, MARKET_CHECKSUM)
    ]:
        raise RunSchemaError("run_schema_invalid", "market receipt mismatch")
    return True


def _intent(workspace: Workspace) -> dict[str, str | None]:
    payload = {
        "schema": "aas-run-schema-install-v1",
        "hash_format": "aas-canonical-json-sha256-v1",
        "state_checksum": STATE_CHECKSUM,
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


def inspect_run_schema(workspace: Workspace) -> RunSchemaState:
    state, market = _local_state(workspace), _local_market(workspace)
    operations = workspace.state.execute(
        "SELECT operation_id FROM storage_operations "
        "WHERE kind='run-schema-install' OR operation_id=?",
        (_OPERATION,),
    ).fetchall()
    if not operations:
        if state or market:
            raise RunSchemaError("run_schema_invalid", "unowned add-on objects")
        return RunSchemaState(
            "absent", state_installed=False, market_installed=False, operation_id=None
        )
    operation = get_operation(workspace.state, _OPERATION)
    if (
        len(operations) != 1
        or operation is None
        or any(operation[key] != value for key, value in _intent(workspace).items())
    ):
        raise RunSchemaError("run_schema_invalid", "install intent identity mismatch")
    if operation["phase"] == "PREPARED":
        return RunSchemaState("partial", state, market, _OPERATION)
    if operation["phase"] != "COMPLETED" or not state or not market:
        raise RunSchemaError("run_schema_invalid", "completed intent requires both exact add-ons")
    return RunSchemaState(
        "complete", state_installed=True, market_installed=True, operation_id=_OPERATION
    )


def require_run_schema(workspace: Workspace) -> None:
    status = inspect_run_schema(workspace)
    if status.state != "complete":
        raise RunSchemaError(
            "run_schema_required" if status.state == "absent" else "run_schema_incomplete",
            "repeat explicit aas db run-install",
        )


def _install_state(workspace: Workspace) -> None:
    with atomic(workspace.state):
        for statement in _STATE_OBJECTS.values():
            workspace.state.execute(statement)
        workspace.state.execute("INSERT INTO run_schema VALUES (1,?)", (STATE_CHECKSUM,))
    _local_state(workspace)


def _install_market(workspace: Workspace) -> None:
    workspace.market.execute("BEGIN TRANSACTION")
    try:
        for statement in _MARKET_TABLES.values():
            workspace.market.execute(statement)
        workspace.market.execute("INSERT INTO run_schema VALUES (1,?)", [MARKET_CHECKSUM])
        workspace.market.execute("COMMIT")
    except BaseException:
        workspace.market.execute("ROLLBACK")
        raise
    _local_market(workspace)


def install_run_schema(home: Path, *, backup_output: Path | None = None) -> dict[str, object]:
    from aegis_alpha.storage.backup import (  # noqa: PLC0415 -- backup verifies this schema
        backup_workspace,
    )
    from aegis_alpha.storage.workspace import open_workspace  # noqa: PLC0415

    with open_workspace(home, writable=True) as workspace:
        status = inspect_run_schema(workspace)
        if (
            workspace.state.execute("SELECT 1 FROM runs WHERE status='RUNNING'").fetchone()
            or workspace.state.execute(
                "SELECT 1 FROM storage_operations WHERE phase='PREPARED' AND operation_id!=?",
                (_OPERATION,),
            ).fetchone()
        ):
            raise RunSchemaError(
                "run_schema_incomplete", "stop running analyses and recover unrelated operations"
            )
        if status.state == "absent":
            backup_workspace(workspace, backup_output)
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
            _install_state(workspace)
        if not status.market_installed:
            _install_market(workspace)
        if status.state != "complete":
            checked = inspect_run_schema(workspace)
            if not checked.state_installed or not checked.market_installed:
                raise RunSchemaError("run_schema_incomplete")
            complete_operation(workspace.state, _OPERATION, str(_intent(workspace)["request_hash"]))
        require_run_schema(workspace)
        return {
            **asdict(inspect_run_schema(workspace)),
            "state_checksum": STATE_CHECKSUM,
            "market_checksum": MARKET_CHECKSUM,
        }
