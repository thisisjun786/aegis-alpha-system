"""Small relational state and idempotent cross-store operation intents."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager

from aegis_alpha.storage.sqlite import initialize
from aegis_alpha.storage.state_schema import DDL


@contextmanager
def atomic(connection: sqlite3.Connection) -> Iterator[None]:
    """Own a transaction only when the caller does not already own one."""
    nested = connection.in_transaction
    connection.execute("SAVEPOINT aas_operation" if nested else "BEGIN IMMEDIATE")
    try:
        yield
        connection.execute("RELEASE SAVEPOINT aas_operation" if nested else "COMMIT")
    except BaseException:
        if nested:
            connection.execute("ROLLBACK TO SAVEPOINT aas_operation")
            connection.execute("RELEASE SAVEPOINT aas_operation")
        else:
            connection.rollback()
        raise


def initialize_state(connection: sqlite3.Connection, installation_id: str) -> None:
    initialize(connection, installation_id, "state", DDL)


def get_operation(connection: sqlite3.Connection, operation_id: str) -> dict[str, object] | None:
    row = connection.execute(
        "SELECT operation_id,kind,request_hash,target_id,expected_parent,payload_hash,phase,"
        "failure_reason,created_at_us,completed_at_us FROM storage_operations WHERE operation_id=?",
        (operation_id,),
    ).fetchone()
    return None if row is None else dict(row)


def prepare_operation(  # noqa: PLR0913 -- durable intent fields are all explicit
    connection: sqlite3.Connection,
    *,
    operation_id: str,
    kind: str,
    request_hash: str,
    target_id: str,
    expected_parent: str | None,
    payload_hash: str,
    created_at_us: int | None = None,
) -> dict[str, object]:
    wanted = {
        "operation_id": operation_id,
        "kind": kind,
        "request_hash": request_hash,
        "target_id": target_id,
        "expected_parent": expected_parent,
        "payload_hash": payload_hash,
    }
    if any(
        not isinstance(v, str) or not v.strip() for k, v in wanted.items() if k != "expected_parent"
    ):
        raise ValueError("operation identity fields must be nonempty")
    with atomic(connection):
        previous = get_operation(connection, operation_id)
        if previous is not None:
            if any(previous[key] != value for key, value in wanted.items()):
                raise ValueError("operation ID already identifies a different request")
            if previous["phase"] == "QUARANTINED":
                raise ValueError("operation is quarantined")
            return previous
        connection.execute(
            "INSERT INTO "
            "storage_operations(operation_id,kind,request_hash,target_id,expected_parent,"
            "payload_hash,phase,created_at_us) VALUES (?,?,?,?,?,?,'PREPARED',?)",
            (
                operation_id,
                kind,
                request_hash,
                target_id,
                expected_parent,
                payload_hash,
                time.time_ns() // 1000 if created_at_us is None else created_at_us,
            ),
        )
    result = get_operation(connection, operation_id)
    if result is None:
        raise RuntimeError("prepared operation disappeared")
    return result


def complete_operation(
    connection: sqlite3.Connection, operation_id: str, request_hash: str
) -> None:
    with atomic(connection):
        previous = get_operation(connection, operation_id)
        if previous is None or previous["request_hash"] != request_hash:
            raise ValueError("completion has no matching operation intent")
        if previous["phase"] == "COMPLETED":
            return
        if previous["phase"] != "PREPARED":
            raise ValueError("quarantined operation cannot complete")
        connection.execute(
            "UPDATE storage_operations SET phase='COMPLETED', completed_at_us=? "
            "WHERE operation_id=?",
            (time.time_ns() // 1000, operation_id),
        )


def state_status(connection: sqlite3.Connection) -> dict[str, object]:
    return {
        "datasets": connection.execute("SELECT count(*) FROM dataset_versions").fetchone()[0],
        "pending_operations": connection.execute(
            "SELECT count(*) FROM storage_operations WHERE phase='PREPARED'"
        ).fetchone()[0],
        "runs": connection.execute("SELECT count(*) FROM runs").fetchone()[0],
    }


def quarantine_operation(connection: sqlite3.Connection, operation_id: str, reason: str) -> None:
    if not reason.strip():
        raise ValueError("quarantine requires a reason")
    with atomic(connection):
        previous = get_operation(connection, operation_id)
        if previous is None or previous["phase"] != "PREPARED":
            raise ValueError("only a prepared operation can be quarantined")
        connection.execute(
            "UPDATE storage_operations SET phase='QUARANTINED',failure_reason=? "
            "WHERE operation_id=?",
            (reason, operation_id),
        )
