"""Optional, independently versioned source-library metadata; core schemas stay pinned."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3

    import duckdb

    from aegis_alpha.storage.workspace import Workspace

_VERSION = 1
_DDL = """CREATE TABLE source_library_schema(
 version INTEGER PRIMARY KEY, checksum VARCHAR NOT NULL);
CREATE TABLE source_library_commits(
 source_id VARCHAR PRIMARY KEY, operation_id VARCHAR NOT NULL,
 request_hash VARCHAR NOT NULL, source_sha256 VARCHAR NOT NULL,
 store_kind VARCHAR NOT NULL, manifest_json VARCHAR NOT NULL
);"""
_CHECKSUM = hashlib.sha256(_DDL.encode()).hexdigest()


def quoted(value: str) -> str:
    """Quote an identifier without ever evaluating source DDL."""
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("source identifier must be nonempty text without NUL")
    return '"' + value.replace('"', '""') + '"'


def encoded(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def admit(connection: sqlite3.Connection | duckdb.DuckDBPyConnection, *, create: bool) -> bool:
    present = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='source_library_schema'"
    ).fetchone()
    if present is None:
        if not create:
            return False
        try:
            connection.execute("BEGIN TRANSACTION")
            for statement in _DDL.split(";"):
                if statement.strip():
                    connection.execute(statement)
            connection.execute(
                "INSERT INTO source_library_schema VALUES (?,?)", [_VERSION, _CHECKSUM]
            )
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
    # Two rows suffice to reject duplicates; never fetch an unbounded checksum.
    # HEX measures every byte (including NUL/UTF-8) on both engines. Return the
    # original value, not a prefix, so the exact Python comparison stays intact.
    rows = connection.execute(
        """SELECT version,CASE WHEN length(hex(checksum))=? THEN checksum END
        FROM source_library_schema LIMIT 2""",
        [2 * len(_CHECKSUM.encode())],
    ).fetchall()
    if [tuple(row) for row in rows] != [(_VERSION, _CHECKSUM)]:
        raise ValueError("unsupported source-library schema/checksum")
    return True


def connections(workspace: Workspace) -> dict[str, sqlite3.Connection | duckdb.DuckDBPyConnection]:
    if workspace.strategies is None:
        raise ValueError("source library requires the private strategy store")
    return {
        "state": workspace.state,
        "strategies": workspace.strategies,
        "market": workspace.market,
    }


def ensure(workspace: Workspace, *, create: bool = False) -> bool:
    states = [admit(conn, create=create) for conn in connections(workspace).values()]
    if any(states) and not all(states):
        raise ValueError("incomplete source-library schema; repeat the explicit import")
    return all(states)
