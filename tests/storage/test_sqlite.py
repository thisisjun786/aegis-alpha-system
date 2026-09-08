from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from aegis_alpha.storage.sqlite import connect, initialize

_DDL = (
    "CREATE TABLE parents(id TEXT PRIMARY KEY) STRICT; "
    "CREATE TABLE children(id TEXT PRIMARY KEY, parent_id TEXT REFERENCES parents(id)) STRICT;"
)


def test_atomic_schema_failure_leaves_no_tables(tmp_path: Path) -> None:
    connection = connect(tmp_path / "test.sqlite3")
    try:
        with pytest.raises(sqlite3.Error):
            initialize(connection, "installation", "test", "CREATE TABLE x(id TEXT); INVALID;")
        assert (
            connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == []
        )
        initialize(connection, "installation", "test", _DDL)
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute("INSERT INTO children VALUES ('child', 'absent')")
        connection.rollback()
    finally:
        connection.close()


def test_schema_checksum_mismatch_and_read_only(tmp_path: Path) -> None:
    path = tmp_path / "test.sqlite3"
    connection = connect(path)
    initialize(connection, "installation", "test", _DDL)
    connection.close()
    connection = connect(path, read_only=True)
    try:
        initialize(connection, "installation", "test", _DDL)
        with pytest.raises(ValueError, match="mismatch"):
            initialize(connection, "installation", "test", _DDL + " ")
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("INSERT INTO parents VALUES ('new')")
    finally:
        connection.close()
