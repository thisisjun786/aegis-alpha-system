from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from aegis_alpha.storage.sqlite import connect
from aegis_alpha.storage.state import (
    complete_operation,
    get_operation,
    initialize_state,
    prepare_operation,
)


def test_idempotent_intent_conflict_and_completed_immutability(tmp_path: Path) -> None:
    connection = connect(tmp_path / "state.sqlite3")
    initialize_state(connection, "synthetic")
    kwargs = {
        "operation_id": "op",
        "kind": "market_publish",
        "request_hash": "a" * 64,
        "target_id": "g",
        "payload_hash": "b" * 64,
    }
    first = prepare_operation(connection, expected_parent=None, created_at_us=None, **kwargs)
    assert (
        prepare_operation(connection, expected_parent=None, created_at_us=None, **kwargs) == first
    )
    with pytest.raises(ValueError, match="different"):
        prepare_operation(
            connection,
            operation_id="op",
            kind="market_publish",
            request_hash="a" * 64,
            target_id="g",
            expected_parent=None,
            payload_hash="c" * 64,
        )
    complete_operation(connection, "op", "a" * 64)
    complete_operation(connection, "op", "a" * 64)
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute(
            "UPDATE storage_operations SET request_hash=? WHERE operation_id='op'", ("c" * 64,)
        )
    connection.close()


def test_operation_completion_rolls_back_with_catalog_transaction(tmp_path: Path) -> None:
    connection = connect(tmp_path / "state.sqlite3")
    initialize_state(connection, "synthetic")
    prepare_operation(
        connection,
        operation_id="op",
        kind="market_publish",
        request_hash="a" * 64,
        target_id="g",
        expected_parent=None,
        payload_hash="b" * 64,
    )
    with pytest.raises(RuntimeError, match="crash"), connection:  # noqa: PT012 -- injected transaction interruption
        connection.execute("INSERT INTO issuers VALUES ('synthetic','issuer')")
        complete_operation(connection, "op", "a" * 64)
        raise RuntimeError("crash")
    assert connection.execute("SELECT count(*) FROM issuers").fetchone()[0] == 0
    assert get_operation(connection, "op")["phase"] == "PREPARED"
    connection.close()
