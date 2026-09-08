from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.market import (
    initialize_market,
    publish_generation,
    read_generation,
    verify_generation,
)
from tests.storage.test_publication import document


def publish(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, object]],
    *,
    version: str,
    parent: str | None = None,
    domain: str = "prices",
) -> dict[str, object]:
    return publish_generation(
        connection,
        dataset_id="synthetic",
        version=version,
        generation_id="g" + version,
        operation_id="op" + version,
        request_hash=version * 64,
        parent_id=parent,
        domain=domain,
        rows=rows,
    )


def test_revision_replay_tombstone_and_restart(tmp_path: Path) -> None:
    path = tmp_path / "market.duckdb"
    connection = duckdb.connect(str(path))
    initialize_market(connection, "synthetic")
    row = parse_import(document()).rows[0]
    publish(connection, [row], version="1")
    corrected = {
        **row,
        "revision_id": "r2",
        "op": "SUPERSEDE",
        "supersedes_revision_id": "r1",
        "close": "12",
        "available_at_us": 40,
        "revision_known_at_us": 40,
        "ingested_at_us": 50,
    }
    publish(connection, [corrected], version="2", parent="g1")
    assert read_generation(connection, "g2", cutoff_us=30)[0]["close"] == Decimal(11)
    assert read_generation(connection, "g2", cutoff_us=40)[0]["close"] == Decimal(12)
    assert read_generation(connection, "g2", cutoff_us=40, ingestion_cutoff_us=35)[0][
        "close"
    ] == Decimal(11)
    tombstone = {
        **corrected,
        "revision_id": "r3",
        "op": "TOMBSTONE",
        "supersedes_revision_id": "r2",
        "available_at_us": 60,
        "revision_known_at_us": 60,
        "ingested_at_us": 70,
    }
    publish(connection, [tombstone], version="3", parent="g2")
    assert read_generation(connection, "g3", cutoff_us=60) == []
    connection.close()
    connection = duckdb.connect(str(path))
    try:
        assert read_generation(connection, "g3", cutoff_us=40)[0]["close"] == Decimal(12)
        assert verify_generation(connection, "g1")["row_count"] == 1
    finally:
        connection.close()


def test_ambiguous_revision_and_float_value_rejected() -> None:
    connection = duckdb.connect()
    initialize_market(connection, "synthetic")
    row = parse_import(document()).rows[0]
    publish(connection, [row], version="1")
    with pytest.raises(ValueError, match="ASSERT"):
        publish(connection, [{**row, "revision_id": "r2"}], version="2", parent="g1")
    with pytest.raises(TypeError, match="floating"):
        publish(connection, [{**row, "close": 1.1}], version="3", parent="g1")
    assert connection.execute("SELECT count(*) FROM market_generations").fetchall() == [(1,)]
    connection.close()


def test_unknown_availability_and_adjusted_reference_excluded() -> None:
    connection = duckdb.connect()
    initialize_market(connection, "synthetic")
    row = parse_import(document()).rows[0]
    publish(connection, [{**row, "available_at_us": None}], version="1")
    assert read_generation(connection, "g1", cutoff_us=100) == []
    assert len(read_generation(connection, "g1")) == 1
    connection.close()
    connection = duckdb.connect()
    initialize_market(connection, "synthetic")
    publish(
        connection, [{**row, "basis": "split_adjusted", "price_role": "reference"}], version="1"
    )
    assert read_generation(connection, "g1", cutoff_us=100) == []
    connection.close()


def test_logical_tampering_detected() -> None:
    connection = duckdb.connect()
    initialize_market(connection, "synthetic")
    publish(connection, parse_import(document()).rows, version="1")
    connection.execute("UPDATE prices SET close=99")
    with pytest.raises(ValueError, match="hash"):
        verify_generation(connection, "g1")
    connection.close()
