from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import cast

import duckdb
import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.storage import market
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


@pytest.fixture
def revision_chain() -> Iterator[duckdb.DuckDBPyConnection]:
    with duckdb.connect() as connection:
        initialize_market(connection, "synthetic")
        row = parse_import(document()).rows[0]
        publish(connection, [row, {**row, "instrument_id": "ASSET_B", "close": "21"}], version="1")
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
        publish(
            connection,
            [
                {
                    **corrected,
                    "revision_id": "r3",
                    "op": "TOMBSTONE",
                    "supersedes_revision_id": "r2",
                    "available_at_us": 60,
                    "revision_known_at_us": 60,
                    "ingested_at_us": 70,
                }
            ],
            version="3",
            parent="g2",
        )
        yield connection


def test_full_chain_preserves_every_revision(revision_chain: duckdb.DuckDBPyConnection) -> None:
    # This assertion deliberately supplies RED before the new public API exists.
    assert callable(getattr(market, "read_chain_rows", None)), (
        "full revision history is unavailable"
    )
    rows = market.read_chain_rows(
        revision_chain, "g3", budget=ComputeBudget(Fraction(1), 64 * 1024 * 1024)
    )
    assert [row["generation_id"] for row in rows] == ["g1", "g1", "g2", "g3"]
    assert [
        (row["revision_id"], row["supersedes_revision_id"], row["op"])
        for row in rows
        if row["instrument_id"] == "ASSET_A"
    ] == [("r1", None, "ASSERT"), ("r2", "r1", "SUPERSEDE"), ("r3", "r2", "TOMBSTONE")]
    assert {row["price_role"] for row in rows} == {"canonical"}
    assert all("source_row_hash" in row and "available_at_us" in row for row in rows)
    assert verify_generation(revision_chain, "g3")["row_count"] == 1


@pytest.mark.parametrize(
    ("cutoff", "ingestion", "expected"),
    [
        (19, None, {}),
        (30, None, {"ASSET_A": Decimal(11), "ASSET_B": Decimal(21)}),
        (40, None, {"ASSET_A": Decimal(12), "ASSET_B": Decimal(21)}),
        (60, None, {"ASSET_B": Decimal(21)}),
        (60, 35, {"ASSET_A": Decimal(11), "ASSET_B": Decimal(21)}),
    ],
)
def test_full_chain_projects_independent_cutoffs(
    revision_chain: duckdb.DuckDBPyConnection,
    cutoff: int,
    ingestion: int | None,
    expected: dict[str, Decimal],
) -> None:
    assert callable(getattr(market, "read_chain_rows", None)), (
        "full revision history is unavailable"
    )
    rows = market.read_chain_rows(
        revision_chain, "g3", budget=ComputeBudget(Fraction(1), 64 * 1024 * 1024)
    )
    projected = market.project_heads(rows, cutoff_us=cutoff, ingestion_cutoff_us=ingestion)
    assert {row["instrument_id"]: row["close"] for row in projected} == expected
    assert projected == read_generation(
        revision_chain, "g3", cutoff_us=cutoff, ingestion_cutoff_us=ingestion
    )


def test_full_chain_has_no_legacy_limit() -> None:
    with duckdb.connect() as connection:
        initialize_market(connection, "synthetic")
        row = parse_import(document()).rows[0]
        publish(
            connection, [{**row, "instrument_id": f"ASSET_{i}"} for i in range(101)], version="1"
        )
        assert callable(getattr(market, "read_chain_rows", None)), "full history must not truncate"
        rows = market.read_chain_rows(
            connection, "g1", budget=ComputeBudget(Fraction(1), 64 * 1024 * 1024)
        )
        projected = market.project_heads(rows)
        expected_count = 101
        assert len(rows) == expected_count
        assert len(projected) == expected_count
        assert [row["record_id"] for row in projected] == sorted(
            str(row["record_id"]) for row in rows
        )
        assert read_generation(connection, "g1") == projected[:100]
        assert read_generation(connection, "g1", limit=1) == projected[:1]
        assert read_generation(connection, "g1", limit=100000) == projected


def test_full_chain_is_immutable_and_projection_is_detached(
    revision_chain: duckdb.DuckDBPyConnection,
) -> None:
    assert callable(getattr(market, "read_chain_rows", None)), "immutable history is unavailable"
    rows = market.read_chain_rows(
        revision_chain, "g3", budget=ComputeBudget(Fraction(1), 64 * 1024 * 1024)
    )
    assert isinstance(rows, tuple)
    with pytest.raises(TypeError):
        cast("dict[str, object]", rows[0])["close"] = Decimal(99)
    projected = market.project_heads(rows, cutoff_us=30)
    projected[0]["close"] = Decimal(99)
    assert {row["close"] for row in market.project_heads(rows, cutoff_us=30)} == {
        Decimal(11),
        Decimal(21),
    }


@pytest.mark.parametrize("field", ["available_at_us", "revision_known_at_us"])
def test_full_chain_unknown_revision_knowledge(
    field: str,
) -> None:
    with duckdb.connect() as connection:
        initialize_market(connection, "synthetic")
        row = parse_import(document()).rows[0]
        publish(connection, [row], version="1")
        publish(
            connection,
            [
                {
                    **row,
                    "revision_id": "r2",
                    "op": "SUPERSEDE",
                    "supersedes_revision_id": "r1",
                    field: None,
                }
            ],
            version="2",
            parent="g1",
        )
        assert callable(getattr(market, "read_chain_rows", None)), "full history is unavailable"
        rows = market.read_chain_rows(
            connection, "g2", budget=ComputeBudget(Fraction(1), 64 * 1024 * 1024)
        )
        projected = market.project_heads(rows, cutoff_us=100)
        expected = [] if field == "available_at_us" else ["r1"]
        assert [row["revision_id"] for row in projected] == expected
        assert [row["revision_id"] for row in market.project_heads(rows)] == ["r2"]


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE market_generations SET row_count=3 WHERE generation_id='g1'",
        "UPDATE market_generations SET row_count=4 WHERE generation_id='g3'",
        "UPDATE prices SET close=99 WHERE generation_id='g1'",
    ],
)
def test_full_chain_rejects_ancestor_or_head_tampering(
    revision_chain: duckdb.DuckDBPyConnection,
    sql: str,
) -> None:
    revision_chain.execute(sql)
    assert callable(getattr(market, "read_chain_rows", None)), "verified history is unavailable"
    with pytest.raises(ValueError, match="hash/count"):
        market.read_chain_rows(
            revision_chain, "g3", budget=ComputeBudget(Fraction(1), 64 * 1024 * 1024)
        )


@pytest.mark.parametrize("wide_text", [False, True])
def test_chain_materialization_rejected_before_fetch(
    monkeypatch: pytest.MonkeyPatch,
    *,
    wide_text: bool,
) -> None:
    with duckdb.connect() as connection:
        initialize_market(connection, "synthetic")
        row = parse_import(document()).rows[0]
        rows = (
            [{**row, "instrument_id": "A" * 100000}]
            if wide_text
            else [{**row, "instrument_id": f"ASSET_{i}"} for i in range(101)]
        )
        publish(connection, rows, version="1")
        assert callable(getattr(market, "read_chain_rows", None)), "memory admission is unavailable"

        def reject_fetch(*_args: object) -> list[dict[str, object]]:
            pytest.fail("oversized chain fetched before memory admission")

        monkeypatch.setattr(market, "_rows", reject_fetch)
        with pytest.raises(ComputeResourceError, match="memory"):
            market.read_chain_rows(
                connection, "g1", budget=ComputeBudget(Fraction(1), 2 * 1024 * 1024)
            )


def test_chain_materialization_budget_covers_all_generations() -> None:
    with duckdb.connect() as connection:
        initialize_market(connection, "synthetic")
        row = parse_import(document()).rows[0]
        publish(connection, [{**row, "instrument_id": f"A_{i}"} for i in range(16)], version="1")
        publish(
            connection,
            [{**row, "instrument_id": f"B_{i}"} for i in range(16)],
            version="2",
            parent="g1",
        )
        budget = ComputeBudget(Fraction(1), 2 * 1024 * 1024)
        expected_parent_count = 16
        assert len(market.read_chain_rows(connection, "g1", budget=budget)) == expected_parent_count
        with pytest.raises(ComputeResourceError, match="memory"):
            market.read_chain_rows(connection, "g2", budget=budget)


@pytest.mark.parametrize("cutoff", [-1, True, 1.5])
def test_project_heads_rejects_invalid_cutoffs(cutoff: int) -> None:
    with pytest.raises(ValueError, match="cutoff"):
        market.project_heads((), cutoff_us=cutoff)
    with pytest.raises(ValueError, match="cutoff"):
        market.project_heads((), ingestion_cutoff_us=cutoff)


@pytest.mark.parametrize("limit", [0, -1, True, 100001])
def test_legacy_reader_preserves_limit_admission(limit: int) -> None:
    with duckdb.connect() as connection, pytest.raises(ValueError, match="limit"):
        read_generation(connection, "absent", limit=limit)
