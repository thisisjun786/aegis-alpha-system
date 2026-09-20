from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import cast
from unittest.mock import Mock

import duckdb
import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.storage import market, publication
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.market import (
    initialize_market,
    publish_generation,
    read_generation,
    verify_generation,
)
from aegis_alpha.storage.paths import read_json
from aegis_alpha.storage.workspace import initialize, open_workspace, write_json
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


@pytest.mark.parametrize("shape", ["many-rows", "wide-ascii", "wide-four-byte"])
def test_chain_materialization_rejected_before_fetch(
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
) -> None:
    with duckdb.connect() as connection:
        initialize_market(connection, "synthetic")
        row = parse_import(document()).rows[0]
        if shape == "many-rows":
            rows = [{**row, "instrument_id": f"ASSET_{i}"} for i in range(808)]
        elif shape == "wide-ascii":
            rows = [{**row, "instrument_id": "A" * 800000}]
        else:
            # Four-byte text that the decoded value's own charge would admit: three
            # hundred thousand characters hold about 1.2MB against a 4MB allowance,
            # while hashing them holds three UTF-8 copies of 1.2MB each besides.
            rows = [{**row, "instrument_id": "\U0001f642" * 300000}]
        publish(connection, rows, version="1")
        assert callable(getattr(market, "read_chain_rows", None)), "memory admission is unavailable"

        def reject_fetch(*_args: object) -> list[dict[str, object]]:
            pytest.fail("oversized chain fetched before memory admission")

        monkeypatch.setattr(market, "_rows", reject_fetch)
        with pytest.raises(ComputeResourceError, match="memory"):
            market.read_chain_rows(
                connection, "g1", budget=ComputeBudget(Fraction(1), 16 * 1024 * 1024)
            )
        assert connection.execute("SELECT count(*) FROM prices").fetchone() == (len(rows),)


def test_chain_materialization_budget_covers_all_generations() -> None:
    with duckdb.connect() as connection:
        initialize_market(connection, "synthetic")
        row = parse_import(document()).rows[0]
        # Scale rows with the budget so DuckDB itself has room for this in-memory store.
        # One generation has to fit and two have to not, and what a generation costs is
        # now its rows rather than a flat charge per character of their text.
        publish(connection, [{**row, "instrument_id": f"A_{i}"} for i in range(240)], version="1")
        publish(
            connection,
            [{**row, "instrument_id": f"B_{i}"} for i in range(240)],
            version="2",
            parent="g1",
        )
        budget = ComputeBudget(Fraction(1), 16 * 1024 * 1024)
        expected_parent_count = 240
        assert len(market.read_chain_rows(connection, "g1", budget=budget)) == expected_parent_count
        with pytest.raises(ComputeResourceError, match="memory"):
            market.read_chain_rows(connection, "g2", budget=budget)


def test_chain_encoding_workspace_is_the_widest_generation_not_their_sum() -> None:
    # Two generations of equally wide four-byte text. Both deltas' decoded rows are
    # returned together, so they accumulate, but the codec hashes one delta at a time
    # and keeps only the digest, so one encoding workspace is live at the peak. This
    # budget holds that peak; it would not hold one workspace per generation.
    with duckdb.connect() as connection:
        initialize_market(connection, "synthetic")
        row = parse_import(document()).rows[0]
        publish(connection, [{**row, "instrument_id": "\U0001f642" * 150000}], version="1")
        publish(
            connection,
            [{**row, "instrument_id": "\U0001f643" * 150000}],
            version="2",
            parent="g1",
        )
        budget = ComputeBudget(Fraction(1), 16 * 1024 * 1024)
        expected_chain_count = 2
        assert len(market.read_chain_rows(connection, "g2", budget=budget)) == expected_chain_count


class QueryObserver:
    """Observe the borrowed query connection; forward real SQL and results unchanged."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, *, fail_query: bool = False) -> None:
        self.connection: duckdb.DuckDBPyConnection = connection
        self.fail_query: bool = fail_query
        self.observations: list[tuple[str, tuple[int, str]]] = []
        self.close_calls: int = 0
        # A spec-typed forwarding adapter; execute/close always reach the real connection.
        self.borrowed: duckdb.DuckDBPyConnection = cast(
            "duckdb.DuckDBPyConnection", Mock(spec=duckdb.DuckDBPyConnection, wraps=self)
        )

    def execute(
        self, query: str, parameters: list[object] | None = None
    ) -> duckdb.DuckDBPyConnection:
        if ' FROM "' in query or " FROM market_generations" in query:
            settings = cast(
                "tuple[int, str]",
                self.connection.execute(
                    "SELECT current_setting('threads'), current_setting('memory_limit')"
                ).fetchone(),
            )
            self.observations.append((query, settings))
            if self.fail_query and query.startswith('SELECT "generation_id"'):
                return self.connection.execute("SELECT error('synthetic chain query failure')")
        return self.connection.execute(query, parameters)

    def close(self) -> None:
        self.close_calls += 1
        self.connection.close()


@pytest.mark.parametrize(
    ("threads", "memory", "expected"),
    [
        (4, "512MB", (2, "48.0 MiB")),
        (1, "32MB", (1, "30.5 MiB")),
        (1, "35MB", (1, "33.3 MiB")),
        (4, "32MB", (2, "30.5 MiB")),
        (1, "512MB", (1, "48.0 MiB")),
    ],
)
def test_chain_queries_apply_budget_in_workspace(
    tmp_path: Path, threads: int, memory: str, expected: tuple[int, str]
) -> None:
    # Given a published synthetic pin and runtime limits independent of the budget.
    home = tmp_path / "aas"
    _ = initialize(home)
    runtime = read_json(home / "runtime.json")
    runtime["resources"] = {"threads": threads, "memory_limit": memory}
    write_json(home / "runtime.json", runtime)
    raw = document()
    with open_workspace(home, writable=True) as workspace:
        _ = publication.publish_document(workspace, parse_import(raw))
    with open_workspace(home) as workspace:
        marker = market.marker_for(workspace.market, "synthetic-generation")
        observer = QueryObserver(workspace.market)
        # The observer is a forwarding test adapter, not a fake query implementation.
        rows = market.read_chain_rows(
            observer.borrowed,
            "synthetic-generation",
            budget=ComputeBudget(Fraction(3, 2), 64 * 1024 * 1024),
        )
        # Then every admission, verification and materialization query sees the limits.
        assert observer.observations
        assert {settings for _, settings in observer.observations} == {expected}
        assert any(query.startswith("SELECT count(*)") for query, _ in observer.observations)
        assert any(query.startswith('SELECT "generation_id"') for query, _ in observer.observations)
        assert [(row["instrument_id"], row["revision_id"], row["close"]) for row in rows] == [
            ("ASSET_A", "r1", Decimal(11))
        ]
        source_row = cast("dict[str, object]", json.loads(raw)["rows"][0])
        expected_hash = hashlib.sha256(
            json.dumps(source_row, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        assert rows[0]["source_row_hash"] == expected_hash
        assert rows[0]["source_snapshot_id"] == "source-" + hashlib.sha256(raw).hexdigest()
        assert market.marker_for(workspace.market, "synthetic-generation") == marker
        assert workspace.market.execute("SELECT 42").fetchone() == (42,)
        assert observer.close_calls == 0


def test_chain_query_error_preserves_borrowed_connection(
    revision_chain: duckdb.DuckDBPyConnection,
) -> None:
    # Given a real chain and an error injected at the first actual materialization query.
    _ = revision_chain.execute("SET threads=4; SET memory_limit='512MB'")
    observer = QueryObserver(revision_chain, fail_query=True)
    with pytest.raises(duckdb.InvalidInputException, match="synthetic chain query failure"):
        _ = market.read_chain_rows(
            observer.borrowed,
            "g3",
            budget=ComputeBudget(Fraction(1), 64 * 1024 * 1024),
        )
    # Then the failure propagates without closing the caller's connection or raising limits.
    assert {settings for _, settings in observer.observations} == {(1, "48.0 MiB")}
    assert observer.close_calls == 0
    assert revision_chain.execute("SELECT count(*) FROM prices").fetchone() == (4,)
    assert verify_generation(revision_chain, "g3")["row_count"] == 1


def test_duckdb_budget_admission_failure_preserves_borrowed_connection(
    revision_chain: duckdb.DuckDBPyConnection,
) -> None:
    # Given an in-memory chain that cannot fit the minimum budget's DuckDB share.
    _ = revision_chain.execute("SET threads=4; SET memory_limit='512MB'")
    observer = QueryObserver(revision_chain)
    with pytest.raises(ComputeResourceError) as error:
        _ = market.read_chain_rows(
            observer.borrowed, "g3", budget=ComputeBudget(Fraction(1), 2 * 1024 * 1024)
        )
    # Then memory failure remains a capacity error, with the original DuckDB cause.
    assert isinstance(error.value.__cause__, duckdb.OutOfMemoryException)
    assert observer.observations == []
    assert observer.close_calls == 0
    assert revision_chain.execute("SELECT count(*) FROM prices").fetchone() == (4,)


def test_legacy_unbudgeted_queries_keep_runtime_settings(
    revision_chain: duckdb.DuckDBPyConnection,
) -> None:
    # Given legacy reads without a ComputeBudget.
    _ = revision_chain.execute("SET threads=4; SET memory_limit='512MB'")
    observer = QueryObserver(revision_chain)
    rows = read_generation(observer.borrowed, "g3", cutoff_us=40)
    # Then real queries retain runtime settings and the independent revision projection.
    assert {settings for _, settings in observer.observations} == {(4, "488.2 MiB")}
    assert {row["instrument_id"]: row["close"] for row in rows} == {
        "ASSET_A": Decimal(12),
        "ASSET_B": Decimal(21),
    }
    assert observer.close_calls == 0
    assert revision_chain.execute("SELECT 42").fetchone() == (42,)


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
