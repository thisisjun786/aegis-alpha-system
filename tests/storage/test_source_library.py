# ruff: noqa: PLR2004
"""Synthetic public-API coverage for the private source library."""

from __future__ import annotations

import base64
import hashlib
import sqlite3
import sys
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.storage import source_library
from aegis_alpha.storage import source_library_digest as source_digest
from aegis_alpha.storage.backup import backup, restore
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace

if TYPE_CHECKING:
    import duckdb

_PRIVATE_FILE = 0o600
_SQLITE_SOURCE = "sqlite-source"
_ARROW_SOURCE = "arrow-source"
_MIXED_TABLE = "mixed"
_AUDIT_TABLE = "audit"
_ARROW_TABLE = "observations"
_BLOB = b"\x00\xff"
_BLOB_JSON = {"base64": base64.b64encode(_BLOB).decode("ascii")}
_ARROW_LIMIT = 1


@pytest.fixture
def home(tmp_path: Path) -> Path:
    root = tmp_path / "aas"
    initialize(root)
    return root


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_sqlite(path: Path) -> str:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute(
            "CREATE TABLE mixed(label TEXT, amount REAL, count INTEGER, payload BLOB)"
        )
        connection.execute("CREATE TABLE audit(note TEXT)")
        connection.execute(
            "CREATE TRIGGER mixed_audit AFTER INSERT ON mixed BEGIN "
            "INSERT INTO audit VALUES ('fired'); END"
        )
        connection.execute("CREATE VIEW mixed_view AS SELECT label FROM mixed")
        connection.execute("INSERT INTO mixed VALUES (?,?,?,?)", ("a'b", 1.25, 7, _BLOB))
        connection.execute("INSERT INTO mixed VALUES (NULL, NULL, NULL, NULL)")
        connection.commit()
    finally:
        connection.close()
    path.chmod(_PRIVATE_FILE)
    return _digest(path)


def _arrow_table() -> pa.Table:
    stamp = pa.array([1_609_459_200_000_000_123, None], type=pa.timestamp("ns"))
    return pa.table(
        {
            "n": pa.array([42, None], type=pa.int32()),
            "x": pa.array([1.25, None], type=pa.float32()),
            "ts": stamp,
            "name": pa.array(["alpha", None], type=pa.large_string()),
            "tags": pa.array([["k", "v"], None], type=pa.list_(pa.string())),
            "price": pa.array([Decimal("1.25"), None], type=pa.decimal128(10, 2)),
        }
    )


def _provenance(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _reader(table: pa.Table) -> pa.RecordBatchReader:
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())


def _source(sources: list[dict[str, object]], source_id: str) -> dict[str, object]:
    match = [row for row in sources if row["source_id"] == source_id]
    assert match, sources
    return match[0]


def _table(tables: list[dict[str, object]], name: str) -> dict[str, object]:
    match = [row for row in tables if row["name"] == name]
    assert match, tables
    return match[0]


def _schema_row(connection: sqlite3.Connection | duckdb.DuckDBPyConnection) -> tuple[int, str]:
    row = connection.execute("SELECT version, checksum FROM source_library_schema").fetchone()
    assert row is not None
    return int(row[0]), str(row[1])


def _json_rows(payload: dict[str, object]) -> list[dict[str, object]]:
    rows = payload["rows"]
    assert isinstance(rows, list)
    return cast("list[dict[str, object]]", rows)


def _fixture_counts(path: Path) -> tuple[int, int]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        mixed = int(connection.execute("SELECT COUNT(*) FROM mixed").fetchone()[0])
        audit = int(connection.execute("SELECT COUNT(*) FROM audit").fetchone()[0])
    finally:
        connection.close()
    return mixed, audit


def test_empty_library_reports_absent_verification(home: Path) -> None:
    with open_workspace(home) as workspace:
        assert source_library.list_sources(workspace) == []
        assert source_library.verify_sources(workspace) is None
        report = verify_workspace(workspace)
        assert "source_library" not in report
        assert report["strategy_versions"] == 0
        assert workspace.doctor()["strategy_versions"] == 0


def test_sqlite_preserves_mixed_values_and_regular_tables_only(tmp_path: Path, home: Path) -> None:
    source = tmp_path / "private-snapshot.sqlite3"
    digest = _write_sqlite(source)
    original = source.read_bytes()
    mixed_count, audit_count = _fixture_counts(source)
    assert (mixed_count, audit_count) == (2, 2)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        imported = source_library.import_sqlite(workspace, source, _SQLITE_SOURCE, digest)
        sources = source_library.list_sources(workspace)
        listed = source_library.list_tables(workspace, _SQLITE_SOURCE)
        payload = source_library.read_table(workspace, _SQLITE_SOURCE, _MIXED_TABLE)
        strategies = workspace.strategies
        assert isinstance(strategies, sqlite3.Connection)
        schema = _schema_row(strategies)
        report = source_library.verify_sources(workspace)
        workspace_report = verify_workspace(workspace)
        assert workspace.doctor()["strategy_versions"] == 0
    assert source.read_bytes() == original
    assert not Path(str(source) + "-wal").exists()
    assert not Path(str(source) + "-journal").exists()
    assert imported["source_id"] == _SQLITE_SOURCE
    assert imported["reused"] is False
    entry = _source(sources, _SQLITE_SOURCE)
    assert entry == {
        "source_id": _SQLITE_SOURCE,
        "store": "strategies",
        "sha256": digest,
        "source_only": True,
    }
    names = {row["name"] for row in listed}
    assert names == {_AUDIT_TABLE, _MIXED_TABLE}
    assert "mixed_view" not in names
    assert "sqlite_master" not in names
    mixed = _table(listed, _MIXED_TABLE)
    audit = _table(listed, _AUDIT_TABLE)
    assert mixed["rows"] == 2
    assert audit["rows"] == 2
    assert mixed["columns"] == ["label", "amount", "count", "payload"]
    assert mixed["format"] == "sqlite"
    assert mixed["target"] != _MIXED_TABLE
    assert payload["source_id"] == _SQLITE_SOURCE
    assert payload["table"] == _MIXED_TABLE
    assert payload["columns"] == ["label", "amount", "count", "payload"]
    assert payload["source_only"] is True
    assert payload["rows"] == [
        {"label": "a'b", "amount": 1.25, "count": 7, "payload": _BLOB_JSON},
        {"label": None, "amount": None, "count": None, "payload": None},
    ]
    assert schema[0] == 1
    assert len(schema[1]) == 64
    assert report == {"sources": 1, "tables": 2, "rows": 4}
    assert workspace_report["source_library"] == report
    assert workspace_report["strategy_versions"] == 0
    assert (home / "strategies.sqlite3").stat().st_mode & 0o777 == _PRIVATE_FILE


def test_sqlite_replay_same_hash_is_idempotent(tmp_path: Path, home: Path) -> None:
    source = tmp_path / "private-snapshot.sqlite3"
    digest = _write_sqlite(source)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        first = source_library.import_sqlite(workspace, source, _SQLITE_SOURCE, digest)
        second = source_library.import_sqlite(workspace, source, _SQLITE_SOURCE, digest)
        sources = source_library.list_sources(workspace)
        tables = source_library.list_tables(workspace, _SQLITE_SOURCE)
        rows = _json_rows(source_library.read_table(workspace, _SQLITE_SOURCE, _MIXED_TABLE))
        report = source_library.verify_sources(workspace)
        assert workspace.doctor()["strategy_versions"] == 0
    assert first["reused"] is False
    assert second["reused"] is True
    assert [row["source_id"] for row in sources] == [_SQLITE_SOURCE]
    assert _table(tables, _MIXED_TABLE)["rows"] == 2
    assert len(rows) == 2
    assert report == {"sources": 1, "tables": 2, "rows": 4}


def test_sqlite_same_id_different_hash_is_rejected(tmp_path: Path, home: Path) -> None:
    first = tmp_path / "first.sqlite3"
    second = tmp_path / "second.sqlite3"
    first_digest = _write_sqlite(first)
    connection = sqlite3.connect(second)
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("CREATE TABLE mixed(label TEXT)")
        connection.execute("INSERT INTO mixed VALUES ('other')")
        connection.commit()
    finally:
        connection.close()
    second.chmod(_PRIVATE_FILE)
    second_digest = _digest(second)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_sqlite(workspace, first, _SQLITE_SOURCE, first_digest)
        with pytest.raises(ValueError, match="different content"):
            source_library.import_sqlite(workspace, second, _SQLITE_SOURCE, second_digest)
        sources = source_library.list_sources(workspace)
        payload = source_library.read_table(workspace, _SQLITE_SOURCE, _MIXED_TABLE)
        assert workspace.doctor()["strategy_versions"] == 0
    assert _source(sources, _SQLITE_SOURCE)["sha256"] == first_digest
    assert _json_rows(payload)[0]["label"] == "a'b"


def test_unknown_source_and_negative_limit_are_rejected(tmp_path: Path, home: Path) -> None:
    source = tmp_path / "private-snapshot.sqlite3"
    digest = _write_sqlite(source)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_sqlite(workspace, source, _SQLITE_SOURCE, digest)
        with pytest.raises(ValueError, match="unknown or incomplete source"):
            source_library.list_tables(workspace, "missing-source")
        with pytest.raises(ValueError, match="unknown or incomplete source"):
            source_library.read_table(workspace, "missing-source", _MIXED_TABLE)
        with pytest.raises(ValueError, match="unknown source table"):
            source_library.read_table(workspace, _SQLITE_SOURCE, "missing-table")
        with pytest.raises(ValueError, match="source read limit"):
            source_library.read_table(workspace, _SQLITE_SOURCE, _MIXED_TABLE, limit=-1)
        with pytest.raises(ValueError, match="source read limit"):
            source_library.read_table(workspace, _SQLITE_SOURCE, _MIXED_TABLE, limit=0)


@pytest.mark.parametrize("column", ["_AAS_ORDINAL", "_AaS_Ordinal", "_aas_ordinal"])
def test_arrow_rejects_reserved_column_before_intent_when_case_varies(
    home: Path, column: str
) -> None:
    # Given a source column whose values differ from the ingestion ordinals.
    table = pa.table({column: [2, 1]})
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        before = workspace.market.execute(
            "SELECT table_name FROM duckdb_tables() WHERE NOT temporary ORDER BY table_name"
        ).fetchall()
        # When the real importer receives a case variant of its reserved column.
        with pytest.raises(ValueError, match="ambiguous or reserved Arrow column"):
            source_library.import_arrow(
                workspace, _ARROW_SOURCE, _provenance(_ARROW_SOURCE), _ARROW_TABLE, _reader(table)
            )
        # Then validation leaves no intent, source catalog, or target artifact.
        assert workspace.state.execute("SELECT * FROM storage_operations").fetchall() == []
        assert source_library.list_sources(workspace) == []
        assert source_library.verify_sources(workspace) is None
        assert (
            workspace.market.execute(
                "SELECT table_name FROM duckdb_tables() WHERE NOT temporary ORDER BY table_name"
            ).fetchall()
            == before
        )


def test_arrow_native_roundtrip(home: Path) -> None:
    table = _arrow_table()
    digest = _provenance(_ARROW_SOURCE)
    expected_ts = table.column("ts").cast(pa.string()).to_pylist()
    expected_price = table.column("price").cast(pa.string()).to_pylist()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        imported = source_library.import_arrow(
            workspace, _ARROW_SOURCE, digest, _ARROW_TABLE, _reader(table)
        )
        sources = source_library.list_sources(workspace)
        listed = source_library.list_tables(workspace, _ARROW_SOURCE)
        payload = source_library.read_table(workspace, _ARROW_SOURCE, _ARROW_TABLE)
        limited = source_library.read_table(
            workspace, _ARROW_SOURCE, _ARROW_TABLE, limit=_ARROW_LIMIT
        )
        schema = _schema_row(workspace.market)
        report = source_library.verify_sources(workspace)
        workspace_report = verify_workspace(workspace)
        assert workspace.doctor()["strategy_versions"] == 0
    assert imported["source_id"] == _ARROW_SOURCE
    assert imported["reused"] is False
    entry = _source(sources, _ARROW_SOURCE)
    assert entry == {
        "source_id": _ARROW_SOURCE,
        "store": "market",
        "sha256": digest,
        "source_only": True,
    }
    listed_table = _table(listed, _ARROW_TABLE)
    assert listed_table["rows"] == 2
    assert listed_table["columns"] == ["n", "x", "ts", "name", "tags", "price"]
    assert listed_table["format"] == "arrow"
    assert payload["source_id"] == _ARROW_SOURCE
    assert payload["table"] == _ARROW_TABLE
    assert payload["columns"] == ["n", "x", "ts", "name", "tags", "price"]
    assert payload["source_only"] is True
    first, second = _json_rows(payload)
    tags = first["tags"]
    assert first["n"] == 42
    assert first["x"] == pytest.approx(1.25)
    assert first["ts"] == expected_ts[0]
    assert first["name"] == "alpha"
    assert isinstance(tags, list)
    assert tags == ["k", "v"]
    assert first["price"] == expected_price[0]
    assert Decimal(str(first["price"])) == Decimal("1.25")
    assert second == {
        "n": None,
        "x": None,
        "ts": None,
        "name": None,
        "tags": None,
        "price": None,
    }
    assert limited["rows"] == [first]
    assert schema[0] == 1
    assert len(schema[1]) == 64
    assert report == {"sources": 1, "tables": 1, "rows": 2}
    assert workspace_report["source_library"] == report
    assert workspace_report["strategy_versions"] == 0


@pytest.mark.parametrize("partition", [1, 2, 100])
def test_arrow_canonical_hashes_remain_stable(partition: int) -> None:
    cases = [
        (
            pa.table({"n": [1, None, 3], "raw": pa.array([b"a", None, b"bc"], type=pa.binary())}),
            (3, "704eae4e37dc4a92cd05fb37d06b24aceabe8a2b2b59a9bf5237e40b304ae9c4"),
        ),
        (
            pa.table(
                {
                    "x": pa.array([1.0, float("nan"), None, -0.0], type=pa.float64()),
                    "s": ["alpha", "beta", None, "delta"],
                }
            ),
            (4, "4cc0ecd88a45e89bb4290b332cc177c3a06e2d26d72378706edf5add9eacbc1b"),
        ),
    ]
    for table, expected in cases:
        reader = pa.RecordBatchReader.from_batches(
            table.schema, table.to_batches(max_chunksize=partition)
        )
        assert source_digest.arrow_digest(reader) == expected


def test_canonical_batch_rejects_multiple_chunks() -> None:
    batch = pa.record_batch({"value": [b"a"]})
    with pytest.raises(ValueError, match="one canonical Arrow batch"):
        source_digest._single_batch(pa.Table.from_batches([batch, batch]))  # noqa: SLF001


def test_arrow_batch_failure_rolls_back_and_allows_retry(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = pa.table({"value": pa.nulls(source_digest.BATCH_ROWS + 1, type=pa.int32())})
    single = source_digest._single_batch  # noqa: SLF001
    calls = 0

    def split_second(combined: pa.Table) -> pa.RecordBatch:
        nonlocal calls
        calls += 1
        if calls == 2:
            small = pa.record_batch({"value": [1]})
            return single(pa.Table.from_batches([small, small]))
        return single(combined)

    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        before = workspace.market.execute(
            "SELECT table_name FROM duckdb_tables() WHERE NOT temporary"
        ).fetchall()
        with monkeypatch.context() as patch:
            patch.setattr(source_digest, "_single_batch", split_second)
            with pytest.raises(ValueError, match="one canonical Arrow batch"):
                source_library.import_arrow(
                    workspace, _ARROW_SOURCE, "a" * 64, _ARROW_TABLE, _reader(table)
                )
        assert calls == 2
        assert source_library.list_sources(workspace) == []
        after = workspace.market.execute(
            "SELECT table_name FROM duckdb_tables() WHERE NOT temporary"
        ).fetchall()
        assert set(after) - set(before) == {("source_library_schema",), ("source_library_commits",)}
        assert [
            tuple(row)
            for row in workspace.state.execute(
                "SELECT phase FROM storage_operations WHERE kind='source_import'"
            )
        ] == [("PREPARED",)]
        source_library.import_arrow(
            workspace, _ARROW_SOURCE, "a" * 64, _ARROW_TABLE, _reader(table)
        )
        assert source_library.verify_sources(workspace) == {
            "sources": 1,
            "tables": 1,
            "rows": table.num_rows,
        }
    with open_workspace(home) as workspace:
        assert len(source_library.list_sources(workspace)) == 1
        report = source_library.verify_sources(workspace)
        assert report is not None
        assert report["rows"] == table.num_rows


def test_arrow_target_corruption_fails_verify(home: Path) -> None:
    table = _arrow_table()
    digest = _provenance(_ARROW_SOURCE)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_arrow(workspace, _ARROW_SOURCE, digest, _ARROW_TABLE, _reader(table))
        listed = source_library.list_tables(workspace, _ARROW_SOURCE)
        target = str(_table(listed, _ARROW_TABLE)["target"])
        assert target.startswith("sl_")
        quoted = '"' + target.replace('"', '""') + '"'
        workspace.market.execute("UPDATE " + quoted + " SET n = n + 1")  # noqa: S608
        workspace.market.execute("CHECKPOINT")
        with pytest.raises(ValueError, match="content/count mismatch"):
            source_library.verify_sources(workspace)
        with pytest.raises(ValueError, match="content/count mismatch"):
            verify_workspace(workspace)


def test_source_library_survives_backup_restore(tmp_path: Path, home: Path) -> None:
    sqlite_source = tmp_path / "private-snapshot.sqlite3"
    sqlite_digest = _write_sqlite(sqlite_source)
    table = _arrow_table()
    arrow_digest = _provenance(_ARROW_SOURCE)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_sqlite(workspace, sqlite_source, _SQLITE_SOURCE, sqlite_digest)
        source_library.import_arrow(
            workspace, _ARROW_SOURCE, arrow_digest, _ARROW_TABLE, _reader(table)
        )
        before_sources = source_library.list_sources(workspace)
        before_sqlite = source_library.read_table(workspace, _SQLITE_SOURCE, _MIXED_TABLE)
        before_arrow = source_library.read_table(workspace, _ARROW_SOURCE, _ARROW_TABLE)
        before_verify = source_library.verify_sources(workspace)
        before_workspace = verify_workspace(workspace)
        assert workspace.doctor()["strategy_versions"] == 0
    root = Path(str(backup(home)["backup_root"]))
    restored_home = tmp_path / "restored"
    restored = restore(root, restored_home)
    with open_workspace(restored_home) as workspace:
        assert source_library.list_sources(workspace) == before_sources
        assert source_library.read_table(workspace, _SQLITE_SOURCE, _MIXED_TABLE) == before_sqlite
        assert source_library.read_table(workspace, _ARROW_SOURCE, _ARROW_TABLE) == before_arrow
        after_verify = source_library.verify_sources(workspace)
        after_workspace = verify_workspace(workspace)
        assert after_verify == before_verify
        assert after_workspace["source_library"] == before_workspace["source_library"]
        assert after_workspace["strategy_versions"] == 0
        verification = restored["verification"]
        assert isinstance(verification, dict)
        assert verification["source_library"] == before_verify
        assert workspace.doctor()["strategy_versions"] == 0


def _blob_source(path: Path, payloads: list[bytes]) -> str:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("CREATE TABLE payloads(payload BLOB)")
        connection.executemany(
            "INSERT INTO payloads VALUES (?)", [(payload,) for payload in payloads]
        )
        connection.commit()
    finally:
        connection.close()
    path.chmod(_PRIVATE_FILE)
    return _digest(path)


def _budget(total: int) -> ComputeBudget:
    return ComputeBudget(Fraction(1), total)


def test_source_verification_admits_the_largest_batch_not_the_first(
    tmp_path: Path, home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A large cell placed after the first batch must still be charged."""
    source = tmp_path / "late-blob.sqlite3"
    digest = _blob_source(source, [b"\x00", b"\x00", b"z" * (1024 * 1024)])
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_sqlite(workspace, source, "late-blob", digest)
    # Two tiny rows land in the first batch and the megabyte payload in the second,
    # so an implementation that measured only the first batch would admit this.
    monkeypatch.setattr(source_library, "BATCH_ROWS", 2)
    with open_workspace(home) as workspace:
        with pytest.raises(ComputeResourceError, match="source table batch"):
            source_library.verify_sources(workspace, budget=_budget(16 * 1024 * 1024))
        report = source_library.verify_sources(workspace, budget=_budget(256 * 1024 * 1024))
    assert report == {"sources": 1, "tables": 1, "rows": 3}


def test_source_verification_rejects_before_any_digest_read(
    tmp_path: Path, home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An over-budget table is refused before the digest encodes a single row."""
    source = tmp_path / "wide-blob.sqlite3"
    digest = _blob_source(source, [b"z" * (1024 * 1024)])
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_sqlite(workspace, source, "wide-blob", digest)

    def unexpected(_rows: object) -> tuple[int, str]:
        raise AssertionError("digest ran before admission rejected the table")

    with open_workspace(home) as workspace:
        monkeypatch.setattr(source_library, "sqlite_digest", unexpected)
        with pytest.raises(ComputeResourceError, match="source table batch"):
            source_library.verify_sources(workspace, budget=_budget(16 * 1024 * 1024))


def test_admission_groups_by_the_generated_bucket_not_a_source_column(
    tmp_path: Path, home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A source column named like the bucket must not regroup the charge."""
    source = tmp_path / "bucket-name.sqlite3"
    connection = sqlite3.connect(source)
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("CREATE TABLE rows(_aas_batch INTEGER, payload BLOB)")
        connection.executemany(
            "INSERT INTO rows VALUES (?,?)", [(index, b"p" * 1000) for index in range(4)]
        )
        connection.commit()
    finally:
        connection.close()
    source.chmod(_PRIVATE_FILE)
    digest = _digest(source)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_sqlite(workspace, source, "bucket-name", digest)
    monkeypatch.setattr(source_library, "BATCH_ROWS", 4)
    # One batch of four rows, each a thousand-byte payload plus a one-byte bucket
    # value, over two columns. Grouping by the source column would instead see four
    # single-row batches and charge a quarter of this.
    exact = 32 * 4 * 1001 + 64 * 4 * 2
    with open_workspace(home) as workspace:
        private = workspace.strategies
        assert isinstance(private, sqlite3.Connection)
        table = source_library.list_tables(workspace, "bucket-name")[0]
        columns = cast("list[str]", table["columns"])
        source_library._admit_source_batch(  # noqa: SLF001
            private, table, "strategies", columns, exact
        )
        with pytest.raises(ComputeResourceError, match="source table batch"):
            source_library._admit_source_batch(  # noqa: SLF001
                private, table, "strategies", columns, exact - 1
            )


def test_admission_uses_each_batch_combined_charge(
    tmp_path: Path, home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The widest batch and the fullest batch need not be the same one."""
    source = tmp_path / "combined.sqlite3"
    payloads = [b"x"] * 100 + [b"y" * 100_000]
    digest = _blob_source(source, payloads)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_sqlite(workspace, source, "combined", digest)
    monkeypatch.setattr(source_library, "BATCH_ROWS", 100)
    # Batch one holds a hundred tiny rows and batch two the single large row.
    # Taking the row count and the byte total from different batches would invent
    # a heavier batch than either of the two that exist.
    exact = 32 * 100_000 + 64 * 1 * 1
    inflated = 32 * 100_000 + 64 * 100 * 1
    assert exact < inflated
    with open_workspace(home) as workspace:
        private = workspace.strategies
        assert isinstance(private, sqlite3.Connection)
        table = source_library.list_tables(workspace, "combined")[0]
        columns = cast("list[str]", table["columns"])
        source_library._admit_source_batch(  # noqa: SLF001
            private, table, "strategies", columns, exact
        )
        with pytest.raises(ComputeResourceError, match="source table batch"):
            source_library._admit_source_batch(  # noqa: SLF001
                private, table, "strategies", columns, exact - 1
            )


def test_sqlite_source_verifies_without_pyarrow(
    tmp_path: Path, home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A default installation ships no pyarrow, so a SQLite source must not need it."""
    source = tmp_path / "private-snapshot.sqlite3"
    digest = _write_sqlite(source)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_sqlite(workspace, source, _SQLITE_SOURCE, digest)
    with open_workspace(home) as workspace:
        monkeypatch.setitem(sys.modules, "pyarrow", None)
        report = source_library.verify_sources(workspace)
    assert report == {"sources": 1, "tables": 2, "rows": 4}
