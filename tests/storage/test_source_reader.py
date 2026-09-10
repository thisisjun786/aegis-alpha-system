"""Pinned source content is verified before any research row is consumed."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest

from aegis_alpha.storage import source_library
from aegis_alpha.storage.source_library import import_arrow, import_sqlite, list_tables
from aegis_alpha.storage.source_library_schema import quoted
from aegis_alpha.storage.source_reader import SourcePin, iter_source_rows
from aegis_alpha.storage.workspace import initialize, open_workspace


def test_arrow_pin_streams_nulls_and_projects_columns(tmp_path: Path) -> None:
    home = tmp_path / "native"
    initialize(home)
    digest = hashlib.sha256(b"synthetic-source").hexdigest()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_arrow(
            workspace,
            "prices",
            digest,
            "bars",
            pa.table({"symbol": ["ETF-A", "ETF-B"], "close": [12.5, None]}).to_reader(),
        )
        table = list_tables(workspace, "prices")[0]
        pin = SourcePin("prices", digest, "bars", str(table["digest"]))
        batches = list(iter_source_rows(workspace, pin, columns=["close"], batch_size=1))
        assert [dict(batch[0]) for batch in batches] == [{"close": 12.5}, {"close": None}]
        with pytest.raises(TypeError):
            batches[0][0]["close"] = 0  # ty: ignore[invalid-assignment] -- runtime immutability
        with pytest.raises(ValueError, match="SHA-256"):
            list(iter_source_rows(workspace, replace(pin, source_sha256="0" * 64)))
        with pytest.raises(ValueError, match="selected columns"):
            list(iter_source_rows(workspace, pin, columns=["close); DROP TABLE x;--"]))
        workspace.market.execute(
            "UPDATE " + quoted(str(table["target"])) + " SET close=13 WHERE _aas_ordinal=0"  # noqa: S608 -- synthetic manifest-owned table
        )
        with pytest.raises(ValueError, match="content or row count"):
            list(iter_source_rows(workspace, pin))


def test_sqlite_pin_does_not_require_migration_provenance_columns(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE recipe (payload BLOB, value)")
        connection.execute("INSERT INTO recipe VALUES (?,?)", (b"\x00\xff", "001"))
    source.chmod(0o600)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    home = tmp_path / "native"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_sqlite(workspace, source, "recipes", digest)
        table = list_tables(workspace, "recipes")[0]
        pin = SourcePin("recipes", digest, "recipe", str(table["digest"]))
        rows = [dict(row) for batch in iter_source_rows(workspace, pin) for row in batch]
        assert rows == [{"payload": b"\x00\xff", "value": "001"}]


def test_committed_target_with_unfinished_intent_is_not_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "native"
    initialize(home)

    def crash(*_args: object) -> None:
        raise RuntimeError("simulated crash after target commit")

    monkeypatch.setattr(source_library, "complete_operation", crash)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        with pytest.raises(RuntimeError, match="simulated crash"):
            import_arrow(
                workspace, "unfinished", "0" * 64, "bars", pa.table({"x": [1]}).to_reader()
            )
        row = workspace.market.execute(
            "SELECT manifest_json FROM source_library_commits WHERE source_id='unfinished'"
        ).fetchone()
        assert row is not None
        manifest = json.loads(row[0])
        pin = SourcePin("unfinished", "0" * 64, "bars", manifest["tables"][0]["digest"])
        with pytest.raises(ValueError, match="missing"):
            list(iter_source_rows(workspace, pin))


@pytest.mark.parametrize("size", [0, -1, True, 1_000_001])
def test_invalid_batch_size_rejected_before_source_lookup(tmp_path: Path, size: int) -> None:
    home = tmp_path / "native"
    initialize(home)
    with open_workspace(home) as workspace, pytest.raises(ValueError, match="batch_size"):
        list(
            iter_source_rows(
                workspace, SourcePin("missing", "0" * 64, "x", "0" * 64), batch_size=size
            )
        )


def test_interleaved_duckdb_readers_keep_independent_results(tmp_path: Path) -> None:
    home = tmp_path / "native"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_arrow(
            workspace, "prices", "0" * 64, "bars", pa.table({"value": [10, 20]}).to_reader()
        )
        table = list_tables(workspace, "prices")[0]
        pin = SourcePin("prices", "0" * 64, "bars", str(table["digest"]))
        first = iter_source_rows(workspace, pin, batch_size=1)
        second = iter_source_rows(workspace, pin, batch_size=1)
        assert [dict(next(first)[0]), dict(next(second)[0])] == [{"value": 10}, {"value": 10}]
        assert [dict(next(first)[0]), dict(next(second)[0])] == [{"value": 20}, {"value": 20}]
        first.close()
        second.close()
