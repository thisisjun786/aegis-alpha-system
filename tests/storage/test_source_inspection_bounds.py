"""Synthetic inspection bounds are separate from pinned full-source verification."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from aegis_alpha.storage import source_reader
from aegis_alpha.storage.source_library import import_arrow, import_sqlite, list_tables
from aegis_alpha.storage.source_reader import SourcePin, iter_source_rows
from aegis_alpha.storage.workspace import initialize, open_workspace

_OVERSIZED_BYTES = 1_048_577


def import_large_cell(home: Path, kind: str) -> None:
    """Publish a modest fixture larger than the fixed 1 MiB inspection budget."""
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        if kind == "sqlite":
            source = home.parent / "snapshot.sqlite3"
            with sqlite3.connect(source) as connection:
                connection.execute("CREATE TABLE cells (value)")
                connection.execute("INSERT INTO cells VALUES (zeroblob(1048577))")
            source.chmod(0o600)
            with source.open("rb") as handle:
                digest = hashlib.file_digest(handle, "sha256").hexdigest()
            import_sqlite(workspace, source, "synthetic", digest)
        else:
            values = {
                "binary": pa.array([b"x" * 1_048_577]),
                "string": pa.array(["\x00" * 200_000]),
                "list": pa.array([[0] * 100_000]),
                "nested": pa.array([[["x" * 1_048_577]]]),
            }
            import_arrow(
                workspace,
                "synthetic",
                "0" * 64,
                "cells",
                pa.table({"value": values[kind]}).to_reader(),
            )


@pytest.mark.parametrize("kind", ["sqlite", "binary", "string", "list", "nested"])
def test_rejects_oversized_cells_when_row_limit_is_one(tmp_path: Path, kind: str) -> None:
    # Given an immutable source containing one oversized cell.
    home = tmp_path / "native"
    import_large_cell(home, kind)
    # When bounded inspection is requested, Then it rejects rather than truncates.
    with (
        open_workspace(home, require_strategies=True) as workspace,
        pytest.raises(ValueError, match="byte budget"),
    ):
        source_reader.inspect_source(workspace, "synthetic", "cells", limit=1)


def test_rejects_before_python_text_materialization_when_sqlite_cell_is_large(
    tmp_path: Path,
) -> None:
    # Given a valid imported text value and a conversion hook that forbids fetching it.
    home = tmp_path / "native"
    initialize(home)
    source = tmp_path / "text.sqlite3"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE cells (value TEXT)")
        connection.execute("INSERT INTO cells VALUES (?)", ("x" * 1_048_577,))
    source.chmod(0o600)
    with source.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_sqlite(workspace, source, "synthetic", digest)
    with open_workspace(home, require_strategies=True) as workspace:
        assert workspace.strategies is not None

        def bounded_text(raw: bytes) -> str:
            assert len(raw) < _OVERSIZED_BYTES, "oversized cell reached Python"
            return raw.decode()

        workspace.strategies.text_factory = bounded_text
        # When inspection runs, Then only the size query reaches the driver boundary.
        with pytest.raises(ValueError, match="byte budget"):
            source_reader.inspect_source(workspace, "synthetic", "cells", limit=1)


def test_preserves_streaming_when_inspection_exceeds_budget(tmp_path: Path) -> None:
    # Given a source exceeding the inspection budget, with an independent digest pin.
    home = tmp_path / "native"
    import_large_cell(home, "binary")
    with open_workspace(home, require_strategies=True) as workspace:
        table = list_tables(workspace, "synthetic")[0]
        pin = SourcePin("synthetic", "0" * 64, "cells", str(table["digest"]))
        # When the full verification/streaming API is used, Then the cell is intact.
        rows = [dict(row) for batch in iter_source_rows(workspace, pin) for row in batch]
        assert rows == [{"value": b"x" * 1_048_577}]


def test_counts_combined_rows_when_each_cell_fits(tmp_path: Path) -> None:
    # Given individually small cells whose combined escaped output exceeds the budget.
    home = tmp_path / "native"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_arrow(
            workspace,
            "synthetic",
            "0" * 64,
            "cells",
            pa.table({"value": ["\x00" * 100_000] * 2}).to_reader(),
        )
        # When both rows are inspected, Then the cumulative budget rejects them.
        with pytest.raises(ValueError, match="byte budget"):
            source_reader.inspect_source(workspace, "synthetic", "cells", limit=2)


def test_ignores_unselected_rows_when_first_row_fits(tmp_path: Path) -> None:
    # Given a tiny first row followed by a cell exceeding the inspection budget.
    home = tmp_path / "native"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_arrow(
            workspace,
            "synthetic",
            "0" * 64,
            "cells",
            pa.table({"value": ["small", "x" * 1_048_577]}).to_reader(),
        )
        # When only the first row is inspected, Then its complete JSON is returned.
        result = source_reader.inspect_source(workspace, "synthetic", "cells", limit=1)
        assert json.loads(json.dumps(result, allow_nan=False))["rows"] == [{"value": "small"}]


def test_rejects_before_arrow_export_when_cell_is_large(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given an oversized imported Arrow cell and a forbidden value-export hook.
    home = tmp_path / "native"
    import_large_cell(home, "nested")

    def forbidden_export(_connection: duckdb.DuckDBPyConnection) -> pa.Table:
        pytest.fail("oversized source was exported before byte admission")

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "to_arrow_table", forbidden_export)
    # When inspection evaluates its budget, Then no Arrow values are exported.
    with (
        open_workspace(home, require_strategies=True) as workspace,
        pytest.raises(ValueError, match="byte budget"),
    ):
        source_reader.inspect_source(workspace, "synthetic", "cells", limit=1)


def test_reads_sqlite_when_optional_arrow_dependency_is_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given an imported SQLite source and a runtime without optional Arrow.
    home = tmp_path / "native"
    initialize(home)
    source = tmp_path / "scalar.sqlite3"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE cells (value)")
        connection.execute("INSERT INTO cells VALUES (?)", (b"a",))
    source.chmod(0o600)
    with source.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_sqlite(workspace, source, "synthetic", digest)
        monkeypatch.setitem(sys.modules, "pyarrow", None)
        # When SQLite inspection executes, Then it preserves its base64 representation.
        result = source_reader.inspect_source(workspace, "synthetic", "cells", limit=1)
        assert result["rows"] == [{"value": {"base64": "YQ=="}}]
