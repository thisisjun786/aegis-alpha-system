from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from aegis_alpha.storage import source_library
from aegis_alpha.storage.backup import backup
from aegis_alpha.storage.publication import recover_operations
from aegis_alpha.storage.workspace import initialize, open_workspace


def test_target_commit_recovers_after_state_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "aas"
    initialize(home)
    path = tmp_path / "snapshot"
    with sqlite3.connect(path) as origin:
        origin.execute("CREATE TABLE source(value)")
        origin.execute("INSERT INTO source VALUES ('preserved')")
    path.chmod(0o600)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    complete = source_library.complete_operation

    def crash(*_args: object) -> None:
        raise RuntimeError("synthetic state completion failure")

    monkeypatch.setattr(source_library, "complete_operation", crash)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        with pytest.raises(RuntimeError, match="completion failure"):
            source_library.import_sqlite(workspace, path, "source", digest)
        assert source_library.list_sources(workspace) == []
    with pytest.raises(ValueError, match="recovered operations"):
        backup(home)
    monkeypatch.setattr(source_library, "complete_operation", complete)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        recovered = recover_operations(workspace)
        assert len(cast("list[str]", recovered["recovered"])) == 1
        result = source_library.import_sqlite(workspace, path, "source", digest)
        assert result["reused"] is True
        assert source_library.read_table(workspace, "source", "source")["rows"] == [
            {"value": "preserved"}
        ]
        assert source_library.verify_sources(workspace) == {"sources": 1, "tables": 1, "rows": 1}
        assert (
            workspace.state.execute(
                "SELECT count(*) FROM storage_operations WHERE phase='PREPARED'"
            ).fetchone()[0]
            == 0
        )


def test_arrow_replay_different_content_rejected(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_arrow(
            workspace, "arrow", "a" * 64, "values", pa.table({"x": [1, 2]}).to_reader()
        )
        with pytest.raises(ValueError, match="different content"):
            source_library.import_arrow(
                workspace, "arrow", "a" * 64, "values", pa.table({"x": [1, 3]}).to_reader()
            )
        assert source_library.read_table(workspace, "arrow", "values")["rows"] == [
            {"x": 1},
            {"x": 2},
        ]


def test_large_arrow_source_is_not_limited_by_arbitrary_row_count(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    data = pa.table({"value": pa.nulls(2_000_001, type=pa.int32())})
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        result = source_library.import_arrow(
            workspace, "large", "b" * 64, "values", data.to_reader()
        )
        assert result["reused"] is False
        assert source_library.verify_sources(workspace) == {
            "sources": 1,
            "tables": 1,
            "rows": 2_000_001,
        }
        assert recover_operations(workspace)["pending"] == []


def test_unknown_extension_checksum_rejected(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source_library.import_arrow(
            workspace, "small", "a" * 64, "values", pa.table({"x": [1]}).to_reader()
        )
        workspace.market.execute("UPDATE source_library_schema SET checksum='invalid'")
        with pytest.raises(ValueError, match="schema/checksum"):
            source_library.list_sources(workspace)
