# ruff: noqa: PLR2004 -- explicit expected protocol values.
from __future__ import annotations

import hashlib
import sqlite3
import sys
from pathlib import Path
from typing import TYPE_CHECKING, cast

from aegis_alpha.console.catalog import Catalog
from aegis_alpha.console.registry import Registry
from aegis_alpha.storage.locks import file_lock
from aegis_alpha.storage.source_library import import_sqlite
from aegis_alpha.storage.workspace import initialize, open_workspace

if TYPE_CHECKING:
    import pytest


def seed(home: Path, source: Path) -> None:
    initialize(home)
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE observations(label TEXT, value INTEGER)")
        connection.execute("INSERT INTO observations VALUES (?,?)", ("synthetic", 17))
    source.chmod(0o600)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_sqlite(workspace, source, "synthetic-source", digest)


def test_catalog_and_sqlite_preview_without_arrow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "aas"
    seed(home, tmp_path / "source.sqlite3")
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    catalog = Catalog(home)
    overview = catalog.overview()
    assert overview["catalog_status"] == "ready"
    assert len(cast("list[object]", overview["sources"])) == 1
    assert catalog.sample("synthetic-source", "observations")["rows"] == [
        {"label": "synthetic", "value": 17}
    ]
    assert "secrets" not in [r["kind"] for r in cast("list[dict[str, object]]", overview["stores"])]
    assert overview["datasets"] == []
    assert overview["runs"] == []


def test_busy_catalog_does_not_block_registry(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    catalog = Catalog(home)
    registry = Registry(tmp_path / "registry")
    with file_lock(home / ".storage.lock"):
        overview = catalog.overview()
        assert overview["catalog_status"] == "busy"
        assert len(cast("list[object]", overview["stores"])) == 6
        item = registry.create(
            {"name": "저장소", "path": str(home), "kind": "directory", "note": ""}
        )
        assert (
            registry.update(str(item["id"]), {"name": "저장소", "note": "수집 중", "revision": 1})[
                "note"
            ]
            == "수집 중"
        )
    assert catalog.overview()["catalog_status"] == "ready"
    with catalog.lock:
        assert catalog.overview()["catalog_status"] == "busy"


def test_missing_home_returns_explicit_state(tmp_path: Path) -> None:
    result = Catalog(tmp_path / "absent").overview()
    assert result["catalog_status"] in {"missing", "invalid"}
    assert result["sources"] == []


def test_native_dataset_and_run_metadata(tmp_path: Path) -> None:
    from aegis_alpha.storage.import_document import parse_import  # noqa: PLC0415
    from aegis_alpha.storage.publication import publish_document  # noqa: PLC0415
    from tests.storage.test_publication import document  # noqa: PLC0415

    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as workspace:
        publish_document(workspace, parse_import(document()))
        workspace.state.execute(
            "INSERT INTO input_bundles VALUES (?,?,?)", ("synthetic-bundle", "a" * 64, "synthetic")
        )
        workspace.state.execute(
            "INSERT INTO runs(run_id,bundle_id,engine_hash,environment_hash,"
            "reason,status,created_at_us) VALUES (?,?,?,?,?,?,?)",
            ("synthetic-run", "synthetic-bundle", "b" * 64, "c" * 64, "synthetic", "RUNNING", 1),
        )
        workspace.state.commit()
    overview = Catalog(home).overview()
    datasets = cast("list[dict[str, object]]", overview["datasets"])
    runs = cast("list[dict[str, object]]", overview["runs"])
    assert datasets[0]["dataset_id"] == "synthetic-prices"
    assert datasets[0]["row_count"] == 1
    assert runs[0]["run_id"] == "synthetic-run"
    assert runs[0]["status"] == "RUNNING"


def test_preview_omits_blob_and_truncates_text_in_database(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    source = tmp_path / "large.sqlite3"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE payloads(binary BLOB, prose TEXT)")
        connection.execute(
            "INSERT INTO payloads VALUES (?,?)", (b"x" * (2 * 1024 * 1024), "a" * 10000)
        )
    source.chmod(0o600)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_sqlite(
            workspace, source, "large-source", hashlib.sha256(source.read_bytes()).hexdigest()
        )
    result = Catalog(home).sample("large-source", "payloads")
    rows = cast("list[dict[str, object]]", result["rows"])
    assert rows == [{"binary": "[binary omitted]", "prose": "a" * 2000}]


def test_source_catalog_sql_limit_and_metadata_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from aegis_alpha.storage import inspection  # noqa: PLC0415

    home = tmp_path / "aas"
    source = tmp_path / "source.sqlite3"
    seed(home, source)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        import_sqlite(workspace, source, "second-source", digest)
    monkeypatch.setattr(inspection, "SOURCE_LIMIT", 1)
    overview = Catalog(home).overview()
    assert len(cast("list[object]", overview["sources"])) == 1
    assert overview["source_total"] == 2
    assert overview["truncated"] is True
    monkeypatch.setattr(inspection, "CATALOG_BYTES", 1)
    assert Catalog(home).overview()["sources"] == []
    monkeypatch.setattr(inspection, "CATALOG_BYTES", 10000)
    monkeypatch.setattr(inspection, "MANIFEST_BYTES", 1)
    sources = cast("list[dict[str, object]]", Catalog(home).overview()["sources"])
    assert sources[0]["metadata_limited"] is True
    assert sources[0]["tables"] == []


def test_arrow_preview_omits_complex_and_preserves_scalar_text(tmp_path: Path) -> None:
    import pyarrow as pa  # noqa: PLC0415

    from aegis_alpha.storage.source_library import import_arrow  # noqa: PLC0415

    home = tmp_path / "aas"
    initialize(home)
    table = pa.table(
        {"label": ["visible"], "amount": [17], "blob": [b"x" * 10000], "items": [[1, 2]]}
    )
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        import_arrow(workspace, "arrow-preview", "a" * 64, "observations", table.to_reader())
    result = Catalog(home).sample("arrow-preview", "observations")
    assert result["rows"] == [
        {
            "label": "visible",
            "amount": "17",
            "blob": "[binary or complex value omitted]",
            "items": "[binary or complex value omitted]",
        }
    ]
