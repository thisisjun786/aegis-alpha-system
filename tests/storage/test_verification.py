from __future__ import annotations

from pathlib import Path

import pytest

from aegis_alpha.storage import publication
from aegis_alpha.storage.backup import backup
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.test_backup import seed_workspace
from tests.storage.test_publication import document


def test_verify_workspace_reports_nonempty_logical_refs(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    seed_workspace(home)
    with open_workspace(home) as workspace:
        report = verify_workspace(workspace)
    assert report["verified"] is True
    assert report["dataset_versions"] == 1
    assert report["strategy_versions"] == 1
    assert report["pending_operations"] == 0
    assert report["orphan_generations"] == []


def test_verify_reports_pending_and_orphan_and_backup_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "aas"
    initialize(home)
    complete = publication._complete_publication  # noqa: SLF001 -- inject crash before catalog completion

    def crash(*_args: object) -> None:
        raise RuntimeError("synthetic verification pending")

    monkeypatch.setattr(publication, "_complete_publication", crash)
    with (
        open_workspace(home, writable=True) as workspace,
        pytest.raises(RuntimeError, match="pending"),
    ):
        publication.publish_document(workspace, parse_import(document()))
    monkeypatch.setattr(publication, "_complete_publication", complete)
    with open_workspace(home) as workspace:
        report = verify_workspace(workspace)
        assert report["verified"] is True
        assert report["pending_operations"] == 1
        assert report["orphan_generations"] == ["synthetic-generation"]
        assert report["dataset_versions"] == 0
    with pytest.raises(ValueError, match="recovered operations"):
        backup(home)


def test_raw_corruption_fails_verify(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    seed_workspace(home)
    with open_workspace(home) as workspace:
        relative = workspace.state.execute("SELECT relative_path FROM source_files").fetchone()[0]
        raw_file = workspace.paths.raw / relative
    raw_file.write_bytes(b"corrupt-raw")
    raw_file.chmod(0o600)
    with open_workspace(home) as workspace, pytest.raises(ValueError, match=r"checksum|hash"):
        verify_workspace(workspace)
