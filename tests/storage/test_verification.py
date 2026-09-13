from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from aegis_alpha.storage import publication
from aegis_alpha.storage.backup import backup
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.strategies import LineageSpec, load_strategy
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


def test_verify_unresolved_import_checks_integrity_not_execution(tmp_path: Path) -> None:
    # Given a real completed import with a missing direct parent.
    home = tmp_path / "aas"
    seed_workspace(home, lineage=LineageSpec("absent-parent", "7", "derived", "synthetic"))
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        before = "\n".join(workspace.strategies.iterdump())
        state = "\n".join(workspace.state.iterdump())
        digest = workspace.strategies.execute(
            "SELECT raw_sha256 FROM strategy_versions"
        ).fetchone()[0]

        # When workspace integrity is verified, unresolved lineage is valid stored state.
        report = verify_workspace(workspace)

        # Then verification is read-only and does not grant execution eligibility.
        assert report == {
            "verified": True,
            "dataset_versions": 1,
            "strategy_versions": 1,
            "pending_operations": 0,
            "orphan_generations": [],
        }
        assert "\n".join(workspace.strategies.iterdump()) == before
        assert "\n".join(workspace.state.iterdump()) == state
        with pytest.raises(ValueError, match="unresolved parent lineage"):
            load_strategy(workspace.strategies, "synthetic-probe", "1", digest)


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        ("UPDATE strategy_versions SET raw_bundle=x'7b7d'", "raw payload SHA-256"),
        ("UPDATE strategy_versions SET raw_sha256='" + "0" * 64 + "'", "raw payload SHA-256"),
        ("UPDATE strategy_versions SET contract_json='{}'", "parsed contract hash"),
        ("UPDATE strategy_versions SET contract_sha256='" + "0" * 64 + "'", "parsed contract hash"),
    ],
    ids=["raw", "raw-pin", "contract", "contract-pin"],
)
def test_copied_unresolved_corruption_fails_verify_and_backup(
    tmp_path: Path, mutation: str, error: str
) -> None:
    # Given a copy of a legitimately imported unresolved child, corrupt only that copy.
    home = tmp_path / "aas"
    seed_workspace(home, lineage=LineageSpec("absent-parent", "7", "derived", "synthetic"))
    copied = tmp_path / "corrupt-copy"
    shutil.copytree(home, copied)
    with open_workspace(copied, writable=True, strategy_write=True) as workspace:
        assert workspace.strategies is not None
        connection = workspace.strategies
        triggers = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name IN "
            "('strategy_versions_reject_update', 'immutable_strategy_versions_update')"
        ).fetchall()
        connection.executescript(
            "DROP TRIGGER strategy_versions_reject_update;"
            "DROP TRIGGER immutable_strategy_versions_update;"
        )
        connection.execute(mutation)
        for trigger in triggers:
            connection.execute(trigger[0])
        connection.commit()

    # When either integrity surface examines the copy, actual content validation still fails.
    with open_workspace(copied) as workspace, pytest.raises(ValueError, match=error):
        verify_workspace(workspace)
    output = tmp_path / "rejected-backup"
    with pytest.raises(ValueError, match=error):
        backup(copied, output)

    # Then no partial backup is published and the original unresolved workspace remains valid.
    assert not output.exists()
    with open_workspace(home) as workspace:
        assert verify_workspace(workspace)["verified"] is True


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
