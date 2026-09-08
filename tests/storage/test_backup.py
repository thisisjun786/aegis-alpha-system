from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from aegis_alpha.storage import publication
from aegis_alpha.storage.backup import backup, restore
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.raw import put_raw_file
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace, write_json
from tests.engine.engine_support import contract, raw_bundle
from tests.storage.test_publication import document

_MIN_BACKUP_FILES = 5


def seed_workspace(home: Path) -> None:
    initialize(home)
    payload = raw_bundle(contract())
    digest = hashlib.sha256(payload).hexdigest()
    source = home.parent / "synthetic-strategy.json"
    source.write_bytes(payload)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        register_strategy(workspace, source, digest, "synthetic-probe", "1")
        publication.publish_document(workspace, parse_import(document()))


def test_nonempty_backup_restore_preserves_identity_and_excludes_secrets(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    seed_workspace(home)
    secret = home / "secrets" / "provider.json"
    secret.write_text(json.dumps({"api_key": "synthetic-secret"}) + "\n", encoding="utf-8")
    secret.chmod(0o600)
    runtime = json.loads((home / "runtime.json").read_text())
    runtime["providers"] = {"synthetic": {"api_key": "synthetic-secret"}}
    runtime["jobs"] = {"enabled": True}
    write_json(home / "runtime.json", runtime)
    original = json.loads((home / "installation.json").read_text())
    result = backup(home)
    assert result["backed_up"] is True
    assert result["secrets_included"] is False
    assert int(str(result["files"])) >= _MIN_BACKUP_FILES
    root = Path(str(result["backup_root"]))
    assert (root / "backup.json").is_file()
    assert (root / "state.sqlite3").is_file()
    assert (root / "strategies.sqlite3").is_file()
    assert (root / "market.duckdb").is_file()
    exported = json.loads((root / "runtime.json").read_text())
    assert exported["providers"] == {}
    assert exported["jobs"] == {"enabled": False}
    assert not (root / "secrets").exists()
    restored_home = tmp_path / "restored"
    restored = restore(root, restored_home)
    assert restored["restored"] is True
    assert restored["secrets_restored"] is False
    receipt = json.loads((restored_home / "installation.json").read_text())
    assert receipt["installation_id"] == original["installation_id"]
    assert receipt["stores"] == original["stores"]
    assert receipt["deployment_id"] != original["deployment_id"]
    assert original == json.loads((home / "installation.json").read_text())
    assert not any(restored_home.joinpath("secrets").iterdir())
    with open_workspace(restored_home) as workspace:
        report = verify_workspace(workspace)
        assert report == restored["verification"]
        assert report["dataset_versions"] == 1
        assert report["strategy_versions"] == 1
        assert report["pending_operations"] == 0
        assert report["orphan_generations"] == []
        assert publication.read_dataset(workspace, "synthetic-prices", "1")["row_count"] == 1


def test_backup_includes_wal_contents(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    seed_workspace(home)
    keeper = sqlite3.connect(home / "state.sqlite3")
    try:
        keeper.execute("PRAGMA wal_autocheckpoint=0")
        keeper.execute("INSERT INTO issuers VALUES ('wal-only','synthetic issuer')")
        keeper.commit()
        wal = home / "state.sqlite3-wal"
        assert wal.exists()
        assert wal.stat().st_size > 0
        root = Path(str(backup(home)["backup_root"]))
        restored_home = tmp_path / "from-wal"
        restore(root, restored_home)
        with open_workspace(restored_home) as workspace:
            assert (
                workspace.state.execute(
                    "SELECT name FROM issuers WHERE issuer_id='wal-only'"
                ).fetchone()[0]
                == "synthetic issuer"
            )
            assert (
                publication.read_dataset(workspace, "synthetic-prices", "1")["generation_id"]
                == "synthetic-generation"
            )
    finally:
        keeper.close()


def test_existing_target_and_symlink_backup_refused(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    seed_workspace(home)
    existing = tmp_path / "existing"
    existing.mkdir(mode=0o700)
    with pytest.raises(ValueError, match="new directory"):
        backup(home, existing)
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path / "missing-target")
    with pytest.raises(ValueError, match="new directory"):
        backup(home, alias)


def test_restore_refuses_existing_home_symlink_and_corrupt_bytes(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    seed_workspace(home)
    root = Path(str(backup(home)["backup_root"]))
    occupied = tmp_path / "occupied"
    occupied.mkdir(mode=0o700)
    with pytest.raises(ValueError, match="never overwritten"):
        restore(root, occupied)
    alias = tmp_path / "restored-alias"
    alias.symlink_to(tmp_path / "elsewhere")
    with pytest.raises(ValueError, match="never overwritten"):
        restore(root, alias)
    (root / "state.sqlite3").write_bytes((root / "state.sqlite3").read_bytes() + b"corrupt")
    with pytest.raises(ValueError, match="hash/size"):
        restore(root, tmp_path / "fresh")


def test_backup_refuses_unresolved_prepared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "aas"
    initialize(home)
    complete = publication._complete_publication  # noqa: SLF001 -- inject crash before catalog completion

    def crash(*_args: object) -> None:
        raise RuntimeError("synthetic backup pending")

    monkeypatch.setattr(publication, "_complete_publication", crash)
    with (
        open_workspace(home, writable=True) as workspace,
        pytest.raises(RuntimeError, match="pending"),
    ):
        publication.publish_document(workspace, parse_import(document()))
    monkeypatch.setattr(publication, "_complete_publication", complete)
    with pytest.raises(ValueError, match="recovered operations"):
        backup(home)


def test_streamed_source_archive_survives_backup_restore(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    source = tmp_path / "synthetic-archive"
    payload = b"synthetic retained bytes\x00" * 10_000
    source.write_bytes(payload)
    source.chmod(0o600)
    with open_workspace(home, writable=True) as workspace:
        relative, digest, size = put_raw_file(workspace.paths.raw, source)
        with workspace.state:
            workspace.state.execute(
                "INSERT INTO source_snapshots VALUES "
                "('local-copy','local-migration',1,2,NULL,'raw_verified')"
            )
            workspace.state.execute(
                "INSERT INTO source_files VALUES ('local-copy',?,?,?)", (relative, digest, size)
            )
    backup_root = Path(str(backup(home)["backup_root"]))
    restored = tmp_path / "restored"
    restore(backup_root, restored)
    assert (restored / "raw" / relative).read_bytes() == payload
    with open_workspace(restored) as workspace:
        assert verify_workspace(workspace)["verified"] is True
        assert tuple(
            workspace.state.execute(
                "SELECT requested_at_us,retrieved_at_us,publication_at_us "
                "FROM source_snapshots WHERE snapshot_id='local-copy'"
            ).fetchone()
        ) == (1, 2, None)
