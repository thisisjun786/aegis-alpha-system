from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from aegis_alpha.storage import publication
from aegis_alpha.storage.backup import backup, restore
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.raw import put_raw_file
from aegis_alpha.storage.strategies import LineageSpec, load_strategy
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace, write_json
from tests.engine.engine_support import contract, raw_bundle
from tests.storage.test_market_inputs import BUDGET, mixed_proxy_publications
from tests.storage.test_membership_pins import (
    I1,
    U1,
    VECTORS,
    evidence,
    register,
    source_evidence,
    state_image,
)
from tests.storage.test_publication import document

_MIN_BACKUP_FILES = 5


def seed_workspace(home: Path, *, lineage: LineageSpec | None = None) -> None:
    initialize(home)
    payload = raw_bundle(contract())
    digest = hashlib.sha256(payload).hexdigest()
    source = home.parent / "synthetic-strategy.json"
    source.write_bytes(payload)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        register_strategy(workspace, source, digest, "synthetic-probe", "1", lineage=lineage)
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


def test_unresolved_import_backup_restore_preserves_content_and_eligibility(tmp_path: Path) -> None:
    # Given a completed import whose direct parent is legitimately absent.
    home = tmp_path / "aas"
    seed_workspace(home, lineage=LineageSpec("absent-parent", "7", "derived", "synthetic"))
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        original = "\n".join(workspace.strategies.iterdump())
        state = "\n".join(workspace.state.iterdump())
        digest = workspace.strategies.execute(
            "SELECT raw_sha256 FROM strategy_versions"
        ).fetchone()[0]
    identity = json.loads((home / "installation.json").read_text())

    # When the complete workspace is backed up and restored to a fresh root.
    result = backup(home, tmp_path / "backup")
    restored_home = tmp_path / "restored"
    restored = restore(Path(str(result["backup_root"])), restored_home)

    # Then immutable content/identities survive, without resolving or enabling the child.
    assert result["backed_up"] is True
    assert restored["restored"] is True
    restored_identity = json.loads((restored_home / "installation.json").read_text())
    assert restored_identity["installation_id"] == identity["installation_id"]
    assert restored_identity["stores"] == identity["stores"]
    assert restored_identity["deployment_id"] != identity["deployment_id"]
    for root in (home, restored_home):
        with open_workspace(root) as workspace:
            assert workspace.strategies is not None
            assert "\n".join(workspace.strategies.iterdump()) == original
            assert "\n".join(workspace.state.iterdump()) == state
            report = verify_workspace(workspace)
            assert report == restored["verification"]
            assert report["verified"] is True
            assert report["strategy_versions"] == 1
            assert (
                workspace.strategies.execute(
                    "SELECT parent_status FROM strategy_lineage"
                ).fetchone()[0]
                == "unresolved"
            )
            with pytest.raises(ValueError, match="unresolved parent lineage"):
                load_strategy(workspace.strategies, "synthetic-probe", "1", digest)


def test_mixed_proxy_backup_restore_preserves_exact_pins(tmp_path: Path) -> None:
    from aegis_alpha.storage.market import read_chain_rows  # noqa: PLC0415
    from aegis_alpha.storage.market_inputs import load_pinned_proxy  # noqa: PLC0415

    home = tmp_path / "proxy-home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        old_pin, mixed_pin = mixed_proxy_publications(workspace, tmp_path)
        old = load_pinned_proxy(workspace, old_pin, budget=BUDGET)
        history = read_chain_rows(workspace.market, mixed_pin.generation_id, budget=BUDGET)
        before = "\n".join(workspace.state.iterdump())
    for source in tmp_path.glob("*.sqlite3"):
        source.unlink()
    for spec in tmp_path.glob("*.json"):
        spec.unlink()
    root = Path(str(backup(home, tmp_path / "backup")["backup_root"]))
    target = tmp_path / "restored"
    assert restore(root, target)["restored"] is True
    with open_workspace(target) as workspace:
        assert "\n".join(workspace.state.iterdump()) == before
        assert read_chain_rows(workspace.market, mixed_pin.generation_id, budget=BUDGET) == history
        assert load_pinned_proxy(workspace, old_pin, budget=BUDGET) == old
        with pytest.raises(ValueError, match=r"proxy.*conflict"):
            load_pinned_proxy(workspace, mixed_pin, budget=BUDGET)
        assert verify_workspace(workspace)["verified"] is True


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


def test_membership_backup_reconstructs_all_literal_vectors_without_originals(
    tmp_path: Path,
) -> None:
    home = tmp_path / "memberships"
    initialize(home)
    documents = tmp_path / "documents"
    documents.mkdir()
    pins = []
    with open_workspace(home, writable=True) as workspace:
        source_evidence(workspace)
        for index, (raw, _, digest) in enumerate(VECTORS):
            path = documents / f"{index}.json"
            path.write_bytes(raw)
            pin = register(workspace, path.read_bytes())
            assert pin.content_hash == digest
            pins.append(pin)
            path.unlink()
        # Closed, quarantined source evidence with no files is valid preservation,
        # not source eligibility or execution readiness.
        workspace.state.execute(
            "INSERT INTO source_snapshots VALUES ('q','other',0,0,NULL,'quarantined')"
        )
        workspace.state.commit()
        body = json.loads(U1)
        body["universe_id"] = "quarantined"
        body["members"][0]["source_snapshot_id"] = "q"
        body["sources"] = [
            {
                "snapshot_id": "q",
                "provider": "other",
                "requested_at_us": 0,
                "retrieved_at_us": 0,
                "publication_at_us": None,
                "status": "quarantined",
                "files": [],
            }
        ]
        register(workspace, json.dumps(body).encode())
        before = state_image(workspace)
    documents.rmdir()
    root = Path(str(backup(home)["backup_root"]))
    restored = tmp_path / "restored-memberships"
    assert restore(root, restored)["restored"] is True
    with open_workspace(restored) as workspace:
        assert state_image(workspace) == before
        for pin, (raw, _, digest) in zip(pins, VECTORS, strict=True):
            actual = evidence(workspace, pin)
            assert actual.canonical_bytes == raw
            assert actual.pin.content_hash == digest
        assert verify_workspace(workspace)["verified"] is True


@pytest.mark.parametrize("fault", ["member", "join", "interval", "inventory"])
def test_corrupt_membership_backup_rejected_with_refreshed_outer_hashes(
    tmp_path: Path, fault: str
) -> None:
    home = tmp_path / "original"
    initialize(home)
    with open_workspace(home, writable=True) as workspace:
        source_evidence(workspace)
        register(workspace, I1)
    root = Path(str(backup(home)["backup_root"]))
    with closing(sqlite3.connect(root / "state.sqlite3")) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        schema_query = (
            "SELECT type,name,tbl_name,rootpage,sql FROM sqlite_master ORDER BY type,name"
        )
        original_schema = connection.execute(schema_query).fetchall()
        trigger_sql = {name: sql for kind, name, _, _, sql in original_schema if kind == "trigger"}
        if fault == "member":
            connection.execute(
                "INSERT INTO identity_assertions VALUES "
                "('b','ASSET_A','synthetic','ticker','A',0,25,35,'a','s',?)",
                ("d" * 64,),
            )
            connection.execute(
                "INSERT INTO identity_snapshot_members VALUES ('ids',1,'b',0,25,35,NULL)"
            )
        elif fault == "join":
            connection.execute("INSERT INTO instruments VALUES ('OTHER',NULL,'etf','X')")
            connection.execute("DROP TRIGGER immutable_identity_assertions_update")
            connection.execute("UPDATE identity_assertions SET instrument_id='OTHER'")
            connection.execute(trigger_sql["immutable_identity_assertions_update"])
        elif fault == "interval":
            connection.execute("DROP TRIGGER immutable_identity_snapshot_members_update")
            connection.execute("UPDATE identity_snapshot_members SET known_to_us=36")
            connection.execute(trigger_sql["immutable_identity_snapshot_members_update"])
        else:
            connection.execute(
                "INSERT INTO source_files VALUES ('s','new-evidence',?,0)",
                (hashlib.sha256(b"").hexdigest(),),
            )
        connection.commit()
        assert connection.execute(schema_query).fetchall() == original_schema
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    raw = (root / "state.sqlite3").read_bytes()
    manifest = json.loads((root / "backup.json").read_bytes())
    manifest["files"]["state.sqlite3"] = {
        "size_bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }
    write_json(root / "backup.json", manifest)
    before = (root / "state.sqlite3").read_bytes()
    target = tmp_path / "rejected"
    with pytest.raises(ValueError, match=r"membership.*mismatch"):
        restore(root, target)
    assert json.loads((target / "installation.json").read_bytes())["phase"] == "restore-incomplete"
    assert (root / "state.sqlite3").read_bytes() == before


def test_arbitrary_membership_header_blocks_backup_without_mutation(tmp_path: Path) -> None:
    home = tmp_path / "old-store"
    initialize(home)
    with open_workspace(home, writable=True) as workspace:
        workspace.state.execute(
            "INSERT INTO identity_snapshots VALUES ('unverified',?,0)", ("b" * 64,)
        )
        workspace.state.commit()
        before = state_image(workspace)
    target = tmp_path / "must-not-exist"
    with pytest.raises(ValueError, match=r"membership.*mismatch"):
        backup(home, target)
    assert not target.exists()
    with open_workspace(home) as workspace:
        assert state_image(workspace) == before


def test_backup_and_restore_receipts_count_recorded_runs(tmp_path: Path) -> None:
    """An operator restoring a run-bearing backup must be told what it carried.

    The count belongs on the receipt, never in the verification report: restore compares
    that report against the manifest by full equality, so a new key there would make every
    backup taken before this change restore as incomplete.
    """
    home = tmp_path / "aas"
    seed_workspace(home)
    result = backup(home)
    assert result["runs"] == {}
    root = Path(str(result["backup_root"]))
    restored = restore(root, tmp_path / "restored")
    assert restored["runs"] == {}
    verification = restored["verification"]
    assert isinstance(verification, dict)
    assert "runs" not in verification
    stored = json.loads((root / "backup.json").read_text())
    assert "runs" not in stored["logical"]
