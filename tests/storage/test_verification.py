from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from aegis_alpha.storage import publication
from aegis_alpha.storage.backup import backup, restore
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.input_pins import register_convention
from aegis_alpha.storage.strategies import LineageSpec, load_strategy
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.engine.engine_support import contract, raw_bundle
from tests.storage.test_backup import seed_workspace
from tests.storage.test_input_pins import A
from tests.storage.test_publication import document
from tests.storage.test_strategy_import import _interrupted_registration


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


# Every mutation preserves valid SQLite constraints and restores the exact shipped schema.
_EVIDENCE_CORRUPTIONS = [
    pytest.param(
        "state",
        "conventions",
        "UPDATE conventions SET payload=replace(payload,'capital','total_return')",
        id="convention-payload",
    ),
    pytest.param(
        "state",
        "conventions",
        "UPDATE conventions SET content_hash='" + "0" * 64 + "'",
        id="convention-hash",
    ),
    pytest.param(
        "strategies",
        "strategy_lineage",
        "UPDATE strategy_lineage SET reason_hash='" + "0" * 64 + "'",
        id="lineage-reason-hash",
    ),
    pytest.param(
        "strategies",
        "strategy_lineage",
        "UPDATE strategy_lineage SET reason='changed',"
        "reason_hash='9abf9994927acc503ec87e4433239aee75bb937357616bf8cca0290a1f3aa60a'",
        id="lineage-rehashed-reason",
    ),
    pytest.param(
        "strategies",
        "strategy_imports",
        "UPDATE strategy_imports SET request_hash='" + "0" * 64 + "'",
        id="private-receipt-hash",
    ),
    pytest.param(
        "state",
        "storage_operations",
        "UPDATE storage_operations SET request_hash='"
        + "0" * 64
        + "' WHERE kind='strategy_import'",
        id="completed-intent-hash",
    ),
    pytest.param(
        "strategies",
        "strategy_lineage",
        "UPDATE strategy_lineage SET parent_status='resolved'",
        id="resolved-absent-parent",
    ),
    pytest.param(
        "strategies",
        "strategy_lineage",
        "DELETE FROM strategy_lineage",
        id="lineage-removed",
    ),
    pytest.param(
        "strategies",
        "strategy_imports",
        "DELETE FROM strategy_imports",
        id="receipt-removed",
    ),
    pytest.param(
        "state",
        "storage_operations",
        "DELETE FROM storage_operations WHERE kind='strategy_import'",
        id="intent-removed",
    ),
    pytest.param(
        "state",
        "storage_operations",
        "UPDATE storage_operations SET kind='other' WHERE kind='strategy_import'",
        id="intent-kind",
    ),
    pytest.param(
        "state",
        "storage_operations",
        "UPDATE storage_operations SET target_id='other:1' WHERE kind='strategy_import'",
        id="intent-target",
    ),
    pytest.param(
        "state",
        "storage_operations",
        "UPDATE storage_operations SET payload_hash='"
        + "0" * 64
        + "' WHERE kind='strategy_import'",
        id="intent-payload",
    ),
    pytest.param(
        "state",
        "storage_operations",
        "UPDATE storage_operations SET expected_parent='other' WHERE kind='strategy_import'",
        id="intent-parent",
    ),
]


def corrupt_closed_store(home: Path, store: str, table: str, mutation: str) -> None:
    connection = sqlite3.connect(home / (store + ".sqlite3"))
    try:
        schema_sql = "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
        schema = connection.execute(schema_sql).fetchall()
        triggers = connection.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (table,)
        ).fetchall()
        for name, _sql in triggers:
            connection.execute('DROP TRIGGER "' + name.replace('"', '""') + '"')
        connection.execute(mutation)
        for _name, sql in triggers:
            connection.execute(sql)
        connection.commit()
        assert connection.execute(schema_sql).fetchall() == schema
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        connection.close()


def evidence_snapshot(home: Path) -> tuple[str, str]:
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        return "\n".join(workspace.state.iterdump()), "\n".join(workspace.strategies.iterdump())


@pytest.mark.parametrize(("store", "table", "mutation"), _EVIDENCE_CORRUPTIONS)
def test_logical_evidence_rejected_at_all_integrity_boundaries(
    tmp_path: Path, store: str, table: str, mutation: str
) -> None:
    home = tmp_path / "original"
    seed_workspace(home, lineage=LineageSpec("absent-parent", "7", "derived", "synthetic"))
    with open_workspace(home, writable=True) as workspace:
        register_convention(workspace.state, A, expected_file_sha256=hashlib.sha256(A).hexdigest())
    original = evidence_snapshot(home)
    clean_backup = tmp_path / "clean-backup"
    backup(home, clean_backup)
    copied = tmp_path / "corrupt-copy"
    shutil.copytree(home, copied)
    corrupt_closed_store(copied, store, table, mutation)
    corrupt = evidence_snapshot(copied)
    with (
        open_workspace(copied) as workspace,
        pytest.raises(ValueError, match=r"convention|strategy"),
    ):
        verify_workspace(workspace)
    with pytest.raises(ValueError, match=r"convention|strategy"):
        backup(copied, tmp_path / "rejected-backup")
    assert not (tmp_path / "rejected-backup").exists()
    assert evidence_snapshot(copied) == corrupt

    # Outer file hashes are honest: restore must reject *logical* corruption beneath them.
    corrupt_closed_store(clean_backup, store, table, mutation)
    manifest_path = clean_backup / "backup.json"
    manifest = json.loads(manifest_path.read_text())
    name = store + ".sqlite3"
    raw = (clean_backup / name).read_bytes()
    manifest["files"][name] = {
        "size_bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }
    manifest_path.write_text(json.dumps(manifest))
    target = tmp_path / "rejected-restore"
    with pytest.raises(ValueError, match=r"convention|strategy"):
        restore(clean_backup, target)
    assert json.loads((target / "installation.json").read_text())["phase"] == "restore-incomplete"
    assert evidence_snapshot(home) == original


def test_matching_receipt_and_intent_must_match_current_exact_lineage(tmp_path: Path) -> None:
    home = tmp_path / "original"
    seed_workspace(home, lineage=LineageSpec("absent-parent", "7", "derived", "synthetic"))
    copied = tmp_path / "corrupt-copy"
    shutil.copytree(home, copied)
    for store, table in (("state", "storage_operations"), ("strategies", "strategy_imports")):
        where = " WHERE kind='strategy_import'" if store == "state" else ""
        corrupt_closed_store(
            copied,
            store,
            table,
            "UPDATE " + table + " SET request_hash='" + "0" * 64 + "'" + where,
        )
    with open_workspace(copied) as workspace, pytest.raises(ValueError, match="stored lineage"):
        verify_workspace(workspace)


def test_load_rejects_resolved_edge_without_exact_parent(tmp_path: Path) -> None:
    home = tmp_path / "original"
    seed_workspace(home, lineage=LineageSpec("absent-parent", "7", "derived", "synthetic"))
    copied = tmp_path / "corrupt-copy"
    shutil.copytree(home, copied)
    corrupt_closed_store(
        copied,
        "strategies",
        "strategy_lineage",
        "UPDATE strategy_lineage SET parent_status='resolved'",
    )
    before = evidence_snapshot(copied)
    with open_workspace(copied) as workspace:
        assert workspace.strategies is not None
        digest = workspace.strategies.execute(
            "SELECT raw_sha256 FROM strategy_versions"
        ).fetchone()[0]
        assert (
            workspace.strategies.execute(
                "SELECT count(*) FROM strategy_versions "
                "WHERE strategy_id='absent-parent' AND version='7'"
            ).fetchone()[0]
            == 0
        )
        with pytest.raises(ValueError, match="resolved parent missing"):
            load_strategy(workspace.strategies, "synthetic-probe", "1", digest)
    assert evidence_snapshot(copied) == before


@pytest.mark.parametrize("parent_status", ["unresolved", "none", "resolved"])
def test_verify_private_commit_remains_pending_and_select_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, parent_status: str
) -> None:
    home, _digest, operation_id = _interrupted_registration(tmp_path, monkeypatch, parent_status)
    with open_workspace(home, writable=True) as workspace:
        register_convention(workspace.state, A, expected_file_sha256=hashlib.sha256(A).hexdigest())
    before = evidence_snapshot(home)
    actions: set[int] = set()

    def select_only(action: int, *_args: str | None) -> int:
        actions.add(action)
        return (
            sqlite3.SQLITE_OK
            if action
            in {
                sqlite3.SQLITE_SELECT,
                sqlite3.SQLITE_READ,
                sqlite3.SQLITE_FUNCTION,
                sqlite3.SQLITE_PRAGMA,
                sqlite3.SQLITE_RECURSIVE,
            }
            else sqlite3.SQLITE_DENY
        )

    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        workspace.state.set_authorizer(select_only)
        workspace.strategies.set_authorizer(select_only)
        try:
            report = verify_workspace(workspace)
        finally:
            workspace.state.set_authorizer(None)
            workspace.strategies.set_authorizer(None)
        assert report["verified"] is True
        assert report["pending_operations"] == 1
        assert (
            workspace.state.execute(
                "SELECT phase FROM storage_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()[0]
            == "PREPARED"
        )
    assert sqlite3.SQLITE_READ in actions
    assert evidence_snapshot(home) == before
    with pytest.raises(ValueError, match="recovered operations"):
        backup(home, tmp_path / "pending-backup")
    assert evidence_snapshot(home) == before


@pytest.mark.parametrize("status", ["none", "unresolved", "resolved", "late-parent", "ancestor"])
def test_valid_evidence_round_trip_keeps_immutable_status(tmp_path: Path, status: str) -> None:
    home = tmp_path / "original"
    initialize(home)
    child = raw_bundle(contract())
    digest = hashlib.sha256(child).hexdigest()
    parent = child.replace(b'"synthetic-probe"', b'"parent"')
    source = tmp_path / "synthetic.json"
    lineage = None if status == "none" else LineageSpec("parent", "1", "derived", " exact\n")
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        if status in {"resolved", "ancestor"}:
            source.write_bytes(parent)
            register_strategy(
                workspace,
                source,
                hashlib.sha256(parent).hexdigest(),
                "parent",
                "1",
                lineage=LineageSpec("absent", "7", "derived", "ancestor")
                if status == "ancestor"
                else None,
            )
        source.write_bytes(child)
        register_strategy(workspace, source, digest, "synthetic-probe", "1", lineage=lineage)
        if status == "late-parent":
            source.write_bytes(parent)
            register_strategy(workspace, source, hashlib.sha256(parent).hexdigest(), "parent", "1")
        register_convention(workspace.state, A, expected_file_sha256=hashlib.sha256(A).hexdigest())
    source.unlink()
    before = evidence_snapshot(home)
    identity = json.loads((home / "installation.json").read_text())
    output, restored = tmp_path / "backup", tmp_path / "restored"
    backup(home, output)
    restore(output, restored)
    restored_identity = json.loads((restored / "installation.json").read_text())
    assert restored_identity["installation_id"] == identity["installation_id"]
    assert restored_identity["stores"] == identity["stores"]
    assert restored_identity["deployment_id"] != identity["deployment_id"]
    for root in (home, restored):
        assert evidence_snapshot(root) == before
        with open_workspace(root) as workspace:
            assert workspace.strategies is not None
            assert verify_workspace(workspace)["verified"] is True
            rows = workspace.strategies.execute(
                "SELECT parent_status FROM strategy_lineage WHERE strategy_id='synthetic-probe'"
            ).fetchall()
            assert [row[0] for row in rows] == (
                []
                if status == "none"
                else ["unresolved"]
                if status in {"unresolved", "late-parent"}
                else ["resolved"]
            )
            if status in {"unresolved", "late-parent"}:
                with pytest.raises(ValueError, match="unresolved parent"):
                    load_strategy(workspace.strategies, "synthetic-probe", "1", digest)
            else:
                assert (
                    load_strategy(
                        workspace.strategies,
                        "synthetic-probe",
                        "1",
                        digest,
                    ).source_sha256
                    == digest
                )
            if status == "none":
                assert (
                    workspace.strategies.execute(
                        "SELECT request_hash FROM strategy_imports"
                    ).fetchone()[0]
                    == digest
                )
                assert workspace.state.execute(
                    "SELECT request_hash,payload_hash FROM storage_operations"
                ).fetchone()[:] == (digest, digest)
        assert evidence_snapshot(root) == before


def test_verify_before_private_commit_keeps_missing_receipt_pending(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    raw = raw_bundle(contract())
    source = tmp_path / "synthetic.json"
    source.write_bytes(raw)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        assert workspace.strategies is not None
        workspace.strategies.execute(
            "CREATE TEMP TRIGGER interrupt BEFORE INSERT ON strategy_versions "
            "BEGIN SELECT RAISE(ABORT,'before private commit'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="before private commit"):
            register_strategy(
                workspace, source, hashlib.sha256(raw).hexdigest(), "synthetic-probe", "1"
            )
    source.unlink()
    before = evidence_snapshot(home)
    with open_workspace(home) as workspace:
        report = verify_workspace(workspace)
        assert report["verified"] is True
        assert report["strategy_versions"] == 0
        assert report["pending_operations"] == 1
    assert evidence_snapshot(home) == before
