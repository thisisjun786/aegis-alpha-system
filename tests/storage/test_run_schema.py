from __future__ import annotations

import hashlib
import json
import os
import select
import shutil
import signal
import sqlite3
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import duckdb
import pytest

from aegis_alpha.storage.backup import backup, restore
from aegis_alpha.storage.workspace import initialize, open_workspace


def test_explicit_two_store_install_and_restore(tmp_path: Path) -> None:
    from aegis_alpha.storage.run_schema import (  # noqa: PLC0415 -- RED missing owner
        RunSchemaError,
        inspect_run_schema,
        install_run_schema,
        require_run_schema,
    )

    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home) as workspace:
        original = tuple(workspace.state.execute("SELECT * FROM schema_migrations").fetchone())
        market_original = workspace.market.execute("SELECT * FROM schema_migrations").fetchall()
        assert inspect_run_schema(workspace).state == "absent"
        with pytest.raises(RunSchemaError, match="run_schema_required"):
            require_run_schema(workspace)
        assert workspace.state.total_changes == 0
    pre = tmp_path / "pre"
    result = install_run_schema(home, backup_output=pre)
    assert result["state"] == "complete"
    assert install_run_schema(home) == result
    with open_workspace(home, writable=True) as workspace:
        assert inspect_run_schema(workspace).state == "complete"
        assert (
            workspace.market.execute("SELECT * FROM schema_migrations").fetchall()
            == market_original
        )
        assert (
            tuple(workspace.state.execute("SELECT * FROM schema_migrations").fetchone()) == original
        )
        with pytest.raises(sqlite3.IntegrityError):
            workspace.state.execute("DELETE FROM run_schema")
        workspace.state.rollback()
        with pytest.raises(duckdb.ConstraintException):
            workspace.market.execute(
                "INSERT INTO result_trade_decisions VALUES ('missing','aegis',0,1)"
            )
    restored = tmp_path / "old-restored"
    restore(pre, restored)
    with open_workspace(restored) as workspace:
        assert inspect_run_schema(workspace).state == "absent"
    complete = Path(str(backup(home, tmp_path / "complete")["backup_root"]))
    fresh = tmp_path / "fresh"
    restore(complete, fresh)
    with open_workspace(fresh) as workspace:
        require_run_schema(workspace)


def test_unowned_partial_schema_is_rejected_without_intent(tmp_path: Path) -> None:
    from aegis_alpha.storage.run_schema import RunSchemaError, install_run_schema  # noqa: PLC0415

    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True) as workspace:
        workspace.state.execute("CREATE TABLE backtest_requests(bundle_id TEXT)")
        workspace.state.commit()
        before = "\n".join(workspace.state.iterdump())
    with pytest.raises(RunSchemaError, match="run_schema_invalid"):
        install_run_schema(home, backup_output=tmp_path / "backup")
    assert not (tmp_path / "backup").exists()
    with open_workspace(home) as workspace:
        assert "\n".join(workspace.state.iterdump()) == before


@pytest.mark.parametrize("stage", ["state", "market", "complete"])
def test_original_intent_partial_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    from aegis_alpha.storage import run_schema  # noqa: PLC0415
    from aegis_alpha.storage.state import get_operation  # noqa: PLC0415

    home = tmp_path / "home"
    initialize(home)
    target = {
        "state": "_install_state",
        "market": "_install_market",
        "complete": "complete_operation",
    }[stage]
    original = getattr(run_schema, target)

    def interrupted(*_args: object) -> None:
        raise RuntimeError("acknowledged boundary")

    monkeypatch.setattr(run_schema, target, interrupted)
    pre = tmp_path / "pre"
    with pytest.raises(RuntimeError, match="acknowledged"):
        run_schema.install_run_schema(home, backup_output=pre)
    with open_workspace(home) as workspace:
        status = run_schema.inspect_run_schema(workspace)
        assert status.state == "partial"
        assert status.state_installed == (stage != "state")
        assert status.market_installed == (stage == "complete")
        intent = get_operation(workspace.state, "run-schema-install-v1")
        assert intent is not None
        with pytest.raises(run_schema.RunSchemaError, match="run_schema_incomplete"):
            run_schema.require_run_schema(workspace)
    with pytest.raises(ValueError, match="recovered operations"):
        backup(home, tmp_path / "partial-backup")
    assert not (tmp_path / "partial-backup").exists()
    preserved = (pre / "backup.json").read_bytes()
    monkeypatch.setattr(run_schema, target, original)
    assert run_schema.install_run_schema(home, backup_output=pre)["state"] == "complete"
    assert (pre / "backup.json").read_bytes() == preserved
    with open_workspace(home) as workspace:
        completed = get_operation(workspace.state, "run-schema-install-v1")
        assert completed is not None
        excluded = {"phase", "completed_at_us"}
        assert {k: v for k, v in completed.items() if k not in excluded} == {
            k: v for k, v in intent.items() if k not in excluded
        }
        assert completed["phase"] == "COMPLETED"
        before = "\n".join(workspace.state.iterdump())
    run_schema.install_run_schema(home)
    with open_workspace(home) as workspace:
        assert "\n".join(workspace.state.iterdump()) == before


@pytest.mark.parametrize("fault", ["checksum", "trigger", "state_fk", "market_fk", "intent"])
def test_actual_schema_and_intent_corruption_rejects_without_repair(
    tmp_path: Path, fault: str
) -> None:
    from aegis_alpha.storage.run_schema import (  # noqa: PLC0415
        RunSchemaError,
        inspect_run_schema,
        install_run_schema,
    )

    home = tmp_path / "home"
    initialize(home)
    install_run_schema(home)
    with open_workspace(home, writable=True) as workspace:
        if fault == "checksum":
            workspace.market.execute("UPDATE run_schema SET checksum=?", ["0" * 64])
        elif fault == "trigger":
            workspace.state.execute("DROP TRIGGER immutable_run_schema_delete")
        elif fault == "state_fk":
            workspace.state.execute("DROP TABLE backtest_requests")
            workspace.state.execute(
                "CREATE TABLE backtest_requests(bundle_id TEXT PRIMARY KEY, "
                "request_bytes BLOB, request_hash TEXT)"
            )
        elif fault == "market_fk":
            workspace.market.execute("DROP TABLE result_trade_decisions")
            workspace.market.execute(
                "CREATE TABLE result_trade_decisions(run_id VARCHAR NOT NULL, "
                "module VARCHAR NOT NULL, ordinal BIGINT NOT NULL CHECK(ordinal>=0), "
                "decision_at_us BIGINT NOT NULL CHECK(decision_at_us>=0), "
                "PRIMARY KEY(run_id,module,ordinal))"
            )
        else:
            workspace.state.execute("DROP TRIGGER operation_identity")
            workspace.state.execute(
                "UPDATE storage_operations SET request_hash=? WHERE kind='run-schema-install'",
                ("0" * 64,),
            )
        workspace.state.commit()
        before = "\n".join(workspace.state.iterdump())
        with pytest.raises(RunSchemaError, match="run_schema_invalid"):
            inspect_run_schema(workspace)
    with pytest.raises(RunSchemaError, match="run_schema_invalid"):
        install_run_schema(home, backup_output=tmp_path / "unused")
    assert not (tmp_path / "unused").exists()
    with open_workspace(home) as workspace:
        assert "\n".join(workspace.state.iterdump()) == before


def test_constraints_are_real_and_request_hash_is_not_unique(tmp_path: Path) -> None:
    from aegis_alpha.storage.run_schema import install_run_schema  # noqa: PLC0415

    home = tmp_path / "home"
    initialize(home)
    install_run_schema(home)
    with open_workspace(home, writable=True) as workspace:
        for sql, params in (
            ("INSERT INTO backtest_requests VALUES (?,?,?)", ("missing", b"{}", "a" * 64)),
            (
                "INSERT INTO run_details VALUES (?,?,?,?)",
                ("missing", "a" * 64, None, "aas-backtest-request-v1"),
            ),
        ):
            with pytest.raises(sqlite3.IntegrityError):
                workspace.state.execute(sql, params)
            workspace.state.rollback()
        workspace.state.execute(
            "INSERT INTO input_bundles VALUES ('b',?,'aas-input-bundle-v1')", ("a" * 64,)
        )
        for identity in ("first", "second"):
            workspace.state.execute(
                "INSERT INTO runs VALUES (?,NULL,'b','engine','environment',NULL,"
                "'fixture','RUNNING',0,NULL,NULL)",
                (identity,),
            )
            workspace.state.execute(
                "INSERT INTO run_details VALUES (?,?,NULL,'aas-backtest-request-v1')",
                (identity, "a" * 64),
            )
        workspace.state.commit()
        for sql in ("DELETE FROM run_details", "UPDATE run_details SET request_hash=request_hash"):
            with pytest.raises(sqlite3.IntegrityError):
                workspace.state.execute(sql)
            workspace.state.rollback()
        workspace.market.execute(
            "INSERT INTO result_commits VALUES ('r','op',?,?, '{}','{}')", ["a" * 64, "b" * 64]
        )
        workspace.market.execute("INSERT INTO result_trade_decisions VALUES ('r','aegis',0,20)")
        for values in (
            ("r", "aegis", 0, 20),
            ("r", "aegis", -1, 20),
            ("r", "aegis", 1, -1),
            ("absent", "aegis", 1, 20),
            (None, "aegis", 1, 20),
        ):
            with pytest.raises(duckdb.ConstraintException):
                workspace.market.execute(
                    "INSERT INTO result_trade_decisions VALUES (?,?,?,?)", values
                )


def test_backup_failure_and_pending_work_leave_install_unmodified(tmp_path: Path) -> None:
    from aegis_alpha.storage.run_schema import RunSchemaError, install_run_schema  # noqa: PLC0415
    from aegis_alpha.storage.state import prepare_operation  # noqa: PLC0415

    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home) as workspace:
        before = "\n".join(workspace.state.iterdump())
    occupied = tmp_path / "occupied"
    occupied.mkdir()
    with pytest.raises(ValueError, match="new directory"):
        install_run_schema(home, backup_output=occupied)
    with open_workspace(home, writable=True) as workspace:
        assert "\n".join(workspace.state.iterdump()) == before
        prepare_operation(
            workspace.state,
            operation_id="other",
            kind="other",
            request_hash="a" * 64,
            target_id="other",
            expected_parent=None,
            payload_hash="b" * 64,
        )
        before = "\n".join(workspace.state.iterdump())
    with pytest.raises(RunSchemaError, match="unrelated"):
        install_run_schema(home, backup_output=tmp_path / "no-backup")
    assert not (tmp_path / "no-backup").exists()
    with open_workspace(home) as workspace:
        assert "\n".join(workspace.state.iterdump()) == before


@pytest.mark.parametrize("store", ["state", "market"])
def test_case_insensitive_owned_name_collision_precedes_backup(tmp_path: Path, store: str) -> None:
    from aegis_alpha.storage.run_schema import RunSchemaError, install_run_schema  # noqa: PLC0415

    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True) as workspace:
        if store == "state":
            workspace.state.execute("CREATE TABLE BACKTEST_REQUESTS (unrelated TEXT)")
            workspace.state.commit()
        else:
            workspace.market.execute("CREATE TABLE RESULT_TRADE_DECISIONS (unrelated VARCHAR)")
        before = "\n".join(workspace.state.iterdump())
    with pytest.raises(RunSchemaError, match="run_schema_invalid"):
        install_run_schema(home, backup_output=tmp_path / "forbidden-backup")
    assert not (tmp_path / "forbidden-backup").exists()
    with open_workspace(home) as workspace:
        assert "\n".join(workspace.state.iterdump()) == before


def _tree_snapshot(root: Path) -> dict[str, str | None]:
    return {
        str(path.relative_to(root)): (
            hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
        )
        for path in root.rglob("*")
    }


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE TABLE run_details_request_hash(unrelated TEXT)",
        "CREATE TABLE RUN_DETAILS_REQUEST_HASH(unrelated TEXT)",
        "CREATE INDEX backtest_requests ON issuers(name)",
        "CREATE INDEX BACKTEST_REQUESTS ON issuers(name)",
    ],
    ids=[
        "table-index-name",
        "table-index-name-ascii-case",
        "index-table-name",
        "index-table-name-ascii-case",
    ],
)
def test_native_cross_type_name_collision_leaves_all_stores_unchanged(
    tmp_path: Path, ddl: str
) -> None:
    from tests.application.test_storage_cli import run_cli  # noqa: PLC0415

    home = tmp_path / "home"
    initialized = run_cli("init", home=home)
    assert initialized.returncode == 0, initialized.stderr
    backups = tmp_path / "backups"
    backups.mkdir()
    output = backups / "pre-install"
    with open_workspace(home, writable=True) as workspace:
        workspace.state.execute(ddl)
        workspace.state.commit()
        state_before = "\n".join(workspace.state.iterdump())
        assert workspace.strategies is not None
        private_before = "\n".join(workspace.strategies.iterdump())
        assert workspace.state.execute("SELECT * FROM storage_operations").fetchall() == []
    # Closed-store bytes plus all directory entries cover state/private/market,
    # raw files, default and explicit backups, and any new SQLite journal files.
    before = _tree_snapshot(tmp_path)
    rejected = run_cli("db", "run-install", "--backup-output", str(output), home=home)
    after = _tree_snapshot(tmp_path)
    with open_workspace(home) as workspace:
        state_after = "\n".join(workspace.state.iterdump())
        assert workspace.strategies is not None
        private_after = "\n".join(workspace.strategies.iterdump())
        operations = workspace.state.execute("SELECT * FROM storage_operations").fetchall()
    assert rejected.returncode == 1, rejected.stdout
    assert rejected.stdout == ""
    assert (after, state_after, private_after, operations) == (
        before,
        state_before,
        private_before,
        [],
    )
    assert not output.exists()
    assert json.loads(rejected.stderr)["error"].split(":", 1)[0] == "run_schema_invalid"


@pytest.mark.parametrize(
    "ddl",
    [
        'CREATE INDEX "bac\u212atest_requests" ON issuers(name);',
        (
            'CREATE TABLE "bac\u212atest_requests"(unrelated TEXT);'
            "INSERT INTO \"bac\u212atest_requests\" VALUES ('preserved');"
            'CREATE INDEX foreign_probe ON "bac\u212atest_requests"(unrelated);'
        ),
    ],
    ids=["occupied-name", "owning-table"],
)
def test_native_sqlite_distinct_unicode_objects_survive_install_and_restore(
    tmp_path: Path, ddl: str
) -> None:
    from aegis_alpha.storage.run_schema import inspect_run_schema  # noqa: PLC0415
    from tests.application.test_storage_cli import run_cli  # noqa: PLC0415

    home = tmp_path / "home"
    initialized = run_cli("init", home=home)
    assert initialized.returncode == 0, initialized.stderr
    foreign_name = "bac\u212atest_requests"
    catalog_query = "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name=? OR tbl_name=?"
    with open_workspace(home, writable=True) as workspace:
        workspace.state.executescript(ddl)
        foreign_before = workspace.state.execute(
            catalog_query, (foreign_name, foreign_name)
        ).fetchall()
        state_before = "\n".join(workspace.state.iterdump())
    pre = tmp_path / "pre"
    installed = run_cli("db", "run-install", "--backup-output", str(pre), home=home)
    assert installed.returncode == 0, installed.stderr
    assert json.loads(installed.stdout)["state"] == "complete"
    with open_workspace(home, writable=True) as workspace:
        assert (
            workspace.state.execute(catalog_query, (foreign_name, foreign_name)).fetchall()
            == foreign_before
        )
        assert inspect_run_schema(workspace).state == "complete"
        state_complete = "\n".join(workspace.state.iterdump())
    before_retry = _tree_snapshot(tmp_path)
    retried = run_cli("db", "run-install", home=home)
    assert retried.returncode == 0, retried.stderr
    assert retried.stdout == installed.stdout
    assert _tree_snapshot(tmp_path) == before_retry
    old = tmp_path / "old"
    restore(pre, old)
    with open_workspace(old) as workspace:
        assert inspect_run_schema(workspace).state == "absent"
        assert "\n".join(workspace.state.iterdump()) == state_before
    complete = tmp_path / "complete"
    backup(home, complete)
    fresh = tmp_path / "fresh"
    restore(complete, fresh)
    with open_workspace(fresh) as workspace:
        assert inspect_run_schema(workspace).state == "complete"
        assert "\n".join(workspace.state.iterdump()) == state_complete
    verified = run_cli("db", "verify", home=fresh)
    assert verified.returncode == 0, verified.stderr


def test_native_interruption_between_stores_and_explicit_retry(tmp_path: Path) -> None:
    from aegis_alpha.storage.run_schema import inspect_run_schema  # noqa: PLC0415
    from aegis_alpha.storage.state import get_operation  # noqa: PLC0415
    from tests.application.test_storage_cli import run_cli  # noqa: PLC0415

    home = tmp_path / "home"
    assert run_cli("init", home=home).returncode == 0
    pre = tmp_path / "pre"
    ack_read, ack_write = os.pipe()
    release_read, release_write = os.pipe()
    observer = """
import os, sys
from aegis_alpha.application.cli import main
from aegis_alpha.storage import run_schema, workspace
ack, release = map(int, sys.argv[1:3])
original = run_schema._install_market
def boundary(ws):
    os.write(ack, b'STATE_COMMITTED')
    if os.read(release, 1) != b'R':
        raise RuntimeError('release closed')
    original(ws)
run_schema._install_market = boundary
raise SystemExit(main(sys.argv[3:]))
"""
    try:
        with subprocess.Popen(  # noqa: S603 -- native CLI and observed real commit boundary
            [
                sys.executable,
                "-c",
                observer,
                str(ack_write),
                str(release_read),
                "db",
                "run-install",
                "--home",
                str(home),
                "--backup-output",
                str(pre),
            ],
            pass_fds=(ack_write, release_read),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as child:
            try:
                assert select.select([ack_read], [], [], 30)[0], "missing commit acknowledgement"
                assert os.read(ack_read, 64) == b"STATE_COMMITTED"
                busy = run_cli("doctor", home=home)
                assert busy.returncode == 1
                assert "installation_busy" in json.loads(busy.stderr)["error"]
            finally:
                child.kill()
                stdout, stderr = child.communicate(timeout=30)
            assert child.returncode == -signal.SIGKILL, (stdout, stderr)
            assert stdout == b""
    finally:
        for descriptor in (ack_read, ack_write, release_read, release_write):
            os.close(descriptor)
    with open_workspace(home) as workspace:
        status = inspect_run_schema(workspace)
        assert status.state == "partial"
        assert status.state_installed
        assert not status.market_installed
        original_intent = get_operation(workspace.state, "run-schema-install-v1")
        assert original_intent is not None
    denied = run_cli("db", "recover", home=home)
    assert denied.returncode == 1
    assert "run_schema_incomplete" in json.loads(denied.stderr)["error"]
    retry = run_cli("db", "run-install", "--backup-output", str(pre), home=home)
    assert retry.returncode == 0, retry.stderr
    assert json.loads(retry.stdout)["state"] == "complete"
    assert run_cli("db", "run-install", home=home).stdout == retry.stdout
    with open_workspace(home) as workspace:
        completed = get_operation(workspace.state, "run-schema-install-v1")
        assert completed is not None
        assert completed["created_at_us"] == original_intent["created_at_us"]
        assert completed["request_hash"] == original_intent["request_hash"]
        assert completed["phase"] == "COMPLETED"
        assert (
            workspace.state.execute(
                "SELECT count(*) FROM storage_operations WHERE kind='run-schema-install'"
            ).fetchone()[0]
            == 1
        )
    assert run_cli("doctor", home=home).returncode == 0


def test_one_admission_spans_closed_market_backup_and_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from aegis_alpha.storage import backup as backup_owner  # noqa: PLC0415
    from aegis_alpha.storage import run_schema  # noqa: PLC0415
    from aegis_alpha.storage.locks import file_lock  # noqa: PLC0415

    home = tmp_path / "home"
    initialize(home)
    copied = backup_owner._copy_file  # noqa: SLF001 -- observe actual closed-file copy
    install = run_schema._install_state  # noqa: SLF001 -- observe actual DDL boundary
    phases = []

    def retained() -> None:
        for name in (
            ".storage.lock",
            "state.sqlite3.lock",
            "strategies.sqlite3.lock",
            "market.duckdb.lock",
        ):
            with pytest.raises(RuntimeError, match="installation_busy"), file_lock(home / name):
                pytest.fail("maintenance released a store lock")

    def copy(source: Path, target: Path) -> dict[str, object]:
        if source == home / "market.duckdb":
            retained()
            phases.append("closed_market_copy")
        return copied(source, target)

    def state(workspace: object) -> None:
        from aegis_alpha.storage.workspace import Workspace  # noqa: PLC0415

        assert isinstance(workspace, Workspace)
        retained()
        assert workspace.market.execute("SELECT 1").fetchone() == (1,)
        phases.append("reopened_market_install")
        install(workspace)

    monkeypatch.setattr(backup_owner, "_copy_file", copy)
    monkeypatch.setattr(run_schema, "_install_state", state)
    assert run_schema.install_run_schema(home)["state"] == "complete"
    assert phases == ["closed_market_copy", "reopened_market_install"]


def _native_close_boundary(
    home: Path, arguments: tuple[str, ...], replace: Callable[[], None]
) -> subprocess.CompletedProcess[bytes]:
    from tests.application.test_storage_cli import run_cli  # noqa: PLC0415

    ack_read, ack_write = os.pipe()
    release_read, release_write = os.pipe()
    observer = """
import os, sys
import duckdb
from aegis_alpha.application.cli import main
from aegis_alpha.storage import workspace
ack, release = map(int, sys.argv[1:3])
original = workspace.market_connect
class Observed:
    def __init__(self, connection):
        self.connection = connection
        self.closed = False
    def __getattr__(self, name):
        return getattr(self.connection, name)
    def close(self):
        self.connection.close()
        if not self.closed:
            self.closed = True
            try:
                self.connection.execute('SELECT 1')
            except duckdb.ConnectionException:
                pass
            else:
                raise AssertionError('real market connection is still open')
            os.write(ack, b'MARKET_CLOSED')
            if os.read(release, 1) != b'R':
                raise RuntimeError('release closed')
def connect(*args, **kwargs):
    workspace.market_connect = original
    return Observed(original(*args, **kwargs))
workspace.market_connect = connect
raise SystemExit(main(sys.argv[3:]))
"""
    try:
        with subprocess.Popen(  # noqa: S603 -- native CLI with real-close acknowledgement
            [
                sys.executable,
                "-c",
                observer,
                str(ack_write),
                str(release_read),
                "--home",
                str(home),
                *arguments,
            ],
            pass_fds=(ack_write, release_read),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as child:
            try:
                assert select.select([ack_read], [], [], 30)[0], "missing close acknowledgement"
                assert os.read(ack_read, 64) == b"MARKET_CLOSED"
                busy = run_cli("doctor", home=home)
                assert busy.returncode == 1
                assert "installation_busy" in json.loads(busy.stderr)["error"]
                replace()
                os.write(release_write, b"R")
                stdout, stderr = child.communicate(timeout=30)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.communicate(timeout=30)
            return subprocess.CompletedProcess(child.args, child.returncode, stdout, stderr)
    finally:
        for descriptor in (ack_read, ack_write, release_read, release_write):
            os.close(descriptor)


@pytest.mark.parametrize("operation", ["backup", "run-install"])
@pytest.mark.parametrize("replacement", ["older", "clone"])
def test_native_real_close_replacement_cannot_complete_backup_or_start_install(
    tmp_path: Path, operation: str, replacement: str
) -> None:
    from aegis_alpha.storage.import_document import parse_import  # noqa: PLC0415
    from aegis_alpha.storage.publication import publish_document  # noqa: PLC0415
    from aegis_alpha.storage.run_schema import inspect_run_schema  # noqa: PLC0415
    from tests.storage.test_publication import document  # noqa: PLC0415

    home, old, output = tmp_path / "home", tmp_path / "old", tmp_path / "output"
    initialize(home)
    backup(home, old)
    path, incoming = home / "market.duckdb", tmp_path / "incoming.duckdb"
    with open_workspace(home, writable=True) as workspace:
        publish_document(workspace, parse_import(document()))
        info = workspace.market.execute("SELECT * FROM store_info").fetchall()
        before_state = "\n".join(workspace.state.iterdump())
    before = _tree_snapshot(home)
    admitted = path.stat()
    observed = []

    def replace() -> None:
        shutil.copyfile(old / "market.duckdb" if replacement == "older" else path, incoming)
        incoming.chmod(0o600)
        observed.append((incoming.stat(), incoming.read_bytes()))
        incoming.replace(path)

    option = "--output" if operation == "backup" else "--backup-output"
    result = _native_close_boundary(home, ("db", operation, option, str(output)), replace)
    assert result.returncode == 1, (result.stdout, result.stderr)
    assert result.stdout == b""
    assert "market file changed" in json.loads(result.stderr)["error"]
    assert len(observed) == 1
    foreign, payload = observed[0]
    assert not os.path.samestat(admitted, foreign)
    assert os.path.samestat(foreign, path.stat())
    assert path.read_bytes() == payload
    assert not (output / "backup.json").exists()
    after = _tree_snapshot(home)
    assert {k: v for k, v in after.items() if k != path.name} == {
        k: v for k, v in before.items() if k != path.name
    }
    with open_workspace(home) as workspace:
        assert "\n".join(workspace.state.iterdump()) == before_state
        assert inspect_run_schema(workspace).state == "absent"
        assert workspace.market.execute("SELECT * FROM store_info").fetchall() == info
        assert workspace.market.execute("SELECT count(*) FROM market_generations").fetchone() == (
            0 if replacement == "older" else 1,
        )
