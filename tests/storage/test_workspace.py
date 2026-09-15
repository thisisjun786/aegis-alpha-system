from __future__ import annotations

import json
import os
import shutil
from collections.abc import Callable
from pathlib import Path

import duckdb
import pytest

from aegis_alpha.storage.locks import file_lock
from aegis_alpha.storage.paths import load_paths, resolve_home
from aegis_alpha.storage.sqlite import connect
from aegis_alpha.storage.workspace import initialize, open_workspace, write_json

_PRIVATE_FILE = 0o600
_PRIVATE_DIRECTORY = 0o700


def test_empty_install_idempotent_private_and_restart(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    result = initialize(home)
    before = json.loads((home / "installation.json").read_text())
    assert result["strategies_seeded"] is False
    assert initialize(home) == result
    assert json.loads((home / "installation.json").read_text()) == before
    with open_workspace(home) as workspace:
        assert workspace.doctor()["strategy_versions"] == 0
        assert workspace.doctor()["market_generations"] == 0
        assert workspace.state.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    for name in (
        "runtime.json",
        "installation.json",
        "state.sqlite3",
        "strategies.sqlite3",
        "market.duckdb",
    ):
        assert (home / name).stat().st_mode & 0o777 == _PRIVATE_FILE
    assert home.stat().st_mode & 0o777 == _PRIVATE_DIRECTORY


def test_home_precedence_and_relative_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "env"
    monkeypatch.setenv("AAS_HOME", str(home))
    assert resolve_home() == home
    assert resolve_home(tmp_path / "explicit") == tmp_path / "explicit"
    initialize(home)
    monkeypatch.chdir(tmp_path)
    assert load_paths(home).market == home / "market.duckdb"


def test_second_owner_and_symlink_root_refused(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home), pytest.raises(RuntimeError, match="installation_busy"):
        initialize(home)
    alias = tmp_path / "alias"
    alias.symlink_to(home, target_is_directory=True)
    with pytest.raises(ValueError, match=r"created|aliases|opened"):
        initialize(alias)


def test_foreign_store_and_missing_state_not_reinitialized(tmp_path: Path) -> None:
    home, other = tmp_path / "one", tmp_path / "two"
    initialize(home)
    initialize(other)
    (home / "strategies.sqlite3").write_bytes((other / "strategies.sqlite3").read_bytes())
    with pytest.raises(ValueError, match="identity"):
        initialize(home)
    (other / "state.sqlite3").unlink()
    with pytest.raises(ValueError, match="missing"):
        initialize(other)
    assert not (other / "state.sqlite3").exists()


def test_external_store_lock_and_installation_identity(tmp_path: Path) -> None:
    first, second = tmp_path / "one", tmp_path / "two"
    initialize(first)
    second.mkdir(mode=0o700)
    write_json(
        second / "runtime.json",
        {"format_version": 1, "paths": {"market": str(first / "market.duckdb")}},
    )
    with (
        file_lock(first / "market.duckdb.lock"),
        pytest.raises(RuntimeError, match="installation_busy"),
    ):
        initialize(second)
    with pytest.raises(ValueError, match=r"identity|installation"):
        initialize(second)


def test_sqlite_reader_does_not_create_or_write(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        connect(tmp_path / "absent.sqlite3", read_only=True)
    assert not (tmp_path / "absent.sqlite3").exists()


def test_config_cannot_alias_stores_or_git_checkout(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    config = {"format_version": 1, "paths": {"market": "state.sqlite3"}}
    write_json(home / "runtime.json", config)
    with pytest.raises(ValueError, match="distinct"):
        load_paths(home)
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".git").mkdir()
    with pytest.raises(ValueError, match="Git checkout"):
        initialize(repo / "data")


def test_hardlinked_database_refused(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    os.link(home / "state.sqlite3", tmp_path / "alias.sqlite3")
    with pytest.raises(ValueError, match="linked"), open_workspace(home):
        pytest.fail("hardlinked database was admitted")


def test_empty_home_environment_uses_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AAS_HOME", "")
    assert resolve_home() == Path.home() / ".aas"


def test_partial_initialization_resumes_preserving_store_ids(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    receipt = json.loads((home / "installation.json").read_text())
    original = receipt["stores"].copy()
    receipt["phase"] = "initializing"
    del receipt["stores"]["strategies"]
    write_json(home / "installation.json", receipt)
    (home / "strategies.sqlite3").unlink()
    initialize(home)
    resumed = json.loads((home / "installation.json").read_text())
    assert resumed["phase"] == "ready"
    assert resumed["stores"]["state"] == original["state"]
    assert resumed["stores"]["market"] == original["market"]
    assert resumed["stores"]["strategies"]["store_id"] != original["strategies"]["store_id"]


def test_unowned_sqlite_not_adopted_or_reconfigured(tmp_path: Path) -> None:
    import sqlite3  # noqa: PLC0415 -- create a foreign database independently

    home = tmp_path / "aas"
    home.mkdir(mode=0o700)
    path = home / "state.sqlite3"
    foreign = sqlite3.connect(path)
    foreign.execute("CREATE TABLE unrelated(value TEXT)")
    foreign.close()
    before = path.read_bytes()
    with pytest.raises(ValueError, match="unowned"):
        initialize(home)
    assert path.read_bytes() == before


class MarketCloseBoundary:
    """Delegate DB behavior unchanged; invoke a fault only after the real close."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, boundary: Callable[[], None]) -> None:
        self.connection = connection
        self.boundary = boundary
        self.closed = False

    def __getattr__(self, name: str) -> object:
        return getattr(self.connection, name)

    def close(self) -> None:
        self.connection.close()
        if not self.closed:
            self.closed = True
            with pytest.raises(duckdb.ConnectionException):
                self.connection.execute("SELECT 1")
            self.boundary()


@pytest.mark.parametrize("replacement", ["older", "clone"])
@pytest.mark.parametrize("boundary", ["before_maintenance", "real_close"])
def test_backup_rejects_same_store_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replacement: str, boundary: str
) -> None:
    from aegis_alpha.storage.backup import backup, backup_workspace  # noqa: PLC0415
    from aegis_alpha.storage.import_document import parse_import  # noqa: PLC0415
    from aegis_alpha.storage.publication import publish_document  # noqa: PLC0415
    from aegis_alpha.storage.verification import verify_workspace  # noqa: PLC0415
    from tests.storage.test_publication import document  # noqa: PLC0415

    home, old, output = tmp_path / "home", tmp_path / "old", tmp_path / "output"
    initialize(home)
    backup(home, old)
    path = home / "market.duckdb"
    incoming = tmp_path / "incoming.duckdb"
    with open_workspace(home, writable=True) as workspace:
        publish_document(workspace, parse_import(document()))
        assert verify_workspace(workspace)["dataset_versions"] == 1
        workspace.market.execute("CHECKPOINT")
        shutil.copyfile(old / "market.duckdb" if replacement == "older" else path, incoming)
        incoming.chmod(0o600)
        retained = path.stat()
        foreign = incoming.stat()
        original_info = workspace.market.execute("SELECT * FROM store_info").fetchall()
        before = "\n".join(workspace.state.iterdump())
        payload = incoming.read_bytes()
        swaps = []

        def replace() -> None:
            incoming.replace(path)
            swaps.append(path.stat().st_ino)
            assert swaps == [foreign.st_ino]
            for lock in (".storage.lock", "market.duckdb.lock"):
                with pytest.raises(RuntimeError, match="installation_busy"), file_lock(home / lock):
                    pytest.fail("maintenance released admission")

        if boundary == "before_maintenance":
            replace()
        elif boundary == "real_close":
            monkeypatch.setattr(workspace, "market", MarketCloseBoundary(workspace.market, replace))
        with pytest.raises(ValueError, match=r"market .*changed"):
            backup_workspace(workspace, output)
        assert swaps == [foreign.st_ino]
        assert "\n".join(workspace.state.iterdump()) == before
        assert not (output / "backup.json").exists()
    assert (path.stat().st_dev, path.stat().st_ino) == (foreign.st_dev, foreign.st_ino)
    assert path.stat().st_ino != retained.st_ino
    assert path.read_bytes() == payload
    with open_workspace(home) as workspace:
        assert workspace.market.execute("SELECT * FROM store_info").fetchall() == original_info
        if replacement == "older":
            with pytest.raises(ValueError, match="market generation does not exist"):
                verify_workspace(workspace)
        else:
            assert verify_workspace(workspace)["dataset_versions"] == 1


def test_copy_interval_replacement_is_not_reopened(tmp_path: Path) -> None:
    home = tmp_path / "home"
    initialize(home)
    incoming = tmp_path / "incoming.duckdb"
    with open_workspace(home, writable=True) as workspace:
        original = workspace.market
        original.execute("CHECKPOINT")
        shutil.copyfile(workspace.paths.market, incoming)
        incoming.chmod(0o600)
        foreign = incoming.stat()
        payload = incoming.read_bytes()
        with (
            pytest.raises(ValueError, match="market file changed"),
            workspace.checkpointed_market(),
        ):
            incoming.replace(workspace.paths.market)
        assert workspace.market is original
        assert os.path.samestat(foreign, workspace.paths.market.stat())
        assert workspace.paths.market.read_bytes() == payload


@pytest.mark.parametrize("boundary", ["admission", "reopen"])
def test_connection_boundary_replacement_is_not_adopted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    from aegis_alpha.storage import workspace as owner  # noqa: PLC0415

    home, incoming = tmp_path / "home", tmp_path / "incoming.duckdb"
    initialize(home)
    path = home / "market.duckdb"
    shutil.copyfile(path, incoming)
    incoming.chmod(0o600)
    payload, foreign = incoming.read_bytes(), incoming.stat()
    connect_market = owner.market_connect
    opened = []

    def connect_and_replace(
        path: Path, *, read_only: bool = False, resources: dict[str, object] | None = None
    ) -> duckdb.DuckDBPyConnection:
        connection = connect_market(path, read_only=read_only, resources=resources)
        opened.append(connection)
        incoming.replace(path)
        return connection

    if boundary == "admission":
        monkeypatch.setattr(owner, "market_connect", connect_and_replace)
        with pytest.raises(ValueError, match="market file changed"), open_workspace(home):
            pytest.fail("replacement admitted")
    else:
        with open_workspace(home, writable=True) as workspace:
            monkeypatch.setattr(owner, "market_connect", connect_and_replace)
            with (
                pytest.raises(ValueError, match="market file changed"),
                workspace.checkpointed_market(),
            ):
                pass
    assert len(opened) == 1
    with pytest.raises(duckdb.ConnectionException):
        opened[0].execute("SELECT 1")
    assert os.path.samestat(foreign, path.stat())
    assert path.read_bytes() == payload


def test_maintenance_retains_initial_store_identity(tmp_path: Path) -> None:
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True) as workspace:
        workspace.market.execute("UPDATE store_info SET store_id='foreign'")
        with (
            pytest.raises(ValueError, match="market identity changed"),
            workspace.checkpointed_market(),
        ):
            pytest.fail("changed store identity admitted")
        assert workspace.market.execute("SELECT store_id FROM store_info").fetchone() == (
            "foreign",
        )


def test_same_file_maintenance_reopens_after_copy_error_with_resources(tmp_path: Path) -> None:
    from aegis_alpha.storage.backup import backup_workspace  # noqa: PLC0415

    home = tmp_path / "home"
    initialize(home)
    runtime = json.loads((home / "runtime.json").read_text())
    runtime["resources"] = {"threads": 1, "memory_limit": "256MB"}
    write_json(home / "runtime.json", runtime)
    with open_workspace(home, writable=True) as workspace:
        original = workspace.market
        admitted = workspace.paths.market.stat()
        info = original.execute("SELECT * FROM store_info").fetchall()
        settings = original.execute(
            "SELECT current_setting('threads'), current_setting('memory_limit'), "
            "current_setting('enable_external_access')"
        ).fetchone()
        with pytest.raises(RuntimeError, match="copy failed"), workspace.checkpointed_market():
            raise RuntimeError("copy failed")
        assert workspace.market is not original
        assert os.path.samestat(admitted, workspace.paths.market.stat())
        assert workspace.market.execute("SELECT * FROM store_info").fetchall() == info
        assert (
            workspace.market.execute(
                "SELECT current_setting('threads'), current_setting('memory_limit'), "
                "current_setting('enable_external_access')"
            ).fetchone()
            == settings
        )
        assert backup_workspace(workspace, tmp_path / "backup")["backed_up"] is True
        assert os.path.samestat(admitted, workspace.paths.market.stat())
