from __future__ import annotations

import json
import os
from pathlib import Path

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
