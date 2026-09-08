from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def run_cli(*args: str, home: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 -- fixed interpreter, temporary synthetic home
        [sys.executable, "-m", "aegis_alpha", *args],
        env={**os.environ, "AAS_HOME": str(home), "PYTHONPATH": str(_ROOT / "src")},
        cwd=home.parent,
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )


def test_init_doctor_repeat_without_database_server(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    result = run_cli("init", home=home)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["initialized"] is True
    result = run_cli("doctor", home=home)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["home"] == str(home)
    assert report["strategy_versions"] == 0
    assert report["docker_required"] is False
    assert run_cli("init", home=home).returncode == 0
    assert run_cli("strategy", "list", home=home).returncode == 0


def test_uninitialized_doctor_no_autoinstall(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    result = run_cli("doctor", home=home)
    assert result.returncode == 1
    assert "error" in json.loads(result.stderr)
    assert not home.exists()


def test_explicit_nested_home_wins(tmp_path: Path) -> None:
    env_home, explicit = tmp_path / "env", tmp_path / "explicit"
    result = run_cli("init", "--home", str(explicit), home=env_home)
    assert result.returncode == 0, result.stderr
    assert explicit.exists()
    assert not env_home.exists()


def test_restore_never_defaults_over_current_home(tmp_path: Path) -> None:
    result = run_cli("db", "restore", "--backup", str(tmp_path / "missing"), home=tmp_path / "aas")
    assert result.returncode == 1
    assert "explicit --home" in json.loads(result.stderr)["error"]


@pytest.mark.parametrize("name", ["init", "doctor", "db", "data", "strategy"])
def test_native_help_without_site_packages(name: str) -> None:
    result = subprocess.run(  # noqa: S603 -- verify dependency-free argument parser
        [sys.executable, "-S", "-m", "aegis_alpha", name, "--help"],
        env={**os.environ, "PYTHONPATH": str(_ROOT / "src")},
        cwd=_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_source_catalog_cli_roundtrip(tmp_path: Path) -> None:
    import hashlib  # noqa: PLC0415 -- synthetic source fixture
    import sqlite3  # noqa: PLC0415

    home = tmp_path / "aas"
    source = tmp_path / "snapshot"
    with sqlite3.connect(source) as origin:
        origin.execute("CREATE TABLE catalog (name TEXT, value INTEGER)")
        origin.execute("INSERT INTO catalog VALUES ('synthetic',7)")
    source.chmod(0o600)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    assert run_cli("init", home=home).returncode == 0
    result = run_cli(
        "db", "source-import", str(source), "--id", "synthetic", "--sha256", digest, home=home
    )
    assert result.returncode == 0, result.stderr
    result = run_cli("db", "sources", home=home)
    assert json.loads(result.stdout)["sources"][0]["source_id"] == "synthetic"
    result = run_cli(
        "db",
        "source-read",
        "--source",
        "synthetic",
        "--table",
        "catalog",
        "--limit",
        "1",
        home=home,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["rows"] == [{"name": "synthetic", "value": 7}]
    assert json.loads(run_cli("strategy", "list", home=home).stdout)["strategies"] == []
    result = run_cli("db", "verify", home=home)
    assert json.loads(result.stdout)["source_library"] == {"sources": 1, "tables": 1, "rows": 1}
