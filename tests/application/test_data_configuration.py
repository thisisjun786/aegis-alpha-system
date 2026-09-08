from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from aegis_alpha.application.data_config import load_data_config, read_secret

_ROOT = Path(__file__).resolve().parents[2]


def test_private_secret_requires_owner_only_mode_and_no_alias(tmp_path: Path) -> None:
    secret = tmp_path / "credential"
    secret.write_text("fixture-value\n")
    secret.chmod(0o644)
    with pytest.raises(ValueError, match="inaccessible"):
        read_secret(secret)
    secret.chmod(0o600)
    assert read_secret(secret) == "fixture-value"
    alias = tmp_path / "alias"
    alias.symlink_to(secret)
    with pytest.raises(ValueError, match=r"regular file|link|symlink"):
        read_secret(alias)


def test_duplicate_profile_and_location_are_refused(tmp_path: Path) -> None:
    path = tmp_path / "profile.json"
    path.write_text('{"version":1,"version":1}')
    with pytest.raises(ValueError, match="duplicate"):
        load_data_config(path)
    location = {"dataset_id": "fixture", "dataset_version": "v1", "root": str(tmp_path)}
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "database_url_file": str(tmp_path / "credential"),
                "dataset_roots": [location, location],
            }
        )
    )
    with pytest.raises(ValueError, match="duplicate"):
        load_data_config(path)


def test_database_configuration_failure_does_not_echo_secret(tmp_path: Path) -> None:
    sentinel = "private-fixture-sentinel"
    secret = tmp_path / "credential"
    secret.write_text("sqlite:///" + sentinel)
    secret.chmod(0o600)
    profile = tmp_path / "profile.json"
    profile.write_text(
        json.dumps({"version": 1, "database_url_file": str(secret), "dataset_roots": []})
    )
    result = subprocess.run(  # noqa: S603 -- fixed interpreter and isolated fixture config
        [sys.executable, "-m", "aegis_alpha", "legacy-db", "status", "--config", str(profile)],
        env={**os.environ, "PYTHONPATH": str(_ROOT / "src")},
        cwd=_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert not result.stdout
    assert sentinel not in result.stderr
    assert "error" in json.loads(result.stderr)


@pytest.mark.parametrize("command", ["status", "install", "adopt"])
def test_malformed_secret_url_port_is_redacted(tmp_path: Path, command: str) -> None:
    sentinel = "private-fixture-sentinel"
    secret = tmp_path / "credential"
    secret.write_text("postgresql+psycopg://test:fixture@localhost:" + sentinel + "/aas")
    secret.chmod(0o600)
    profile = tmp_path / "profile.json"
    profile.write_text(
        json.dumps({"version": 1, "database_url_file": str(secret), "dataset_roots": []})
    )
    arguments = ["legacy-db", command]
    if command == "status":
        arguments += ["--config", str(profile)]
    else:
        arguments += ["--admin-url-file", str(secret), "--database", "app"]
        if command == "install":
            password = tmp_path / "password"
            password.write_text("synthetic-long-test-password-value")
            password.chmod(0o600)
            arguments += ["--runtime-role", "app_role", "--runtime-password-file", str(password)]
        else:
            arguments += ["--snapshot", str(tmp_path), "--sha256", "a" * 64]
    result = subprocess.run(  # noqa: S603 -- fixed interpreter with synthetic private fixture
        [sys.executable, "-m", "aegis_alpha", *arguments],
        env={**os.environ, "PYTHONPATH": str(_ROOT / "src")},
        cwd=_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert not result.stdout
    assert sentinel not in result.stderr
    assert json.loads(result.stderr)["error"] == "invalid database connection configuration"
