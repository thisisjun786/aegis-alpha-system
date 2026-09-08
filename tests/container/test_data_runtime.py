"""Data-stack mount, environment and scheduler contracts; never start services."""

from __future__ import annotations

import configparser
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from aegis_alpha.application import cli
from aegis_alpha.compute_resources import compute_lock_path

ROOT = Path(__file__).resolve().parents[2]
CLIENTS = ("aas", "collector", "admin")
MAPPINGS = {
    "AAS_DATA_CONFIG": "/config/runtime.json",
    "AAS_COLLECTION_CONFIG": "/config/collection.json",
    "AAS_COLLECTION_STATE": "/state/collection",
    "AAS_COMPUTE_LOCK_FILE": "/state/compute.lock",
}


def _config() -> dict[str, Any]:
    return json.loads((ROOT / "docker-compose.data.yml").read_text())


def _mounts(service: dict[str, Any]) -> dict[str, dict[str, Any]]:
    mounts = service["volumes"]
    assert len({mount["target"] for mount in mounts}) == len(mounts)
    return {mount["target"]: mount for mount in mounts}


@pytest.mark.parametrize("name", CLIENTS)
def test_clients_keep_explicit_environment_and_runtime_restrictions(name: str) -> None:
    service = _config()["services"][name]
    assert service["image"].startswith("${AAS_DATA_IMAGE:?")
    assert service["pull_policy"] == "never"
    assert service["user"] == "${AAS_UID:?AAS_UID is required}:${AAS_GID:?AAS_GID is required}"
    for key, value in MAPPINGS.items():
        assert service["environment"][key] == value
    for key in ("AAS_HOST_CPU_LIMIT", "AAS_HOST_MEMORY_LIMIT_BYTES"):
        assert service["environment"][key].startswith("${" + key + ":?")
    assert service["read_only"] is True
    assert service["cap_drop"] == ["ALL"]
    assert service["security_opt"] == ["no-new-privileges:true"]
    assert service["tmpfs"] == ["/tmp:rw,nosuid,nodev,size=256m,mode=1777"]  # noqa: S108 -- isolated container tmpfs
    assert service["cpus"].startswith("${AAS_CONTAINER_CPU_LIMIT:?")
    assert service["mem_limit"].startswith("${AAS_CONTAINER_MEMORY_BYTES:?")
    assert service["pids_limit"] > 0
    assert service["restart"] == "no"
    assert service["stop_signal"] == "SIGINT"
    assert service["stop_grace_period"] == "5m"
    assert service["labels"]["aas.installation"].startswith("${AAS_INSTALL_ID:?")
    assert service["labels"]["aas.role"] == name
    for forbidden in (
        "build",
        "depends_on",
        "ports",
        "privileged",
        "cap_add",
        "env_file",
        "container_name",
    ):
        assert forbidden not in service


def test_existing_database_attach_is_external_and_fresh_database_is_opt_in() -> None:
    config = _config()
    assert set(config["services"]) == {*CLIENTS, "postgres"}
    assert config["services"]["admin"]["profiles"] == ["maintenance"]
    database = config["services"]["postgres"]
    assert database["profiles"] == ["database"]
    native = json.loads((ROOT / "docker-compose.yml").read_text())["services"]
    assert set(native) == {"aas"}
    assert "@sha256:" in database["image"]
    assert database["pull_policy"] == "never"
    assert database["environment"]["POSTGRES_DB"] == "postgres"
    assert database["environment"]["POSTGRES_PASSWORD_FILE"] == "/run/secrets/postgres-password"  # noqa: S105 -- path only
    assert "POSTGRES_PASSWORD" not in database["environment"]
    assert "POSTGRES_HOST_AUTH_METHOD" not in database["environment"]
    assert database["networks"] == ["database"]
    assert "ports" not in database
    assert config["networks"]["database"] == {
        "external": True,
        "name": "${AAS_DATABASE_NETWORK:?AAS_DATABASE_NETWORK is required}",
    }
    assert config["volumes"]["postgres-data"] == {
        "external": True,
        "name": "${AAS_POSTGRES_VOLUME:?AAS_POSTGRES_VOLUME is required}",
    }
    assert _mounts(database)["/var/lib/postgresql"]["source"] == "postgres-data"
    assert _mounts(database)["/run/secrets/postgres-password"]["source"].endswith(
        "/secrets/postgres-owner-password"
    )


def test_only_collector_gets_egress_and_provider_credentials() -> None:
    config = _config()
    assert config["networks"]["egress"] == {"driver": "bridge", "internal": False}
    assert config["services"]["collector"]["networks"] == ["database", "egress"]
    for name in ("aas", "admin"):
        assert config["services"][name]["networks"] == ["database"]
        assert "/run/secrets/fmp.env" not in _mounts(config["services"][name])
    collector = _mounts(config["services"]["collector"])
    assert collector["/run/secrets/fmp.env"]["source"].startswith("${AAS_FMP_CREDENTIAL_FILE:?")
    assert collector["/etc/aegis-trust/owner-authority.json"]["source"].startswith(
        "${AAS_FMP_OWNER_TRUST_FILE:?"
    )
    assert collector["/etc/aegis-trust/owner-authority.json"]["read_only"] is True
    for name in CLIENTS:
        mounts = _mounts(config["services"][name])
        assert mounts["/run/secrets/runtime-url"]["source"].endswith(
            "/secrets/container-runtime-url"
        )
        if name != "admin":
            assert "/run/secrets/admin-url" not in mounts
            assert "/run/secrets/runtime-password" not in mounts
    admin = _mounts(config["services"]["admin"])
    assert admin["/run/secrets/admin-url"]["source"].endswith("/secrets/container-admin-url")
    assert admin["/run/secrets/runtime-password"]["source"].endswith("/secrets/runtime-password")


@pytest.mark.parametrize("name", CLIENTS)
def test_data_children_and_trust_preserve_write_boundaries(name: str) -> None:
    mounts = _mounts(_config()["services"][name])
    data = "/data/aegis-alpha-system"
    assert data not in mounts
    for child in ("raw", "normalized", "owner-receipts"):
        assert mounts[f"{data}/{child}"]["read_only"] is (name != "collector")
    for child in ("canonical", "identity", "artifacts/fmp"):
        assert mounts[f"{data}/{child}"]["read_only"] is True
    assert mounts[f"{data}/artifacts/fmp"]["source"].startswith("${AAS_FMP_USAGE_TRUST_DIR:?")
    if name == "collector":
        for child in ("raw/norgate", "normalized/norgate", "raw/fmp/authority-revocations"):
            assert mounts[f"{data}/{child}"]["read_only"] is True
        assert mounts[f"{data}/raw/fmp/authority-revocations"]["source"].startswith(
            "${AAS_FMP_REVOCATIONS_DIR:?"
        )
    assert mounts["/config"]["read_only"] is True
    assert mounts["/state"]["read_only"] is False


def test_binds_never_create_missing_inputs_or_mount_host_control_sockets() -> None:
    for service in _config()["services"].values():
        for mount in service["volumes"]:
            if mount["type"] == "bind":
                assert mount["bind"] == {"create_host_path": False}
                assert "read_only" in mount
                assert "/docker.sock" not in mount["source"]
                assert "uid" not in mount
                assert "gid" not in mount
                if mount["target"].startswith("/run/secrets/"):
                    assert mount["read_only"] is True


def _unit(filename: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser(interpolation=None)
    config.read(ROOT / "config/systemd" / filename)
    return config


def test_daily_unit_has_bounded_foreground_execution_and_scoped_cleanup() -> None:
    unit = _unit("aas-data-collection.service")["Service"]
    assert unit["Type"] == "exec"  # RuntimeMaxSec must apply; oneshot would ignore it.
    assert unit["ExecStart"] == "%h/.local/bin/aas collect daily"
    assert unit["ExecStopPost"] == "%h/.local/bin/aas --stop-managed"
    assert unit["WorkingDirectory"] == "%h"
    assert unit["RuntimeMaxSec"] == "22h"
    assert unit["TimeoutStopSec"] == "5min"
    assert unit["KillSignal"] == "SIGINT"
    assert unit["KillMode"] == "mixed"
    assert unit["SendSIGKILL"] == "yes"
    assert unit["Restart"] == "no"
    assert unit["UMask"] == "0077"


def test_daily_timer_preserves_utc_schedule_and_catch_up() -> None:
    timer = _unit("aas-data-collection.timer")
    assert timer["Timer"]["OnCalendar"] == "*-*-* 06:30:00 UTC"
    assert timer["Timer"]["Persistent"] == "true"
    assert timer["Timer"]["Unit"] == "aas-data-collection.service"
    assert timer["Install"]["WantedBy"] == "timers.target"


def test_compose_environment_is_consumed_by_actual_cli_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = _config()["services"]["collector"]["environment"]
    for key in MAPPINGS:
        monkeypatch.setenv(key, env[key])
    parser = cli._parser()  # noqa: SLF001 -- inspect actual CLI defaults without executing a job
    assert parser.parse_args(["legacy-db", "status"]).config == Path("/config/runtime.json")
    daily = parser.parse_args(["collect", "daily"])
    assert daily.config == Path("/config/collection.json")
    assert daily.state_root == Path("/state/collection")
    assert compute_lock_path() == Path("/state/compute.lock")


def test_compose_renders_with_explicit_contract_without_touching_services() -> None:
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker CLI unavailable for Compose model validation")
    capability = subprocess.run(  # noqa: S603 -- read-only plugin capability probe
        [docker, "compose", "version", "--short"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    if capability.returncode:
        pytest.skip("Docker Compose plugin unavailable for model validation")
    env = {
        **os.environ,
        "AAS_DATA_IMAGE": "aas-data:fixture",
        "AAS_INSTALL_ROOT": "/fixture/install",
        "AAS_DATA_ROOT": "/fixture/data",
        "AAS_DATABASE_NETWORK": "fixture-private",
        "AAS_POSTGRES_VOLUME": "fixture-postgres",
        "AAS_INSTALL_ID": "fixture-install",
        "AAS_UID": "1000",
        "AAS_GID": "1000",
        "AAS_CONTAINER_CPU_LIMIT": "2.5",
        "AAS_CONTAINER_MEMORY_BYTES": "1073741824",
        "AAS_HOST_CPU_LIMIT": "2.5",
        "AAS_HOST_MEMORY_LIMIT_BYTES": "1073741824",
        "AAS_FMP_CREDENTIAL_FILE": "/fixture/secrets/fmp.env",
        "AAS_FMP_OWNER_TRUST_FILE": "/fixture/owner-trust/owner-authority.json",
        "AAS_FMP_USAGE_TRUST_DIR": "/fixture/usage-trust",
        "AAS_FMP_REVOCATIONS_DIR": "/fixture/revocations",
    }
    command = [
        docker,
        "compose",
        "--env-file",
        "/dev/null",
        "-f",
        str(ROOT / "docker-compose.data.yml"),
        "config",
        "--format",
        "json",
    ]
    result = subprocess.run(  # noqa: S603 -- fixed read-only Compose config command
        command, env=env, check=False, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr
    assert set(json.loads(result.stdout)["services"]) == {"aas", "collector"}
    command[2:2] = ["--profile", "*"]
    result = subprocess.run(  # noqa: S603 -- render opt-in profiles without starting them
        command, env=env, check=True, capture_output=True, text=True, timeout=30
    )
    rendered = json.loads(result.stdout)
    for name in CLIENTS:
        service = rendered["services"][name]
        assert service["user"] == "1000:1000"
        assert float(service["cpus"]) == 2.5  # noqa: PLR2004 -- fractional fixture quota
        assert int(service["mem_limit"]) == 1073741824  # noqa: PLR2004 -- fixture byte ceiling
        for key, value in MAPPINGS.items():
            assert service["environment"][key] == value
    env.pop("AAS_HOST_CPU_LIMIT")
    refused = subprocess.run(  # noqa: S603 -- same read-only model validation
        command, env=env, check=False, capture_output=True, text=True, timeout=30
    )
    assert refused.returncode != 0
    assert "AAS_HOST_CPU_LIMIT" in refused.stderr
