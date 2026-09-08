from __future__ import annotations

import os
from dataclasses import replace
from fractions import Fraction
from pathlib import Path

import pytest

from aegis_alpha.application import container_runtime as runtime
from aegis_alpha.compute_resources import VisibleLimits

_IMAGE = "sha256:" + "a" * 64
_INVOCATION = "b" * 32


def _installation(tmp_path: Path) -> runtime.ContainerInstallation:
    environment = {
        "AAS_DATA_IMAGE": _IMAGE,
        "AAS_INSTALL_ROOT": str(tmp_path),
        "AAS_DATA_ROOT": str(tmp_path / "data"),
        "AAS_DATABASE_NETWORK": "aas-private",
        "AAS_POSTGRES_VOLUME": "aas-data",
        "AAS_INSTALL_ID": "test-install",
        "AAS_UID": str(os.getuid()),
        "AAS_GID": str(os.getgid()),
        "AAS_FMP_CREDENTIAL_FILE": str(tmp_path / "fmp.env"),
        "AAS_FMP_OWNER_TRUST_FILE": str(tmp_path / "owner.json"),
        "AAS_FMP_USAGE_TRUST_DIR": str(tmp_path / "trust"),
        "AAS_FMP_REVOCATIONS_DIR": str(tmp_path / "revocations"),
    }
    return runtime.ContainerInstallation(
        "test-install", _IMAGE, tmp_path / "compose.json", environment
    )


def test_argument_boundary_cannot_inject_docker_options(tmp_path: Path) -> None:
    install = _installation(tmp_path)
    args = runtime.launch_arguments(install, ["collect", "run", "--entrypoint", "/bin/sh"], {})
    delimiter = args.index("--")
    assert args[delimiter + 1 :] == ["collector", "collect", "run", "--entrypoint", "/bin/sh"]
    assert "--entrypoint" not in args[:delimiter]
    managed = runtime.launch_arguments(install, ["collect", "run"], {"INVOCATION_ID": _INVOCATION})
    assert managed[managed.index("--name") + 1] == "aas-data-daily-" + _INVOCATION
    assert runtime.launch_arguments(install, ["providers"], {})[-2:] == ["collector", "providers"]
    assert runtime.launch_arguments(install, ["collect", "plan"], {})[-3:] == [
        "collector",
        "collect",
        "plan",
    ]
    assert runtime.launch_arguments(install, ["legacy-db", "install"], {})[-3:] == [
        "admin",
        "legacy-db",
        "install",
    ]
    assert runtime.launch_arguments(install, ["collect", "recover"], {})[-3:] == [
        "aas",
        "collect",
        "recover",
    ]


def test_managed_cleanup_requires_exact_owned_labels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    install = _installation(tmp_path)
    commands = []

    def docker(args: list[str], **_kwargs: object) -> str:
        commands.append(args)
        return "found" if args[0] == "ps" else args[-1] if args[0] == "stop" else ""

    monkeypatch.setattr(runtime, "_docker", docker)
    monkeypatch.setattr(
        runtime, "_inspect", lambda _name: {"Config": {"Labels": {"aas.installation": "other"}}}
    )
    with pytest.raises(ValueError, match="unowned"):
        runtime.stop_managed(install, {"INVOCATION_ID": _INVOCATION})
    assert all(command[0] != "stop" for command in commands)
    monkeypatch.setattr(
        runtime,
        "_inspect",
        lambda _name: {
            "Id": "d" * 64,
            "Config": {
                "Labels": {
                    "aas.installation": install.installation_id,
                    "aas.invocation": _INVOCATION,
                    "aas.role": "collector",
                }
            },
        },
    )
    assert runtime.stop_managed(install, {"INVOCATION_ID": _INVOCATION})["stopped"] is True
    assert commands[-1][-1] == "d" * 64
    with pytest.raises(ValueError, match="invocation"):
        runtime.stop_managed(install, {})


def test_probe_host_pid_bounds_are_propagated_and_probe_is_reaped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    install = _installation(tmp_path)
    calls = []
    cid = "c" * 64

    def docker(args: list[str], **_kwargs: object) -> str:
        calls.append(args)
        return cid if args[0] == "run" else ""

    monkeypatch.setattr(runtime, "_docker", docker)
    monkeypatch.setattr(runtime, "_inspect", lambda _id: {"State": {"Running": True, "Pid": 123}})

    def limits(pid: int) -> VisibleLimits:
        assert pid == 123  # noqa: PLR2004 -- known fake host PID
        return VisibleLimits(Fraction(3, 2), 10000000)

    monkeypatch.setattr(runtime, "inspect_visible_limits", limits)
    values = runtime.host_environment(install)
    assert values["AAS_HOST_CPU_LIMIT"] == "3/2"
    assert values["AAS_CONTAINER_CPU_LIMIT"] == "1.5"
    assert values["AAS_HOST_MEMORY_LIMIT_BYTES"] == "10000000"
    assert calls[-1] == ["stop", "--time", "1", cid]
    assert calls[0][calls[0].index("--network") : calls[0].index("--network") + 2] == [
        "--network",
        "none",
    ]


def test_invalid_manifest_identity_and_mount_paths_are_refused(tmp_path: Path) -> None:
    install = _installation(tmp_path)
    with pytest.raises(ValueError, match="immutable"):
        replace(install, image="aas:latest")
    with pytest.raises(ValueError, match="mount paths"):
        replace(install, environment={**install.environment, "AAS_DATA_ROOT": "../outside"})
    with pytest.raises(ValueError, match="UID/GID"):
        replace(install, environment={**install.environment, "AAS_UID": "999999"})
    with pytest.raises(ValueError, match="missing"):
        replace(install, environment={"AAS_DATA_IMAGE": _IMAGE, "AAS_INSTALL_ID": "test-install"})
