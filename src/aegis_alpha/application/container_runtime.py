from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from uuid import uuid4

from aegis_alpha.application.data_config import _pairs, _read
from aegis_alpha.application.installation_lock import installation_admission
from aegis_alpha.compute_resources import inspect_visible_limits

_ALLOWED_ENV = frozenset(
    {
        "AAS_DATA_IMAGE",
        "AAS_INSTALL_ROOT",
        "AAS_DATA_ROOT",
        "AAS_DATABASE_NETWORK",
        "AAS_POSTGRES_VOLUME",
        "AAS_INSTALL_ID",
        "AAS_UID",
        "AAS_GID",
        "AAS_FMP_CREDENTIAL_FILE",
        "AAS_FMP_OWNER_TRUST_FILE",
        "AAS_FMP_USAGE_TRUST_DIR",
        "AAS_FMP_REVOCATIONS_DIR",
        "AAS_POSTGRES_MEMORY_BYTES",
    }
)
_HASH = re.compile(r"sha256:[0-9a-f]{64}")
_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")
_PATH_ENV = frozenset(
    {
        "AAS_INSTALL_ROOT",
        "AAS_DATA_ROOT",
        "AAS_FMP_CREDENTIAL_FILE",
        "AAS_FMP_OWNER_TRUST_FILE",
        "AAS_FMP_USAGE_TRUST_DIR",
        "AAS_FMP_REVOCATIONS_DIR",
    }
)


@dataclass(frozen=True, slots=True)
class ContainerInstallation:
    installation_id: str
    image: str
    compose_file: Path
    environment: Mapping[str, str]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.installation_id, str)
            or _IDENTIFIER.fullmatch(self.installation_id) is None
        ):
            raise ValueError("invalid installation identity")
        if not isinstance(self.image, str) or _HASH.fullmatch(self.image) is None:
            raise ValueError("installed image must be an immutable local digest")
        if (
            not isinstance(self.compose_file, Path)
            or not self.compose_file.is_absolute()
            or ".." in self.compose_file.parts
        ):
            raise ValueError("installed Compose path must be absolute")
        _validate_environment(self.environment, self.installation_id, self.image)
        object.__setattr__(self, "environment", MappingProxyType(dict(self.environment)))


def _validate_environment(environment: Mapping[str, str], installation_id: str, image: str) -> None:
    if not isinstance(environment, Mapping) or set(environment) - _ALLOWED_ENV:
        raise ValueError("installation contains unknown environment fields")
    if not (_ALLOWED_ENV - {"AAS_POSTGRES_MEMORY_BYTES"}) <= environment.keys():
        raise ValueError("installation is missing required environment fields")
    if any(
        not isinstance(value, str) or not value or any(c in value for c in "\x00\r\n")
        for value in environment.values()
    ):
        raise ValueError("installation environment values must be nonempty single lines")
    if (
        environment.get("AAS_INSTALL_ID") != installation_id
        or environment.get("AAS_DATA_IMAGE") != image
    ):
        raise ValueError("installation/image identity differs from its environment")
    for name in _PATH_ENV:
        path = Path(environment[name])
        if not path.is_absolute() or ".." in path.parts:
            raise ValueError("installation mount paths must be absolute without traversal")
    for name, expected in (("AAS_UID", os.getuid()), ("AAS_GID", os.getgid())):
        if environment[name] != str(expected):
            raise ValueError("installation UID/GID must match the invoking host user")


def load_installation(path: Path) -> ContainerInstallation:
    value = json.loads(_read(path, 1024 * 1024, private=False), object_pairs_hook=_pairs)
    if not isinstance(value, dict) or set(value) != {
        "version",
        "installation_id",
        "image",
        "compose_file",
        "environment",
    }:
        raise ValueError("invalid container installation manifest")
    if type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("unsupported container installation version")
    if not isinstance(value["compose_file"], str):
        raise TypeError("Compose file must be a path")
    return ContainerInstallation(
        value["installation_id"], value["image"], Path(value["compose_file"]), value["environment"]
    )


def _docker(arguments: Sequence[str], *, optional: bool = False) -> str:
    result = subprocess.run(["docker", *arguments], capture_output=True, text=True, check=False)  # noqa: S603,S607 -- fixed local Docker executable; arguments are structured
    if result.returncode and not optional:
        raise RuntimeError("local Docker operation failed; inspect the installation and daemon")
    return result.stdout.strip()


def require_local_host(environ: Mapping[str, str]) -> None:
    if Path("/.dockerenv").exists() or Path("/run/.containerenv").exists():
        raise ValueError("launch this installation wrapper on the Docker host")
    explicit = environ.get("DOCKER_HOST")
    if explicit is not None and not explicit.startswith("unix://"):
        raise ValueError("this installation requires a local Docker daemon")
    endpoint = json.loads(
        _docker(["context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"])
    )
    if not isinstance(endpoint, str) or not endpoint.startswith("unix://"):
        raise ValueError("this installation requires a local Docker context")
    options = json.loads(_docker(["info", "--format", "{{json .SecurityOptions}}"]))
    if not isinstance(options, list) or any(
        "rootless" in str(item) or "userns" in str(item) for item in options
    ):
        raise ValueError("this installation requires matching host/container UID ownership")


def _inspect(identifier: str) -> dict[str, object]:
    value = json.loads(_docker(["inspect", identifier]))
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise RuntimeError("Docker identity inspection failed")
    return value[0]


def host_environment(installation: ContainerInstallation) -> dict[str, str]:
    identifier = _docker(
        [
            "run",
            "--detach",
            "--rm",
            "--network",
            "none",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--label",
            "aas.installation=" + installation.installation_id,
            "--label",
            "aas.role=resource-probe",
            "--entrypoint",
            "/bin/sleep",
            installation.image,
            "60",
        ]
    )
    if re.fullmatch(r"[0-9a-f]{64}", identifier) is None:
        raise RuntimeError("Docker returned an invalid probe identity")
    try:
        before = _inspect(identifier)
        state = before["State"]
        if (
            not isinstance(state, dict)
            or state.get("Running") is not True
            or type(state.get("Pid")) is not int
        ):
            raise RuntimeError("resource probe is not running")
        pid = state.get("Pid")
        if not isinstance(pid, int) or pid < 1:
            raise RuntimeError("resource probe has no host PID")
        limits = inspect_visible_limits(pid)
        after = _inspect(identifier)
        if (
            not isinstance(after["State"], dict)
            or after["State"].get("Pid") != pid
            or after["State"].get("Running") is not True
        ):
            raise RuntimeError("resource probe changed during host inspection")
        return {
            **limits.to_host_environment(),
            "AAS_CONTAINER_CPU_LIMIT": str(float(limits.cpu_limit)),
            "AAS_CONTAINER_MEMORY_BYTES": str(limits.memory_headroom_bytes),
        }
    finally:
        _docker(["stop", "--time", "1", identifier], optional=True)


def _invocation(environ: Mapping[str, str], *, required: bool = False) -> str:
    value = environ.get("INVOCATION_ID")
    if value is None and not required:
        return uuid4().hex
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{32}", value) is None:
        raise ValueError("managed cleanup requires its exact systemd invocation identity")
    return value


def stop_managed(
    installation: ContainerInstallation, environ: Mapping[str, str]
) -> dict[str, object]:
    invocation = _invocation(environ, required=True)
    name = "aas-data-daily-" + invocation
    found = _docker(["ps", "--all", "--filter", "name=^/" + name + "$", "--format", "{{.ID}}"])
    if not found:
        return {"stopped": False, "reason": "already_absent"}
    info = _inspect(name)
    config = info["Config"]
    labels = config.get("Labels") if isinstance(config, dict) else None
    if (
        not isinstance(labels, dict)
        or labels.get("aas.installation") != installation.installation_id
        or labels.get("aas.invocation") != invocation
        or labels.get("aas.role") != "collector"
    ):
        raise ValueError("refusing to stop an unowned container")
    container_id = info.get("Id")
    if not isinstance(container_id, str) or re.fullmatch(r"[0-9a-f]{64}", container_id) is None:
        raise ValueError("managed container has no immutable identity")
    stopped = _docker(["stop", "--time", "30", container_id], optional=True)
    if stopped != container_id:
        remaining = _docker(
            ["ps", "--all", "--filter", "id=" + container_id, "--format", "{{.ID}}"]
        )
        if remaining:
            raise RuntimeError("managed container could not be stopped")
        return {"stopped": False, "reason": "already_absent"}
    return {"stopped": True, "container": container_id}


def launch_arguments(
    installation: ContainerInstallation, argv: list[str], environ: Mapping[str, str]
) -> list[str]:
    args = argv or ["status"]
    daily = args[:2] == ["collect", "daily"]
    needs_provider_mounts = args[:1] == ["providers"] or args[:2] in (
        ["collect", "run"],
        ["collect", "daily"],
        ["collect", "plan"],
    )
    service = "collector" if needs_provider_mounts else "aas"
    if args[:2] in (["legacy-db", "install"], ["legacy-db", "adopt"]):
        service = "admin"
    invocation = _invocation(environ)
    managed_collector = service == "collector" and environ.get("INVOCATION_ID") is not None
    name = ("aas-data-daily-" if daily or managed_collector else "aas-data-cli-") + invocation
    return [
        "docker",
        "compose",
        "--project-directory",
        str(installation.compose_file.parent),
        "--file",
        str(installation.compose_file),
        "run",
        "--rm",
        "--no-deps",
        "--name",
        name,
        "--label",
        "aas.invocation=" + invocation,
        "--",
        service,
        *args,
    ]


def _run_attached(arguments: list[str], environment: dict[str, str]) -> int:
    with subprocess.Popen(arguments, env=environment, start_new_session=True) as child:  # noqa: S603 -- fixed Docker command from validated launcher
        previous = {}

        def forward(signum: int, _frame: object) -> None:
            if child.poll() is None:
                os.killpg(child.pid, signum)

        try:
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous[signum] = signal.signal(signum, forward)
            code = child.wait()
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
        return code if code >= 0 else 128 - code


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    path = Path(
        os.environ.get(
            "AAS_INSTALL_CONFIG",
            str(Path.home() / ".local/share/aegis-alpha/container-install.json"),
        )
    )
    try:
        installation = load_installation(path)
        require_local_host(os.environ)
        if arguments == ["--stop-managed"]:
            print(json.dumps(stop_managed(installation, os.environ)))  # noqa: T201 -- host wrapper JSON
            return 0
        root = Path(installation.environment["AAS_INSTALL_ROOT"])
        with installation_admission(root, update=False):
            current = load_installation(path)
            if Path(current.environment["AAS_INSTALL_ROOT"]) != root:
                raise ValueError("installation root changed during admission")  # noqa: TRY301 -- reject stale admission before any container launch
            environment = {**os.environ, **current.environment, **host_environment(current)}
            return _run_attached(launch_arguments(current, arguments, environment), environment)
    except (ValueError, OSError, RuntimeError, TypeError):
        print(  # noqa: T201 -- no secret-bearing subprocess diagnostics
            json.dumps({"error": "container launch failed; check installation and local Docker"}),
            file=sys.stderr,
        )
        return 1
    return 0
