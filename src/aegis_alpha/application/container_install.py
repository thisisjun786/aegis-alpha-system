from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
import sys
import tarfile
from dataclasses import replace
from pathlib import Path

from aegis_alpha.application.collection_journal import journal_status
from aegis_alpha.application.container_runtime import (
    ContainerInstallation,
    load_installation,
    require_local_host,
)
from aegis_alpha.application.installation_lock import installation_admission
from aegis_alpha.data.descriptor_tree import DescriptorTree

_MANAGED = "# AAS_MANAGED_CONTAINER_V1 "
_MAX_INSTALL_FILE_BYTES = 64 * 1024


def _preflight_owned(path: Path, owner: str) -> None:
    if not path.is_absolute() or ".." in path.parts or path.is_symlink():
        raise ValueError("installation output must be an absolute non-aliased path")
    if path.exists():
        with DescriptorTree.open_path(path.parent) as tree:
            if (_MANAGED + owner).encode() not in tree.read_bytes(
                path.name, max_bytes=_MAX_INSTALL_FILE_BYTES
            ).splitlines()[:2]:
                raise ValueError("refusing to replace an unowned installation file")


def _command(arguments: list[str], *, cwd: Path | None = None, required: bool = True) -> str:
    result = subprocess.run(arguments, cwd=cwd, capture_output=True, text=True, check=False)  # noqa: S603 -- structured local installation commands
    if required and result.returncode:
        raise RuntimeError("installation command failed; existing runtime was preserved")
    return result.stdout.strip()


def _private_directory(path: Path) -> None:
    if not path.is_absolute() or ".." in path.parts or path.is_symlink():
        raise ValueError("installation state must be an absolute non-aliased directory")
    with DescriptorTree.open_path(path) as tree:
        info = os.fstat(tree.descriptor)
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise ValueError("installation state must be private and owned by this user")
    if any((parent / ".git").exists() for parent in (path, *path.parents)):
        raise ValueError("installation state must be outside Git")


def _write_owned(path: Path, payload: bytes, owner: str, *, executable: bool = False) -> None:
    _preflight_owned(path, owner)
    with DescriptorTree.open_path(Path(path.anchor)) as root:
        for index in range(1, len(path.parent.parts)):
            root.mkdir(Path(*path.parent.parts[1 : index + 1]), mode=0o700, exist_ok=True)
    with DescriptorTree.open_path(path.parent) as tree:
        parent = os.fstat(tree.descriptor)
        if parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) & 0o022:
            raise ValueError("installation target parent must be owned and private-writable")
        if tree.exists(path.name):
            existing = tree.read_bytes(path.name, max_bytes=_MAX_INSTALL_FILE_BYTES)
            if existing == payload:
                return
            if (_MANAGED + owner).encode() not in existing.splitlines()[:2]:
                raise ValueError("refusing to replace an unowned installation file")
            backup = path.name + ".previous-" + hashlib.sha256(existing).hexdigest()[:16]
            if not tree.exists(backup):
                with tree.binary_writer(backup, exclusive=True) as handle:
                    handle.write(existing)
                    handle.flush()
                    os.fsync(handle.fileno())
        tree.atomic_write_bytes(path.name, payload)
        with tree.binary_reader(path.name) as handle:
            os.fchmod(handle.fileno(), 0o700 if executable else 0o600)
        tree.fsync_file(path.name)
        tree.fsync_directory()


def _release(repository: Path, state: Path) -> tuple[Path, str]:
    dirty = _command(
        [
            "git",
            "status",
            "--porcelain",
            "--",
            "src",
            "scripts",
            "config/systemd",
            "docker-compose.data.yml",
        ],
        cwd=repository,
    )
    if dirty:
        raise ValueError("commit installation inputs before creating a permanent release")
    revision = _command(["git", "rev-parse", "HEAD"], cwd=repository)
    if len(revision) != 40 or any(c not in "0123456789abcdef" for c in revision):  # noqa: PLR2004 -- full Git object ID
        raise ValueError("repository revision is not an exact commit")
    root = state / "releases" / revision
    marker = root / ".aas-release"
    if root.exists():
        if not marker.is_file() or marker.read_text().strip() != revision:
            raise ValueError("permanent release exists without matching identity")
        return root, revision
    root.mkdir(parents=True, mode=0o700)
    archive = root / "source.tar"
    with archive.open("xb") as handle:
        result = subprocess.run(  # noqa: S603 -- verified local commit and fixed Git command
            ["git", "archive", "--format=tar", revision],  # noqa: S607 -- repository Git toolchain
            cwd=repository,
            stdout=handle,
            stderr=subprocess.PIPE,
            check=False,
        )
    if result.returncode:
        raise RuntimeError("source release archive failed")
    with tarfile.open(archive) as bundle:
        bundle.extractall(root, filter="data")
    archive.unlink()
    marker.write_text(revision + "\n")
    for folder, _directories, files in os.walk(root, topdown=False):
        for name in files:
            path = Path(folder) / name
            if path.is_symlink():
                continue
            with path.open("rb") as handle:
                os.fchmod(handle.fileno(), 0o444)
                os.fsync(handle.fileno())
        descriptor = os.open(folder, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fchmod(descriptor, 0o500)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return root, revision


def install(
    manifest: Path, repository: Path, command_path: Path, *, enable_timer: bool = False
) -> dict[str, object]:
    if command_path != Path.home() / ".local/bin/aas":
        raise ValueError("systemd installation requires the default ~/.local/bin/aas command")
    requested = load_installation(manifest)
    state = Path(requested.environment["AAS_INSTALL_ROOT"])
    _private_directory(state)
    with installation_admission(state, update=True):
        return _install_locked(requested, repository, command_path, enable_timer=enable_timer)


def _install_locked(
    requested: ContainerInstallation, repository: Path, command_path: Path, *, enable_timer: bool
) -> dict[str, object]:
    require_local_host(os.environ)
    state = Path(requested.environment["AAS_INSTALL_ROOT"])
    _private_directory(state)
    active_state = _command(
        ["systemctl", "--user", "is-active", "aas-data-collection.service"], required=False
    )
    if (
        active_state in {"active", "activating", "reloading", "deactivating"}
        or journal_status(state / "state/collection")["active"]
    ):
        raise ValueError("finish the active daily job before updating its installation")
    if (
        _command(["systemctl", "--user", "is-active", "aas-data-collection.timer"], required=False)
        == "active"
    ):
        raise ValueError("stop the daily timer before changing its installation")
    _preflight_owned(command_path, requested.installation_id)
    for name in ("aas-data-collection.service", "aas-data-collection.timer"):
        _preflight_owned(Path.home() / ".config/systemd/user" / name, requested.installation_id)
    if (
        _command(["docker", "image", "inspect", requested.image, "--format", "{{.Id}}"])
        != requested.image
    ):
        raise ValueError("installed image identity is unavailable or mismatched")
    release, revision = _release(repository, state)
    selected = replace(requested, compose_file=release / "docker-compose.data.yml")
    control = state / "container-install.json"
    value = {
        "version": 1,
        "installation_id": selected.installation_id,
        "image": selected.image,
        "compose_file": str(selected.compose_file),
        "environment": dict(selected.environment),
    }
    payload = json.dumps(value, indent=2).encode() + b"\n"
    with DescriptorTree.open_path(state) as tree:
        if tree.exists(control.name):
            previous = load_installation(control)
            if previous.installation_id != selected.installation_id:
                raise ValueError("installation manifest belongs to a different instance")
            old = tree.read_bytes(control.name)
            backup = "container-install.previous-" + hashlib.sha256(old).hexdigest()[:16] + ".json"
            if old != payload and not tree.exists(backup):
                with tree.binary_writer(backup, exclusive=True) as handle:
                    handle.write(old)
                    handle.flush()
                    os.fsync(handle.fileno())
        tree.atomic_write_bytes(control.name, payload)
    wrapper = (
        "#!/usr/bin/env python3\n" + _MANAGED + selected.installation_id + "\n"
        "import os,sys\n"
        + "sys.dont_write_bytecode=True\n"
        + f"os.environ['AAS_INSTALL_CONFIG']={str(control)!r}\n"
        + f"sys.path.insert(0,{str(release / 'src')!r})\n"
        + "from aegis_alpha.application.container_runtime import main\nraise SystemExit(main())\n"
    ).encode()
    _write_owned(command_path, wrapper, selected.installation_id, executable=True)
    units = Path.home() / ".config/systemd/user"
    for name in ("aas-data-collection.service", "aas-data-collection.timer"):
        unit = (release / "config/systemd" / name).read_bytes()
        _write_owned(
            units / name,
            (_MANAGED + selected.installation_id + "\n").encode() + unit,
            selected.installation_id,
        )
    _command(["systemctl", "--user", "daemon-reload"])
    if enable_timer:
        _command(["systemctl", "--user", "enable", "--now", "aas-data-collection.timer"])
    return {
        "installation_id": selected.installation_id,
        "release": str(release),
        "revision": revision,
        "command": str(command_path),
        "timer_enabled": enable_timer,
        "database_mode": "attached",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Install the AAS container CLI from a reviewed manifest"
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--command-path", type=Path, default=Path.home() / ".local/bin/aas")
    parser.add_argument("--enable-timer", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = install(
            args.manifest, args.repository, args.command_path, enable_timer=args.enable_timer
        )
        print(json.dumps(result, indent=2))  # noqa: T201 -- installer JSON
    except (ValueError, OSError, RuntimeError, TypeError, tarfile.TarError):
        print(  # noqa: T201 -- safe installer error
            json.dumps(
                {"error": "installation failed; inspect the owned manifest and preserved release"}
            ),
            file=sys.stderr,
        )
        return 1
    return 0
