"""Quiesced private backup sets and restore to a fresh local installation root."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import uuid
from pathlib import Path
from typing import cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.storage.locks import private_directory, private_file
from aegis_alpha.storage.paths import DEFAULT_PATHS, load_paths, read_json, resolve_home
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import open_workspace, write_json

_MANIFEST = "backup.json"


def _copy_file(source: Path, target: Path) -> dict[str, object]:
    private_file(source)
    private_directory(target.parent, create=True)
    hasher = hashlib.sha256()
    size = 0
    with (
        DescriptorTree.open_path(source.parent) as origin,
        DescriptorTree.open_path(target.parent) as destination,
    ):
        with (
            origin.binary_reader(source.name, require_single_link=True) as reader,
            destination.binary_writer(target.name, exclusive=True) as writer,
        ):
            for chunk in iter(lambda: reader.read(1024 * 1024), b""):
                writer.write(chunk)
                hasher.update(chunk)
                size += len(chunk)
            writer.flush()
            os.fsync(writer.fileno())
        destination.fsync_directory()
    return {"size_bytes": size, "sha256": hasher.hexdigest()}


def _file_hash(path: Path) -> dict[str, object]:
    private_file(path)
    with (
        DescriptorTree.open_path(path.parent) as tree,
        tree.binary_reader(path.name, require_single_link=True) as reader,
    ):
        hasher = hashlib.sha256()
        size = 0
        for chunk in iter(lambda: reader.read(1024 * 1024), b""):
            hasher.update(chunk)
            size += len(chunk)
    return {"size_bytes": size, "sha256": hasher.hexdigest()}


def _copy_tree(source: Path, target: Path, files: dict[str, object], prefix: str) -> None:
    private_directory(source)
    private_directory(target, create=True)
    with DescriptorTree.open_path(source) as tree:
        names = tree.listdir()
    for name in names:
        child = source / name
        if child.is_symlink():
            raise ValueError("backup refuses symlinks")
        if child.is_dir():
            _copy_tree(child, target / name, files, prefix + name + "/")
        else:
            files[prefix + name] = _copy_file(child, target / name)


def backup(home: Path, output: Path | None = None) -> dict[str, object]:
    with open_workspace(home, writable=True) as workspace:
        verification = verify_workspace(workspace)
        if verification["pending_operations"] or verification["orphan_generations"]:
            raise ValueError("backup requires recovered operations and no orphan generations")
        if workspace.state.execute("SELECT 1 FROM runs WHERE status='RUNNING'").fetchone():
            raise ValueError("backup requires all running analyses to stop")
        target = (
            resolve_home(output)
            if output is not None
            else workspace.paths.backups / uuid.uuid4().hex
        )
        if target.exists() or target.is_symlink():
            raise ValueError("backup destination must be a new directory")
        if any(
            target.is_relative_to(path)
            for path in (workspace.paths.raw, workspace.paths.runs, workspace.paths.secrets)
        ):
            raise ValueError("backup destination cannot be inside raw, runs, or secrets")
        private_directory(target, create=True)
        files: dict[str, object] = {}
        for name, connection in (("state", workspace.state), ("strategies", workspace.strategies)):
            if connection is None:
                raise ValueError("backup requires all three stores")
            path = target / DEFAULT_PATHS[name]
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(descriptor)
            destination = sqlite3.connect(path)
            try:
                connection.backup(destination)
            finally:
                destination.close()
            with DescriptorTree.open_path(target) as tree:
                tree.fsync_file(path.name)
            files[path.name] = _file_hash(path)
        workspace.market.execute("CHECKPOINT")
        workspace.market.close()
        files["market.duckdb"] = _copy_file(workspace.paths.market, target / "market.duckdb")
        _copy_tree(workspace.paths.raw, target / "raw", files, "raw/")
        _copy_tree(workspace.paths.runs, target / "runs", files, "runs/")
        # Whitelist-only export: no provider config, credentials, or external operating paths.
        original_config = read_json(workspace.paths.root / "runtime.json")
        resources = original_config.get("resources", {"threads": 2, "memory_limit": "512MB"})
        if not isinstance(resources, dict):
            raise TypeError("resources must be an object")
        runtime = {
            "format_version": 1,
            "paths": DEFAULT_PATHS,
            "resources": {
                key: cast("dict[str, object]", resources)[key]
                for key in ("threads", "memory_limit")
            },
            "providers": {},
            "jobs": {"enabled": False},
        }
        write_json(target / "runtime.json", runtime)
        receipt = read_json(workspace.paths.root / "installation.json")
        write_json(target / "installation.json", receipt)
        for name in ("runtime.json", "installation.json"):
            files[name] = _file_hash(target / name)
        manifest = {
            "format_version": 1,
            "complete": True,
            "installation_id": workspace.installation_id,
            "files": files,
            "logical": verification,
            "secrets_included": False,
        }
        write_json(target / _MANIFEST, manifest)
    return {
        "backed_up": True,
        "backup_root": str(target),
        "secrets_included": False,
        "files": len(files),
    }


def _validated_manifest(root: Path) -> dict[str, object]:
    private_directory(root)
    manifest = read_json(root / _MANIFEST)
    if (
        type(manifest.get("format_version")) is not int
        or manifest.get("format_version") != 1
        or manifest.get("complete") is not True
        or manifest.get("secrets_included") is not False
    ):
        raise ValueError("backup is incomplete or unsupported")
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise TypeError("backup manifest needs files")
    required = {
        "state.sqlite3",
        "strategies.sqlite3",
        "market.duckdb",
        "runtime.json",
        "installation.json",
    }
    if not required <= files.keys():
        raise ValueError("backup lacks required databases/configuration")
    for relative, expected in files.items():
        if not isinstance(relative, str):
            raise TypeError("backup path must be text")
        path = Path(relative)
        if (
            path.is_absolute()
            or ".." in path.parts
            or path.as_posix() != relative
            or not (relative in required or relative.startswith(("raw/", "runs/")))
        ):
            raise ValueError("backup contains an unsafe or unsupported path")
        if _file_hash(root / path) != expected:
            raise ValueError("backup file hash/size mismatch")
    paths = load_paths(root)
    if any(Path(value) != root / DEFAULT_PATHS[key] for key, value in paths.to_dict().items()):
        raise ValueError("backup configuration must contain only local relative storage paths")
    return manifest


def restore(backup_root: Path, new_home: Path) -> dict[str, object]:
    backup_root = resolve_home(backup_root)
    new_home = resolve_home(new_home)
    if new_home.exists() or new_home.is_symlink():
        raise ValueError(
            "restore requires a new nonexistent home; existing data is never overwritten"
        )
    manifest = _validated_manifest(backup_root)
    private_directory(new_home, create=True)
    files = cast("dict[str,object]", manifest["files"])
    receipt = read_json(backup_root / "installation.json")
    receipt["phase"] = "restoring"
    receipt["deployment_id"] = uuid.uuid4().hex
    write_json(new_home / "installation.json", receipt)
    for relative in files:
        if relative != "installation.json":
            _copy_file(backup_root / relative, new_home / relative)
    for name in ("raw", "runs", "secrets", "backups", "runtime"):
        private_directory(new_home / name, create=True)
    try:
        with open_workspace(new_home, validating_restore=True) as workspace:
            verification = verify_workspace(workspace)
            if verification != manifest["logical"]:
                raise ValueError("restored logical verification differs from backup")  # noqa: TRY301 -- persist failed restore receipt
    except BaseException:
        receipt["phase"] = "restore-incomplete"
        write_json(new_home / "installation.json", receipt)
        raise
    receipt["phase"] = "ready"
    write_json(new_home / "installation.json", receipt)
    return {
        "restored": True,
        "home": str(new_home),
        "verification": verification,
        "secrets_restored": False,
    }
