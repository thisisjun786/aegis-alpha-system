"""Prepare stored inputs without accounting or registration.

ENVELOPE.preparation.json contains the exact T18 provenance bytes. Both files
are exclusively created; failures retain any partial outputs for inspection,
never remove foreign files, and never return a success receipt.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
from importlib import import_module
from pathlib import Path
from typing import cast

from aegis_alpha.application.backtest_prepare import (
    PrepareRequest,
    parse_prepare_request,
    prepare_backtest,
)
from aegis_alpha.application.compute_cli import price_compute
from aegis_alpha.application.storage_cli import home_option
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.storage.locks import private_directory, storage_lock_targets
from aegis_alpha.storage.paths import load_paths, resolve_home
from aegis_alpha.storage.workspace import open_workspace

_MAX_REQUEST_BYTES = 1024 * 1024
_SIDECAR = ".preparation.json"


def add_commands(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = commands.add_parser("prepare", help="Export stored inputs without executing fills")
    home_option(parser)
    parser.add_argument(
        "--request", type=Path, required=True, help="Exact prepare request document"
    )
    parser.add_argument("--sha256", required=True, help="Expected exact request file SHA-256")
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New envelope path; also creates PATH.preparation.json (no overwrites)",
    )


def admitted_path(path: Path) -> Path:
    """Absolute, alias-free path. Shared with every command that seals these outputs."""
    if ".." in path.parts:
        raise ValueError("prepare paths cannot contain parent aliases")
    return path.absolute()


def read_request_bytes(path: Path, expected_sha256: str) -> bytes:
    """Read the exact request document, refusing anything but its own bytes.

    Kept separate from parsing so a caller that owns a compute lease can decode inside
    it. The read itself is bounded and cheap, so it stays available before the lease.
    """
    request = admitted_path(path)
    with DescriptorTree.open_path(request.parent) as tree:
        raw = tree.read_bytes(request.name, max_bytes=_MAX_REQUEST_BYTES)
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("prepare request SHA-256 mismatch")
    return raw


def read_prepare_request(path: Path, expected_sha256: str) -> PrepareRequest:
    """Read and parse the exact request document, refusing anything but its own bytes."""
    return PrepareRequest(parse_prepare_request(read_request_bytes(path, expected_sha256)))


def require_new_outputs(tree: DescriptorTree, name: str) -> None:
    """Refuse before any work when either output name is already taken."""
    if tree.exists(name) or tree.exists(name + _SIDECAR):
        raise FileExistsError("prepare outputs must both be new files")


def seal_outputs(
    tree: DescriptorTree, name: str, envelope_bytes: bytes, provenance_bytes: bytes
) -> dict[str, dict[str, str]]:
    """Create, fsync and re-read both outputs under one admitted directory handle."""
    require_new_outputs(tree, name)
    sidecar = name + _SIDECAR
    envelope_stat = _write(tree, name, envelope_bytes)
    sidecar_stat = _write(tree, sidecar, provenance_bytes)
    tree.fsync_directory()
    envelope = _verify(tree, name, envelope_bytes, envelope_stat)
    preparation = _verify(tree, sidecar, provenance_bytes, sidecar_stat)
    with DescriptorTree.open_path(tree.logical_root) as visible:
        if visible.identity != tree.identity:
            raise ValueError("prepare output directory changed during sealing")
    return {"envelope": envelope, "preparation": preparation}


def _write(tree: DescriptorTree, name: str, raw: bytes) -> os.stat_result:
    with tree.binary_writer(name, exclusive=True) as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
        return os.fstat(handle.fileno())


def _verify(tree: DescriptorTree, name: str, raw: bytes, owned: os.stat_result) -> dict[str, str]:
    with tree.binary_reader(name, require_single_link=True) as handle:
        before = os.fstat(handle.fileno())
        actual = handle.read(len(raw) + 1)
        after = os.fstat(handle.fileno())
    if (
        not os.path.samestat(owned, before)
        or not os.path.samestat(after, tree.stat(name))
        or before.st_mtime_ns != after.st_mtime_ns
        or actual != raw
    ):
        raise ValueError("prepared output changed during sealing")
    return {"path": str(tree.logical_root / name), "sha256": hashlib.sha256(actual).hexdigest()}


def execute(args: argparse.Namespace) -> dict[str, object]:
    request = read_prepare_request(cast("Path", args.request), args.sha256)
    output = admitted_path(cast("Path", args.output))
    home = resolve_home(args.home)
    private_directory(home)
    targets = storage_lock_targets(home, load_paths(home).stores())
    # Load the optional runtime driver only for this command, not help/status.
    database_error = import_module("duckdb").Error
    with DescriptorTree.open_path(output.parent) as tree:
        require_new_outputs(tree, output.name)
        try:
            with price_compute(excluded_locks=targets) as budget:
                if budget is None:
                    raise ValueError("prepare requires the explicit AAS compute budget environment")
                with open_workspace(home) as workspace:
                    prepared = prepare_backtest(workspace, request, budget=budget)
        except (sqlite3.Error, database_error):
            raise ValueError("local database operation failed; run aas db verify") from None
        sealed = seal_outputs(
            tree, output.name, prepared.envelope.canonical_bytes, prepared.provenance
        )
    return {
        "prepared": True,
        "request_sha256": args.sha256,
        "request_hash": prepared.request_hash,
        "envelope": sealed["envelope"],
        "preparation": sealed["preparation"],
        "certified": False,
    }
