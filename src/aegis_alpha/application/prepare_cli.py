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
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.storage.locks import private_directory, storage_lock_targets
from aegis_alpha.storage.paths import load_paths, resolve_home
from aegis_alpha.storage.workspace import open_workspace

_MAX_REQUEST_BYTES = 1024 * 1024


def add_commands(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = commands.add_parser("prepare", help="Export stored inputs without executing fills")
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--sha256", required=True, help="Expected exact request file SHA-256")
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New envelope path; also creates PATH.preparation.json (no overwrites)",
    )


def _path(path: Path) -> Path:
    if ".." in path.parts:
        raise ValueError("prepare paths cannot contain parent aliases")
    return path.absolute()


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
    path = _path(cast("Path", args.request))
    with DescriptorTree.open_path(path.parent) as tree:
        raw = tree.read_bytes(path.name, max_bytes=_MAX_REQUEST_BYTES)
    if hashlib.sha256(raw).hexdigest() != args.sha256:
        raise ValueError("prepare request SHA-256 mismatch")
    request = PrepareRequest(parse_prepare_request(raw))
    output = _path(cast("Path", args.output))
    sidecar = output.name + ".preparation.json"
    home = resolve_home(args.home)
    private_directory(home)
    targets = storage_lock_targets(home, load_paths(home).stores())
    # Load the optional runtime driver only for this command, not help/status.
    database_error = import_module("duckdb").Error
    with DescriptorTree.open_path(output.parent) as tree:
        if tree.exists(output.name) or tree.exists(sidecar):
            raise FileExistsError("prepare outputs must both be new files")
        try:
            with price_compute(excluded_locks=targets) as budget:
                if budget is None:
                    raise ValueError("prepare requires the explicit AAS compute budget environment")
                with open_workspace(home) as workspace:
                    prepared = prepare_backtest(workspace, request, budget=budget)
        except (sqlite3.Error, database_error):
            raise ValueError("local database operation failed; run aas db verify") from None
        envelope_stat = _write(tree, output.name, prepared.envelope.canonical_bytes)
        sidecar_stat = _write(tree, sidecar, prepared.provenance)
        tree.fsync_directory()
        envelope = _verify(tree, output.name, prepared.envelope.canonical_bytes, envelope_stat)
        preparation = _verify(tree, sidecar, prepared.provenance, sidecar_stat)
        with DescriptorTree.open_path(output.parent) as visible:
            if visible.identity != tree.identity:
                raise ValueError("prepare output directory changed during sealing")
    return {
        "prepared": True,
        "request_sha256": args.sha256,
        "request_hash": prepared.request_hash,
        "envelope": envelope,
        "preparation": preparation,
        "certified": False,
    }
