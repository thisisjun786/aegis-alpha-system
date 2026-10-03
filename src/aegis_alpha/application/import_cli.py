"""``aas import legacy``: retain legacy originals as content-addressed source-library sources."""

from __future__ import annotations

# ruff: noqa: PLC0415 -- lazy storage imports keep help independent of database drivers.
import argparse
from pathlib import Path

from aegis_alpha.application.storage_cli import home_option


def add_commands(commands: argparse._SubParsersAction) -> None:
    importer = commands.add_parser(
        "import", help="Import legacy originals into raw/ and the source library"
    )
    home_option(importer)
    sub = importer.add_subparsers(dest="import_command", required=True)
    legacy = sub.add_parser(
        "legacy",
        help="Plan, apply or verify an aas-legacy-import-v1 manifest",
    )
    home_option(legacy)
    legacy.add_argument(
        "--manifest", required=True, type=Path, help="An exact aas-legacy-import-v1 manifest"
    )
    legacy.add_argument("--sha256", required=True, help="SHA-256 of the --manifest bytes")
    mode = legacy.add_mutually_exclusive_group()
    mode.add_argument(
        "--plan",
        action="store_true",
        help="Read and reconcile the originals; open no installation and write nothing",
    )
    mode.add_argument(
        "--verify",
        action="store_true",
        help="Check every planned source is committed, identical and linked (read-only)",
    )


def execute(args: argparse.Namespace) -> dict[str, object]:
    import sqlite3

    import duckdb

    from aegis_alpha.storage.legacy_import.engine import (
        apply_import,
        plan_import,
        read_manifest_file,
        verify_import,
    )

    manifest = read_manifest_file(args.manifest, args.sha256)
    if args.plan:
        return plan_import(manifest)

    from aegis_alpha.application.compute_cli import price_compute
    from aegis_alpha.storage.locks import private_directory, storage_lock_targets
    from aegis_alpha.storage.paths import load_paths, resolve_home
    from aegis_alpha.storage.workspace import open_workspace

    home = resolve_home(getattr(args, "home", None))
    private_directory(home)
    targets = storage_lock_targets(home, load_paths(home).stores())
    try:
        with (
            price_compute(excluded_locks=targets),
            open_workspace(
                home, writable=not args.verify, strategy_write=not args.verify
            ) as workspace,
        ):
            if args.verify:
                return verify_import(workspace, manifest)
            return apply_import(workspace, manifest)
    except (sqlite3.Error, duckdb.Error):
        raise ValueError("local database operation failed; run aas db verify") from None
