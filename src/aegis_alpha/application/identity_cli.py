"""``aas identity``: register issuers, instruments and assertions, snapshot and inspect them."""

from __future__ import annotations

# ruff: noqa: PLC0415 -- lazy storage imports keep help independent of database drivers.
import argparse
import time
from pathlib import Path

from aegis_alpha.application.storage_cli import home_option


def add_commands(commands: argparse._SubParsersAction) -> None:
    identity = commands.add_parser(
        "identity", help="Register and inspect issuers, instruments and identity assertions"
    )
    home_option(identity)
    sub = identity.add_subparsers(dest="identity_command", required=True)
    register = sub.add_parser(
        "register", help="Append an exact aas-identity-registry-v1 document to the registry"
    )
    home_option(register)
    register.add_argument("--file", type=Path, required=True)
    register.add_argument("--sha256", required=True)
    register.add_argument("--plan", action="store_true", help="Classify every row; write nothing")
    snapshot = sub.add_parser(
        "snapshot", help="Register the chunked identity snapshot of the registered assertions"
    )
    home_option(snapshot)
    snapshot.add_argument("--id", required=True, dest="snapshot_id")
    snapshot.add_argument("--provider", action="append", default=[], help="Repeatable filter")
    snapshot.add_argument("--namespace", action="append", default=[], help="Repeatable filter")
    snapshot.add_argument("--plan", action="store_true", help="Report the parts; write nothing")
    kr_import = sub.add_parser(
        "kr-import",
        help="Commit collected KIND listing and EODHD symbol-list receipts as content sources",
    )
    home_option(kr_import)
    kr_import.add_argument(
        "--kind-receipt", type=Path, action="append", default=[], help="KIND response.json"
    )
    kr_import.add_argument(
        "--eodhd-job", type=Path, action="append", default=[], help="Collected job directory"
    )
    kr_import.add_argument("--plan", action="store_true", help="Report the sources; write nothing")
    kr_build = sub.add_parser(
        "kr-build", help="Build the KR aas-identity-registry-v1 document from committed sources"
    )
    home_option(kr_build)
    kr_build.add_argument("--eodhd", action="append", required=True, help="Symbol-list source ID")
    kr_build.add_argument("--kind", action="append", default=[], help="KIND listing source ID")
    kr_build.add_argument("--dart", help="Source ID holding the DART corp_codes receipt")
    kr_build.add_argument("--output", type=Path, required=True, help="New registry file")
    kr_build.add_argument("--report", type=Path, help="New file for the full JSON report")
    show = sub.add_parser("show", help="Inspect one instrument, provider key or snapshot")
    home_option(show)
    target = show.add_mutually_exclusive_group(required=True)
    target.add_argument("--instrument", help="Opaque instrument ID")
    target.add_argument("--anchor", nargs=2, metavar=("NAMESPACE", "TOKEN"))
    target.add_argument("--key", nargs=3, metavar=("PROVIDER", "NAMESPACE", "TOKEN"))
    target.add_argument("--snapshot", help="Identity snapshot ID")


def _registry_bytes(path: Path) -> bytes:
    from aegis_alpha.data.descriptor_tree import DescriptorTree, DescriptorTreeError
    from aegis_alpha.storage.identity import MAX_REGISTRY_BYTES

    path = path.absolute()
    try:
        with DescriptorTree.open_path(path.parent) as tree:
            return tree.read_bytes(path.name, max_bytes=MAX_REGISTRY_BYTES)
    except (OSError, DescriptorTreeError) as error:
        raise ValueError("cannot read bounded regular identity registry document") from error


def _new_file(path: Path, raw: bytes) -> None:
    try:
        with path.open("xb") as handle:
            handle.write(raw)
    except OSError as error:
        raise ValueError(f"cannot create new file {path.name}") from error


def _kr(args: argparse.Namespace, home: Path) -> dict[str, object]:
    import json

    from aegis_alpha.storage import kr_identity
    from aegis_alpha.storage.source_library import list_sources
    from aegis_alpha.storage.workspace import open_workspace

    if args.identity_command == "kr-build":
        with open_workspace(home, writable=False, require_strategies=False) as workspace:
            registry = kr_identity.build_from_workspace(
                workspace, eodhd=args.eodhd, kind=args.kind, dart=args.dart
            )
        _new_file(args.output, registry.raw())
        if args.report is not None:
            full = json.dumps(registry.report(sample=None), ensure_ascii=False, sort_keys=True)
            _new_file(args.report, full.encode())
        return {"file": str(args.output), **registry.report()}
    units = [
        *(kr_identity.read_kind_receipt(path) for path in args.kind_receipt),
        *(kr_identity.read_eodhd_job(path) for path in args.eodhd_job),
    ]
    if not units:
        raise ValueError("kr-import needs at least one --kind-receipt or --eodhd-job")
    with open_workspace(
        home, writable=not args.plan, strategy_write=not args.plan, require_strategies=False
    ) as workspace:
        if args.plan:
            committed = {str(row["source_id"]) for row in list_sources(workspace)}
            return {
                "mode": "plan",
                "sources": [
                    {
                        "source_id": unit.content.source_id,
                        "table": unit.table,
                        "rows": len(unit.rows),
                        "committed": unit.content.source_id in committed,
                    }
                    for unit in units
                ],
            }
        return {
            "mode": "apply",
            "sources": [kr_identity.import_unit(workspace, unit) for unit in units],
        }


def execute(args: argparse.Namespace) -> dict[str, object]:
    import sqlite3

    from aegis_alpha.storage import identity
    from aegis_alpha.storage.paths import resolve_home
    from aegis_alpha.storage.workspace import open_workspace

    home = resolve_home(getattr(args, "home", None))
    command = args.identity_command
    if command in {"kr-import", "kr-build"}:
        import duckdb

        try:
            return _kr(args, home)
        except (sqlite3.Error, duckdb.Error):
            raise ValueError("local database operation failed; run aas db verify") from None
    document = None
    if command == "register":
        document = identity.decode_registry(
            _registry_bytes(args.file), expected_file_sha256=args.sha256
        )
    writes = command in {"register", "snapshot"} and not args.plan
    try:
        with open_workspace(home, writable=writes, require_strategies=False) as workspace:
            state = workspace.state
            if document is not None:
                return identity.register_identities(state, document, apply=writes)
            if command == "snapshot":
                return identity.snapshot_identities(
                    state,
                    args.snapshot_id,
                    identity.AssertionSelection(tuple(args.provider), tuple(args.namespace)),
                    created_at_us=time.time_ns() // 1000,
                    apply=writes,
                )
            if args.snapshot is not None:
                return identity.show_snapshot(state, args.snapshot)
            if args.key is not None:
                return identity.show_key(state, *args.key)
            instrument = args.instrument or identity.mint_instrument(*args.anchor)
            return identity.show_instrument(state, instrument)
    except sqlite3.Error:
        raise ValueError("local database operation failed; run aas db verify") from None
