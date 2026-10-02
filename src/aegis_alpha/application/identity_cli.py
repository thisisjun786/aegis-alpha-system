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


def execute(args: argparse.Namespace) -> dict[str, object]:
    import sqlite3

    from aegis_alpha.storage import identity
    from aegis_alpha.storage.paths import resolve_home
    from aegis_alpha.storage.workspace import open_workspace

    home = resolve_home(getattr(args, "home", None))
    command = args.identity_command
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
