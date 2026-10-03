"""``aas calendar refresh``: promote declared venue calendars as session generations."""

from __future__ import annotations

# ruff: noqa: PLC0415 -- lazy storage imports keep help independent of database drivers.
import argparse
import hashlib
import time
from pathlib import Path

from aegis_alpha.application.storage_cli import home_option


def add_commands(commands: argparse._SubParsersAction) -> None:
    calendar = commands.add_parser(
        "calendar", help="Promote declared venue calendars (XNYS, XKRX) as session generations"
    )
    home_option(calendar)
    sub = calendar.add_subparsers(dest="calendar_command", required=True)
    refresh = sub.add_parser(
        "refresh",
        help="Promote each declaration as the next generation of sessions.<mic>",
    )
    home_option(refresh)
    refresh.add_argument(
        "--calendar",
        action="append",
        choices=("XKRX", "XNYS"),
        help="Packaged declaration to refresh; repeatable (default: every packaged calendar)",
    )
    refresh.add_argument(
        "--declaration", type=Path, help="An exact aas-calendar-declaration-v1 document"
    )
    refresh.add_argument("--sha256", help="SHA-256 of the --declaration bytes")
    refresh.add_argument(
        "--plan", action="store_true", help="Report the sessions and changes; write nothing"
    )


def _documents(args: argparse.Namespace) -> list[tuple[bytes, str]]:
    from aegis_alpha.data.descriptor_tree import DescriptorTree, DescriptorTreeError
    from aegis_alpha.storage.calendar_declaration import (
        MAX_DECLARATION_BYTES,
        PACKAGED,
        packaged_declaration,
    )

    if args.declaration is None:
        if args.sha256 is not None:
            raise ValueError("--sha256 names the bytes of --declaration")
        packaged = [packaged_declaration(name) for name in args.calendar or PACKAGED]
        return [(raw, hashlib.sha256(raw).hexdigest()) for raw in packaged]
    if args.calendar or args.sha256 is None:
        raise ValueError("--declaration takes its --sha256 and no --calendar")
    path = args.declaration.absolute()
    try:
        with DescriptorTree.open_path(path.parent) as tree:
            raw = tree.read_bytes(path.name, max_bytes=MAX_DECLARATION_BYTES)
    except (OSError, DescriptorTreeError) as error:
        raise ValueError("cannot read a bounded regular calendar declaration") from error
    return [(raw, args.sha256)]


def execute(args: argparse.Namespace) -> dict[str, object]:
    import sqlite3

    import duckdb

    from aegis_alpha.application.compute_cli import price_compute
    from aegis_alpha.storage.calendar_refresh import refresh_calendar
    from aegis_alpha.storage.locks import private_directory, storage_lock_targets
    from aegis_alpha.storage.paths import load_paths, resolve_home
    from aegis_alpha.storage.workspace import open_workspace

    documents = _documents(args)
    home = resolve_home(getattr(args, "home", None))
    private_directory(home)
    targets = storage_lock_targets(home, load_paths(home).stores())
    now_us = time.time_ns() // 1000
    try:
        with (
            price_compute(excluded_locks=targets) as budget,
            open_workspace(home, writable=not args.plan, strategy_write=not args.plan) as workspace,
        ):
            return {
                "calendars": [
                    refresh_calendar(
                        workspace, raw, sha256, apply=not args.plan, now_us=now_us, budget=budget
                    )
                    for raw, sha256 in documents
                ]
            }
    except (sqlite3.Error, duckdb.Error):
        raise ValueError("local database operation failed; run aas db verify") from None
