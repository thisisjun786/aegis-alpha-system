"""``aas universe``: build, register and inspect index membership and listing universes."""

from __future__ import annotations

# ruff: noqa: PLC0415 -- lazy storage imports keep help independent of database drivers.
import argparse
from pathlib import Path

from aegis_alpha.application.storage_cli import home_option


def add_commands(commands: argparse._SubParsersAction) -> None:
    universe = commands.add_parser(
        "universe", help="Register and inspect index membership and listing universes"
    )
    home_option(universe)
    sub = universe.add_subparsers(dest="universe_command", required=True)
    index = sub.add_parser(
        "index",
        help="Register one universe per Norgate index from committed membership sources",
    )
    home_option(index)
    index.add_argument(
        "--source", action="append", required=True, help="Index membership source ID"
    )
    index.add_argument(
        "--index", action="append", default=[], help="Repeatable Norgate index name filter"
    )
    _common(index)
    listings = sub.add_parser(
        "listings", help="Register the US listing universe from the Norgate security master"
    )
    home_option(listings)
    listings.add_argument("--master", required=True, help="Norgate security master source ID")
    _common(listings)
    show = sub.add_parser("show", help="Inspect one registered universe version")
    home_option(show)
    show.add_argument("--id", required=True, dest="universe_id")
    show.add_argument("--version", required=True)


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--version", required=True, help="Exact universe version to register")
    parser.add_argument("--report", type=Path, help="New file for the full JSON report")
    parser.add_argument("--plan", action="store_true", help="Report the universes; write nothing")


def execute(args: argparse.Namespace) -> dict[str, object]:
    import json
    import sqlite3

    import duckdb

    from aegis_alpha.application.identity_cli import _new_files
    from aegis_alpha.storage import universe
    from aegis_alpha.storage.paths import resolve_home
    from aegis_alpha.storage.workspace import open_workspace

    home = resolve_home(getattr(args, "home", None))
    command = args.universe_command
    try:
        if command == "show":
            with open_workspace(home, writable=False, require_strategies=False) as workspace:
                return universe.show_universe(workspace.state, args.universe_id, args.version)
        writes = not args.plan
        report = args.report
        if report is not None and (
            report.exists() or report.is_symlink() or not report.parent.is_dir()
        ):
            # Checked before anything is registered, so a refused path never follows a write.
            raise ValueError(f"cannot create new file {report.name}: it exists or has no folder")
        with open_workspace(home, writable=writes, require_strategies=False) as workspace:
            if command == "index":
                build = universe.build_index_universes(
                    workspace, args.source, version=args.version, indexes=args.index
                )
            else:
                build = universe.build_listing_universe(
                    workspace, args.master, version=args.version
                )
            registered = universe.register_universes(workspace.state, build, apply=writes)
    except (sqlite3.Error, duckdb.Error):
        raise ValueError("local database operation failed; run aas db verify") from None
    if report is not None:
        full = {**build.summary(sample=None), **registered}
        _new_files([(report, json.dumps(full, ensure_ascii=False, sort_keys=True).encode())])
    return {**build.summary(), **registered}
