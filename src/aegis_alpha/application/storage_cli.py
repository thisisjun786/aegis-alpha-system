"""Native local database commands. Help does not import database drivers."""

from __future__ import annotations

# ruff: noqa: PLC0415 -- lazy imports keep preview/status dependency-free.
import argparse
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3

    from aegis_alpha.storage.input_pins import ConventionPin
    from aegis_alpha.storage.strategies import LineageSpec
    from aegis_alpha.storage.workspace import Workspace


def _home(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--home", type=Path, default=argparse.SUPPRESS, help="AAS user data directory"
    )


def add_commands(commands: argparse._SubParsersAction) -> None:
    for name in ("init", "doctor"):
        _home(commands.add_parser(name, help=f"{name.capitalize()} the local AAS installation"))
    db = commands.add_parser("db", help="Manage local SQLite and DuckDB storage")
    _home(db)
    sub = db.add_subparsers(dest="db_command", required=True)
    for name in ("status", "verify", "recover", "quarantine", "backup", "restore"):
        command = sub.add_parser(name)
        _home(command)
        if name == "quarantine":
            command.add_argument("--operation", required=True)
            command.add_argument("--reason", required=True)
        if name == "backup":
            command.add_argument("--output", type=Path)
        if name == "restore":
            command.add_argument("--backup", type=Path, required=True)
    _source_parsers(sub)
    strategy = commands.add_parser("strategy", help="Manage the private strategy database")
    _home(strategy)
    sub = strategy.add_subparsers(dest="strategy_command", required=True)
    _home(sub.add_parser("list"))
    importer = sub.add_parser("import", help="Validate and persist an exact strategy bundle")
    _home(importer)
    importer.add_argument("file", type=Path)
    importer.add_argument("--id", required=True)
    importer.add_argument("--version", required=True)
    importer.add_argument("--sha256", required=True)
    for option in ("parent-id", "parent-version", "change-kind", "reason"):
        importer.add_argument(f"--{option}", help="Optional lineage; supply all four fields")
    show = sub.add_parser("show", help="Inspect a pinned execution definition without executing")
    _home(show)
    for option in ("id", "version", "sha256"):
        show.add_argument(f"--{option}", required=True)
    show.add_argument("--requirements", type=Path, help="Pinned execution-requirements JSON file")
    show.add_argument("--requirements-sha256", help="SHA-256 of exact requirements file bytes")
    data = commands.add_parser("data", help="Read and publish pinned local market generations")
    _home(data)
    sub = data.add_subparsers(dest="data_command", required=True)
    _home(sub.add_parser("datasets"))
    importer = sub.add_parser("import", help="Import a validated local typed data envelope")
    _home(importer)
    importer.add_argument("file", type=Path)
    importer.add_argument("--sha256", required=True)
    for name in ("inspect", "read"):
        reader = sub.add_parser(name)
        _home(reader)
        reader.add_argument("--dataset", required=True)
        reader.add_argument("--version", required=True)
        if name == "read":
            reader.add_argument("--cutoff-us", type=int)
            reader.add_argument("--ingestion-cutoff-us", type=int)
            reader.add_argument("--limit", type=int, default=100)


def _source_parsers(sub: argparse._SubParsersAction) -> None:
    for name in ("sources", "source-tables", "source-read", "source-import"):
        command = sub.add_parser(name, help="Inspect or import the private source library")
        _home(command)
        if name in {"source-tables", "source-read"}:
            command.add_argument("--source", required=True)
        if name == "source-read":
            command.add_argument("--table", required=True)
            command.add_argument("--limit", type=int, default=100)
        if name == "source-import":
            command.add_argument("file", type=Path)
            command.add_argument("--id", required=True)
            command.add_argument("--sha256", required=True)


def execute(args: argparse.Namespace) -> dict[str, object]:
    import sqlite3

    import duckdb

    from aegis_alpha.storage.paths import resolve_home
    from aegis_alpha.storage.workspace import initialize, open_workspace

    home = resolve_home(getattr(args, "home", None))
    try:
        if args.command == "init":
            return initialize(home)
        if args.command == "db" and args.db_command == "backup":
            from aegis_alpha.storage.backup import backup

            return backup(home, args.output)
        if args.command == "db" and args.db_command == "restore":
            if getattr(args, "home", None) is None:
                raise ValueError("restore requires an explicit --home for a new directory")
            from aegis_alpha.storage.backup import restore

            return restore(args.backup.absolute(), home)
        mutation = (
            (args.command == "strategy" and args.strategy_command == "import")
            or (args.command == "data" and args.data_command == "import")
            or (
                args.command == "db"
                and args.db_command in {"recover", "quarantine", "source-import"}
            )
        )
        with open_workspace(
            home,
            writable=mutation,
            strategy_write=mutation,
            require_strategies=args.command == "strategy"
            or (
                args.command == "db"
                and args.db_command
                in {"verify", "recover", "sources", "source-tables", "source-read"}
            ),
        ) as workspace:
            return _workspace_command(workspace, args)
    except (sqlite3.Error, duckdb.Error):
        raise ValueError("local database operation failed; run aas db verify") from None


def _workspace_command(workspace: object, args: argparse.Namespace) -> dict[str, object]:  # noqa: PLR0911 -- CLI routing
    from aegis_alpha.storage.workspace import Workspace

    if not isinstance(workspace, Workspace):
        raise TypeError("expected admitted workspace")
    if args.command == "doctor" or (args.command == "db" and args.db_command == "status"):
        return workspace.doctor()
    if args.command == "db":
        if args.db_command in {"sources", "source-tables", "source-read", "source-import"}:
            return _source_command(workspace, args)
        if args.db_command == "verify":
            from aegis_alpha.storage.verification import verify_workspace

            return verify_workspace(workspace)
        if args.db_command == "quarantine":
            from aegis_alpha.storage.publication import quarantine

            return quarantine(workspace, args.operation, args.reason)
        from aegis_alpha.storage.publication import recover_operations

        return recover_operations(workspace)
    if args.command == "strategy":
        return _strategy_command(workspace, args)
    from aegis_alpha.storage.publication import execute_data

    return execute_data(workspace, args)


def _strategy_command(workspace: Workspace, args: argparse.Namespace) -> dict[str, object]:
    if workspace.strategies is None:
        raise ValueError("strategy database is unavailable")
    if args.strategy_command == "list":
        from aegis_alpha.storage.strategies import list_strategies

        return {"strategies": list_strategies(workspace.strategies)}
    if args.strategy_command == "show":
        return _show_definition(workspace.strategies, workspace.state, args)
    from aegis_alpha.storage.strategy_import import register_strategy

    return register_strategy(
        workspace,
        args.file.absolute(),
        args.sha256,
        args.id,
        args.version,
        lineage=_strategy_lineage(args),
    )


def _source_command(workspace: object, args: argparse.Namespace) -> dict[str, object]:
    from aegis_alpha.storage import source_library
    from aegis_alpha.storage.workspace import Workspace

    if not isinstance(workspace, Workspace):
        raise TypeError("expected admitted workspace")
    if args.db_command == "sources":
        return {"sources": source_library.list_sources(workspace)}
    if args.db_command == "source-tables":
        return {"tables": source_library.list_tables(workspace, args.source)}
    if args.db_command == "source-read":
        from aegis_alpha.storage.source_reader import inspect_source

        return dict(inspect_source(workspace, args.source, args.table, limit=args.limit))
    return source_library.import_sqlite(workspace, args.file.absolute(), args.id, args.sha256)


def _strategy_lineage(args: argparse.Namespace) -> LineageSpec | None:
    from aegis_alpha.storage.strategies import LineageSpec

    fields = (args.parent_id, args.parent_version, args.change_kind, args.reason)
    if fields.count(None) not in (0, len(fields)):
        raise ValueError("lineage requires parent-id, parent-version, change-kind and reason")
    return None if args.parent_id is None else LineageSpec(*fields)


def _show_definition(
    strategies: sqlite3.Connection, state: sqlite3.Connection, args: argparse.Namespace
) -> dict[str, object]:
    from dataclasses import asdict

    from aegis_alpha.engine.errors import ContractDefinitionError
    from aegis_alpha.storage.strategy_requirements import read_execution_definition

    bindings = _execution_bindings(args.requirements, args.requirements_sha256)
    try:
        definition = read_execution_definition(
            strategies,
            args.id,
            args.version,
            args.sha256,
            bindings,
            state_connection=state,
        )
    except ContractDefinitionError as error:
        raise ValueError(str(error)) from error
    return asdict(definition)


def _execution_bindings(path: Path | None, sha256: str | None) -> tuple[ConventionPin, ...] | None:
    """Parse bounded UTF-8 transport into exact stored pins, never inline conventions."""
    import hashlib
    import re

    from aegis_alpha.engine.codec import decode_json

    if (path is None) != (sha256 is None):
        raise ValueError("requirements and requirements-sha256 must be supplied together")
    if path is None:
        return None
    if sha256 is None or re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
        raise ValueError("requirements hash must be lowercase SHA-256 hex")
    try:
        with path.open("rb") as source:
            raw = source.read(1024 * 1024 + 1)
    except OSError:
        raise ValueError("cannot read execution requirements document") from None
    if len(raw) > 1024 * 1024 or hashlib.sha256(raw).hexdigest() != sha256:
        raise ValueError("requirements exceeds 1 MiB or file hash does not match")
    if raw.startswith(b"\xef\xbb\xbf") or b"\x00" in raw:
        raise ValueError("requirements must be UTF-8 without BOM or literal NUL")
    try:
        raw.decode("utf-8", errors="strict")
        return _convention_bindings(decode_json(raw))
    except (ValueError, TypeError, RecursionError) as error:
        raise ValueError("invalid execution requirements document") from error


def _convention_bindings(document: object) -> tuple[ConventionPin, ...]:
    from aegis_alpha.storage.input_pins import ConventionPin

    if not isinstance(document, dict) or document.keys() != {
        "schema_version",
        "convention_bindings",
    }:
        raise ValueError("invalid execution requirements keys")
    if document["schema_version"] != "aas-execution-requirements-v1":
        raise ValueError("unsupported execution requirements schema")
    pins = document["convention_bindings"]
    if not isinstance(pins, list):
        raise TypeError("convention_bindings must be an array")
    bindings = []
    for pin in pins:
        if not isinstance(pin, dict) or pin.keys() != {"kind", "id", "version", "hash"}:
            raise ValueError("invalid convention pin keys")
        bindings.append(ConventionPin(pin["kind"], pin["id"], pin["version"], pin["hash"]))
    return tuple(bindings)
