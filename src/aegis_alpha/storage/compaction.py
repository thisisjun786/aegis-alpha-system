"""Reclaim space by rebuilding an installation into a new root, never in place.

``aas db compact --to NEW_ROOT`` is a restore whose source is the live installation
instead of a backup. Under the installation's maintenance locks it verifies the
installation, writes compact copies of the stores into ``NEW_ROOT`` (SQLite through the
backup API and ``VACUUM``, DuckDB through ``COPY FROM DATABASE`` into a fresh file, which
leaves behind the blocks dropped tables and superseded pages held), copies ``raw/``,
``runs/`` and ``secrets/`` file by file, then opens the new root and requires the same
logical verification. Only then is the new receipt ``ready``. Nothing in the original
installation is changed or removed: switching ``AAS_HOME`` (or ``--home``) to the new root
is the operator's step, and an interrupted compaction leaves the new root marked
``restore-incomplete`` while the original stays as it was.
"""

from __future__ import annotations

# ruff: noqa: S608 -- every identifier is a catalog name passed through quoted.
import os
import sqlite3
import uuid
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, cast

from aegis_alpha.storage.backup import copy_tree
from aegis_alpha.storage.locks import private_directory, require_outside_checkout
from aegis_alpha.storage.market import limit_duckdb
from aegis_alpha.storage.paths import DEFAULT_PATHS, read_json, resolve_home
from aegis_alpha.storage.source_library_schema import quoted
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import open_workspace, write_json

if TYPE_CHECKING:
    import duckdb

    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace

_SOURCE_ALIAS = "aas_compact_source"


def _sizes(paths: dict[str, Path]) -> dict[str, int]:
    return {name: path.stat().st_size for name, path in paths.items()}


def _target(workspace: Workspace, to: Path) -> Path:
    """A new root that neither overlaps the installation's paths nor exists yet."""
    target = resolve_home(to)
    if target.exists() or target.is_symlink():
        raise ValueError("compaction requires a new nonexistent root; nothing is overwritten")
    require_outside_checkout(target)
    paths = workspace.paths
    for path in (paths.root, paths.raw, paths.runs, paths.secrets, paths.backups, paths.runtime):
        if target.is_relative_to(path) or path.is_relative_to(target):
            raise ValueError("compaction root must lie outside the installation's paths")
    for store in paths.stores():
        if store.is_relative_to(target):
            raise ValueError("compaction root must lie outside the installation's paths")
    return target


def _sqlite(connection: sqlite3.Connection, target: Path) -> None:
    descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(descriptor)
    destination = sqlite3.connect(target)
    try:
        connection.backup(destination)
        destination.execute("VACUUM")
    finally:
        destination.close()


def _key(table: tuple[str, str]) -> str:
    return quoted(table[0]) + "." + quoted(table[1])


def _match(left: str, right: str, pairs: list[tuple[str, str]]) -> str:
    return " AND ".join(f"{left}.{quoted(a)} = {right}.{quoted(b)}" for a, b in pairs)


def _ordered_tables(
    connection: duckdb.DuckDBPyConnection,
) -> tuple[list[tuple[str, str]], dict[tuple[str, str], list[list[tuple[str, str]]]]]:
    """Source tables parent-first along their foreign keys, and each table's self references.

    A self reference is the list of (referencing, referenced) column pairs of one foreign
    key whose referenced table is the table itself.
    """
    tables = [
        (str(row[0]), str(row[1]))
        for row in connection.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name=? "
            "ORDER BY schema_name, table_name",
            [_SOURCE_ALIAS],
        ).fetchall()
    ]
    parents: dict[tuple[str, str], set[tuple[str, str]]] = {table: set() for table in tables}
    selves: dict[tuple[str, str], list[list[tuple[str, str]]]] = defaultdict(list)
    for schema, table, columns, referenced, referenced_columns in connection.execute(
        "SELECT schema_name, table_name, constraint_column_names, referenced_table, "
        "referenced_column_names FROM duckdb_constraints() "
        "WHERE database_name=? AND constraint_type='FOREIGN KEY' "
        "ORDER BY schema_name, table_name, constraint_index",
        [_SOURCE_ALIAS],
    ).fetchall():
        child, parent = (str(schema), str(table)), (str(schema), str(referenced))
        if child == parent:
            selves[child].append(list(zip(columns, referenced_columns, strict=True)))
        else:
            parents[child].add(parent)
    ordered: list[tuple[str, str]] = []
    placed: set[tuple[str, str]] = set()
    while len(ordered) < len(tables):
        ready = [t for t in tables if t not in placed and parents[t] <= placed]
        if not ready:
            raise ValueError("the market tables' foreign keys form a cycle")
        ordered.extend(ready)
        placed.update(ready)
    return ordered, selves


def _copy_rows(
    connection: duckdb.DuckDBPyConnection,
    target: str,
    table: tuple[str, str],
    selves: list[list[tuple[str, str]]],
) -> None:
    """Copy one table's rows; a self-referencing table goes one chain level at a time."""
    source = quoted(_SOURCE_ALIAS) + "." + _key(table)
    destination = quoted(target) + "." + _key(table)
    if not selves:
        connection.execute(f"INSERT INTO {destination} SELECT * FROM {source}")
        return
    # The referenced columns of a foreign key are unique, so they name a copied row.
    key = [(referenced, referenced) for _, referenced in selves[0]]
    copied = f"EXISTS (SELECT 1 FROM {destination} d WHERE {_match('d', 's', key)})"
    conditions = []
    for pairs in selves:
        unset = " OR ".join(f"s.{quoted(column)} IS NULL" for column, _ in pairs)
        parent = _match("p", "s", [(referenced, column) for column, referenced in pairs])
        conditions.append(f"({unset} OR EXISTS (SELECT 1 FROM {destination} p WHERE {parent}))")
    ready = " AND ".join(conditions)
    total = cast("tuple[int]", connection.execute(f"SELECT count(*) FROM {source}").fetchone())[0]
    done = 0
    while done < total:
        inserted = connection.execute(
            f"INSERT INTO {destination} SELECT s.* FROM {source} s WHERE NOT {copied} AND {ready}"
        ).fetchone()
        count = 0 if inserted is None else int(inserted[0])
        if count == 0:
            raise ValueError(f"rows of {table[1]} reference rows that are not in it")
        done += count


def _market(workspace: Workspace, target: Path, budget: ComputeBudget | None) -> None:
    """Copy every catalog object and row of the market store into a fresh DuckDB file.

    ``COPY FROM DATABASE`` copies data in no order its foreign keys respect, so the catalog
    is copied first and the rows after it, parent table first.
    """
    import duckdb  # noqa: PLC0415 -- DB-independent help/preview

    with workspace.checkpointed_market():
        resources = workspace.market_resources
        connection: duckdb.DuckDBPyConnection = duckdb.connect(
            str(target),
            config={
                "threads": cast("int", resources.get("threads", 2)),
                "memory_limit": str(resources.get("memory_limit", "512MB")),
            },
        )
        try:
            target.chmod(0o600)
            if budget is not None:
                limit_duckdb(connection, budget)
            source = str(workspace.paths.market).replace("'", "''")
            connection.execute(f"ATTACH '{source}' AS {_SOURCE_ALIAS} (READ_ONLY)")
            name = cast("tuple[str]", connection.execute("SELECT current_database()").fetchone())
            connection.execute(f"COPY FROM DATABASE {_SOURCE_ALIAS} TO {quoted(name[0])} (SCHEMA)")
            tables, selves = _ordered_tables(connection)
            # One statement per table (or chain level): a failure leaves the new root
            # incomplete, never the original changed.
            for table in tables:
                _copy_rows(connection, name[0], table, selves.get(table, []))
            connection.execute(f"DETACH {_SOURCE_ALIAS}")
            connection.execute("CHECKPOINT")
        finally:
            connection.close()


def compact(home: Path, to: Path, *, budget: ComputeBudget | None = None) -> dict[str, object]:
    """Rebuild the installation at ``home`` into the new root ``to`` and verify it there."""
    root = resolve_home(home)
    with open_workspace(root, writable=True) as workspace:
        before = verify_workspace(workspace, budget=budget)
        if before["pending_operations"] or before["orphan_generations"]:
            raise ValueError("compaction requires recovered operations and no orphan generations")
        if workspace.state.execute("SELECT 1 FROM runs WHERE status='RUNNING'").fetchone():
            raise ValueError("compaction requires all running analyses to stop")
        if workspace.strategies is None:
            raise ValueError("compaction requires all three stores")
        target = _target(workspace, to)
        private_directory(target, create=True)
        receipt = read_json(root / "installation.json")
        receipt["phase"] = "restoring"
        receipt["deployment_id"] = uuid.uuid4().hex
        write_json(target / "installation.json", receipt)
        # The new root keeps every setting except where its files are: all local now.
        runtime = read_json(root / "runtime.json")
        runtime["paths"] = dict(DEFAULT_PATHS)
        write_json(target / "runtime.json", runtime)
        original = {
            "state": workspace.paths.state,
            "strategies": workspace.paths.strategies,
            "market": workspace.paths.market,
        }
        compacted = {name: target / DEFAULT_PATHS[name] for name in original}
        try:
            _sqlite(workspace.state, compacted["state"])
            _sqlite(workspace.strategies, compacted["strategies"])
            _market(workspace, compacted["market"], budget)
            files: dict[str, object] = {}
            copy_tree(workspace.paths.raw, target / "raw", files, "raw/")
            copy_tree(workspace.paths.runs, target / "runs", files, "runs/")
            copy_tree(workspace.paths.secrets, target / "secrets", files, "secrets/")
            for name in ("backups", "runtime"):
                private_directory(target / name, create=True)
            with open_workspace(target, validating_restore=True) as rebuilt:
                after = verify_workspace(rebuilt, budget=budget)
            if after != before:
                raise ValueError("compacted logical verification differs from the original")  # noqa: TRY301 -- persist the failed receipt
        except BaseException:
            receipt["phase"] = "restore-incomplete"
            write_json(target / "installation.json", receipt)
            raise
        receipt["phase"] = "ready"
        write_json(target / "installation.json", receipt)
        return {
            "compacted": True,
            "home": str(target),
            "original_home": str(root),
            "original_unchanged": True,
            "verification": after,
            "bytes_before": _sizes(original),
            "bytes_after": _sizes(compacted),
            "raw_files": sum(1 for name in files if name.startswith("raw/")),
            "switch": "set AAS_HOME (or --home) to the new root; the original is not removed",
        }
