"""Lossless source-only catalogs, separate from executable bundles and PIT publications."""

from __future__ import annotations

# ruff: noqa: S608 -- every dynamic identifier passes schema.quoted; values are bound.
# ruff: noqa: TRY301 -- transaction owner rolls back all boundary validation failures.
import base64
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, cast

from aegis_alpha.storage import source_library_schema as schema
from aegis_alpha.storage.locks import private_file
from aegis_alpha.storage.source_library_digest import arrow_digest, sqlite_digest
from aegis_alpha.storage.state import complete_operation, get_operation, prepare_operation

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

    from aegis_alpha.storage.workspace import Workspace


_MAX_SOURCE_ID = 240
_SHA_LENGTH = 64
_MAX_READ_ROWS = 1000


def _identity(source_id: str, digest: str) -> None:
    schema.quoted(source_id)
    if (
        len(source_id) > _MAX_SOURCE_ID
        or len(digest) != _SHA_LENGTH
        or any(c not in "0123456789abcdef" for c in digest)
    ):
        raise ValueError("source ID or sha256 is invalid")


def _marker(workspace: Workspace, source_id: str) -> tuple[object, ...] | None:
    for conn in schema.connections(workspace).values():
        if schema.admit(conn, create=False):
            row = conn.execute(
                "SELECT operation_id,request_hash,source_sha256,store_kind,manifest_json "
                "FROM source_library_commits WHERE source_id=?",
                [source_id],
            ).fetchone()
            if row is not None:
                return tuple(row)
    return None


def _prepare(
    workspace: Workspace, source_id: str, digest: str, kind: str, detail: object
) -> tuple[str, str, dict[str, object] | None]:
    _identity(source_id, digest)
    schema.ensure(workspace, create=True)
    request = hashlib.sha256(schema.encoded([source_id, digest, kind, detail]).encode()).hexdigest()
    op_id = "source:" + hashlib.sha256(source_id.encode()).hexdigest()
    previous = _marker(workspace, source_id)
    if previous and (previous[0], previous[1], previous[2], previous[3]) != (
        op_id,
        request,
        digest,
        kind,
    ):
        raise ValueError("source ID already identifies different content")
    prepare_operation(
        workspace.state,
        operation_id=op_id,
        kind="source_import",
        request_hash=request,
        target_id=source_id,
        expected_parent=None,
        payload_hash=digest,
    )
    if previous:
        manifest = json.loads(str(previous[4]))
        _verify_manifest(workspace, manifest)
        complete_operation_if_pending(workspace, op_id, request)
        return (
            op_id,
            request,
            {"source_id": source_id, "reused": True, "tables": manifest["tables"]},
        )
    return op_id, request, None


def complete_operation_if_pending(workspace: Workspace, op_id: str, request: str) -> None:
    operation = get_operation(workspace.state, op_id)
    if operation and operation["phase"] == "PREPARED":
        complete_operation(workspace.state, op_id, request)


def _commit(  # noqa: PLR0913, PLR0917 -- explicit cross-store commit identity
    workspace: Workspace,
    source_id: str,
    digest: str,
    kind: str,
    op_id: str,
    request: str,
    tables: list[dict[str, object]],
    metadata: object = None,
) -> dict[str, object]:
    conn = schema.connections(workspace)[kind]
    manifest = {"source_id": source_id, "store": kind, "tables": tables, "metadata": metadata}
    conn.execute(
        "INSERT INTO source_library_commits VALUES (?,?,?,?,?,?)",
        [source_id, op_id, request, digest, kind, schema.encoded(manifest)],
    )
    conn.execute("COMMIT")
    complete_operation(workspace.state, op_id, request)
    return {"source_id": source_id, "reused": False, "tables": tables}


def import_sqlite(  # noqa: C901, PLR0912, PLR0915 -- one snapshot transaction boundary
    workspace: Workspace, path: Path, source_id: str, sha256: str, *, store: str = "strategies"
) -> dict[str, object]:
    """Import a closed, private SQLite backup snapshot without replaying its DDL."""
    if store != "strategies":
        raise ValueError("SQLite source catalogs use the private strategy store")
    info = private_file(path)
    if any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-journal")):
        raise ValueError("source requires a closed SQLite backup snapshot without WAL/journal")
    with path.open("rb") as handle:
        if hashlib.file_digest(handle, "sha256").hexdigest() != sha256:
            raise ValueError("source sha256 mismatch")
    op_id, request, reused = _prepare(workspace, source_id, sha256, store, None)
    if reused:
        return reused
    destination = cast("sqlite3.Connection", schema.connections(workspace)[store])
    origin = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    origin.execute("PRAGMA query_only=ON")
    origin.execute("PRAGMA trusted_schema=OFF")
    origin.execute("BEGIN")
    tables: list[dict[str, object]] = []
    try:
        definitions = origin.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        destination.execute("BEGIN TRANSACTION")
        for name, ddl in definitions:
            if "VIRTUAL TABLE" in (ddl or "").upper():
                raise ValueError("virtual source tables are unsupported")
            fields = origin.execute("PRAGMA table_xinfo(" + schema.quoted(name) + ")").fetchall()
            if any(row[6] for row in fields):
                raise ValueError("generated source columns are unsupported")
            columns = [row[1] for row in fields]
            if "_aas_ordinal" in {c.casefold() for c in columns}:
                raise ValueError("reserved SQLite source column")
            target = "sl_" + hashlib.sha256(schema.encoded([source_id, name]).encode()).hexdigest()
            destination.execute(
                "CREATE TABLE "
                + schema.quoted(target)
                + " (_aas_ordinal INTEGER PRIMARY KEY,"
                + ",".join(schema.quoted(c) + " ANY" for c in columns)
                + ") STRICT"
            )
            selection = ",".join(map(schema.quoted, columns))
            source_rows = origin.execute("SELECT " + selection + " FROM " + schema.quoted(name))
            count = 0
            while batch := source_rows.fetchmany(10000):
                destination.executemany(
                    "INSERT INTO "
                    + schema.quoted(target)
                    + " VALUES ("
                    + ",".join("?" for _ in range(len(columns) + 1))
                    + ")",
                    [(count + n, *row) for n, row in enumerate(batch)],
                )
                count += len(batch)
            observed, digest = sqlite_digest(
                destination.execute(
                    "SELECT "
                    + selection
                    + " FROM "
                    + schema.quoted(target)
                    + " ORDER BY _aas_ordinal"
                )
            )
            original_count, original_digest = sqlite_digest(
                origin.execute("SELECT " + selection + " FROM " + schema.quoted(name))
            )
            if (observed, digest) != (original_count, original_digest) or count != observed:
                raise ValueError("source SQLite reconciliation failed")
            tables.append(
                {
                    "name": name,
                    "target": target,
                    "rows": count,
                    "digest": digest,
                    "columns": columns,
                    "original_ddl": ddl,
                    "format": "sqlite",
                }
            )
        after = private_file(path)
        if (info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise ValueError("SQLite snapshot changed during import")
        return _commit(workspace, source_id, sha256, store, op_id, request, tables)
    except BaseException:
        if destination.in_transaction:
            destination.rollback()
        raise
    finally:
        origin.close()


def import_arrow(  # noqa: PLR0913 -- public provenance and reader inputs
    workspace: Workspace,
    source_id: str,
    sha256: str,
    table_name: str,
    reader: pa.RecordBatchReader,
    *,
    metadata: object = None,
) -> dict[str, object]:
    from aegis_alpha.storage.source_library_arrow import ingest_arrow  # noqa: PLC0415

    return ingest_arrow(workspace, source_id, sha256, table_name, reader, metadata=metadata)


def _verify_manifest(workspace: Workspace, manifest: dict[str, object]) -> None:
    import pyarrow as pa  # noqa: PLC0415

    conn = schema.connections(workspace)[str(manifest["store"])]
    for table in cast("list[dict[str, object]]", manifest["tables"]):
        columns = cast("list[str]", table["columns"])
        order = "_aas_ordinal"
        query = (
            "SELECT "
            + ",".join(map(schema.quoted, columns))
            + " FROM "
            + schema.quoted(str(table["target"]))
            + " ORDER BY "
            + order
        )
        if table["format"] == "sqlite":
            observed = sqlite_digest(cast("sqlite3.Connection", conn).execute(query))
        else:
            original = pa.ipc.read_schema(
                pa.BufferReader(base64.b64decode(str(table["arrow_schema"])))
            )
            batches = cast("duckdb.DuckDBPyConnection", conn).execute(query).to_arrow_reader(65536)
            cast_reader = pa.RecordBatchReader.from_batches(
                original, (batch.cast(original) for batch in batches)
            )
            observed = arrow_digest(cast_reader)
        if observed != (table["rows"], table["digest"]):
            raise ValueError("source library content/count mismatch")


def list_sources(workspace: Workspace) -> list[dict[str, object]]:
    if not schema.ensure(workspace):
        return []
    result = []
    for kind, conn in schema.connections(workspace).items():
        for row in conn.execute(
            "SELECT source_id,operation_id,request_hash,source_sha256,manifest_json "
            "FROM source_library_commits"
        ).fetchall():
            operation = get_operation(workspace.state, row[1])
            if (
                operation
                and operation["phase"] == "COMPLETED"
                and operation["request_hash"] == row[2]
            ):
                result.append(
                    {"source_id": row[0], "store": kind, "sha256": row[3], "source_only": True}
                )
    return sorted(result, key=lambda value: str(value["source_id"]))


def _visible(workspace: Workspace, source_id: str) -> dict[str, object]:
    if source_id not in {row["source_id"] for row in list_sources(workspace)}:
        raise ValueError("unknown or incomplete source")
    marker = _marker(workspace, source_id)
    if marker is None:
        raise ValueError("source marker missing")
    return json.loads(str(marker[4]))


def list_tables(workspace: Workspace, source_id: str) -> list[dict[str, object]]:
    return cast("list[dict[str, object]]", _visible(workspace, source_id)["tables"])


def read_table(
    workspace: Workspace, source_id: str, table_name: str, limit: int = 100
) -> dict[str, object]:
    from aegis_alpha.storage.source_library_arrow import json_rows  # noqa: PLC0415

    if type(limit) is not int or not 1 <= limit <= _MAX_READ_ROWS:
        raise ValueError("source read limit must be from 1 to 1000")
    manifest = _visible(workspace, source_id)
    table = next(
        (t for t in cast("list[dict[str, object]]", manifest["tables"]) if t["name"] == table_name),
        None,
    )
    if table is None:
        raise ValueError("unknown source table")
    columns = cast("list[str]", table["columns"])
    conn = schema.connections(workspace)[str(manifest["store"])]
    order = "_aas_ordinal"
    query = (
        "SELECT "
        + ",".join(map(schema.quoted, columns))
        + " FROM "
        + schema.quoted(str(table["target"]))
        + " ORDER BY "
        + order
        + " LIMIT ?"
    )
    cursor = conn.execute(query, [limit])
    rows = json_rows(cursor, table)
    return {
        "source_id": source_id,
        "table": table_name,
        "columns": columns,
        "rows": rows,
        "source_only": True,
    }


def verify_sources(workspace: Workspace) -> dict[str, object] | None:
    if not schema.ensure(workspace):
        return None
    total = tables = sources = 0
    for conn in schema.connections(workspace).values():
        for row in conn.execute(
            "SELECT source_id,operation_id,request_hash,source_sha256,manifest_json "
            "FROM source_library_commits"
        ).fetchall():
            operation = get_operation(workspace.state, row[1])
            if not operation or (
                operation["kind"],
                operation["request_hash"],
                operation["target_id"],
                operation["payload_hash"],
            ) != ("source_import", row[2], row[0], row[3]):
                raise ValueError("source marker/intent mismatch")
            manifest = json.loads(row[4])
            _verify_manifest(workspace, manifest)
            if operation["phase"] == "COMPLETED":
                sources += 1
                tables += len(manifest["tables"])
                total += sum(t["rows"] for t in manifest["tables"])
    for row in workspace.state.execute(
        "SELECT target_id FROM storage_operations WHERE kind='source_import' AND phase='COMPLETED'"
    ):
        if _marker(workspace, row[0]) is None:
            raise ValueError("completed source intent lacks target marker")
    return {"sources": sources, "tables": tables, "rows": total}


def recover_source(workspace: Workspace, operation_id: str) -> bool:
    """Finish a prepared source intent only after verifying its committed target."""
    operation = get_operation(workspace.state, operation_id)
    if operation is None or operation["kind"] != "source_import":
        raise ValueError("expected a source import operation")
    if not schema.ensure(workspace):
        return False
    marker = _marker(workspace, str(operation["target_id"]))
    if marker is None:
        return False
    if tuple(marker[:3]) != (
        operation_id,
        operation["request_hash"],
        operation["payload_hash"],
    ):
        raise ValueError("source marker/intent mismatch")
    _verify_manifest(workspace, json.loads(str(marker[4])))
    complete_operation_if_pending(workspace, operation_id, str(operation["request_hash"]))
    return True
