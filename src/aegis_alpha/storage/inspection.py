"""Bounded metadata and text previews for local consumers; no source writes."""

from __future__ import annotations

# ruff: noqa: S608 -- table/column identifiers come from manifests and are quoted.
import json
import sqlite3
from typing import TYPE_CHECKING, cast

from aegis_alpha.storage import source_library_schema as schema
from aegis_alpha.storage.state import get_operation

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

LIMIT = 500
SOURCE_LIMIT = 5000
TABLE_LIMIT = 6000
MANIFEST_BYTES = 262144
CATALOG_BYTES = 32 * 1024 * 1024
COLUMN_LIMIT = 64
CELL_CHARS = 2000
PREVIEW_ROWS = 50


def list_datasets(connection: sqlite3.Connection) -> list[dict[str, object]]:
    return [
        dict(row)
        for row in connection.execute(
            "SELECT dataset_id,version,generation_id,row_count,coverage,status,chain_hash "
            "FROM dataset_versions ORDER BY dataset_id,sequence LIMIT ?",
            (LIMIT,),
        )
    ]


def list_runs(connection: sqlite3.Connection) -> list[dict[str, object]]:
    return [
        dict(row)
        for row in connection.execute(
            "SELECT run_id,bundle_id,status,created_at_us,completed_at_us,result_hash "
            "FROM runs ORDER BY created_at_us DESC,run_id LIMIT ?",
            (LIMIT,),
        )
    ]


def _manifest(encoded: object) -> dict[str, object]:
    if encoded is None:
        raise ValueError("source manifest exceeds console metadata limit")
    value = json.loads(str(encoded))
    if not isinstance(value, dict) or not isinstance(value.get("tables"), list):
        raise TypeError("invalid source manifest")
    return value


def _visible(workspace: Workspace, operation_id: str, request_hash: str) -> bool:
    operation = get_operation(workspace.state, operation_id)
    return bool(
        operation
        and operation["phase"] == "COMPLETED"
        and operation["request_hash"] == request_hash
    )


def source_catalog(workspace: Workspace) -> tuple[list[dict[str, object]], int]:
    """Apply SQL limits before loading manifests and cap aggregate table descriptors."""
    if not schema.ensure(workspace):
        return [], 0
    sources: list[dict[str, object]] = []
    scanned = table_count = total = metadata_bytes = 0
    for kind, connection in schema.connections(workspace).items():
        total += int(
            connection.execute("SELECT count(*) FROM source_library_commits").fetchall()[0][0]
        )
        cursor = connection.execute(
            "SELECT source_id,operation_id,request_hash,source_sha256,"
            "CASE WHEN length(manifest_json)<=? THEN manifest_json ELSE NULL END "
            "FROM source_library_commits ORDER BY source_id LIMIT ?",
            [MANIFEST_BYTES, max(0, SOURCE_LIMIT - scanned)],
        )
        while row := cursor.fetchone():
            source_id, operation_id, request_hash, digest, encoded = row
            metadata_bytes += len(str(encoded).encode())
            if metadata_bytes > CATALOG_BYTES:
                break
            scanned += 1
            if not _visible(workspace, operation_id, request_hash):
                continue
            tables = []
            if encoded is not None:
                manifest = _manifest(encoded)
                for table in cast("list[dict[str, object]]", manifest["tables"]):
                    if table_count >= TABLE_LIMIT:
                        break
                    columns = table.get("columns")
                    if not isinstance(columns, list) or any(
                        not isinstance(c, str) for c in columns
                    ):
                        raise ValueError("invalid source table columns")
                    tables.append(
                        {
                            "name": table["name"],
                            "rows": table["rows"],
                            "columns": columns[:COLUMN_LIMIT],
                            "digest": table["digest"],
                        }
                    )
                    table_count += 1
            sources.append(
                {
                    "source_id": source_id,
                    "store": kind,
                    "sha256": digest,
                    "source_only": True,
                    "tables": tables,
                    "metadata_limited": encoded is None or table_count >= TABLE_LIMIT,
                }
            )
    sources.sort(key=lambda source: str(source["source_id"]))
    return sources, total


def preview_source(workspace: Workspace, source_id: str, table_name: str) -> dict[str, object]:
    """Truncate text in SQL; binary and complex values never enter Python."""
    if not schema.ensure(workspace):
        raise LookupError("source library absent")
    for connection in schema.connections(workspace).values():
        row = connection.execute(
            "SELECT operation_id,request_hash,"
            "CASE WHEN length(manifest_json)<=? THEN manifest_json ELSE NULL END "
            "FROM source_library_commits WHERE source_id=?",
            [MANIFEST_BYTES, source_id],
        ).fetchone()
        if row is None or not _visible(workspace, str(row[0]), str(row[1])):
            continue
        manifest = _manifest(row[2])
        table = next(
            (
                t
                for t in cast("list[dict[str, object]]", manifest["tables"])
                if t.get("name") == table_name
            ),
            None,
        )
        if table is None:
            raise LookupError("table absent")
        columns = cast("list[str]", table["columns"])[:COLUMN_LIMIT]
        target = schema.quoted(str(table["target"]))
        if isinstance(connection, sqlite3.Connection):
            expressions = [
                f"CASE WHEN typeof({schema.quoted(c)})='blob' THEN '[binary omitted]' "
                f"WHEN typeof({schema.quoted(c)})='text' "
                f"THEN substr({schema.quoted(c)},1,{CELL_CHARS}) "
                f"ELSE {schema.quoted(c)} END"
                for c in columns
            ]
        else:
            description = connection.execute(f"DESCRIBE SELECT * FROM {target}").fetchall()
            types = {row[0]: row[1] for row in description}
            expressions = [_duck_expression(c, types[c]) for c in columns]
        values = connection.execute(
            f"SELECT {','.join(expressions)} FROM {target} ORDER BY _aas_ordinal LIMIT ?",
            [PREVIEW_ROWS],
        ).fetchall()
        return {
            "source_id": source_id,
            "table": table_name,
            "columns": columns,
            "rows": [dict(zip(columns, row, strict=True)) for row in values],
            "source_only": True,
            "limit": PREVIEW_ROWS,
            "truncated": int(str(table["rows"])) > PREVIEW_ROWS,
            "text_truncated": True,
            "preview_only": True,
        }
    raise LookupError("source absent")


def _duck_expression(column: str, kind: str) -> str:
    quoted = schema.quoted(column)
    if kind == "VARCHAR":
        return f"left({quoted},{CELL_CHARS})"
    if kind in {
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "FLOAT",
        "DOUBLE",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "TIMESTAMP_NS",
        "TIMESTAMP WITH TIME ZONE",
    } or kind.startswith("DECIMAL("):
        return f"CAST({quoted} AS VARCHAR)"
    return "'[binary or complex value omitted]'"
