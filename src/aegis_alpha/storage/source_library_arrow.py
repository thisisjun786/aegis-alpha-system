"""Arrow-native ingestion and bounded JSON inspection for source-only tables."""

from __future__ import annotations

# ruff: noqa: S608 -- generated and quoted identifiers only; source values use Arrow.
import base64
import hashlib
import math
import sqlite3
from contextlib import suppress
from decimal import Decimal
from typing import TYPE_CHECKING, cast

from aegis_alpha.storage import source_library_schema as schema
from aegis_alpha.storage.source_library_digest import arrow_digest, canonical_batch, fixed_batches

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

    from aegis_alpha.storage.workspace import Workspace


def _supported_type(kind: pa.DataType) -> bool:
    """Admit canonical scalar types and variable lists of those types only."""
    import pyarrow as pa  # noqa: PLC0415

    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        return _supported_type(kind.value_type)
    return (
        pa.types.is_boolean(kind)
        or pa.types.is_integer(kind)
        or pa.types.is_float32(kind)
        or pa.types.is_float64(kind)
        or pa.types.is_string(kind)
        or pa.types.is_large_string(kind)
        or pa.types.is_binary(kind)
        or pa.types.is_large_binary(kind)
        or pa.types.is_date(kind)
        or pa.types.is_time(kind)
        or pa.types.is_timestamp(kind)
        or (pa.types.is_decimal128(kind) and 0 <= kind.scale <= kind.precision)
    )


def ingest_arrow(  # noqa: PLR0913 -- explicit provenance and reader input
    workspace: Workspace,
    source_id: str,
    sha256: str,
    table_name: str,
    reader: pa.RecordBatchReader,
    *,
    metadata: object = None,
) -> dict[str, object]:
    import duckdb  # noqa: PLC0415
    import pyarrow as pa  # noqa: PLC0415

    from aegis_alpha.storage.source_library import (  # noqa: PLC0415
        _commit,
        _prepare,
        _verify_manifest,
    )

    schema.quoted(table_name)
    original = reader.schema
    for name in original.names:
        schema.quoted(name)
    if len({n.casefold() for n in original.names}) != len(original.names) or "_aas_ordinal" in {
        n.casefold() for n in original.names
    }:
        raise ValueError("ambiguous or reserved Arrow column")
    for field in original:
        if not _supported_type(field.type):
            raise ValueError(f"unsupported Arrow source type for {field.name!r}: {field.type}")
    serialized = base64.b64encode(original.serialize().to_pybytes()).decode()
    op_id, request, reused = _prepare(
        workspace, source_id, sha256, "market", [table_name, serialized, metadata]
    )
    if reused:
        observed = arrow_digest(reader)
        previous = cast("list[dict[str, object]]", reused["tables"])[0]
        if observed != (previous["rows"], previous["digest"]):
            raise ValueError("replayed Arrow source has different content")
        return reused
    target = "sl_" + hashlib.sha256(schema.encoded([source_id, table_name]).encode()).hexdigest()
    conn = workspace.market
    augmented = original.append(pa.field("_aas_ordinal", pa.int64()))
    empty = pa.Table.from_batches([], schema=augmented)
    selection = ",".join(map(schema.quoted, augmented.names))
    count = 0
    digest = hashlib.sha256()
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.register("source_incoming", empty)
        conn.execute(
            "CREATE TABLE "
            + schema.quoted(target)
            + " AS SELECT "
            + selection
            + " FROM source_incoming"
        )
        conn.unregister("source_incoming")
        for batch in fixed_batches(reader):
            digest.update(canonical_batch(batch))
            ordinal = pa.array(range(count, count + len(batch)), type=pa.int64())
            incoming = pa.RecordBatch.from_arrays([*batch.columns, ordinal], schema=augmented)
            conn.register("source_incoming", incoming)
            conn.execute(
                "INSERT INTO "
                + schema.quoted(target)
                + " SELECT "
                + selection
                + " FROM source_incoming"
            )
            conn.unregister("source_incoming")
            count += len(batch)
        table = {
            "name": table_name,
            "target": target,
            "rows": count,
            "digest": digest.hexdigest(),
            "columns": original.names,
            "arrow_schema": serialized,
            "format": "arrow",
        }
        _verify_manifest(workspace, {"store": "market", "tables": [table]})
        return _commit(workspace, source_id, sha256, "market", op_id, request, [table], metadata)
    except BaseException:
        # A failed state completion follows a committed target; preserve that exception.
        with suppress(duckdb.TransactionException):
            conn.execute("ROLLBACK")
        raise
    finally:
        conn.unregister("source_incoming")


def _json(value: object) -> object:
    if isinstance(value, bytes):
        return {"base64": base64.b64encode(value).decode()}
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return {"float": value.hex()}
    return value


def json_rows(
    cursor: sqlite3.Cursor | duckdb.DuckDBPyConnection, table: dict[str, object]
) -> list[dict[str, object]]:
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.compute as pc  # noqa: PLC0415

    if isinstance(cursor, sqlite3.Cursor):
        return [
            dict(zip(cast("list[str]", table["columns"]), map(_json, row), strict=True))
            for row in cursor.fetchall()
        ]
    original = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(str(table["arrow_schema"]))))
    values = cursor.to_arrow_table().cast(original)
    columns = {}
    for field, column in zip(original, values.columns, strict=True):
        if pa.types.is_temporal(field.type) or pa.types.is_decimal(field.type):
            columns[field.name] = pc.cast(column, pa.string()).to_pylist()
        else:
            columns[field.name] = [_json(value) for value in column.to_pylist()]
    return [dict(zip(columns, row, strict=True)) for row in zip(*columns.values(), strict=True)]
