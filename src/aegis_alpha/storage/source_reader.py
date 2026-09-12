"""Read a pinned source table after validating its committed content.

This internal streaming path does not grant PIT or trading eligibility. Its
workspace admission must remain open for the lifetime of the iterator.
Consume or close each iterator before mutating source stores; verification and
streaming must not straddle a caller-managed write transaction.
"""

from __future__ import annotations

# ruff: noqa: S608 -- dynamic identifiers are manifest-owned and always quoted.
import base64
import math
from collections.abc import Generator, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, assert_never, cast

from aegis_alpha.storage.source_library import list_sources, list_tables
from aegis_alpha.storage.source_library_digest import arrow_digest, sqlite_digest
from aegis_alpha.storage.source_library_schema import connections, quoted
from aegis_alpha.storage.state import get_operation

if TYPE_CHECKING:
    import sqlite3

    import duckdb
    import pyarrow as pa

    from aegis_alpha.storage.workspace import Workspace

_MAX_BATCH_ROWS = 1_000_000
MAX_INSPECTION_BYTES: Final = 1024 * 1024
type JsonValue = bool | int | float | str | list[JsonValue] | dict[str, JsonValue] | None


class SourceInspectionSizeError(ValueError):
    def __init__(self) -> None:
        super().__init__(f"source inspection exceeds {MAX_INSPECTION_BYTES}-byte budget")


@dataclass(frozen=True, slots=True)
class SourcePin:
    source_id: str
    source_sha256: str
    table: str
    table_digest: str

    def __post_init__(self) -> None:
        for name in (self.source_id, self.table):
            if not isinstance(name, str) or not name.strip():
                raise ValueError("source and table names must be nonempty")
            quoted(name)
        for digest in (self.source_sha256, self.table_digest):
            if (
                not isinstance(digest, str)
                or len(digest) != 64  # noqa: PLR2004 -- SHA-256 hex width
                or any(char not in "0123456789abcdef" for char in digest)
            ):
                raise ValueError("source pins require lowercase SHA-256 digests")


def _query(target: str, columns: Sequence[str]) -> str:
    return (
        "SELECT "
        + ",".join(quoted(column) for column in columns)
        + " FROM "
        + quoted(target)
        + " ORDER BY _aas_ordinal"
    )


def resolve_source(workspace: Workspace, pin: SourcePin) -> dict[str, object]:
    """Verify exactly the requested table and return its resolved descriptor."""
    source = next(
        (row for row in list_sources(workspace) if row["source_id"] == pin.source_id), None
    )
    if source is None or source["sha256"] != pin.source_sha256:
        raise ValueError("source is missing or its SHA-256 differs from the pin")
    table = next(
        (row for row in list_tables(workspace, pin.source_id) if row["name"] == pin.table), None
    )
    if table is None or table["digest"] != pin.table_digest:
        raise ValueError("source table is missing or its digest differs from the pin")
    conn = connections(workspace)[str(source["store"])]
    marker = conn.execute(
        "SELECT operation_id,request_hash,source_sha256 FROM source_library_commits "
        "WHERE source_id=?",
        [pin.source_id],
    ).fetchone()
    if marker is None:
        raise ValueError("source lacks a committed marker")
    operation = get_operation(workspace.state, str(marker[0]))
    if operation is None or (
        operation["phase"],
        operation["kind"],
        operation["request_hash"],
        operation["payload_hash"],
        operation["target_id"],
    ) != ("COMPLETED", "source_import", marker[1], marker[2], pin.source_id):
        raise ValueError("source lacks a matching completed operation")
    columns = cast("list[str]", table["columns"])
    query = _query(str(table["target"]), columns)
    if table["format"] == "sqlite":
        observed = sqlite_digest(cast("sqlite3.Connection", conn).execute(query))
    else:
        import pyarrow as pa  # noqa: PLC0415 -- optional Arrow source format

        schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(str(table["arrow_schema"]))))
        reader = cast("duckdb.DuckDBPyConnection", conn).execute(query).to_arrow_reader(65536)
        observed = arrow_digest(
            pa.RecordBatchReader.from_batches(schema, (batch.cast(schema) for batch in reader))
        )
    if observed != (table["rows"], pin.table_digest):
        raise ValueError("source table content or row count differs from the pin")
    return {**table, "store": source["store"], "source_only": True}


def iter_source_rows(
    workspace: Workspace,
    pin: SourcePin,
    *,
    columns: Sequence[str] | None = None,
    batch_size: int = 65536,
) -> Generator[tuple[Mapping[str, object], ...], None, None]:
    """Yield selected columns in bounded batches, without reading other tables."""
    if type(batch_size) is not int or not 1 <= batch_size <= _MAX_BATCH_ROWS:
        raise ValueError("batch_size must be an integer in [1, 1000000]")
    table = resolve_source(workspace, pin)
    available = cast("list[str]", table["columns"])
    selected = list(available if columns is None else columns)
    if (
        not selected
        or any(not isinstance(name, str) or name not in available for name in selected)
        or len(set(selected)) != len(selected)
    ):
        raise ValueError("selected columns must be unique members of the source schema")
    cursor = connections(workspace)[str(table["store"])].cursor()
    try:
        cursor.execute(_query(str(table["target"]), selected))
        while rows := cursor.fetchmany(batch_size):
            yield tuple(MappingProxyType(dict(zip(selected, row, strict=True))) for row in rows)
    finally:
        cursor.close()


def _inspection_size(column: str, kind: pa.DataType | None, depth: int = 0) -> str:
    """SQL upper bound for indented JSON, without serializing or fetching cell values."""
    overhead = 128 + 4 * depth  # scalar wrappers, punctuation and indent=2 at this depth
    if depth > 32:  # noqa: PLR2004 -- bound recursive SQL/JSON construction
        raise SourceInspectionSizeError
    if kind is None:  # SQLite dynamic scalars; BLOB length includes NUL and UTF-8 bytes.
        return f"{overhead}+6*coalesce(length(CAST({column} AS BLOB)),0)"
    import pyarrow as pa  # noqa: PLC0415 -- optional source inspection

    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        item = f"item_{depth}"
        child = _inspection_size(item, kind.value_type, depth + 1)
        # Do not build a length-vector for an already oversized list.
        return (
            f"CASE WHEN len({column})>{MAX_INSPECTION_BYTES // overhead} "
            f"THEN {MAX_INSPECTION_BYTES + 1} ELSE {overhead}+"
            f"coalesce(list_sum(list_transform({column},lambda {item}: {child})),0) END"
        )
    if pa.types.is_string(kind) or pa.types.is_large_string(kind):
        return f"{overhead}+coalesce(bit_length({column}),0)"
    if pa.types.is_binary(kind) or pa.types.is_large_binary(kind):
        return f"{overhead}+2*coalesce(octet_length({column}),0)"
    if (
        pa.types.is_boolean(kind)
        or pa.types.is_integer(kind)
        or pa.types.is_floating(kind)
        or pa.types.is_temporal(kind)
        or pa.types.is_decimal128(kind)
    ):
        return str(overhead)
    raise ValueError("unsupported source inspection type")


def _inspection_scalar(value: bytes | str | float | None) -> JsonValue:
    match value:
        case bytes():
            return {"base64": base64.b64encode(value).decode("ascii")}
        case float() if not math.isfinite(value):
            return {"float": value.hex()}
        case None | bool() | int() | float() | str():
            return value
        case unreachable:
            assert_never(unreachable)


def _inspection_arrow(value: pa.Scalar) -> JsonValue:
    import pyarrow as pa  # noqa: PLC0415 -- optional source inspection

    if not value.is_valid:
        return None
    match value:
        case pa.ListScalar() | pa.LargeListScalar():
            return [_inspection_arrow(item) for item in value.values]
        case _:  # Arrow has an open Scalar hierarchy; the source schema admits its leaves.
            if pa.types.is_temporal(value.type) or pa.types.is_decimal(value.type):
                return value.cast(pa.string()).as_py()
            return _inspection_scalar(value.as_py())


def inspect_source(
    workspace: Workspace, source_id: str, table_name: str, *, limit: int = 100
) -> dict[str, JsonValue]:
    """Inspect at most 1000 rows/1 MiB JSON under admission, not full digest verification.

    Conservative size accounting includes escaping, base64, keys and CLI indentation.
    The workspace lock protects both queries; no value query runs before admission.
    Full pinned streaming deliberately retains its separate verification contract.
    """
    if type(limit) is not int or not 1 <= limit <= 1000:  # noqa: PLR2004 -- existing read limit
        raise ValueError("source read limit must be from 1 to 1000")
    source = next((s for s in list_sources(workspace) if s["source_id"] == source_id), None)
    if source is None:
        raise ValueError("unknown or incomplete source")
    table = next((t for t in list_tables(workspace, source_id) if t["name"] == table_name), None)
    if table is None:
        raise ValueError("unknown source table")
    columns = cast("list[str]", table["columns"])  # validated by source-library import
    overhead = 256 + 6 * (len(source_id) + len(table_name))
    overhead += (limit + 1) * (32 + sum(6 * len(name) + 32 for name in columns))
    if overhead > MAX_INSPECTION_BYTES:
        raise SourceInspectionSizeError
    original = None
    if table["format"] == "arrow":
        import pyarrow as pa  # noqa: PLC0415 -- optional Arrow source format

        original = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(str(table["arrow_schema"]))))
    sizes = [
        _inspection_size(quoted(name), None if original is None else original.field(name).type)
        for name in columns
    ]
    conn = connections(workspace)[str(source["store"])]
    query = _query(str(table["target"]), columns) + " LIMIT ?"
    size = conn.execute(
        "SELECT coalesce(sum(" + "+".join(sizes) + "),0) FROM (" + query + ") AS inspection",
        [limit],
    ).fetchall()[0][0]
    if size + overhead > MAX_INSPECTION_BYTES:
        raise SourceInspectionSizeError
    rows: list[JsonValue]
    if original is None:
        cursor = conn.execute(query, [limit])
        rows = [
            dict(zip(columns, map(_inspection_scalar, row), strict=True))
            for row in cursor.fetchall()
        ]
    else:
        values = workspace.market.execute(query, [limit]).to_arrow_table().cast(original)
        rows = [
            {name: _inspection_arrow(values[name][i]) for name in columns}
            for i in range(values.num_rows)
        ]
    return {
        "source_id": source_id,
        "table": table_name,
        "columns": list(columns),
        "rows": rows,
        "source_only": True,
    }
