"""Read a pinned source table after validating its committed content.

This internal streaming path does not grant PIT or trading eligibility. Its
workspace admission must remain open for the lifetime of the iterator.
Consume or close each iterator before mutating source stores; verification and
streaming must not straddle a caller-managed write transaction.
"""

from __future__ import annotations

# ruff: noqa: S608 -- dynamic identifiers are manifest-owned and always quoted.
import base64
from collections.abc import Generator, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

from aegis_alpha.storage.source_library import list_sources, list_tables
from aegis_alpha.storage.source_library_digest import arrow_digest, sqlite_digest
from aegis_alpha.storage.source_library_schema import connections, quoted
from aegis_alpha.storage.state import get_operation

if TYPE_CHECKING:
    import sqlite3

    import duckdb

    from aegis_alpha.storage.workspace import Workspace

_MAX_BATCH_ROWS = 1_000_000


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
