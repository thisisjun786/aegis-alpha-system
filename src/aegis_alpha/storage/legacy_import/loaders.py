"""The legacy loader contract and the shared helpers its loaders use.

A loader is registered as ``<provider>.<shape>@<major>``. It fixes three things:

- **units**: which original files form one complete unit whose boundary the bytes fix (one
  export batch's result file and the CSV files it lists, one capture directory's journal and
  the files it names, one archive and its receipt). Each unit becomes one source-library
  commit per output table, so re-running a manifest, or splitting it, yields the same IDs.
- **tables**: the provider, shape, table name and exact Arrow schema of each output. The
  major is the output schema major of the source ID, so it rises only when columns change.
- **rows**: every row comes from the unit's bytes. Text stays the original text (dates and
  decimal numbers are never parsed into another type), JSON values keep their JSON form,
  and nothing is filled, trimmed, deduplicated or reordered. A unit whose bytes contradict
  their own index (a hash, a row count, a header, a date range) is refused, never repaired.

``metrics`` are the reconciliation counts a loader reports for its entry; a manifest's
``expect`` names some of them.
"""

from __future__ import annotations

# ruff: noqa: TRY004 -- untrusted legacy bytes raise one ingress error type, ValueError.
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Protocol

from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage import source_library_schema as schema

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from pathlib import Path

    import pyarrow as pa

    from aegis_alpha.storage.legacy_import.files import Bytes, OriginalBytes
    from aegis_alpha.storage.legacy_import.manifest import Entry

MAX_INDEX_BYTES: Final = 256 * 1024 * 1024
MAX_FILE_BYTES: Final = 256 * 1024 * 1024
SCHEMA_MAJOR: Final = 1


@dataclass(frozen=True, slots=True)
class Table:
    """One output table of a loader: its source ID prefix and exact columns."""

    provider: str
    shape: str
    name: str
    columns: tuple[tuple[str, str], ...]

    def schema(self) -> pa.Schema:
        import pyarrow as pa  # noqa: PLC0415 -- the Arrow loaders need the legacy extra

        kinds = {
            "string": pa.string(),
            "int64": pa.int64(),
            "float64": pa.float64(),
            "date32": pa.date32(),
            "timestamp_us_utc": pa.timestamp("us", tz="UTC"),
        }
        return pa.schema([(name, kinds[kind]) for name, kind in self.columns])


@dataclass(frozen=True, slots=True)
class Unit:
    """One complete original unit: its name below the entry root and every file it covers."""

    name: str
    files: tuple[Path, ...]
    tables: tuple[Table, ...]
    context: Mapping[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class Run:
    """Per-entry reconciliation state shared by the entry's units."""

    metrics: Counter[str] = field(default_factory=Counter)
    state: dict[str, object] = field(default_factory=dict)


class Loader(Protocol):
    """A registered legacy format reader."""

    name: str
    arg_names: frozenset[str]
    metric_names: frozenset[str]

    def units(self, entry: Entry, source: OriginalBytes, run: Run) -> list[Unit]:
        """Discover the entry's units, reading only the index files discovery needs."""
        ...

    def batches(
        self, unit: Unit, table: Table, source: Bytes, run: Run
    ) -> Iterator[pa.RecordBatch]:
        """Yield the table's rows from the unit's bytes, validating them as they are read."""
        ...

    def finish(self, entry: Entry, source: OriginalBytes, run: Run) -> None:
        """Add cross-unit reconciliation metrics after every unit was read."""
        ...


def admit_entry(loader: Loader, entry: Entry) -> None:
    """Refuse an argument or expected metric the loader does not define, before any read."""
    unknown = sorted(set(entry.args) - loader.arg_names)
    if unknown:
        raise ValueError(f"loader {entry.loader} has no argument {unknown[0]!r}")
    metrics = sorted(set(entry.expect) - loader.metric_names)
    if metrics:
        raise ValueError(f"loader {entry.loader} reports no metric {metrics[0]!r}")


def json_document(payload: bytes, name: str) -> object:
    try:
        return decode_json(payload)
    except ValueError:
        raise ValueError(f"legacy file is not strict JSON: {name}") from None


def json_object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"legacy {name} must be a JSON object")
    return value


def json_text(value: object) -> str:
    """A nested JSON value as canonical JSON text (sorted keys, no spaces)."""
    return schema.encoded(value)


def text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"legacy {name} must be text")
    return value


def integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"legacy {name} must be an integer")
    return value


def record_batch(table: Table, rows: list[tuple[object, ...]]) -> pa.RecordBatch:
    import pyarrow as pa  # noqa: PLC0415 -- the Arrow loaders need the legacy extra

    target = table.schema()
    columns = list(zip(*rows, strict=True)) if rows else [() for _ in table.columns]
    return pa.record_batch(
        [
            pa.array(list(values), type=column.type)
            for values, column in zip(columns, target, strict=True)
        ],
        schema=target,
    )


def json_records(
    table: Table, records: object, name: str, *, nested: frozenset[str] = frozenset()
) -> list[tuple[object, ...]]:
    """Rows from a list of JSON objects whose keys are exactly the table's columns.

    ``nested`` columns hold canonical JSON text; other columns take the JSON scalar of the
    declared type or null. Any other key, missing key or type is refused, so no value of
    the original object is dropped or coerced.
    """
    if not isinstance(records, list):
        raise ValueError(f"legacy {name} must be a JSON list")
    names = [column for column, _ in table.columns]
    kinds = dict(table.columns)
    rows: list[tuple[object, ...]] = []
    for record in records:
        if not isinstance(record, dict) or set(record) != set(names):
            raise ValueError(f"legacy {name} record keys must be exactly {sorted(names)}")
        row: list[object] = []
        for column in names:
            value = record[column]
            if column in nested:
                row.append(json_text(value))
            elif value is None:
                row.append(None)
            elif kinds[column] == "string":
                row.append(text(value, f"{name}.{column}"))
            else:
                row.append(integer(value, f"{name}.{column}"))
        rows.append(tuple(row))
    return rows


def csv_table(payload: bytes, header: tuple[str, ...], name: str) -> list[pa.Array]:
    """Parse a CSV whose first line is exactly ``header`` into one text array per column.

    Every value stays its original text: no type inference, no quote processing, no
    trimming and no null conversion. Line ends are ``\\n`` or ``\\r\\n``; a bare
    carriage return, an empty line or a row of another width is refused.
    """
    import pyarrow as pa  # noqa: PLC0415 -- the Arrow loaders need the legacy extra
    from pyarrow import csv  # noqa: PLC0415

    first = payload.split(b"\n", 1)[0].removesuffix(b"\r")
    if first != ",".join(header).encode():
        raise ValueError(f"legacy CSV header is not {','.join(header)}: {name}")
    if payload.count(b"\r") != payload.count(b"\r\n"):
        raise ValueError(f"legacy CSV has a bare carriage return: {name}")
    names = [f"c{index}" for index in range(len(header))]
    try:
        table = csv.read_csv(
            pa.BufferReader(payload),
            read_options=csv.ReadOptions(column_names=names, skip_rows=1, use_threads=False),
            parse_options=csv.ParseOptions(
                quote_char=False,
                double_quote=False,
                escape_char=False,
                newlines_in_values=False,
                ignore_empty_lines=False,
            ),
            convert_options=csv.ConvertOptions(
                column_types=dict.fromkeys(names, pa.string()),
                strings_can_be_null=False,
                quoted_strings_can_be_null=False,
                null_values=[],
                check_utf8=True,
            ),
        )
    except (pa.ArrowInvalid, pa.ArrowTypeError) as error:
        raise ValueError(f"legacy CSV is not a plain {len(header)}-column table: {name}") from (
            error
        )
    return [column.combine_chunks() for column in table.columns]
