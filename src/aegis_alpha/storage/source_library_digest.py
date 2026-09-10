"""Bounded, typed source digests independent of Arrow batch boundaries."""

from __future__ import annotations

import base64
import hashlib
import math
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING

from aegis_alpha.storage.source_library_schema import encoded

if TYPE_CHECKING:
    import pyarrow as pa

BATCH_ROWS = 65536


def scalar(value: object) -> object:
    if isinstance(value, bytes):
        return {"base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, float):
        return {"float_hex": value.hex()}
    return value


def sqlite_digest(rows: Iterable[tuple[object, ...]]) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        digest.update((encoded([scalar(value) for value in row]) + "\n").encode())
        count += 1
    return count, digest.hexdigest()


def _single_batch(table: pa.Table) -> pa.RecordBatch:
    batches = table.to_batches()
    if len(batches) != 1 or batches[0].num_rows != table.num_rows:
        raise ValueError("source rows must fit one canonical Arrow batch; use large-offset arrays")
    return batches[0]


def fixed_batches(reader: pa.RecordBatchReader) -> Iterator[pa.RecordBatch]:
    import pyarrow as pa  # noqa: PLC0415

    pending: list[pa.RecordBatch] = []
    size = 0
    for batch in reader:
        offset = 0
        while offset < len(batch):
            length = min(BATCH_ROWS - size, len(batch) - offset)
            pending.append(batch.slice(offset, length))
            size += length
            offset += length
            if size == BATCH_ROWS:
                yield _single_batch(
                    pa.Table.from_batches(pending, schema=reader.schema).combine_chunks()
                )
                pending, size = [], 0
    if pending:
        yield _single_batch(pa.Table.from_batches(pending, schema=reader.schema).combine_chunks())


def canonical_batch(batch: pa.RecordBatch) -> bytes:
    """Normalize null payload buffers and NaNs before serializing typed Arrow values."""
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.compute as pc  # noqa: PLC0415

    arrays = []
    names = []
    indices = pa.array(range(len(batch)), type=pa.int64())
    for number, array in enumerate(batch.columns):
        kind = array.type
        if pa.types.is_string(kind) or pa.types.is_large_string(kind):
            default = ""
        elif pa.types.is_binary(kind) or pa.types.is_large_binary(kind):
            default = b""
        elif pa.types.is_list(kind) or pa.types.is_large_list(kind):
            default = []
        elif pa.types.is_boolean(kind):
            default = False
        else:
            default = 0
        mask = pc.call_function("is_null", [array])
        filled = pc.fill_null(array, pa.scalar(default, type=kind))
        if pa.types.is_floating(kind):
            filled = pc.call_function(
                "if_else",
                [pc.call_function("is_nan", [filled]), pa.scalar(math.nan, type=kind), filled],
            )
        # take removes sliced offsets and normalizes unused bitmap tail bits.
        arrays.extend((pc.take(mask, indices), pc.take(filled, indices)))
        names.extend((f"null_{number}", f"value_{number}"))
    normalized = pa.record_batch(arrays, names=names)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, normalized.schema) as writer:
        writer.write_batch(normalized)
    return sink.getvalue().to_pybytes()


def arrow_digest(reader: pa.RecordBatchReader) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    for batch in fixed_batches(reader):
        count += len(batch)
        digest.update(canonical_batch(batch))
    return count, digest.hexdigest()
