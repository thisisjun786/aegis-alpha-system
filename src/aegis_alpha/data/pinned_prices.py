"""Bounded canonical-v1 inspection from exact catalog-pinned descriptors.

This adapter filters snapshot availability. It does not resolve identities from
current registries or implement historical revision replay.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from threading import Event, Thread
from typing import TYPE_CHECKING, BinaryIO, Literal, cast

import duckdb
import pyarrow.parquet as pq

from aegis_alpha.compute_resources import HASH_CHUNK_BYTES, ComputeBudget, check_cancelled
from aegis_alpha.data.canonical_records import (
    CANONICAL_CONTRACT_VERSION,
    CANONICAL_SCHEMA_VERSION,
    require_aware,
)
from aegis_alpha.data.catalog_access import ArtifactReference, DatasetView
from aegis_alpha.data.contracts import AdjustmentBasis
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.duckdb_engine import assert_v2_prerelease
from aegis_alpha.data.price_schema import PARQUET_MEDIA_TYPE, PRICE_SCHEMA
from aegis_alpha.data.serialization import canonical_json_bytes

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

MAX_PRICE_ROWS = 1000
DEFAULT_PRICE_ROWS = 100
_PRICE_PATH = re.compile(
    r"prices/adjustment_basis=(RAW|SPLIT_ADJUSTED|TOTAL_RETURN)/year=([0-9]{4})/part-[0-9]{5}\.parquet"
)
_FLOAT_COLUMNS = ("open", "high", "low", "close", "volume", "unadjusted_close", "dividend")
_ORDER = "observation_date, instrument_id, adjustment_basis, available_at, observation_id"


@dataclass(frozen=True, slots=True)
class PriceQuery:
    start_date: date
    end_date: date
    decision_cutoff: datetime
    instruments: tuple[str, ...]
    basis: AdjustmentBasis
    limit: int = DEFAULT_PRICE_ROWS
    purpose: Literal["inspection", "backtest"] = "backtest"

    def __post_init__(self) -> None:
        if type(self.start_date) is not date or type(self.end_date) is not date:
            raise TypeError("start_date and end_date must be dates, not timestamps or text")
        if self.end_date < self.start_date:
            raise ValueError("end_date cannot precede start_date")
        if not isinstance(self.decision_cutoff, datetime):
            raise TypeError("decision_cutoff must be a timezone-aware datetime")
        require_aware("decision_cutoff", self.decision_cutoff)
        object.__setattr__(self, "decision_cutoff", self.decision_cutoff.astimezone(UTC))
        if not isinstance(self.instruments, tuple) or not self.instruments:
            raise ValueError("instruments must be a nonempty tuple of instrument IDs")
        if len(self.instruments) > MAX_PRICE_ROWS or any(
            not isinstance(item, str)
            or not item.strip()
            or item != item.strip()
            or any(ord(char) < ord(" ") for char in item)
            for item in self.instruments
        ):
            raise ValueError("instruments must contain at most 1000 exact nonempty IDs")
        if len(set(self.instruments)) != len(self.instruments):
            raise ValueError("duplicate instrument IDs")
        object.__setattr__(self, "instruments", tuple(sorted(self.instruments)))
        if not isinstance(self.basis, AdjustmentBasis):
            raise TypeError("basis must be AdjustmentBasis")
        if type(self.limit) is not int or not 1 <= self.limit <= MAX_PRICE_ROWS:
            raise ValueError("limit must be an integer from 1 to 1000")
        if not isinstance(self.purpose, str) or self.purpose not in {"inspection", "backtest"}:
            raise ValueError("purpose must be inspection or backtest")


@dataclass(frozen=True, slots=True)
class _PinnedFile:
    artifact: ArtifactReference
    handle: BinaryIO
    state: tuple[int, ...]

    @property
    def scan_path(self) -> str:
        # Generated from an owned, open FD, never a caller path or glob.
        return f"/proc/self/fd/{self.handle.fileno()}"


def _state(information: os.stat_result) -> tuple[int, ...]:
    return (
        information.st_dev,
        information.st_ino,
        information.st_mode,
        information.st_nlink,
        information.st_size,
        information.st_mtime_ns,
        information.st_ctime_ns,
    )


def _verify_file(
    tree: DescriptorTree,
    pin: _PinnedFile,
    *,
    cancel_event: Event | None = None,
    stop_event: Event | None = None,
) -> None:
    check_cancelled(cancel_event)
    check_cancelled(stop_event)
    handle = pin.handle
    if _state(os.fstat(handle.fileno())) != pin.state:
        raise ValueError("pinned file state changed")
    if _state(tree.stat(pin.artifact.relative_path)) != pin.state:
        raise ValueError("catalog file path changed during read")
    if pin.state[4] != pin.artifact.size_bytes:
        raise ValueError("catalog file size mismatch")
    handle.seek(0)
    hasher = hashlib.sha256()
    for chunk in iter(lambda: handle.read(HASH_CHUNK_BYTES), b""):
        check_cancelled(cancel_event)
        check_cancelled(stop_event)
        hasher.update(chunk)
    digest = hasher.hexdigest()
    handle.seek(0)
    if digest != pin.artifact.content_sha256:
        raise ValueError("catalog file hash mismatch")
    if _state(os.fstat(handle.fileno())) != pin.state:
        raise ValueError("pinned file state changed during hashing")


def _verify_files(
    tree: DescriptorTree,
    pins: list[_PinnedFile],
    budget: ComputeBudget | None,
    cancel_event: Event | None,
) -> None:
    workers = 1 if budget is None else min(len(pins), budget.hash_workers)
    if workers <= 1:
        for pin in pins:
            _verify_file(tree, pin, cancel_event=cancel_event)
        return
    stop = Event()
    iterator = iter(pins)
    pending: set[Future[None]] = set()
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="aas-price-hash") as pool:
        try:
            while True:
                check_cancelled(cancel_event)
                while len(pending) < 2 * workers:
                    pin = next(iterator, None)
                    if pin is None:
                        break
                    pending.add(
                        pool.submit(
                            _verify_file, tree, pin, cancel_event=cancel_event, stop_event=stop
                        )
                    )
                if not pending:
                    break
                done, pending = wait(pending, timeout=0.05, return_when=FIRST_COMPLETED)
                for future in done:
                    future.result()
        except BaseException:
            stop.set()
            for future in pending:
                future.cancel()
            raise


@contextmanager
def _connection(
    budget: ComputeBudget | None, cancel_event: Event | None
) -> Iterator[DuckDBPyConnection]:
    configuration: dict[str, str | int | float | list[str]] = {
        "threads": 1 if budget is None else budget.duckdb_threads,
        "memory_limit": "256MB" if budget is None else f"{budget.duckdb_memory_limit_bytes}B",
        "max_temp_directory_size": "0B",
    }
    with duckdb.connect(config=configuration) as connection:
        done = Event()

        def interrupt_on_cancel() -> None:
            while not done.wait(0.05):
                if cancel_event is not None and cancel_event.is_set():
                    connection.interrupt()
                    return

        watcher = Thread(target=interrupt_on_cancel, name="aas-price-cancel", daemon=True)
        if cancel_event is not None:
            watcher.start()
        try:
            check_cancelled(cancel_event)
            yield connection
            check_cancelled(cancel_event)
        except duckdb.Error:
            check_cancelled(cancel_event)
            raise
        finally:
            done.set()
            if cancel_event is not None:
                watcher.join()


def _partition(artifact: ArtifactReference) -> tuple[AdjustmentBasis, int] | None:
    if not artifact.relative_path.startswith("prices/"):
        return None
    match = _PRICE_PATH.fullmatch(artifact.relative_path)
    if match is None or artifact.media_type != PARQUET_MEDIA_TYPE:
        raise ValueError("unsupported canonical price artifact format")
    basis, year_text = match.groups()
    year = int(year_text)
    if not 1 <= year <= 9999:  # noqa: PLR2004 - calendar year bounds
        raise ValueError("invalid price partition year")
    values = artifact.partition_values
    if (
        set(values) != {"adjustment_basis", "year"}
        or values["adjustment_basis"] != basis
        or type(values["year"]) is not int
        or values["year"] != year
    ):
        raise ValueError("catalog partition values disagree with exact artifact path")
    return AdjustmentBasis(basis), year


def _validate_prices(connection: DuckDBPyConnection, pin: _PinnedFile) -> None:
    parquet = pq.ParquetFile(pin.handle)
    if not parquet.schema_arrow.equals(PRICE_SCHEMA, check_metadata=False):
        raise ValueError(
            "unsupported canonical price schema; revision/history replay is unavailable"
        )
    if parquet.metadata.num_rows != pin.artifact.row_count:
        raise ValueError("catalog price row count mismatch")
    nulls = " OR ".join(f'"{name}" IS NULL' for name in PRICE_SCHEMA.names if name != "issuer_id")
    nonfinite = " OR ".join(f"NOT isfinite({name})" for name in _FLOAT_COLUMNS)
    partition = _partition(pin.artifact)
    if partition is None:
        raise ValueError("expected a canonical price partition")
    # Only constant schema column names enter SQL; path and partition values are bound.
    invalid = connection.execute(
        f"""SELECT EXISTS(SELECT 1 FROM read_parquet(?, hive_partitioning=false)
        WHERE ({nulls}) OR ({nonfinite}) OR schema_version != ? OR volume < 0
        OR available_at < observed_at OR adjustment_basis != ?
        OR year(observation_date) != ? OR trim(instrument_id) = ''
        OR trim(observation_id) = '' OR trim(currency) = '')""",  # noqa: S608
        [pin.scan_path, CANONICAL_SCHEMA_VERSION, partition[0].value, partition[1]],
    ).fetchone()
    if invalid is None or invalid[0]:
        raise ValueError("canonical price partition contains invalid rows")


def _query_prices(
    connection: DuckDBPyConnection, paths: list[str], query: PriceQuery
) -> tuple[list[dict[str, object]], bool]:
    result = connection.execute(
        f"""SELECT * REPLACE (
            strftime(available_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ') AS available_at,
            strftime(observed_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ') AS observed_at
        ) FROM read_parquet(?, hive_partitioning=false)
        WHERE adjustment_basis = ? AND observation_date BETWEEN ? AND ?
        AND available_at <= ? AND instrument_id IN (SELECT unnest(?))
        ORDER BY {_ORDER} LIMIT ?""",  # noqa: S608 - fixed ordering; all inputs bound
        [
            paths,
            query.basis.value,
            query.start_date,
            query.end_date,
            query.decision_cutoff,
            list(query.instruments),
            query.limit + 1,
        ],
    )
    columns = [item[0] for item in result.description]
    fetched = result.fetchall()  # SQL LIMIT guarantees at most 1001 materialized rows.
    rows = [dict(zip(columns, row, strict=True)) for row in fetched]
    keys = [
        (row["observation_date"], row["instrument_id"], row["adjustment_basis"]) for row in rows
    ]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate price observations require unsupported revision/history replay")
    return rows[: query.limit], len(rows) > query.limit


def _read_verified(
    view: DatasetView,
    root: Path,
    query: PriceQuery,
    budget: ComputeBudget | None,
    cancel_event: Event | None,
) -> dict[str, object]:
    with ExitStack() as stack:
        tree = stack.enter_context(DescriptorTree.open_path(root))
        pins: list[_PinnedFile] = []
        selected: list[_PinnedFile] = []
        for artifact in view.artifacts:
            handle = stack.enter_context(tree.binary_reader(artifact.relative_path))
            pin = _PinnedFile(artifact, handle, _state(os.fstat(handle.fileno())))
            check_cancelled(cancel_event)
            pins.append(pin)
            partition = _partition(artifact)
            if partition is not None and (
                partition[0] is query.basis
                and query.start_date.year <= partition[1] <= query.end_date.year
            ):
                selected.append(pin)
        _verify_files(tree, pins, budget, cancel_event)
        rows: list[dict[str, object]] = []
        truncated = False
        with _connection(budget, cancel_event) as connection:
            assert_v2_prerelease(connection)
            for pin in selected:
                check_cancelled(cancel_event)
                _validate_prices(connection, pin)
            if selected:
                rows, truncated = _query_prices(
                    connection, [pin.scan_path for pin in selected], query
                )
        # Reopen from the explicit root, detecting root/ancestor replacements as well as leaves.
        with DescriptorTree.open_path(root) as visible:
            if visible.identity != tree.identity:
                raise ValueError("profile root changed during read")
            _verify_files(visible, pins, budget, cancel_event)
        check_cancelled(cancel_event)
        report = {
            "dataset_id": view.dataset_id,
            "dataset_version": view.dataset_version,
            "aggregate_content_sha256": view.aggregate_content_sha256,
            "eligibility": view.eligibility,
            "purpose": query.purpose,
            "read_semantics": "canonical-v1-snapshot-cutoff-filter",
            "historical_revision_replay": False,
            "query": query,
            "verified_artifact_count": len(pins),
            "selected_price_artifact_count": len(selected),
            "row_count": len(rows),
            "truncated": truncated,
            "rows": rows,
        }
        return cast("dict[str, object]", json.loads(canonical_json_bytes(report)))


def read_prices(
    view: DatasetView,
    root: Path,
    query: PriceQuery,
    *,
    budget: ComputeBudget | None = None,
    cancel_event: Event | None = None,
) -> dict[str, object]:
    """Read an explicit root and catalog version; fail closed before returning bytes."""
    check_cancelled(cancel_event)
    if budget is not None and not isinstance(budget, ComputeBudget):
        raise TypeError("budget must be ComputeBudget")
    if not isinstance(view, DatasetView) or not isinstance(query, PriceQuery):
        raise TypeError("read_prices requires DatasetView and PriceQuery")
    if not isinstance(root, Path) or not root.is_absolute() or ".." in root.parts:
        raise ValueError("root must be an explicit absolute path without parent traversal")
    if (
        view.schema_version != CANONICAL_SCHEMA_VERSION
        or view.transformation_version != CANONICAL_CONTRACT_VERSION
    ):
        raise ValueError("unsupported dataset format; revision/history replay is unavailable")
    if query.purpose == "backtest" and not view.eligibility.backtest:
        raise ValueError("dataset is not backtest eligible; use explicit inspection")
    if not any(_partition(artifact) is not None for artifact in view.artifacts):
        raise ValueError("dataset has no supported canonical price artifacts")
    try:
        return _read_verified(view, root, query, budget, cancel_event)
    except (duckdb.Error, OSError) as error:
        raise ValueError("pinned price read failed") from error
