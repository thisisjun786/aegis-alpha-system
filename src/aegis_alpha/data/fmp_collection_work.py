"""Bounded collection, normalization, and immutable publication work."""

from __future__ import annotations

import fcntl
import hashlib
import os
import stat
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from aegis_alpha.collection.records import CollectionMode
from aegis_alpha.data.fmp_collector import (
    FmpCollector,
    QualityResult,
    QualityResultKind,
    publish_bundle,
)
from aegis_alpha.data.fmp_collector_state import marker_path
from aegis_alpha.data.fmp_dataset_selection import (
    ACTION_DATASETS,
    FULL_PRICE_DATASET,
    PROFILE_DATASET,
    DatasetSelection,
)
from aegis_alpha.data.fmp_historical_index import (
    historical_index_publications,
    history_index_pointer_publications,
    merge_historical_index_payloads,
    update_historical_index,
)
from aegis_alpha.data.fmp_historical_state import (
    known_dates,
    preload_history,
    prior_cik,
)
from aegis_alpha.data.fmp_normalize import collected_dates, normalized_columns
from aegis_alpha.data.fmp_windows import (
    DateWindow,
    UniverseEntry,
    UniverseManifest,
    plan_backfill_windows,
)
from aegis_alpha.data.serialization import canonical_json_bytes

COLLECTION_DATASET = FULL_PRICE_DATASET  # compatibility name for plan identity
NORMALIZED_BATCH_ROWS = 8192


@dataclass(frozen=True, slots=True)
class CollectionBatch:
    dataset: str
    rows: tuple[Mapping[str, object], ...]
    advance: tuple[str, str, date] | None


@dataclass(frozen=True, slots=True)
class _StagedPart:
    destination: Path
    staged_path: Path
    sha256: str


@dataclass(frozen=True, slots=True)
class _StageContext:
    collector: FmpCollector
    run_id: str
    part_indexes: dict[str, int]
    staging_root: Path


def collection_parameters(
    collector: FmpCollector,
    manifest: UniverseManifest,
    digest: str,
    dataset_selection: DatasetSelection = DatasetSelection.PROBE,
) -> dict[str, object]:
    parameters: dict[str, object] = {
        "artifact_hashes": dict(sorted(collector.config.artifact_hashes.items())),
        "as_of": collector.config.as_of.isoformat(),
        "manifest_sha256": digest,
        "max_calls": collector.config.max_calls,
        "symbols_sha256": hashlib.sha256(canonical_json_bytes(manifest.symbols())).hexdigest(),
    }
    if collector.config.shard is not None:
        parameters["shard"] = [collector.config.shard[0], collector.config.shard[1]]
    if dataset_selection is not DatasetSelection.PROBE:
        parameters["dataset_selection"] = dataset_selection.value
        parameters["datasets"] = list(dataset_selection.datasets)
    if "recurring_authority" in collector.config.artifact_hashes:
        if collector.config.mode is CollectionMode.BACKFILL:
            if collector.config.operator_from is None:
                raise ValueError("recurring backfill recovery requires operator_from")
            service_day = collector.config.operator_from
        else:
            service_day = collector.config.as_of
        parameters["recurring_recovery"] = {
            "contract": "fmp-recurring-recovery-v1",
            "manifest_path": (
                None
                if collector.config.manifest_path is None
                else str(collector.config.manifest_path)
            ),
            "mode": collector.config.mode.value,
            "operator_from": (
                None
                if collector.config.operator_from is None
                else collector.config.operator_from.isoformat()
            ),
            "output_path": str(collector.config.receipt_path),
            "service_day": service_day.isoformat(),
        }
    return parameters


def _windows(collector: FmpCollector, dataset: str, entry: UniverseEntry) -> tuple[DateWindow, ...]:
    if dataset in ACTION_DATASETS:
        # Action endpoints are unwindowed snapshots and can announce future events.
        # Only manifest bounds gate admission; event watermarks are not price cursors.
        plan = plan_backfill_windows(
            entry=entry,
            operator_from=collector.config.operator_from or collector.config.as_of,
            as_of=collector.config.as_of,
        )
        if plan.skipped_reason is not None:
            collector._quality.append(  # noqa: SLF001 -- same retained empty-range evidence
                QualityResult(
                    kind=QualityResultKind.EMPTY_RANGE_SKIPPED,
                    symbol=entry.symbol,
                    dataset=dataset,
                    detail={"reason": plan.skipped_reason},
                )
            )
        return plan.windows
    return collector.plan_symbol_windows(
        dataset=dataset,
        entry=entry,
        watermark=collector.latest_watermark_date(dataset=dataset, symbol=entry.symbol),
        known_dates=known_dates(collector, dataset, entry.symbol),
    )


def _collect_windowed(
    collector: FmpCollector,
    entry: UniverseEntry,
    dataset: str,
    windows: Sequence[DateWindow],
) -> tuple[list[Mapping[str, object]], tuple[str, str, date] | None]:
    rows: list[Mapping[str, object]] = []
    for window in windows:
        collected, _ = collector.collect_window(dataset=dataset, symbol=entry.symbol, window=window)
        if not collected and not entry.active:
            collector.record_delisted_shortfall(symbol=entry.symbol, dataset=dataset, window=window)
        rows.extend(collected)
    dates = collected_dates(rows)
    window_end = max(window.end for window in windows)
    if dates and min(dates) > min(window.start for window in windows):
        collector.record_observed_coverage_start(
            symbol=entry.symbol,
            dataset=dataset,
            observed_start=min(dates),
            requested_start=min(window.start for window in windows),
        )
    in_window = tuple(value for value in dates if value <= window_end)
    advance = (
        (dataset, entry.symbol, max(in_window))
        if in_window and entry.symbol not in collector.blocked_symbols
        else None
    )
    return rows, advance


def _collect_action(
    collector: FmpCollector, entry: UniverseEntry, dataset: str
) -> tuple[list[Mapping[str, object]], tuple[str, str, date] | None]:
    rows, _ = collector.collect_symbol_observations(dataset=dataset, symbol=entry.symbol)
    dates = tuple(value for value in collected_dates(rows) if value <= collector.config.as_of)
    previous = collector.latest_watermark_date(dataset=dataset, symbol=entry.symbol)
    latest = max(dates) if dates else None
    advance = (
        (dataset, entry.symbol, latest)
        if latest is not None
        and (previous is None or latest > previous)
        and entry.symbol not in collector.blocked_symbols
        else None
    )
    return list(rows), advance


def collect_manifest(
    collector: FmpCollector,
    manifest: UniverseManifest,
    dataset_selection: DatasetSelection = DatasetSelection.PROBE,
) -> Iterator[CollectionBatch]:
    selected = dataset_selection.datasets
    preload_history(collector, selected)
    for entry in manifest.entries:
        planned_windows = {
            dataset: _windows(collector, dataset, entry)
            for dataset in selected
            if dataset not in (*ACTION_DATASETS, PROFILE_DATASET)
        }
        action_windows = {
            dataset: _windows(collector, dataset, entry)
            for dataset in ACTION_DATASETS
            if dataset in selected
        }
        if (planned_windows or action_windows) and not any(
            (*planned_windows.values(), *action_windows.values())
        ):
            continue
        profile_rows, _ = collector.collect_profile(symbol=entry.symbol)
        cik = profile_rows[0].get("cik") if profile_rows else None
        if collector.check_recycled_ticker(
            symbol=entry.symbol,
            collected_cik=cik if isinstance(cik, str) else None,
            prior_cik=prior_cik(collector, PROFILE_DATASET, entry.symbol),
        ):
            continue
        if profile_rows:
            yield CollectionBatch(PROFILE_DATASET, tuple(profile_rows), None)
        for dataset, windows in planned_windows.items():
            rows, advance = _collect_windowed(collector, entry, dataset, windows)
            if rows or advance is not None:
                yield CollectionBatch(dataset, tuple(rows), advance)
        for dataset, windows in action_windows.items():
            if windows:
                rows, advance = _collect_action(collector, entry, dataset)
                if rows or advance is not None:
                    yield CollectionBatch(dataset, tuple(rows), advance)


def parquet_bytes(dataset: str, rows: Sequence[Mapping[str, object]]) -> bytes:
    columns = normalized_columns(dataset)
    table = pa.Table.from_pylist([{name: row.get(name) for name in columns} for row in rows])
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _dataset_path(
    collector: FmpCollector,
    run_id: str,
    dataset: str,
    part_index: int,
) -> Path:
    return (
        collector.config.dataset_root
        / "normalized"
        / "fmp"
        / ".runs"
        / f"run_id={run_id}"
        / dataset
        / f"part-{part_index:05d}.parquet"
    )


def publish_normalized(
    collector: FmpCollector,
    run_id: str,
    batches: Iterator[CollectionBatch],
    *,
    plan_id: str,
) -> dict[str, object]:
    advances: list[tuple[str, str, date]] = []
    part_indexes: dict[str, int] = {}
    buffers: dict[str, list[Mapping[str, object]]] = {}
    staging_parent = collector.config.dataset_root / "normalized" / ".staging"
    staging_parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(
        prefix="aegis-fmp-normalized-",
        dir=staging_parent,
    ) as directory:
        staging_root = Path(directory)
        staged: list[_StagedPart] = []
        context = _StageContext(collector, run_id, part_indexes, staging_root)
        for batch in batches:
            if batch.dataset not in collector._historical_index_cache:  # noqa: SLF001
                preload_history(collector, (batch.dataset,))
            update_historical_index(collector, batch.dataset, batch.rows)
            buffer = buffers.setdefault(batch.dataset, [])
            for row in batch.rows:
                buffer.append(row)
                if len(buffer) == NORMALIZED_BATCH_ROWS:
                    staged.append(
                        _stage_part(
                            context,
                            batch.dataset,
                            tuple(buffer),
                        )
                    )
                    buffer.clear()
            if batch.advance is not None:
                advances.append(batch.advance)
        staged.extend(
            _stage_part(
                context,
                dataset,
                tuple(buffers[dataset]),
            )
            for dataset in sorted(buffers)
            if buffers[dataset]
        )
        for dataset, destination, payload in historical_index_publications(collector, run_id):
            staged_path = staging_root / f"{dataset}-history-index.json"
            staged_path.write_bytes(payload)
            staged.append(
                _StagedPart(
                    destination=destination,
                    staged_path=staged_path,
                    sha256=hashlib.sha256(payload).hexdigest(),
                )
            )
        ledger = collector._limiter.ledger()  # noqa: SLF001
        document: dict[str, object] = {
            "attempt_ledger_sha256": collector.attempt_ledger_sha256,
            "plan_id": plan_id,
            "run_id": run_id,
            "artifacts": [
                {"path": str(item.destination), "sha256": item.sha256} for item in staged
            ],
            "advances": [
                {"dataset": dataset, "stream": stream, "value": value.isoformat()}
                for dataset, stream, value in advances
            ],
            "usage": {
                "calls_attempted": ledger.calls_attempted,
                "bytes_received": ledger.bytes_received,
                "retry_after_waits": ledger.retry_after_waits,
                "rate_limited_attempts": ledger.rate_limited_attempts,
            },
        }
        collector.require_bound_approval()
        for item in staged:
            collector.require_bound_approval()
            publish_bundle([(item.destination, item.staged_path.read_bytes())])
        collector.require_bound_approval()
        publish_bundle([(marker_path(collector, run_id), canonical_json_bytes(document))])
        return document


def _stage_part(
    context: _StageContext,
    dataset: str,
    rows: Sequence[Mapping[str, object]],
) -> _StagedPart:
    part_index = context.part_indexes.get(dataset, 0)
    destination = _dataset_path(context.collector, context.run_id, dataset, part_index)
    payload = parquet_bytes(dataset, rows)
    staged_path = context.staging_root / f"{dataset}-{part_index:05d}.parquet"
    staged_path.write_bytes(payload)
    context.part_indexes[dataset] = part_index + 1
    return _StagedPart(
        destination=destination,
        staged_path=staged_path,
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def publish_completion(
    collector: FmpCollector, *, run_id: str, plan_id: str, receipt: bytes
) -> None:
    marker = marker_path(collector, run_id).read_bytes()
    marker_sha256 = hashlib.sha256(marker).hexdigest()
    completion = canonical_json_bytes(
        {
            "marker_sha256": marker_sha256,
            "plan_id": plan_id,
            "receipt_path": str(collector.receipt_destination()),
            "receipt_sha256": hashlib.sha256(receipt).hexdigest(),
            "run_id": run_id,
        }
    )
    pointers = history_index_pointer_publications(
        collector,
        run_id,
        marker_sha256,
    )
    collector.require_bound_approval()
    publish_bundle([(marker_path(collector, run_id).parent / "completion.json", completion)])
    for pointer_path, pointer in pointers:
        _replace_history_index_pointer(collector, pointer_path, pointer)


def _replace_history_index_pointer(collector: FmpCollector, path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f"{path.name}.lock")
    flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        lock_fd = os.open(lock_path, flags, 0o600)
    except OSError:
        raise OSError(f"unsafe history-index lock target rejected at {lock_path}") from None
    if not stat.S_ISREG(os.fstat(lock_fd).st_mode):
        os.close(lock_fd)
        raise OSError(f"unsafe history-index lock target rejected at {lock_path}")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        merged = merge_historical_index_payloads(collector, path, payload)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(merged)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(path)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(lock_fd)
