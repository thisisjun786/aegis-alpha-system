"""Indexed immutable history used by full-universe FMP collection."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq

from aegis_alpha.collection.records import RunEventType
from aegis_alpha.data.fmp_collector import FmpCollector
from aegis_alpha.data.fmp_collector_state import _validate_complete_artifact_inventory
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_historical_index import (
    latest_cik_index,
    latest_published_index,
    recent_date_index,
)
from aegis_alpha.data.fmp_normalize import DATASET_SPECS, canonical_symbol, collected_dates
from aegis_alpha.data.fmp_windows import CollectorContractError

_HISTORY_BATCH_ROWS = 8192
_PROFILE_DATASET = "fmp_profile"
_COLLECTION_PLAN_DATASETS = frozenset(selection.plan_dataset for selection in DatasetSelection)


def _projection_columns(dataset: str) -> tuple[str, ...]:
    spec = DATASET_SPECS.get(dataset)
    if spec is None:
        raise CollectorContractError(f"unknown provider-normalized dataset: {dataset!r}")
    if dataset == _PROFILE_DATASET:
        return ("symbol", "cik", "retrieved_at_utc")
    if "date" not in spec.field_names:
        raise CollectorContractError(f"historical dataset has no date projection: {dataset!r}")
    return ("symbol", "date")


def _projected_rows(
    collector: FmpCollector,
    dataset: str,
) -> Iterator[Mapping[str, object]]:
    columns = _projection_columns(dataset)
    for path in _successful_run_parts(collector, dataset):
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(
            batch_size=_HISTORY_BATCH_ROWS,
            columns=list(columns),
        ):
            yield from cast("list[Mapping[str, object]]", batch.to_pylist())


def _successful_run_parts(collector: FmpCollector, dataset: str) -> tuple[Path, ...]:
    markers_root = collector.config.raw_store_root / "fmp" / "runs"
    normalized_root = collector.config.dataset_root / "normalized" / "fmp"
    markers = sorted(markers_root.glob("*/publication.json")) if markers_root.is_dir() else ()
    parts: list[Path] = []
    for marker in markers:
        run_id = marker.parent.name
        state = collector._control_plane.current_run_state(run_id)  # noqa: SLF001
        if state is None or state.state is not RunEventType.RUN_SUCCEEDED:
            continue
        if (
            collector._control_plane.run_plan_dataset(run_id)  # noqa: SLF001
            not in _COLLECTION_PLAN_DATASETS
        ):
            continue
        artifacts = _validate_complete_artifact_inventory(collector, run_id, state.plan_id)
        accepted_parents = {
            normalized_root / ".runs" / f"run_id={run_id}" / dataset,
            normalized_root / dataset / f"run_id={run_id}",
        }
        parts.extend(
            path
            for path in (Path(value) for value in artifacts)
            if path.parent in accepted_parents and path.match("part-*.parquet")
        )
    return tuple(sorted(parts))


def existing_rows(collector: FmpCollector, dataset: str) -> tuple[Mapping[str, object], ...]:
    return tuple(_projected_rows(collector, dataset))


def _historical_index(
    collector: FmpCollector,
    dataset: str,
) -> dict[str, tuple[Mapping[str, object], ...]]:
    cached = collector._historical_index_cache.get(dataset)  # noqa: SLF001
    if cached is not None:
        return cached
    index = latest_published_index(collector, dataset)
    if index is None:
        if dataset == _PROFILE_DATASET:
            index = latest_cik_index(_projected_rows(collector, dataset))
        else:
            index = recent_date_index(_projected_rows(collector, dataset))
    collector._historical_index_cache[dataset] = index  # noqa: SLF001
    return index


def preload_history(collector: FmpCollector, datasets: Sequence[str]) -> None:
    for dataset in sorted(set(datasets)):
        _historical_index(collector, dataset)


def known_dates(collector: FmpCollector, dataset: str, symbol: str) -> tuple[date, ...]:
    return collected_dates(_historical_index(collector, dataset).get(canonical_symbol(symbol), ()))


def prior_cik(collector: FmpCollector, dataset: str, symbol: str) -> str | None:
    matches = list(_historical_index(collector, dataset).get(canonical_symbol(symbol), ()))
    matches.sort(key=lambda row: str(row.get("retrieved_at_utc", "")))
    value = None if not matches else matches[-1].get("cik")
    return value if isinstance(value, str) else None
