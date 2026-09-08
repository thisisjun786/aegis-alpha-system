"""Versioned bounded indexes for immutable FMP normalized history."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, date, datetime
from itertools import chain
from pathlib import Path
from typing import cast

from aegis_alpha.collection.records import RunEventType
from aegis_alpha.data.fmp_collector import FmpCollector
from aegis_alpha.data.fmp_collector_state import _validate_complete_artifact_inventory
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_normalize import canonical_symbol
from aegis_alpha.data.fmp_windows import REVALIDATION_TRADING_DAYS
from aegis_alpha.data.serialization import canonical_json_bytes

_SCHEMA_VERSION = 1
_INDEX_NAME = "history-index.json"
_POINTER_PREFIX = "latest-history-index-"
_PROFILE_DATASET = "fmp_profile"
_COLLECTION_PLAN_DATASETS = frozenset(selection.plan_dataset for selection in DatasetSelection)


def _index_path(collector: FmpCollector, run_id: str, dataset: str) -> Path:
    return (
        collector.config.dataset_root
        / "normalized"
        / "fmp"
        / ".runs"
        / f"run_id={run_id}"
        / dataset
        / _INDEX_NAME
    )


def latest_published_index(
    collector: FmpCollector,
    dataset: str,
) -> dict[str, tuple[Mapping[str, object], ...]] | None:
    markers_root = collector.config.raw_store_root / "fmp" / "runs"
    pointer = _pointer_path(collector, dataset)
    if pointer.is_file():
        return _pointed_index(collector, pointer, dataset)
    if not markers_root.is_dir():
        return None
    markers = sorted(
        markers_root.glob("*/publication.json"),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    for marker in markers:
        run_id = marker.parent.name
        path = _index_path(collector, run_id, dataset)
        if not path.is_file():
            continue
        state = collector._control_plane.current_run_state(run_id)  # noqa: SLF001
        if state is None or state.state is not RunEventType.RUN_SUCCEEDED:
            continue
        if (
            collector._control_plane.run_plan_dataset(run_id)  # noqa: SLF001
            not in _COLLECTION_PLAN_DATASETS
        ):
            continue
        artifacts = _validate_complete_artifact_inventory(collector, run_id, state.plan_id)
        if str(path) not in artifacts:
            raise ValueError("history index is absent from its publication marker")
        return _load_index(path, dataset)
    return None


def _pointed_index(
    collector: FmpCollector,
    pointer: Path,
    dataset: str,
) -> dict[str, tuple[Mapping[str, object], ...]] | None:
    payload = pointer.read_bytes()
    document = cast("dict[str, object]", json.loads(payload))
    run_id = document.get("run_id")
    pointed_dataset = document.get("dataset")
    marker_sha256 = document.get("marker_sha256")
    if (
        payload != canonical_json_bytes(document)
        or not isinstance(run_id, str)
        or pointed_dataset != dataset
        or not isinstance(marker_sha256, str)
    ):
        raise ValueError("latest history index pointer is invalid")
    marker = collector.config.raw_store_root / "fmp" / "runs" / run_id / "publication.json"
    if not marker.is_file() or hashlib.sha256(marker.read_bytes()).hexdigest() != marker_sha256:
        raise ValueError("latest history index pointer marker binding is invalid")
    path = _index_path(collector, run_id, dataset)
    if not path.is_file():
        return None
    state = collector._control_plane.current_run_state(run_id)  # noqa: SLF001
    if state is None or state.state is not RunEventType.RUN_SUCCEEDED:
        return None
    if (
        collector._control_plane.run_plan_dataset(run_id)  # noqa: SLF001
        not in _COLLECTION_PLAN_DATASETS
    ):
        raise ValueError("latest history index pointer plan type is invalid")
    artifacts = _validate_complete_artifact_inventory(collector, run_id, state.plan_id)
    if str(path) not in artifacts:
        raise ValueError("history index is absent from its publication marker")
    return _load_index(path, dataset)


def _pointer_path(collector: FmpCollector, dataset: str) -> Path:
    return collector.config.raw_store_root / "fmp" / f"{_POINTER_PREFIX}{dataset}.json"


def history_index_pointer_publications(
    collector: FmpCollector,
    run_id: str,
    marker_sha256: str,
) -> tuple[tuple[Path, bytes], ...]:
    return tuple(
        (
            _pointer_path(collector, dataset),
            canonical_json_bytes(
                {
                    "dataset": dataset,
                    "marker_sha256": marker_sha256,
                    "run_id": run_id,
                }
            ),
        )
        for dataset in sorted(collector._historical_index_cache)  # noqa: SLF001
    )


def merge_historical_index_payloads(
    collector: FmpCollector,
    pointer_path: Path,
    incoming_pointer: bytes,
) -> bytes:
    """Merge this shard's index with the on-disk pointer so other shards are kept."""

    incoming = cast("dict[str, object]", json.loads(incoming_pointer))
    dataset = incoming.get("dataset")
    run_id = incoming.get("run_id")
    marker_sha256 = incoming.get("marker_sha256")
    if (
        not isinstance(dataset, str)
        or not isinstance(run_id, str)
        or not isinstance(marker_sha256, str)
    ):
        raise TypeError("history index pointer is invalid")
    current = _current_pointer_index(collector, pointer_path, dataset)
    incoming_index = _load_index(_index_path(collector, run_id, dataset), dataset)
    if current is None:
        return incoming_pointer
    current_run_id, current_index = current
    merged: dict[str, tuple[Mapping[str, object], ...]] = dict(current_index)
    for symbol, rows in incoming_index.items():
        existing = merged.get(symbol, ())
        combined = chain(
            ({**row, "symbol": symbol} for row in existing),
            ({**row, "symbol": symbol} for row in rows),
        )
        if dataset == _PROFILE_DATASET:
            merged[symbol] = latest_cik_index(combined).get(symbol, rows)
        else:
            merged[symbol] = recent_date_index(combined).get(symbol, rows)
    if dataset == _PROFILE_DATASET:
        symbols: object = {symbol: dict(rows[0]) for symbol, rows in sorted(merged.items()) if rows}
    else:
        symbols = {
            symbol: [
                value.isoformat() for row in rows if isinstance((value := row.get("date")), date)
            ]
            for symbol, rows in sorted(merged.items())
        }
    index_payload = canonical_json_bytes(
        {"dataset": dataset, "schema_version": _SCHEMA_VERSION, "symbols": symbols}
    )
    destination = _index_path(collector, current_run_id, dataset)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(index_payload)
    current_marker = (
        collector.config.raw_store_root / "fmp" / "runs" / current_run_id / "publication.json"
    )
    current_marker_sha256 = hashlib.sha256(current_marker.read_bytes()).hexdigest()
    return canonical_json_bytes(
        {
            "dataset": dataset,
            "marker_sha256": current_marker_sha256,
            "run_id": current_run_id,
        }
    )


def _current_pointer_index(
    collector: FmpCollector,
    pointer_path: Path,
    dataset: str,
) -> tuple[str, dict[str, tuple[Mapping[str, object], ...]]] | None:
    if not pointer_path.is_file():
        return None
    payload = pointer_path.read_bytes()
    document = cast("dict[str, object]", json.loads(payload))
    run_id = document.get("run_id")
    pointed_dataset = document.get("dataset")
    if not isinstance(run_id, str) or pointed_dataset != dataset:
        raise ValueError("latest history index pointer is invalid")
    path = _index_path(collector, run_id, dataset)
    if not path.is_file():
        return None
    return run_id, _load_index(path, dataset)


def _load_index(
    path: Path,
    dataset: str,
) -> dict[str, tuple[Mapping[str, object], ...]]:
    payload = path.read_bytes()
    document = cast("dict[str, object]", json.loads(payload))
    if (
        payload != canonical_json_bytes(document)
        or document.get("schema_version") != _SCHEMA_VERSION
        or document.get("dataset") != dataset
    ):
        raise ValueError("historical index artifact is invalid")
    symbols = cast("dict[str, object]", document.get("symbols"))
    if dataset == _PROFILE_DATASET:
        return {
            symbol: (cast("Mapping[str, object]", row),)
            for symbol, row in symbols.items()
            if isinstance(symbol, str) and isinstance(row, dict)
        }
    return {
        symbol: tuple(
            {"date": date.fromisoformat(str(value))} for value in cast("list[str]", values)
        )
        for symbol, values in symbols.items()
        if isinstance(symbol, str) and isinstance(values, list)
    }


def update_historical_index(
    collector: FmpCollector,
    dataset: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    current = collector._historical_index_cache[dataset]  # noqa: SLF001
    incoming = tuple(rows)
    symbols = {
        canonical_symbol(symbol)
        for row in incoming
        if isinstance((symbol := row.get("symbol")), str)
    }
    existing = ({**row, "symbol": symbol} for symbol in symbols for row in current.get(symbol, ()))
    merged = chain(existing, incoming)
    current.update(
        latest_cik_index(merged) if dataset == _PROFILE_DATASET else recent_date_index(merged)
    )


def historical_index_publications(
    collector: FmpCollector,
    run_id: str,
) -> tuple[tuple[str, Path, bytes], ...]:
    publications = []
    for dataset, index in sorted(collector._historical_index_cache.items()):  # noqa: SLF001
        if dataset == _PROFILE_DATASET:
            symbols: object = {
                symbol: dict(rows[0]) for symbol, rows in sorted(index.items()) if rows
            }
        else:
            symbols = {
                symbol: [
                    value.isoformat()
                    for row in rows
                    if isinstance((value := row.get("date")), date)
                ]
                for symbol, rows in sorted(index.items())
            }
        payload = canonical_json_bytes(
            {"dataset": dataset, "schema_version": _SCHEMA_VERSION, "symbols": symbols}
        )
        publications.append((dataset, _index_path(collector, run_id, dataset), payload))
    return tuple(publications)


def recent_date_index(
    rows: Iterator[Mapping[str, object]],
) -> dict[str, tuple[Mapping[str, object], ...]]:
    recent: dict[str, set[date]] = {}
    limit = REVALIDATION_TRADING_DAYS + 1
    for row in rows:
        symbol = row.get("symbol")
        value = row.get("date")
        if not isinstance(symbol, str) or not isinstance(value, date):
            continue
        values = recent.setdefault(canonical_symbol(symbol), set())
        values.add(value)
        if len(values) > limit:
            values.remove(min(values))
    return {
        symbol: tuple({"date": value} for value in sorted(values))
        for symbol, values in recent.items()
    }


def latest_cik_index(
    rows: Iterator[Mapping[str, object]],
) -> dict[str, tuple[Mapping[str, object], ...]]:
    latest: dict[str, Mapping[str, object]] = {}
    for row in rows:
        symbol = row.get("symbol")
        if not isinstance(symbol, str):
            continue
        key = canonical_symbol(symbol)
        candidate = {
            "cik": row.get("cik"),
            "retrieved_at_utc": row.get("retrieved_at_utc"),
        }
        previous = latest.get(key)
        if previous is not None:
            if candidate["cik"] is None and previous["cik"] is not None:
                continue
            if (candidate["cik"] is None or previous["cik"] is not None) and _utc_timestamp(
                candidate["retrieved_at_utc"]
            ) < _utc_timestamp(previous["retrieved_at_utc"]):
                continue
        latest[key] = candidate
    return {symbol: (row,) for symbol, row in latest.items()}


def _utc_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value)
    else:
        return datetime.min.replace(tzinfo=UTC)
    if parsed.utcoffset() is None:
        raise ValueError("profile retrieval timestamp must include UTC offset")
    return parsed.astimezone(UTC)
