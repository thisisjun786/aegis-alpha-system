# ruff: noqa: PLR2004, PT011
# Synthetic boundary matrices use literal expected values and several refusal reasons.
"""Real synthetic Parquet/DuckDB checks for the bounded descriptor reader."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from aegis_alpha.data import pinned_prices
from aegis_alpha.data.canonical_records import CANONICAL_CONTRACT_VERSION
from aegis_alpha.data.catalog_access import ArtifactReference, DatasetView
from aegis_alpha.data.contracts import AdjustmentBasis, Eligibility
from aegis_alpha.data.pinned_prices import PriceQuery, read_prices
from aegis_alpha.data.price_schema import PARQUET_MEDIA_TYPE, PRICE_SCHEMA
from aegis_alpha.metadata.records import dataset_artifact_digest

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

DAY = date(2020, 1, 2)
NOW = datetime(2020, 1, 3, tzinfo=UTC)
BASIS = AdjustmentBasis.SPLIT_ADJUSTED


def _row(
    instrument: str = "i-1", *, basis: AdjustmentBasis = BASIS, ordinal: int = 0
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "observation_id": f"observation-{instrument}-{ordinal}",
        "instrument_id": instrument,
        "issuer_id": None,
        "observation_date": DAY,
        "adjustment_basis": basis.value,
        "open": 10.0,
        "high": 12.0,
        "low": 9.0,
        "close": 11.0,
        "volume": 100.0,
        "unadjusted_close": 11.0,
        "dividend": 0.0,
        "currency": "USD",
        "observed_at": NOW,
        "available_at": NOW,
        "source_provider": "synthetic",
        "source_dataset_id": "input",
        "source_dataset_version": "v1",
        "source_snapshot_id": "snapshot-1",
        "source_artifact_path": "source.parquet",
        "source_artifact_sha256": "a" * 64,
        "source_row_ordinal": ordinal,
        "quality_flags": "",
    }


def _artifact(
    root: Path,
    rows: list[dict[str, object]],
    *,
    basis: AdjustmentBasis = BASIS,
    schema: pa.Schema = PRICE_SCHEMA,
) -> ArtifactReference:
    relative = f"prices/adjustment_basis={basis.value}/year=2020/part-00000.parquet"
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), target)
    return ArtifactReference(
        relative,
        PARQUET_MEDIA_TYPE,
        target.stat().st_size,
        len(rows),
        hashlib.sha256(target.read_bytes()).hexdigest(),
        {"adjustment_basis": basis.value, "year": 2020},
    )


def _view(*artifacts: ArtifactReference) -> DatasetView:
    return DatasetView(
        "canonical-market-data",
        "canonical-v1.test",
        1,
        CANONICAL_CONTRACT_VERSION,
        dataset_artifact_digest(artifacts),
        sum(item.row_count or 0 for item in artifacts),
        Eligibility.blocked(),
        artifacts,
    )


def _query(**changes: object) -> PriceQuery:
    query = PriceQuery(DAY, DAY, NOW, ("i-1", "i-2"), BASIS, purpose="inspection")
    return replace(query, **changes)


def test_exact_files_cutoff_basis_order_and_same_version_rerun(tmp_path: Path) -> None:
    late = {**_row("i-late"), "available_at": datetime(2021, 1, 1, tzinfo=UTC)}
    capital = _artifact(tmp_path, [_row("i-2"), late, _row("i-1")])
    total = _artifact(
        tmp_path, [_row(basis=AdjustmentBasis.TOTAL_RETURN)], basis=AdjustmentBasis.TOTAL_RETURN
    )
    # Unregistered adjacent data must never enter the reader.
    pq.write_table(
        pa.Table.from_pylist([_row("intruder")], schema=PRICE_SCHEMA),
        (tmp_path / capital.relative_path).with_name("part-00001.parquet"),
    )
    view = _view(capital, total)
    query = _query(instruments=("i-1", "i-2", "i-late", "intruder"))
    report = read_prices(view, tmp_path, query)
    assert report == read_prices(view, tmp_path, query)
    assert [row["instrument_id"] for row in cast("list[dict[str, object]]", report["rows"])] == [
        "i-1",
        "i-2",
    ]
    assert report["eligibility"] == {
        "canonical": False,
        "backtest": False,
        "paper": False,
        "order": False,
    }
    assert report["historical_revision_replay"] is False
    assert report["selected_price_artifact_count"] == 1
    json.dumps(report, allow_nan=False)
    empty = read_prices(view, tmp_path, _query(decision_cutoff=datetime(2020, 1, 2, tzinfo=UTC)))
    assert empty["rows"] == []
    total_report = read_prices(view, tmp_path, _query(basis=AdjustmentBasis.TOTAL_RETURN))
    assert total_report["row_count"] == 1


def test_bounded_results_with_pushdown(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, [_row(f"i-{i:04}", ordinal=i) for i in range(1500)])
    query = _query(instruments=tuple(f"i-{i:04}" for i in range(1000)), limit=100)
    report = read_prices(_view(artifact), tmp_path, query)
    assert report["row_count"] == 100
    assert report["truncated"] is True
    assert cast("list[dict[str, object]]", report["rows"])[0]["instrument_id"] == "i-0000"


def test_backtest_default_refuses_ineligible_dataset(tmp_path: Path) -> None:
    view = _view(_artifact(tmp_path, [_row()]))
    with pytest.raises(ValueError, match="not backtest eligible"):
        read_prices(view, tmp_path, PriceQuery(DAY, DAY, NOW, ("i-1",), BASIS))
    eligible = replace(
        view, eligibility=Eligibility(canonical=True, backtest=True, paper=False, order=False)
    )
    assert read_prices(eligible, tmp_path, _query(purpose="backtest"))["row_count"] == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"limit": True},
        {"limit": float("nan")},
        {"limit": 0},
        {"limit": 1001},
        {"basis": "SPLIT_ADJUSTED"},
        {"instruments": ()},
        {"instruments": (True,)},
        {"instruments": ("i-1", "i-1")},
        {"instruments": ["i-1"]},
        {"start_date": "2020-01-02"},
        {"start_date": NOW},
        {"end_date": date(2019, 1, 1)},
        {"decision_cutoff": datetime(2020, 1, 3)},  # noqa: DTZ001 - intentionally invalid cutoff
        {"decision_cutoff": True},
        {"purpose": "paper"},
    ],
)
def test_query_validation(changes: dict[str, object]) -> None:
    with pytest.raises((ValueError, TypeError)):
        _query(**changes)


@pytest.mark.parametrize(
    "attack", ["missing", "truncate", "same_size", "symlink", "ancestor_symlink", "root_symlink"]
)
def test_file_tampering_refuses(tmp_path: Path, attack: str) -> None:
    root = tmp_path / "root"
    artifact = _artifact(root, [_row()])
    view = _view(artifact)
    target = root / artifact.relative_path
    if attack == "missing":
        target.unlink()
    elif attack == "truncate":
        target.write_bytes(b"bad")
    elif attack == "same_size":
        target.write_bytes(b"x" * artifact.size_bytes)
    elif attack == "symlink":
        saved = tmp_path / "saved.parquet"
        target.rename(saved)
        target.symlink_to(saved)
    elif attack == "ancestor_symlink":
        saved = tmp_path / "saved-dir"
        target.parent.rename(saved)
        target.parent.symlink_to(saved, target_is_directory=True)
    else:
        saved = tmp_path / "saved-root"
        root.rename(saved)
        root.symlink_to(saved, target_is_directory=True)
    with pytest.raises(ValueError):
        read_prices(view, root, _query())


@pytest.mark.parametrize("attack", ["leaf", "root", "inplace"])
def test_file_state_held_through_duckdb_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, attack: str
) -> None:
    root = tmp_path / "root"
    artifact = _artifact(root, [_row()])
    original = pinned_prices._query_prices  # noqa: SLF001 - inject a read-time race

    reached: list[bool] = []

    def race(
        connection: DuckDBPyConnection, paths: list[str], query: PriceQuery
    ) -> tuple[list[dict[str, object]], bool]:
        result = original(connection, paths, query)
        reached.append(True)
        target = root / artifact.relative_path
        if attack == "leaf":
            saved = tmp_path / "replacement"
            saved.write_bytes(target.read_bytes())
            saved.replace(target)
        elif attack == "root":
            root.rename(tmp_path / "old-root")
            root.mkdir()
        else:
            with target.open("r+b") as handle:
                handle.write(b"bad!")
                os.fsync(handle.fileno())
        return result

    monkeypatch.setattr(pinned_prices, "_query_prices", race)
    with pytest.raises(ValueError):
        read_prices(_view(artifact), root, _query())
    assert reached == [True]


@pytest.mark.parametrize(
    "change", ["nan", "bool_schema", "revision", "partition", "duplicate", "unknown_version"]
)
def test_invalid_price_formats_and_values(tmp_path: Path, change: str) -> None:
    row = _row()
    schema = PRICE_SCHEMA
    if change == "nan":
        row["close"] = float("nan")
    elif change == "bool_schema":
        schema = PRICE_SCHEMA.set(0, pa.field("schema_version", pa.bool_()))
        row["schema_version"] = True
    elif change == "revision":
        schema = PRICE_SCHEMA.append(pa.field("revision_id", pa.string()))
        row["revision_id"] = "v2"
    rows = [row, row] if change == "duplicate" else [row]
    artifact = _artifact(tmp_path, rows, schema=schema)
    if change == "partition":
        artifact = replace(
            artifact, partition_values={"adjustment_basis": BASIS.value, "year": True}
        )
    view = _view(artifact)
    if change == "unknown_version":
        view = replace(view, transformation_version="canonical-history-v2")
    with pytest.raises(ValueError):
        read_prices(view, tmp_path, _query())


def test_identity_sidecar_is_pinned_even_when_not_queried(tmp_path: Path) -> None:
    price = _artifact(tmp_path, [_row()])
    relative = "identity_bindings/part-00000.parquet"
    path = tmp_path / relative
    path.parent.mkdir()
    payload = b"synthetic pinned identity sidecar"
    path.write_bytes(payload)
    identity = ArtifactReference(
        relative,
        PARQUET_MEDIA_TYPE,
        len(payload),
        None,
        hashlib.sha256(payload).hexdigest(),
        {"role": "identity_bindings"},
    )
    view = _view(price, identity)
    assert read_prices(view, tmp_path, _query())["verified_artifact_count"] == 2
    path.write_bytes(b"x" * len(payload))
    with pytest.raises(ValueError, match="hash mismatch"):
        read_prices(view, tmp_path, _query())


def test_date_filter_and_partition_pruning(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    artifact = _artifact(tmp_path, [_row(), {**_row("i-2"), "observation_date": date(2020, 2, 1)}])
    view = _view(artifact)
    assert read_prices(view, tmp_path, _query())["row_count"] == 1
    original = pinned_prices._validate_prices  # noqa: SLF001 - observe selected partitions
    selected: list[str] = []

    def track(connection: DuckDBPyConnection, pin: pinned_prices._PinnedFile) -> None:
        selected.append(pin.artifact.relative_path)
        original(connection, pin)

    monkeypatch.setattr(pinned_prices, "_validate_prices", track)
    report = read_prices(
        view, tmp_path, _query(start_date=date(2021, 1, 1), end_date=date(2021, 1, 2))
    )
    assert selected == []
    assert report["rows"] == []
    assert report["selected_price_artifact_count"] == 0
    assert report["verified_artifact_count"] == 1


def test_instrument_filter_is_parameterized(tmp_path: Path) -> None:
    injection = "i'); SELECT * FROM secret; --"
    artifact = _artifact(tmp_path, [_row(), _row(injection)])
    report = read_prices(_view(artifact), tmp_path, _query(instruments=(injection,)))
    rows = cast("list[dict[str, object]]", report["rows"])
    assert [row["instrument_id"] for row in rows] == [injection]


def test_aware_non_utc_cutoff_means_same_instant(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, [_row()])
    query = _query(decision_cutoff=datetime.fromisoformat("2020-01-03T09:00:00+09:00"))
    assert read_prices(_view(artifact), tmp_path, query) == read_prices(
        _view(artifact), tmp_path, _query()
    )
