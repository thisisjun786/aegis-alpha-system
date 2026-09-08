# ruff: noqa: PLR2004, PT011
# Synthetic boundary matrices use literal expected values and several refusal reasons.
"""Catalog boundary tests using a safe SQLAlchemy-shaped fake, without a DB."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Engine
from sqlalchemy.sql import Select

from aegis_alpha.data.catalog_access import ArtifactReference, list_datasets, load_dataset
from aegis_alpha.metadata.records import dataset_artifact_digest

if TYPE_CHECKING:
    from collections.abc import Mapping


def _catalog() -> tuple[MagicMock, dict[str, object], dict[str, object]]:
    artifact = {
        "relative_path": "prices/adjustment_basis=SPLIT_ADJUSTED/year=2020/part-00000.parquet",
        "media_type": "application/vnd.apache.parquet",
        "size_bytes": 12,
        "row_count": 1,
        "content_sha256": "a" * 64,
        "partition_values_json": {"adjustment_basis": "SPLIT_ADJUSTED", "year": 2020},
    }
    reference = ArtifactReference(
        relative_path=str(artifact["relative_path"]),
        media_type=str(artifact["media_type"]),
        size_bytes=12,
        row_count=1,
        content_sha256="a" * 64,
        partition_values={"adjustment_basis": "SPLIT_ADJUSTED", "year": 2020},
    )
    row = {
        "dataset_id": "canonical-market-data",
        "dataset_version": "canonical-v1.test",
        "schema_version": 1,
        "transformation_version": "aas-data-009.canonical-v1",
        "aggregate_content_sha256": dataset_artifact_digest((reference,)),
        "row_count": 1,
        "canonical_eligible": False,
        "backtest_eligible": False,
        "paper_eligible": False,
        "order_eligible": False,
        "identity_coverage": Decimal("1.00000"),
        "created_at_utc": datetime(2020, 1, 1, tzinfo=UTC),
    }
    engine = MagicMock(spec=Engine)
    return engine, row, artifact


def _results(
    engine: MagicMock, row: Mapping[str, object] | None, artifacts: list[dict[str, object]]
) -> MagicMock:
    connection = engine.connect.return_value.__enter__.return_value
    header = MagicMock()
    header.mappings.return_value.one_or_none.return_value = row
    files = MagicMock()
    files.mappings.return_value = artifacts
    connection.execute.side_effect = [header, files]
    return connection


def test_load_is_exact_read_only_and_immutable() -> None:
    engine, row, artifact = _catalog()
    connection = _results(engine, row, [artifact])
    view = load_dataset(engine, "canonical-market-data", "canonical-v1.test")
    assert view.dataset_version == row["dataset_version"]
    assert view.eligibility.backtest is False
    assert (
        json.loads(json.dumps(view.to_dict(), allow_nan=False))["artifacts"][0]["size_bytes"] == 12
    )
    for call in connection.execute.call_args_list:
        statement = call.args[0]
        assert isinstance(statement, Select)
        assert "canonical-market-data" in statement.compile().params.values()
        assert "canonical-v1.test" in statement.compile().params.values()
    with pytest.raises(FrozenInstanceError):
        setattr(view, "row_count", 2)  # noqa: B010 - explicit frozen-object refusal test
    with pytest.raises(TypeError):
        view.artifacts[0].partition_values["year"] = 2021  # ty: ignore[invalid-assignment] # immutable boundary
    artifact["partition_values_json"] = {"year": 2022}
    assert view.artifacts[0].partition_values["year"] == 2020
    connection.commit.assert_not_called()


def test_lookup_binds_sql_metacharacters() -> None:
    engine, row, artifact = _catalog()
    identifier = "data'; DROP TABLE dataset_versions; --"
    connection = _results(engine, row, [artifact])
    load_dataset(engine, identifier, "canonical-v1.test")
    statement = connection.execute.call_args_list[0].args[0]
    assert identifier not in str(statement)
    assert identifier in statement.compile().params.values()


def test_missing_dataset_refuses_without_reading_artifacts() -> None:
    engine, _, _ = _catalog()
    connection = _results(engine, None, [])
    with pytest.raises(ValueError, match="not registered"):
        load_dataset(engine, "missing", "v1")
    assert connection.execute.call_count == 1


@pytest.mark.parametrize(
    "mutation", ["hash", "missing", "duplicate", "bool_size", "nan_partition", "flag"]
)
def test_invalid_catalog_fails_closed(mutation: str) -> None:
    engine, row, artifact = _catalog()
    artifacts = [artifact]
    if mutation == "hash":
        row["aggregate_content_sha256"] = "b" * 64
    elif mutation == "missing":
        artifacts = []
    elif mutation == "duplicate":
        artifacts *= 2
    elif mutation == "bool_size":
        artifact["size_bytes"] = True
    elif mutation == "nan_partition":
        artifact["partition_values_json"] = {"year": float("nan")}
    else:
        row["backtest_eligible"] = 1
    _results(engine, row, artifacts)
    with pytest.raises((TypeError, ValueError, RuntimeError)):
        load_dataset(engine, "canonical-market-data", "canonical-v1.test")


@pytest.mark.parametrize(
    "path", ["../bad", "/bad", "a//b", "a/./b", "a/*", "a/[x]", "a\\b", "a\x00b", "~/.ssh"]
)
def test_artifact_paths_refuse_aliases_and_patterns(path: str) -> None:
    reference = ArtifactReference("safe", "test", 1, None, "a" * 64, {})
    with pytest.raises(ValueError):
        replace(reference, relative_path=path)


def test_list_is_json_safe_and_ordered_select() -> None:
    engine, row, _ = _catalog()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.mappings.return_value = [row]
    result = list_datasets(engine)
    assert json.loads(json.dumps(result, allow_nan=False))[0]["backtest_eligible"] is False
    assert result[0]["created_at_utc"] == "2020-01-01T00:00:00Z"
    statement = connection.execute.call_args.args[0]
    assert "ORDER BY dataset_versions.dataset_id, dataset_versions.dataset_version" in str(
        statement
    )
    connection.commit.assert_not_called()
