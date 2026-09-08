from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from sqlalchemy.exc import DBAPIError, IntegrityError

from aegis_alpha.metadata.schema import dataset_versions, quality_results

if TYPE_CHECKING:
    from sqlalchemy import Engine


type _SqlValue = str | int | bool | datetime | None


def _dataset_values() -> dict[str, _SqlValue]:
    return {
        "dataset_id": "dataset",
        "dataset_version": "v1",
        "schema_version": 1,
        "row_count": 1,
        "coverage_start": None,
        "coverage_end": None,
        "identity_coverage": 1,
        "freshness_status": "PASS",
        "transformation_version": "test-v1",
        "aggregate_content_sha256": "a" * 64,
        "canonical_eligible": False,
        "backtest_eligible": False,
        "paper_eligible": False,
        "order_eligible": False,
        "created_at_utc": datetime(2026, 7, 29, tzinfo=UTC),
    }


@pytest.mark.parametrize(
    "field",
    [
        "canonical_eligible",
        "backtest_eligible",
        "paper_eligible",
        "order_eligible",
    ],
)
def test_dataset_insert_requires_current_grant_for_true_eligibility_flag(
    clean_postgres: Engine,
    field: str,
) -> None:
    values = _dataset_values()
    values[field] = True

    with pytest.raises(DBAPIError), clean_postgres.begin() as connection:
        connection.execute(dataset_versions.insert().values(values))


@pytest.mark.parametrize(
    "field",
    [
        "canonical_eligible",
        "backtest_eligible",
        "paper_eligible",
        "order_eligible",
    ],
)
def test_dataset_update_requires_current_grant_for_true_eligibility_flag(
    clean_postgres: Engine,
    field: str,
) -> None:
    with clean_postgres.begin() as connection:
        connection.execute(dataset_versions.insert().values(_dataset_values()))

    with pytest.raises(DBAPIError), clean_postgres.begin() as connection:
        connection.execute(
            dataset_versions.update()
            .where(dataset_versions.c.dataset_id == "dataset")
            .values({field: True})
        )


@pytest.mark.parametrize("field", ["dataset_id", "dataset_version", "transformation_version"])
def test_database_rejects_blank_dataset_identity_and_version_fields(
    clean_postgres: Engine,
    field: str,
) -> None:
    values = _dataset_values()
    values[field] = "  "

    with pytest.raises(IntegrityError), clean_postgres.begin() as connection:
        connection.execute(dataset_versions.insert().values(values))


@pytest.mark.parametrize("field", ["result_id", "check_id", "check_version"])
def test_database_rejects_blank_quality_identity_and_version_fields(
    clean_postgres: Engine,
    field: str,
) -> None:
    with clean_postgres.begin() as connection:
        connection.execute(dataset_versions.insert().values(_dataset_values()))
    values = {
        "result_id": "quality",
        "check_id": "check",
        "check_version": "1",
        "status": "PASS",
        "dimensions_json": ["integrity"],
        "details_json": ["details"],
        "safe_next_action": "none",
        "checked_at_utc": datetime(2026, 7, 29, tzinfo=UTC),
        "source_snapshot_id": None,
        "dataset_id": "dataset",
        "dataset_version": "v1",
    }
    values[field] = " "

    with pytest.raises(IntegrityError), clean_postgres.begin() as connection:
        connection.execute(quality_results.insert().values(values))


@pytest.mark.parametrize(
    ("source_snapshot_id", "dataset_id", "dataset_version"),
    [
        (None, None, None),
        ("snapshot", "dataset", "v1"),
        (None, "dataset", None),
    ],
)
def test_quality_subject_requires_exactly_one_complete_typed_reference(
    clean_postgres: Engine,
    source_snapshot_id: str | None,
    dataset_id: str | None,
    dataset_version: str | None,
) -> None:
    with pytest.raises(IntegrityError), clean_postgres.begin() as connection:
        connection.execute(
            quality_results.insert().values(
                result_id="quality",
                check_id="check",
                check_version="1",
                status="PASS",
                dimensions_json=["integrity"],
                details_json=["details"],
                safe_next_action="none",
                checked_at_utc=datetime(2026, 7, 29, tzinfo=UTC),
                source_snapshot_id=source_snapshot_id,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            )
        )
