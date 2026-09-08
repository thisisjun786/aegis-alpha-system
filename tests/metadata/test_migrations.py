from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

import pytest
from alembic import command
from sqlalchemy import Engine, inspect, make_url, text

from aegis_alpha.metadata.eligibility_schema import eligibility_decisions
from aegis_alpha.metadata.schema import dataset_versions

if TYPE_CHECKING:
    from alembic.config import Config


class OwnedDatabaseContract(Protocol):
    name: str
    created_empty: bool


_EXPECTED_TABLES = {
    "alembic_version",
    "collection_run_events",
    "collection_run_plans",
    "collection_run_receipts",
    "collection_runs",
    "collection_usage_checkpoints",
    "collection_usage_records",
    "collection_watermarks",
    "canonical_generation_artifacts",
    "canonical_generation_attestations",
    "canonical_generation_sources",
    "canonical_generations",
    "canonical_partition_evidence",
    "canonical_series",
    "canonical_series_heads",
    "dataset_artifacts",
    "dataset_input_files",
    "dataset_sources",
    "dataset_versions",
    "eligibility_decisions",
    "feature_contract_inputs",
    "collection_targets",
    "feature_contracts",
    "identity_identifier_assertions",
    "identity_instruments",
    "identity_issuers",
    "identity_mapping_conflicts",
    "identity_provider_mappings",
    "quality_results",
    "source_snapshot_files",
    "source_snapshots",
}


def test_metadata_tests_use_owned_postgresql_database(
    postgres_url: str,
    test_database: OwnedDatabaseContract,
) -> None:
    url = make_url(postgres_url)

    assert url.get_backend_name() == "postgresql"
    assert url.drivername == "postgresql+psycopg"
    assert url.database is not None
    assert url.database.startswith("aas_owned_")
    assert url.database.endswith("_test")
    assert test_database.name == url.database
    assert test_database.created_empty is True


def test_initial_migration_round_trips_only_in_owned_empty_database(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.downgrade(alembic_config, "base")
    assert inspect(postgres_engine).get_table_names() == ["alembic_version"]

    command.upgrade(alembic_config, "head")
    assert set(inspect(postgres_engine).get_table_names()) == _EXPECTED_TABLES
    command.check(alembic_config)

    command.downgrade(alembic_config, "base")
    assert inspect(postgres_engine).get_table_names() == ["alembic_version"]
    with postgres_engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM alembic_version")) == 0

    command.upgrade(alembic_config, "head")
    assert set(inspect(postgres_engine).get_table_names()) == _EXPECTED_TABLES


def test_downgrade_refuses_database_with_unowned_user_table(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.upgrade(alembic_config, "head")
    with postgres_engine.begin() as connection:
        connection.execute(text("CREATE TABLE unowned_user_data (id integer PRIMARY KEY)"))

    with pytest.raises(RuntimeError, match="refusing destructive downgrade"):
        command.downgrade(alembic_config, "base")

    assert "unowned_user_data" in inspect(postgres_engine).get_table_names()
    with postgres_engine.begin() as connection:
        connection.execute(text("DROP TABLE unowned_user_data"))


def test_downgrade_0007_refuses_leftover_ledger_row(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.upgrade(alembic_config, "head")
    with postgres_engine.begin() as connection:
        connection.execute(
            dataset_versions.insert().values(
                dataset_id="dataset",
                dataset_version="v1",
                schema_version=1,
                row_count=1,
                coverage_start=None,
                coverage_end=None,
                identity_coverage=1,
                freshness_status="PASS",
                transformation_version="test-v1",
                aggregate_content_sha256="a" * 64,
                canonical_eligible=False,
                backtest_eligible=False,
                paper_eligible=False,
                order_eligible=False,
                created_at_utc=datetime(2026, 7, 29, tzinfo=UTC),
            )
        )
        connection.execute(
            eligibility_decisions.insert().values(
                dataset_id="dataset",
                dataset_version="v1",
                flag="backtest",
                polarity="GRANT",
                owner_receipt_sha256="a" * 64,
                decided_at_utc=datetime(2026, 8, 22, tzinfo=UTC),
                decided_by="test-owner",
            )
        )

    try:
        with pytest.raises(
            RuntimeError,
            match="refusing destructive downgrade: eligibility_decisions contains rows",
        ):
            command.downgrade(alembic_config, "20260822_0006")
        assert "eligibility_decisions" in inspect(postgres_engine).get_table_names()
        with postgres_engine.connect() as connection:
            assert connection.scalar(text("SELECT COUNT(*) FROM eligibility_decisions")) == 1
    finally:
        tables = set(inspect(postgres_engine).get_table_names())
        with postgres_engine.begin() as connection:
            if "eligibility_decisions" in tables:
                connection.execute(text("TRUNCATE TABLE eligibility_decisions"))
            if "dataset_versions" in tables:
                connection.execute(
                    text("DELETE FROM dataset_versions WHERE dataset_id = 'dataset'")
                )


def test_wave4_downgrade_removes_generation_trigger_functions(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.upgrade(alembic_config, "head")
    command.downgrade(alembic_config, "20260731_0003")
    with postgres_engine.connect() as connection:
        assert (
            connection.scalar(text("SELECT to_regprocedure('canonical_enforce_forward_head()')"))
            is None
        )
        assert (
            connection.scalar(
                text("SELECT to_regprocedure('canonical_enforce_generation_sequence()')")
            )
            is None
        )
        assert not {
            "canonical_series",
            "canonical_generations",
            "canonical_generation_sources",
            "canonical_generation_artifacts",
            "canonical_partition_evidence",
            "canonical_generation_attestations",
            "canonical_series_heads",
        } & set(inspect(postgres_engine).get_table_names())
    command.upgrade(alembic_config, "head")
