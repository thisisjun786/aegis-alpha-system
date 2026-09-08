from __future__ import annotations

import os
import uuid

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, inspect, make_url, text

_REGISTRY_TABLES = {
    "dataset_artifacts",
    "dataset_input_files",
    "dataset_sources",
    "dataset_versions",
    "quality_results",
    "source_snapshot_files",
    "source_snapshots",
}
_COLLECTION_TABLES = {
    "collection_run_events",
    "collection_run_plans",
    "collection_run_receipts",
    "collection_runs",
    "collection_usage_checkpoints",
    "collection_usage_records",
    "collection_watermarks",
}
_IDENTITY_TABLES = {
    "identity_identifier_assertions",
    "identity_instruments",
    "identity_issuers",
    "identity_mapping_conflicts",
    "identity_provider_mappings",
}
_CANONICAL_GENERATION_TABLES = {
    "canonical_generation_artifacts",
    "canonical_generation_attestations",
    "canonical_generation_sources",
    "canonical_generations",
    "canonical_partition_evidence",
    "canonical_series",
    "canonical_series_heads",
}
_EXPECTED_TABLES = {
    "alembic_version",
    "collection_targets",
    "eligibility_decisions",
    "feature_contract_inputs",
    "feature_contracts",
    *_REGISTRY_TABLES,
    *_COLLECTION_TABLES,
    *_IDENTITY_TABLES,
    *_CANONICAL_GENERATION_TABLES,
}
_EXPECTED_VIEWS = {"collection_run_states", "collection_watermark_current"}


def test_head_migration_creates_collection_control_plane(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.upgrade(alembic_config, "head")

    assert set(inspect(postgres_engine).get_table_names()) == _EXPECTED_TABLES
    assert set(inspect(postgres_engine).get_view_names()) >= _EXPECTED_VIEWS
    command.check(alembic_config)


def test_usage_checkpoint_revision_round_trip(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.upgrade(alembic_config, "head")
    assert "collection_usage_checkpoints" in inspect(postgres_engine).get_table_names()

    command.downgrade(alembic_config, "20260731_0003")
    assert "collection_usage_checkpoints" not in inspect(postgres_engine).get_table_names()

    command.upgrade(alembic_config, "head")
    assert "collection_usage_checkpoints" in inspect(postgres_engine).get_table_names()
    command.check(alembic_config)


def test_collection_revision_downgrade_preserves_provenance_registry(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.upgrade(alembic_config, "head")
    assert set(inspect(postgres_engine).get_table_names()) == _EXPECTED_TABLES

    command.downgrade(alembic_config, "20260729_0001")
    assert set(inspect(postgres_engine).get_table_names()) == {
        "alembic_version",
        *_REGISTRY_TABLES,
    }
    assert inspect(postgres_engine).get_view_names() == []

    command.upgrade(alembic_config, "head")
    assert set(inspect(postgres_engine).get_table_names()) == _EXPECTED_TABLES
    assert set(inspect(postgres_engine).get_view_names()) >= _EXPECTED_VIEWS


@pytest.mark.database
def test_downgrade_refuses_forged_fixture_markers() -> None:
    value = os.environ.get("AAS_TEST_DATABASE_URL")
    if value is None:
        pytest.fail("migration tests require AAS_TEST_DATABASE_URL")
    base_url = make_url(value)
    name_token = uuid.uuid4().hex
    comment_token = uuid.uuid4().hex
    database_name = f"aas_owned_{name_token}_test"
    admin_engine = create_engine(
        base_url,
        isolation_level="AUTOCOMMIT",
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            connection.exec_driver_sql(
                f"COMMENT ON DATABASE \"{database_name}\" IS '{comment_token}'"
            )
        scratch_url = base_url.set(database=database_name).render_as_string(hide_password=False)
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", scratch_url.replace("%", "%%"))
        config.attributes["aas_test_database_name"] = database_name
        config.attributes["aas_test_owner_token"] = comment_token
        command.upgrade(config, "20260731_0003")

        with pytest.raises(RuntimeError, match="refusing Wave 4 migration"):
            command.upgrade(config, "head")

        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f"COMMENT ON DATABASE \"{database_name}\" IS '{name_token}'")
        config.attributes["aas_test_owner_token"] = name_token
        command.upgrade(config, "head")

        config.attributes["aas_test_owner_token"] = comment_token
        with pytest.raises(RuntimeError, match="refusing destructive downgrade"):
            command.downgrade(config, "base")
    finally:
        with admin_engine.connect() as connection:
            connection.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :name AND pid <> pg_backend_pid()"
                ),
                {"name": database_name},
            )
            connection.exec_driver_sql(f'DROP DATABASE IF EXISTS "{database_name}"')
        admin_engine.dispose()
