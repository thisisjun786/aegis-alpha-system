"""Rehearse the 0001-to-0007 registry extension chain on a disposable database."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from alembic import command
from sqlalchemy import Engine, func, inspect, select, text

from aegis_alpha.metadata.eligibility_schema import eligibility_decisions
from aegis_alpha.metadata.feature_contract_schema import (
    feature_contract_inputs,
    feature_contracts,
)
from aegis_alpha.metadata.schema import dataset_versions
from tests.metadata.test_migrations import _EXPECTED_TABLES

if TYPE_CHECKING:
    from alembic.config import Config


def _assert_empty_extension_tables(engine: Engine) -> None:
    assert set(inspect(engine).get_table_names()) == _EXPECTED_TABLES
    with engine.connect() as connection:
        for table in (feature_contracts, feature_contract_inputs, eligibility_decisions):
            assert connection.scalar(select(func.count()).select_from(table)) == 0
        flags = connection.execute(
            select(
                dataset_versions.c.canonical_eligible,
                dataset_versions.c.backtest_eligible,
                dataset_versions.c.paper_eligible,
                dataset_versions.c.order_eligible,
            )
        ).all()
    assert all(not any(row) for row in flags)


def test_upgrade_downgrade_upgrade_leaves_empty_extension_tables(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.downgrade(alembic_config, "base")
    command.upgrade(alembic_config, "head")
    _assert_empty_extension_tables(postgres_engine)
    command.check(alembic_config)

    command.downgrade(alembic_config, "base")
    command.upgrade(alembic_config, "head")
    _assert_empty_extension_tables(postgres_engine)


def test_downgrade_refuses_when_feature_contract_row_exists(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.upgrade(alembic_config, "head")
    with postgres_engine.begin() as connection:
        connection.execute(
            feature_contracts.insert().values(
                contract_name="feature-contract",
                contract_version="v1",
                schema_version=1,
                parameters_json={},
                definition_artifact_sha256="a" * 64,
                canonical_serialization_sha256="b" * 64,
                output_schema_ref="test-schema-ref",
                consumes_capital=True,
                consumes_totalreturn=False,
                created_at_utc=datetime(2026, 8, 22, tzinfo=UTC),
            )
        )

    try:
        with pytest.raises(
            RuntimeError,
            match=(
                "refusing destructive downgrade: feature_contracts or "
                "feature_contract_inputs contain rows"
            ),
        ):
            command.downgrade(alembic_config, "base")
        assert "feature_contracts" in inspect(postgres_engine).get_table_names()
        with postgres_engine.connect() as connection:
            remaining = connection.execute(
                select(
                    feature_contracts.c.contract_name,
                    feature_contracts.c.contract_version,
                )
            ).one()
        assert remaining == ("feature-contract", "v1")
    finally:
        tables = set(inspect(postgres_engine).get_table_names())
        with postgres_engine.begin() as connection:
            if "feature_contracts" in tables:
                connection.execute(
                    text("TRUNCATE TABLE feature_contract_inputs, feature_contracts")
                )
        command.upgrade(alembic_config, "head")
