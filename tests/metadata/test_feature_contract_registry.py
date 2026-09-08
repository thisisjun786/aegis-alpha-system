"""Database-level tests for the 0006 feature contract registry.

These tests exercise the frozen physical contract from ADR 0010:
append-only tables, dual hashes, adjustment-basis declaration, and typed
inputs.  They run against the disposable PostgreSQL database and assume the
matching Alembic migration is present.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from alembic import command
from sqlalchemy import Engine, Table, inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from aegis_alpha.metadata.feature_contract_schema import (
    feature_contract_inputs,
    feature_contracts,
)
from aegis_alpha.metadata.schema import dataset_versions

if TYPE_CHECKING:
    from alembic.config import Config
    from sqlalchemy import Connection


def _dataset_values() -> dict[str, object]:
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


def _contract_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "contract_name": "feature-contract",
        "contract_version": "v1",
        "schema_version": 1,
        "parameters_json": {},
        "definition_artifact_sha256": "a" * 64,
        "canonical_serialization_sha256": "b" * 64,
        "output_schema_ref": "test-schema-ref",
        "consumes_capital": True,
        "consumes_totalreturn": False,
        "created_at_utc": datetime(2026, 8, 22, tzinfo=UTC),
    }
    values.update(overrides)
    return values


def _input_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "contract_name": "feature-contract",
        "contract_version": "v1",
        "input_ordinal": 1,
        "input_kind": "dataset_version",
        "dataset_id": "dataset",
        "dataset_version": "v1",
        "upstream_contract_name": None,
        "upstream_contract_version": None,
        "expected_digest_sha256": "a" * 64,
    }
    values.update(overrides)
    return values


@pytest.fixture
def contract_connection(clean_postgres: Engine) -> Iterator[Connection]:
    """A connection whose transaction is always rolled back."""
    with clean_postgres.connect() as connection:
        transaction = connection.begin()
        try:
            yield connection
        finally:
            transaction.rollback()


def _insert_contract_and_parents(connection: Connection) -> None:
    connection.execute(dataset_versions.insert().values(_dataset_values()))
    connection.execute(feature_contracts.insert().values(_contract_values()))
    connection.execute(
        feature_contracts.insert().values(
            _contract_values(
                contract_name="upstream-contract",
                definition_artifact_sha256="e" * 64,
                canonical_serialization_sha256="f" * 64,
            )
        )
    )


def test_inserts_immutable_contract_with_dual_hash_and_inputs(
    contract_connection: Connection,
) -> None:
    _insert_contract_and_parents(contract_connection)
    contract_connection.execute(feature_contract_inputs.insert().values(_input_values()))
    contract_connection.execute(
        feature_contract_inputs.insert().values(
            _input_values(
                input_ordinal=2,
                input_kind="feature_contract",
                dataset_id=None,
                dataset_version=None,
                upstream_contract_name="upstream-contract",
                upstream_contract_version="v1",
                expected_digest_sha256="d" * 64,
            )
        )
    )

    row = (
        contract_connection.execute(
            feature_contracts.select().where(
                feature_contracts.c.contract_name == "feature-contract"
            )
        )
        .mappings()
        .one()
    )
    assert row["definition_artifact_sha256"] == "a" * 64
    assert row["canonical_serialization_sha256"] == "b" * 64
    assert row["consumes_capital"] is True
    assert row["consumes_totalreturn"] is False

    inputs = (
        contract_connection.execute(
            feature_contract_inputs.select().where(
                feature_contract_inputs.c.contract_name == "feature-contract"
            )
        )
        .mappings()
        .all()
    )
    expected_kinds = {"dataset_version", "feature_contract"}
    assert len(inputs) == len(expected_kinds)
    assert {i["input_kind"] for i in inputs} == expected_kinds


def test_rejects_dataset_digest_update_while_feature_contract_references_it(
    contract_connection: Connection,
) -> None:
    _insert_contract_and_parents(contract_connection)
    contract_connection.execute(feature_contract_inputs.insert().values(_input_values()))
    where = (dataset_versions.c.dataset_id == "dataset") & (
        dataset_versions.c.dataset_version == "v1"
    )
    with pytest.raises(DBAPIError, match="frozen"):
        contract_connection.execute(
            dataset_versions.update().where(where).values(aggregate_content_sha256="b" * 64)
        )


def test_insert_rejects_dataset_input_when_digest_does_not_match(
    contract_connection: Connection,
) -> None:
    _insert_contract_and_parents(contract_connection)

    with pytest.raises(DBAPIError, match="digest"):
        contract_connection.execute(
            feature_contract_inputs.insert().values(_input_values(expected_digest_sha256="c" * 64))
        )


def _clear_append_only_contract_rows(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE feature_contract_inputs, feature_contracts"))
        connection.execute(text("DELETE FROM dataset_versions WHERE dataset_id = 'dataset'"))


def test_insert_rejects_stale_digest_after_concurrent_dataset_update(
    clean_postgres: Engine,
) -> None:
    try:
        with clean_postgres.begin() as connection:
            connection.execute(dataset_versions.insert().values(_dataset_values()))
            connection.execute(feature_contracts.insert().values(_contract_values()))
            connection.execute(
                dataset_versions.update()
                .where(
                    (dataset_versions.c.dataset_id == "dataset")
                    & (dataset_versions.c.dataset_version == "v1")
                )
                .values(aggregate_content_sha256="b" * 64)
            )

        with (
            pytest.raises(DBAPIError, match="digest"),
            clean_postgres.begin() as connection,
        ):
            connection.execute(
                feature_contract_inputs.insert().values(
                    _input_values(expected_digest_sha256="a" * 64)
                )
            )
    finally:
        _clear_append_only_contract_rows(clean_postgres)


def test_dataset_digest_update_blocks_while_input_insert_holds_row_lock(
    clean_postgres: Engine,
) -> None:
    locker = clean_postgres.connect()
    updater = clean_postgres.connect()
    try:
        with clean_postgres.begin() as connection:
            connection.execute(dataset_versions.insert().values(_dataset_values()))
            connection.execute(feature_contracts.insert().values(_contract_values()))
        locker_txn = locker.begin()
        locker.execute(
            feature_contract_inputs.insert().values(_input_values(expected_digest_sha256="a" * 64))
        )
        updater.execute(text("SET lock_timeout = '200ms'"))
        where = (dataset_versions.c.dataset_id == "dataset") & (
            dataset_versions.c.dataset_version == "v1"
        )
        with pytest.raises(DBAPIError, match="lock timeout"):
            updater.execute(
                dataset_versions.update().where(where).values(aggregate_content_sha256="b" * 64)
            )
        locker_txn.rollback()
    finally:
        locker.close()
        updater.close()
        _clear_append_only_contract_rows(clean_postgres)


def test_allows_dataset_digest_update_without_feature_contract_reference(
    contract_connection: Connection,
) -> None:
    contract_connection.execute(dataset_versions.insert().values(_dataset_values()))
    where = (dataset_versions.c.dataset_id == "dataset") & (
        dataset_versions.c.dataset_version == "v1"
    )
    contract_connection.execute(
        dataset_versions.update().where(where).values(aggregate_content_sha256="b" * 64)
    )
    digest = contract_connection.execute(
        dataset_versions.select().with_only_columns(dataset_versions.c.aggregate_content_sha256)
    ).scalar_one()
    assert digest == "b" * 64


def test_rejects_contract_update(contract_connection: Connection) -> None:
    _insert_contract_and_parents(contract_connection)
    where = feature_contracts.c.contract_name == "feature-contract"
    with pytest.raises(DBAPIError, match="append-only"):
        contract_connection.execute(
            feature_contracts.update().where(where).values(output_schema_ref="mutated")
        )


@pytest.mark.parametrize("table", [feature_contracts, feature_contract_inputs])
def test_rejects_delete_as_append_only(
    contract_connection: Connection,
    table: Table,
) -> None:
    _insert_contract_and_parents(contract_connection)
    contract_connection.execute(feature_contract_inputs.insert().values(_input_values()))
    where = table.c.contract_name == "feature-contract"
    with pytest.raises(DBAPIError, match="append-only"):
        contract_connection.execute(table.delete().where(where))


def test_rejects_duplicate_canonical_serialization_sha256(
    contract_connection: Connection,
) -> None:
    contract_connection.execute(feature_contracts.insert().values(_contract_values()))
    with pytest.raises(IntegrityError):
        contract_connection.execute(
            feature_contracts.insert().values(
                _contract_values(
                    contract_name="other-contract",
                    definition_artifact_sha256="e" * 64,
                )
            )
        )


def test_rejects_contract_without_consumed_basis(
    contract_connection: Connection,
) -> None:
    with pytest.raises(IntegrityError):
        contract_connection.execute(
            feature_contracts.insert().values(
                _contract_values(consumes_capital=False, consumes_totalreturn=False)
            )
        )


@pytest.mark.parametrize(
    ("input_kind", "dataset_id", "upstream_contract_name"),
    [
        ("dataset_version", None, "upstream-contract"),
        ("feature_contract", "dataset", None),
    ],
)
def test_rejects_input_kind_subject_mismatch(
    contract_connection: Connection,
    input_kind: str,
    dataset_id: str | None,
    upstream_contract_name: str | None,
) -> None:
    _insert_contract_and_parents(contract_connection)
    with pytest.raises(IntegrityError):
        contract_connection.execute(
            feature_contract_inputs.insert().values(
                _input_values(
                    input_ordinal=1,
                    input_kind=input_kind,
                    dataset_id=dataset_id,
                    dataset_version="v1" if dataset_id else None,
                    upstream_contract_name=upstream_contract_name,
                    upstream_contract_version="v1" if upstream_contract_name else None,
                )
            )
        )


def test_upgrade_head_creates_tables_and_passes_check(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.upgrade(alembic_config, "head")
    tables = set(inspect(postgres_engine).get_table_names())
    assert "feature_contracts" in tables
    assert "feature_contract_inputs" in tables
    command.check(alembic_config)


def test_empty_downgrade_to_0005_removes_both_tables(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.upgrade(alembic_config, "head")
    command.downgrade(alembic_config, "20260821_0005")
    tables = set(inspect(postgres_engine).get_table_names())
    assert "feature_contracts" not in tables
    assert "feature_contract_inputs" not in tables


def test_downgrade_0006_refuses_leftover_contract_row(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.upgrade(alembic_config, "head")
    with postgres_engine.begin() as connection:
        connection.execute(dataset_versions.insert().values(_dataset_values()))
        connection.execute(feature_contracts.insert().values(_contract_values()))

    with pytest.raises(RuntimeError):
        command.downgrade(alembic_config, "20260821_0005")

    assert "feature_contracts" in inspect(postgres_engine).get_table_names()

    # Clean up the fixture-owned row without relying on clean_postgres, which
    # cannot DELETE from append-only tables.
    with postgres_engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE feature_contract_inputs, feature_contracts"))


def test_upgrade_0009_refuses_preexisting_mismatched_input_digest(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.upgrade(alembic_config, "head")
    with postgres_engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE feature_contract_inputs, feature_contracts"))
        connection.execute(text("DELETE FROM dataset_versions WHERE dataset_id = 'dataset'"))
    command.downgrade(alembic_config, "20260825_0008")
    with postgres_engine.begin() as connection:
        connection.execute(dataset_versions.insert().values(_dataset_values()))
        connection.execute(feature_contracts.insert().values(_contract_values()))
        connection.execute(
            feature_contract_inputs.insert().values(_input_values(expected_digest_sha256="c" * 64))
        )
    try:
        with pytest.raises(RuntimeError, match="digest"):
            command.upgrade(alembic_config, "20260825_0009")
    finally:
        with postgres_engine.begin() as connection:
            connection.execute(text("TRUNCATE TABLE feature_contract_inputs, feature_contracts"))
            connection.execute(text("DELETE FROM dataset_versions WHERE dataset_id = 'dataset'"))
        command.upgrade(alembic_config, "head")
