"""Database-level tests for eligibility ledger triggers and constraints."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from alembic import command
from sqlalchemy import Connection, Engine, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from aegis_alpha.metadata.eligibility_schema import eligibility_decisions
from aegis_alpha.metadata.schema import dataset_versions

if TYPE_CHECKING:
    from alembic.config import Config

_VALID_RECEIPT = "a" * 64

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


def _decision_values(
    *,
    flag: str = "backtest",
    polarity: str = "GRANT",
    owner_receipt_sha256: str = _VALID_RECEIPT,
) -> dict[str, _SqlValue]:
    return {
        "dataset_id": "dataset",
        "dataset_version": "v1",
        "flag": flag,
        "polarity": polarity,
        "owner_receipt_sha256": owner_receipt_sha256,
        "decided_at_utc": datetime(2026, 8, 22, tzinfo=UTC),
        "decided_by": "test-owner",
    }


@pytest.fixture
def decision_connection(clean_postgres: Engine) -> Iterator[Connection]:
    """Provide a connection whose decision rows are always rolled back."""
    with clean_postgres.connect() as connection:
        transaction = connection.begin()
        try:
            yield connection
        finally:
            transaction.rollback()


def _insert_dataset(connection: Connection) -> None:
    connection.execute(dataset_versions.insert().values(_dataset_values()))


def _append_decision(
    connection: Connection,
    flag: str = "backtest",
    polarity: str = "GRANT",
) -> None:
    connection.execute(
        eligibility_decisions.insert().values(_decision_values(flag=flag, polarity=polarity))
    )


def _insert_decision_and_parent(connection: Connection) -> None:
    _insert_dataset(connection)
    _append_decision(connection)


def test_appends_grant_row(decision_connection: Connection) -> None:
    _insert_decision_and_parent(decision_connection)

    row = decision_connection.execute(eligibility_decisions.select()).mappings().one()
    assert row["flag"] == "backtest"
    assert row["polarity"] == "GRANT"
    assert row["owner_receipt_sha256"] == _VALID_RECEIPT


def test_rejects_decision_update(decision_connection: Connection) -> None:
    _insert_decision_and_parent(decision_connection)

    with pytest.raises(DBAPIError, match="append-only"):
        decision_connection.execute(
            eligibility_decisions.update()
            .where(eligibility_decisions.c.dataset_id == "dataset")
            .values(polarity="REVOKE")
        )


def test_rejects_decision_delete(decision_connection: Connection) -> None:
    _insert_decision_and_parent(decision_connection)

    with pytest.raises(DBAPIError, match="append-only"):
        decision_connection.execute(
            eligibility_decisions.delete().where(eligibility_decisions.c.dataset_id == "dataset")
        )


@pytest.mark.parametrize(("flag", "polarity"), [("canonical", "GRANT"), ("backtest", "ALLOW")])
def test_rejects_invalid_decision_variant(
    decision_connection: Connection,
    flag: str,
    polarity: str,
) -> None:
    _insert_dataset(decision_connection)

    with pytest.raises(IntegrityError):
        decision_connection.execute(
            eligibility_decisions.insert().values(_decision_values(flag=flag, polarity=polarity))
        )


@pytest.mark.parametrize("receipt", ["", "A" * 64, "a" * 63])
def test_rejects_invalid_owner_receipt(
    decision_connection: Connection,
    receipt: str,
) -> None:
    _insert_dataset(decision_connection)

    with pytest.raises(IntegrityError):
        decision_connection.execute(
            eligibility_decisions.insert().values(_decision_values(owner_receipt_sha256=receipt))
        )


def test_rejects_missing_dataset_version(decision_connection: Connection) -> None:
    with pytest.raises(IntegrityError):
        decision_connection.execute(eligibility_decisions.insert().values(_decision_values()))


def test_grant_backtest_allows_flag(decision_connection: Connection) -> None:
    _insert_dataset(decision_connection)
    _append_decision(decision_connection)

    decision_connection.execute(
        dataset_versions.update()
        .where(dataset_versions.c.dataset_id == "dataset")
        .values(backtest_eligible=True)
    )

    assert decision_connection.scalar(select(dataset_versions.c.backtest_eligible)) is True


def test_canonical_flag_remains_blocked_after_grant(
    decision_connection: Connection,
) -> None:
    _insert_dataset(decision_connection)
    _append_decision(decision_connection)

    with pytest.raises(DBAPIError, match="canonical eligibility is permanently blocked"):
        decision_connection.execute(
            dataset_versions.update()
            .where(dataset_versions.c.dataset_id == "dataset")
            .values(canonical_eligible=True)
        )


def test_grant_paper_requires_current_backtest_grant(
    decision_connection: Connection,
) -> None:
    _insert_dataset(decision_connection)

    with pytest.raises(DBAPIError):
        _append_decision(decision_connection, "paper")


def test_grant_order_requires_current_paper_grant(
    decision_connection: Connection,
) -> None:
    _insert_dataset(decision_connection)
    _append_decision(decision_connection)

    with pytest.raises(DBAPIError):
        _append_decision(decision_connection, "order")


def test_revoke_backtest_cascades_and_reblocks(
    decision_connection: Connection,
) -> None:
    _insert_dataset(decision_connection)
    for flag in ("backtest", "paper", "order"):
        _append_decision(decision_connection, flag)

    _append_decision(decision_connection, polarity="REVOKE")

    rows = decision_connection.execute(
        select(eligibility_decisions.c.flag, eligibility_decisions.c.polarity)
    ).tuples()
    assert sorted(rows) == [
        ("backtest", "GRANT"),
        ("backtest", "REVOKE"),
        ("order", "GRANT"),
        ("order", "REVOKE"),
        ("paper", "GRANT"),
        ("paper", "REVOKE"),
    ]

    _append_decision(decision_connection)
    with pytest.raises(DBAPIError), decision_connection.begin_nested():
        decision_connection.execute(
            dataset_versions.update()
            .where(dataset_versions.c.dataset_id == "dataset")
            .values(backtest_eligible=True, paper_eligible=True)
        )


def test_revoke_clears_promoted_flags_in_same_transaction(
    decision_connection: Connection,
) -> None:
    _insert_dataset(decision_connection)
    for flag in ("backtest", "paper", "order"):
        _append_decision(decision_connection, flag)
    decision_connection.execute(
        dataset_versions.update()
        .where(dataset_versions.c.dataset_id == "dataset")
        .values(
            backtest_eligible=True,
            paper_eligible=True,
            order_eligible=True,
        )
    )

    _append_decision(decision_connection, polarity="REVOKE")

    flags = decision_connection.execute(
        select(
            dataset_versions.c.backtest_eligible,
            dataset_versions.c.paper_eligible,
            dataset_versions.c.order_eligible,
        )
    ).one()
    assert flags == (False, False, False)


def test_new_grant_after_revoke_uses_highest_sequence(
    decision_connection: Connection,
) -> None:
    _insert_dataset(decision_connection)
    _append_decision(decision_connection)
    _append_decision(decision_connection, polarity="REVOKE")
    _append_decision(decision_connection)

    decision_connection.execute(
        dataset_versions.update()
        .where(dataset_versions.c.dataset_id == "dataset")
        .values(backtest_eligible=True)
    )

    assert decision_connection.scalar(select(dataset_versions.c.backtest_eligible)) is True


def test_lowering_stage_with_higher_flag_true_fails_closed(
    decision_connection: Connection,
) -> None:
    _insert_dataset(decision_connection)
    for flag in ("backtest", "paper", "order"):
        _append_decision(decision_connection, flag)
    decision_connection.execute(
        dataset_versions.update()
        .where(dataset_versions.c.dataset_id == "dataset")
        .values(
            backtest_eligible=True,
            paper_eligible=True,
            order_eligible=True,
        )
    )

    with pytest.raises(DBAPIError):
        decision_connection.execute(
            dataset_versions.update()
            .where(dataset_versions.c.dataset_id == "dataset")
            .values(paper_eligible=False)
        )


def test_upgrade_head_preserves_v1_rows_and_replaces_constraint(
    alembic_config: Config,
    postgres_engine: Engine,
) -> None:
    command.downgrade(alembic_config, "base")
    try:
        command.upgrade(alembic_config, "20260729_0001")
        with postgres_engine.begin() as connection:
            connection.execute(dataset_versions.insert().values(_dataset_values()))

        command.upgrade(alembic_config, "head")
        command.check(alembic_config)
        with postgres_engine.connect() as connection:
            flags = connection.execute(
                select(
                    dataset_versions.c.canonical_eligible,
                    dataset_versions.c.backtest_eligible,
                    dataset_versions.c.paper_eligible,
                    dataset_versions.c.order_eligible,
                )
            ).one()
            constraint_names = set(
                connection.execute(
                    text(
                        "SELECT conname FROM pg_constraint "
                        "WHERE conrelid = 'dataset_versions'::regclass"
                    )
                ).scalars()
            )

        assert tuple(flags) == (False, False, False, False)
        assert "ck_dataset_versions_eligibility_blocked_in_v1" not in constraint_names
        assert "ck_dataset_versions_canonical_eligibility_blocked" in constraint_names
    finally:
        command.downgrade(alembic_config, "base")
        command.upgrade(alembic_config, "head")
