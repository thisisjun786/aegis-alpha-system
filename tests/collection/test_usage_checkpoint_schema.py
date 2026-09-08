from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import DateTime, Engine, inspect, select
from sqlalchemy.exc import DBAPIError, IntegrityError

from aegis_alpha.collection.schema import collection_usage_checkpoints

_START = datetime(2026, 8, 17, tzinfo=UTC)
_END = datetime(2026, 8, 18, tzinfo=UTC)
_GENERATED = _END + timedelta(minutes=1)


def _valid_checkpoint(checkpoint_id: str = "usage-checkpoint-1") -> dict[str, object]:
    return {
        "checkpoint_id": checkpoint_id,
        "schema_version": 1,
        "provider": "fmp",
        "coverage_start_utc": _START,
        "coverage_end_utc": _END,
        "usage_record_count": 2,
        "usage_records_root_sha256": "a" * 64,
        "authority_id": "aas-usage-authority-v1",
        "key_id": "ed25519:2026-08-18",
        "generated_at_utc": _GENERATED,
        "signature_algorithm": "ed25519",
        "signature_version": 1,
        "signature": bytes(range(64)),
    }


def test_checkpoint_table_uses_utc_timestamps_and_has_no_private_key(
    clean_postgres: Engine,
) -> None:
    columns = {
        column["name"]: column
        for column in inspect(clean_postgres).get_columns("collection_usage_checkpoints")
    }

    for name in ("coverage_start_utc", "coverage_end_utc", "generated_at_utc"):
        column_type = columns[name]["type"]
        assert isinstance(column_type, DateTime)
        assert column_type.timezone is True
    assert "private_key" not in columns
    assert all("private" not in name for name in columns)


def test_signed_usage_checkpoint_appends(clean_postgres: Engine) -> None:
    row = _valid_checkpoint()
    with clean_postgres.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(collection_usage_checkpoints.insert().values(**row))
            stored = (
                connection.execute(
                    select(collection_usage_checkpoints).where(
                        collection_usage_checkpoints.c.checkpoint_id == row["checkpoint_id"]
                    )
                )
                .mappings()
                .one()
            )

            assert {key: stored[key] for key in row} == row
            assert stored["registered_at_utc"].tzinfo is not None
        finally:
            transaction.rollback()


def test_checkpoint_identity_is_unique(clean_postgres: Engine) -> None:
    row = _valid_checkpoint()
    with clean_postgres.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(collection_usage_checkpoints.insert().values(**row))
            with pytest.raises(IntegrityError):
                connection.execute(collection_usage_checkpoints.insert().values(**row))
        finally:
            transaction.rollback()


_INVALID_OVERRIDES = [
    pytest.param({"checkpoint_id": " "}, id="blank-checkpoint-id"),
    pytest.param({"checkpoint_id": " checkpoint"}, id="unnormalized-checkpoint-id"),
    pytest.param({"schema_version": 2}, id="unsupported-schema-version"),
    pytest.param({"provider": "FMP"}, id="unnormalized-provider"),
    pytest.param({"provider": "fmp provider"}, id="provider-with-whitespace"),
    pytest.param({"coverage_end_utc": _START}, id="empty-coverage-interval"),
    pytest.param({"coverage_end_utc": _START - timedelta(seconds=1)}, id="reversed-interval"),
    pytest.param({"usage_record_count": -1}, id="negative-record-count"),
    pytest.param({"usage_records_root_sha256": "A" * 64}, id="uppercase-root"),
    pytest.param({"usage_records_root_sha256": "a" * 63}, id="short-root"),
    pytest.param({"authority_id": ""}, id="blank-authority-id"),
    pytest.param({"key_id": "key id"}, id="key-id-with-whitespace"),
    pytest.param({"generated_at_utc": _END - timedelta(seconds=1)}, id="premature-generation"),
    pytest.param({"signature_algorithm": "rsa-pss-sha256"}, id="unsupported-algorithm"),
    pytest.param({"signature_version": 2}, id="unsupported-signature-version"),
    pytest.param({"signature": b""}, id="empty-signature"),
    pytest.param({"signature": b"x" * 63}, id="wrong-ed25519-signature-size"),
]


@pytest.mark.parametrize("overrides", _INVALID_OVERRIDES)
def test_invalid_signed_usage_checkpoint_is_rejected(
    clean_postgres: Engine,
    overrides: dict[str, object],
) -> None:
    row = _valid_checkpoint()
    row.update(overrides)

    with pytest.raises(IntegrityError), clean_postgres.begin() as connection:
        connection.execute(collection_usage_checkpoints.insert().values(**row))


@pytest.mark.parametrize("mutation", ["update", "delete"])
def test_signed_usage_checkpoint_is_append_only(
    clean_postgres: Engine,
    mutation: str,
) -> None:
    row = _valid_checkpoint(f"usage-checkpoint-{mutation}")
    with clean_postgres.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(collection_usage_checkpoints.insert().values(**row))
            where = collection_usage_checkpoints.c.checkpoint_id == row["checkpoint_id"]
            statement = (
                collection_usage_checkpoints.update()
                .where(where)
                .values(authority_id="different-authority")
                if mutation == "update"
                else collection_usage_checkpoints.delete().where(where)
            )
            with pytest.raises(DBAPIError, match="append-only"):
                connection.execute(statement)
        finally:
            transaction.rollback()
