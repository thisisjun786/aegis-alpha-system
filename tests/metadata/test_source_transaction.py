from __future__ import annotations

from sqlalchemy import Engine, text

from aegis_alpha.metadata.registry import MetadataRegistry
from tests.metadata.test_registry import _source_registration


def test_source_registration_joins_outer_transaction_without_committing(
    clean_postgres: Engine,
) -> None:
    registry = MetadataRegistry(clean_postgres)
    source = _source_registration()
    with clean_postgres.connect() as connection:
        transaction = connection.begin()
        try:
            registry.register_source_snapshot(source, connection=connection)
            registry.register_source_snapshot(source, connection=connection)
            assert connection.scalar(text("SELECT count(*) FROM source_snapshots")) == 1
        finally:
            transaction.rollback()
    with clean_postgres.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM source_snapshots")) == 0
        assert connection.scalar(text("SELECT count(*) FROM source_snapshot_files")) == 0
