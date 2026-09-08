"""Alembic environment for retained contracts and scoped runtime adoption."""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, engine_from_config, pool

from aegis_alpha.collection.schema import metadata as collection_metadata  # noqa: F401
from aegis_alpha.data import canonical_generation_schema  # noqa: F401
from aegis_alpha.identity.schema import metadata as identity_metadata  # noqa: F401
from aegis_alpha.metadata import (
    adoption_schema,  # noqa: F401
    eligibility_schema,  # noqa: F401
    feature_contract_schema,  # noqa: F401
)
from aegis_alpha.metadata.database import load_database_url
from aegis_alpha.metadata.schema import metadata

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = metadata
# Container deployment (ADR 0014): the append-only migration guards accept
# an explicitly named owned database. The test conftest sets these via
# config.attributes; the container reads them from env vars so the same
# fail-closed check runs against the provisioned ledger database.
_container_database_name = os.environ.get("AAS_MIGRATION_DATABASE_NAME")
_container_owner_token = os.environ.get("AAS_MIGRATION_OWNER_TOKEN")
if _container_database_name is not None and _container_owner_token is not None:
    config.attributes.setdefault("aas_test_database_name", _container_database_name)
    config.attributes.setdefault("aas_test_owner_token", _container_owner_token)


def run_migrations_offline() -> None:
    url = _configured_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    supplied = config.attributes.get("connection")
    if supplied is not None:
        if not isinstance(supplied, Connection):
            raise TypeError("migration connection must be a SQLAlchemy connection")
        _run_online_connection(supplied)
        return
    if not config.get_main_option("sqlalchemy.url"):
        config.set_main_option("sqlalchemy.url", _configured_url().replace("%", "%%"))
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        _run_online_connection(connection)


def _run_online_connection(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        include_schemas=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def _configured_url() -> str:
    configured = config.get_main_option("sqlalchemy.url")
    return configured or load_database_url()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
