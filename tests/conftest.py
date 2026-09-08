from __future__ import annotations

import os
import socket
import tempfile
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, inspect, make_url, text

if TYPE_CHECKING:
    from sqlalchemy.engine import URL

# Production has no data-root default.  The test process installs one explicit,
# already-existing synthetic root before test modules import path-bound
# constants.  A caller-provided root (for mounted real-input acceptance) wins.
_PYTEST_DATA_ROOT = tempfile.TemporaryDirectory(prefix="aas-pytest-data-root-")
os.environ.setdefault("AAS_DATA_ROOT", os.path.realpath(_PYTEST_DATA_ROOT.name))

from aegis_alpha.data import canonical_generation_schema  # noqa: E402, F401
from aegis_alpha.metadata.schema import metadata  # noqa: E402

_DATABASE_PREFIX = "aas_owned_"
_DATABASE_SUFFIX = "_test"

# Every PostgreSQL-backed test reaches the server through the ``test_database``
# session fixture (directly or via ``postgres_url`` / ``alembic_config`` /
# ``postgres_engine`` / ``clean_postgres``).  Tagging on that fixture is the
# mechanism that routes PostgreSQL tests into ``verify-lane-database``;
# a test that opens PostgreSQL without the fixture must carry
# ``@pytest.mark.database`` itself.
_DATABASE_FIXTURE = "test_database"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if _DATABASE_FIXTURE in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.database)


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail every test that attempts to resolve or connect to a network host."""

    def denied(*_args: object, **_kwargs: object) -> None:
        pytest.fail("network access is forbidden in offline tests")

    monkeypatch.setattr(socket, "create_connection", denied)
    monkeypatch.setattr(socket, "getaddrinfo", denied)
    monkeypatch.setattr(socket.socket, "connect", denied)


@dataclass(frozen=True, slots=True)
class TestDatabase:
    url: URL
    name: str
    owner_token: str
    created_empty: bool


@pytest.fixture(scope="session")
def test_database() -> Iterator[TestDatabase]:
    value = os.environ.get("AAS_TEST_DATABASE_URL")
    if value is None:
        pytest.fail("metadata tests require AAS_TEST_DATABASE_URL")
    base_url = make_url(value)
    if base_url.drivername != "postgresql+psycopg":
        pytest.fail("AAS_TEST_DATABASE_URL must use postgresql+psycopg")
    if base_url.database is None or not base_url.database.endswith(_DATABASE_SUFFIX):
        pytest.fail("AAS_TEST_DATABASE_URL must identify a disposable *_test admin database")

    owner_token = uuid.uuid4().hex
    database_name = f"{_DATABASE_PREFIX}{owner_token}{_DATABASE_SUFFIX}"
    database_url = base_url.set(database=database_name)
    admin_engine = create_engine(
        base_url,
        isolation_level="AUTOCOMMIT",
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )
    database_engine: Engine | None = None
    created_by_fixture = False
    try:
        with admin_engine.connect() as connection:
            exists = connection.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": database_name},
            )
            assert exists is None
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            created_by_fixture = True
            connection.exec_driver_sql(
                f"COMMENT ON DATABASE \"{database_name}\" IS '{owner_token}'"
            )

        database_engine = create_engine(
            database_url,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 5},
        )
        with database_engine.connect() as connection:
            assert connection.scalar(text("SELECT current_database()")) == database_name
            server_version = connection.scalar(text("SHOW server_version"))
            assert isinstance(server_version, str)
            assert server_version.startswith("18.4")
            observed_token = connection.scalar(
                text(
                    "SELECT shobj_description(oid, 'pg_database') "
                    "FROM pg_database WHERE datname = current_database()"
                )
            )
            assert observed_token == owner_token
        assert inspect(database_engine).get_table_names() == []
        yield TestDatabase(database_url, database_name, owner_token, created_empty=True)
    finally:
        if database_engine is not None:
            database_engine.dispose()
        with admin_engine.connect() as connection:
            observed_token = connection.scalar(
                text(
                    "SELECT shobj_description(oid, 'pg_database') "
                    "FROM pg_database WHERE datname = :name"
                ),
                {"name": database_name},
            )
            safe_generated_name = database_name.startswith(
                _DATABASE_PREFIX
            ) and database_name.endswith(_DATABASE_SUFFIX)
            if safe_generated_name and (observed_token == owner_token or created_by_fixture):
                connection.execute(
                    text(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = :name AND pid <> pg_backend_pid()"
                    ),
                    {"name": database_name},
                )
                connection.exec_driver_sql(f'DROP DATABASE "{database_name}"')
        admin_engine.dispose()


@pytest.fixture(scope="session")
def postgres_url(test_database: TestDatabase) -> str:
    return test_database.url.render_as_string(hide_password=False)


@pytest.fixture(scope="session")
def alembic_config(postgres_url: str, test_database: TestDatabase) -> Config:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url.replace("%", "%%"))
    config.attributes["aas_test_database_name"] = test_database.name
    config.attributes["aas_test_owner_token"] = test_database.owner_token
    return config


@pytest.fixture(scope="session")
def postgres_engine(postgres_url: str) -> Iterator[Engine]:
    engine = create_engine(postgres_url, pool_pre_ping=True, connect_args={"connect_timeout": 5})
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def clean_postgres(
    postgres_engine: Engine,
    alembic_config: Config,
) -> Iterator[Engine]:
    command.upgrade(alembic_config, "head")
    with postgres_engine.begin() as connection:
        inspector = inspect(connection)
        for table in reversed(metadata.sorted_tables):
            if inspector.has_table(table.name, schema=table.schema):
                connection.execute(table.delete())
    yield postgres_engine
    with postgres_engine.begin() as connection:
        inspector = inspect(connection)
        for table in reversed(metadata.sorted_tables):
            if inspector.has_table(table.name, schema=table.schema):
                connection.execute(table.delete())
