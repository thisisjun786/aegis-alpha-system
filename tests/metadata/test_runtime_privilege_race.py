"""Serialize an uncommitted installer grant against removal of its registry guard."""

from __future__ import annotations

import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Event
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from psycopg import sql
from sqlalchemy import Connection, Engine, event, inspect, text
from sqlalchemy.exc import IntegrityError

from aegis_alpha.collection.registry import CollectionRegistry
from tests.collection.test_plans import _plan

_PREVIOUS = "20260905_0011"
_LOCK_REVISION = "20260905_0012"
# Independent expected lock order, including the schema-version interlock first.
_DOWNGRADE_LOCK = (
    "LOCK TABLE public.alembic_version, public.collection_run_events, "
    "public.collection_run_plans, public.collection_run_receipts, public.collection_runs, "
    "public.collection_usage_records, public.collection_watermarks, public.dataset_versions, "
    "public.source_snapshots IN ACCESS EXCLUSIVE MODE"
)


@pytest.fixture
def lock_revision(clean_postgres: Engine, alembic_config: Config) -> Iterator[Engine]:
    # Isolate this revision's lock: a newer downgrade's version UPDATE must not
    # become an accidental interlock that hides a missing ACL-before-DDL guard.
    latest = ScriptDirectory.from_config(alembic_config).get_current_head()
    assert latest is not None
    command.downgrade(alembic_config, _LOCK_REVISION)
    try:
        yield clean_postgres
    finally:
        command.upgrade(alembic_config, latest)


@pytest.fixture
def limited_role(lock_revision: Engine) -> Iterator[str]:
    clean_postgres = lock_revision
    role = "aas_lock_race_" + uuid4().hex
    identifier = sql.Identifier(role)
    with clean_postgres.begin() as connection:
        connection.exec_driver_sql(
            sql.SQL("CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE")
            .format(identifier)
            .as_string()
        )
    try:
        yield role
    finally:
        with clean_postgres.begin() as connection:
            connection.exec_driver_sql(
                sql.SQL("REVOKE UPDATE (plan_id) ON public.collection_run_plans FROM {}")
                .format(identifier)
                .as_string()
            )
            connection.exec_driver_sql(sql.SQL("DROP ROLE {}").format(identifier).as_string())


def _downgrade(engine: Engine, config: Config, pids: Queue[int], acl_checked: Event) -> None:
    def record_acl_check(
        _connection: Connection, _cursor: object, statement: str, *_args: object
    ) -> None:
        if "pg_catalog.aclexplode" in statement:
            acl_checked.set()

    # Supply the actual migration connection, rather than sampling and returning
    # another connection to the pool before Alembic opens its transaction.
    with engine.begin() as connection:
        connection.execute(text("SET LOCAL statement_timeout = '12s'"))
        migration_config = Config(config.config_file_name)
        migration_config.attributes.update(config.attributes)
        migration_config.attributes["connection"] = connection
        event.listen(connection, "before_cursor_execute", record_acl_check)
        try:
            pids.put(connection.execute(text("SELECT pg_backend_pid()")).scalar_one())
            command.downgrade(migration_config, _PREVIOUS)
        finally:
            event.remove(connection, "before_cursor_execute", record_acl_check)


def _blocked_statement(engine: Engine, follower: int, holder: int) -> str:
    deadline = time.monotonic() + 5
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        while time.monotonic() < deadline:
            row = connection.execute(
                text(
                    "SELECT query, wait_event_type, pg_blocking_pids(pid) AS blockers "
                    "FROM pg_stat_activity WHERE pid = :pid"
                ),
                {"pid": follower},
            ).one()
            if row.wait_event_type == "Lock" and holder in row.blockers:
                return str(row.query)
            time.sleep(0.01)
    pytest.fail("downgrade did not block on the installer's actual backend")


def _assert_guard_and_head(engine: Engine, role: str, plan_id: str, expected_head: str) -> None:
    with engine.connect() as connection:
        assert (
            connection.scalar(text("SELECT version_num FROM public.alembic_version"))
            == expected_head
        )
        assert (
            connection.scalar(
                text(
                    "SELECT has_column_privilege(:role, 'public.collection_run_plans', "
                    "'plan_id', 'UPDATE')"
                ),
                {"role": role},
            )
            is True
        )
        assert (
            connection.scalar(
                text(
                    "SELECT tgenabled FROM pg_trigger "
                    "WHERE tgrelid='public.collection_run_plans'::regclass "
                    "AND tgname='reject_registry_key_update'"
                )
            )
            == "O"
        )
        assert (
            connection.scalar(
                text("SELECT plan_id FROM public.collection_run_plans WHERE plan_id=:id"),
                {"id": plan_id},
            )
            == plan_id
        )
    with (
        engine.begin() as connection,
        pytest.raises(IntegrityError, match="registry lock column is immutable"),
    ):
        connection.execute(
            text("UPDATE public.collection_run_plans SET plan_id=plan_id WHERE plan_id=:id"),
            {"id": plan_id},
        )


def test_downgrade_waits_for_uncommitted_grant_before_checking_acl(
    lock_revision: Engine, alembic_config: Config, limited_role: str
) -> None:
    clean_postgres = lock_revision
    expected_head = _LOCK_REVISION
    plan = _plan()
    CollectionRegistry(clean_postgres).register_plan(plan)
    pids: Queue[int] = Queue()
    acl_checked = Event()
    with ThreadPoolExecutor(max_workers=1) as executor:
        # Context exit commits the grant only after the downgrade's lock wait is
        # proven; failures roll it back and release the waiter before joining it.
        with clean_postgres.begin() as holder:
            assert (
                holder.scalar(text("SELECT version_num FROM public.alembic_version"))
                == expected_head
            )
            tables = sorted(inspect(holder).get_table_names(schema="public"))
            holder.exec_driver_sql(
                sql.SQL("LOCK TABLE {} IN SHARE MODE")
                .format(sql.SQL(", ").join(sql.Identifier("public", table) for table in tables))
                .as_string()
            )
            holder.exec_driver_sql(
                sql.SQL("GRANT UPDATE (plan_id) ON public.collection_run_plans TO {}")
                .format(sql.Identifier(limited_role))
                .as_string()
            )
            holder_pid = holder.execute(text("SELECT pg_backend_pid()")).scalar_one()
            future = executor.submit(_downgrade, clean_postgres, alembic_config, pids, acl_checked)
            statement = _blocked_statement(clean_postgres, pids.get(timeout=5), holder_pid)
            assert " ".join(statement.split()) == _DOWNGRADE_LOCK
            assert not acl_checked.is_set(), "ACL inspection must follow the conflicting lock"
            with clean_postgres.connect() as observer:
                assert (
                    observer.scalar(
                        text(
                            "SELECT has_column_privilege(:role, 'public.collection_run_plans', "
                            "'plan_id', 'UPDATE')"
                        ),
                        {"role": limited_role},
                    )
                    is False
                )
        with pytest.raises(RuntimeError, match="revoke runtime lock-column grants"):
            future.result(timeout=15)
    assert acl_checked.is_set()
    _assert_guard_and_head(clean_postgres, limited_role, plan.plan_id, expected_head)
