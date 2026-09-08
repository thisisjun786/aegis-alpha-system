from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from psycopg import sql
from sqlalchemy import Engine, create_engine, make_url, text
from sqlalchemy.exc import DBAPIError

from aegis_alpha.collection.records import CollectionRun, CollectionUsageRecord
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.usage_checkpoint_crypto import Ed25519PublicKeyring
from aegis_alpha.metadata.registry import MetadataRegistry
from aegis_alpha.metadata.runtime_install import (
    InstallRequest,
    RuntimeInstallError,
    _config,
    install_runtime,
    runtime_engine,
)
from tests.collection.test_plans import _plan
from tests.collection.test_usage_checkpoint_registry import _leaf, _signed
from tests.metadata.test_registry import _source_registration

_ROOT = Path(__file__).resolve().parents[2]
_PASSWORD = "isolated-runtime-test-password-value"  # noqa: S105 -- synthetic test-only credential


def test_install_clone_and_runtime_permissions(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = uuid4().hex[:12]
    database, role = "install_probe_" + token, "role_probe_" + token
    request = InstallRequest(database, role, _PASSWORD)
    admin = create_engine(postgres_url, isolation_level="AUTOCOMMIT")
    monkeypatch.setenv("AAS_MIGRATION_DATABASE_NAME", "unrelated_database")
    monkeypatch.setenv("AAS_MIGRATION_OWNER_TOKEN", "unrelated_token")
    try:
        assert install_runtime(postgres_url, request, _ROOT)["created"] is True
        assert install_runtime(postgres_url, request, _ROOT)["created"] is False
        runtime = create_engine(
            make_url(postgres_url).set(database=database, username=role, password=_PASSWORD)
        )
        try:
            _exercise_runtime_writers(runtime)
            with runtime.connect() as connection:
                assert (
                    connection.scalar(
                        text("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")
                    )
                    is False
                )
                assert (
                    connection.scalar(
                        text("SELECT has_table_privilege(current_user,'source_snapshots','INSERT')")
                    )
                    is True
                )
                assert (
                    connection.scalar(
                        text("SELECT has_table_privilege(current_user,'alembic_version','INSERT')")
                    )
                    is False
                )
                assert (
                    connection.scalar(
                        text(
                            "SELECT has_table_privilege(current_user,'engine.engine_runs','INSERT')"
                        )
                    )
                    is False
                )
                assert (
                    connection.scalar(
                        text(
                            "SELECT has_column_privilege(current_user,"
                            "'collection_usage_records','quantity','UPDATE')"
                        )
                    )
                    is False
                )
                assert connection.scalar(text("SELECT count(*) FROM engine.data_adoptions")) == 0
                assert (
                    connection.scalar(
                        text(
                            "SELECT has_function_privilege(current_user,"
                            "'engine.settle_fred_budget(text,text,bigint,bigint,"
                            "timestamp with time zone)',"
                            "'EXECUTE')"
                        )
                    )
                    is True
                )
            _refuse_stale_grants(postgres_url, request, runtime)
        finally:
            runtime.dispose()
    finally:
        # Names were generated in this fixture; this server contains synthetic test databases only.
        with admin.connect() as connection:
            connection.exec_driver_sql(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database)).as_string()
            )
            connection.exec_driver_sql(
                sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)).as_string()
            )
        admin.dispose()


def test_install_refuses_existing_foreign_database(postgres_url: str) -> None:
    database = "foreign_probe_" + uuid4().hex[:12]
    admin = create_engine(postgres_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as connection:
        connection.exec_driver_sql(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)).as_string()
        )
    foreign = create_engine(make_url(postgres_url).set(database=database))
    try:
        with foreign.begin() as connection:
            connection.execute(text("CREATE TABLE preserved (id integer PRIMARY KEY)"))
            connection.execute(text("INSERT INTO preserved VALUES (7)"))
        with pytest.raises(RuntimeInstallError, match="already exists"):
            install_runtime(postgres_url, InstallRequest(database, "unused_role", _PASSWORD), _ROOT)
        with foreign.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM preserved")) == 1
    finally:
        foreign.dispose()
        with admin.connect() as connection:
            connection.exec_driver_sql(
                sql.SQL("DROP DATABASE {}").format(sql.Identifier(database)).as_string()
            )
        admin.dispose()


@pytest.mark.parametrize("name", ["postgres", "template0", "ordinary_test", "unsafe;drop"])
def test_reserved_or_unsafe_database_names_are_rejected(name: str) -> None:
    with pytest.raises(ValueError, match=r"database|identifier"):
        InstallRequest(name, "runtime_role", _PASSWORD)


def test_password_is_not_in_request_representation() -> None:
    assert _PASSWORD not in repr(InstallRequest("application", "runtime_role", _PASSWORD))


def test_connection_query_cannot_override_target_identity() -> None:
    with pytest.raises(RuntimeInstallError, match="query options"):
        runtime_engine("postgresql+psycopg://test:fixture@127.0.0.1/aas?dbname=other")


def _exercise_runtime_writers(engine: Engine) -> None:
    registry = CollectionRegistry(engine)
    plan = _plan()
    registry.register_plan(plan)
    registry.register_plan(plan)
    moment = datetime.now(UTC)
    registry.start_run(CollectionRun("runtime-probe", plan.plan_id, moment))
    usage = CollectionUsageRecord(
        "runtime-probe", 1, "calls_attempted", Decimal(1), "call", moment, {}
    )
    registry.record_usage(usage)
    registry.record_usage(usage)
    private_key = Ed25519PrivateKey.generate()
    leaves = (_leaf(),)
    signed = _signed(private_key, leaves)
    keyring = Ed25519PublicKeyring(
        {
            (
                signed.checkpoint.authority_id,
                signed.checkpoint.key_id,
            ): private_key.public_key().public_bytes_raw()
        }
    )
    registry.register_usage_checkpoint(signed, leaves, keyring)
    registry.register_usage_checkpoint(signed, leaves, keyring)
    metadata = MetadataRegistry(engine)
    metadata.register_source_snapshot(_source_registration())
    metadata.register_source_snapshot(_source_registration())
    for query in (
        "UPDATE public.collection_run_plans SET plan_id=plan_id",
        "UPDATE public.source_snapshots SET snapshot_id=snapshot_id",
        "UPDATE public.collection_usage_records SET run_id=run_id",
        "UPDATE public.collection_usage_records SET quantity=0",
        "UPDATE public.collection_usage_checkpoints SET checkpoint_id=checkpoint_id",
        "DELETE FROM public.collection_usage_records",
    ):
        with engine.connect() as connection, pytest.raises(DBAPIError):
            connection.execute(text(query))
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT quantity FROM public.collection_usage_records")) == 1


def test_supplied_migration_connection_preserves_outer_rollback(clean_postgres: Engine) -> None:
    with clean_postgres.connect() as connection:
        before = connection.scalar(text("SELECT version_num FROM public.alembic_version"))
    with clean_postgres.connect() as connection:
        transaction = connection.begin()
        try:
            config = _config(_ROOT, clean_postgres.url)
            config.attributes["connection"] = connection
            command.downgrade(config, "20260905_0011")
            assert (
                connection.scalar(text("SELECT version_num FROM public.alembic_version"))
                == "20260905_0011"
            )
        finally:
            transaction.rollback()
    with clean_postgres.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM public.alembic_version")) == before
        assert (
            connection.scalar(
                text("SELECT to_regprocedure('engine.reject_registry_key_update()') IS NOT NULL")
            )
            is True
        )


def _refuse_stale_grants(admin_url: str, request: InstallRequest, runtime: Engine) -> None:
    owner = create_engine(make_url(admin_url).set(database=request.database))
    role = sql.Identifier(request.runtime_role)
    try:
        with owner.begin() as connection:
            # Synthetic installation only: reproduce stale grants after migration rollback.
            granted = connection.execute(
                text("""
                SELECT c.relname,a.attname FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
                JOIN pg_namespace n ON n.oid=c.relnamespace
                CROSS JOIN LATERAL aclexplode(a.attacl) acl
                JOIN pg_roles r ON r.oid=acl.grantee
                WHERE n.nspname='public' AND r.rolname=:role AND acl.privilege_type='UPDATE'
            """),
                {"role": request.runtime_role},
            ).all()
            for table, column in granted:
                connection.exec_driver_sql(
                    sql.SQL("REVOKE UPDATE ({}) ON public.{} FROM {}")
                    .format(sql.Identifier(column), sql.Identifier(table), role)
                    .as_string()
                )
            connection.execute(
                text(
                    "ALTER TABLE public.collection_run_plans "
                    "DISABLE TRIGGER reject_registry_key_update"
                )
            )
        with pytest.raises(RuntimeInstallError, match="guard is absent"):
            install_runtime(admin_url, request, _ROOT)
        _assert_no_plan_update(runtime)
        with owner.begin() as connection:
            connection.execute(
                text(
                    "ALTER TABLE public.collection_run_plans "
                    "ENABLE TRIGGER reject_registry_key_update"
                )
            )
            config = _config(_ROOT, owner.url)
            config.attributes["connection"] = connection
            command.downgrade(config, "20260905_0011")
        with pytest.raises(RuntimeInstallError, match="schema head differs"):
            install_runtime(admin_url, request, _ROOT)
        _assert_no_plan_update(runtime)
    finally:
        owner.dispose()


def _assert_no_plan_update(engine: Engine) -> None:
    with engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    "SELECT has_column_privilege(current_user,"
                    "'collection_run_plans','plan_id','UPDATE')"
                )
            )
            is False
        )
