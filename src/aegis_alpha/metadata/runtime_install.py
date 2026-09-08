from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from psycopg import Connection as PsycopgConnection
from psycopg import Error as PsycopgError
from psycopg import sql
from sqlalchemy import Connection, Engine, create_engine, make_url, text
from sqlalchemy.exc import SQLAlchemyError

if TYPE_CHECKING:
    from sqlalchemy.engine import URL

_NAME = re.compile(r"[a-z][a-z0-9_]{0,47}")
_INSTALL_MARKER = "aas-runtime-install/v1:"
_ROLE_MARKER = "aas-runtime-role/v1:"


class RuntimeInstallError(RuntimeError):
    """An installation cannot prove ownership or compatible state."""


@dataclass(frozen=True, slots=True)
class InstallRequest:
    database: str
    runtime_role: str
    runtime_password: str = field(repr=False)

    def __post_init__(self) -> None:
        for name in (self.database, self.runtime_role):
            if not isinstance(name, str) or _NAME.fullmatch(name) is None:
                raise ValueError("database and role need safe lowercase identifiers")
        if self.database in {"postgres", "template0", "template1"} or self.database.endswith(
            "_test"
        ):
            raise ValueError("the application needs a normal non-system database name")
        if not isinstance(self.runtime_password, str) or len(self.runtime_password) < 24:  # noqa: PLR2004 -- minimum generated-secret length
            raise ValueError("runtime password needs at least 24 characters")
        if any(character in self.runtime_password for character in "\x00\r\n"):
            raise ValueError("runtime password must be a single line")


def parse_runtime_url(value: str) -> URL:
    try:
        url = make_url(value)
    except (SQLAlchemyError, ValueError, TypeError):
        raise RuntimeInstallError("invalid database connection configuration") from None
    if url.drivername != "postgresql+psycopg":
        raise RuntimeInstallError("database connection must use postgresql+psycopg")
    if url.query:
        raise RuntimeInstallError("database URL query options are not supported by this profile")
    return url


def runtime_engine(value: str, *, read_only: bool = False) -> Engine:
    arguments: dict[str, object] = {"connect_timeout": 5}
    if read_only:
        arguments["options"] = "-c default_transaction_read_only=on"
    return create_engine(
        parse_runtime_url(value), pool_pre_ping=True, hide_parameters=True, connect_args=arguments
    )


def _config(project_root: Path, url: URL) -> Config:
    root = project_root.resolve(strict=True)
    config = Config(str(root / "alembic.ini"), stdout=sys.stderr)
    config.set_main_option("script_location", str(root / "migrations"))
    config.set_main_option(
        "sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%")
    )
    return config


def _head(config: Config) -> str:
    head = ScriptDirectory.from_config(config).get_current_head()
    if head is None:
        raise RuntimeInstallError("migration graph has no single head")
    return head


def _raw(engine: Engine, statement: sql.Composable) -> None:
    with engine.connect() as connection:
        driver = connection.connection.driver_connection
        if not isinstance(driver, PsycopgConnection):
            raise RuntimeInstallError("psycopg driver required")
        try:
            with driver.cursor() as cursor:
                cursor.execute(statement)
        except PsycopgError:
            raise RuntimeInstallError("database DDL operation failed") from None


def _comment(engine: Engine, name: str) -> str | None:
    with engine.connect() as connection:
        return connection.scalar(
            text(
                "SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname=:name"
            ),
            {"name": name},
        )


def _exists(engine: Engine, name: str) -> bool:
    with engine.connect() as connection:
        return bool(
            connection.scalar(
                text("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=:name)"),
                {"name": name},
            )
        )


def _assert_empty(engine: Engine, *, migrated: bool) -> None:
    with engine.connect() as connection:
        if connection.scalar(text("SELECT current_database()")) != engine.url.database:
            raise RuntimeInstallError("database identity differs from the configured target")
        tables = connection.execute(
            text(
                "SELECT n.nspname, c.relname FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE c.relkind IN ('r','p') AND n.nspname NOT LIKE 'pg_%' "
                "AND n.nspname<>'information_schema'"
            )
        ).all()
        if tables and not migrated:
            raise RuntimeInstallError("schema template is not empty")
        for schema, table in tables:
            if schema == "public" and table == "alembic_version" and migrated:
                continue
            statement = sql.SQL("SELECT count(*) FROM {}.{}").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
            if connection.scalar(text(statement.as_string())):
                raise RuntimeInstallError("schema template contains business data")


def _build_template(admin: Engine, url: URL, root: Path) -> tuple[str, str]:
    token = uuid4().hex
    name = f"aas_owned_{token}_test"
    if _exists(admin, name):
        raise RuntimeInstallError("generated template already exists")
    _raw(admin, sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
    _raw(
        admin,
        sql.SQL("COMMENT ON DATABASE {} IS {}").format(sql.Identifier(name), sql.Literal(token)),
    )
    template_url = url.set(database=name)
    template = runtime_engine(template_url.render_as_string(hide_password=False))
    try:
        _assert_empty(template, migrated=False)
        config = _config(root, template_url)
        config.attributes["aas_test_database_name"] = name
        config.attributes["aas_test_owner_token"] = token
        command.upgrade(config, "head")
        command.check(config)
        _assert_empty(template, migrated=True)
    finally:
        template.dispose()
    return name, token


def _provision_role(admin: Engine, request: InstallRequest) -> None:
    marker = _ROLE_MARKER + request.database
    with admin.connect() as connection:
        existing = connection.execute(
            text(
                "SELECT rolsuper,rolcreatedb,rolcreaterole,shobj_description(oid,'pg_authid') "
                "FROM pg_roles WHERE rolname=:name"
            ),
            {"name": request.runtime_role},
        ).one_or_none()
    if existing is not None:
        if any(existing[:3]) or existing[3] != marker:
            raise RuntimeInstallError("runtime role already exists outside this installation")
        return
    _raw(
        admin,
        sql.SQL(
            "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD {}"
        ).format(sql.Identifier(request.runtime_role), sql.Literal(request.runtime_password)),
    )
    _raw(
        admin,
        sql.SQL("COMMENT ON ROLE {} IS {}").format(
            sql.Identifier(request.runtime_role), sql.Literal(marker)
        ),
    )


def _grant_runtime(url: URL, request: InstallRequest, expected_head: str) -> None:
    role = sql.Identifier(request.runtime_role)
    target = runtime_engine(
        url.set(database=request.database).render_as_string(hide_password=False)
    )
    try:
        with target.begin() as connection:
            # Hold every catalog relation against trigger/schema removal until
            # all validated grants commit. A stale DB comment never authorizes grants.
            tables = (
                connection.execute(
                    text(
                        "SELECT c.relname FROM pg_catalog.pg_class c "
                        "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                        "WHERE n.nspname='public' AND c.relkind IN ('r','p') ORDER BY c.relname"
                    )
                )
                .scalars()
                .all()
            )
            if not tables:
                raise RuntimeInstallError("runtime schema is absent")
            connection.exec_driver_sql(
                sql.SQL("LOCK TABLE {} IN SHARE MODE")
                .format(sql.SQL(",").join(sql.Identifier("public", table) for table in tables))
                .as_string()
            )
            if (
                connection.scalar(text("SELECT version_num FROM public.alembic_version"))
                != expected_head
            ):
                raise RuntimeInstallError("runtime schema head differs; no privileges changed")
            _grant_locked(connection, request, role)
    finally:
        target.dispose()


def _grant_locked(connection: Connection, request: InstallRequest, role: sql.Identifier) -> None:
    lock_columns = {
        "source_snapshots": "snapshot_id",
        "dataset_versions": "dataset_id",
        "collection_run_plans": "plan_id",
        "collection_runs": "run_id",
        "collection_run_events": "run_id",
        "collection_run_receipts": "run_id",
        "collection_watermarks": "provider",
        "collection_usage_records": "run_id",
        "collection_usage_checkpoints": "checkpoint_id",
    }
    for table, column in lock_columns.items():
        checkpoint = table == "collection_usage_checkpoints"
        function = (
            "public.collection_refuse_usage_checkpoint_mutation()"
            if checkpoint
            else "engine.reject_registry_key_update()"
        )
        trigger = (
            "collection_usage_checkpoints_append_only"
            if checkpoint
            else "reject_registry_key_update"
        )
        guarded = connection.scalar(
            text(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t "
                "JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid "
                "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                "JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attname=:column "
                "WHERE n.nspname='public' AND c.relname=:table "
                "AND t.tgname=:trigger AND t.tgenabled IN ('O','A') AND t.tgqual IS NULL "
                "AND t.tgfoid=to_regprocedure(:function) AND t.tgtype=:type "
                "AND (CASE WHEN :checkpoint THEN cardinality(t.tgattr)=0 "
                "ELSE cardinality(t.tgattr)=1 AND a.attnum=ANY(t.tgattr) END))"
            ),
            {
                "table": table,
                "column": column,
                "function": function,
                "trigger": trigger,
                "type": 27 if checkpoint else 19,
                "checkpoint": checkpoint,
            },
        )
        if not guarded:
            raise RuntimeInstallError("runtime lock-column guard is absent; no privileges changed")
    statements = [
        sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(request.database)),
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(request.database), role
        ),
        sql.SQL("REVOKE CREATE ON SCHEMA public FROM PUBLIC"),
        sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(role),
        sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}").format(role),
        sql.SQL("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {}").format(role),
        sql.SQL(
            "REVOKE UPDATE (quantity,evidence_json) ON public.collection_usage_records FROM {}"
        ).format(role),
        sql.SQL("GRANT USAGE ON SCHEMA engine TO {}").format(role),
        sql.SQL("GRANT SELECT ON engine.data_adoptions TO {}").format(role),
        sql.SQL(
            "GRANT EXECUTE ON FUNCTION engine.settle_fred_budget"
            "(text,text,bigint,bigint,timestamp with time zone) TO {}"
        ).format(role),
    ]
    writable = (
        "source_snapshots",
        "source_snapshot_files",
        "dataset_versions",
        "dataset_sources",
        "dataset_input_files",
        "dataset_artifacts",
        "quality_results",
        "collection_run_plans",
        "collection_runs",
        "collection_run_events",
        "collection_run_receipts",
        "collection_watermarks",
        "collection_usage_records",
        "collection_usage_checkpoints",
    )
    statements.extend(
        sql.SQL("GRANT INSERT ON public.{} TO {}").format(sql.Identifier(table), role)
        for table in writable
    )
    statements.extend(
        sql.SQL("GRANT UPDATE ({}) ON public.{} TO {}").format(
            sql.Identifier(column), sql.Identifier(table), role
        )
        for table, column in lock_columns.items()
    )
    for statement in statements:
        connection.exec_driver_sql(statement.as_string())


def _inspect_installed(url: URL, request: InstallRequest, expected_head: str) -> None:
    engine = runtime_engine(
        url.set(
            database=request.database,
            username=request.runtime_role,
            password=request.runtime_password,
        ).render_as_string(hide_password=False)
    )
    try:
        with engine.connect() as connection:
            head = connection.scalar(text("SELECT version_num FROM public.alembic_version"))
            superuser = connection.scalar(
                text("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")
            )
            actual_database = connection.scalar(text("SELECT current_database()"))
            if (
                head != expected_head
                or superuser is not False
                or actual_database != request.database
            ):
                raise RuntimeInstallError("runtime role or schema verification failed")
    finally:
        engine.dispose()


def install_runtime(
    admin_url: str, request: InstallRequest, project_root: Path
) -> dict[str, object]:
    """Create a normal app DB from an actually migrated empty template; never overwrite data."""
    url = parse_runtime_url(admin_url)
    head = _head(_config(project_root, url))
    admin = create_engine(
        url, isolation_level="AUTOCOMMIT", hide_parameters=True, connect_args={"connect_timeout": 5}
    )
    try:
        return _install_locked(admin, url, request, project_root, head)
    except SQLAlchemyError:
        raise RuntimeInstallError(
            "database installation failed; existing data was not overwritten"
        ) from None
    finally:
        admin.dispose()


def _install_locked(
    admin: Engine, url: URL, request: InstallRequest, root: Path, head: str
) -> dict[str, object]:
    with admin.connect() as lock:
        key = "aas-runtime-install:" + request.database
        if not lock.scalar(
            text("SELECT pg_try_advisory_lock(hashtextextended(:key,0))"), {"key": key}
        ):
            raise RuntimeInstallError("another installation is active")
        try:
            if _exists(admin, request.database):
                if _comment(admin, request.database) != _INSTALL_MARKER + head:
                    raise RuntimeInstallError(
                        "target database already exists without a matching installation identity"
                    )
                _provision_role(admin, request)
                _grant_runtime(url, request, head)
                _inspect_installed(url, request, head)
                return {"database": request.database, "schema_head": head, "created": False}
            template, token = _build_template(admin, url, root)
            _provision_role(admin, request)
            _raw(
                admin,
                sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                    sql.Identifier(request.database), sql.Identifier(template)
                ),
            )
            _raw(
                admin,
                sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                    sql.Identifier(request.database), sql.Literal(_INSTALL_MARKER + head)
                ),
            )
            _grant_runtime(url, request, head)
            _inspect_installed(url, request, head)
            if _comment(admin, template) != token:
                raise RuntimeInstallError("template ownership changed; preserved for inspection")
            _raw(admin, sql.SQL("DROP DATABASE {}").format(sql.Identifier(template)))
            return {"database": request.database, "schema_head": head, "created": True}
        finally:
            lock.execute(text("SELECT pg_advisory_unlock(hashtextextended(:key,0))"), {"key": key})
