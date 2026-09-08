"""SEC fixture execution through the installer's actual limited runtime role."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from psycopg import sql
from sqlalchemy import create_engine, make_url, text

from aegis_alpha.collection.records import RunEventType
from aegis_alpha.metadata.runtime_install import InstallRequest, install_runtime
from tests.data.test_sec_runtime import _config, _no_http, _run


def test_installed_limited_role_collects_and_recovers(postgres_url: str, tmp_path: Path) -> None:
    suffix = uuid4().hex[:12]
    database, role = "sec_fixture_" + suffix, "sec_role_" + suffix
    password = "synthetic-sec-runtime-password-only"  # noqa: S105 - isolated fixture credential
    request = InstallRequest(database, role, password)
    admin = create_engine(postgres_url, isolation_level="AUTOCOMMIT")
    installed: dict[str, object] | None = None
    try:
        installed = install_runtime(postgres_url, request, Path(__file__).resolve().parents[2])
        runtime = create_engine(
            make_url(postgres_url).set(database=database, username=role, password=password)
        )
        try:
            with runtime.connect() as connection:
                assert (
                    connection.scalar(
                        text("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")
                    )
                    is False
                )
                assert (
                    connection.scalar(
                        text(
                            "SELECT has_table_privilege(current_user,"
                            "'collection_usage_records','UPDATE')"
                        )
                    )
                    is False
                )
            config = _config(tmp_path)
            first = _run(runtime, config)
            assert first.terminal_event is RunEventType.RUN_SUCCEEDED
            assert _run(runtime, config, transport=_no_http) == first
        finally:
            runtime.dispose()
    finally:
        if installed is not None:
            with admin.connect() as connection:
                marker = connection.scalar(
                    text(
                        "SELECT shobj_description(oid,'pg_database') "
                        "FROM pg_database WHERE datname=:name"
                    ),
                    {"name": database},
                )
                assert marker == "aas-runtime-install/v1:" + str(installed["schema_head"])
                connection.exec_driver_sql(
                    sql.SQL("DROP DATABASE {}").format(sql.Identifier(database)).as_string()
                )
                connection.exec_driver_sql(
                    sql.SQL("DROP ROLE {}").format(sql.Identifier(role)).as_string()
                )
        admin.dispose()
