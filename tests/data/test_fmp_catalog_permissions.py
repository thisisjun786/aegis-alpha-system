"""FMP catalog commit and zero-HTTP recovery through the installed runtime role."""

from __future__ import annotations

# ruff: noqa: F811 -- imported pytest fixtures are consumed by their parameter names
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import sql
from sqlalchemy import Engine, create_engine, make_url, text
from sqlalchemy.exc import DBAPIError
from test_fmp_catalog import catalog_counts, lifecycle
from test_fmp_daily_budget import SUCCESS_CALLS, DailyHarness, daily_harness  # noqa: F401

from aegis_alpha.data import fmp_collector_command, fmp_daily_cli, fmp_deferred_transport
from aegis_alpha.data.fmp_catalog import (
    recover_completed_collections,
    register_completed_collection,
)
from aegis_alpha.metadata.runtime_install import InstallRequest, install_runtime


@pytest.fixture
def limited_runtime(postgres_url: str) -> Iterator[Engine]:
    token = uuid4().hex[:12]
    database, role = "fmp_catalog_fixture_" + token, "fmp_catalog_role_" + token
    password = "synthetic-fmp-catalog-runtime-only"  # noqa: S105 -- disposable fixture credential
    admin = create_engine(postgres_url, isolation_level="AUTOCOMMIT", hide_parameters=True)
    installed: dict[str, object] | None = None
    runtime: Engine | None = None
    try:
        installed = install_runtime(
            postgres_url,
            InstallRequest(database, role, password),
            Path(__file__).resolve().parents[2],
        )
        runtime = create_engine(
            make_url(postgres_url).set(
                database=database,
                username=role,
                password=password,
            ),
            hide_parameters=True,
        )
        yield runtime
    finally:
        if runtime is not None:
            runtime.dispose()
        if installed is not None:
            with admin.connect() as connection:
                marker = connection.scalar(
                    text(
                        "SELECT shobj_description(oid,'pg_database') "
                        "FROM pg_database WHERE datname=:name"
                    ),
                    {"name": database},
                )
                role_marker = connection.scalar(
                    text(
                        "SELECT shobj_description(oid,'pg_authid') "
                        "FROM pg_roles WHERE rolname=:name"
                    ),
                    {"name": role},
                )
                assert marker == "aas-runtime-install/v1:" + str(installed["schema_head"])
                assert role_marker == "aas-runtime-role/v1:" + database
                connection.exec_driver_sql(
                    sql.SQL("DROP DATABASE {}").format(sql.Identifier(database)).as_string()
                )
                connection.exec_driver_sql(
                    sql.SQL("DROP ROLE {}").format(sql.Identifier(role)).as_string()
                )
        admin.dispose()


def _assert_restricted(engine: Engine) -> None:
    with engine.connect() as connection:
        powers = connection.execute(
            text(
                "SELECT rolsuper,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=current_user"
            )
        ).one()
        assert tuple(powers) == (False, False, False)
        assert (
            connection.scalar(text("SELECT has_schema_privilege(current_user,'public','CREATE')"))
            is False
        )
        for column in ("quantity", "evidence_json"):
            assert (
                connection.scalar(
                    text(
                        "SELECT has_column_privilege(current_user,"
                        "'collection_usage_records',:column,'UPDATE')"
                    ),
                    {"column": column},
                )
                is False
            )


@pytest.mark.parametrize("catalog_outage", [False, True])
def test_limited_role_collects_registers_and_recovers_without_extra_privileges(
    daily_harness: DailyHarness,
    limited_runtime: Engine,
    monkeypatch: pytest.MonkeyPatch,
    *,
    catalog_outage: bool,
) -> None:
    daily_harness.engine = limited_runtime
    _assert_restricted(limited_runtime)

    def unavailable(*_args: object) -> None:
        raise RuntimeError("synthetic catalog interruption")

    with monkeypatch.context() as patch:
        if catalog_outage:
            patch.setattr(fmp_collector_command, "register_completed_collection", unavailable)
        code, report = daily_harness.run(SUCCESS_CALLS)
    assert len(daily_harness.transport.calls) == SUCCESS_CALLS
    if catalog_outage:
        assert code == 1, report
        assert report["collection_status"] == "collection_succeeded"
        assert report["catalog_status"] == "catalog_pending"
        assert catalog_counts(limited_runtime) == (0, 0, 0, 0, 0)
        run_id = str(report["run_id"])
    else:
        assert code == 0, report
        run_id = str(report["collection_run_id"])
    before = lifecycle(limited_runtime)

    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("limited-role catalog recovery must not use authority or transport")

    with monkeypatch.context() as patch:
        patch.delenv("FMP_API_KEY", raising=False)
        patch.delenv("AAS_FMP_OWNER_APPROVAL_AUTHORITY_PATH", raising=False)
        patch.setattr(fmp_daily_cli, "_verified_authority", forbidden)
        patch.setattr(fmp_daily_cli, "authorize_recurring_operation", forbidden)
        patch.setattr(fmp_deferred_transport, "make_fmp_https_transport", forbidden)
        recovered = recover_completed_collections(
            limited_runtime, daily_harness.root / "raw", daily_harness.root / "normalized"
        )
        assert recovered["status"] == "catalog_complete", recovered
        assert recovered["recovered_run_ids"] == ([run_id] if catalog_outage else [])
        assert recovered["provider_calls"] == 0
        replay = register_completed_collection(
            limited_runtime, daily_harness.root / "raw", daily_harness.root / "normalized", run_id
        )
        assert replay["status"] == "catalog_complete"
    assert len(daily_harness.transport.calls) == SUCCESS_CALLS
    assert lifecycle(limited_runtime) == before
    counts = catalog_counts(limited_runtime)
    assert counts[:3] == (1, 1, 4)
    assert counts[-1] == 1
    with pytest.raises(DBAPIError) as rejected, limited_runtime.begin() as connection:
        connection.execute(text("UPDATE collection_usage_records SET quantity=quantity"))
    assert getattr(rejected.value.orig, "sqlstate", None) == "42501"
    assert lifecycle(limited_runtime) == before
