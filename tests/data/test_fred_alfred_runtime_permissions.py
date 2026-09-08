"""One complete FRED collection through the installed limited runtime role."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from fred_alfred_collector_support import CREDENTIAL
from psycopg import sql
from sqlalchemy import create_engine, make_url, select

from aegis_alpha.collection.records import RunEventType
from aegis_alpha.data.fred_alfred_runtime import run_runtime
from aegis_alpha.metadata.runtime_install import InstallRequest, install_runtime
from aegis_alpha.metadata.schema import dataset_versions
from tests.data.test_fred_alfred_runtime import _setup

if TYPE_CHECKING:
    import pytest

_PASSWORD = "synthetic-fred-installation-password"  # noqa: S105 -- disposable fixture only
_ROOT = Path(__file__).resolve().parents[2]


def test_installed_limited_role_collects_sources_and_catalog(
    tmp_path: Path, postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = uuid4().hex
    database, role = "fred_install_" + token, "fred_role_" + token
    request = InstallRequest(database, role, _PASSWORD)
    admin = create_engine(postgres_url, isolation_level="AUTOCOMMIT")
    installed = False
    try:
        install_runtime(postgres_url, request, _ROOT)
        installed = True
        runtime = create_engine(
            make_url(postgres_url).set(database=database, username=role, password=_PASSWORD)
        )
        try:
            config, authority, clock, opener = _setup(tmp_path, runtime, monkeypatch)
            outcome = run_runtime(
                config=config,
                engine=runtime,
                authority=authority,
                credential=CREDENTIAL,
                clock=clock.now,
                monotonic=clock.time,
                sleep=clock.sleep,
            )
            assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
            assert outcome.calls_attempted == len(opener.urls)
            with runtime.connect() as connection:
                row = connection.execute(select(dataset_versions)).one()
                assert row.dataset_id == "fred_alfred_observations"
                assert row.row_count == len(outcome.rows)
                assert row.backtest_eligible is False
        finally:
            runtime.dispose()
    finally:
        if installed:
            with admin.connect() as connection:
                connection.exec_driver_sql(
                    sql.SQL("DROP DATABASE {}").format(sql.Identifier(database)).as_string()
                )
                connection.exec_driver_sql(
                    sql.SQL("DROP ROLE {}").format(sql.Identifier(role)).as_string()
                )
        admin.dispose()
