"""Real limited-role settlement and forged-binding refusals on synthetic PostgreSQL."""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from alembic import command
from fred_alfred_authority_support import make_standing_authority
from psycopg import sql
from sqlalchemy import Engine, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from aegis_alpha.collection.records import CollectionMode, CollectionRun
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import collection_usage_records
from aegis_alpha.data.fred_alfred_collector import build_run_plan
from aegis_alpha.data.fred_alfred_recurring_authority import verify_recurring_authority
from aegis_alpha.data.fred_alfred_usage_budget import (
    DailyUsageBudget,
    admit_requested_calls,
    load_daily_usage_budget,
)

if TYPE_CHECKING:
    from alembic.config import Config

_SIGNATURE = "engine.settle_fred_budget(text,text,bigint,bigint,timestamp with time zone)"
_CALL = text("SELECT engine.settle_fred_budget(:reservation, :run, :requested, :actual, :recorded)")


def _bound(engine: Engine, tmp_path: Path) -> tuple[DailyUsageBudget, str, datetime]:
    now = datetime.now(UTC)
    fixture = make_standing_authority(tmp_path)
    authority = verify_recurring_authority(
        fixture.payload, fixture.signature, fixture.owner_authority, now=now
    )
    registry = CollectionRegistry(engine)
    plan = build_run_plan(mode=CollectionMode.PROBE, series_ids=("T10Y2Y",), created_at_utc=now)
    registry.register_plan(plan)
    registry.start_run(CollectionRun("fred-bound-run", plan.plan_id, now))
    reservation = admit_requested_calls(
        engine=engine,
        authority=authority,
        requested_calls=5,
        now=now,
        actual_run_id="fred-bound-run",
    )
    return reservation, "fred-bound-run", now


def _arguments(bound: tuple[DailyUsageBudget, str, datetime]) -> dict[str, object]:
    reservation, run_id, now = bound
    return {
        "reservation": reservation.reservation_run_id,
        "run": run_id,
        "requested": 5,
        "actual": 3,
        "recorded": now,
    }


@pytest.fixture
def limited_role(clean_postgres: Engine) -> Iterator[str]:
    role = "fred_settlement_" + uuid4().hex
    identifier = sql.Identifier(role)
    with clean_postgres.begin() as connection:
        connection.exec_driver_sql(
            sql.SQL("CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE")
            .format(identifier)
            .as_string()
        )
        connection.exec_driver_sql(
            sql.SQL("GRANT USAGE ON SCHEMA engine TO {}").format(identifier).as_string()
        )
    try:
        yield role
    finally:
        with clean_postgres.begin() as connection:
            connection.exec_driver_sql(
                sql.SQL("REVOKE ALL ON SCHEMA engine FROM {}").format(identifier).as_string()
            )
            connection.exec_driver_sql(
                sql.SQL("REVOKE ALL ON FUNCTION " + _SIGNATURE + " FROM {}")
                .format(identifier)
                .as_string()
            )
            connection.exec_driver_sql(sql.SQL("DROP ROLE {}").format(identifier).as_string())


def test_limited_role_can_only_settle_bound_usage(
    tmp_path: Path, clean_postgres: Engine, limited_role: str
) -> None:
    bound = _bound(clean_postgres, tmp_path)
    arguments = _arguments(bound)
    with clean_postgres.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT has_function_privilege(:role, :signature, 'EXECUTE')"),
                {"role": limited_role, "signature": _SIGNATURE},
            )
            is False
        )
        connection.exec_driver_sql(
            sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(limited_role)).as_string()
        )
        with pytest.raises(DBAPIError, match="permission denied"):
            connection.execute(_CALL, arguments)
    with clean_postgres.begin() as connection:
        connection.exec_driver_sql(
            sql.SQL("GRANT EXECUTE ON FUNCTION " + _SIGNATURE + " TO {}")
            .format(sql.Identifier(limited_role))
            .as_string()
        )
    for _ in range(2):
        with clean_postgres.begin() as connection:
            connection.exec_driver_sql(
                sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(limited_role)).as_string()
            )
            connection.execute(_CALL, arguments)
    with clean_postgres.connect() as connection:
        connection.exec_driver_sql(
            sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(limited_role)).as_string()
        )
        with pytest.raises(IntegrityError, match="binding"):
            connection.execute(_CALL, {**arguments, "run": "forged-run"})
    with clean_postgres.connect() as connection:
        quantities = {
            row.run_id: row.quantity
            for row in connection.execute(
                select(collection_usage_records.c.run_id, collection_usage_records.c.quantity)
            )
        }
        assert quantities == {bound[0].reservation_run_id: 0, bound[1]: 3}
        connection.exec_driver_sql(
            sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(limited_role)).as_string()
        )
        with pytest.raises(DBAPIError, match="permission denied"):
            connection.execute(text("UPDATE public.collection_usage_records SET quantity=0"))


@pytest.mark.parametrize(
    "change",
    [
        {"run": "missing-run"},
        {"reservation": "missing-reservation"},
        {"requested": 4},
        {"requested": 0},
        {"requested": 2147483648},
        {"actual": -1},
        {"actual": 6},
        {"actual": None},
        {"reservation": "fred-bound-run"},
    ],
)
def test_forged_settlement_rolls_back_reservation(
    tmp_path: Path, clean_postgres: Engine, change: dict[str, object]
) -> None:
    bound = _bound(clean_postgres, tmp_path)
    with clean_postgres.connect() as connection, pytest.raises(IntegrityError):
        connection.execute(_CALL, {**_arguments(bound), **change})
    with clean_postgres.connect() as connection:
        assert connection.execute(
            select(collection_usage_records.c.run_id, collection_usage_records.c.quantity)
        ).all() == [(bound[0].reservation_run_id, 5)]


def test_foreign_existing_run_cannot_adopt_reservation(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    bound = _bound(clean_postgres, tmp_path)
    registry = CollectionRegistry(clean_postgres)
    plan = build_run_plan(
        mode=CollectionMode.PROBE, series_ids=("T10Y2Y",), created_at_utc=bound[2]
    )
    registry.start_run(CollectionRun("another-fred-run", plan.plan_id, bound[2]))
    with clean_postgres.connect() as connection, pytest.raises(IntegrityError, match="binding"):
        connection.execute(_CALL, {**_arguments(bound), "run": "another-fred-run"})


def test_settlement_rejects_cross_midnight_and_conflicting_retry(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    bound = _bound(clean_postgres, tmp_path)
    with clean_postgres.connect() as connection, pytest.raises(IntegrityError, match="UTC day"):
        connection.execute(_CALL, {**_arguments(bound), "recorded": bound[2] - timedelta(days=1)})
    with clean_postgres.begin() as connection:
        connection.execute(_CALL, _arguments(bound))
    with clean_postgres.connect() as connection, pytest.raises(IntegrityError, match="conflicts"):
        connection.execute(_CALL, {**_arguments(bound), "actual": 2})


def test_concurrent_identical_settlement_inserts_once(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    arguments = _arguments(_bound(clean_postgres, tmp_path))

    def settle() -> None:
        with clean_postgres.begin() as connection:
            connection.execute(_CALL, arguments)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = executor.submit(settle), executor.submit(settle)
        first.result(timeout=10)
        second.result(timeout=10)
    with clean_postgres.connect() as connection:
        assert connection.execute(
            select(collection_usage_records.c.quantity).order_by(
                collection_usage_records.c.quantity
            )
        ).scalars().all() == [0, 3]


def test_settlement_migration_roundtrip(clean_postgres: Engine, alembic_config: Config) -> None:
    command.downgrade(alembic_config, "20260905_0012")
    with clean_postgres.connect() as connection:
        assert (
            connection.scalar(text("SELECT to_regprocedure(:signature)"), {"signature": _SIGNATURE})
            is None
        )
    command.upgrade(alembic_config, "head")
    with clean_postgres.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT to_regprocedure(:signature) IS NOT NULL"), {"signature": _SIGNATURE}
            )
            is True
        )


def test_unsettled_prior_day_bound_reservation_remains_charged(
    tmp_path: Path, clean_postgres: Engine
) -> None:

    now = datetime.now(UTC)
    previous = now - timedelta(days=1)
    fixture = make_standing_authority(tmp_path)
    authority = verify_recurring_authority(
        fixture.payload, fixture.signature, fixture.owner_authority, now=previous
    )
    registry = CollectionRegistry(clean_postgres)
    plan = build_run_plan(
        mode=CollectionMode.PROBE, series_ids=("T10Y2Y",), created_at_utc=previous
    )
    registry.register_plan(plan)
    registry.start_run(CollectionRun("uncertain-prior-day", plan.plan_id, previous))
    held = admit_requested_calls(
        engine=clean_postgres,
        authority=authority,
        requested_calls=5,
        now=previous,
        actual_run_id="uncertain-prior-day",
    )
    today = load_daily_usage_budget(engine=clean_postgres, authority=authority, now=now)
    assert today.calls_used == held.reserved_calls
    assert today.remaining_calls == authority.calls_per_day - held.reserved_calls
