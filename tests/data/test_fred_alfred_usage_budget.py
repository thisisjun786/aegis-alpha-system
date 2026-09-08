from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Final

import pytest
from fred_alfred_authority_support import AUTHORITY_NOW, make_standing_authority
from sqlalchemy import func, select, text

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunPlan,
    CollectionUsageRecord,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import collection_usage_records
from aegis_alpha.data.fred_alfred_recurring_authority import (
    VerifiedRecurringAuthority,
    verify_recurring_authority,
)
from aegis_alpha.data.fred_alfred_recurring_errors import (
    DailyBudgetError,
    RecurringAuthorityError,
)
from aegis_alpha.data.fred_alfred_series import PLAN_DATASET, PROVIDER
from aegis_alpha.data.fred_alfred_usage_budget import (
    CONTROL_PLANE_URL_ENV,
    DailyUsageBudget,
    admit_requested_calls,
    load_daily_usage_budget,
    record_run_consumption,
    require_control_plane_url,
    utc_day_window,
)

if TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy.engine import Engine

NOW = AUTHORITY_NOW
_TWO_CALLS: Final = 2
_THREE_CALLS: Final = 3
_FOUR_CALLS: Final = 4
_FIVE_CALLS: Final = 5


def _run_calls(engine: Engine, run_id: str) -> int:
    """Sum one run's calls_attempted quantity from the control plane."""

    with engine.connect() as connection:
        total = connection.execute(
            select(func.coalesce(func.sum(collection_usage_records.c.quantity), 0)).where(
                collection_usage_records.c.run_id == run_id
            )
        ).scalar_one()
    return int(total)


def _reservation_quantity(engine: Engine, reservation_run_id: str | None) -> Decimal:
    assert reservation_run_id is not None
    with engine.connect() as connection:
        return connection.execute(
            select(collection_usage_records.c.quantity).where(
                (collection_usage_records.c.run_id == reservation_run_id)
                & (collection_usage_records.c.usage_seq == 1)
            )
        ).scalar_one()


def _verified(tmp_path: Path, *, calls_per_day: int) -> VerifiedRecurringAuthority:
    fixture = make_standing_authority(tmp_path, document_changes={"calls_per_day": calls_per_day})
    return verify_recurring_authority(
        fixture.payload, fixture.signature, fixture.owner_authority, now=NOW
    )


def _seed_fred_calls(
    registry: CollectionRegistry,
    *,
    run_id: str,
    calls: int,
    recorded_at: datetime,
    provider: str = PROVIDER,
) -> None:
    plan = CollectionRunPlan(
        plan_id=f"plan-{run_id}",
        schema_version=1,
        provider=provider,
        dataset=PLAN_DATASET,
        mode=CollectionMode.INCREMENTAL,
        requested_window_start=None,
        requested_window_end=None,
        parameters={"source": "seeded daily budget", "run_id": run_id},
        created_at_utc=recorded_at,
    )
    registry.register_plan(plan)
    registry.start_run(
        CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=recorded_at)
    )
    registry.record_usage(
        CollectionUsageRecord(
            run_id=run_id,
            usage_seq=1,
            metric="calls_attempted",
            quantity=Decimal(calls),
            unit="call",
            recorded_at_utc=recorded_at,
            evidence={"source": "seeded daily budget"},
        )
    )


def test_require_control_plane_url_fails_closed_when_absent() -> None:
    with pytest.raises(DailyBudgetError, match=CONTROL_PLANE_URL_ENV):
        require_control_plane_url({})


def test_mid_day_exhaustion_fails_closed(tmp_path: Path, clean_postgres: Engine) -> None:
    authority = _verified(tmp_path, calls_per_day=5)
    registry = CollectionRegistry(clean_postgres)
    _seed_fred_calls(registry, run_id="fred-prior-1", calls=3, recorded_at=NOW)

    snapshot = load_daily_usage_budget(engine=clean_postgres, authority=authority, now=NOW)
    assert snapshot.calls_used == _THREE_CALLS
    assert snapshot.remaining_calls == _TWO_CALLS

    admitted = admit_requested_calls(
        engine=clean_postgres,
        authority=authority,
        requested_calls=2,
        now=NOW + timedelta(seconds=1),
    )
    assert admitted.calls_used == _FIVE_CALLS
    assert admitted.remaining_calls == 0

    with pytest.raises(DailyBudgetError, match="remaining signed calls_per_day"):
        admit_requested_calls(
            engine=clean_postgres,
            authority=authority,
            requested_calls=1,
            now=NOW + timedelta(seconds=2),
        )


def test_restart_continues_from_durable_usage(tmp_path: Path, clean_postgres: Engine) -> None:
    authority = _verified(tmp_path, calls_per_day=4)
    registry = CollectionRegistry(clean_postgres)
    _seed_fred_calls(registry, run_id="fred-restart-prior", calls=2, recorded_at=NOW)

    first = admit_requested_calls(
        engine=clean_postgres,
        authority=authority,
        requested_calls=1,
        now=NOW + timedelta(seconds=1),
    )
    assert first.calls_used == _THREE_CALLS

    restarted = CollectionRegistry(clean_postgres)
    continued = load_daily_usage_budget(engine=restarted.engine, authority=authority, now=NOW)
    assert continued.calls_used == _THREE_CALLS
    assert continued.remaining_calls == 1

    second = admit_requested_calls(
        engine=restarted.engine,
        authority=authority,
        requested_calls=1,
        now=NOW + timedelta(seconds=2),
    )
    assert second.remaining_calls == 0
    with pytest.raises(DailyBudgetError, match="remaining signed calls_per_day"):
        admit_requested_calls(
            engine=restarted.engine,
            authority=authority,
            requested_calls=1,
            now=NOW + timedelta(seconds=3),
        )


def test_utc_day_rollover_resets_the_budget(tmp_path: Path, clean_postgres: Engine) -> None:
    authority = _verified(tmp_path, calls_per_day=2)
    registry = CollectionRegistry(clean_postgres)
    yesterday = datetime(2026, 8, 17, 23, 59, 59, tzinfo=UTC)
    _seed_fred_calls(registry, run_id="fred-yesterday", calls=2, recorded_at=yesterday)

    rolled = load_daily_usage_budget(engine=clean_postgres, authority=authority, now=NOW)
    assert rolled.calls_used == 0
    assert rolled.remaining_calls == _TWO_CALLS

    admitted = admit_requested_calls(
        engine=clean_postgres,
        authority=authority,
        requested_calls=2,
        now=NOW,
    )
    assert admitted.calls_used == _TWO_CALLS
    next_day = datetime(2026, 8, 19, 0, 0, tzinfo=UTC)
    reset = load_daily_usage_budget(engine=clean_postgres, authority=authority, now=next_day)
    assert reset.calls_used == 0
    assert reset.remaining_calls == _TWO_CALLS


def test_foreign_provider_usage_does_not_consume_fred_budget(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    authority = _verified(tmp_path, calls_per_day=1)
    registry = CollectionRegistry(clean_postgres)
    _seed_fred_calls(
        registry,
        run_id="fmp-noise",
        calls=50,
        recorded_at=NOW,
        provider="fmp",
    )

    snapshot = load_daily_usage_budget(engine=clean_postgres, authority=authority, now=NOW)
    assert snapshot.calls_used == 0
    admitted = admit_requested_calls(
        engine=clean_postgres,
        authority=authority,
        requested_calls=1,
        now=NOW,
    )
    assert admitted.remaining_calls == 0


def test_actual_consumption_is_visible_to_a_later_admission(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    authority = _verified(tmp_path, calls_per_day=3)
    registry = CollectionRegistry(clean_postgres)
    _seed_fred_calls(registry, run_id="fred-actual-run", calls=0, recorded_at=NOW)
    record_run_consumption(
        engine=clean_postgres,
        run_id="fred-actual-run",
        calls_attempted=2,
        now=NOW + timedelta(seconds=1),
        usage_seq=2,
    )

    snapshot = load_daily_usage_budget(engine=clean_postgres, authority=authority, now=NOW)
    assert snapshot.calls_used == _TWO_CALLS
    with pytest.raises(DailyBudgetError, match="remaining signed calls_per_day"):
        admit_requested_calls(
            engine=clean_postgres,
            authority=authority,
            requested_calls=2,
            now=NOW + timedelta(seconds=2),
        )
    admitted = admit_requested_calls(
        engine=clean_postgres,
        authority=authority,
        requested_calls=1,
        now=NOW + timedelta(seconds=3),
    )
    assert admitted.calls_used == _THREE_CALLS


def test_concurrent_admissions_cannot_overspend(tmp_path: Path, clean_postgres: Engine) -> None:
    authority = _verified(tmp_path, calls_per_day=3)
    started = {"count": 0}

    def _attempt() -> str:
        started["count"] += 1
        try:
            admit_requested_calls(
                engine=clean_postgres,
                authority=authority,
                requested_calls=2,
                now=NOW,
            )
        except DailyBudgetError:
            return "rejected"
        return "admitted"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _index: _attempt(), range(2)))

    assert started["count"] == _TWO_CALLS
    assert "admitted" in outcomes
    assert "rejected" in outcomes
    snapshot = load_daily_usage_budget(engine=clean_postgres, authority=authority, now=NOW)
    assert snapshot.calls_used == _TWO_CALLS
    assert snapshot.remaining_calls == 1


def test_actual_consumption_replaces_the_reservation(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    authority = _verified(tmp_path, calls_per_day=_FIVE_CALLS)
    registry = CollectionRegistry(clean_postgres)
    _seed_fred_calls(registry, run_id="fred-reconcile-run", calls=0, recorded_at=NOW)

    admitted = admit_requested_calls(
        engine=clean_postgres,
        authority=authority,
        requested_calls=_THREE_CALLS,
        now=NOW,
    )
    record_run_consumption(
        engine=clean_postgres,
        run_id="fred-reconcile-run",
        calls_attempted=_TWO_CALLS,
        now=NOW + timedelta(seconds=1),
        usage_seq=_TWO_CALLS,
        reservation=admitted,
    )

    snapshot = load_daily_usage_budget(engine=clean_postgres, authority=authority, now=NOW)
    assert snapshot.calls_used == _TWO_CALLS
    assert _run_calls(clean_postgres, "fred-reconcile-run") == _TWO_CALLS
    assert _reservation_quantity(clean_postgres, admitted.reservation_run_id) == Decimal(0)


def test_reconciliation_is_retry_idempotent(tmp_path: Path, clean_postgres: Engine) -> None:
    authority = _verified(tmp_path, calls_per_day=_FIVE_CALLS)
    registry = CollectionRegistry(clean_postgres)
    _seed_fred_calls(registry, run_id="fred-retry-run", calls=0, recorded_at=NOW)

    admitted = admit_requested_calls(
        engine=clean_postgres,
        authority=authority,
        requested_calls=_THREE_CALLS,
        now=NOW,
    )
    for _attempt in range(_TWO_CALLS):
        record_run_consumption(
            engine=clean_postgres,
            run_id="fred-retry-run",
            calls_attempted=_TWO_CALLS,
            now=NOW + timedelta(seconds=1),
            usage_seq=_TWO_CALLS,
            reservation=admitted,
        )

    snapshot = load_daily_usage_budget(engine=clean_postgres, authority=authority, now=NOW)
    assert snapshot.calls_used == _TWO_CALLS


def test_actual_consumption_above_reservation_adds_only_the_delta(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    authority = _verified(tmp_path, calls_per_day=_FIVE_CALLS)
    registry = CollectionRegistry(clean_postgres)
    _seed_fred_calls(registry, run_id="fred-overage-run", calls=0, recorded_at=NOW)

    admitted = admit_requested_calls(
        engine=clean_postgres,
        authority=authority,
        requested_calls=_TWO_CALLS,
        now=NOW,
    )
    record_run_consumption(
        engine=clean_postgres,
        run_id="fred-overage-run",
        calls_attempted=4,
        now=NOW + timedelta(seconds=1),
        usage_seq=_TWO_CALLS,
        reservation=admitted,
    )

    snapshot = load_daily_usage_budget(engine=clean_postgres, authority=authority, now=NOW)
    assert snapshot.calls_used == _FOUR_CALLS
    assert _run_calls(clean_postgres, "fred-overage-run") == _FOUR_CALLS
    assert _reservation_quantity(clean_postgres, admitted.reservation_run_id) == Decimal(0)


def test_provider_wide_lock_spans_distinct_authorities(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    first = _verified(tmp_path / "first", calls_per_day=_THREE_CALLS)
    second = _verified(tmp_path / "second", calls_per_day=_THREE_CALLS)

    admit_requested_calls(
        engine=clean_postgres,
        authority=first,
        requested_calls=_TWO_CALLS,
        now=NOW,
    )
    with pytest.raises(DailyBudgetError, match="remaining signed calls_per_day"):
        admit_requested_calls(
            engine=clean_postgres,
            authority=second,
            requested_calls=_TWO_CALLS,
            now=NOW,
        )

    third = _verified(tmp_path / "third", calls_per_day=_FIVE_CALLS)
    lock_key = (
        f"aegis_alpha.fred_alfred.daily_budget:{PROVIDER}:"
        f"{utc_day_window(NOW)[0].strftime('%Y-%m-%dT%H:%M:%S.%fZ')}"
    )
    holder = clean_postgres.connect()
    holder.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": lock_key}
    )
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            blocked = pool.submit(
                admit_requested_calls,
                engine=clean_postgres,
                authority=third,
                requested_calls=1,
                now=NOW,
            )
            with pytest.raises(TimeoutError):
                blocked.result(timeout=0.5)
            holder.rollback()
            budget = blocked.result(timeout=10)
        assert budget.reservation_run_id is not None
    finally:
        holder.close()


def test_settlement_after_utc_midnight_fails_closed(tmp_path: Path, clean_postgres: Engine) -> None:
    authority = _verified(tmp_path, calls_per_day=_FIVE_CALLS)
    registry = CollectionRegistry(clean_postgres)
    _seed_fred_calls(registry, run_id="fred-midnight-run", calls=0, recorded_at=NOW)

    admitted = admit_requested_calls(
        engine=clean_postgres,
        authority=authority,
        requested_calls=_TWO_CALLS,
        now=NOW,
    )
    next_day = datetime(2026, 8, 19, 0, 0, 1, tzinfo=UTC)
    with pytest.raises(DailyBudgetError, match="settled after UTC midnight"):
        record_run_consumption(
            engine=clean_postgres,
            run_id="fred-midnight-run",
            calls_attempted=1,
            now=next_day,
            reservation=admitted,
        )


def test_foreign_run_row_cannot_be_zeroed_as_a_reservation(clean_postgres: Engine) -> None:
    registry = CollectionRegistry(clean_postgres)
    _seed_fred_calls(registry, run_id="fred-ordinary-run", calls=_THREE_CALLS, recorded_at=NOW)

    forged = DailyUsageBudget(
        day_start_utc=utc_day_window(NOW)[0],
        day_end_utc=utc_day_window(NOW)[1],
        calls_used=_THREE_CALLS,
        calls_per_day=_FIVE_CALLS,
        remaining_calls=_TWO_CALLS,
        reservation_run_id="fred-ordinary-run",
        reserved_calls=_THREE_CALLS,
    )
    with pytest.raises(DailyBudgetError, match="not a daily-budget grant"):
        record_run_consumption(
            engine=clean_postgres,
            run_id="fred-ordinary-run",
            calls_attempted=1,
            now=NOW + timedelta(seconds=1),
            usage_seq=_TWO_CALLS,
            reservation=forged,
        )


def test_admission_revalidates_an_expired_authority(tmp_path: Path, clean_postgres: Engine) -> None:
    fixture = make_standing_authority(
        tmp_path,
        document_changes={"calls_per_day": _FIVE_CALLS},
    )
    authority = verify_recurring_authority(
        fixture.payload, fixture.signature, fixture.owner_authority, now=NOW
    )
    expired = replace(authority, key_valid_until_utc=NOW + timedelta(minutes=_THREE_CALLS))
    with pytest.raises(RecurringAuthorityError, match="signing key is expired"):
        admit_requested_calls(
            engine=clean_postgres,
            authority=expired,
            requested_calls=1,
            now=NOW + timedelta(hours=1),
        )


def test_foreign_provider_run_cannot_settle_fred_usage(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    authority = _verified(tmp_path, calls_per_day=_FIVE_CALLS)
    registry = CollectionRegistry(clean_postgres)
    _seed_fred_calls(
        registry,
        run_id="fmp-lineage-run",
        calls=0,
        recorded_at=NOW,
        provider="fmp",
    )

    admitted = admit_requested_calls(
        engine=clean_postgres,
        authority=authority,
        requested_calls=_TWO_CALLS,
        now=NOW,
    )
    with pytest.raises(DailyBudgetError, match="foreign provider lineage"):
        record_run_consumption(
            engine=clean_postgres,
            run_id="fmp-lineage-run",
            calls_attempted=_TWO_CALLS,
            now=NOW + timedelta(seconds=1),
            usage_seq=_TWO_CALLS,
            reservation=admitted,
        )
    snapshot = load_daily_usage_budget(engine=clean_postgres, authority=authority, now=NOW)
    assert snapshot.calls_used == _TWO_CALLS


def test_conflicting_retry_is_rejected(tmp_path: Path, clean_postgres: Engine) -> None:
    authority = _verified(tmp_path, calls_per_day=_FIVE_CALLS)
    registry = CollectionRegistry(clean_postgres)
    _seed_fred_calls(registry, run_id="fred-conflict-run", calls=0, recorded_at=NOW)

    admitted = admit_requested_calls(
        engine=clean_postgres,
        authority=authority,
        requested_calls=_THREE_CALLS,
        now=NOW,
    )
    record_run_consumption(
        engine=clean_postgres,
        run_id="fred-conflict-run",
        calls_attempted=_TWO_CALLS,
        now=NOW + timedelta(seconds=1),
        usage_seq=_TWO_CALLS,
        reservation=admitted,
    )
    with pytest.raises(
        DailyBudgetError, match=r"settled by a different run|conflicts with its retry"
    ):
        record_run_consumption(
            engine=clean_postgres,
            run_id="fred-conflict-run",
            calls_attempted=_THREE_CALLS,
            now=NOW + timedelta(seconds=2),
            usage_seq=_TWO_CALLS,
            reservation=admitted,
        )
