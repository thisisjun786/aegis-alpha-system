"""Cumulative UTC-day FRED usage with serialized admission and bound settlement.

Preflight is read-only. Durable runs reserve their maximum before HTTP, then
settle through the narrow database function. Legacy fixture callers retain the
original owner-only accounting path; runtime roles receive no direct UPDATE.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Final
from uuid import uuid4

from sqlalchemy import Connection, Engine, create_engine, make_url, select, text
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.exc import ArgumentError, SQLAlchemyError

from aegis_alpha.collection.provider_usage_repository import ProviderUsageRepository
from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunPlan,
    CollectionUsageRecord,
)
from aegis_alpha.collection.registry import CollectionConflictError, CollectionRegistry
from aegis_alpha.collection.schema import (
    collection_run_plans,
    collection_runs,
    collection_usage_records,
)
from aegis_alpha.collection.usage_checkpoint import UsageRecordLeaf
from aegis_alpha.data.fred_alfred_recurring_authority import VerifiedRecurringAuthority
from aegis_alpha.data.fred_alfred_recurring_errors import DailyBudgetError
from aegis_alpha.data.fred_alfred_series import PLAN_DATASET, PROVIDER

CONTROL_PLANE_URL_ENV: Final = "AAS_FRED_CONTROL_PLANE_DATABASE_URL"
CALLS_ATTEMPTED_METRIC: Final = "calls_attempted"
CALL_UNIT: Final = "call"
_BUDGET_DATASET: Final = "fred_alfred_daily_budget"
_LOCK_PREFIX: Final = "aegis_alpha.fred_alfred.daily_budget"
_CONNECT_TIMEOUT_SECONDS: Final = 5
_MAX_SETTLEMENT_CALLS: Final = 2_147_483_647


@dataclass(frozen=True, slots=True)
class DailyUsageBudget:
    """One UTC day's signed remaining FRED/ALFRED call budget."""

    day_start_utc: datetime
    day_end_utc: datetime
    calls_used: int
    calls_per_day: int
    remaining_calls: int
    reservation_run_id: str | None = None
    reserved_calls: int = 0


def utc_day_window(now: datetime) -> tuple[datetime, datetime]:
    """Return the half-open ``[start, end)`` UTC calendar day containing ``now``."""

    moment = _utc(now)
    start = moment.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def require_control_plane_url(environment: Mapping[str, str]) -> str:
    """Return the dedicated FRED control-plane URL; refuse an absent or invalid value."""

    raw = environment.get(CONTROL_PLANE_URL_ENV)
    if not isinstance(raw, str) or not raw.strip():
        raise DailyBudgetError(f"{CONTROL_PLANE_URL_ENV} is required for cumulative daily budget")
    try:
        url = make_url(raw)
    except ArgumentError:
        raise DailyBudgetError(f"{CONTROL_PLANE_URL_ENV} is invalid") from None
    if url.drivername != "postgresql+psycopg":
        raise DailyBudgetError(f"{CONTROL_PLANE_URL_ENV} must use postgresql+psycopg")
    return raw


def connect_control_plane(database_url: str) -> Engine:
    """Open a short-lived PostgreSQL engine against the dedicated control plane."""

    return create_engine(
        database_url,
        pool_pre_ping=True,
        hide_parameters=True,
        connect_args={"connect_timeout": _CONNECT_TIMEOUT_SECONDS},
    )


def load_daily_usage_budget(
    *,
    engine: Engine,
    authority: VerifiedRecurringAuthority,
    now: datetime,
) -> DailyUsageBudget:
    """Sum provider-lineage FRED calls for the current UTC day without locking."""

    day_start, day_end = utc_day_window(now)
    try:
        with engine.connect() as connection:
            used = _calls_used(
                connection,
                coverage_start_utc=day_start,
                coverage_end_utc=day_end,
            )
    except DailyBudgetError:
        raise
    except (OSError, ValueError, SQLAlchemyError) as error:
        raise DailyBudgetError("FRED/ALFRED daily usage ledger is unavailable") from error
    remaining = authority.calls_per_day - used
    remaining = max(remaining, 0)
    return DailyUsageBudget(
        day_start_utc=day_start,
        day_end_utc=day_end,
        calls_used=used,
        calls_per_day=authority.calls_per_day,
        remaining_calls=remaining,
    )


def require_remaining_budget(
    *,
    engine: Engine,
    authority: VerifiedRecurringAuthority,
    requested_calls: int,
    now: datetime,
) -> DailyUsageBudget:
    """Read-only admission preview: reserves nothing, fails closed when short.

    G-A preflights use this so repeated dry validation can never spend the
    signed daily budget; the gated run itself reserves through
    ``admit_requested_calls``.
    """

    if type(requested_calls) is not int or requested_calls < 1:
        raise DailyBudgetError("--max-calls is mandatory and must be a positive integer")
    snapshot = load_daily_usage_budget(engine=engine, authority=authority, now=now)
    if snapshot.calls_used + requested_calls > authority.calls_per_day:
        raise DailyBudgetError(
            "requested --max-calls exceeds the remaining signed calls_per_day budget"
        )
    return snapshot


def admit_requested_calls(  # noqa: PLR0913 -- signed budget and transaction-time clock
    *,
    engine: Engine,
    authority: VerifiedRecurringAuthority,
    requested_calls: int,
    now: datetime,
    actual_run_id: str | None = None,
    clock: Callable[[], datetime] | None = None,
) -> DailyUsageBudget:
    """Fail closed when prior usage plus ``requested_calls`` would exceed the grant.

    The advisory lock is held for the remainder of the transaction that also
    records reserved consumption, so a concurrent peer cannot observe the same
    residual budget.
    """

    if type(requested_calls) is not int or requested_calls < 1:
        raise DailyBudgetError("--max-calls is mandatory and must be a positive integer")
    day_start, day_end = utc_day_window(now)
    if actual_run_id is not None and requested_calls > _MAX_SETTLEMENT_CALLS:
        raise DailyBudgetError("bound FRED request count exceeds the settlement integer range")
    registry = CollectionRegistry(engine)
    used, reservation_run_id = _admit_under_lock(
        registry,
        authority=authority,
        requested_calls=requested_calls,
        day_start=day_start,
        day_end=day_end,
        now=now,
        actual_run_id=actual_run_id,
        clock=clock,
    )
    return DailyUsageBudget(
        day_start_utc=day_start,
        day_end_utc=day_end,
        calls_used=used + requested_calls,
        calls_per_day=authority.calls_per_day,
        remaining_calls=authority.calls_per_day - (used + requested_calls),
        reservation_run_id=reservation_run_id,
        reserved_calls=requested_calls,
    )


def _admit_under_lock(  # noqa: PLR0913 - each input is one explicit budget boundary
    registry: CollectionRegistry,
    *,
    authority: VerifiedRecurringAuthority,
    requested_calls: int,
    day_start: datetime,
    day_end: datetime,
    now: datetime,
    actual_run_id: str | None,
    clock: Callable[[], datetime] | None,
) -> tuple[int, str | None]:
    """Reserve ``requested_calls`` under the daily advisory lock; fail closed.

    This is the durable reservation boundary: the standing scope is revalidated
    against the current clock immediately before the grant so an authority
    that expired or was revoked after preflight cannot consume budget.
    """

    authority.require_request(now)

    admitted = False
    used = 0
    reservation_run_id: str | None = None
    try:
        with registry.begin_registration() as connection:
            _lock_daily_budget(connection, day_start=day_start)
            checked_at = now if clock is None else clock()
            authority.require_request(checked_at)
            if utc_day_window(checked_at)[0] != day_start:
                raise DailyBudgetError("FRED reservation wait crossed UTC midnight")
            used = _calls_used(
                connection,
                coverage_start_utc=day_start,
                coverage_end_utc=day_end,
            )
            admitted = used + requested_calls <= authority.calls_per_day
            if admitted:
                reservation_run_id = _record_reservation(
                    registry,
                    connection=connection,
                    authority=authority,
                    requested_calls=requested_calls,
                    now=now,
                    actual_run_id=actual_run_id,
                )
    except (CollectionConflictError, OSError, SQLAlchemyError) as error:
        raise DailyBudgetError("FRED/ALFRED daily usage ledger is unavailable") from error
    if not admitted:
        raise DailyBudgetError(
            "requested --max-calls exceeds the remaining signed calls_per_day budget"
        )
    return used, reservation_run_id


def record_run_consumption(  # noqa: PLR0913 - reservation receipt keeps reconciliation explicit
    *,
    engine: Engine,
    run_id: str,
    calls_attempted: int,
    now: datetime,
    usage_seq: int = 1,
    reservation: DailyUsageBudget | None = None,
    database_settlement: bool = False,
    connection: Connection | None = None,
) -> CollectionUsageRecord:
    """Persist one run's actual ``calls_attempted`` quantity on the control plane.

    Every write takes the same provider-wide daily advisory lock that gates
    ``admit_requested_calls``, so an unreserved write can never race an
    admission past the signed bound. When ``reservation`` is this run's
    admission receipt, the reservation row is verified against its plan and
    zeroed in the same transaction, and the full actual quantity is recorded
    under the real run identity.
    """

    if type(calls_attempted) is not int or calls_attempted < 0:
        raise DailyBudgetError("actual FRED/ALFRED consumption must be a nonnegative integer")
    record = CollectionUsageRecord(
        run_id=run_id,
        usage_seq=usage_seq,
        metric=CALLS_ATTEMPTED_METRIC,
        quantity=Decimal(calls_attempted),
        unit=CALL_UNIT,
        recorded_at_utc=_utc(now),
        evidence={"source": "fred-alfred-actual-consumption"},
    )
    if database_settlement:
        _record_database_consumption(engine, connection, record, reservation)
        return record
    if connection is not None:
        raise DailyBudgetError("caller transaction requires database settlement")
    day_start, _day_end = utc_day_window(record.recorded_at_utc)
    if (
        reservation is not None
        and reservation.reservation_run_id is not None
        and reservation.day_start_utc != day_start
    ):
        raise DailyBudgetError(
            "FRED/ALFRED consumption settled after UTC midnight; "
            "re-admit under the new UTC day's signed budget"
        )
    try:
        with CollectionRegistry(engine).begin_registration() as active:
            _lock_daily_budget(active, day_start=day_start)
            if reservation is not None and reservation.reservation_run_id is not None:
                _zero_verified_reservation(
                    active,
                    reservation=reservation,
                    actual_run_id=run_id,
                    actual_calls=calls_attempted,
                )
            _insert_actual_once(active, record=record, reservation=reservation)
    except DailyBudgetError:
        raise
    except (CollectionConflictError, OSError, ValueError, SQLAlchemyError) as error:
        raise DailyBudgetError("FRED/ALFRED daily usage ledger is unavailable") from error
    return record


def _record_database_consumption(
    engine: Engine,
    connection: Connection | None,
    record: CollectionUsageRecord,
    reservation: DailyUsageBudget | None,
) -> None:
    if reservation is None or reservation.reservation_run_id is None or record.usage_seq != 1:
        raise DailyBudgetError("database settlement requires a bound reservation and usage_seq 1")
    if connection is None:
        with engine.begin() as active:
            _settle_database(active, record, reservation)
    else:
        if connection.engine is not engine:
            raise DailyBudgetError("settlement connection belongs to another engine")
        _settle_database(connection, record, reservation)


def _settle_database(
    connection: Connection, record: CollectionUsageRecord, reservation: DailyUsageBudget
) -> None:
    connection.execute(
        text(
            "SELECT engine.settle_fred_budget(:reservation, :run, :requested, :actual, :recorded)"
        ),
        {
            "reservation": reservation.reservation_run_id,
            "run": record.run_id,
            "requested": reservation.reserved_calls,
            "actual": int(record.quantity),
            "recorded": record.recorded_at_utc,
        },
    )


def settled_consumption_matches(
    engine: Engine,
    record: CollectionUsageRecord,
    reservation: DailyUsageBudget,
    *,
    connection: Connection | None = None,
) -> bool:
    """Verify a past committed settlement without attempting a cross-day rewrite."""
    if connection is not None and connection.engine is not engine:
        raise DailyBudgetError("settlement verification connection belongs to another engine")
    with engine.connect() if connection is None else nullcontext(connection) as active:
        row = active.execute(
            text("""
            SELECT u.quantity, u.metric, u.unit, u.evidence_json, u.recorded_at_utc,
                   b.quantity AS reserved_quantity, b.evidence_json AS reservation_evidence,
                   p.parameters_json, p.provider AS budget_provider, p.dataset AS budget_dataset,
                   a.provider AS actual_provider, a.dataset AS actual_dataset
            FROM public.collection_usage_records u
            JOIN public.collection_runs r ON r.run_id=u.run_id
            JOIN public.collection_run_plans a ON a.plan_id=r.plan_id
            JOIN public.collection_usage_records b ON b.run_id=:reservation AND b.usage_seq=1
            JOIN public.collection_runs br ON br.run_id=b.run_id
            JOIN public.collection_run_plans p ON p.plan_id=br.plan_id
            WHERE u.run_id=:run AND u.usage_seq=1
        """),
            {"reservation": reservation.reservation_run_id, "run": record.run_id},
        ).one_or_none()
    return row is not None and (
        row.quantity == record.quantity
        and row.metric == CALLS_ATTEMPTED_METRIC
        and row.unit == CALL_UNIT
        and row.recorded_at_utc == record.recorded_at_utc
        and row.budget_provider == PROVIDER
        and row.budget_dataset == _BUDGET_DATASET
        and row.actual_provider == PROVIDER
        and row.actual_dataset == PLAN_DATASET
        and row.parameters_json.get("actual_run_id") == record.run_id
        and row.parameters_json.get("requested_calls") == reservation.reserved_calls
        and row.reserved_quantity == 0
        and row.evidence_json
        == {
            "source": "fred-alfred-actual-consumption",
            "reconciles_reservation": reservation.reservation_run_id,
        }
        and row.reservation_evidence
        == {
            "source": "fred-alfred-reconciled-reservation",
            "actual_run_id": record.run_id,
            "reserved_calls": reservation.reserved_calls,
            "actual_calls": int(record.quantity),
        }
    )


def _zero_verified_reservation(
    connection: Connection,
    *,
    reservation: DailyUsageBudget,
    actual_run_id: str,
    actual_calls: int,
) -> None:
    """Zero one reservation row after proving it is this receipt's own grant."""

    reservation_run_id = reservation.reservation_run_id
    if reservation_run_id is None:  # pragma: no cover - caller-checked invariant
        raise AssertionError("reconciliation requires a reservation identity")
    row = connection.execute(
        select(
            collection_usage_records.c.quantity,
            collection_usage_records.c.evidence_json,
            collection_usage_records.c.recorded_at_utc,
            collection_run_plans.c.dataset,
        )
        .join(collection_runs, collection_runs.c.run_id == collection_usage_records.c.run_id)
        .join(
            collection_run_plans,
            collection_run_plans.c.plan_id == collection_runs.c.plan_id,
        )
        .where(
            (collection_usage_records.c.run_id == reservation_run_id)
            & (collection_usage_records.c.usage_seq == 1)
        )
    ).one_or_none()
    if row is None:
        raise DailyBudgetError("FRED/ALFRED budget reservation row is missing or ambiguous")
    quantity, evidence, recorded_at, dataset = row
    if dataset != _BUDGET_DATASET:
        raise DailyBudgetError("FRED/ALFRED reservation row is not a daily-budget grant")
    evidence_map = dict(evidence) if isinstance(evidence, Mapping) else {}
    source = evidence_map.get("source")
    if source == "fred-alfred-reconciled-reservation":
        # Retry after a committed settlement: verify it describes this exact
        # receipt and treat the zeroed row as already reconciled.
        if (
            evidence_map.get("actual_run_id") == actual_run_id
            and evidence_map.get("actual_calls") == actual_calls
        ):
            return
        raise DailyBudgetError("FRED/ALFRED reservation was settled by a different run")
    if source != "fred-alfred-daily-budget-reservation":
        raise DailyBudgetError("FRED/ALFRED reservation row carries foreign evidence")
    if quantity != Decimal(reservation.reserved_calls):
        raise DailyBudgetError("FRED/ALFRED reservation row does not match its receipt")
    if not reservation.day_start_utc <= recorded_at < reservation.day_end_utc:
        raise DailyBudgetError("FRED/ALFRED reservation row belongs to another UTC day")
    updated = connection.execute(
        collection_usage_records.update()
        .where(
            (collection_usage_records.c.run_id == reservation_run_id)
            & (collection_usage_records.c.usage_seq == 1)
        )
        .values(
            quantity=Decimal(0),
            evidence_json={
                "source": "fred-alfred-reconciled-reservation",
                "actual_run_id": actual_run_id,
                "reserved_calls": reservation.reserved_calls,
                "actual_calls": actual_calls,
            },
        )
    )
    if updated.rowcount != 1:
        raise DailyBudgetError("FRED/ALFRED budget reservation row vanished under the lock")


def _insert_actual_once(
    connection: Connection,
    *,
    record: CollectionUsageRecord,
    reservation: DailyUsageBudget | None,
) -> None:
    """Insert one run's actual row once; retries must match, never duplicate."""

    lineage = connection.execute(
        select(collection_run_plans.c.provider)
        .join(collection_runs, collection_runs.c.plan_id == collection_run_plans.c.plan_id)
        .where(collection_runs.c.run_id == record.run_id)
    ).scalar_one_or_none()
    if lineage is None:
        raise DailyBudgetError("actual FRED/ALFRED run is unknown to the control plane")
    if lineage != PROVIDER:
        raise DailyBudgetError("actual FRED/ALFRED run carries foreign provider lineage")
    evidence = dict(record.evidence)
    if reservation is not None and reservation.reservation_run_id is not None:
        evidence["reconciles_reservation"] = reservation.reservation_run_id
    existing = connection.execute(
        select(
            collection_usage_records.c.quantity,
            collection_usage_records.c.metric,
            collection_usage_records.c.unit,
            collection_usage_records.c.evidence_json,
        ).where(
            (collection_usage_records.c.run_id == record.run_id)
            & (collection_usage_records.c.usage_seq == record.usage_seq)
        )
    ).first()
    if existing is None:
        connection.execute(
            postgres_insert(collection_usage_records).values(
                {
                    "run_id": record.run_id,
                    "usage_seq": record.usage_seq,
                    "metric": record.metric,
                    "quantity": record.quantity,
                    "unit": CALL_UNIT,
                    "evidence_json": evidence,
                    "recorded_at_utc": record.recorded_at_utc,
                }
            )
        )
        return
    quantity, metric, unit, row_evidence = existing
    row_evidence_map = dict(row_evidence) if isinstance(row_evidence, Mapping) else {}
    if (
        metric != CALLS_ATTEMPTED_METRIC
        or unit != CALL_UNIT
        or quantity != record.quantity
        or row_evidence_map.get("source") != evidence.get("source")
        or row_evidence_map.get("reconciles_reservation") != evidence.get("reconciles_reservation")
    ):
        raise DailyBudgetError("FRED/ALFRED reconciled usage conflicts with its retry")


def _lock_daily_budget(connection: Connection, *, day_start: datetime) -> None:
    """Serialize every admission that consumes the provider-wide FRED ledger.

    The key deliberately excludes the authority payload: two concurrently valid
    scopes share one ledger, so they must serialize on the same lock or a
    rotation window could double-admit the same residual budget.
    """

    key = f"{_LOCK_PREFIX}:{PROVIDER}:{day_start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}"
    connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": key},
    )


def _calls_used(
    connection: Connection,
    *,
    coverage_start_utc: datetime,
    coverage_end_utc: datetime,
) -> int:
    current = _integral_calls(
        ProviderUsageRepository.leaves(
            connection,
            provider=PROVIDER,
            coverage_start_utc=coverage_start_utc,
            coverage_end_utc=coverage_end_utc,
        )
    )
    # An interrupted bound run cannot silently gain a fresh budget at midnight.
    # Keep its unresolved maximum charged until explicit reconciliation is possible.
    pending = connection.scalar(
        text("""
        SELECT coalesce(sum(u.quantity), 0)
        FROM public.collection_usage_records u
        JOIN public.collection_runs r ON r.run_id=u.run_id
        JOIN public.collection_run_plans p ON p.plan_id=r.plan_id
        WHERE p.provider=:provider AND p.dataset=:dataset
          AND p.parameters_json->>'actual_run_id' IS NOT NULL
          AND u.metric='calls_attempted' AND u.unit='call'
          AND u.evidence_json->>'source'='fred-alfred-daily-budget-reservation'
          AND u.recorded_at_utc < :start
    """),
        {"provider": PROVIDER, "dataset": _BUDGET_DATASET, "start": coverage_start_utc},
    )
    return current + _require_integral(Decimal(pending))


def _record_reservation(  # noqa: PLR0913 -- reservation records its bound run before transport
    registry: CollectionRegistry,
    *,
    connection: Connection,
    authority: VerifiedRecurringAuthority,
    requested_calls: int,
    now: datetime,
    actual_run_id: str | None,
) -> str:
    recorded_at = _utc(now)
    if actual_run_id is not None:
        provider = connection.scalar(
            select(collection_run_plans.c.provider)
            .join(collection_runs, collection_runs.c.plan_id == collection_run_plans.c.plan_id)
            .where(collection_runs.c.run_id == actual_run_id)
        )
        if provider != PROVIDER:
            raise DailyBudgetError("reservation requires an existing FRED run")
    token = uuid4().hex
    plan = CollectionRunPlan(
        plan_id=f"fred-alfred-budget-{authority.payload_sha256[:16]}-{token}",
        schema_version=1,
        provider=PROVIDER,
        dataset=_BUDGET_DATASET,
        mode=CollectionMode.PROBE,
        requested_window_start=None,
        requested_window_end=None,
        parameters={
            "kind": "daily-budget-reservation",
            "authority_payload_sha256": authority.payload_sha256,
            "requested_calls": requested_calls,
            "reservation_id": token,
            "actual_run_id": actual_run_id,
        },
        created_at_utc=recorded_at,
    )
    run_id = f"fred-alfred-budget-{token}"
    registry.register_plan(plan, connection=connection)
    registry.create_or_observe_run(
        CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=recorded_at),
        connection=connection,
    )
    record = CollectionUsageRecord(
        run_id=run_id,
        usage_seq=1,
        metric=CALLS_ATTEMPTED_METRIC,
        quantity=Decimal(requested_calls),
        unit=CALL_UNIT,
        recorded_at_utc=recorded_at,
        evidence={
            "source": "fred-alfred-daily-budget-reservation",
            "authority_payload_sha256": authority.payload_sha256,
        },
    )
    connection.execute(
        postgres_insert(collection_usage_records).values(
            {
                "run_id": record.run_id,
                "usage_seq": record.usage_seq,
                "metric": record.metric,
                "quantity": record.quantity,
                "unit": record.unit,
                "evidence_json": dict(record.evidence),
                "recorded_at_utc": record.recorded_at_utc,
            }
        )
    )
    return record.run_id


def _integral_calls(leaves: Sequence[UsageRecordLeaf]) -> int:
    quantity = Decimal(0)
    for leaf in leaves:
        if leaf.metric != CALLS_ATTEMPTED_METRIC:
            continue
        if leaf.unit != CALL_UNIT:
            raise DailyBudgetError("FRED/ALFRED daily usage ledger unit is invalid")
        quantity += leaf.quantity
    return _require_integral(quantity)


def _require_integral(value: Decimal) -> int:
    if value != value.to_integral_value() or value < 0:
        raise DailyBudgetError("FRED/ALFRED daily usage quantity must be a nonnegative integer")
    return int(value)


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise DailyBudgetError("FRED/ALFRED daily budget clock must be timezone-aware UTC")
    moment = value.astimezone(UTC)
    if moment.utcoffset() != UTC.utcoffset(None):
        raise DailyBudgetError("FRED/ALFRED daily budget clock must be UTC")
    return moment


__all__ = [
    "CALLS_ATTEMPTED_METRIC",
    "CALL_UNIT",
    "CONTROL_PLANE_URL_ENV",
    "DailyUsageBudget",
    "admit_requested_calls",
    "connect_control_plane",
    "load_daily_usage_budget",
    "record_run_consumption",
    "require_control_plane_url",
    "require_remaining_budget",
    "utc_day_window",
]
