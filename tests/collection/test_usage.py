from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import Engine, func, select

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunPlan,
    CollectionUsageRecord,
)
from aegis_alpha.collection.registry import CollectionConflictError, CollectionRegistry
from aegis_alpha.collection.schema import collection_usage_records

_CREATED_AT = datetime(2026, 7, 29, 10, 0, tzinfo=UTC)
_EXPECTED_USAGE_ROWS = 2


def _started_run(collection_registry: CollectionRegistry, run_id: str) -> CollectionRun:
    plan = CollectionRunPlan(
        plan_id=f"plan-{run_id}",
        schema_version=1,
        provider="fmp",
        dataset=f"eod_prices_{run_id}",
        mode=CollectionMode.INCREMENTAL,
        requested_window_start=None,
        requested_window_end=None,
        parameters={},
        created_at_utc=_CREATED_AT,
    )
    collection_registry.register_plan(plan)
    run = CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=_CREATED_AT)
    collection_registry.start_run(run)
    return run


def _usage(run_id: str, usage_seq: int = 1, **overrides: object) -> CollectionUsageRecord:
    values: dict[str, object] = {
        "run_id": run_id,
        "usage_seq": usage_seq,
        "metric": "provider_requests",
        "quantity": Decimal(3),
        "unit": "count",
        "recorded_at_utc": _CREATED_AT,
        "evidence": {"endpoint": "historical-price-eod/full", "symbols": 1},
    }
    values.update(overrides)
    return CollectionUsageRecord(**values)


def test_record_usage_captures_usage_and_cost_evidence(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    run = _started_run(collection_registry, "run-us-1")

    collection_registry.record_usage(_usage(run.run_id))
    collection_registry.record_usage(
        _usage(
            run.run_id,
            usage_seq=2,
            metric="provider_cost",
            quantity=Decimal("0.0004"),
            unit="USD",
            evidence={"invoice_window": "2026-07"},
        )
    )

    with clean_postgres.connect() as connection:
        rows = (
            connection.execute(
                select(collection_usage_records).order_by(collection_usage_records.c.usage_seq)
            )
            .mappings()
            .all()
        )
    assert len(rows) == _EXPECTED_USAGE_ROWS
    assert rows[0]["metric"] == "provider_requests"
    assert rows[0]["quantity"] == Decimal(3)
    assert rows[1]["unit"] == "USD"
    assert rows[1]["quantity"] == Decimal("0.0004")


def test_usage_replay_is_idempotent_and_differences_conflict(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    run = _started_run(collection_registry, "run-us-1")
    record = _usage(run.run_id)

    collection_registry.record_usage(record)
    collection_registry.record_usage(record)

    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(collection_usage_records)) == 1
    with pytest.raises(CollectionConflictError):
        collection_registry.record_usage(_usage(run.run_id, quantity=Decimal(4)))


def test_usage_requires_a_registered_run(collection_registry: CollectionRegistry) -> None:
    with pytest.raises(ValueError, match="unknown run"):
        collection_registry.record_usage(_usage("run-missing"))


def test_usage_record_validation() -> None:
    with pytest.raises(ValueError, match="usage_seq"):
        _usage("run-x", usage_seq=0)
    with pytest.raises(ValueError, match="metric"):
        _usage("run-x", metric=" ")
    with pytest.raises(ValueError, match="unit"):
        _usage("run-x", unit="")
    with pytest.raises(ValueError, match="quantity"):
        _usage("run-x", quantity=Decimal(-1))
    with pytest.raises(ValueError, match="quantity"):
        _usage("run-x", quantity=Decimal("NaN"))
    with pytest.raises(ValueError, match="timezone"):
        _usage("run-x", recorded_at_utc=_CREATED_AT.replace(tzinfo=None))
    with pytest.raises(ValueError, match="credential"):
        _usage("run-x", evidence={"api_secret": "x"})
    with pytest.raises(TypeError, match="JSON object"):
        _usage("run-x", evidence="not-an-object")
