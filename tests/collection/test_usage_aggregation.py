from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunPlan,
    CollectionUsageRecord,
)
from aegis_alpha.collection.registry import CollectionRegistry

_CREATED_AT = datetime(2026, 7, 29, 10, 0, tzinfo=UTC)
_SINCE = datetime(2026, 8, 1, 0, 0, tzinfo=UTC)
_UNTIL = datetime(2026, 8, 31, 0, 0, tzinfo=UTC)
_MID_WINDOW = datetime(2026, 8, 15, 12, 0, tzinfo=UTC)


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


def _usage(run_id: str, usage_seq: int, **overrides: object) -> CollectionUsageRecord:
    values: dict[str, object] = {
        "run_id": run_id,
        "usage_seq": usage_seq,
        "metric": "provider_requests",
        "quantity": Decimal(1),
        "unit": "count",
        "recorded_at_utc": _MID_WINDOW,
        "evidence": {"endpoint": "historical-price-eod/full"},
    }
    values.update(overrides)
    return CollectionUsageRecord(**values)


def test_aggregate_usage_sums_quantities_grouped_by_metric_and_unit(
    collection_registry: CollectionRegistry,
) -> None:
    first = _started_run(collection_registry, "run-agg-1")
    second = _started_run(collection_registry, "run-agg-2")
    collection_registry.record_usage(_usage(first.run_id, 1, quantity=Decimal(3)))
    collection_registry.record_usage(_usage(first.run_id, 2, quantity=Decimal(4)))
    collection_registry.record_usage(_usage(second.run_id, 1, quantity=Decimal(2)))
    collection_registry.record_usage(
        _usage(first.run_id, 3, metric="provider_egress", quantity=Decimal("0.1"), unit="byte")
    )
    collection_registry.record_usage(
        _usage(second.run_id, 2, metric="provider_egress", quantity=Decimal("0.2"), unit="byte")
    )

    totals = collection_registry.aggregate_usage(since_utc=_SINCE, until_utc=_UNTIL)

    assert totals == {
        ("provider_requests", "count"): Decimal(9),
        ("provider_egress", "byte"): Decimal("0.3"),
    }


def test_aggregate_usage_preserves_numeric_24_6_precision(
    collection_registry: CollectionRegistry,
) -> None:
    run = _started_run(collection_registry, "run-agg-precision")
    collection_registry.record_usage(
        _usage(run.run_id, 1, quantity=Decimal("999999999999999999.999999"))
    )
    collection_registry.record_usage(_usage(run.run_id, 2, quantity=Decimal("0.000001")))

    totals = collection_registry.aggregate_usage(since_utc=_SINCE, until_utc=_UNTIL)

    assert totals == {("provider_requests", "count"): Decimal("1000000000000000000.000000")}


def test_aggregate_usage_window_is_since_inclusive_until_exclusive(
    collection_registry: CollectionRegistry,
) -> None:
    run = _started_run(collection_registry, "run-agg-edges")
    collection_registry.record_usage(
        _usage(run.run_id, 1, recorded_at_utc=_SINCE - timedelta(seconds=1))
    )
    collection_registry.record_usage(_usage(run.run_id, 2, recorded_at_utc=_SINCE))
    collection_registry.record_usage(_usage(run.run_id, 3, recorded_at_utc=_MID_WINDOW))
    collection_registry.record_usage(_usage(run.run_id, 4, recorded_at_utc=_UNTIL))
    collection_registry.record_usage(
        _usage(run.run_id, 5, recorded_at_utc=_UNTIL + timedelta(seconds=1))
    )

    totals = collection_registry.aggregate_usage(since_utc=_SINCE, until_utc=_UNTIL)

    assert totals == {("provider_requests", "count"): Decimal(2)}


def test_aggregate_usage_filters_run_ids_when_provided(
    collection_registry: CollectionRegistry,
) -> None:
    first = _started_run(collection_registry, "run-agg-1")
    second = _started_run(collection_registry, "run-agg-2")
    collection_registry.record_usage(_usage(first.run_id, 1, quantity=Decimal(3)))
    collection_registry.record_usage(_usage(second.run_id, 1, quantity=Decimal(5)))

    selected = collection_registry.aggregate_usage(
        since_utc=_SINCE, until_utc=_UNTIL, run_ids=[first.run_id]
    )
    everything = collection_registry.aggregate_usage(since_utc=_SINCE, until_utc=_UNTIL)
    none = collection_registry.aggregate_usage(since_utc=_SINCE, until_utc=_UNTIL, run_ids=[])

    assert selected == {("provider_requests", "count"): Decimal(3)}
    assert everything == {("provider_requests", "count"): Decimal(8)}
    assert none == {}


def test_aggregate_usage_returns_empty_mapping_when_window_matches_nothing(
    collection_registry: CollectionRegistry,
) -> None:
    run = _started_run(collection_registry, "run-agg-empty")
    collection_registry.record_usage(_usage(run.run_id, 1))

    empty_window = collection_registry.aggregate_usage(since_utc=_UNTIL, until_utc=_UNTIL)
    outside = collection_registry.aggregate_usage(
        since_utc=_UNTIL, until_utc=_UNTIL + timedelta(days=1)
    )

    assert empty_window == {}
    assert outside == {}


def test_aggregate_usage_requires_timezone_aware_bounds(
    collection_registry: CollectionRegistry,
) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        collection_registry.aggregate_usage(since_utc=_SINCE.replace(tzinfo=None), until_utc=_UNTIL)
    with pytest.raises(ValueError, match="timezone-aware"):
        collection_registry.aggregate_usage(since_utc=_SINCE, until_utc=_UNTIL.replace(tzinfo=None))


def test_usage_record_rejects_quantity_beyond_numeric_24_6() -> None:
    with pytest.raises(ValueError, match="NUMERIC"):
        _usage("run-x", 1, quantity=Decimal(1000000000000000000))
    with pytest.raises(ValueError, match="NUMERIC"):
        _usage("run-x", 1, quantity=Decimal("0.0000001"))
