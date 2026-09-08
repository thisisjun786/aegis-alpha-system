from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine, func, select

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRunPlan,
    collection_plan_digest,
)
from aegis_alpha.collection.registry import CollectionConflictError, CollectionRegistry
from aegis_alpha.collection.schema import collection_run_plans

_CREATED_AT = datetime(2026, 7, 29, 10, 0, tzinfo=UTC)
_WINDOW_START = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
_WINDOW_END = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)


def _plan(**overrides: object) -> CollectionRunPlan:
    values: dict[str, object] = {
        "plan_id": "fmp-eod-incremental-2026-07",
        "schema_version": 1,
        "provider": "fmp",
        "dataset": "eod_prices",
        "mode": CollectionMode.INCREMENTAL,
        "requested_window_start": _WINDOW_START,
        "requested_window_end": _WINDOW_END,
        "parameters": {"adjustment": "unadjusted", "universe": "probe-basket"},
        "created_at_utc": _CREATED_AT,
    }
    values.update(overrides)
    return CollectionRunPlan(**values)


def test_plan_digest_is_deterministic_and_input_aware() -> None:
    plan = _plan()

    assert plan.plan_sha256 == collection_plan_digest(plan)
    assert _plan().plan_sha256 == plan.plan_sha256
    assert _plan(requested_window_end=datetime(2026, 7, 27, tzinfo=UTC)).plan_sha256 != (
        plan.plan_sha256
    )
    assert _plan(parameters={"adjustment": "split"}).plan_sha256 != plan.plan_sha256


def test_register_plan_persists_and_replays_idempotently(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    plan = _plan()

    collection_registry.register_plan(plan)
    collection_registry.register_plan(plan)

    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(collection_run_plans)) == 1
        row = (
            connection.execute(
                select(collection_run_plans).where(collection_run_plans.c.plan_id == plan.plan_id)
            )
            .mappings()
            .one()
        )
    assert row["mode"] == "incremental"
    assert row["plan_sha256"] == plan.plan_sha256
    assert row["requested_window_start"] == _WINDOW_START


def test_register_plan_rejects_different_projection_for_same_identity(
    collection_registry: CollectionRegistry,
) -> None:
    collection_registry.register_plan(_plan())

    with pytest.raises(CollectionConflictError):
        collection_registry.register_plan(_plan(dataset="eod_prices_full"))


def test_plan_sha256_cannot_be_reused_under_another_plan_id(
    collection_registry: CollectionRegistry,
) -> None:
    collection_registry.register_plan(_plan())

    with pytest.raises(CollectionConflictError):
        collection_registry.register_plan(_plan(plan_id="fmp-eod-incremental-copy"))


def test_mode_is_limited_to_probe_incremental_backfill() -> None:
    for mode in (CollectionMode.PROBE, CollectionMode.INCREMENTAL, CollectionMode.BACKFILL):
        assert _plan(mode=mode).mode is mode
    with pytest.raises(ValueError, match="mode"):
        _plan(mode="scheduled")


def test_requested_window_must_be_paired_ordered_and_timezone_aware() -> None:
    with pytest.raises(ValueError, match="window"):
        _plan(requested_window_end=None)
    with pytest.raises(ValueError, match="window"):
        _plan(requested_window_start=None)
    with pytest.raises(ValueError, match="window"):
        _plan(requested_window_start=_WINDOW_END, requested_window_end=_WINDOW_START)
    with pytest.raises(ValueError, match="timezone"):
        _plan(requested_window_start=_WINDOW_START.replace(tzinfo=None))


def test_credential_parameters_are_rejected() -> None:
    with pytest.raises(ValueError, match="credential"):
        _plan(parameters={"api_key": "secret"})
    with pytest.raises(ValueError, match="credential"):
        _plan(parameters={"nested": {"authorization": "Bearer x"}})
    with pytest.raises(ValueError, match="credential"):
        _plan(parameters={"endpoint": "https://user:pw@example.com/v1"})


def test_plan_identity_fields_must_be_nonempty() -> None:
    with pytest.raises(ValueError, match="plan_id"):
        _plan(plan_id="  ")
    with pytest.raises(ValueError, match="provider"):
        _plan(provider="")
    with pytest.raises(ValueError, match="schema_version"):
        _plan(schema_version=0)


def test_plan_parameters_must_be_a_json_object() -> None:
    with pytest.raises(TypeError, match="JSON object"):
        _plan(parameters=["not", "an", "object"])
