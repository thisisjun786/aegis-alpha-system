from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import Engine, select

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    CollectionUsageRecord,
    RunEventType,
)
from aegis_alpha.collection.registry import CollectionConflictError, CollectionRegistry
from aegis_alpha.collection.schema import collection_run_plans

_CREATED_AT = datetime(2026, 7, 29, 10, 0, tzinfo=UTC)
_T0 = datetime(2026, 7, 29, 11, 0, tzinfo=UTC)


def _plan(**overrides: object) -> CollectionRunPlan:
    values: dict[str, object] = {
        "plan_id": "plan-hardening",
        "schema_version": 1,
        "provider": "fmp",
        "dataset": "eod_prices",
        "mode": CollectionMode.INCREMENTAL,
        "requested_window_start": None,
        "requested_window_end": None,
        "parameters": {"adjustment": "unadjusted"},
        "created_at_utc": _CREATED_AT,
    }
    values.update(overrides)
    return CollectionRunPlan(**values)


def _started_run(collection_registry: CollectionRegistry, run_id: str = "run-hardening") -> str:
    plan = _plan(plan_id=f"plan-{run_id}", dataset=f"eod_prices_{run_id}")
    collection_registry.register_plan(plan)
    collection_registry.start_run(
        CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=_CREATED_AT)
    )
    return run_id


def _failed_event(run_id: str, error_message: str) -> CollectionRunEvent:
    return CollectionRunEvent(
        run_id=run_id,
        event_type=RunEventType.ATTEMPT_FAILED,
        occurred_at_utc=_T0,
        attempt_number=1,
        error_class="ProviderError",
        error_message=error_message,
    )


def test_plan_with_nested_parameters_registers_deterministically(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    nested = {"window": {"preset": "month", "filters": {"exchanges": ["XNYS", "XNAS"]}}}
    plan = _plan(parameters=nested)

    assert plan.plan_sha256 == _plan(parameters=nested).plan_sha256
    collection_registry.register_plan(plan)
    collection_registry.register_plan(plan)

    with clean_postgres.connect() as connection:
        row = (
            connection.execute(
                select(collection_run_plans).where(collection_run_plans.c.plan_id == plan.plan_id)
            )
            .mappings()
            .one()
        )
    assert row["parameters_json"] == nested


def test_event_with_nested_details_appends(
    collection_registry: CollectionRegistry,
) -> None:
    run_id = _started_run(collection_registry)
    event = CollectionRunEvent(
        run_id=run_id,
        event_type=RunEventType.ATTEMPT_STARTED,
        occurred_at_utc=_T0,
        attempt_number=1,
        details={"request": {"page": {"number": 1, "size": 5000}}},
    )

    assert collection_registry.append_event(event) == 1


def test_usage_with_nested_evidence_records(
    collection_registry: CollectionRegistry,
) -> None:
    run_id = _started_run(collection_registry)
    collection_registry.record_usage(
        CollectionUsageRecord(
            run_id=run_id,
            usage_seq=1,
            metric="provider_requests",
            quantity=Decimal(3),
            unit="count",
            recorded_at_utc=_T0,
            evidence={"calls": [{"endpoint": "profile", "meta": {"status": 200}}]},
        )
    )


@pytest.mark.parametrize("key", ["private_key", "access_key", "PRIVATE-KEY"])
def test_credential_key_forms_are_rejected_in_parameters(key: str) -> None:
    with pytest.raises(ValueError, match="credential"):
        _plan(parameters={key: "x"})


def test_credential_keys_are_rejected_anywhere_in_nested_json() -> None:
    with pytest.raises(ValueError, match="credential"):
        _plan(parameters={"outer": {"private_key": "x"}})
    with pytest.raises(ValueError, match="credential"):
        _plan(parameters={"items": [{"access_key": "y"}]})


def test_bearer_and_wrapped_uri_values_are_rejected_in_json_fields() -> None:
    with pytest.raises(ValueError, match="credential"):
        _plan(parameters={"note": "Bearer abcdef1234567890"})
    with pytest.raises(ValueError, match="credential"):
        _plan(parameters={"ref": "url=https://user:pw@host.invalid/x"})
    with pytest.raises(ValueError, match="credential"):
        _plan(parameters={"ref": "https://host.invalid/x?access_token=abc"})


def test_error_message_is_scrubbed_for_credentials() -> None:
    with pytest.raises(ValueError, match="credential"):
        _failed_event("run-x", "GET https://user:pw@host.invalid/v1 failed")
    with pytest.raises(ValueError, match="credential"):
        _failed_event("run-x", "401 for Authorization: Bearer abcdef1234567890")

    clean = _failed_event("run-x", "provider returned HTTP 500 after 3 retries")
    assert clean.error_message is not None


def test_error_message_scrubbing_is_enforced_at_the_database_write(
    collection_registry: CollectionRegistry,
) -> None:
    run_id = _started_run(collection_registry)
    event = CollectionRunEvent(
        run_id=run_id,
        event_type=RunEventType.ATTEMPT_STARTED,
        occurred_at_utc=_T0,
        attempt_number=1,
    )
    collection_registry.append_event(event)
    failed = CollectionRunEvent(
        run_id=run_id,
        event_type=RunEventType.ATTEMPT_FAILED,
        occurred_at_utc=_T0 + timedelta(minutes=1),
        attempt_number=1,
        error_class="ProviderError",
        error_message="clean",
    )
    object.__setattr__(failed, "error_message", "GET https://user:pw@host.invalid/v1")

    with pytest.raises(ValueError, match="credential"):
        collection_registry.append_event(failed)


def test_register_plan_revalidates_and_verifies_digest_at_the_write_boundary(
    collection_registry: CollectionRegistry,
) -> None:
    plan = _plan()
    object.__setattr__(plan, "parameters", {"api_key": "stolen"})
    with pytest.raises(ValueError, match="credential"):
        collection_registry.register_plan(plan)

    tampered = _plan(plan_id="plan-tampered", dataset="eod_prices_tampered")
    object.__setattr__(tampered, "parameters", {"adjustment": "split-adjusted"})
    with pytest.raises(CollectionConflictError, match="digest"):
        collection_registry.register_plan(tampered)


def test_usage_quantity_must_fit_numeric_24_6() -> None:
    base: dict[str, object] = {
        "run_id": "run-x",
        "usage_seq": 1,
        "metric": "provider_cost",
        "unit": "USD",
        "recorded_at_utc": _T0,
        "evidence": {},
    }
    with pytest.raises(ValueError, match="NUMERIC"):
        CollectionUsageRecord(**{**base, "quantity": Decimal("0.0000004")})  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError, match="NUMERIC"):
        CollectionUsageRecord(**{**base, "quantity": Decimal(1234567890123456789)})  # ty: ignore[invalid-argument-type]

    exact = CollectionUsageRecord(**{**base, "quantity": Decimal("123456789012345678.999999")})  # ty: ignore[invalid-argument-type]
    assert exact.quantity == Decimal("123456789012345678.999999")


def test_plain_public_key_and_non_credential_values_still_pass() -> None:
    plan = _plan(parameters={"ref": "https://host.invalid/docs", "label": "bearer of notes"})
    assert plan.plan_id == "plan-hardening"
