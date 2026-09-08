"""Catalog registration is atomic, idempotent, and independent of live authority."""

from __future__ import annotations

# ruff: noqa: F811 -- imported pytest fixtures are consumed by their parameter names
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from sqlalchemy import func, select
from test_fmp_daily_budget import (  # noqa: F401 -- shared fixture
    NOW,
    SUCCESS_CALLS,
    DailyHarness,
    daily_harness,
)

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    RunEventType,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import (
    collection_run_events,
    collection_run_receipts,
    collection_usage_records,
    collection_watermarks,
)
from aegis_alpha.data import fmp_collector_command, fmp_daily_cli, fmp_deferred_transport
from aegis_alpha.data.fmp_catalog import (
    recover_completed_collections,
    register_completed_collection,
)
from aegis_alpha.data.fmp_catalog_io import FmpCatalogError
from aegis_alpha.data.fmp_catalog_records import source_id
from aegis_alpha.metadata.registry import MetadataRegistry
from aegis_alpha.metadata.schema import (
    dataset_artifacts,
    dataset_versions,
    source_snapshot_files,
    source_snapshots,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine, Table


def catalog_counts(engine: Engine) -> tuple[int, ...]:
    with engine.connect() as connection:
        return tuple(
            int(connection.scalar(select(func.count()).select_from(table)) or 0)
            for table in (
                source_snapshots,
                source_snapshot_files,
                dataset_versions,
                dataset_artifacts,
                collection_run_receipts,
            )
        )


def lifecycle(engine: Engine) -> list[tuple[str, list[object]]]:
    tables: tuple[Table, ...] = (
        collection_run_events,
        collection_usage_records,
        collection_watermarks,
    )
    with engine.connect() as connection:
        return [
            (
                table.name,
                list(connection.execute(select(table).order_by(*table.primary_key.columns))),
            )
            for table in tables
        ]


@pytest.fixture
def uncataloged(daily_harness: DailyHarness, monkeypatch: pytest.MonkeyPatch) -> str:
    def unavailable(*_args: object) -> None:
        raise RuntimeError("synthetic catalog outage")

    with monkeypatch.context() as patch:
        patch.setattr(fmp_collector_command, "register_completed_collection", unavailable)
        code, report = daily_harness.run(SUCCESS_CALLS)
    assert code == 1
    assert report["collection_status"] == "collection_succeeded"
    assert report["catalog_status"] == "catalog_pending"
    assert report["invocation_calls_attempted"] == SUCCESS_CALLS
    assert catalog_counts(daily_harness.engine) == (0, 0, 0, 0, 0)
    run_id = str(report["run_id"])
    state = CollectionRegistry(daily_harness.engine).current_run_state(run_id)
    assert state is not None
    assert state.state is RunEventType.RUN_SUCCEEDED
    return run_id


def test_success_registers_exact_artifacts_and_blocked_eligibility(
    daily_harness: DailyHarness,
) -> None:
    code, report = daily_harness.run(SUCCESS_CALLS)
    assert code == 0, report
    run_id = str(report["collection_run_id"])
    with daily_harness.engine.connect() as connection:
        source = connection.execute(select(source_snapshots)).mappings().one()
        assert source["snapshot_id"] == source_id(run_id)
        assert source["provider"] == "fmp"
        assert source["dataset"] == "fmp_daily_all"
        assert source["parameters_json"]["kind"] == "receipt-backed-run-aggregate"
        receipt = connection.execute(select(collection_run_receipts)).mappings().one()
        assert receipt["run_id"] == run_id
        assert receipt["source_snapshot_id"] == source["snapshot_id"]
        assert receipt["receipt_sha256"] == source["content_sha256"]
        datasets = connection.execute(select(dataset_versions)).mappings().all()
        assert {row["dataset_id"] for row in datasets} == {
            "fmp_profile",
            "fmp_price_eod_full",
            "fmp_price_eod_non_split_adjusted",
            "fmp_price_eod_dividend_adjusted",
        }
        for row in datasets:
            assert row["row_count"] == 1
            for flag in (
                "canonical_eligible",
                "backtest_eligible",
                "paper_eligible",
                "order_eligible",
            ):
                assert row[flag] is False
        artifacts = connection.execute(select(dataset_artifacts)).mappings().all()
        assert any(row["relative_path"].endswith("history-index.json") for row in artifacts)
        for row in artifacts:
            if row["relative_path"].endswith(".json"):
                assert row["row_count"] is None
    before = catalog_counts(daily_harness.engine), lifecycle(daily_harness.engine)
    result = register_completed_collection(
        daily_harness.engine, daily_harness.root / "raw", daily_harness.root / "normalized", run_id
    )
    assert result["status"] == "catalog_complete"
    assert (catalog_counts(daily_harness.engine), lifecycle(daily_harness.engine)) == before


def test_prior_day_recovery_needs_no_credentials_authority_or_transport(
    daily_harness: DailyHarness,
    uncataloged: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("catalog recovery must not reach authority or transport")

    monkeypatch.delenv("FMP_API_KEY", raising=False)
    monkeypatch.delenv("AAS_FMP_OWNER_APPROVAL_AUTHORITY_PATH", raising=False)
    monkeypatch.setattr(fmp_daily_cli, "_verified_authority", forbidden)
    monkeypatch.setattr(fmp_daily_cli, "authorize_recurring_operation", forbidden)
    monkeypatch.setattr(fmp_deferred_transport, "make_fmp_https_transport", forbidden)
    before = lifecycle(daily_harness.engine)
    calls = len(daily_harness.transport.calls)
    result = recover_completed_collections(
        daily_harness.engine, daily_harness.root / "raw", daily_harness.root / "normalized", limit=1
    )
    assert result["status"] == "catalog_complete", result
    assert result["recovered_run_ids"] == [uncataloged]
    assert result["provider_calls"] == 0
    assert len(daily_harness.transport.calls) == calls
    assert lifecycle(daily_harness.engine) == before
    assert json.loads(json.dumps(result)) == result
    again = recover_completed_collections(
        daily_harness.engine, daily_harness.root / "raw", daily_harness.root / "normalized"
    )
    assert again["recovered_run_ids"] == []


@pytest.mark.parametrize("stage", ["source", "dataset", "receipt"])
def test_registration_failure_rolls_back_every_catalog_row(
    daily_harness: DailyHarness,
    uncataloged: str,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    owner, method = {
        "source": (MetadataRegistry, "register_source_snapshot"),
        "dataset": (MetadataRegistry, "register_dataset"),
        "receipt": (CollectionRegistry, "record_receipt"),
    }[stage]
    original = getattr(owner, method)

    def failed(*args: object, **kwargs: object) -> None:
        original(*args, **kwargs)
        raise RuntimeError("synthetic post-insert failure")

    before = lifecycle(daily_harness.engine)
    with monkeypatch.context() as patch:
        patch.setattr(owner, method, failed)
        with pytest.raises(RuntimeError, match="post-insert"):
            register_completed_collection(
                daily_harness.engine,
                daily_harness.root / "raw",
                daily_harness.root / "normalized",
                uncataloged,
            )
    assert catalog_counts(daily_harness.engine) == (0, 0, 0, 0, 0)
    assert lifecycle(daily_harness.engine) == before
    assert recover_completed_collections(
        daily_harness.engine, daily_harness.root / "raw", daily_harness.root / "normalized"
    )["recovered_run_ids"] == [uncataloged]


def test_concurrent_registration_preserves_one_catalog_projection(
    daily_harness: DailyHarness,
    uncataloged: str,
) -> None:
    def register() -> dict[str, object]:
        return register_completed_collection(
            daily_harness.engine,
            daily_harness.root / "raw",
            daily_harness.root / "normalized",
            uncataloged,
        )

    before = lifecycle(daily_harness.engine)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(register) for _ in range(2)]
        results = [future.result(timeout=15) for future in futures]
    assert results[0] == results[1]
    assert catalog_counts(daily_harness.engine)[0] == 1
    assert catalog_counts(daily_harness.engine)[-1] == 1
    assert lifecycle(daily_harness.engine) == before


@pytest.mark.parametrize("limit", [0, -1, 1001, True])
def test_invalid_limit_refuses_before_database(limit: int, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="limit"):
        recover_completed_collections(
            cast("Engine", object()), tmp_path / "raw", tmp_path / "normalized", limit=limit
        )


def test_foreign_provider_success_cannot_enter_fmp_catalog(daily_harness: DailyHarness) -> None:
    registry = CollectionRegistry(daily_harness.engine)
    plan = CollectionRunPlan(
        plan_id="foreign-plan",
        schema_version=1,
        provider="sec",
        dataset="fmp_daily_all",
        mode=CollectionMode.PROBE,
        requested_window_start=None,
        requested_window_end=None,
        parameters={},
        created_at_utc=NOW,
    )
    registry.register_plan(plan)
    registry.start_run(CollectionRun("foreign-run", plan.plan_id, NOW))
    for event, attempt in (
        (RunEventType.ATTEMPT_STARTED, 1),
        (RunEventType.ATTEMPT_SUCCEEDED, 1),
        (RunEventType.RUN_SUCCEEDED, None),
    ):
        registry.append_event(CollectionRunEvent("foreign-run", event, NOW, attempt_number=attempt))
    with pytest.raises(FmpCatalogError, match="foreign"):
        register_completed_collection(
            daily_harness.engine,
            daily_harness.root / "raw",
            daily_harness.root / "normalized",
            "foreign-run",
        )
    assert catalog_counts(daily_harness.engine) == (0, 0, 0, 0, 0)
