"""Missing historical completion files cannot starve later catalog recovery."""

from __future__ import annotations

# ruff: noqa: F811 -- imported pytest fixtures are consumed by their parameter names
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from test_fmp_catalog import catalog_counts, lifecycle, uncataloged  # noqa: F401 -- shared fixture
from test_fmp_daily_budget import NOW, DailyHarness, daily_harness  # noqa: F401 -- shared fixture

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    RunEventType,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.data.fmp_catalog import recover_completed_collections
from aegis_alpha.data.fmp_catalog_io import FmpCatalogError


def _missing(
    harness: DailyHarness,
    run_id: str,
    created_at: datetime,
    *,
    provider: str = "fmp",
    dataset: str = "fmp_daily_all",
) -> None:
    registry = CollectionRegistry(harness.engine)
    plan = CollectionRunPlan(
        plan_id="plan-" + run_id,
        schema_version=1,
        provider=provider,
        dataset=dataset,
        mode=CollectionMode.PROBE,
        requested_window_start=None,
        requested_window_end=None,
        parameters={"cursor_fixture": run_id},
        created_at_utc=created_at,
    )
    registry.register_plan(plan)
    registry.start_run(CollectionRun(run_id, plan.plan_id, created_at))
    for event, attempt in (
        (RunEventType.ATTEMPT_STARTED, 1),
        (RunEventType.ATTEMPT_SUCCEEDED, 1),
        (RunEventType.RUN_SUCCEEDED, None),
    ):
        registry.append_event(CollectionRunEvent(run_id, event, created_at, attempt_number=attempt))


def _recover(harness: DailyHarness, limit: int, after: str | None = None) -> dict[str, object]:
    return recover_completed_collections(
        harness.engine,
        harness.root / "raw",
        harness.root / "normalized",
        limit=limit,
        after_run_id=after,
    )


@pytest.mark.parametrize("earlier_day", [False, True])
def test_limit_one_advances_past_missing_completion_to_valid_run(
    daily_harness: DailyHarness,
    uncataloged: str,
    *,
    earlier_day: bool,
) -> None:
    missing = "a-missing-completion"
    _missing(daily_harness, missing, NOW - timedelta(days=int(earlier_day)))
    before = lifecycle(daily_harness.engine)
    calls = len(daily_harness.transport.calls)
    first = _recover(daily_harness, 1)
    assert first["status"] == "catalog_pending"
    assert first["skipped_run_ids"] == [missing]
    assert first["recovered_run_ids"] == []
    assert first["next_after_run_id"] == missing
    assert first["has_more"] is True
    second = _recover(daily_harness, 1, missing)
    assert second["status"] == "catalog_complete", second
    assert second["recovered_run_ids"] == [uncataloged]
    assert second["next_after_run_id"] == uncataloged
    assert second["has_more"] is False
    assert second["provider_calls"] == 0
    assert lifecycle(daily_harness.engine) == before
    assert len(daily_harness.transport.calls) == calls
    # A cataloged successful run remains a valid cursor; an empty page is terminal.
    empty = _recover(daily_harness, 1, uncataloged)
    assert empty["recovered_run_ids"] == []
    assert empty["next_after_run_id"] == uncataloged
    assert empty["has_more"] is False


@pytest.mark.parametrize("cursor_kind", ["unknown", "foreign", "universe"])
def test_unknown_and_noncollection_cursors_refuse(
    daily_harness: DailyHarness,
    uncataloged: str,
    cursor_kind: str,
) -> None:
    del uncataloged
    cursor = "cursor-" + cursor_kind
    if cursor_kind == "foreign":
        _missing(daily_harness, cursor, NOW, provider="sec")
    elif cursor_kind == "universe":
        _missing(daily_harness, cursor, NOW, dataset="fmp_universe")
    before = lifecycle(daily_harness.engine)
    with pytest.raises(FmpCatalogError, match="existing FMP collection"):
        _recover(daily_harness, 1, cursor)
    assert lifecycle(daily_harness.engine) == before
    assert catalog_counts(daily_harness.engine) == (0, 0, 0, 0, 0)


@pytest.mark.parametrize("skip_first", [False, True])
def test_conflict_does_not_advance_cursor_past_failed_run(
    daily_harness: DailyHarness,
    uncataloged: str,
    *,
    skip_first: bool,
) -> None:
    missing = "a-missing-completion"
    if skip_first:
        _missing(daily_harness, missing, NOW)
    _missing(daily_harness, "later-missing-completion", NOW + timedelta(days=1))
    completion = json.loads(
        (daily_harness.root / "raw/fmp/runs" / uncataloged / "completion.json").read_bytes()
    )
    receipt = Path(completion["receipt_path"])
    original = receipt.read_bytes()
    receipt.write_bytes(original + b" ")
    before = lifecycle(daily_harness.engine)
    page = _recover(daily_harness, 3)
    assert page["status"] == "catalog_pending"
    assert page["recovered_run_ids"] == []
    assert page["failures"] == [{"run_id": uncataloged, "error_type": "FmpCatalogError"}]
    assert page["next_after_run_id"] == (missing if skip_first else None)
    assert page["has_more"] is True
    assert page["skipped_run_ids"] == ([missing] if skip_first else [])
    cursor = missing if skip_first else None
    retry = _recover(daily_harness, 1, cursor)
    assert retry["failures"] == page["failures"]
    assert retry["next_after_run_id"] == cursor
    assert retry["has_more"] is True
    receipt.write_bytes(original)
    repaired = _recover(daily_harness, 1, cursor)
    assert repaired["recovered_run_ids"] == [uncataloged]
    assert repaired["next_after_run_id"] == uncataloged
    assert repaired["has_more"] is True
    last = _recover(daily_harness, 1, uncataloged)
    assert last["skipped_run_ids"] == ["later-missing-completion"]
    assert last["has_more"] is False
    assert lifecycle(daily_harness.engine) == before
