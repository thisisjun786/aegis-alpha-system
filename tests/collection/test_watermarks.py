from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier

import pytest
from sqlalchemy import Engine, create_engine, func, select

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    RunEventType,
    WatermarkAdvance,
)
from aegis_alpha.collection.registry import (
    CollectionConflictError,
    CollectionRegistry,
    CollectionStateError,
)
from aegis_alpha.collection.schema import collection_watermarks

_CREATED_AT = datetime(2026, 7, 29, 10, 0, tzinfo=UTC)
_POSITION = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
_EXPECTED_SECOND_ADVANCE_SEQ = 2


def _plan(
    plan_id: str,
    *,
    provider: str = "fmp",
    dataset: str = "eod_prices",
) -> CollectionRunPlan:
    return CollectionRunPlan(
        plan_id=plan_id,
        schema_version=1,
        provider=provider,
        dataset=dataset,
        mode=CollectionMode.INCREMENTAL,
        requested_window_start=None,
        requested_window_end=None,
        parameters={},
        created_at_utc=_CREATED_AT,
    )


def _registered_run(
    collection_registry: CollectionRegistry,
    run_id: str,
    *,
    provider: str = "fmp",
    dataset: str = "eod_prices",
) -> CollectionRun:
    plan_id = _plan_id(provider, dataset)
    collection_registry.register_plan(_plan(plan_id, provider=provider, dataset=dataset))
    run = CollectionRun(run_id=run_id, plan_id=plan_id, created_at_utc=_CREATED_AT)
    collection_registry.start_run(run)
    return run


def _plan_id(provider: str, dataset: str) -> str:
    coordinate = f"{provider}\x00{dataset}".encode()
    return f"plan-{hashlib.sha256(coordinate).hexdigest()}"


def _succeeded_run(
    collection_registry: CollectionRegistry,
    run_id: str,
    *,
    provider: str = "fmp",
    dataset: str = "eod_prices",
) -> CollectionRun:
    run = _registered_run(
        collection_registry,
        run_id,
        provider=provider,
        dataset=dataset,
    )
    for event_type in (
        RunEventType.ATTEMPT_STARTED,
        RunEventType.ATTEMPT_SUCCEEDED,
        RunEventType.RUN_SUCCEEDED,
    ):
        collection_registry.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=event_type,
                occurred_at_utc=_CREATED_AT,
                attempt_number=1 if event_type is not RunEventType.RUN_SUCCEEDED else None,
            )
        )
    return run


def _advance(run_id: str, **overrides: object) -> WatermarkAdvance:
    values: dict[str, object] = {
        "provider": "fmp",
        "dataset": "eod_prices",
        "stream": "daily_bars",
        "run_id": run_id,
        "watermark_value": "2026-07-28",
        "watermark_position": _POSITION,
    }
    values.update(overrides)
    return WatermarkAdvance(**values)


def test_watermark_advance_persists_and_projects_current_position(
    collection_registry: CollectionRegistry,
) -> None:
    run = _succeeded_run(collection_registry, "run-wm-1")

    assert collection_registry.advance_watermark(_advance(run.run_id)) == 1

    current = collection_registry.latest_watermark("fmp", "eod_prices", "daily_bars")
    assert current is not None
    assert current.watermark_value == "2026-07-28"
    assert current.watermark_position == _POSITION
    assert current.run_id == run.run_id
    assert current.watermark_seq == 1


def test_watermark_advance_joins_caller_transaction_commit(
    collection_registry: CollectionRegistry,
) -> None:
    run = _succeeded_run(collection_registry, "run-wm-caller-commit")
    advance = _advance(run.run_id)

    with collection_registry.begin_registration() as connection:
        assert collection_registry.advance_watermark(advance, connection=connection) == 1

    assert collection_registry.latest_watermark("fmp", "eod_prices", "daily_bars") is not None


def test_watermark_advance_joins_caller_transaction_rollback(
    collection_registry: CollectionRegistry,
) -> None:
    run = _succeeded_run(collection_registry, "run-wm-caller-rollback")
    advance = _advance(run.run_id)

    def advance_then_fail() -> None:
        with collection_registry.begin_registration() as connection:
            assert collection_registry.advance_watermark(advance, connection=connection) == 1
            raise RuntimeError("rollback caller transaction")

    with pytest.raises(RuntimeError, match="rollback caller transaction"):
        advance_then_fail()

    assert collection_registry.latest_watermark("fmp", "eod_prices", "daily_bars") is None


def test_watermark_advance_rejects_connection_from_another_engine(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    run = _succeeded_run(collection_registry, "run-wm-cross-engine")
    other_engine = create_engine(
        clean_postgres.url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )
    try:
        with (
            other_engine.connect() as connection,
            pytest.raises(ValueError, match="single PostgreSQL engine"),
        ):
            collection_registry.advance_watermark(_advance(run.run_id), connection=connection)
    finally:
        other_engine.dispose()


def test_later_run_advances_the_stream_position(
    collection_registry: CollectionRegistry,
) -> None:
    first = _succeeded_run(collection_registry, "run-wm-1")
    second = _succeeded_run(collection_registry, "run-wm-2")
    assert first.plan_id == second.plan_id
    collection_registry.advance_watermark(_advance(first.run_id))

    later = _POSITION + timedelta(days=1)
    later = _POSITION + timedelta(days=1)
    second_seq = collection_registry.advance_watermark(
        _advance(second.run_id, watermark_value="2026-07-29", watermark_position=later)
    )
    assert second_seq == _EXPECTED_SECOND_ADVANCE_SEQ

    current = collection_registry.latest_watermark("fmp", "eod_prices", "daily_bars")
    assert current is not None
    assert current.run_id == second.run_id
    assert current.watermark_position == later


def test_watermark_regression_is_rejected(collection_registry: CollectionRegistry) -> None:
    first = _succeeded_run(collection_registry, "run-wm-1")
    second = _succeeded_run(collection_registry, "run-wm-2")
    third = _succeeded_run(collection_registry, "run-wm-3")
    collection_registry.advance_watermark(_advance(first.run_id))

    with pytest.raises(CollectionStateError):
        collection_registry.advance_watermark(_advance(second.run_id))
    with pytest.raises(CollectionStateError):
        collection_registry.advance_watermark(
            _advance(third.run_id, watermark_position=_POSITION - timedelta(days=1))
        )


def test_identical_advance_replay_is_idempotent(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    run = _succeeded_run(collection_registry, "run-wm-1")
    advance = _advance(run.run_id)

    assert collection_registry.advance_watermark(advance) == 1
    assert collection_registry.advance_watermark(advance) == 1

    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(collection_watermarks)) == 1


def test_same_run_cannot_change_its_advance_projection(
    collection_registry: CollectionRegistry,
) -> None:
    run = _succeeded_run(collection_registry, "run-wm-1")
    collection_registry.advance_watermark(_advance(run.run_id))

    with pytest.raises(CollectionConflictError):
        collection_registry.advance_watermark(
            _advance(run.run_id, watermark_value="2026-07-28-restatement")
        )


def test_watermark_advance_requires_a_registered_run(
    collection_registry: CollectionRegistry,
) -> None:
    with pytest.raises(ValueError, match="unknown run"):
        collection_registry.advance_watermark(_advance("run-missing"))


@pytest.mark.parametrize(
    "terminal_event",
    [RunEventType.RUN_FAILED, RunEventType.RUN_CANCELLED],
)
def test_watermark_advance_requires_a_successful_terminal_run(
    collection_registry: CollectionRegistry,
    terminal_event: RunEventType,
) -> None:
    run = _registered_run(collection_registry, f"run-wm-{terminal_event.value}")
    collection_registry.append_event(
        CollectionRunEvent(
            run_id=run.run_id,
            event_type=RunEventType.ATTEMPT_STARTED,
            occurred_at_utc=_CREATED_AT,
            attempt_number=1,
        )
    )
    if terminal_event is RunEventType.RUN_FAILED:
        collection_registry.append_event(
            CollectionRunEvent(
                run_id=run.run_id,
                event_type=RunEventType.ATTEMPT_FAILED,
                occurred_at_utc=_CREATED_AT,
                attempt_number=1,
                error_class="ProviderError",
            )
        )
    collection_registry.append_event(
        CollectionRunEvent(
            run_id=run.run_id,
            event_type=terminal_event,
            occurred_at_utc=_CREATED_AT,
            error_class="ProviderError" if terminal_event is RunEventType.RUN_FAILED else None,
        )
    )

    with pytest.raises(CollectionStateError, match="run_succeeded"):
        collection_registry.advance_watermark(_advance(run.run_id))


def test_watermark_advance_rejects_never_started_and_active_runs(
    collection_registry: CollectionRegistry,
) -> None:
    never_started = _registered_run(collection_registry, "run-wm-never-started")
    active = _registered_run(collection_registry, "run-wm-active")
    collection_registry.append_event(
        CollectionRunEvent(
            run_id=active.run_id,
            event_type=RunEventType.ATTEMPT_STARTED,
            occurred_at_utc=_CREATED_AT,
            attempt_number=1,
        )
    )

    for run in (never_started, active):
        with pytest.raises(CollectionStateError, match="run_succeeded"):
            collection_registry.advance_watermark(_advance(run.run_id))


def test_watermark_advance_requires_the_run_plan_provider_and_dataset(
    collection_registry: CollectionRegistry,
) -> None:
    run = _succeeded_run(
        collection_registry,
        "run-wm-lineage",
        provider="other-provider",
        dataset="other-dataset",
    )

    with pytest.raises(CollectionStateError, match="provider/dataset"):
        collection_registry.advance_watermark(_advance(run.run_id))


def test_watermark_record_validation() -> None:
    with pytest.raises(ValueError, match="stream"):
        _advance("run-x", stream=" ")
    with pytest.raises(ValueError, match="watermark_value"):
        _advance("run-x", watermark_value="")
    with pytest.raises(ValueError, match="timezone"):
        _advance("run-x", watermark_position=_POSITION.replace(tzinfo=None))


def test_streams_advance_independently(collection_registry: CollectionRegistry) -> None:
    first = _succeeded_run(collection_registry, "run-wm-1")
    second = _succeeded_run(collection_registry, "run-wm-2")
    collection_registry.advance_watermark(_advance(first.run_id))

    assert (
        collection_registry.advance_watermark(_advance(second.run_id, stream="corp_actions")) == 1
    )
    assert collection_registry.latest_watermark("fmp", "eod_prices", "corp_actions") is not None


def test_concurrent_advances_on_a_new_stream_yield_exactly_one_winner(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    first = _succeeded_run(collection_registry, "run-wm-race-1")
    second = _succeeded_run(collection_registry, "run-wm-race-2")
    ready = Barrier(2)

    def advance(pair: tuple[str, str]) -> int:
        run_id, marker = pair
        try:
            with collection_registry.begin_registration() as connection:
                ready.wait(timeout=10)
                return collection_registry.advance_watermark(
                    _advance(run_id, watermark_value=marker), connection=connection
                )
        except (CollectionConflictError, CollectionStateError):
            return -1

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = sorted(pool.map(advance, [(first.run_id, "a"), (second.run_id, "b")]))

    assert results == [-1, 1]
    with clean_postgres.connect() as connection:
        rows = connection.execute(select(collection_watermarks)).mappings().all()
    assert len(rows) == 1
    assert rows[0]["watermark_seq"] == 1
    assert rows[0]["watermark_value"] in {"a", "b"}
