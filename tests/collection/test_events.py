from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine, create_engine, func, select, text

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    RunEventType,
)
from aegis_alpha.collection.registry import (
    CollectionConflictError,
    CollectionRegistry,
    CollectionStateError,
)
from aegis_alpha.collection.schema import collection_run_events, collection_runs

_CREATED_AT = datetime(2026, 7, 29, 10, 0, tzinfo=UTC)
_T0 = datetime(2026, 7, 29, 11, 0, tzinfo=UTC)
_EXPECTED_LIFECYCLE_EVENTS = 5
_EXPECTED_LIFECYCLE_ATTEMPTS = 2


def _plan(
    plan_id: str = "fmp-eod-incremental-2026-07",
    dataset: str = "eod_prices",
) -> CollectionRunPlan:
    return CollectionRunPlan(
        plan_id=plan_id,
        schema_version=1,
        provider="fmp",
        dataset=dataset,
        mode=CollectionMode.INCREMENTAL,
        requested_window_start=datetime(2026, 7, 1, tzinfo=UTC),
        requested_window_end=datetime(2026, 7, 28, tzinfo=UTC),
        parameters={"adjustment": "unadjusted"},
        created_at_utc=_CREATED_AT,
    )


def _run(
    run_id: str = "run-2026-07-29-a", plan_id: str = "fmp-eod-incremental-2026-07"
) -> CollectionRun:
    return CollectionRun(run_id=run_id, plan_id=plan_id, created_at_utc=_CREATED_AT)


def _event(run_id: str, event_type: RunEventType, **overrides: object) -> CollectionRunEvent:
    values: dict[str, object] = {
        "run_id": run_id,
        "event_type": event_type,
        "occurred_at_utc": _T0,
        "details": {},
    }
    if event_type in {
        RunEventType.ATTEMPT_STARTED,
        RunEventType.ATTEMPT_SUCCEEDED,
        RunEventType.ATTEMPT_FAILED,
    }:
        values["attempt_number"] = 1
    if event_type in {RunEventType.ATTEMPT_FAILED, RunEventType.RUN_FAILED}:
        values["error_class"] = "ProviderTimeout"
        values["error_message"] = "provider did not respond"
    values.update(overrides)
    return CollectionRunEvent(**values)  # ty: ignore[invalid-argument-type]


def _started_run(collection_registry: CollectionRegistry) -> CollectionRun:
    collection_registry.register_plan(_plan())
    run = _run()
    collection_registry.start_run(run)
    return run


def test_start_run_persists_and_replays_idempotently(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    collection_registry.register_plan(_plan())

    collection_registry.start_run(_run())
    collection_registry.start_run(_run())

    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(collection_runs)) == 1


def test_start_run_rejects_different_projection_and_unknown_plan(
    collection_registry: CollectionRegistry,
) -> None:
    collection_registry.register_plan(_plan())
    collection_registry.register_plan(_plan("fmp-eod-backfill-2026", dataset="eod_prices_full"))
    collection_registry.start_run(_run())

    with pytest.raises(CollectionConflictError):
        collection_registry.start_run(_run(plan_id="fmp-eod-backfill-2026"))
    with pytest.raises(ValueError, match="unknown plan"):
        collection_registry.start_run(_run(run_id="run-orphan", plan_id="plan-missing"))


def test_full_lifecycle_with_retry_projects_current_state(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    run = _started_run(collection_registry)

    appended_seqs = [
        collection_registry.append_event(_event(run.run_id, RunEventType.ATTEMPT_STARTED)),
        collection_registry.append_event(
            _event(
                run.run_id, RunEventType.ATTEMPT_FAILED, occurred_at_utc=_T0 + timedelta(minutes=5)
            )
        ),
        collection_registry.append_event(
            _event(
                run.run_id,
                RunEventType.ATTEMPT_STARTED,
                attempt_number=2,
                retry_of_attempt=1,
                occurred_at_utc=_T0 + timedelta(minutes=6),
            )
        ),
        collection_registry.append_event(
            _event(
                run.run_id,
                RunEventType.ATTEMPT_SUCCEEDED,
                attempt_number=2,
                occurred_at_utc=_T0 + timedelta(minutes=9),
            )
        ),
        collection_registry.append_event(
            _event(
                run.run_id, RunEventType.RUN_SUCCEEDED, occurred_at_utc=_T0 + timedelta(minutes=10)
            )
        ),
    ]

    assert appended_seqs == [1, 2, 3, 4, 5]
    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(collection_run_events)) == (
            _EXPECTED_LIFECYCLE_EVENTS
        )
    state = collection_registry.current_run_state(run.run_id)
    assert state is not None
    assert state.state is RunEventType.RUN_SUCCEEDED
    assert state.terminal is True
    assert state.attempt_count == _EXPECTED_LIFECYCLE_ATTEMPTS
    assert state.last_event_seq == _EXPECTED_LIFECYCLE_EVENTS


def test_current_run_state_ignores_temporary_relation_shadow_in_pooled_session(
    clean_postgres: Engine,
) -> None:
    """The lifecycle projection must never resolve through a pooled temp schema."""

    isolated_engine = create_engine(
        clean_postgres.url,
        max_overflow=0,
        pool_pre_ping=True,
        pool_size=1,
        connect_args={"connect_timeout": 5},
    )
    try:
        registry = CollectionRegistry(isolated_engine)
        plan = _plan(plan_id="plan-run-state-shadow")
        run = _run(run_id="run-state-shadow", plan_id=plan.plan_id)
        registry.register_plan(plan)
        registry.start_run(run)

        with isolated_engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TEMPORARY TABLE collection_run_states ("
                    "run_id TEXT, plan_id TEXT, state TEXT, terminal BOOLEAN, "
                    "attempt_count INTEGER, last_event_seq INTEGER, "
                    "last_occurred_at_utc TIMESTAMPTZ)"
                )
            )
            connection.execute(
                text(
                    "INSERT INTO collection_run_states "
                    "(run_id, plan_id, state, terminal, attempt_count, last_event_seq, "
                    "last_occurred_at_utc) VALUES "
                    "(:run_id, 'shadow-plan', 'run_succeeded', true, 99, 99, :occurred_at)"
                ),
                {"run_id": run.run_id, "occurred_at": _T0},
            )

        state = registry.current_run_state(run.run_id)
    finally:
        isolated_engine.dispose()

    assert state is not None
    assert state.plan_id == plan.plan_id
    assert state.state is None
    assert not state.terminal
    assert state.attempt_count == 0
    assert state.last_event_seq is None


def test_first_event_must_start_attempt_one(collection_registry: CollectionRegistry) -> None:
    run = _started_run(collection_registry)

    with pytest.raises(CollectionStateError):
        collection_registry.append_event(_event(run.run_id, RunEventType.ATTEMPT_FAILED))
    with pytest.raises(CollectionStateError):
        collection_registry.append_event(
            _event(run.run_id, RunEventType.ATTEMPT_STARTED, attempt_number=2, retry_of_attempt=1)
        )


def test_terminal_states_reject_every_further_event(
    collection_registry: CollectionRegistry,
) -> None:
    run = _started_run(collection_registry)
    collection_registry.append_event(_event(run.run_id, RunEventType.ATTEMPT_STARTED))
    collection_registry.append_event(
        _event(run.run_id, RunEventType.RUN_CANCELLED, occurred_at_utc=_T0 + timedelta(minutes=1))
    )

    with pytest.raises(CollectionStateError):
        collection_registry.append_event(
            _event(run.run_id, RunEventType.ATTEMPT_SUCCEEDED, occurred_at_utc=_T0 + timedelta(2))
        )


def test_run_succeeded_requires_a_succeeded_attempt(
    collection_registry: CollectionRegistry,
) -> None:
    run = _started_run(collection_registry)
    collection_registry.append_event(_event(run.run_id, RunEventType.ATTEMPT_STARTED))
    collection_registry.append_event(
        _event(run.run_id, RunEventType.ATTEMPT_FAILED, occurred_at_utc=_T0 + timedelta(minutes=1))
    )

    with pytest.raises(CollectionStateError):
        collection_registry.append_event(
            _event(
                run.run_id,
                RunEventType.RUN_SUCCEEDED,
                occurred_at_utc=_T0 + timedelta(minutes=2),
            )
        )


def test_retry_requires_the_failed_attempt_it_names(
    collection_registry: CollectionRegistry,
) -> None:
    run = _started_run(collection_registry)
    collection_registry.append_event(_event(run.run_id, RunEventType.ATTEMPT_STARTED))
    collection_registry.append_event(
        _event(run.run_id, RunEventType.ATTEMPT_FAILED, occurred_at_utc=_T0 + timedelta(minutes=1))
    )

    with pytest.raises(CollectionStateError):
        collection_registry.append_event(
            _event(
                run.run_id,
                RunEventType.ATTEMPT_STARTED,
                attempt_number=2,
                retry_of_attempt=3,
                occurred_at_utc=_T0 + timedelta(minutes=2),
            )
        )
    with pytest.raises(CollectionStateError):
        collection_registry.append_event(
            _event(
                run.run_id,
                RunEventType.ATTEMPT_STARTED,
                attempt_number=2,
                occurred_at_utc=_T0 + timedelta(minutes=2),
            )
        )


def test_events_must_not_move_backwards_in_time(collection_registry: CollectionRegistry) -> None:
    run = _started_run(collection_registry)
    collection_registry.append_event(_event(run.run_id, RunEventType.ATTEMPT_STARTED))

    with pytest.raises(CollectionStateError):
        collection_registry.append_event(
            _event(
                run.run_id,
                RunEventType.ATTEMPT_FAILED,
                occurred_at_utc=_T0 - timedelta(minutes=1),
            )
        )


def test_identical_event_replay_is_idempotent(collection_registry: CollectionRegistry) -> None:
    run = _started_run(collection_registry)
    event = _event(run.run_id, RunEventType.ATTEMPT_STARTED, details={"trigger": "manual"})

    assert collection_registry.append_event(event) == 1
    assert collection_registry.append_event(event) == 1

    state = collection_registry.current_run_state(run.run_id)
    assert state is not None
    assert state.last_event_seq == 1
    assert state.attempt_count == 1


def test_failed_events_require_error_class_and_success_forbids_it() -> None:
    with pytest.raises(ValueError, match="error_class"):
        _event("run-x", RunEventType.ATTEMPT_FAILED, error_class=None, error_message=None)
    with pytest.raises(ValueError, match="error_class"):
        _event("run-x", RunEventType.ATTEMPT_SUCCEEDED, error_class="Boom")
    with pytest.raises(ValueError, match="error_class"):
        _event("run-x", RunEventType.RUN_FAILED, error_class="   ", error_message=None)


def test_event_record_requires_consistent_attempt_fields() -> None:
    with pytest.raises(ValueError, match="attempt_number"):
        _event("run-x", RunEventType.ATTEMPT_STARTED, attempt_number=None)
    with pytest.raises(ValueError, match="attempt_number"):
        _event("run-x", RunEventType.RUN_CANCELLED, attempt_number=1)
    with pytest.raises(ValueError, match="retry"):
        _event("run-x", RunEventType.ATTEMPT_SUCCEEDED, retry_of_attempt=1)
    with pytest.raises(ValueError, match="timezone"):
        _event("run-x", RunEventType.ATTEMPT_STARTED, occurred_at_utc=_T0.replace(tzinfo=None))
    with pytest.raises(ValueError, match="credential"):
        _event("run-x", RunEventType.ATTEMPT_STARTED, details={"token": "abc"})


def test_concurrent_first_attempts_yield_exactly_one_winner(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    run = _started_run(collection_registry)

    def append(worker: str) -> int:
        event = _event(run.run_id, RunEventType.ATTEMPT_STARTED, details={"worker": worker})
        try:
            return collection_registry.append_event(event)
        except CollectionStateError:
            return -1

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = sorted(pool.map(append, ["a", "b"]))

    assert results == [-1, 1]
    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(collection_run_events)) == 1
