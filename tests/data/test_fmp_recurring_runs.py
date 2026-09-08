from __future__ import annotations

from datetime import UTC, datetime

from aegis_alpha.collection.records import CollectionRunState, RunEventType
from aegis_alpha.data.fmp_recurring_runs import select_recurring_run_identity


def test_terminal_failure_advances_to_a_new_deterministic_attempt() -> None:
    identities = tuple(f"fmp-run-attempt-{index}" for index in range(3))

    def state(run_id: str) -> CollectionRunState | None:
        if run_id == identities[0]:
            return CollectionRunState(
                run_id=run_id,
                plan_id="plan-0",
                state=RunEventType.RUN_FAILED,
                terminal=True,
                attempt_count=1,
                last_event_seq=2,
                last_occurred_at_utc=datetime(2026, 8, 21, tzinfo=UTC),
            )
        return None

    selected = select_recurring_run_identity(lambda index: identities[index], state)

    assert selected.run_id == identities[1]
    assert selected.attempt_index == 1


def test_successful_or_incomplete_attempt_reuses_the_same_identity() -> None:
    identity = "fmp-run-attempt-0"
    for observed in (
        CollectionRunState(
            run_id=identity,
            plan_id="plan-0",
            state=RunEventType.ATTEMPT_STARTED,
            terminal=False,
            attempt_count=1,
            last_event_seq=1,
            last_occurred_at_utc=datetime(2026, 8, 21, tzinfo=UTC),
        ),
        CollectionRunState(
            run_id=identity,
            plan_id="plan-0",
            state=RunEventType.RUN_SUCCEEDED,
            terminal=True,
            attempt_count=1,
            last_event_seq=3,
            last_occurred_at_utc=datetime(2026, 8, 21, tzinfo=UTC),
        ),
    ):
        selected = select_recurring_run_identity(
            lambda _index: identity,
            lambda _run_id, observed=observed: observed,
        )
        assert selected.run_id == identity
        assert selected.attempt_index == 0
