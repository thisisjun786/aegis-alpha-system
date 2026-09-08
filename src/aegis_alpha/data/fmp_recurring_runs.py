"""Deterministic attempt selection for daily recurring FMP runs."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Final, Protocol

from sqlalchemy import create_engine

from aegis_alpha.collection.records import RunEventType
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.data.fmp_cli_artifacts import PreconditionError
from aegis_alpha.data.fmp_recurring_authority import VerifiedRecurringAuthority

_MAX_DAILY_ATTEMPTS: Final = 1024


class RunStateView(Protocol):
    @property
    def terminal(self) -> bool: ...

    @property
    def state(self) -> RunEventType | None: ...


@dataclass(frozen=True, slots=True)
class RecurringRunSelection:
    run_id: str
    attempt_index: int


def recurring_attempt_index(
    authority: VerifiedRecurringAuthority,
    command: str,
    service_day: date,
    run_id: str,
    shard: tuple[int, int] | None = None,
) -> int:
    for attempt_index in range(_MAX_DAILY_ATTEMPTS):
        if (
            authority.run_identity(command, service_day, attempt_index=attempt_index, shard=shard)
            == run_id
        ):
            return attempt_index
    raise PreconditionError("pending run identity is outside recurring attempts")


def select_recurring_run_identity(
    identity: Callable[[int], str],
    state_lookup: Callable[[str], RunStateView | None],
) -> RecurringRunSelection:
    """Reuse active/successful attempts and advance past terminal failures."""

    for attempt_index in range(_MAX_DAILY_ATTEMPTS):
        run_id = identity(attempt_index)
        state = state_lookup(run_id)
        if state is None or not state.terminal or state.state is RunEventType.RUN_SUCCEEDED:
            return RecurringRunSelection(run_id=run_id, attempt_index=attempt_index)
    raise PreconditionError("daily recurring attempts are exhausted")


def select_recurring_run_from_database(
    *,
    authority: VerifiedRecurringAuthority,
    command: str,
    service_day: date,
    database_url: str,
    shard: tuple[int, int] | None = None,
) -> RecurringRunSelection:
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        hide_parameters=True,
        connect_args={"connect_timeout": 5},
    )
    try:
        registry = CollectionRegistry(engine)
        return select_recurring_run_identity(
            lambda attempt_index: authority.run_identity(
                command, service_day, attempt_index=attempt_index, shard=shard
            ),
            registry.current_run_state,
        )
    finally:
        engine.dispose()
