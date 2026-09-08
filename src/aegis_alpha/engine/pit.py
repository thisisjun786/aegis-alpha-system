"""Point-in-time and stale-gate enforcement before any receipt exists."""

from __future__ import annotations

from datetime import date
from typing import assert_never

from aegis_alpha.engine.errors import (
    BlockReason,
    ObservationKind,
    ReplayBlockedError,
)
from aegis_alpha.engine.models import StaleGateSpec


def reject_as_of_mismatch(*, replay_as_of: date, fixture_as_of: date) -> None:
    if replay_as_of != fixture_as_of:
        raise ReplayBlockedError(
            BlockReason.AS_OF_MISMATCH,
            f"replay as_of {replay_as_of.isoformat()} != fixture {fixture_as_of.isoformat()}",
        )


def reject_post_cutoff(*, observation_date: date, as_of: date) -> None:
    if observation_date > as_of:
        raise ReplayBlockedError(
            BlockReason.POST_CUTOFF,
            f"observation {observation_date.isoformat()} is after as_of {as_of.isoformat()}",
        )


def reject_stale(
    *,
    observation_date: date,
    as_of: date,
    gates: StaleGateSpec,
    kind: ObservationKind,
) -> None:
    reject_post_cutoff(observation_date=observation_date, as_of=as_of)
    age = (as_of - observation_date).days
    match kind:
        case ObservationKind.PRICE:
            if age > gates.price_stale_after_days:
                raise ReplayBlockedError(
                    BlockReason.STALE_PRICE,
                    f"price observation is {age} days old",
                )
        case ObservationKind.MACRO:
            if age > gates.macro_stale_after_days:
                raise ReplayBlockedError(
                    BlockReason.STALE_MACRO,
                    f"macro observation is {age} days old",
                )
        case unreachable:
            assert_never(unreachable)
