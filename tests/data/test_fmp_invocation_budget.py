"""The invocation admission runs once, after every existing limiter gate."""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.data import fmp_daily_cli
from aegis_alpha.data.fmp_rate_limit import (
    BudgetExhaustedError,
    RateLimiter,
    RateResumeState,
    TierArtifact,
    TrustedUsageSnapshot,
)

if TYPE_CHECKING:
    from collections.abc import Callable

NOW = datetime(2026, 8, 21, 23, 59, tzinfo=UTC)
RESTORED_CALLS = 7


def _baseline(moment: datetime) -> TrustedUsageSnapshot:
    return TrustedUsageSnapshot(
        source="synthetic",
        recorded_at_utc=moment.isoformat(),
        integrity_sha256="a" * 64,
        authority_verified=True,
        calls_used_today=0,
        bytes_used_30d=0,
    )


@pytest.mark.parametrize("fail_guard", [0, 1, 2])
def test_admission_is_once_after_rollover_and_both_approval_checks(fail_guard: int) -> None:
    events: list[str] = []
    wall = [NOW]
    guards = 0

    def guard() -> None:
        nonlocal guards
        guards += 1
        events.append("approval")
        if guards == fail_guard:
            raise ValueError("synthetic authority refusal")
        wall[0] = NOW + timedelta(days=1)

    def refresh() -> TrustedUsageSnapshot:
        events.append("refresh")
        return _baseline(wall[0])

    limiter = RateLimiter(
        tier=TierArtifact(3000, 100, None),
        max_calls=None,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
        run_seed=1,
        usage_baseline=_baseline(NOW),
        utc_clock=lambda: wall[0],
        daily_usage_refresh=refresh,
        invocation_admission=lambda: events.append("invocation"),
    )
    if fail_guard:
        with pytest.raises(ValueError, match="authority refusal"):
            limiter.before_request(before_consume=guard)
        assert "invocation" not in events
        assert limiter.calls_attempted == 0
    else:
        limiter.before_request(before_consume=guard)
        assert events == ["approval", "refresh", "approval", "invocation"]
        assert limiter.calls_attempted == 1


def _limiter(admit: Callable[[], None], *, max_calls: int | None, daily: int) -> RateLimiter:
    return RateLimiter(
        tier=TierArtifact(3000, daily, None),
        max_calls=max_calls,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
        run_seed=1,
        invocation_admission=admit,
    )


@pytest.mark.parametrize(("run_cap", "daily_cap"), [(1, 100), (None, 1)])
def test_existing_run_and_day_limits_do_not_debit_invocation_on_refusal(
    run_cap: int | None,
    daily_cap: int,
) -> None:
    admitted: list[None] = []
    limiter = _limiter(lambda: admitted.append(None), max_calls=run_cap, daily=daily_cap)
    limiter.before_request()
    with pytest.raises(BudgetExhaustedError):
        limiter.before_request()
    assert len(admitted) == limiter.calls_attempted == 1


def test_restored_usage_does_not_consume_invocation_admission() -> None:
    budget = fmp_daily_cli._InvocationBudget(1)  # noqa: SLF001 -- inspect the actual invocation owner
    limiter = _limiter(budget.admit, max_calls=None, daily=100)
    limiter.restore_run_usage(
        RateResumeState(
            calls_attempted=RESTORED_CALLS,
            bytes_received=0,
            rate_limited_attempts=0,
            retry_after_waits=0,
            jitter_draws=0,
        )
    )
    assert budget.calls_attempted == 0
    limiter.before_request()
    with pytest.raises(BudgetExhaustedError, match="invocation call budget"):
        limiter.before_request()
    assert limiter.calls_attempted == RESTORED_CALLS + 1
    assert budget.calls_attempted == 1


@pytest.mark.parametrize(
    "extra",
    [
        ["--max-calls", "0"],
        ["--max-calls", "-1"],
        ["--max-calls", "1", "--shard-index", "1", "--shard-total", "2"],
        ["--max-calls", "1", "--shard-index", "1"],
        ["--max-calls", "1", "--revoke-authority"],
    ],
)
def test_invalid_invocation_budget_refuses_before_authority(
    extra: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args: object) -> None:
        pytest.fail("invalid budget must not reach authority or provider access")

    monkeypatch.setattr(fmp_daily_cli, "_verified_authority", forbidden)
    stderr = io.StringIO()
    code = fmp_daily_cli.main(
        [
            "--registry",
            "registry.json",
            "--storage-notification",
            "notification.json",
            "--tier",
            "tier.json",
            "--recurring-authority",
            "scope.json",
            "--recurring-authority-signature",
            "scope.sig",
            *extra,
        ],
        environ={},
        stderr=stderr,
    )
    assert code == 2  # noqa: PLR2004 -- CLI precondition exit
    report = json.loads(stderr.getvalue())
    assert report["invocation_calls_attempted"] == 0
    assert "--max-calls" in report["error"]
