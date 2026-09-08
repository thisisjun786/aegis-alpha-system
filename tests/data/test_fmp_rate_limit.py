from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.data.fmp_rate_limit import (
    BANDWIDTH_CUTOFF_FRACTION,
    MAX_ATTEMPTS_PER_REQUEST,
    MAX_RETRY_AFTER_SECONDS,
    PACING_SAFETY_FACTOR,
    BudgetExhaustedError,
    EntitlementError,
    FailureClass,
    RateLimiter,
    RateResumeState,
    RetryCeilingError,
    TierArtifact,
    TrustedUsageSnapshot,
    UsageEvidenceUnavailableError,
    classify_failure,
    parse_retry_after,
    parse_tier_artifact,
    require_trusted_usage_baseline,
)

if TYPE_CHECKING:
    from collections.abc import Callable

RUN_SEED = 20260729
_SECONDS_PER_MINUTE = 60
_BUDGETED_CALLS = 3
_RETRY_AFTER_SECONDS = 9.0
_BACKOFF_ATTEMPT_2_MIN = 4.0
_BACKOFF_ATTEMPT_2_MAX = 5.0
_TIER_CALLS_PER_MINUTE = 750
_TIER_BANDWIDTH_GB = 25.0
_SAMPLE_BYTES = 2048
_RECURRING_TEST_CALLS = 3001
_RESTORED_CALLS = 5000
_RESTORED_BYTES = 123
_RESUME_DAILY_CAP = 2
_ROLLOVER_TOTAL_CALLS = 4
_USAGE_ONLY_REFRESH_CALLS = 2


class FakeClock:
    """A deterministic clock; tests never sleep for real."""

    def __init__(self) -> None:
        self.now = 0.0
        self.waits: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        if seconds < 0:
            raise AssertionError("sleep cannot be negative")
        self.waits.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _limiter(  # noqa: PLR0913 - explicit recovery inputs
    clock: FakeClock,
    *,
    tier: TierArtifact | None = None,
    max_calls: int | None = 50,
    usage_baseline: TrustedUsageSnapshot | None = None,
    daily_usage_refresh: Callable[[], TrustedUsageSnapshot] | None = None,
    utc_clock: Callable[[], datetime] | None = None,
) -> RateLimiter:
    return RateLimiter(
        tier=tier or TierArtifact(calls_per_minute=300, calls_per_day=None, bandwidth_gb_30d=None),
        max_calls=max_calls,
        clock=clock.time,
        sleep=clock.sleep,
        run_seed=RUN_SEED,
        usage_baseline=usage_baseline,
        daily_usage_refresh=daily_usage_refresh,
        utc_clock=utc_clock,
    )


def _trusted_snapshot(**overrides: object) -> TrustedUsageSnapshot:
    values: dict[str, object] = {
        "source": "aas-data-005-aggregation",
        "recorded_at_utc": "2026-07-29T00:00:00+00:00",
        "integrity_sha256": "a" * 64,
        "authority_verified": True,
        "calls_used_today": 0,
        "bytes_used_30d": 0,
    }
    values.update(overrides)
    return TrustedUsageSnapshot(**values)  # type: ignore[arg-type]


def test_minimum_interval_is_eighty_percent_of_the_plan_cap() -> None:
    tier = TierArtifact(calls_per_minute=300, calls_per_day=None, bandwidth_gb_30d=None)
    assert tier.minimum_interval_seconds == pytest.approx(60 / (PACING_SAFETY_FACTOR * 300))


def test_pacing_spaces_requests_and_never_bursts_within_any_60_second_window() -> None:
    clock = FakeClock()
    tier = TierArtifact(calls_per_minute=60, calls_per_day=None, bandwidth_gb_30d=None)
    limiter = _limiter(clock, tier=tier, max_calls=120)
    starts: list[float] = []
    for _ in range(40):
        limiter.before_request()
        starts.append(clock.now)
    interval = tier.minimum_interval_seconds
    for earlier, later in pairwise(starts):
        assert later - earlier == pytest.approx(interval)
    # No rolling 60-second window may exceed 80% of the per-minute cap.
    allowed = PACING_SAFETY_FACTOR * tier.calls_per_minute
    for index, start in enumerate(starts):
        in_window = sum(1 for value in starts[index:] if value - start < _SECONDS_PER_MINUTE)
        assert in_window <= allowed + 1


def test_pacing_does_not_sleep_when_enough_time_already_elapsed() -> None:
    clock = FakeClock()
    limiter = _limiter(clock)
    limiter.before_request()
    clock.advance(10.0)
    limiter.before_request()
    assert clock.waits == []


def test_every_attempt_including_retries_consumes_the_call_budget() -> None:
    clock = FakeClock()
    limiter = _limiter(clock, max_calls=_BUDGETED_CALLS)
    for _ in range(_BUDGETED_CALLS):
        limiter.before_request()
    assert limiter.calls_attempted == _BUDGETED_CALLS
    assert limiter.calls_remaining == 0
    with pytest.raises(BudgetExhaustedError, match="call budget"):
        limiter.before_request()


def test_recurring_limiter_has_no_run_cap_but_stays_below_provider_rate() -> None:
    clock = FakeClock()
    tier = TierArtifact(calls_per_minute=3000, calls_per_day=None, bandwidth_gb_30d=None)
    limiter = _limiter(clock, tier=tier, max_calls=None)
    starts: list[float] = []

    for _ in range(_RECURRING_TEST_CALLS):
        limiter.before_request()
        starts.append(clock.now)

    assert limiter.calls_remaining is None
    assert limiter.calls_attempted == _RECURRING_TEST_CALLS
    assert starts[-1] > _SECONDS_PER_MINUTE
    for index, start in enumerate(starts):
        in_window = sum(1 for value in starts[index:] if value - start < _SECONDS_PER_MINUTE)
        assert in_window < tier.calls_per_minute


def test_recurring_limiter_restores_usage_without_a_run_cap() -> None:
    clock = FakeClock()
    limiter = _limiter(clock, max_calls=None)
    limiter.restore_run_usage(
        RateResumeState(
            calls_attempted=_RESTORED_CALLS,
            bytes_received=_RESTORED_BYTES,
            rate_limited_attempts=1,
            retry_after_waits=2,
            jitter_draws=3,
        )
    )

    assert limiter.calls_attempted == _RESTORED_CALLS
    assert limiter.calls_remaining is None
    assert limiter.bytes_received == _RESTORED_BYTES


def test_resume_does_not_double_count_daily_calls_already_in_baseline() -> None:
    clock = FakeClock()
    limiter = _limiter(
        clock,
        tier=TierArtifact(
            calls_per_minute=300,
            calls_per_day=_RESUME_DAILY_CAP,
            bandwidth_gb_30d=None,
        ),
        max_calls=None,
        usage_baseline=_trusted_snapshot(calls_used_today=1),
    )
    limiter.restore_run_usage(
        RateResumeState(
            calls_attempted=1,
            bytes_received=0,
            rate_limited_attempts=0,
            retry_after_waits=0,
            jitter_draws=0,
        )
    )

    limiter.before_request()

    assert limiter.calls_attempted == _RESUME_DAILY_CAP
    with pytest.raises(BudgetExhaustedError, match="daily cap"):
        limiter.before_request()


def test_resume_does_not_double_count_bandwidth_already_in_baseline() -> None:
    clock = FakeClock()
    tier = TierArtifact(calls_per_minute=300, calls_per_day=None, bandwidth_gb_30d=0.000001)
    cutoff = tier.bandwidth_cutoff_bytes
    assert isinstance(cutoff, int)
    limiter = _limiter(
        clock,
        tier=tier,
        max_calls=None,
        usage_baseline=_trusted_snapshot(bytes_used_30d=cutoff - 1),
    )
    limiter.restore_run_usage(
        RateResumeState(
            calls_attempted=1,
            bytes_received=cutoff - 1,
            bandwidth_bytes_received=cutoff - 1,
            rate_limited_attempts=0,
            retry_after_waits=0,
            jitter_draws=0,
        )
    )

    limiter.before_request()

    assert limiter.calls_attempted == _RESUME_DAILY_CAP


def test_resume_uses_new_utc_day_baseline_instead_of_lifetime_calls() -> None:
    clock = FakeClock()
    limiter = _limiter(
        clock,
        tier=TierArtifact(
            calls_per_minute=300,
            calls_per_day=_RESUME_DAILY_CAP,
            bandwidth_gb_30d=None,
        ),
        max_calls=None,
        usage_baseline=_trusted_snapshot(calls_used_today=0),
    )
    limiter.restore_run_usage(
        RateResumeState(
            calls_attempted=_RESUME_DAILY_CAP,
            bytes_received=0,
            rate_limited_attempts=0,
            retry_after_waits=0,
            jitter_draws=0,
        )
    )

    limiter.before_request()
    limiter.before_request()

    assert limiter.calls_attempted == _ROLLOVER_TOTAL_CALLS
    with pytest.raises(BudgetExhaustedError, match="daily cap"):
        limiter.before_request()


def test_usage_only_refresh_does_not_require_a_new_run_identity() -> None:
    clock = FakeClock()
    start = datetime(2026, 8, 28, 23, 59, 59, tzinfo=UTC)
    run_ids: list[str] = ["keep"]

    def refresh_usage() -> TrustedUsageSnapshot:
        assert run_ids[-1] == "keep"
        return _trusted_snapshot(
            recorded_at_utc=datetime(2026, 8, 29, tzinfo=UTC).isoformat(),
            calls_used_today=0,
        )

    limiter = _limiter(
        clock,
        tier=TierArtifact(calls_per_minute=300, calls_per_day=2, bandwidth_gb_30d=None),
        max_calls=None,
        usage_baseline=_trusted_snapshot(recorded_at_utc=start.isoformat(), calls_used_today=1),
        daily_usage_refresh=refresh_usage,
        utc_clock=lambda: start + timedelta(seconds=clock.now),
    )
    limiter.before_request()
    clock.advance(2)
    limiter.before_request()
    assert limiter.calls_attempted == _USAGE_ONLY_REFRESH_CALLS
    assert run_ids == ["keep"]


def test_live_limiter_resets_daily_budget_at_utc_rollover() -> None:
    clock = FakeClock()
    refreshed_at: list[datetime] = []

    def refresh_usage() -> TrustedUsageSnapshot:
        moment = datetime(2026, 7, 30, tzinfo=UTC)
        refreshed_at.append(moment)
        return _trusted_snapshot(recorded_at_utc=moment.isoformat())

    limiter = _limiter(
        clock,
        tier=TierArtifact(calls_per_minute=300, calls_per_day=1, bandwidth_gb_30d=None),
        max_calls=None,
        usage_baseline=_trusted_snapshot(
            recorded_at_utc=datetime(2026, 7, 29, 23, 59, 59, tzinfo=UTC).isoformat()
        ),
        daily_usage_refresh=refresh_usage,
    )

    limiter.before_request()
    clock.advance(2)
    limiter.before_request()

    assert limiter.calls_attempted == _RESUME_DAILY_CAP
    assert refreshed_at == [datetime(2026, 7, 30, tzinfo=UTC)]
    with pytest.raises(BudgetExhaustedError, match="daily cap"):
        limiter.before_request()


def test_live_limiter_fails_closed_at_rollover_without_fresh_usage() -> None:
    clock = FakeClock()
    limiter = _limiter(
        clock,
        tier=TierArtifact(calls_per_minute=300, calls_per_day=1, bandwidth_gb_30d=None),
        max_calls=None,
        usage_baseline=_trusted_snapshot(
            recorded_at_utc=datetime(2026, 7, 29, 23, 59, 59, tzinfo=UTC).isoformat()
        ),
    )
    limiter.before_request()
    clock.advance(2)

    with pytest.raises(UsageEvidenceUnavailableError, match="refresh"):
        limiter.before_request()


def test_pacing_crossing_midnight_rechecks_the_new_daily_cap() -> None:
    clock = FakeClock()
    start = datetime(2026, 7, 29, 23, 59, 59, 900000, tzinfo=UTC)
    refreshed: list[TrustedUsageSnapshot] = []

    def refresh_usage() -> TrustedUsageSnapshot:
        snapshot = _trusted_snapshot(
            recorded_at_utc=(start + timedelta(seconds=1)).isoformat(),
            calls_used_today=1,
        )
        refreshed.append(snapshot)
        return snapshot

    limiter = _limiter(
        clock,
        tier=TierArtifact(calls_per_minute=300, calls_per_day=2, bandwidth_gb_30d=None),
        max_calls=None,
        usage_baseline=_trusted_snapshot(recorded_at_utc=start.isoformat()),
        daily_usage_refresh=refresh_usage,
    )

    limiter.before_request()
    limiter.before_request()

    assert len(refreshed) == 1
    with pytest.raises(BudgetExhaustedError, match="daily cap"):
        limiter.before_request()


def test_rollover_refresh_rechecks_approval_before_consuming() -> None:
    clock = FakeClock()
    start = datetime(2026, 7, 29, 23, 59, 59, 900000, tzinfo=UTC)
    expired = [False]
    approvals: list[bool] = []

    def refresh_usage() -> TrustedUsageSnapshot:
        expired[0] = True
        return _trusted_snapshot(recorded_at_utc=(start + timedelta(seconds=1)).isoformat())

    def require_approval() -> None:
        approvals.append(expired[0])
        if expired[0]:
            raise RuntimeError("authority expired during usage refresh")

    limiter = _limiter(
        clock,
        tier=TierArtifact(calls_per_minute=300, calls_per_day=2, bandwidth_gb_30d=None),
        max_calls=None,
        usage_baseline=_trusted_snapshot(recorded_at_utc=start.isoformat()),
        daily_usage_refresh=refresh_usage,
    )
    limiter.before_request(before_consume=require_approval)

    with pytest.raises(RuntimeError, match="expired during usage refresh"):
        limiter.before_request(before_consume=require_approval)

    assert approvals == [False, False, True]
    assert limiter.calls_attempted == 1


def test_resume_excludes_expired_bytes_from_rolling_cutoff() -> None:
    clock = FakeClock()
    tier = TierArtifact(calls_per_minute=300, calls_per_day=None, bandwidth_gb_30d=0.000001)
    cutoff = tier.bandwidth_cutoff_bytes
    assert cutoff is not None
    limiter = _limiter(clock, tier=tier, max_calls=None)
    limiter.restore_run_usage(
        RateResumeState(
            calls_attempted=1,
            bytes_received=cutoff,
            bandwidth_bytes_received=0,
            rate_limited_attempts=0,
            retry_after_waits=0,
            jitter_draws=0,
        )
    )

    limiter.before_request()

    assert limiter.ledger().bytes_received == cutoff


def test_daily_cap_refuses_before_the_request_and_stops_the_run() -> None:
    clock = FakeClock()
    tier = TierArtifact(calls_per_minute=300, calls_per_day=2, bandwidth_gb_30d=None)
    limiter = _limiter(clock, tier=tier, usage_baseline=_trusted_snapshot(calls_used_today=1))
    limiter.before_request()
    with pytest.raises(BudgetExhaustedError, match="daily cap"):
        limiter.before_request()


def test_historical_bandwidth_at_cutoff_refuses_before_the_first_request() -> None:
    clock = FakeClock()
    tier = TierArtifact(calls_per_minute=300, calls_per_day=None, bandwidth_gb_30d=0.000001)
    cutoff = tier.bandwidth_cutoff_bytes
    assert cutoff is not None
    limiter = _limiter(
        clock,
        tier=tier,
        usage_baseline=_trusted_snapshot(bytes_used_30d=cutoff),
    )
    with pytest.raises(BudgetExhaustedError, match="90% cutoff"):
        limiter.before_request()
    assert limiter.calls_attempted == 0


def test_bandwidth_cutoff_cancels_the_run_at_ninety_percent_mid_run() -> None:
    clock = FakeClock()
    tier = TierArtifact(calls_per_minute=300, calls_per_day=None, bandwidth_gb_30d=0.000001)
    cutoff = tier.bandwidth_cutoff_bytes
    assert cutoff == int(0.000001 * 1_000_000_000 * BANDWIDTH_CUTOFF_FRACTION)
    limiter = _limiter(clock, tier=tier, usage_baseline=_trusted_snapshot())
    limiter.before_request()
    limiter.after_response(byte_count=cutoff - 1)
    limiter.before_request()
    with pytest.raises(BudgetExhaustedError, match="90% cutoff"):
        limiter.after_response(byte_count=1)


def test_429_is_accounted_before_its_body_triggers_bandwidth_cutoff() -> None:
    clock = FakeClock()
    tier = TierArtifact(calls_per_minute=300, calls_per_day=None, bandwidth_gb_30d=0.000001)
    cutoff = tier.bandwidth_cutoff_bytes
    assert cutoff is not None
    limiter = _limiter(clock, tier=tier, usage_baseline=_trusted_snapshot())
    limiter.before_request()
    with pytest.raises(BudgetExhaustedError, match="90% cutoff"):
        limiter.after_response(byte_count=cutoff, failure=FailureClass.RATE_LIMITED)
    ledger = limiter.ledger()
    assert ledger.calls_attempted == 1
    assert ledger.bytes_received == cutoff
    assert ledger.rate_limited_attempts == 1


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"Retry-After": "7"}, 7),
        ({"retry-after": " 12 "}, 12),
        ({"Retry-After": "600"}, MAX_RETRY_AFTER_SECONDS),
        ({"Retry-After": "soon"}, None),
        ({"Retry-After": "-3"}, None),
        ({"Retry-After": "2026-07-29T00:00:00Z"}, None),
        ({}, None),
    ],
)
def test_retry_after_parsing(headers: dict[str, str], expected: int | None) -> None:
    assert parse_retry_after(headers) == expected


def test_valid_retry_after_waits_exactly_that_many_seconds() -> None:
    clock = FakeClock()
    limiter = _limiter(clock)
    limiter.after_response(byte_count=0, failure=FailureClass.RATE_LIMITED)
    waited = limiter.wait_before_retry(
        attempt_index=1,
        failure=FailureClass.RATE_LIMITED,
        headers={"Retry-After": "9"},
    )
    assert waited == _RETRY_AFTER_SECONDS
    assert clock.waits == [_RETRY_AFTER_SECONDS]
    assert limiter.ledger().retry_after_waits == 1
    assert limiter.ledger().rate_limited_attempts == 1


def test_malformed_retry_after_falls_back_to_seeded_exponential_jitter() -> None:
    clock = FakeClock()
    limiter = _limiter(clock)
    expected = random.Random(RUN_SEED)  # noqa: S311 - mirrors the recorded run seed
    attempt_indices = (1, 2, 3)
    for attempt_index in attempt_indices:
        limiter.after_response(byte_count=0, failure=FailureClass.RATE_LIMITED)
        waited = limiter.wait_before_retry(
            attempt_index=attempt_index,
            failure=FailureClass.RATE_LIMITED,
            headers={"Retry-After": "not-an-integer"},
        )
        assert waited == pytest.approx(float(2**attempt_index) + expected.random())
    assert limiter.run_seed == RUN_SEED
    assert limiter.ledger().rate_limited_attempts == len(attempt_indices)


def test_server_errors_use_the_same_backoff_policy() -> None:
    clock = FakeClock()
    limiter = _limiter(clock)
    waited = limiter.wait_before_retry(attempt_index=2, failure=FailureClass.SERVER_ERROR)
    assert _BACKOFF_ATTEMPT_2_MIN <= waited < _BACKOFF_ATTEMPT_2_MAX
    assert limiter.ledger().rate_limited_attempts == 0


def test_fifth_attempt_blocks_the_run() -> None:
    clock = FakeClock()
    limiter = _limiter(clock)
    for attempt_index in range(1, MAX_ATTEMPTS_PER_REQUEST):
        limiter.wait_before_retry(attempt_index=attempt_index, failure=FailureClass.SERVER_ERROR)
    with pytest.raises(RetryCeilingError, match="the run is blocked"):
        limiter.wait_before_retry(
            attempt_index=MAX_ATTEMPTS_PER_REQUEST,
            failure=FailureClass.SERVER_ERROR,
        )


def test_terminal_fifth_429_is_counted_before_the_retry_ceiling() -> None:
    clock = FakeClock()
    limiter = _limiter(clock, max_calls=MAX_ATTEMPTS_PER_REQUEST)
    for attempt_index in range(1, MAX_ATTEMPTS_PER_REQUEST + 1):
        limiter.before_request()
        limiter.after_response(byte_count=0, failure=FailureClass.RATE_LIMITED)
        if attempt_index < MAX_ATTEMPTS_PER_REQUEST:
            limiter.wait_before_retry(
                attempt_index=attempt_index,
                failure=FailureClass.RATE_LIMITED,
            )
        else:
            with pytest.raises(RetryCeilingError, match="the run is blocked"):
                limiter.wait_before_retry(
                    attempt_index=attempt_index,
                    failure=FailureClass.RATE_LIMITED,
                )
    ledger = limiter.ledger()
    assert ledger.calls_attempted == MAX_ATTEMPTS_PER_REQUEST
    assert ledger.rate_limited_attempts == MAX_ATTEMPTS_PER_REQUEST


def test_entitlement_failures_block_immediately_without_retrying() -> None:
    clock = FakeClock()
    limiter = _limiter(clock)
    with pytest.raises(EntitlementError, match="entitlement failure"):
        limiter.wait_before_retry(attempt_index=1, failure=FailureClass.ENTITLEMENT)
    assert clock.waits == []


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (429, FailureClass.RATE_LIMITED),
        (401, FailureClass.ENTITLEMENT),
        (403, FailureClass.ENTITLEMENT),
        (503, FailureClass.SERVER_ERROR),
        (None, FailureClass.TIMEOUT),
    ],
)
def test_failure_classification(status_code: int | None, expected: FailureClass) -> None:
    assert classify_failure(status_code) is expected


def test_live_execution_fails_closed_without_a_trusted_usage_baseline() -> None:
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        require_trusted_usage_baseline(None)


def test_a_usage_snapshot_without_verified_authority_is_rejected() -> None:
    with pytest.raises(UsageEvidenceUnavailableError, match="authority verification"):
        _trusted_snapshot(authority_verified=False)


def test_tier_artifact_parsing_and_rejections() -> None:
    tier = parse_tier_artifact(
        {"calls_per_minute": _TIER_CALLS_PER_MINUTE, "calls_per_day": None, "bandwidth_gb_30d": 25}
    )
    assert tier.calls_per_minute == _TIER_CALLS_PER_MINUTE
    assert tier.calls_per_day is None
    assert tier.bandwidth_gb_30d == _TIER_BANDWIDTH_GB
    with pytest.raises(ValueError, match="missing required fields"):
        parse_tier_artifact({"calls_per_minute": 10})
    with pytest.raises(ValueError, match="must be an integer"):
        parse_tier_artifact(
            {"calls_per_minute": "10", "calls_per_day": None, "bandwidth_gb_30d": None}
        )
    with pytest.raises(ValueError, match="positive"):
        parse_tier_artifact(
            {"calls_per_minute": 0, "calls_per_day": None, "bandwidth_gb_30d": None}
        )


def test_tier_artifact_rejects_unknown_fields() -> None:
    with pytest.raises(ValueError, match="unknown fields") as error:
        parse_tier_artifact(
            {
                "calls_per_minute": _TIER_CALLS_PER_MINUTE,
                "calls_per_day": None,
                "bandwidth_gb_30d": None,
                "credential": "must-not-appear-in-error",
            }
        )
    assert "must-not-appear-in-error" not in str(error.value)


def test_usage_ledger_projects_records_for_the_005_api() -> None:
    clock = FakeClock()
    limiter = _limiter(clock)
    limiter.before_request()
    limiter.after_response(byte_count=_SAMPLE_BYTES)
    metrics = {metric: quantity for metric, quantity, _unit in limiter.ledger().as_usage_records()}
    assert int(metrics["calls_attempted"]) == 1
    assert int(metrics["bytes_received"]) == _SAMPLE_BYTES


def test_max_calls_must_be_positive() -> None:
    clock = FakeClock()
    with pytest.raises(ValueError, match="positive integer"):
        _limiter(clock, max_calls=0)


def test_sleep_callable_is_injected_so_tests_never_wait() -> None:
    clock = FakeClock()
    sleeper: Callable[[float], None] = clock.sleep
    limiter = RateLimiter(
        tier=TierArtifact(calls_per_minute=1, calls_per_day=None, bandwidth_gb_30d=None),
        max_calls=2,
        clock=clock.time,
        sleep=sleeper,
        run_seed=RUN_SEED,
    )
    limiter.before_request()
    limiter.before_request()
    assert clock.waits == [pytest.approx(75.0)]
