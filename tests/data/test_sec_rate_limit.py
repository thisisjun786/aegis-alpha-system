"""8 req/s guard and --max-calls budget on a fake clock. No wall-clock sleep."""

from __future__ import annotations

import pytest
from sec_collector_support import FakeClock

from aegis_alpha.data.sec_rate_limit import (
    HTTP_FORBIDDEN,
    HTTP_TOO_MANY_REQUESTS,
    MAX_REQUESTS_PER_SECOND,
    MINIMUM_INTERVAL_SECONDS,
    BudgetExhaustedError,
    FailureClass,
    RateLimiter,
    RetryCeilingError,
    backoff_seconds,
    classify_failure,
)

PAIR_CALLS = 2
HTTP_SERVER_UNAVAILABLE = 503


def _limiter(clock: FakeClock, *, max_calls: int = MAX_REQUESTS_PER_SECOND) -> RateLimiter:
    return RateLimiter(max_calls=max_calls, clock=clock.time, sleep=clock.sleep)


def test_second_request_waits_for_eight_per_second_interval() -> None:
    clock = FakeClock()
    limiter = _limiter(clock)

    limiter.before_request()
    limiter.before_request()

    assert MINIMUM_INTERVAL_SECONDS == 1.0 / MAX_REQUESTS_PER_SECOND
    assert clock.waits == [MINIMUM_INTERVAL_SECONDS]
    assert clock.seconds == pytest.approx(MINIMUM_INTERVAL_SECONDS)
    assert limiter.calls_attempted == PAIR_CALLS


def test_requests_spaced_at_the_cap_do_not_wait() -> None:
    clock = FakeClock()
    limiter = _limiter(clock)

    limiter.before_request()
    clock.seconds += MINIMUM_INTERVAL_SECONDS
    limiter.before_request()

    assert clock.waits == []
    assert limiter.calls_attempted == PAIR_CALLS


def test_max_calls_is_required_and_exhaustion_makes_zero_further_calls() -> None:
    with pytest.raises(ValueError, match="--max-calls"):
        RateLimiter(max_calls=0, clock=lambda: 0.0, sleep=lambda _s: None)

    clock = FakeClock()
    limiter = _limiter(clock, max_calls=1)
    limiter.before_request()

    with pytest.raises(BudgetExhaustedError, match="exhausted"):
        limiter.before_request()
    assert limiter.calls_attempted == 1


def test_retry_backoff_is_exponential_and_hits_the_ceiling() -> None:
    clock = FakeClock()
    limiter = _limiter(clock, max_calls=MAX_REQUESTS_PER_SECOND)

    waited = limiter.wait_before_retry(attempt_index=1, failure=FailureClass.FORBIDDEN)
    assert waited == backoff_seconds(1)
    assert clock.waits == [backoff_seconds(1)]

    with pytest.raises(RetryCeilingError):
        limiter.wait_before_retry(attempt_index=5, failure=FailureClass.RATE_LIMITED)


def test_classify_failure_covers_sec_retry_classes() -> None:
    assert classify_failure(HTTP_TOO_MANY_REQUESTS) is FailureClass.RATE_LIMITED
    assert classify_failure(HTTP_FORBIDDEN) is FailureClass.FORBIDDEN
    assert classify_failure(HTTP_SERVER_UNAVAILABLE) is FailureClass.SERVER_ERROR
    assert classify_failure(None) is FailureClass.TIMEOUT
