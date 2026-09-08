from __future__ import annotations

import pytest
from fred_alfred_collector_support import FakeClock

from aegis_alpha.data.fred_alfred_rate_limit import (
    CALLS_PER_MINUTE,
    MINIMUM_INTERVAL_SECONDS,
    BudgetExhaustedError,
    ConcurrencyError,
    RateLimiter,
)


def test_hard_cap_is_120_per_minute_with_concurrency_one() -> None:
    expected_calls_per_minute = 120
    expected_interval_seconds = 0.5
    assert expected_calls_per_minute == CALLS_PER_MINUTE
    assert expected_interval_seconds == MINIMUM_INTERVAL_SECONDS
    clock = FakeClock()
    limiter = RateLimiter(max_calls=3, clock=clock.time, sleep=clock.sleep)
    limiter.before_request()
    with pytest.raises(ConcurrencyError, match="concurrency is 1"):
        limiter.before_request()
    limiter.after_response(byte_count=10)
    limiter.before_request()
    limiter.after_response(byte_count=10)
    assert clock.waits == [0.5]
    limiter.before_request()
    limiter.after_response(byte_count=10)
    with pytest.raises(BudgetExhaustedError, match="call budget of 3"):
        limiter.before_request()


def test_max_calls_must_be_positive() -> None:
    with pytest.raises(ValueError, match="--max-calls must be a positive integer"):
        RateLimiter(max_calls=0, clock=lambda: 0.0, sleep=lambda _seconds: None)
