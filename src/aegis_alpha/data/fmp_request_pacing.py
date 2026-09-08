"""Durable wall-clock deadlines bridged to monotonic in-process waiting."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta

from aegis_alpha.data.fmp_rate_limit import RateLimiter
from aegis_alpha.data.fmp_rate_types import RetryObligation


class RequestPacing:
    """Own the mutable restored deadline consumed before the next transport."""

    def __init__(self, clock: Callable[[], datetime], limiter: RateLimiter) -> None:
        self._clock: Callable[[], datetime] = clock
        self._limiter: RateLimiter = limiter
        self._restored_deadline: datetime | None = None

    def restore(self, deadline: datetime | None) -> None:
        if deadline is not None and (
            self._restored_deadline is None or deadline > self._restored_deadline
        ):
            self._restored_deadline = deadline

    def honor_restored_deadline(self, *, before_wait: Callable[[], None] | None = None) -> bool:
        """Wait for a positive restored deadline and report whether time advanced."""

        deadline = self._restored_deadline
        if deadline is None:
            return False
        remaining = (deadline - self._clock()).total_seconds()
        if remaining <= 0:
            self._restored_deadline = None
            return False
        if before_wait is not None:
            before_wait()
        self._restored_deadline = None
        self._limiter.apply_retry_obligation(RetryObligation(remaining, from_retry_after=False))
        return True

    def next_deadline(
        self, request_started_at: datetime, retry: RetryObligation | None
    ) -> datetime:
        pacing = request_started_at + timedelta(seconds=self._limiter.minimum_interval_seconds)
        if retry is None:
            return pacing
        return max(
            pacing,
            self._clock() + timedelta(seconds=retry.delay_seconds),
        )
