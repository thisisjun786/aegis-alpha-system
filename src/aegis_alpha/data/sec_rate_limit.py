"""Client-side SEC fair-access pacing for AAS-DATA-013.

SEC publishes a 10 req/s fair-access cap. This client hard-caps at 8 req/s and
concurrency 1. Every live path requires a positive ``--max-calls`` budget.
The clock and sleeper are injectable so G-A tests never wait on wall time.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

MAX_REQUESTS_PER_SECOND = 8
MINIMUM_INTERVAL_SECONDS = 1.0 / MAX_REQUESTS_PER_SECOND
MAX_ATTEMPTS_PER_REQUEST = 5
MAX_BACKOFF_SECONDS = 30.0
HTTP_TOO_MANY_REQUESTS = 429
HTTP_FORBIDDEN = 403
HTTP_SERVER_ERROR_FLOOR = 500
HTTP_SERVER_ERROR_CEILING = 599


class BudgetExhaustedError(RuntimeError):
    """The ``--max-calls`` budget is spent; no further request may be issued."""


class RetryCeilingError(RuntimeError):
    """One request exhausted its retry attempts inside the call budget."""


class FailureClass(StrEnum):
    RATE_LIMITED = "rate_limited"
    FORBIDDEN = "forbidden"
    SERVER_ERROR = "server_error"
    TIMEOUT = "timeout"


def classify_failure(status_code: int | None) -> FailureClass:
    if status_code is None:
        return FailureClass.TIMEOUT
    if status_code == HTTP_TOO_MANY_REQUESTS:
        return FailureClass.RATE_LIMITED
    if status_code == HTTP_FORBIDDEN:
        return FailureClass.FORBIDDEN
    if HTTP_SERVER_ERROR_FLOOR <= status_code <= HTTP_SERVER_ERROR_CEILING:
        return FailureClass.SERVER_ERROR
    raise ValueError(f"status {status_code} is not a retryable SEC failure class")


def backoff_seconds(attempt_index: int) -> float:
    if attempt_index < 1:
        raise ValueError("attempt_index starts at 1")
    return min(float(2**attempt_index), MAX_BACKOFF_SECONDS)


@dataclass(frozen=True, slots=True)
class UsageLedger:
    calls_attempted: int
    bytes_received: int
    retries: int

    def as_usage_records(self) -> tuple[tuple[str, Decimal, str], ...]:
        return (
            ("calls_attempted", Decimal(self.calls_attempted), "call"),
            ("bytes_received", Decimal(self.bytes_received), "byte"),
            ("retries", Decimal(self.retries), "attempt"),
        )


class RateLimiter:
    """Sequential 8 req/s limiter. Concurrency is structurally 1."""

    def __init__(
        self,
        *,
        max_calls: int,
        clock: Callable[[], float],
        sleep: Callable[[float], None],
    ) -> None:
        if type(max_calls) is not int or max_calls < 1:
            raise ValueError("--max-calls must be a positive integer")
        self._max_calls = max_calls
        self._clock = clock
        self._sleep = sleep
        self._calls_attempted = 0
        self._bytes_received = 0
        self._retries = 0
        self._last_request_start: float | None = None

    @property
    def max_calls(self) -> int:
        return self._max_calls

    @property
    def calls_attempted(self) -> int:
        return self._calls_attempted

    @property
    def calls_remaining(self) -> int:
        return self._max_calls - self._calls_attempted

    @property
    def sleeper(self) -> Callable[[float], None]:
        return self._sleep

    def ledger(self) -> UsageLedger:
        return UsageLedger(
            calls_attempted=self._calls_attempted,
            bytes_received=self._bytes_received,
            retries=self._retries,
        )

    def before_request(self, *, revalidate: Callable[[], None] | None = None) -> None:
        if self._calls_attempted >= self._max_calls:
            raise BudgetExhaustedError(f"call budget of {self._max_calls} attempts is exhausted")
        self._pace()
        if revalidate is not None:
            revalidate()
        self._calls_attempted += 1
        self._last_request_start = self._clock()

    def after_response(self, *, byte_count: int) -> None:
        if byte_count < 0:
            raise ValueError("byte_count cannot be negative")
        self._bytes_received += byte_count

    def wait_before_retry(
        self,
        *,
        attempt_index: int,
        failure: FailureClass,
        headers: Mapping[str, str] | None = None,
    ) -> float:
        del headers
        if attempt_index < 1:
            raise ValueError("attempt_index starts at 1")
        if attempt_index >= MAX_ATTEMPTS_PER_REQUEST:
            raise RetryCeilingError(
                f"request failed {MAX_ATTEMPTS_PER_REQUEST} times; the run is blocked"
            )
        self._retries += 1
        delay = backoff_seconds(attempt_index)
        if failure is FailureClass.RATE_LIMITED:
            delay = max(delay, MINIMUM_INTERVAL_SECONDS)
        self._sleep(delay)
        return delay

    def _pace(self) -> None:
        if self._last_request_start is None:
            return
        elapsed = self._clock() - self._last_request_start
        remaining = MINIMUM_INTERVAL_SECONDS - elapsed
        if remaining > 0:
            self._sleep(remaining)
