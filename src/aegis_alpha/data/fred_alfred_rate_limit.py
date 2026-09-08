"""Client-side FRED/ALFRED pacing: 120/min hard cap and concurrency 1."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from threading import Lock
from typing import Final

CALLS_PER_MINUTE: Final = 120
_SECONDS_PER_MINUTE: Final = 60
MINIMUM_INTERVAL_SECONDS: Final = _SECONDS_PER_MINUTE / CALLS_PER_MINUTE


class BudgetExhaustedError(RuntimeError):
    """The run cancels at its --max-calls budget before another request."""


class ConcurrencyError(RuntimeError):
    """A second in-flight request would violate the concurrency-1 contract."""


@dataclass(frozen=True, slots=True)
class UsageLedger:
    calls_attempted: int
    bytes_received: int

    def as_usage_records(self) -> tuple[tuple[str, Decimal, str], ...]:
        return (
            ("calls_attempted", Decimal(self.calls_attempted), "call"),
            ("bytes_received", Decimal(self.bytes_received), "byte"),
        )


class RateLimiter:
    """Sequential 120/min limiter. ``--max-calls`` is required and consumed first."""

    def __init__(
        self,
        *,
        max_calls: int,
        clock: Callable[[], float],
        sleep: Callable[[float], None],
        calls_per_minute: int = CALLS_PER_MINUTE,
        revalidate: Callable[[], None] | None = None,
    ) -> None:
        if max_calls < 1:
            raise ValueError("--max-calls must be a positive integer")
        if type(calls_per_minute) is not int or not 1 <= calls_per_minute <= CALLS_PER_MINUTE:
            raise ValueError("signed calls_per_minute must be within the hard cap")
        self._interval = _SECONDS_PER_MINUTE / calls_per_minute
        self._revalidate = revalidate
        self._max_calls = max_calls
        self._clock = clock
        self._sleep = sleep
        self._calls_attempted = 0
        self._bytes_received = 0
        self._last_request_start: float | None = None
        self._in_flight = False
        self._gate = Lock()

    @property
    def calls_attempted(self) -> int:
        return self._calls_attempted

    @property
    def calls_remaining(self) -> int:
        return self._max_calls - self._calls_attempted

    def ledger(self) -> UsageLedger:
        return UsageLedger(
            calls_attempted=self._calls_attempted,
            bytes_received=self._bytes_received,
        )

    def before_request(self) -> None:
        """Consume one budgeted slot after pacing. Concurrency stays at 1."""

        with self._gate:
            if self._in_flight:
                raise ConcurrencyError("FRED/ALFRED collector concurrency is 1")
            if self._calls_attempted >= self._max_calls:
                raise BudgetExhaustedError(
                    f"call budget of {self._max_calls} attempts is exhausted"
                )
            if self._last_request_start is not None:
                elapsed = self._clock() - self._last_request_start
                remaining = self._interval - elapsed
                if remaining > 0:
                    self._sleep(remaining)
            if self._revalidate is not None:
                self._revalidate()
            self._calls_attempted += 1
            self._last_request_start = self._clock()
            self._in_flight = True

    def after_response(self, *, byte_count: int) -> None:
        if byte_count < 0:
            raise ValueError("byte_count cannot be negative")
        with self._gate:
            self._bytes_received += byte_count
            self._in_flight = False


__all__ = [
    "CALLS_PER_MINUTE",
    "MINIMUM_INTERVAL_SECONDS",
    "BudgetExhaustedError",
    "ConcurrencyError",
    "RateLimiter",
    "UsageLedger",
]
