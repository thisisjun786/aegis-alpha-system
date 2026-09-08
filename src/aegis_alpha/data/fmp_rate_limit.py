from __future__ import annotations

import random
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta

# Compatibility re-exports for the established public import surface.
from aegis_alpha.data.fmp_rate_types import (  # noqa: F401
    BANDWIDTH_CUTOFF_FRACTION,
    MAX_ATTEMPTS_PER_REQUEST,
    MAX_RETRY_AFTER_SECONDS,
    PACING_SAFETY_FACTOR,
    REQUEST_TIMEOUT_SECONDS,
    BudgetExhaustedError,
    EntitlementError,
    FailureClass,
    RateResumeState,
    RetryCeilingError,
    RetryObligation,
    TierArtifact,
    TrustedUsageSnapshot,
    UnexpectedStatusError,
    UsageEvidenceUnavailableError,
    UsageLedger,
    classify_failure,
    parse_retry_after,
    parse_tier_artifact,
    require_trusted_usage_baseline,
)


class RateLimiter:
    """Section 7 pacing, budget, and retry policy with an injectable clock."""

    def __init__(  # noqa: PLR0913 - each control is an explicit section 7 input
        self,
        *,
        tier: TierArtifact,
        max_calls: int | None,
        clock: Callable[[], float],
        sleep: Callable[[float], None],
        run_seed: int,
        usage_baseline: TrustedUsageSnapshot | None = None,
        utc_clock: Callable[[], datetime] | None = None,
        daily_usage_refresh: Callable[[], TrustedUsageSnapshot] | None = None,
        invocation_admission: Callable[[], None] | None = None,
    ) -> None:
        if max_calls is not None and max_calls < 1:
            raise ValueError("--max-calls must be a positive integer")
        self._tier = tier
        self._max_calls = max_calls
        self._clock = clock
        self._sleep = sleep
        self._run_seed = run_seed
        self._random = random.Random(run_seed)  # noqa: S311 - jitter, not cryptography
        self._baseline = usage_baseline
        self._daily_baseline_calls = (
            0 if usage_baseline is None else usage_baseline.calls_used_today
        )
        self._daily_budget_day = (
            None if usage_baseline is None else usage_baseline.recorded_at.date()
        )
        self._daily_clock_anchor = (
            None
            if usage_baseline is None or tier.calls_per_day is None
            else (usage_baseline.recorded_at, self._clock())
        )
        self._utc_clock = utc_clock
        self._daily_usage_refresh = daily_usage_refresh
        self._invocation_admission = invocation_admission
        self._calls_attempted = 0
        self._daily_calls_attempted = 0
        self._bytes_received = 0
        self._bandwidth_bytes_received = 0
        self._retry_after_waits = 0
        self._rate_limited_attempts = 0
        self._last_request_start: float | None = None

    @property
    def run_seed(self) -> int:
        return self._run_seed

    @property
    def calls_attempted(self) -> int:
        return self._calls_attempted

    @property
    def minimum_interval_seconds(self) -> float:
        return self._tier.minimum_interval_seconds

    @property
    def calls_remaining(self) -> int | None:
        if self._max_calls is None:
            return None
        return self._max_calls - self._calls_attempted

    @property
    def bytes_received(self) -> int:
        return self._bytes_received

    def restore_run_usage(self, state: RateResumeState) -> None:
        """Restore durable per-run usage and deterministic retry position."""

        bandwidth_bytes = (
            state.bytes_received
            if state.bandwidth_bytes_received is None
            else state.bandwidth_bytes_received
        )
        if (
            min(
                state.calls_attempted,
                state.bytes_received,
                bandwidth_bytes,
                state.rate_limited_attempts,
                state.retry_after_waits,
                state.jitter_draws,
            )
            < 0
        ):
            raise ValueError("restored usage cannot be negative")
        if (
            self._max_calls is not None and state.calls_attempted > self._max_calls
        ) or state.rate_limited_attempts > state.calls_attempted:
            raise ValueError("restored usage exceeds the run budget")
        self._calls_attempted = state.calls_attempted
        self._daily_calls_attempted = state.calls_attempted if self._baseline is None else 0
        self._bytes_received = state.bytes_received
        self._bandwidth_bytes_received = bandwidth_bytes if self._baseline is None else 0
        self._rate_limited_attempts = state.rate_limited_attempts
        self._retry_after_waits = state.retry_after_waits
        self._random = random.Random(self._run_seed)  # noqa: S311 - jitter, not cryptography
        for _ in range(state.jitter_draws):
            self._random.random()

    def ledger(self) -> UsageLedger:
        return UsageLedger(
            calls_attempted=self._calls_attempted,
            bytes_received=self._bytes_received,
            retry_after_waits=self._retry_after_waits,
            rate_limited_attempts=self._rate_limited_attempts,
        )

    def before_request(
        self,
        *,
        before_wait: Callable[[], None] | None = None,
        before_consume: Callable[[], None] | None = None,
    ) -> None:
        """Pace, run the final boundary, then consume one budgeted call slot."""

        self._refresh_daily_window()
        if self._max_calls is not None and self._calls_attempted >= self._max_calls:
            raise BudgetExhaustedError(f"call budget of {self._max_calls} attempts is exhausted")
        self._enforce_daily_cap()
        self._enforce_bandwidth_cutoff()
        self._pace(before_wait=before_wait)
        if before_consume is not None:
            before_consume()
        daily_refreshed = self._refresh_daily_window()
        self._enforce_daily_cap()
        self._enforce_bandwidth_cutoff()
        if daily_refreshed and before_consume is not None:
            before_consume()
        # Separate from restored run totals; all refusal gates precede this debit.
        if self._invocation_admission is not None:
            self._invocation_admission()
        self._calls_attempted += 1
        self._daily_calls_attempted += 1
        self._last_request_start = self._clock()

    def _enforce_daily_cap(self) -> None:
        if self._tier.calls_per_day is None:
            return
        if self._daily_baseline_calls + self._daily_calls_attempted >= self._tier.calls_per_day:
            raise BudgetExhaustedError(
                f"tier daily cap of {self._tier.calls_per_day} calls is reached"
            )

    def _refresh_daily_window(self) -> bool:
        if self._daily_budget_day is None or self._daily_clock_anchor is None:
            return False
        baseline_time, monotonic_start = self._daily_clock_anchor
        moment = (
            self._utc_clock()
            if self._utc_clock is not None
            else baseline_time + timedelta(seconds=self._clock() - monotonic_start)
        )
        if moment.date() > self._daily_budget_day:
            if self._daily_usage_refresh is None:
                raise UsageEvidenceUnavailableError(
                    "daily usage baseline refresh is required at UTC rollover"
                )
            baseline = self._daily_usage_refresh()
            if baseline.recorded_at.date() != moment.date():
                raise UsageEvidenceUnavailableError(
                    "daily usage baseline refresh did not cover the current UTC day"
                )
            self._baseline = baseline
            self._daily_budget_day = baseline.recorded_at.date()
            self._daily_baseline_calls = baseline.calls_used_today
            self._daily_calls_attempted = 0
            self._bandwidth_bytes_received = 0
            self._daily_clock_anchor = (baseline.recorded_at, self._clock())
            return True
        return False

    def _pace(self, *, before_wait: Callable[[], None] | None = None) -> None:
        if self._last_request_start is None:
            return
        elapsed = self._clock() - self._last_request_start
        remaining = self._tier.minimum_interval_seconds - elapsed
        if remaining > 0:
            if before_wait is not None:
                before_wait()
            self._sleep(remaining)

    def account_response(
        self,
        *,
        byte_count: int,
        failure: FailureClass | None = None,
    ) -> None:
        """Account a received response without changing its security disposition."""

        if byte_count < 0:
            raise ValueError("byte_count cannot be negative")
        if failure is FailureClass.RATE_LIMITED:
            self._rate_limited_attempts += 1
        self._bytes_received += byte_count
        self._bandwidth_bytes_received += byte_count

    def after_response(
        self,
        *,
        byte_count: int,
        failure: FailureClass | None = None,
    ) -> None:
        """Record one response's classification and bytes before enforcing the cutoff."""

        self.account_response(byte_count=byte_count, failure=failure)
        self._enforce_bandwidth_cutoff()

    def _enforce_bandwidth_cutoff(self) -> None:
        cutoff = self._tier.bandwidth_cutoff_bytes
        if cutoff is None:
            return
        baseline = 0 if self._baseline is None else self._baseline.bytes_used_30d
        if baseline + self._bandwidth_bytes_received >= cutoff:
            raise BudgetExhaustedError(
                "rolling 30-day bandwidth reached the 90% cutoff; the run is cancelled"
            )

    def retry_obligation(
        self,
        *,
        attempt_index: int,
        failure: FailureClass,
        headers: Mapping[str, str] | None = None,
    ) -> RetryObligation:
        """Create the exact retry delay before it crosses the durable boundary."""

        if failure is FailureClass.ENTITLEMENT:
            raise EntitlementError("provider returned an entitlement failure; the run is blocked")
        if attempt_index < 1:
            raise ValueError("attempt_index starts at 1")
        if attempt_index >= MAX_ATTEMPTS_PER_REQUEST:
            raise RetryCeilingError(
                f"request failed {MAX_ATTEMPTS_PER_REQUEST} times; the run is blocked"
            )
        retry_after = None if headers is None else parse_retry_after(headers)
        if failure is FailureClass.RATE_LIMITED and retry_after is not None:
            self._retry_after_waits += 1
            return RetryObligation(delay_seconds=float(retry_after), from_retry_after=True)
        return RetryObligation(
            delay_seconds=float(2**attempt_index) + self._random.random(),
            from_retry_after=False,
        )

    def apply_retry_obligation(
        self,
        obligation: RetryObligation,
        *,
        before_wait: Callable[[], None] | None = None,
    ) -> None:
        """Wait for one already-durable retry obligation."""

        if obligation.delay_seconds > 0 and before_wait is not None:
            before_wait()
        self._sleep(obligation.delay_seconds)

    def wait_before_retry(
        self,
        *,
        attempt_index: int,
        failure: FailureClass,
        headers: Mapping[str, str] | None = None,
    ) -> float:
        """Apply the section 7 backoff and return the exact waited seconds."""

        obligation = self.retry_obligation(
            attempt_index=attempt_index,
            failure=failure,
            headers=headers,
        )
        self.apply_retry_obligation(obligation)
        return obligation.delay_seconds
