from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Final, assert_never, cast

#: Section 7: pacing never exceeds 80% of the plan's per-minute cap.
PACING_SAFETY_FACTOR: Final = 0.8
#: Section 7: at most 5 attempts per request, the 5th failure blocks the run.
MAX_ATTEMPTS_PER_REQUEST: Final = 5
#: Section 7: a valid ``Retry-After`` wait is capped at 60 seconds.
MAX_RETRY_AFTER_SECONDS: Final = 60
#: Section 7: the rolling bandwidth cutoff cancels the run at 90% of the cap.
BANDWIDTH_CUTOFF_FRACTION: Final = 0.9
#: Section 7: per-request timeout counted as an attempt.
REQUEST_TIMEOUT_SECONDS: Final = 30.0

_SECONDS_PER_MINUTE: Final = 60
_BYTES_PER_GB: Final = 1_000_000_000
_HTTP_SUCCESS_MIN: Final = 200
_HTTP_REDIRECT_MIN: Final = 300
_HTTP_TOO_MANY_REQUESTS: Final = 429
_HTTP_UNAUTHORIZED: Final = 401
_HTTP_FORBIDDEN: Final = 403
_HTTP_SERVER_ERROR_MIN: Final = 500
_HTTP_SERVER_ERROR_MAX: Final = 599


class BudgetExhaustedError(RuntimeError):
    """Guardrail G4: the run cancels at its call or bandwidth budget."""


class EntitlementError(RuntimeError):
    """Section 7: 401/403 blocks the run immediately with no retries."""


class RetryCeilingError(RuntimeError):
    """Section 7: the 5th failed attempt blocks the run."""


class UsageEvidenceUnavailableError(RuntimeError):
    """Sections 6.3/7: no trusted historical usage exists, so the run fails closed."""


@dataclass(frozen=True, slots=True)
class UnexpectedStatusError(RuntimeError):
    """A credential-free provider status has no accepted collector semantics."""

    status_code: int

    def __str__(self) -> str:
        return f"provider returned unsupported HTTP status {self.status_code}"


class AttemptRecordOutcome(StrEnum):
    CREDENTIAL_REJECTED_RESPONSE = "credential_rejected_response"
    NETWORK_ERROR = "network_error"
    RESPONSE = "response"
    TIMEOUT = "timeout"


class TerminalRequestDisposition(StrEnum):
    CREDENTIAL_REJECTED = "credential_rejected"
    ENTITLEMENT = "entitlement"
    RETRY_CEILING = "retry_ceiling"
    UNEXPECTED_STATUS = "unexpected_status"


@dataclass(frozen=True, slots=True)
class DurableRetry:
    request_fingerprint: str
    request_attempt_index: int
    retry_not_before_utc: datetime


@dataclass(frozen=True, slots=True)
class DurableTerminalRequest:
    request_fingerprint: str
    request_attempt_index: int
    disposition: TerminalRequestDisposition
    status_code: int | None = None


type DurableRequestState = DurableRetry | DurableTerminalRequest


def reconcile_request_states(
    attempts: Mapping[int, Mapping[str, object]],
) -> tuple[DurableRequestState, ...]:
    """Project each request's latest durable retry or terminal state."""

    states: dict[str, DurableRequestState] = {}
    for record in attempts.values():
        update_request_states(states, record)
    return tuple(states.values())


def update_request_states(
    states: dict[str, DurableRequestState],
    record: Mapping[str, object],
) -> None:
    """Apply one validated attempt to the unresolved request projection."""

    fingerprint = str(record["request_fingerprint"])
    states.pop(fingerprint, None)
    index = int(str(record["request_attempt_index"]))
    if "retry_not_before_utc" in record:
        states[fingerprint] = DurableRetry(
            request_fingerprint=fingerprint,
            request_attempt_index=index,
            retry_not_before_utc=datetime.fromisoformat(str(record["retry_not_before_utc"])),
        )
        return
    status = int(str(record.get("status_code", 0)))
    match AttemptRecordOutcome(str(record["outcome"])):
        case AttemptRecordOutcome.CREDENTIAL_REJECTED_RESPONSE:
            disposition = TerminalRequestDisposition.CREDENTIAL_REJECTED
        case AttemptRecordOutcome.NETWORK_ERROR | AttemptRecordOutcome.TIMEOUT:
            disposition = TerminalRequestDisposition.RETRY_CEILING
        case AttemptRecordOutcome.RESPONSE:
            disposition = _terminal_response_disposition(status)
        case unreachable:
            assert_never(unreachable)
    if disposition is not None:
        states[fingerprint] = DurableTerminalRequest(
            fingerprint, index, disposition, status or None
        )


def terminal_disposition(
    error: EntitlementError | RetryCeilingError | UnexpectedStatusError,
) -> TerminalRequestDisposition:
    """Map a typed terminal policy error to its durable disposition."""

    match error:
        case EntitlementError():
            return TerminalRequestDisposition.ENTITLEMENT
        case RetryCeilingError():
            return TerminalRequestDisposition.RETRY_CEILING
        case UnexpectedStatusError():
            return TerminalRequestDisposition.UNEXPECTED_STATUS
        case unreachable:
            assert_never(unreachable)


def _terminal_response_disposition(status: int) -> TerminalRequestDisposition | None:
    if status in {_HTTP_UNAUTHORIZED, _HTTP_FORBIDDEN}:
        return TerminalRequestDisposition.ENTITLEMENT
    if (
        status == _HTTP_TOO_MANY_REQUESTS
        or _HTTP_SERVER_ERROR_MIN <= status <= _HTTP_SERVER_ERROR_MAX
    ):
        return TerminalRequestDisposition.RETRY_CEILING
    if _HTTP_SUCCESS_MIN <= status < _HTTP_REDIRECT_MIN:
        return None
    return TerminalRequestDisposition.UNEXPECTED_STATUS


@dataclass(frozen=True, slots=True)
class TierArtifact:
    calls_per_minute: int
    calls_per_day: int | None
    bandwidth_gb_30d: float | None

    def __post_init__(self) -> None:
        if self.calls_per_minute < 1:
            raise ValueError("calls_per_minute must be a positive integer")
        if self.calls_per_day is not None and self.calls_per_day < 1:
            raise ValueError("calls_per_day must be a positive integer or null")
        if self.bandwidth_gb_30d is not None and self.bandwidth_gb_30d <= 0:
            raise ValueError("bandwidth_gb_30d must be a positive number or null")

    @property
    def minimum_interval_seconds(self) -> float:
        return _SECONDS_PER_MINUTE / (PACING_SAFETY_FACTOR * self.calls_per_minute)

    @property
    def bandwidth_cutoff_bytes(self) -> int | None:
        if self.bandwidth_gb_30d is None:
            return None
        return int(self.bandwidth_gb_30d * _BYTES_PER_GB * BANDWIDTH_CUTOFF_FRACTION)


def parse_tier_artifact(raw_document: object) -> TierArtifact:
    if not isinstance(raw_document, Mapping):
        # Artifact validation, not a call-site type bug.
        raise ValueError("tier artifact must be a JSON object")  # noqa: TRY004
    document = cast("Mapping[str, object]", raw_document)
    fields = {"calls_per_minute", "calls_per_day", "bandwidth_gb_30d"}
    missing = sorted(fields.difference(document))
    if missing:
        raise ValueError(f"tier artifact is missing required fields: {', '.join(missing)}")
    unknown = set(document).difference(fields)
    if unknown:
        raise ValueError("tier artifact contains unknown fields")
    calls_per_minute = document["calls_per_minute"]
    if not isinstance(calls_per_minute, int) or isinstance(calls_per_minute, bool):
        raise ValueError(  # noqa: TRY004 - artifact validation, not a call-site type bug
            "tier artifact 'calls_per_minute' must be an integer"
        )
    calls_per_day = document["calls_per_day"]
    if calls_per_day is not None and (
        not isinstance(calls_per_day, int) or isinstance(calls_per_day, bool)
    ):
        raise ValueError("tier artifact 'calls_per_day' must be an integer or null")
    bandwidth = document["bandwidth_gb_30d"]
    if bandwidth is not None and (
        not isinstance(bandwidth, (int, float)) or isinstance(bandwidth, bool)
    ):
        raise ValueError("tier artifact 'bandwidth_gb_30d' must be a number or null")
    return TierArtifact(
        calls_per_minute=calls_per_minute,
        calls_per_day=calls_per_day,
        bandwidth_gb_30d=None if bandwidth is None else float(bandwidth),
    )


@dataclass(frozen=True, slots=True)
class TrustedUsageSnapshot:
    """Section 6.3: the only accepted historical-usage baseline.

    004C never derives this from a caller-supplied ``--rolling-bytes-used``
    value, a dashboard transcription, or a ledger export. Until the AAS-DATA-005
    aggregation API exists, no trusted source can produce one, so every live
    execution fails closed before its first request.
    """

    source: str
    recorded_at_utc: str
    integrity_sha256: str
    authority_verified: bool
    calls_used_today: int
    bytes_used_30d: int

    def __post_init__(self) -> None:
        if not self.authority_verified:
            raise UsageEvidenceUnavailableError(
                "usage snapshot lacks separately-administered authority verification"
            )
        if self.calls_used_today < 0 or self.bytes_used_30d < 0:
            raise ValueError("usage quantities cannot be negative")
        if self.recorded_at.utcoffset() is None:
            raise ValueError("usage snapshot timestamp must be timezone-aware")

    @property
    def recorded_at(self) -> datetime:
        try:
            return datetime.fromisoformat(self.recorded_at_utc)
        except ValueError:
            raise ValueError("usage snapshot timestamp must be ISO-8601") from None


def require_trusted_usage_baseline(
    snapshot: TrustedUsageSnapshot | None,
) -> TrustedUsageSnapshot:
    """Fail closed before the first network call without trusted usage evidence."""

    if snapshot is None:
        raise UsageEvidenceUnavailableError(
            "live execution is BLOCKED_EXTERNAL: AAS-DATA-005 exposes no rolling usage "
            "aggregation and no separately-authorized usage snapshot exists"
        )
    return snapshot


class FailureClass(StrEnum):
    RATE_LIMITED = "rate_limited"
    SERVER_ERROR = "server_error"
    TIMEOUT = "timeout"
    ENTITLEMENT = "entitlement"


def classify_failure(status_code: int | None) -> FailureClass:
    if status_code is None:
        return FailureClass.TIMEOUT
    if status_code == _HTTP_TOO_MANY_REQUESTS:
        return FailureClass.RATE_LIMITED
    if status_code in {_HTTP_UNAUTHORIZED, _HTTP_FORBIDDEN}:
        return FailureClass.ENTITLEMENT
    if _HTTP_SERVER_ERROR_MIN <= status_code <= _HTTP_SERVER_ERROR_MAX:
        return FailureClass.SERVER_ERROR
    raise UnexpectedStatusError(status_code)


def parse_retry_after(headers: Mapping[str, str]) -> int | None:
    """Return a valid integer-seconds ``Retry-After`` capped at 60 seconds."""

    raw = next(
        (value for name, value in headers.items() if name.casefold() == "retry-after"),
        None,
    )
    if raw is None:
        return None
    try:
        seconds = int(raw.strip())
    except (AttributeError, ValueError):
        return None
    if seconds < 0:
        return None
    return min(seconds, MAX_RETRY_AFTER_SECONDS)


@dataclass(frozen=True, slots=True)
class RetryObligation:
    """One durable delay required before the next attempt of a request."""

    delay_seconds: float
    from_retry_after: bool


@dataclass(frozen=True, slots=True)
class RateResumeState:
    calls_attempted: int
    bytes_received: int
    rate_limited_attempts: int
    retry_after_waits: int
    jitter_draws: int
    bandwidth_bytes_received: int | None = None


@dataclass(frozen=True, slots=True)
class UsageLedger:
    calls_attempted: int
    bytes_received: int
    retry_after_waits: int
    rate_limited_attempts: int

    def as_usage_records(self) -> tuple[tuple[str, Decimal, str], ...]:
        """Project usage as ``(metric, quantity, unit)`` triples for 005."""

        return (
            ("calls_attempted", Decimal(self.calls_attempted), "call"),
            ("bytes_received", Decimal(self.bytes_received), "byte"),
            ("rate_limited_attempts", Decimal(self.rate_limited_attempts), "attempt"),
        )
