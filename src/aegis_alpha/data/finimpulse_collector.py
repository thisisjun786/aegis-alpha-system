"""AAS-DATA-008C FinImpulse raw + provider-normalized estimate collector.

Phase G-A is synthetic only: every code path here is exercised against
provider-neutral fixtures, and no module-level code opens a socket. The live
HTTPS transport exists so a separately gated G-B run can use it; nothing in
this module may be scheduled, promoted, or used to substitute a price source.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import secrets
import socket
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import select

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionReceipt,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    CollectionUsageRecord,
    RunEventType,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import collection_run_receipts, collection_usage_records
from aegis_alpha.data.contracts import SourceSnapshot, ValidationStatus
from aegis_alpha.data.raw_store import ContentAddressedRawStore
from aegis_alpha.identity.registry import IdentityAmbiguityError
from aegis_alpha.metadata.records import (
    SourceSnapshotFile,
    SourceSnapshotRegistration,
    source_tree_digest,
)
from aegis_alpha.metadata.registry import MetadataRegistry
from aegis_alpha.metadata.schema import source_snapshots

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from aegis_alpha.data.finimpulse_recurring_authority import VerifiedRecurringAuthority

TASK_ID = "AAS-DATA-008C"
PROVIDER = "finimpulse"
DATASET = "analysis/earnings:eps_trend+eps_revisions"
ENDPOINT = "https://api.finimpulse.com/v1/analysis/earnings"
CREDENTIAL_ENVIRONMENT_VARIABLE = "FINIMPULSE_API_TOKEN"
IDENTITY_NAMESPACE = "ticker"

SCHEMA_ID = "finimpulse_estimates_normalized"
SCHEMA_VERSION = 1
RECEIPT_VERSION = 2

ESTIMATE_TYPES = ("eps_trend", "eps_revisions")
DEFAULT_PAGE_LIMIT = 20
DEFAULT_CALL_PRICE_USD = Decimal("0.0008")
DEFAULT_ROW_PRICE_USD = Decimal("0.00015")
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_SECONDS = 0.5

HTTP_OK = 200
HTTP_TOO_MANY_REQUESTS = 429
HTTP_SERVER_ERROR_FLOOR = 500
PROVIDER_STATUS_OK = 20000
_MAX_BACKOFF_SECONDS = 30.0
_JITTER_RESOLUTION = 1000

ALLOWED_RESPONSE_HEADERS = (
    "content-type",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
)
SENSITIVE_KEY_PARTS = (
    "apikey",
    "authorization",
    "cookie",
    "credential",
    "password",
    "secret",
    "token",
)
SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")

TREND_VALUE_FIELDS = (
    "current",
    "seven_days_ago",
    "thirty_days_ago",
    "sixty_days_ago",
    "ninety_days_ago",
)
REVISION_VALUE_FIELDS = (
    "up_last7days",
    "up_last30days",
    "down_last7days",
    "down_last30days",
)
EXPECTED_TYPE_FIELDS: Mapping[str, tuple[str, ...]] = {
    "eps_trend": ("type", "date", "date_type", *TREND_VALUE_FIELDS),
    "eps_revisions": ("type", "date", "date_type", *REVISION_VALUE_FIELDS),
}
EXPECTED_DATE_TYPES = frozenset({"quarter", "year"})
#: Trend lookback fields and the age, in days, of the estimate they describe.
#: ``current`` is the zero-day reference point of the same series.
TREND_LOOKBACK_DAYS: Mapping[str, int] = {
    "current": 0,
    "seven_days_ago": 7,
    "thirty_days_ago": 30,
    "sixty_days_ago": 60,
    "ninety_days_ago": 90,
}
_DAY_TO_TREND_FIELD: Mapping[int, str] = {days: name for name, days in TREND_LOOKBACK_DAYS.items()}

NORMALIZED_COLUMNS: tuple[str, ...] = (
    "instrument_id",
    "provider_symbol",
    "snapshot_id",
    "observed_at",
    "estimate_type",
    "period_date",
    "date_type",
    *TREND_VALUE_FIELDS,
    *REVISION_VALUE_FIELDS,
    "source_receipt_sha256",
)
NORMALIZED_SCHEMA = pa.schema(
    [
        ("instrument_id", pa.string()),
        ("provider_symbol", pa.string()),
        ("snapshot_id", pa.string()),
        ("observed_at", pa.timestamp("us", tz="UTC")),
        ("estimate_type", pa.string()),
        ("period_date", pa.string()),
        ("date_type", pa.string()),
        ("current", pa.float64()),
        ("seven_days_ago", pa.float64()),
        ("thirty_days_ago", pa.float64()),
        ("sixty_days_ago", pa.float64()),
        ("ninety_days_ago", pa.float64()),
        ("up_last7days", pa.int64()),
        ("up_last30days", pa.int64()),
        ("down_last7days", pa.int64()),
        ("down_last30days", pa.int64()),
        ("source_receipt_sha256", pa.string()),
    ]
)


class CollectorError(Exception):
    """Fail-closed collector error that is safe to report without credentials."""


class BudgetError(CollectorError):
    """Client-side limit violation; no further provider call may be made."""


class ContractError(CollectorError):
    """Provider payload violated the frozen 008A/008C response contract."""


class PublicationError(CollectorError):
    """An immutable publication invariant would have been violated."""


class GateError(CollectorError):
    """A frozen G-B owner gate is missing, stale, negative, or ambiguous."""


class IdentityEvidenceError(CollectorError):
    """Required immutable 007 identity export evidence is absent or mismatched."""


class IdentityState(StrEnum):
    RESOLVED = "RESOLVED"
    UNRESOLVED = "UNRESOLVED"
    AMBIGUOUS = "AMBIGUOUS"


class RestatementClass(StrEnum):
    NEW_KEY = "NEW_KEY"
    MISSING_KEY = "MISSING_KEY"
    EXPECTED_CURRENT_REVISION = "EXPECTED_CURRENT_REVISION"
    EXPECTED_ROLLING_WINDOW = "EXPECTED_ROLLING_WINDOW"
    RESTATEMENT_LOOKBACK_INCONSISTENCY = "RESTATEMENT_LOOKBACK_INCONSISTENCY"
    RESTATEMENT_POST_ACTUALIZATION = "RESTATEMENT_POST_ACTUALIZATION"
    ANOMALY_DUPLICATE_ROW = "ANOMALY_DUPLICATE_ROW"


RESTATEMENT_CLASSES = frozenset(
    {
        RestatementClass.RESTATEMENT_LOOKBACK_INCONSISTENCY,
        RestatementClass.RESTATEMENT_POST_ACTUALIZATION,
        RestatementClass.ANOMALY_DUPLICATE_ROW,
    }
)


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    """Digest a file the caller supplied, without trusting any caller claim."""

    if not path.is_file():
        raise CollectorError(f"evidence artifact is absent: {path}")
    return sha256_hex(path.read_bytes())


def format_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")


def utc_now() -> datetime:
    return datetime.now(UTC)


def instant_literal(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_instant(literal: str) -> datetime:
    """Parse a receipt instant literal back into an aware UTC datetime."""

    parsed = datetime.fromisoformat(literal)
    if parsed.tzinfo is None:
        raise ValueError("receipt instants must carry an explicit offset")
    return parsed.astimezone(UTC)


def reject_credential_like(value: object) -> None:
    """Reject credential-like keys before anything is persisted or reported."""

    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
            if any(part in normalized for part in SENSITIVE_KEY_PARTS):
                raise ContractError(f"credential-like field is not persistable: {key}")
            reject_credential_like(item)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            reject_credential_like(item)


CREDENTIAL_REDACTION = b"[REDACTED_CREDENTIAL]"


def redact_credential(payload: bytes, credential: str) -> tuple[bytes, bool]:
    """Replace any occurrence of the credential before anything is persisted.

    Returns the safe bytes and whether a reflection was found. Plaintext
    credential material must never reach the raw store, a receipt, a log line,
    or an exception message.
    """

    if not credential:
        return payload, False
    secret = credential.encode()
    if secret not in payload:
        return payload, False
    return payload.replace(secret, CREDENTIAL_REDACTION), True


def validate_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(symbol.upper() for symbol in symbols)
    if not normalized:
        raise CollectorError("at least one symbol is required")
    if len(set(normalized)) != len(normalized):
        raise CollectorError("universe symbols must be unique")
    if any(SYMBOL_PATTERN.fullmatch(symbol) is None for symbol in normalized):
        raise CollectorError("symbols must use the credential-free ticker syntax")
    return normalized


def universe_sha256(symbols: Sequence[str]) -> str:
    return sha256_hex(canonical_json_bytes(list(symbols)))


#: Every G-B entry condition from section 1 of the 008C plan. A live run
#: requires an explicitly positive decision for each; anything else blocks.
REQUIRED_GB_GATES: tuple[str, ...] = (
    "probe_evidence_accepted",
    "disposition_008c_start",
    "external_raw_retention_permitted",
    "derived_storage_permitted",
    "purge_capability_available",
    "rate_limit_semantics_reconfirmed",
    "bounded_validation_budget_approved",
)
POSITIVE_GATE_DECISIONS = frozenset({"PERMITTED", "PROCEED", "PROCEED_WITH_CONDITIONS"})

#: Owner-authorized artifact digests, committed to this repository and changed
#: only through a separately reviewed owner PR. A live caller cannot supply or
#: override these values, so a locally fabricated gate artifact or identity
#: export can never authorize itself.
#:
#: Both sets are intentionally empty: G-B is blocked, so no owner has yet
#: authorized any artifact. Every live run therefore fails closed here until
#: the owner commits an exact digest.
AUTHORIZED_GATE_ARTIFACT_SHA256: frozenset[str] = frozenset()
AUTHORIZED_IDENTITY_EXPORT_SHA256: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class GateDecision:
    """One owner decision recorded in the immutable G-B gate artifact."""

    gate: str
    decision: str
    decided_at_utc: datetime
    evidence_ref: str

    @property
    def positive(self) -> bool:
        return self.decision in POSITIVE_GATE_DECISIONS

    def projection(self) -> dict[str, object]:
        return {
            "decided_at_utc": instant_literal(self.decided_at_utc),
            "decision": self.decision,
            "evidence_ref": self.evidence_ref,
            "gate": self.gate,
        }


@dataclass(frozen=True, slots=True)
class GateEvidence:
    """Hash-verified owner authorization for one bounded G-B validation run."""

    artifact_sha256: str
    decisions: tuple[GateDecision, ...]
    expires_at_utc: datetime
    universe_sha256: str
    budget_usd: Decimal

    def projection(self) -> dict[str, object]:
        return {
            "artifact_sha256": self.artifact_sha256,
            "budget_usd": format_decimal(self.budget_usd),
            "decisions": [decision.projection() for decision in self.decisions],
            "expires_at_utc": instant_literal(self.expires_at_utc),
            "universe_sha256": self.universe_sha256,
        }


def _verified_json_artifact(
    path: Path,
    *,
    expected_sha256: str,
    label: str,
    error: type[CollectorError],
) -> tuple[Mapping[str, object], str]:
    """Load a JSON artifact only when it hashes to the authorized digest."""

    if not expected_sha256:
        raise error(f"{label} has no owner-authorized digest")

    if not path.is_file():
        raise error(f"{label} is absent: {path}")
    payload = path.read_bytes()
    observed = sha256_hex(payload)
    if observed != expected_sha256:
        raise error(
            f"{label} does not match the authorized digest "
            f"(expected {expected_sha256}, observed {observed})"
        )
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise error(f"{label} is not valid JSON") from None
    if not isinstance(parsed, dict):
        raise error(f"{label} must be a JSON object")
    document = cast("Mapping[str, object]", parsed)
    reject_credential_like(document)
    return document, observed


class AuthorityReferenceError(CollectorError):
    """The independent owner authority reference is absent or does not authorize."""


def require_authorized_digest(
    digest: str,
    *,
    authorized: frozenset[str],
    label: str,
) -> str:
    """Resolve an artifact digest from the repository-committed owner authority.

    The live caller supplies a file, never the digest that makes it trusted.
    An artifact whose digest is not in the committed owner set is rejected, so
    fabricating a positive gate or an arbitrary mapping locally cannot
    self-authorize a paid run.
    """

    if not authorized:
        raise AuthorityReferenceError(
            f"no owner-authorized {label} digest is committed; G-B remains blocked"
        )
    if digest not in authorized:
        raise AuthorityReferenceError(f"{label} digest is not owner-authorized: {digest}")
    return digest


def authorized_gate_digest(observed_sha256: str) -> str:
    return require_authorized_digest(
        observed_sha256,
        authorized=AUTHORIZED_GATE_ARTIFACT_SHA256,
        label="G-B gate artifact",
    )


def authorized_identity_digest(observed_sha256: str) -> str:
    return require_authorized_digest(
        observed_sha256,
        authorized=AUTHORIZED_IDENTITY_EXPORT_SHA256,
        label="007 identity export",
    )


def _gate_decision(entry: object) -> GateDecision:
    if not isinstance(entry, Mapping):
        raise GateError("each G-B gate decision must be an object")
    typed = cast("Mapping[str, object]", entry)
    missing = [name for name in ("gate", "decision", "decided_at_utc") if name not in typed]
    if missing:
        raise GateError(f"G-B gate decision is missing fields: {sorted(missing)}")
    return GateDecision(
        gate=str(typed["gate"]),
        decision=str(typed["decision"]),
        decided_at_utc=parse_instant(str(typed["decided_at_utc"])),
        evidence_ref=str(typed.get("evidence_ref", "NOT_OBSERVED")),
    )


def load_gate_evidence(path: Path, *, expected_sha256: str) -> GateEvidence:
    """Load and hash-verify the immutable G-B gate artifact.

    The caller must supply the exact digest the owner authorized. A file whose
    content does not hash to that digest is rejected, so an edited or
    substituted artifact can never authorize a paid call.
    """

    document, observed = _verified_json_artifact(
        path,
        expected_sha256=expected_sha256,
        label="G-B gate evidence",
        error=GateError,
    )
    raw_decisions = document.get("decisions")
    if not isinstance(raw_decisions, list):
        raise GateError("G-B gate evidence must contain a decisions array")
    decisions = [_gate_decision(entry) for entry in raw_decisions]
    for name in ("expires_at_utc", "universe_sha256", "budget_usd"):
        if name not in document:
            raise GateError(f"G-B gate evidence is missing {name}")
    try:
        budget = Decimal(str(document["budget_usd"]))
    except InvalidOperation:
        raise GateError("G-B gate budget must be a decimal amount") from None
    return GateEvidence(
        artifact_sha256=observed,
        decisions=tuple(decisions),
        expires_at_utc=parse_instant(str(document["expires_at_utc"])),
        universe_sha256=str(document["universe_sha256"]),
        budget_usd=budget,
    )


def require_gb_gates(
    evidence: GateEvidence,
    *,
    config: CollectorConfig,
    now: datetime,
) -> None:
    """Fail closed unless every frozen G-B condition is explicitly positive.

    Missing, stale, negative, ambiguous, or duplicated decisions all block, and
    the authorization must bind the exact universe and client-side limit.
    """

    seen: dict[str, GateDecision] = {}
    for decision in evidence.decisions:
        if decision.gate in seen:
            raise GateError(f"G-B gate has conflicting decisions: {decision.gate}")
        seen[decision.gate] = decision
    missing = [gate for gate in REQUIRED_GB_GATES if gate not in seen]
    if missing:
        raise GateError(f"G-B gates are not decided: {sorted(missing)}")
    blocked = sorted(gate for gate in REQUIRED_GB_GATES if not seen[gate].positive)
    if blocked:
        raise GateError(f"G-B gates are not explicitly positive: {blocked}")
    if evidence.expires_at_utc <= now:
        raise GateError("G-B gate authorization has expired")
    if evidence.universe_sha256 != universe_sha256(config.universe):
        raise GateError("G-B gate authorization does not cover this universe")
    if config.budget_usd > evidence.budget_usd:
        raise GateError("client-side limit exceeds the approved bounded-validation budget")


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    """Frozen, hash-recorded collection inputs for one snapshot."""

    universe: tuple[str, ...]
    budget_usd: Decimal
    page_limit: int = DEFAULT_PAGE_LIMIT
    call_price_usd: Decimal = DEFAULT_CALL_PRICE_USD
    row_price_usd: Decimal = DEFAULT_ROW_PRICE_USD
    max_retries: int = DEFAULT_MAX_RETRIES
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS
    collector_code_version: str = "NOT_OBSERVED"
    identity_export_sha256: str = "NOT_OBSERVED"
    identity_as_of: datetime | None = None
    predecessor_snapshot_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "universe", validate_symbols(self.universe))
        for name in ("budget_usd", "call_price_usd", "row_price_usd"):
            amount = getattr(self, name)
            if not isinstance(amount, Decimal):
                raise TypeError(f"{name} must be a Decimal")
            if not amount.is_finite() or amount < 0:
                raise ValueError(f"{name} must be a finite nonnegative Decimal")
        if self.page_limit < 1:
            raise ValueError("page_limit must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if self.backoff_seconds < 0:
            raise ValueError("backoff_seconds cannot be negative")
        if self.identity_as_of is not None and self.identity_as_of.tzinfo is None:
            raise ValueError("identity_as_of must be timezone-aware")

    @property
    def call_reservation_usd(self) -> Decimal:
        """Maximum charge one honored call can incur: call price plus limit rows."""

        return self.call_price_usd + self.page_limit * self.row_price_usd

    @property
    def planned_reservation_usd(self) -> Decimal:
        return len(self.universe) * self.call_reservation_usd

    def digest(self) -> str:
        identity_as_of = (
            None if self.identity_as_of is None else instant_literal(self.identity_as_of)
        )
        return sha256_hex(
            canonical_json_bytes(
                {
                    "backoff_seconds": self.backoff_seconds,
                    "budget_usd": format_decimal(self.budget_usd),
                    "call_price_usd": format_decimal(self.call_price_usd),
                    "identity_as_of": identity_as_of,
                    "identity_export_sha256": self.identity_export_sha256,
                    "max_retries": self.max_retries,
                    "page_limit": self.page_limit,
                    "predecessor_snapshot_id": self.predecessor_snapshot_id,
                    "row_price_usd": format_decimal(self.row_price_usd),
                    "schema_version": SCHEMA_VERSION,
                    "types": list(ESTIMATE_TYPES),
                    "universe": list(self.universe),
                    "universe_sha256": universe_sha256(self.universe),
                }
            )
        )


def request_body(symbol: str, config: CollectorConfig) -> dict[str, object]:
    return {
        "symbol": symbol,
        "types": list(ESTIMATE_TYPES),
        "limit": config.page_limit,
        "offset": 0,
        "sort_by": [{"selector": "date", "desc": True}],
    }


@dataclass(frozen=True, slots=True)
class RawCall:
    """One immutable captured call: request projection plus raw response bytes."""

    symbol: str
    request_body: Mapping[str, object]
    requested_at_utc: datetime
    retrieved_at_utc: datetime
    http_status: int
    headers: Mapping[str, str]
    body_bytes: bytes

    @property
    def request_body_sha256(self) -> str:
        return sha256_hex(canonical_json_bytes(dict(self.request_body)))

    @property
    def body_sha256(self) -> str:
        return sha256_hex(self.body_bytes)

    @property
    def byte_length(self) -> int:
        return len(self.body_bytes)


class Transport(Protocol):
    """One credential-bearing provider call; never invoked by G-A tests."""

    def __call__(self, body: Mapping[str, object], credential: str) -> RawCall: ...


class RateLimiter(Protocol):
    """Signed per-minute pacing enforced immediately before each outbound call."""

    def before_request(self) -> None: ...


class StandingRateLimiter:
    """Sequential limiter bound to one signed ``calls_per_minute`` grant."""

    def __init__(
        self,
        *,
        calls_per_minute: int,
        clock: Callable[[], float],
        sleep: Callable[[float], None],
    ) -> None:
        if type(calls_per_minute) is not int or calls_per_minute < 1:
            raise ValueError("calls_per_minute must be a positive integer")
        self._interval_seconds = 60.0 / calls_per_minute
        self._clock = clock
        self._sleep = sleep
        self._last_request_start: float | None = None
        self.calls_attempted = 0
        self.waits: list[float] = []

    def before_request(self) -> None:
        if self._last_request_start is not None:
            remaining = self._interval_seconds - (self._clock() - self._last_request_start)
            if remaining > 0:
                self.waits.append(remaining)
                self._sleep(remaining)
        self.calls_attempted += 1
        self._last_request_start = self._clock()


#: Module-private construction token. A production caller cannot obtain this
#: without reaching into module internals, so `SyntheticExecution` cannot be
#: forged from ordinary application code.
_SYNTHETIC_TOKEN = object()


class SocketAccessError(CollectorError):
    """A non-live transport attempted network access."""


@dataclass(frozen=True, slots=True)
class SyntheticExecution:
    """Capability proving a run uses a specific offline transport.

    This is not a free-standing flag. It is issued only by
    :class:`SyntheticTransport`, and it names the exact transport instance it
    was issued for. :func:`collect_snapshot` requires that the capability, the
    supplied transport, and the offline transport type all agree, so pairing a
    fabricated capability with :func:`make_https_transport` cannot admit a run.
    """

    reason: str
    transport: SyntheticTransport
    _token: object = field(repr=False)

    def __post_init__(self) -> None:
        if self._token is not _SYNTHETIC_TOKEN:
            raise AuthorityReferenceError(
                "synthetic execution capability cannot be constructed directly"
            )
        if not self.reason.strip():
            raise ValueError("synthetic execution requires a stated reason")


class _SealedSyntheticTransportType(type):
    """Prevent runtime replacement of the audited transport implementation."""

    def __setattr__(cls, name: str, value: object) -> None:
        raise TypeError(f"SyntheticTransport class is sealed: {name}")

    def __delattr__(cls, name: str) -> None:
        raise TypeError(f"SyntheticTransport class is sealed: {name}")


class SyntheticTransport(metaclass=_SealedSyntheticTransportType):
    """An offline transport that structurally cannot reach the provider.

    Every call runs its responder with socket creation disabled, so a responder
    that tries to open a connection fails instead of performing provider I/O.
    This is the only source of a :class:`SyntheticExecution` capability.
    """

    __slots__ = ("_responder", "_sealed")
    _responder: Callable[[Mapping[str, object], str], RawCall]
    _sealed: bool

    def __init_subclass__(cls, **_kwargs: object) -> None:
        raise TypeError("SyntheticTransport is sealed and cannot be subclassed")

    def __init__(self, responder: Callable[[Mapping[str, object], str], RawCall]) -> None:
        object.__setattr__(self, "_responder", responder)
        object.__setattr__(self, "_sealed", True)

    def __setattr__(self, name: str, value: object) -> None:
        if getattr(self, "_sealed", False):
            raise TypeError(f"SyntheticTransport instance is immutable: {name}")
        object.__setattr__(self, name, value)

    def __delattr__(self, name: str) -> None:
        raise TypeError(f"SyntheticTransport instance is immutable: {name}")

    def capability(self, reason: str) -> SyntheticExecution:
        """Issue the capability bound to this exact transport instance."""

        return SyntheticExecution(reason=reason, transport=self, _token=_SYNTHETIC_TOKEN)

    def __call__(self, body: Mapping[str, object], credential: str) -> RawCall:
        with _no_socket_access():
            return self._responder(body, credential)


_SYNTHETIC_EXECUTION_ACTIVE: ContextVar[bool] = ContextVar(
    "finimpulse_synthetic_execution_active",
    default=False,
)
_SYNTHETIC_AUDIT_HOOK_INSTALLED = False


def _synthetic_audit_hook(event: str, _args: tuple[object, ...]) -> None:
    if not _SYNTHETIC_EXECUTION_ACTIVE.get():
        return
    if event.startswith(("socket.", "subprocess.", "ctypes.")) or event in {
        "os.system",
        "os.posix_spawn",
        "os.spawn",
    }:
        raise SocketAccessError(f"synthetic transport external I/O is blocked: {event}")


@contextmanager
def _no_socket_access() -> Iterator[None]:
    """Disable direct, aliased, and subprocess network escape paths."""

    global _SYNTHETIC_AUDIT_HOOK_INSTALLED  # noqa: PLW0603 - process-wide hook is append-only
    if not _SYNTHETIC_AUDIT_HOOK_INSTALLED:
        sys.addaudithook(_synthetic_audit_hook)
        _SYNTHETIC_AUDIT_HOOK_INSTALLED = True

    def denied(*_args: object, **_kwargs: object) -> None:
        raise SocketAccessError("a synthetic transport must not open a socket")

    blocked = ("socket", "create_connection", "getaddrinfo")
    originals = {name: getattr(socket, name) for name in blocked}
    for name in blocked:
        setattr(socket, name, denied)
    token = _SYNTHETIC_EXECUTION_ACTIVE.set(True)
    try:
        yield
    finally:
        _SYNTHETIC_EXECUTION_ACTIVE.reset(token)
        for name, original in originals.items():
            setattr(socket, name, original)


@dataclass(frozen=True, slots=True)
class CallOutcome:
    """Terminal per-symbol outcome; publication requires every call terminal."""

    symbol: str
    status: str
    attempts: int
    error_class: str | None = None
    reserved_usd: Decimal = Decimal(0)
    actual_usd: Decimal = Decimal(0)
    row_count: int = 0
    byte_count: int = 0
    content_sha256: str | None = None
    raw_snapshot_id: str | None = None
    rate_limit_headers: Mapping[str, str] = field(default_factory=dict)
    transmission_snapshot_ids: tuple[str, ...] = ()
    transmission_content_sha256: tuple[tuple[str, str], ...] = ()

    @property
    def completed(self) -> bool:
        return self.status == "COMPLETED"


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_optional_number(value: object, *, label: str) -> None:
    if value is None:
        return
    if not _is_number(value) or not Decimal(str(value)).is_finite():
        raise ContractError(f"{label} must be a finite number or null")


def provider_cost(response: Mapping[str, object]) -> Decimal:
    try:
        cost = Decimal(str(response.get("cost")))
    except (InvalidOperation, ValueError):
        raise ContractError("provider cost must be a finite nonnegative amount") from None
    if not cost.is_finite() or cost < 0:
        raise ContractError("provider cost must be a finite nonnegative amount")
    return cost


def _validate_item(item: Mapping[str, object]) -> None:
    record_type = item.get("type")
    if not isinstance(record_type, str) or record_type not in EXPECTED_TYPE_FIELDS:
        raise ContractError("record type is not a requested estimate type")
    expected = frozenset(EXPECTED_TYPE_FIELDS[record_type])
    observed = frozenset(item)
    if observed != expected:
        raise ContractError(
            f"unexpected {record_type} record fields: {sorted(observed ^ expected)}"
        )
    record_date = item["date"]
    if not isinstance(record_date, str):
        raise ContractError("record date must be an ISO date string")
    try:
        date.fromisoformat(record_date)
    except ValueError:
        raise ContractError("record date must be an ISO date string") from None
    if item["date_type"] not in EXPECTED_DATE_TYPES:
        raise ContractError("record date_type must be quarter or year")
    for name in sorted(expected - {"type", "date", "date_type"}):
        value = item[name]
        if record_type == "eps_revisions":
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                raise ContractError(f"{name} must be a nonnegative integer or null")
        else:
            _validate_optional_number(value, label=name)


def _validate_echo(response: Mapping[str, object], raw_call: RawCall) -> None:
    data = response.get("data")
    if not isinstance(data, Mapping):
        raise ContractError("provider data must be an object")
    echo = cast("Mapping[str, object]", data)
    for key in ("symbol", "types", "limit", "offset"):
        if key not in echo:
            raise ContractError(f"provider data is missing {key}")
        if echo[key] != raw_call.request_body.get(key):
            raise ContractError(f"provider data {key} does not echo the request")


def _validate_result(response: Mapping[str, object], raw_call: RawCall) -> None:
    result = response.get("result")
    if not isinstance(result, Mapping):
        raise ContractError("provider result must be an object")
    typed_result = cast("Mapping[str, object]", result)
    if typed_result.get("symbol") != raw_call.request_body.get("symbol"):
        raise ContractError("provider result symbol does not match the request")
    items = typed_result.get("items")
    if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
        raise ContractError("provider result.items must be an array of objects")
    typed_items = cast("list[dict[str, object]]", items)
    items_count = typed_result.get("items_count")
    if not isinstance(items_count, int) or isinstance(items_count, bool):
        raise ContractError("items_count must be an integer")
    if items_count != len(typed_items):
        raise ContractError("items_count does not match result.items length")
    limit = raw_call.request_body.get("limit")
    if not isinstance(limit, int) or items_count > limit:
        raise ContractError("provider returned more rows than the requested limit")
    identities: set[tuple[object, object, object]] = set()
    for item in typed_items:
        _validate_item(item)
        identity = (item["type"], item["date"], item["date_type"])
        if identity in identities:
            raise ContractError("duplicate record identity in provider response")
        identities.add(identity)


def parse_response(raw_call: RawCall) -> dict[str, object]:
    """Decode and contract-validate one captured response."""

    if raw_call.http_status != HTTP_OK:
        raise ContractError(f"captured HTTP status must be 200, observed {raw_call.http_status}")
    try:
        parsed = json.loads(raw_call.body_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ContractError("provider returned a non-JSON response") from None
    if not isinstance(parsed, dict):
        raise ContractError("provider response must decode to an object")
    response = cast("dict[str, object]", parsed)
    reject_credential_like(response)
    if response.get("status_code") != PROVIDER_STATUS_OK:
        raise ContractError("provider status_code must be 20000")
    provider_cost(response)
    _validate_echo(response, raw_call)
    _validate_result(response, raw_call)
    return response


def response_items(response: Mapping[str, object]) -> list[dict[str, object]]:
    result = cast("Mapping[str, object]", response["result"])
    return cast("list[dict[str, object]]", result["items"])


def _charged_amount(raw_call: RawCall) -> tuple[Decimal, bool]:
    """Read the cost a response reported, and whether it was proven.

    Returns ``(amount, proven)``. A body that declares no usable cost yields
    ``(0, False)``: the transmission cannot be shown to be free, so its
    reservation must stay consumed.
    """

    try:
        parsed = json.loads(raw_call.body_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return Decimal(0), False
    if not isinstance(parsed, dict) or "cost" not in parsed:
        return Decimal(0), False
    try:
        cost = Decimal(str(cast("Mapping[str, object]", parsed)["cost"]))
    except InvalidOperation:
        return Decimal(0), False
    if not cost.is_finite() or cost < 0:
        return Decimal(0), False
    return cost, True


class BudgetLedger:
    """Reservation-based client-side limit with immediate per-call reconciliation.

    This bounds only what this client spends while the provider honors ``limit``.
    It is not a provider-side account cap, so a provider metering anomaly has no
    proven maximum loss; such an anomaly halts the run at the first observation.
    """

    def __init__(self, config: CollectorConfig) -> None:
        self._config = config
        self._reserved = Decimal(0)
        self._actual = Decimal(0)
        self._committed = Decimal(0)

    @property
    def reserved_usd(self) -> Decimal:
        return self._reserved

    @property
    def actual_usd(self) -> Decimal:
        return self._actual

    @property
    def remaining_usd(self) -> Decimal:
        """Remaining limit against the larger of committed reservations and charges."""

        return self._config.budget_usd - max(self._committed, self._actual)

    def preflight(self) -> Decimal:
        planned = self._config.planned_reservation_usd
        if planned > self._config.budget_usd:
            raise BudgetError(
                "planned reservation exceeds the client-side limit; zero provider calls were made"
            )
        return planned

    def reserve(self, symbol: str) -> Decimal:
        reservation = self._config.call_reservation_usd
        if reservation > self.remaining_usd:
            raise BudgetError(
                f"reservation for {symbol} exceeds the remaining client-side limit; "
                "the call was never made"
            )
        self._reserved += reservation
        self._committed += reservation
        return reservation

    def reconcile(
        self,
        symbol: str,
        reservation: Decimal,
        charged: Decimal,
        *,
        proven: bool = True,
    ) -> None:
        """Compare one transmission's charge with its reservation immediately.

        ``proven`` is true only when the provider reported an exact cost. An
        unpriced response cannot be shown to be free, so its reservation stays
        consumed; otherwise unpriced retries would spend the client-side limit
        without ever exhausting it.
        """

        self._actual += charged
        self._committed -= reservation
        self._committed += min(reservation, charged) if proven else reservation
        if charged > reservation:
            raise BudgetError(
                f"provider charge for {symbol} exceeded its reservation; the run halted "
                "before any further call"
            )
        if self._actual > self._config.budget_usd:
            raise BudgetError("provider-reported cost exceeded the client-side limit")

    def release(self, reservation: Decimal) -> None:
        """Return an unused reservation after a call failed without a charge."""

        self._committed -= reservation
        self._reserved -= reservation


def raw_snapshot_id(snapshot_id: str, symbol: str) -> str:
    return f"{snapshot_id}.{symbol}"


def transmission_snapshot_id(
    snapshot_id: str,
    symbol: str,
    attempt: int,
    content_sha256: str,
) -> str:
    """Identify one HTTP attempt without collapsing identical response bodies."""

    if attempt < 1:
        raise ValueError("attempt must be positive")
    return f"{snapshot_id}.{symbol}.tx-{attempt:04d}-{content_sha256}"


def _blob_path(raw_store_root: Path, content_sha256: str) -> Path:
    return raw_store_root / "blobs" / "sha256" / content_sha256[:2] / f"{content_sha256}.raw"


def _load_captured_snapshot(
    raw_store_root: Path,
    snapshot_id: str,
) -> tuple[RawCall, ValidationStatus, int | None] | None:
    """Load one exact immutable capture and its persisted validation metadata."""

    provenance_path = raw_store_root / "snapshots" / f"{snapshot_id}.json"
    if not provenance_path.is_file():
        return None
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    content_sha256 = str(provenance["content_sha256"])
    blob_path = _blob_path(raw_store_root, content_sha256)
    if not blob_path.is_file():
        raise PublicationError(
            f"raw provenance for {snapshot_id} has no content-addressed blob: {content_sha256}"
        )
    payload = blob_path.read_bytes()
    if sha256_hex(payload) != content_sha256:
        raise PublicationError(f"stored raw content for {snapshot_id} does not match its digest")
    parameters = provenance["parameters"]
    raw_call = RawCall(
        symbol=str(parameters["symbol"]),
        request_body={
            "symbol": str(parameters["symbol"]),
            "types": list(ESTIMATE_TYPES),
            "limit": int(parameters["limit"]),
            "offset": int(parameters["offset"]),
            "sort_by": [{"selector": "date", "desc": True}],
        },
        requested_at_utc=parse_instant(str(provenance["requested_at_utc"])),
        retrieved_at_utc=parse_instant(str(provenance["retrieved_at_utc"])),
        http_status=int(parameters["http_status"]),
        headers=cast(
            "Mapping[str, str]",
            json.loads(str(parameters.get("response_headers", "{}"))),
        ),
        body_bytes=payload,
    )
    row_count_value = provenance.get("row_count")
    return (
        raw_call,
        ValidationStatus(str(provenance["validation_status"])),
        None if row_count_value is None else int(row_count_value),
    )


def load_captured_symbol(
    raw_store_root: Path,
    snapshot_id: str,
    symbol: str,
) -> RawCall | None:
    """Rebuild an already-captured call so a re-run makes no new provider call.

    A run is identified by ``snapshot_id``. Re-running a snapshot reuses the
    immutable raw capture for every symbol already stored.
    """

    loaded = _load_captured_snapshot(raw_store_root, raw_snapshot_id(snapshot_id, symbol))
    return None if loaded is None else loaded[0]


def build_source_snapshot(
    raw_call: RawCall,
    *,
    snapshot_id: str,
    row_count: int | None = None,
    validation_status: ValidationStatus = ValidationStatus.PASS,
    accepted_transmission_snapshot_id: str | None = None,
) -> SourceSnapshot:
    """Declare credential-free provenance for one immutable raw capture."""

    return SourceSnapshot(
        snapshot_id=snapshot_id,
        schema_version=SCHEMA_VERSION,
        provider=PROVIDER,
        dataset=DATASET,
        source_uri=ENDPOINT,
        request_fingerprint=f"sha256:{raw_call.request_body_sha256}",
        parameters={
            "limit": str(raw_call.request_body.get("limit")),
            "offset": str(raw_call.request_body.get("offset")),
            "symbol": raw_call.symbol,
            "types": ",".join(ESTIMATE_TYPES),
            "http_status": str(raw_call.http_status),
            "response_headers": canonical_json_bytes(dict(raw_call.headers)).decode(),
            **(
                {}
                if accepted_transmission_snapshot_id is None
                else {"accepted_transmission_snapshot_id": accepted_transmission_snapshot_id}
            ),
        },
        requested_at_utc=raw_call.requested_at_utc,
        retrieved_at_utc=raw_call.retrieved_at_utc,
        content_type="application/json",
        encoding="utf-8",
        compression=None,
        raw_byte_length=raw_call.byte_length,
        content_sha256=raw_call.body_sha256,
        parser_name="finimpulse_collector",
        parser_version=str(SCHEMA_VERSION),
        validation_status=validation_status,
        row_count=row_count,
    )


def capture_raw(  # noqa: PLR0913 - immutable provenance has distinct optional metadata
    store: ContentAddressedRawStore,
    raw_call: RawCall,
    *,
    snapshot_id: str,
    row_count: int | None = None,
    validation_status: ValidationStatus = ValidationStatus.PASS,
    accepted_transmission_snapshot_id: str | None = None,
) -> tuple[str, Path]:
    """Write raw bytes to the external content-addressed store before parsing.

    Every received body is captured, including a non-200 or contract-invalid
    response. Such a capture is marked ``BLOCKED`` so it remains durable
    evidence while staying excluded from normalized publication.
    """

    snapshot = build_source_snapshot(
        raw_call,
        snapshot_id=snapshot_id,
        row_count=row_count,
        validation_status=validation_status,
        accepted_transmission_snapshot_id=accepted_transmission_snapshot_id,
    )
    destination = store.capture(snapshot, raw_call.body_bytes)
    return snapshot.snapshot_id, destination


def retry_delay_seconds(attempt: int, config: CollectorConfig) -> float:
    """Exponential backoff with jitter, bounded so a run cannot stall unbounded."""

    if attempt < 1:
        raise ValueError("attempt must be positive")
    base = min(config.backoff_seconds * (2 ** (attempt - 1)), _MAX_BACKOFF_SECONDS)
    jitter = secrets.randbelow(_JITTER_RESOLUTION) / _JITTER_RESOLUTION
    return base * (0.5 + 0.5 * jitter)


class TransportFailureError(CollectorError):
    """A transmission that produced no response body at all.

    ``charged`` records whether the provider may still have metered the
    attempt. Only a request that never reached the provider is proven
    uncharged; anything else keeps its reservation consumed.
    """

    def __init__(self, message: str, error_class: str, *, charged: bool) -> None:
        super().__init__(message)
        self.error_class = error_class
        self.charged = charged


def classify_http_error(status: int) -> str:
    if status == HTTP_TOO_MANY_REQUESTS:
        return "rate_limited"
    if status >= HTTP_SERVER_ERROR_FLOOR:
        return "provider_unavailable"
    return "provider_rejected"


def is_retryable_status(status: int) -> bool:
    """Only 429 and 5xx are retryable; every other status is terminal.

    A 4xx other than 429 is a deterministic rejection, so retrying it spends
    the client-side limit without any chance of a different outcome.
    """

    return status == HTTP_TOO_MANY_REQUESTS or status >= HTTP_SERVER_ERROR_FLOOR


def make_https_transport(
    *,
    timeout_seconds: float = 30.0,
) -> Transport:
    """Build the live transport. G-A never calls the returned function."""

    def transport(body: Mapping[str, object], credential: str) -> RawCall:
        encoded = canonical_json_bytes(dict(body))
        requested_at = utc_now()
        request = urllib.request.Request(
            ENDPOINT,
            data=encoded,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {credential}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
                payload = response.read()
                status = response.status
                headers = {
                    name: response.headers[name]
                    for name in ALLOWED_RESPONSE_HEADERS
                    if response.headers[name] is not None
                }
        except urllib.error.HTTPError as error:
            # Preserve the error body: it is provider evidence and may carry a
            # charge, so it must be captured rather than discarded.
            payload = error.read()
            status = error.code
            headers = {
                name: error.headers[name]
                for name in ALLOWED_RESPONSE_HEADERS
                if error.headers is not None and error.headers[name] is not None
            }
        except urllib.error.URLError as error:
            # urllib cannot prove whether request bytes reached the provider.
            # Keep the reservation consumed so retries remain within the
            # client-side maximum-loss bound.
            raise TransportFailureError(
                f"provider request failed: {type(error.reason).__name__}",
                "transport_error",
                charged=True,
            ) from None
        # Redact at the boundary so no plaintext credential ever leaves this
        # function. The capture path detects the redaction marker and fails
        # closed after storing only the safe bytes.
        payload, _ = redact_credential(payload, credential)
        symbol = body.get("symbol")
        if not isinstance(symbol, str):
            raise CollectorError("request symbol must be a string")
        return RawCall(
            symbol=symbol,
            request_body=dict(body),
            requested_at_utc=requested_at,
            retrieved_at_utc=utc_now(),
            http_status=status,
            headers=headers,
            body_bytes=payload,
        )

    return transport


class InstrumentResolver(Protocol):
    """The 007 resolution surface this collector depends on.

    ``None`` means unmapped, which is a legitimate provider-normalized state.
    Raising ambiguity quarantines the row; the collector never guesses.
    """

    def __call__(
        self,
        provider: str,
        namespace: str,
        provider_identifier: str,
        as_of: datetime,
    ) -> str | None: ...


@dataclass(frozen=True, slots=True)
class IdentityExport:
    """An immutable, hash-verified 007 mapping export used for one snapshot.

    Collection and replay both resolve against this frozen export rather than a
    live registry, so later registry drift cannot change replay output.
    """

    export_sha256: str
    as_of_utc: datetime
    mappings: Mapping[str, tuple[str, ...]]

    def resolve(self, symbol: str) -> IdentityDecision:
        instruments = self.mappings.get(symbol, ())
        if not instruments:
            return IdentityDecision(
                provider_symbol=symbol,
                state=IdentityState.UNRESOLVED,
                instrument_id=None,
                reason="provider identifier has no effective mapping in the pinned export",
            )
        if len(instruments) > 1:
            return IdentityDecision(
                provider_symbol=symbol,
                state=IdentityState.AMBIGUOUS,
                instrument_id=None,
                reason="provider identifier resolves to multiple instruments",
            )
        return IdentityDecision(
            provider_symbol=symbol,
            state=IdentityState.RESOLVED,
            instrument_id=instruments[0],
        )


def load_identity_export(path: Path, *, expected_sha256: str) -> IdentityExport:
    """Load and hash-verify an immutable 007 mapping export.

    Absent evidence or a digest mismatch is fatal: a run must never silently
    fall back to whatever the live registry happens to say right now.
    """

    document, observed = _verified_json_artifact(
        path,
        expected_sha256=expected_sha256,
        label="007 identity export",
        error=IdentityEvidenceError,
    )
    for name in ("as_of_utc", "provider", "namespace", "mappings"):
        if name not in document:
            raise IdentityEvidenceError(f"007 identity export is missing {name}")
    if document["provider"] != PROVIDER or document["namespace"] != IDENTITY_NAMESPACE:
        raise IdentityEvidenceError("007 identity export does not cover the finimpulse ticker key")
    raw_mappings = document["mappings"]
    if not isinstance(raw_mappings, Mapping):
        raise IdentityEvidenceError("007 identity export mappings must be an object")
    mappings: dict[str, tuple[str, ...]] = {}
    for symbol, value in cast("Mapping[str, object]", raw_mappings).items():
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise IdentityEvidenceError(
                f"007 identity export mapping for {symbol} must be an array of instrument IDs"
            )
        mappings[str(symbol)] = tuple(cast("list[str]", value))
    return IdentityExport(
        export_sha256=observed,
        as_of_utc=parse_instant(str(document["as_of_utc"])),
        mappings=mappings,
    )


def export_from_registry(
    resolver: InstrumentResolver,
    symbols: Sequence[str],
    *,
    as_of: datetime,
) -> dict[str, object]:
    """Build an exportable 007 projection from a live registry.

    This is the one place a live registry is read. Its output is hashed and
    frozen, after which collection and replay use only the frozen artifact.
    """

    mappings: dict[str, list[str]] = {}
    for symbol in symbols:
        try:
            instrument_id = resolver(
                provider=PROVIDER,
                namespace=IDENTITY_NAMESPACE,
                provider_identifier=symbol,
                as_of=as_of,
            )
        except IdentityAmbiguityError:
            mappings[symbol] = ["AMBIGUOUS", "AMBIGUOUS"]
            continue
        mappings[symbol] = [] if instrument_id is None else [instrument_id]
    return {
        "as_of_utc": instant_literal(as_of),
        "mappings": mappings,
        "namespace": IDENTITY_NAMESPACE,
        "provider": PROVIDER,
        "schema_version": SCHEMA_VERSION,
    }


@dataclass(frozen=True, slots=True)
class IdentityDecision:
    provider_symbol: str
    state: IdentityState
    instrument_id: str | None
    reason: str | None = None


def normalize_items(  # noqa: PLR0913 - each argument is a distinct frozen schema input
    items: Sequence[Mapping[str, object]],
    *,
    symbol: str,
    snapshot_id: str,
    observed_at: datetime,
    identity: IdentityDecision,
    source_receipt_sha256: str,
) -> list[dict[str, object]]:
    """Project validated provider items onto the frozen normalized schema."""

    rows: list[dict[str, object]] = []
    for item in items:
        estimate_type = str(item["type"])
        row: dict[str, object] = {
            "instrument_id": identity.instrument_id,
            "provider_symbol": symbol,
            "snapshot_id": snapshot_id,
            "observed_at": observed_at,
            "estimate_type": estimate_type,
            "period_date": str(item["date"]),
            "date_type": str(item["date_type"]),
            "source_receipt_sha256": source_receipt_sha256,
        }
        for name in TREND_VALUE_FIELDS:
            value = item.get(name) if estimate_type == "eps_trend" else None
            row[name] = None if value is None else float(cast("float", value))
        for name in REVISION_VALUE_FIELDS:
            value = item.get(name) if estimate_type == "eps_revisions" else None
            row[name] = None if value is None else int(cast("int", value))
        rows.append(row)
    return rows


def natural_key(row: Mapping[str, object]) -> tuple[str, str, str, str]:
    return (
        str(row["provider_symbol"]),
        str(row["estimate_type"]),
        str(row["period_date"]),
        str(row["date_type"]),
    )


def sort_rows(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    return [dict(row) for row in sorted(rows, key=natural_key)]


def _canonical_value(value: object) -> object:
    if isinstance(value, datetime):
        return instant_literal(value)
    return value


def logical_content_hash(rows: Sequence[Mapping[str, object]]) -> str:
    """Hash schema version plus canonically ordered rows, never Parquet bytes.

    Byte-identical Parquet is deliberately out of scope: writer version, codec,
    row groups, dictionaries, and statistics are not pinned by this plan.
    """

    projection = {
        "rows": [
            {name: _canonical_value(row[name]) for name in NORMALIZED_COLUMNS}
            for row in sort_rows(rows)
        ],
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
    }
    return sha256_hex(canonical_json_bytes(projection))


@dataclass(frozen=True, slots=True)
class RestatementFinding:
    """One classified difference between consecutive snapshots on a natural key."""

    provider_symbol: str
    estimate_type: str
    period_date: str
    date_type: str
    classification: RestatementClass
    changed_fields: tuple[str, ...]
    detail: str

    @property
    def is_restatement(self) -> bool:
        return self.classification in RESTATEMENT_CLASSES

    def projection(self) -> dict[str, object]:
        return {
            "changed_fields": list(self.changed_fields),
            "classification": self.classification.value,
            "date_type": self.date_type,
            "detail": self.detail,
            "estimate_type": self.estimate_type,
            "period_date": self.period_date,
            "provider_symbol": self.provider_symbol,
        }


def _index_rows(
    rows: Sequence[Mapping[str, object]],
) -> tuple[dict[tuple[str, str, str, str], dict[str, object]], list[tuple[str, str, str, str]]]:
    indexed: dict[tuple[str, str, str, str], dict[str, object]] = {}
    duplicates: list[tuple[str, str, str, str]] = []
    for row in rows:
        key = natural_key(row)
        if key in indexed:
            duplicates.append(key)
            continue
        indexed[key] = dict(row)
    return indexed, duplicates


def _changed_value_fields(
    previous: Mapping[str, object],
    current: Mapping[str, object],
) -> tuple[str, ...]:
    names = (*TREND_VALUE_FIELDS, *REVISION_VALUE_FIELDS)
    return tuple(name for name in names if previous.get(name) != current.get(name))


def _finding(
    key: tuple[str, str, str, str],
    classification: RestatementClass,
    changed: tuple[str, ...],
    detail: str,
) -> RestatementFinding:
    provider_symbol, estimate_type, period_date, date_type = key
    return RestatementFinding(
        provider_symbol=provider_symbol,
        estimate_type=estimate_type,
        period_date=period_date,
        date_type=date_type,
        classification=classification,
        changed_fields=changed,
        detail=detail,
    )


def _elapsed_days(previous: Mapping[str, object], current: Mapping[str, object]) -> int | None:
    """Infer elapsed days between captures by aligning the trend lookback ladder.

    ``eps_trend`` reports the same estimate series at fixed ages. If the two
    captures are ``d`` days apart, the later capture's ``d``-day-old value must
    equal the earlier capture's ``current`` value.
    """

    previous_current = previous.get("current")
    if previous_current is None:
        return None
    for days in sorted(_DAY_TO_TREND_FIELD):
        if days == 0:
            continue
        if current.get(_DAY_TO_TREND_FIELD[days]) == previous_current:
            return days
    return None


def _trend_history_contradicted(
    previous: Mapping[str, object],
    current: Mapping[str, object],
    elapsed_days: int,
) -> tuple[str, ...]:
    """Return later-capture fields that contradict an earlier immutable capture."""

    contradicted: list[str] = []
    for name, age in sorted(TREND_LOOKBACK_DAYS.items(), key=lambda item: item[1]):
        shifted_age = age + elapsed_days
        counterpart = _DAY_TO_TREND_FIELD.get(shifted_age)
        if counterpart is None:
            continue
        if current.get(counterpart) != previous.get(name):
            contradicted.append(counterpart)
    return tuple(contradicted)


def _classify_trend_change(
    key: tuple[str, str, str, str],
    previous: Mapping[str, object],
    current: Mapping[str, object],
    changed: tuple[str, ...],
) -> RestatementFinding:
    withdrawn = previous.get("current") is not None and current.get("current") is None
    if "current" in changed and withdrawn:
        return _finding(
            key,
            RestatementClass.RESTATEMENT_POST_ACTUALIZATION,
            changed,
            "a previously estimated current value was withdrawn after actualization",
        )
    elapsed_days = _elapsed_days(previous, current)
    if elapsed_days is None:
        # The captures do not align on the lookback ladder, so an earlier
        # published value is not reproduced anywhere in the later capture.
        contradicted = tuple(name for name in changed if name != "current")
        if contradicted:
            return _finding(
                key,
                RestatementClass.RESTATEMENT_LOOKBACK_INCONSISTENCY,
                changed,
                "lookback history does not reproduce a prior immutable capture",
            )
        return _finding(
            key,
            RestatementClass.EXPECTED_CURRENT_REVISION,
            changed,
            "open-period current estimate moved with no contradicted lookback history",
        )
    contradicted = _trend_history_contradicted(previous, current, elapsed_days)
    if contradicted:
        return _finding(
            key,
            RestatementClass.RESTATEMENT_LOOKBACK_INCONSISTENCY,
            contradicted,
            f"lookback values contradict the capture {elapsed_days} days earlier",
        )
    if "current" in changed:
        return _finding(
            key,
            RestatementClass.EXPECTED_CURRENT_REVISION,
            changed,
            "open-period current estimate moved, with consistent lookback history",
        )
    return _finding(
        key,
        RestatementClass.EXPECTED_ROLLING_WINDOW,
        changed,
        "lookback ladder rolled forward consistently with prior captures",
    )


def _classify_changed_key(
    key: tuple[str, str, str, str],
    previous: Mapping[str, object],
    current: Mapping[str, object],
    changed: tuple[str, ...],
) -> RestatementFinding:
    """Separate expected vendor movement from restatement evidence.

    ``eps_revisions`` counts are rolling windows with no reproducible history,
    so their movement is expected. ``eps_trend`` carries an explicit lookback
    ladder that a later capture must not contradict.
    """

    if key[1] == "eps_trend":
        return _classify_trend_change(key, previous, current, changed)
    return _finding(
        key,
        RestatementClass.EXPECTED_ROLLING_WINDOW,
        changed,
        "rolling revision counts advanced within their windows",
    )


def classify_restatements(
    previous_rows: Sequence[Mapping[str, object]],
    current_rows: Sequence[Mapping[str, object]],
) -> tuple[RestatementFinding, ...]:
    """Diff consecutive snapshots on the natural key and classify each change."""

    previous_index, _ = _index_rows(previous_rows)
    current_index, duplicates = _index_rows(current_rows)
    findings: list[RestatementFinding] = []
    for key in sorted(set(previous_index) | set(current_index)):
        previous = previous_index.get(key)
        current = current_index.get(key)
        if previous is None and current is not None:
            findings.append(
                _finding(key, RestatementClass.NEW_KEY, (), "key first observed in this snapshot")
            )
            continue
        if current is None and previous is not None:
            findings.append(
                _finding(
                    key,
                    RestatementClass.MISSING_KEY,
                    (),
                    "key present in the predecessor snapshot is absent here",
                )
            )
            continue
        if previous is None or current is None:
            continue
        changed = _changed_value_fields(previous, current)
        if not changed:
            continue
        findings.append(_classify_changed_key(key, previous, current, changed))
    findings.extend(
        _finding(
            key,
            RestatementClass.ANOMALY_DUPLICATE_ROW,
            (),
            "duplicate natural key observed inside one snapshot",
        )
        for key in sorted(set(duplicates))
    )
    return tuple(findings)


@dataclass(frozen=True, slots=True)
class PublishedPartition:
    relative_path: str
    estimate_type: str
    row_count: int
    content_sha256: str

    def projection(self) -> dict[str, object]:
        return {
            "content_sha256": self.content_sha256,
            "estimate_type": self.estimate_type,
            "relative_path": self.relative_path,
            "row_count": self.row_count,
        }


@dataclass(frozen=True, slots=True)
class RunManifest:
    """Every partition one snapshot published, plus its logical identity."""

    snapshot_id: str
    dataset_version: str
    schema_version: int
    row_count: int
    logical_content_sha256: str
    partitions: tuple[PublishedPartition, ...]

    def projection(self) -> dict[str, object]:
        return {
            "dataset_version": self.dataset_version,
            "logical_content_sha256": self.logical_content_sha256,
            "partitions": [partition.projection() for partition in self.partitions],
            "row_count": self.row_count,
            "schema_id": SCHEMA_ID,
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
        }


def normalized_table(rows: Sequence[Mapping[str, object]]) -> pa.Table:
    ordered = sort_rows(rows)
    columns = {name: [row.get(name) for row in ordered] for name in NORMALIZED_SCHEMA.names}
    return pa.Table.from_pydict(columns, schema=NORMALIZED_SCHEMA)


def partition_relative_path(
    estimate_type: str,
    observed_at: datetime,
    snapshot_id: str,
) -> str:
    moment = observed_at.astimezone(UTC)
    return (
        f"estimate_type={estimate_type}/year={moment.year:04d}/"
        f"month={moment.month:02d}/snapshot_id={snapshot_id}/part-0.parquet"
    )


def dataset_version(
    snapshot_id: str,
    config: CollectorConfig,
    logical_sha256: str,
) -> str:
    """Bind the published version to the snapshot, config, and logical content."""

    digest = sha256_hex(
        canonical_json_bytes(
            {
                "config_sha256": config.digest(),
                "logical_content_sha256": logical_sha256,
                "schema_id": SCHEMA_ID,
                "schema_version": SCHEMA_VERSION,
                "snapshot_id": snapshot_id,
            }
        )
    )
    return f"{SCHEMA_ID}.v{SCHEMA_VERSION}.{digest}"


def publish_snapshot(
    dataset_root: Path,
    rows: Sequence[Mapping[str, object]],
    *,
    snapshot_id: str,
    observed_at: datetime,
    config: CollectorConfig,
) -> RunManifest:
    """Publish one snapshot behind an atomic discovery boundary.

    Partitions are staged privately, then linked into the dataset, and the
    snapshot becomes discoverable only when its commit marker appears through a
    single atomic rename. A concurrent reader that honours the marker, and a
    process that crashes at any point before the rename, can never observe a
    partial snapshot: uncommitted partition files exist but are not visible
    through :func:`committed_snapshot_ids` or :func:`read_partition_rows`.
    """

    ordered = sort_rows(rows)
    logical_sha256 = logical_content_hash(ordered)
    staging_root = dataset_root / ".staging" / snapshot_id
    if staging_root.exists():
        raise PublicationError(f"staging directory already exists for snapshot {snapshot_id}")
    planned: list[tuple[PublishedPartition, Path]] = []
    with publish_lease(dataset_root, snapshot_id):
        try:
            for estimate_type in ESTIMATE_TYPES:
                subset = [row for row in ordered if row["estimate_type"] == estimate_type]
                if not subset:
                    continue
                relative_path = partition_relative_path(estimate_type, observed_at, snapshot_id)
                if (dataset_root / relative_path).exists():
                    raise PublicationError(
                        f"refusing to clobber an existing published partition: {relative_path}"
                    )
                staged = staging_root / relative_path
                staged.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(normalized_table(subset), staged)
                # Write-ahead order: partition bytes are durable before the
                # marker that makes them discoverable.
                _fsync_file(staged)
                planned.append(
                    (
                        PublishedPartition(
                            relative_path=relative_path,
                            estimate_type=estimate_type,
                            row_count=len(subset),
                            content_sha256=sha256_hex(staged.read_bytes()),
                        ),
                        staged,
                    )
                )
            partitions = _commit_staged_partitions(dataset_root, planned)
            manifest = RunManifest(
                snapshot_id=snapshot_id,
                dataset_version=dataset_version(snapshot_id, config, logical_sha256),
                schema_version=SCHEMA_VERSION,
                row_count=len(ordered),
                logical_content_sha256=logical_sha256,
                partitions=tuple(partitions),
            )
            _commit_snapshot_marker(dataset_root, staging_root, manifest)
        finally:
            _discard_staging(staging_root)
    return manifest


COMMITTED_MARKER_DIRECTORY = "_committed"
PUBLISH_LEASE_DIRECTORY = "_leases"


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_lease_path(dataset_root: Path, snapshot_id: str) -> Path:
    return dataset_root / PUBLISH_LEASE_DIRECTORY / f"{snapshot_id}.lease"


@contextmanager
def publish_lease(dataset_root: Path, snapshot_id: str) -> Iterator[Path]:
    """Hold a process-scoped advisory lock so publish and recovery serialize.

    The lock is an OS-held ``flock`` on the lease file rather than an
    ``O_EXCL`` sentinel. The kernel releases it when the holding process dies,
    so a crashed publisher cannot leave a permanently stale lease, while a live
    publisher still blocks concurrent recovery. The lease file itself is left
    in place; only the advisory lock conveys ownership.
    """

    path = publish_lease_path(dataset_root, snapshot_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise PublicationError(
                f"another live publisher holds the lease for snapshot {snapshot_id}"
            ) from None
        os.write(descriptor, f"{os.getpid()}\n".encode())
        try:
            yield path
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def committed_marker_path(dataset_root: Path, snapshot_id: str) -> Path:
    return dataset_root / COMMITTED_MARKER_DIRECTORY / f"{snapshot_id}.json"


def _commit_snapshot_marker(
    dataset_root: Path,
    staging_root: Path,
    manifest: RunManifest,
) -> None:
    """Make one snapshot discoverable through a single atomic rename."""

    destination = committed_marker_path(dataset_root, manifest.snapshot_id)
    if destination.exists():
        raise PublicationError(f"snapshot is already committed: {manifest.snapshot_id}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    pending = staging_root / "commit-marker.json"
    pending.parent.mkdir(parents=True, exist_ok=True)
    pending.write_bytes(canonical_json_bytes(manifest.projection()))
    # The marker itself is durable before it becomes visible, so a crash can
    # never expose a marker whose bytes were lost.
    _fsync_file(pending)
    # Path.replace is atomic on the same filesystem: the marker either exists
    # in full or not at all, so discovery never sees a half-published snapshot.
    pending.replace(destination)
    _fsync_directory(destination.parent)


def committed_snapshot_ids(dataset_root: Path) -> tuple[str, ...]:
    """List only snapshots whose commit marker is present."""

    marker_root = dataset_root / COMMITTED_MARKER_DIRECTORY
    if not marker_root.is_dir():
        return ()
    return tuple(sorted(path.stem for path in marker_root.glob("*.json")))


def load_committed_manifest(dataset_root: Path, snapshot_id: str) -> dict[str, object] | None:
    """Read a committed snapshot manifest, or None when it is not visible."""

    marker = committed_marker_path(dataset_root, snapshot_id)
    if not marker.is_file():
        return None
    return cast("dict[str, object]", json.loads(marker.read_text(encoding="utf-8")))


def recover_uncommitted_partitions(dataset_root: Path, snapshot_id: str) -> tuple[str, ...]:
    """Remove partition files left by a crash before the commit marker.

    A snapshot without its marker was never published, so its partition files
    are unreferenced debris. Recovery takes the same exclusive lease as
    publication, so it can never delete files an active publisher is still
    committing. Returns the removed paths.
    """

    if committed_marker_path(dataset_root, snapshot_id).is_file():
        return ()
    with publish_lease(dataset_root, snapshot_id):
        # Re-check under the lease: a publisher may have committed between the
        # first check and acquiring the lease.
        if committed_marker_path(dataset_root, snapshot_id).is_file():
            return ()
        removed: list[str] = []
        for path in sorted(dataset_root.rglob(f"snapshot_id={snapshot_id}/*.parquet")):
            if COMMITTED_MARKER_DIRECTORY in path.parts or ".staging" in path.parts:
                continue
            removed.append(path.relative_to(dataset_root).as_posix())
            path.unlink(missing_ok=True)
    return tuple(removed)


def _commit_staged_partitions(
    dataset_root: Path,
    planned: Sequence[tuple[PublishedPartition, Path]],
) -> tuple[PublishedPartition, ...]:
    """Link every staged partition into the dataset, or none of them."""

    linked: list[Path] = []
    try:
        for partition, staged in planned:
            destination = dataset_root / partition.relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.link(staged, destination)
            linked.append(destination)
            _fsync_directory(destination.parent)
    except OSError:
        for destination in linked:
            destination.unlink(missing_ok=True)
        raise
    return tuple(partition for partition, _ in planned)


def _discard_staging(staging_root: Path) -> None:
    """Remove only this snapshot's private staging tree."""

    if not staging_root.exists():
        return
    for path in sorted(staging_root.rglob("*"), reverse=True):
        if path.is_file():
            path.unlink(missing_ok=True)
        elif path.is_dir():
            path.rmdir()
    staging_root.rmdir()
    # Remove the shared .staging parent only when this was the last snapshot
    # staging under it, so concurrent snapshots are never disturbed.
    parent = staging_root.parent
    if parent.name == ".staging" and not any(parent.iterdir()):
        parent.rmdir()


def read_partition_rows(dataset_root: Path, manifest: RunManifest) -> list[dict[str, object]]:
    """Read back one committed snapshot as normalized rows.

    Discovery honours the commit marker, so a snapshot whose partitions exist
    but whose marker is absent is invisible to readers.

    Every partition is verified to exist and to match the content hash the
    committed marker recorded, so silently truncated or altered bytes are a
    hard failure rather than a quiet wrong answer.
    """

    committed = load_committed_manifest(dataset_root, manifest.snapshot_id)
    if committed is None:
        raise PublicationError(
            f"snapshot is not committed and must not be read: {manifest.snapshot_id}"
        )
    recorded = {
        str(entry["relative_path"]): str(entry["content_sha256"])
        for entry in cast("list[Mapping[str, object]]", committed["partitions"])
    }
    rows: list[dict[str, object]] = []
    for partition in manifest.partitions:
        path = dataset_root / partition.relative_path
        if not path.is_file():
            raise PublicationError(f"committed partition is missing: {partition.relative_path}")
        expected = recorded.get(partition.relative_path)
        if expected is None:
            raise PublicationError(
                f"partition is not listed in the commit marker: {partition.relative_path}"
            )
        if sha256_hex(path.read_bytes()) != expected:
            raise PublicationError(
                f"committed partition does not match its recorded hash: {partition.relative_path}"
            )
        table = pq.read_table(path)
        rows.extend(cast("list[dict[str, object]]", table.to_pylist()))
    return sort_rows(rows)


@dataclass(frozen=True, slots=True)
class CollectionResult:
    """Everything one snapshot produced, published only when fully terminal."""

    snapshot_id: str
    observed_at: datetime
    outcomes: tuple[CallOutcome, ...]
    identity_decisions: tuple[IdentityDecision, ...]
    rows: tuple[Mapping[str, object], ...]
    manifest: RunManifest | None
    restatements: tuple[RestatementFinding, ...]
    receipt: Mapping[str, object]
    published: bool
    captured: tuple[CapturedSymbol, ...] = ()


def _identity_counts(decisions: Sequence[IdentityDecision]) -> dict[str, int]:
    counts = dict.fromkeys((state.value for state in IdentityState), 0)
    for decision in decisions:
        counts[decision.state.value] += 1
    return counts


def _restatement_counts(findings: Sequence[RestatementFinding]) -> dict[str, int]:
    counts = dict.fromkeys((item.value for item in RestatementClass), 0)
    for finding in findings:
        counts[finding.classification.value] += 1
    return counts


def _rate_limit_observations(
    outcomes: Sequence[CallOutcome],
    *,
    throttle_configured: bool = False,
) -> dict[str, object]:
    limits = [
        int(outcome.rate_limit_headers["x-ratelimit-limit"])
        for outcome in outcomes
        if "x-ratelimit-limit" in outcome.rate_limit_headers
    ]
    remaining = [
        int(outcome.rate_limit_headers["x-ratelimit-remaining"])
        for outcome in outcomes
        if "x-ratelimit-remaining" in outcome.rate_limit_headers
    ]
    return {
        "minimum_remaining": min(remaining) if remaining else None,
        "observed_limit": min(limits) if limits else None,
        "semantics_status": "NOT_RECONFIRMED",
        "status": "OBSERVED" if limits or remaining else "NOT_OBSERVED",
        "throttle_configured": throttle_configured,
    }


def _standing_authority_projection(
    authority: VerifiedRecurringAuthority,
) -> dict[str, object]:
    """Pin the signed grant that authorized paid calls into durable evidence."""

    return {
        "authority_artifact_sha256": authority.authority_artifact_sha256,
        "authority_id": authority.authority_id,
        "calls_per_minute": authority.calls_per_minute,
        "dataset_root": str(authority.dataset_root),
        "max_spend_micros": authority.max_spend_micros,
        "payload_sha256": authority.payload_sha256,
        "raw_store_root": str(authority.raw_store_root),
        "signature_sha256": authority.signature_sha256,
    }


def _standing_grant_evidence(result: CollectionResult) -> dict[str, object]:
    """Extract the standing-grant digest pair from a completed receipt."""

    standing = result.receipt.get("standing_authority")
    if not isinstance(standing, Mapping):
        return {}
    payload = standing.get("payload_sha256")
    signature = standing.get("signature_sha256")
    if not isinstance(payload, str) or not isinstance(signature, str):
        return {}
    return {
        "standing_authority_payload_sha256": payload,
        "standing_authority_signature_sha256": signature,
    }


def build_receipt(  # noqa: PLR0913 - the receipt pins every named evidence input
    *,
    snapshot_id: str,
    observed_at: datetime,
    config: CollectorConfig,
    ledger: BudgetLedger,
    planned_reservation: Decimal,
    outcomes: Sequence[CallOutcome],
    identity_decisions: Sequence[IdentityDecision],
    restatements: Sequence[RestatementFinding],
    manifest: RunManifest | None,
    published: bool,
    halt_reason: str | None,
    gate_evidence: GateEvidence | None = None,
    standing_authority: VerifiedRecurringAuthority | None = None,
) -> dict[str, object]:
    """Build the schema-validated run receipt that pins every replay input."""

    row_counts: dict[str, int] = dict.fromkeys(ESTIMATE_TYPES, 0)
    per_symbol: dict[str, int] = {}
    for outcome in outcomes:
        per_symbol[outcome.symbol] = outcome.row_count
    if manifest is not None:
        for partition in manifest.partitions:
            row_counts[partition.estimate_type] += partition.row_count
    receipt: dict[str, object] = {
        "receipt_version": RECEIPT_VERSION,
        "task_id": TASK_ID,
        "provider": PROVIDER,
        "dataset": DATASET,
        "endpoint": ENDPOINT,
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "observed_at_utc": instant_literal(observed_at),
        "posture": "PROBE_ONLY_NOT_PROMOTED",
        "authentication_handling": {
            "source_name": f"environment variable {CREDENTIAL_ENVIRONMENT_VARIABLE}",
            "external_persistence": "NOT_OBSERVED",
            "console_disclosure": "NOT_OBSERVED",
        },
        "pinned_replay_inputs": {
            "collector_code_version": config.collector_code_version,
            "config_sha256": config.digest(),
            "identity_as_of_utc": (
                None if config.identity_as_of is None else instant_literal(config.identity_as_of)
            ),
            "identity_export_sha256": config.identity_export_sha256,
            "predecessor_snapshot_id": config.predecessor_snapshot_id,
            "raw_content_sha256": {
                outcome.symbol: outcome.content_sha256
                for outcome in outcomes
                if outcome.content_sha256 is not None
            },
            "transmission_content_sha256": {
                transmission_id: content_sha256
                for outcome in outcomes
                for transmission_id, content_sha256 in outcome.transmission_content_sha256
            },
            "schema_version": SCHEMA_VERSION,
            "universe_sha256": universe_sha256(config.universe),
        },
        "window": {
            "requested_symbol_count": len(config.universe),
            "observed_symbol_count": sum(1 for outcome in outcomes if outcome.completed),
            "requested_types": list(ESTIMATE_TYPES),
        },
        "calls": [
            {
                "attempts": outcome.attempts,
                "byte_count": outcome.byte_count,
                "content_sha256": outcome.content_sha256,
                "error_class": outcome.error_class,
                "raw_snapshot_id": outcome.raw_snapshot_id,
                "reserved_usd": format_decimal(outcome.reserved_usd),
                "actual_usd": format_decimal(outcome.actual_usd),
                "row_count": outcome.row_count,
                "status": outcome.status,
                "symbol": outcome.symbol,
                "transmission_snapshot_ids": list(outcome.transmission_snapshot_ids),
                "transmission_content_sha256": [
                    [snapshot_id, content_sha256]
                    for snapshot_id, content_sha256 in outcome.transmission_content_sha256
                ],
            }
            for outcome in outcomes
        ],
        "row_counts": {
            "by_estimate_type": row_counts,
            "by_symbol": per_symbol,
            "total": 0 if manifest is None else manifest.row_count,
        },
        "cost": {
            "pricing": {
                "call_usd": format_decimal(config.call_price_usd),
                "row_usd": format_decimal(config.row_price_usd),
            },
            "client_side_limit_usd": format_decimal(config.budget_usd),
            "planned_reservation_usd": format_decimal(planned_reservation),
            "reserved_usd": format_decimal(ledger.reserved_usd),
            "provider_reported_total_usd": format_decimal(ledger.actual_usd),
            "within_client_side_limit": ledger.actual_usd <= config.budget_usd,
            "reserved_vs_actual_reconciled": all(
                outcome.actual_usd <= outcome.reserved_usd for outcome in outcomes
            ),
            "provider_side_account_cap": "NOT_OBSERVED",
            "maximum_loss_bound": "UNKNOWN_WITHOUT_ACCOUNT_CAP_EVIDENCE",
            "transmission_count": sum(
                max(len(outcome.transmission_snapshot_ids), outcome.attempts)
                for outcome in outcomes
            ),
        },
        "gb_gate_evidence": (
            {"status": "NOT_OBSERVED"} if gate_evidence is None else gate_evidence.projection()
        ),
        "rate_limit": _rate_limit_observations(
            outcomes,
            throttle_configured=standing_authority is not None,
        ),
        "standing_authority": (
            None
            if standing_authority is None
            else _standing_authority_projection(standing_authority)
        ),
        "identity": {
            "namespace": IDENTITY_NAMESPACE,
            "counts": _identity_counts(identity_decisions),
            "quarantined": [
                {
                    "provider_symbol": decision.provider_symbol,
                    "reason": decision.reason,
                    "state": decision.state.value,
                }
                for decision in identity_decisions
                if decision.state is not IdentityState.RESOLVED
            ],
        },
        "restatements": {
            "counts": _restatement_counts(restatements),
            "findings": [finding.projection() for finding in restatements],
            "predecessor_snapshot_id": config.predecessor_snapshot_id,
        },
        "publication": {
            "published": published,
            "halt_reason": halt_reason,
            "dataset_version": None if manifest is None else manifest.dataset_version,
            "logical_content_sha256": (
                None if manifest is None else manifest.logical_content_sha256
            ),
            "partitions": []
            if manifest is None
            else [partition.projection() for partition in manifest.partitions],
        },
        "purge_capability": {
            "status": "BLOCKED",
            "reason": "AAS-DATA-005 has no purge attestation or provider-wide deletion API",
        },
        "eligibility": {
            "canonical": False,
            "backtest": False,
            "paper": False,
            "order": False,
        },
    }
    reject_credential_like(receipt)
    validate_receipt(receipt)
    return receipt


_REQUIRED_RECEIPT_FIELDS = (
    "receipt_version",
    "task_id",
    "provider",
    "dataset",
    "schema_id",
    "schema_version",
    "snapshot_id",
    "observed_at_utc",
    "pinned_replay_inputs",
    "calls",
    "cost",
    "identity",
    "restatements",
    "publication",
    "purge_capability",
    "eligibility",
)
_REQUIRED_PINNED_INPUTS = (
    "collector_code_version",
    "config_sha256",
    "identity_as_of_utc",
    "identity_export_sha256",
    "predecessor_snapshot_id",
    "raw_content_sha256",
    "transmission_content_sha256",
    "schema_version",
    "universe_sha256",
)


def validate_receipt(receipt: Mapping[str, object]) -> None:
    """Fail closed when a receipt omits a required evidence field."""

    missing = [name for name in _REQUIRED_RECEIPT_FIELDS if name not in receipt]
    if missing:
        raise ContractError(f"receipt is missing required fields: {sorted(missing)}")
    if receipt["receipt_version"] != RECEIPT_VERSION:
        raise ContractError("receipt_version does not match the collector contract")
    if receipt["task_id"] != TASK_ID or receipt["provider"] != PROVIDER:
        raise ContractError("receipt identity does not match AAS-DATA-008C")
    if receipt["schema_version"] != SCHEMA_VERSION:
        raise ContractError("receipt schema_version does not match the frozen schema")
    pinned = receipt["pinned_replay_inputs"]
    if not isinstance(pinned, Mapping):
        raise ContractError("pinned_replay_inputs must be an object")
    missing_pinned = [name for name in _REQUIRED_PINNED_INPUTS if name not in pinned]
    if missing_pinned:
        raise ContractError(f"receipt is missing pinned replay inputs: {sorted(missing_pinned)}")
    eligibility = receipt["eligibility"]
    if not isinstance(eligibility, Mapping) or any(eligibility.values()):
        raise ContractError("008C receipts must record zero eligibility")


def receipt_sha256(receipt: Mapping[str, object]) -> str:
    return sha256_hex(canonical_json_bytes(dict(receipt)))


def validate_recovery_inputs(receipt: Mapping[str, object], config: CollectorConfig) -> None:
    """Bind lifecycle recovery to every immutable input pinned by the receipt."""

    validate_receipt(receipt)
    pinned = cast("Mapping[str, object]", receipt["pinned_replay_inputs"])
    expected: Mapping[str, object] = {
        "collector_code_version": config.collector_code_version,
        "config_sha256": config.digest(),
        "identity_as_of_utc": (
            None if config.identity_as_of is None else instant_literal(config.identity_as_of)
        ),
        "identity_export_sha256": config.identity_export_sha256,
        "predecessor_snapshot_id": config.predecessor_snapshot_id,
        "schema_version": SCHEMA_VERSION,
        "universe_sha256": universe_sha256(config.universe),
    }
    mismatched = sorted(name for name, value in expected.items() if pinned[name] != value)
    if mismatched:
        raise ContractError(f"recovery inputs do not match the receipt: {mismatched}")


def lifecycle_is_complete(engine: Engine, result: CollectionResult, run_id: str) -> bool:
    """Return true only when every expected 003/005 projection is durable.

    A run row alone is not completion: a process can die after ``start_run``
    while events, receipts, usage, or source snapshots are still missing.  The
    recovery path may refuse a repeat only after the terminal event and every
    projection derived from the receipt are present.
    """

    state = CollectionRegistry(engine).current_run_state(run_id)
    expected_events = build_run_events(run_id, result)
    if state is None or not state.terminal or state.state is not expected_events[-1].event_type:
        return False
    expected_sources = {transmission.snapshot_id for transmission in _unique_transmissions(result)}
    expected_usage = {
        record.usage_seq
        for record in build_usage_records(
            run_id,
            result,
            recorded_at_utc=result.observed_at,
        )
    }
    with engine.connect() as connection:
        observed_receipts = set(
            connection.execute(
                select(collection_run_receipts.c.source_snapshot_id).where(
                    collection_run_receipts.c.run_id == run_id
                )
            ).scalars()
        )
        observed_usage = set(
            connection.execute(
                select(collection_usage_records.c.usage_seq).where(
                    collection_usage_records.c.run_id == run_id
                )
            ).scalars()
        )
        observed_sources = set()
        if expected_sources:
            observed_sources = set(
                connection.execute(
                    select(source_snapshots.c.snapshot_id).where(
                        source_snapshots.c.snapshot_id.in_(expected_sources)
                    )
                ).scalars()
            )
    return (
        observed_receipts == expected_sources
        and observed_usage == expected_usage
        and observed_sources == expected_sources
    )


def _rebuild_call_outcome(entry: Mapping[str, object]) -> CallOutcome:
    return CallOutcome(
        symbol=str(entry["symbol"]),
        status=str(entry["status"]),
        attempts=int(cast("int", entry["attempts"])),
        error_class=cast("str | None", entry.get("error_class")),
        reserved_usd=Decimal(str(entry["reserved_usd"])),
        actual_usd=Decimal(str(entry["actual_usd"])),
        row_count=int(cast("int", entry["row_count"])),
        byte_count=int(cast("int", entry["byte_count"])),
        content_sha256=cast("str | None", entry.get("content_sha256")),
        raw_snapshot_id=cast("str | None", entry.get("raw_snapshot_id")),
        transmission_snapshot_ids=tuple(
            str(item) for item in cast("Sequence[object]", entry["transmission_snapshot_ids"])
        ),
        transmission_content_sha256=tuple(
            (str(snapshot_id), str(content_sha256))
            for snapshot_id, content_sha256 in cast(
                "Sequence[Sequence[object]]", entry["transmission_content_sha256"]
            )
        ),
    )


def _rebuild_call_items(
    outcome: CallOutcome,
    raw_call: RawCall,
    validation_status: ValidationStatus,
) -> tuple[Mapping[str, object], ...]:
    if not outcome.completed:
        if validation_status is not ValidationStatus.BLOCKED:
            raise PublicationError(
                f"failed call is not backed by BLOCKED evidence: {outcome.symbol}"
            )
        return ()
    if validation_status is not ValidationStatus.PASS:
        raise PublicationError(f"completed call is not backed by PASS evidence: {outcome.symbol}")
    items = tuple(response_items(parse_response(raw_call)))
    if len(items) != outcome.row_count:
        raise PublicationError(
            f"stored capture row count does not match the receipt: {outcome.symbol}"
        )
    return items


def _rebuild_captured_call(  # noqa: C901 - validates the complete immutable evidence chain
    outcome: CallOutcome,
    *,
    raw_store_root: Path,
    raw_hashes: Mapping[str, object],
    transmission_hashes: Mapping[str, object],
) -> CapturedSymbol | None:
    """Rebuild and verify every transmission plus the terminal call evidence."""

    symbol = outcome.symbol
    transmission_ids = tuple(
        snapshot_id for snapshot_id, _content_sha256 in outcome.transmission_content_sha256
    )
    if transmission_ids != outcome.transmission_snapshot_ids:
        raise PublicationError(f"call transmission hashes are incomplete or reordered: {symbol}")
    transmissions: list[CapturedTransmission] = []
    for snapshot_id, expected_hash in outcome.transmission_content_sha256:
        if transmission_hashes.get(snapshot_id) != expected_hash:
            raise PublicationError(f"transmission hash is not pinned by the receipt: {snapshot_id}")
        loaded = _load_captured_snapshot(raw_store_root, snapshot_id)
        if loaded is None:
            raise PublicationError(
                f"receipt references a transmission that is not in the raw store: {snapshot_id}"
            )
        raw_call, validation_status, source_row_count = loaded
        if raw_call.symbol != symbol or raw_call.body_sha256 != expected_hash:
            raise PublicationError(f"stored transmission does not match the receipt: {snapshot_id}")
        transmissions.append(
            CapturedTransmission(
                snapshot_id=snapshot_id,
                raw_call=raw_call,
                validation_status=validation_status,
                row_count=source_row_count,
            )
        )
    if (outcome.content_sha256 is None) != (outcome.raw_snapshot_id is None):
        raise PublicationError(f"receipt has incomplete raw evidence identity for {symbol}")
    if outcome.content_sha256 is None:
        if symbol in raw_hashes:
            raise PublicationError(f"receipt has an unbound pinned raw hash for {symbol}")
        if transmissions:
            raise PublicationError(
                f"receipt has transmissions but no terminal raw evidence: {symbol}"
            )
        return None
    terminal = next(
        (
            transmission
            for transmission in transmissions
            if transmission.snapshot_id == outcome.raw_snapshot_id
        ),
        None,
    )
    if terminal is None:
        raise PublicationError(f"call raw evidence is not listed among its transmissions: {symbol}")
    raw_call = terminal.raw_call
    validation_status = terminal.validation_status
    source_row_count = terminal.row_count
    if raw_call.body_sha256 != outcome.content_sha256 or raw_call.body_sha256 != str(
        raw_hashes.get(symbol)
    ):
        raise PublicationError(f"stored capture does not match the receipt hash: {symbol}")
    if raw_call.byte_length != outcome.byte_count:
        raise PublicationError(f"stored capture byte count does not match the receipt: {symbol}")
    items = _rebuild_call_items(outcome, raw_call, validation_status)
    return CapturedSymbol(
        outcome=outcome,
        raw_call=raw_call,
        items=items,
        validation_status=validation_status,
        source_row_count=source_row_count,
        transmissions=tuple(transmissions),
    )


def rebuild_result_from_receipt(
    receipt: Mapping[str, object],
    *,
    raw_store_root: Path,
    dataset_root: Path,
) -> CollectionResult:
    """Reconstruct a completed run from its durable receipt and artifacts.

    Every capture referenced by the receipt is re-read from the immutable raw
    store and verified against its recorded content hash, and a published
    snapshot must carry its commit marker. Nothing is fetched from the
    provider, so a resume costs nothing and cannot invent evidence.
    """

    validate_receipt(receipt)
    snapshot_id = str(receipt["snapshot_id"])
    observed_at = parse_instant(str(receipt["observed_at_utc"]))
    pinned = cast("Mapping[str, object]", receipt["pinned_replay_inputs"])
    raw_hashes = cast("Mapping[str, object]", pinned["raw_content_sha256"])
    transmission_hashes = cast("Mapping[str, object]", pinned["transmission_content_sha256"])
    outcomes: list[CallOutcome] = []
    captured: list[CapturedSymbol] = []
    for entry in cast("list[Mapping[str, object]]", receipt["calls"]):
        outcome = _rebuild_call_outcome(entry)
        outcomes.append(outcome)
        rebuilt = _rebuild_captured_call(
            outcome,
            raw_store_root=raw_store_root,
            raw_hashes=raw_hashes,
            transmission_hashes=transmission_hashes,
        )
        if rebuilt is not None:
            captured.append(rebuilt)
    observed_raw_hashes = {
        outcome.symbol: outcome.content_sha256
        for outcome in outcomes
        if outcome.content_sha256 is not None
    }
    if dict(raw_hashes) != observed_raw_hashes:
        raise PublicationError("receipt pinned raw hashes do not match its call evidence")
    observed_transmission_hashes = {
        snapshot_id: content_sha256
        for outcome in outcomes
        for snapshot_id, content_sha256 in outcome.transmission_content_sha256
    }
    if dict(transmission_hashes) != observed_transmission_hashes:
        raise PublicationError("receipt pinned transmission hashes do not match its call evidence")
    publication = cast("Mapping[str, object]", receipt["publication"])
    published = bool(publication["published"])
    if published and load_committed_manifest(dataset_root, snapshot_id) is None:
        raise PublicationError(
            f"receipt claims a published snapshot with no commit marker: {snapshot_id}"
        )
    return CollectionResult(
        snapshot_id=snapshot_id,
        observed_at=observed_at,
        outcomes=tuple(outcomes),
        identity_decisions=(),
        rows=(),
        manifest=None,
        restatements=(),
        receipt=receipt,
        published=published,
        captured=tuple(captured),
    )


def capture_receipt_sha256(
    snapshot_id: str,
    config: CollectorConfig,
    raw_content_sha256: Mapping[str, str],
) -> str:
    """Digest the capture evidence each normalized row is derived from.

    This is computed before publication from the snapshot identity, the frozen
    config, and the immutable raw content hashes, so a replay from raw content
    plus the pinned receipt inputs reproduces the same value.
    """

    return sha256_hex(
        canonical_json_bytes(
            {
                "config_sha256": config.digest(),
                "raw_content_sha256": dict(sorted(raw_content_sha256.items())),
                "schema_id": SCHEMA_ID,
                "schema_version": SCHEMA_VERSION,
                "snapshot_id": snapshot_id,
            }
        )
    )


@dataclass(frozen=True, slots=True)
class CapturedTransmission:
    """One immutable provider response retained for 003/005 provenance."""

    snapshot_id: str
    raw_call: RawCall
    validation_status: ValidationStatus
    row_count: int | None = None


@dataclass(frozen=True, slots=True)
class CapturedSymbol:
    """One terminal symbol capture plus every transmission that led to it."""

    outcome: CallOutcome
    raw_call: RawCall
    items: tuple[Mapping[str, object], ...]
    validation_status: ValidationStatus = ValidationStatus.PASS
    source_row_count: int | None = None
    transmissions: tuple[CapturedTransmission, ...] = ()


def _sleep_between_attempts(
    attempt: int, config: CollectorConfig, sleeper: Callable[[float], None]
) -> None:
    sleeper(retry_delay_seconds(attempt, config))


def _replayed_symbol(
    raw_store_root: Path,
    snapshot_id: str,
    symbol: str,
) -> CapturedSymbol | None:
    """Reuse an already-captured symbol so a re-run makes no new call."""

    stable_id = raw_snapshot_id(snapshot_id, symbol)
    loaded = _load_captured_snapshot(raw_store_root, stable_id)
    if loaded is None:
        return None
    provenance = json.loads(
        (raw_store_root / "snapshots" / f"{stable_id}.json").read_text(encoding="utf-8")
    )
    accepted_id = str(provenance["parameters"].get("accepted_transmission_snapshot_id", ""))
    if not accepted_id:
        raise PublicationError(
            f"replay capture does not name its accepted transmission: {stable_id}"
        )
    accepted = _load_captured_snapshot(raw_store_root, accepted_id)
    if accepted is None:
        raise PublicationError(f"replay transmission is absent: {accepted_id}")
    replayed, validation_status, source_row_count = accepted
    if validation_status is not ValidationStatus.PASS:
        raise PublicationError(f"replay capture is not accepted evidence: {accepted_id}")
    items = tuple(response_items(parse_response(replayed)))
    transmission = CapturedTransmission(
        snapshot_id=accepted_id,
        raw_call=replayed,
        validation_status=validation_status,
        row_count=source_row_count,
    )
    return CapturedSymbol(
        outcome=CallOutcome(
            symbol=symbol,
            status="COMPLETED",
            attempts=0,
            reserved_usd=Decimal(0),
            actual_usd=Decimal(0),
            row_count=len(items),
            byte_count=replayed.byte_length,
            content_sha256=replayed.body_sha256,
            raw_snapshot_id=accepted_id,
            rate_limit_headers={},
            transmission_snapshot_ids=(accepted_id,),
            transmission_content_sha256=((accepted_id, replayed.body_sha256),),
        ),
        raw_call=replayed,
        items=items,
        source_row_count=len(items),
        transmissions=(transmission,),
    )


def _failed_symbol(  # noqa: PLR0913 - a failed call still records its full ledger
    symbol: str,
    body: Mapping[str, object],
    *,
    attempts: int,
    error_class: str,
    reserved: Decimal,
    charged: Decimal,
    transmissions: tuple[CapturedTransmission, ...],
) -> CapturedSymbol:
    moment = utc_now()
    terminal = transmissions[-1] if transmissions else None
    raw_call = None if terminal is None else terminal.raw_call
    evidence = raw_call or RawCall(
        symbol=symbol,
        request_body=dict(body),
        requested_at_utc=moment,
        retrieved_at_utc=moment,
        http_status=0,
        headers={},
        body_bytes=b"",
    )
    return CapturedSymbol(
        outcome=CallOutcome(
            symbol=symbol,
            status="FAILED",
            attempts=attempts,
            error_class=error_class,
            reserved_usd=reserved,
            actual_usd=charged,
            byte_count=0 if raw_call is None else raw_call.byte_length,
            content_sha256=None if raw_call is None else raw_call.body_sha256,
            raw_snapshot_id=None if terminal is None else terminal.snapshot_id,
            transmission_snapshot_ids=tuple(item.snapshot_id for item in transmissions),
            transmission_content_sha256=tuple(
                (item.snapshot_id, item.raw_call.body_sha256) for item in transmissions
            ),
        ),
        raw_call=evidence,
        items=(),
        validation_status=ValidationStatus.BLOCKED,
        source_row_count=None,
        transmissions=transmissions,
    )


def _capture_transmission(  # noqa: PLR0913 - capture binds attempt and credential context
    store: ContentAddressedRawStore,
    raw_call: RawCall,
    *,
    snapshot_id: str,
    symbol: str,
    attempt: int,
    credential: str,
) -> tuple[RawCall, str, bool]:
    """Redact, then persist one transmission as quarantined evidence.

    Redaction happens before persistence so plaintext credential material can
    never reach the raw store, even as BLOCKED evidence.
    """

    safe_bytes, reflected_here = redact_credential(raw_call.body_bytes, credential)
    if reflected_here:
        raw_call = replace(raw_call, body_bytes=safe_bytes)
    reflected = reflected_here or CREDENTIAL_REDACTION in raw_call.body_bytes
    store.capture_payload(raw_call.body_bytes)
    captured_id = transmission_snapshot_id(
        snapshot_id,
        symbol,
        attempt,
        raw_call.body_sha256,
    )
    return raw_call, captured_id, reflected


def _capture_symbol(  # noqa: C901,PLR0913,PLR0915 - bounded retry state machine
    symbol: str,
    *,
    config: CollectorConfig,
    credential: str,
    transport: Transport,
    store: ContentAddressedRawStore,
    raw_store_root: Path,
    snapshot_id: str,
    ledger: BudgetLedger,
    sleeper: Callable[[float], None],
    request_guard: Callable[[], None] | None = None,
    rate_limiter: RateLimiter | None = None,
) -> CapturedSymbol:
    """Capture one symbol, reserving and reconciling every transmission.

    Each transmission gets its own reservation, and every received body is
    captured immutably before it is parsed. Only 429 and 5xx are retried; any
    other status is terminal evidence rather than a repeatable attempt.
    """

    replayed = _replayed_symbol(raw_store_root, snapshot_id, symbol)
    if replayed is not None:
        return replayed
    body = request_body(symbol, config)
    reserved_total = Decimal(0)
    charged_total = Decimal(0)
    transmissions: list[CapturedTransmission] = []
    error_class = "transport_error"
    for attempt in range(1, config.max_retries + 2):
        # Every transmission is reserved before it is sent, so a retry can
        # never spend beyond the client-side limit.
        reservation = ledger.reserve(symbol)
        reserved_total += reservation
        try:
            if rate_limiter is not None:
                rate_limiter.before_request()
            if request_guard is not None:
                request_guard()
            raw_call = transport(body, credential)
        except TransportFailureError as failure:
            error_class = failure.error_class
            if not failure.charged:
                ledger.release(reservation)
                reserved_total -= reservation
            if attempt <= config.max_retries:
                _sleep_between_attempts(attempt, config, sleeper)
                continue
            return _failed_symbol(
                symbol,
                body,
                attempts=attempt,
                error_class=error_class,
                reserved=reserved_total,
                charged=charged_total,
                transmissions=tuple(transmissions),
            )
        # Capture first, for every status: the received bytes are evidence
        # before anything about them has been validated.
        raw_call, captured_id, reflected = _capture_transmission(
            store,
            raw_call,
            snapshot_id=snapshot_id,
            symbol=symbol,
            attempt=attempt,
            credential=credential,
        )
        if reflected:
            capture_raw(
                store,
                raw_call,
                snapshot_id=captured_id,
                validation_status=ValidationStatus.BLOCKED,
            )
            # Only the redacted body persists. Fail closed without echoing any
            # part of the response.
            raise ContractError("provider response reflected credential material")
        charged, proven = _charged_amount(raw_call)
        charged_total += charged
        # Reconcile every transmission immediately, including a non-200, so a
        # charge above its reservation halts the run before any retry.
        ledger.reconcile(symbol, reservation, charged, proven=proven)
        if raw_call.http_status != HTTP_OK:
            capture_raw(
                store,
                raw_call,
                snapshot_id=captured_id,
                validation_status=ValidationStatus.BLOCKED,
            )
            transmissions.append(
                CapturedTransmission(
                    snapshot_id=captured_id,
                    raw_call=raw_call,
                    validation_status=ValidationStatus.BLOCKED,
                )
            )
            error_class = classify_http_error(raw_call.http_status)
            if is_retryable_status(raw_call.http_status) and attempt <= config.max_retries:
                _sleep_between_attempts(attempt, config, sleeper)
                continue
            return _failed_symbol(
                symbol,
                body,
                attempts=attempt,
                error_class=error_class,
                reserved=reserved_total,
                charged=charged_total,
                transmissions=tuple(transmissions),
            )
        try:
            response = parse_response(raw_call)
            items = tuple(response_items(response))
            reported = provider_cost(response)
            if reported != charged:
                raise ContractError("provider-reported cost changed between capture and parse")
        except (CollectorError, ValueError):
            capture_raw(
                store,
                raw_call,
                snapshot_id=captured_id,
                validation_status=ValidationStatus.BLOCKED,
            )
            raise
        capture_raw(
            store,
            raw_call,
            snapshot_id=captured_id,
            row_count=len(items),
        )
        published = CapturedTransmission(
            snapshot_id=captured_id,
            raw_call=raw_call,
            validation_status=ValidationStatus.PASS,
            row_count=len(items),
        )
        transmissions.append(published)
        # The stable alias is an internal replay index only. 003/005 register
        # the single accepted attempt identity above, not this index.
        capture_raw(
            store,
            raw_call,
            snapshot_id=raw_snapshot_id(snapshot_id, symbol),
            row_count=len(items),
            accepted_transmission_snapshot_id=captured_id,
        )
        return CapturedSymbol(
            outcome=CallOutcome(
                symbol=symbol,
                status="COMPLETED",
                attempts=attempt,
                reserved_usd=reserved_total,
                actual_usd=charged_total,
                row_count=len(items),
                byte_count=raw_call.byte_length,
                content_sha256=raw_call.body_sha256,
                raw_snapshot_id=captured_id,
                rate_limit_headers=dict(raw_call.headers),
                transmission_snapshot_ids=tuple(item.snapshot_id for item in transmissions),
                transmission_content_sha256=tuple(
                    (item.snapshot_id, item.raw_call.body_sha256) for item in transmissions
                ),
            ),
            raw_call=raw_call,
            items=items,
            source_row_count=len(items),
            transmissions=tuple(transmissions),
        )
    raise CollectorError("retry loop terminated without a terminal outcome")


def _normalize_captured(
    captured: Sequence[CapturedSymbol],
    *,
    snapshot_id: str,
    observed_at: datetime,
    config: CollectorConfig,
    identity_export: IdentityExport,
) -> tuple[list[IdentityDecision], list[dict[str, object]]]:
    """Resolve identity from the pinned export and project normalized rows."""

    capture_sha = capture_receipt_sha256(
        snapshot_id,
        config,
        {
            item.outcome.symbol: str(item.outcome.content_sha256)
            for item in captured
            if item.outcome.content_sha256 is not None
        },
    )
    decisions: list[IdentityDecision] = []
    rows: list[dict[str, object]] = []
    for item in captured:
        if not item.outcome.completed:
            continue
        decision = identity_export.resolve(item.outcome.symbol)
        decisions.append(decision)
        rows.extend(
            normalize_items(
                item.items,
                symbol=item.outcome.symbol,
                snapshot_id=snapshot_id,
                observed_at=observed_at,
                identity=decision,
                source_receipt_sha256=capture_sha,
            )
        )
    return decisions, rows


def _require_execution_authority(
    synthetic: SyntheticExecution | None,
    *,
    transport: Transport,
    gate_evidence: GateEvidence,
    identity_export: IdentityExport,
    standing_authority: VerifiedRecurringAuthority | None,
) -> None:
    """Admit a run only as owner-authorized or as a bound offline execution."""

    if synthetic is None:
        if standing_authority is None:
            raise CollectorError(
                "standing authority is required for non-synthetic collection; "
                "no provider calls were attempted"
            )
        authorized_gate_digest(gate_evidence.artifact_sha256)
        authorized_identity_digest(identity_export.export_sha256)
        return
    # The capability must name this exact offline transport. A fabricated
    # capability paired with the live HTTPS transport is rejected here, before
    # the ledger and before any provider call.
    if type(transport) is not SyntheticTransport:
        raise AuthorityReferenceError("synthetic execution requires an offline SyntheticTransport")
    if synthetic.transport is not transport:
        raise AuthorityReferenceError("synthetic capability was issued for a different transport")


def _bind_standing_scope(
    standing_authority: VerifiedRecurringAuthority,
    *,
    config: CollectorConfig,
    raw_store_root: Path,
    dataset_root: Path,
) -> None:
    """Bind direct inputs to the signed destinations and spend ceiling."""

    from aegis_alpha.data.finimpulse_recurring_authority import (  # noqa: PLC0415 - ENDPOINT import cycle
        usd_to_micros,
    )

    resolved_raw = raw_store_root.resolve(strict=False)
    resolved_dataset = dataset_root.resolve(strict=False)
    if resolved_raw != standing_authority.raw_store_root or (
        resolved_dataset != standing_authority.dataset_root
    ):
        raise CollectorError(
            "run roots do not match the signed standing authority roots; "
            "no provider calls were attempted"
        )
    if usd_to_micros(config.budget_usd) > standing_authority.max_spend_micros:
        raise CollectorError(
            "budget_usd exceeds the signed standing authority max_spend_micros bound; "
            "no provider calls were attempted"
        )


def collect_snapshot(  # noqa: PLR0913, C901 - one bounded run has distinct injected collaborators
    *,
    snapshot_id: str,
    config: CollectorConfig,
    credential: str,
    transport: Transport,
    raw_store_root: Path,
    dataset_root: Path,
    identity_export: IdentityExport,
    gate_evidence: GateEvidence,
    predecessor_rows: Sequence[Mapping[str, object]] = (),
    observed_at: datetime | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    synthetic: SyntheticExecution | None = None,
    standing_authority: VerifiedRecurringAuthority | None = None,
    request_clock: Callable[[], datetime] | None = None,
    rate_limiter: RateLimiter | None = None,
    monotonic_clock: Callable[[], float] = time.monotonic,
) -> CollectionResult:
    """Run one bounded snapshot: capture, normalize, classify, then publish.

    Availability time equals capture time because the vendor exposes no temporal
    field, so ``observed_at`` is stamped by this collector.

    Both the G-B owner gates and the 007 identity export are hash-verified
    immutable evidence. Either one missing or mismatched stops the run before
    a single transport call is made.
    """

    if not credential:
        raise CollectorError(
            f"{CREDENTIAL_ENVIRONMENT_VARIABLE} is required; zero provider calls were made"
        )
    stamp_for_gates = utc_now() if observed_at is None else observed_at
    if stamp_for_gates.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    # Trusted-authority boundary. A run is admitted only when it is explicitly
    # synthetic, or when both artifacts carry owner-authorized digests. This
    # check lives here, not only in the CLI, so direct API use cannot bypass
    # the empty authority sets that keep G-B blocked.
    _require_execution_authority(
        synthetic,
        transport=transport,
        gate_evidence=gate_evidence,
        identity_export=identity_export,
        standing_authority=standing_authority,
    )
    request_guard: Callable[[], None] | None = None
    if standing_authority is not None:
        if request_clock is None:
            raise CollectorError("standing authority requires a current request clock")
        if rate_limiter is not None:
            raise CollectorError(
                "a caller-supplied rate_limiter cannot replace the standing rate limiter; "
                "inject clock and sleeper instead"
            )
        from aegis_alpha.data.finimpulse_recurring_authority import (  # noqa: PLC0415 - ENDPOINT import cycle
            BoundRecurringApproval,
        )

        _bind_standing_scope(
            standing_authority,
            config=config,
            raw_store_root=raw_store_root,
            dataset_root=dataset_root,
        )
        request_guard = BoundRecurringApproval(standing_authority, request_clock).require_request
        rate_limiter = StandingRateLimiter(
            calls_per_minute=standing_authority.calls_per_minute,
            clock=monotonic_clock,
            sleep=sleeper,
        )
    elif request_clock is not None or rate_limiter is not None:
        raise CollectorError("request-boundary controls require a verified standing authority")
    # Gate and identity checks run before the ledger and before any transport
    # call, so a blocked run writes no evidence and spends nothing.
    require_gb_gates(gate_evidence, config=config, now=stamp_for_gates)
    if config.identity_export_sha256 != identity_export.export_sha256:
        raise IdentityEvidenceError(
            "pinned identity_export_sha256 does not match the supplied 007 export"
        )
    if config.identity_as_of is not None and config.identity_as_of != identity_export.as_of_utc:
        raise IdentityEvidenceError(
            "pinned identity effective time does not match the supplied 007 export"
        )
    ledger = BudgetLedger(config)
    planned_reservation = ledger.preflight()
    stamp = stamp_for_gates
    store = ContentAddressedRawStore(raw_store_root)
    captured: list[CapturedSymbol] = []
    halt_reason: str | None = None
    for symbol in config.universe:
        try:
            captured.append(
                _capture_symbol(
                    symbol,
                    config=config,
                    credential=credential,
                    transport=transport,
                    store=store,
                    raw_store_root=raw_store_root,
                    snapshot_id=snapshot_id,
                    ledger=ledger,
                    sleeper=sleeper,
                    request_guard=request_guard,
                    rate_limiter=rate_limiter,
                )
            )
        except BudgetError as error:
            halt_reason = str(error)
            break
    outcomes = tuple(item.outcome for item in captured)
    # A run publishes only when every planned call completed. A call recorded
    # failed-with-error-class is terminal but still makes this a partial run,
    # which publishes nothing and is retried as a new snapshot.
    complete = (
        halt_reason is None
        and len(outcomes) == len(config.universe)
        and all(outcome.completed for outcome in outcomes)
    )
    identity_decisions: list[IdentityDecision] = []
    rows: list[dict[str, object]] = []
    manifest: RunManifest | None = None
    restatements: tuple[RestatementFinding, ...] = ()
    if complete:
        identity_decisions, rows = _normalize_captured(
            captured,
            snapshot_id=snapshot_id,
            observed_at=stamp,
            config=config,
            identity_export=identity_export,
        )
        restatements = classify_restatements(predecessor_rows, rows)
        manifest = publish_snapshot(
            dataset_root,
            rows,
            snapshot_id=snapshot_id,
            observed_at=stamp,
            config=config,
        )
    elif halt_reason is None:
        halt_reason = "one or more planned calls did not complete; nothing was published"
    receipt = build_receipt(
        snapshot_id=snapshot_id,
        observed_at=stamp,
        config=config,
        ledger=ledger,
        planned_reservation=planned_reservation,
        outcomes=outcomes,
        identity_decisions=identity_decisions,
        restatements=restatements,
        manifest=manifest,
        published=manifest is not None,
        halt_reason=halt_reason,
        gate_evidence=gate_evidence,
        standing_authority=standing_authority,
    )
    return CollectionResult(
        snapshot_id=snapshot_id,
        observed_at=stamp,
        outcomes=outcomes,
        identity_decisions=tuple(identity_decisions),
        rows=tuple(rows),
        manifest=manifest,
        restatements=restatements,
        receipt=receipt,
        published=manifest is not None,
        captured=tuple(captured),
    )


def replay_from_raw(
    raw_calls: Sequence[RawCall],
    *,
    receipt: Mapping[str, object],
    config: CollectorConfig,
    identity_export: IdentityExport,
) -> list[dict[str, object]]:
    """Rebuild normalized rows from raw content plus the receipt's pinned inputs.

    Replay resolves identity only through the hash-verified export named by the
    receipt, never a live registry, so later registry drift cannot change the
    logical output of a replay.
    """

    validate_receipt(receipt)
    pinned = cast("Mapping[str, object]", receipt["pinned_replay_inputs"])
    if pinned["config_sha256"] != config.digest():
        raise ContractError("replay config does not match the config pinned in the receipt")
    if pinned["identity_export_sha256"] != identity_export.export_sha256:
        raise IdentityEvidenceError(
            "replay identity export does not match the export pinned in the receipt"
        )
    raw_hashes = cast("Mapping[str, object]", pinned["raw_content_sha256"])
    snapshot_id = str(receipt["snapshot_id"])
    observed_at = parse_instant(str(receipt["observed_at_utc"]))
    identity_as_of_literal = pinned["identity_as_of_utc"]
    identity_as_of = (
        observed_at
        if identity_as_of_literal is None
        else parse_instant(str(identity_as_of_literal))
    )
    if identity_as_of != identity_export.as_of_utc:
        raise IdentityEvidenceError(
            "replay identity effective time does not match the pinned export"
        )
    rows: list[dict[str, object]] = []
    capture_sha = capture_receipt_sha256(
        snapshot_id,
        config,
        {str(symbol): str(value) for symbol, value in raw_hashes.items()},
    )
    for raw_call in raw_calls:
        expected = raw_hashes.get(raw_call.symbol)
        if expected is None:
            raise ContractError(f"receipt does not pin raw content for {raw_call.symbol}")
        if raw_call.body_sha256 != expected:
            raise ContractError(f"raw content hash mismatch for {raw_call.symbol}")
        response = parse_response(raw_call)
        decision = identity_export.resolve(raw_call.symbol)
        rows.extend(
            normalize_items(
                response_items(response),
                symbol=raw_call.symbol,
                snapshot_id=snapshot_id,
                observed_at=observed_at,
                identity=decision,
                source_receipt_sha256=capture_sha,
            )
        )
    return sort_rows(rows)


def build_run_plan(
    plan_id: str,
    config: CollectorConfig,
    *,
    created_at_utc: datetime,
    result: CollectionResult | None = None,
) -> CollectionRunPlan:
    """Register this snapshot's inputs as a 005 run plan.

    Mode is ``PROBE`` because 008C grants no scheduled or canonical collection.
    """

    parameters: dict[str, object] = {
        "config_sha256": config.digest(),
        "page_limit": config.page_limit,
        "schema_id": SCHEMA_ID,
        "types": list(ESTIMATE_TYPES),
        "universe_size": len(config.universe),
        "universe_sha256": universe_sha256(config.universe),
    }
    if result is not None:
        parameters.update(_standing_grant_evidence(result))
    return CollectionRunPlan(
        plan_id=plan_id,
        schema_version=SCHEMA_VERSION,
        provider=PROVIDER,
        dataset=DATASET,
        mode=CollectionMode.PROBE,
        requested_window_start=None,
        requested_window_end=None,
        parameters=parameters,
        created_at_utc=created_at_utc,
    )


def build_run(run_id: str, plan_id: str, *, created_at_utc: datetime) -> CollectionRun:
    return CollectionRun(run_id=run_id, plan_id=plan_id, created_at_utc=created_at_utc)


def build_run_events(
    run_id: str,
    result: CollectionResult,
) -> tuple[CollectionRunEvent, ...]:
    """Project one snapshot as a single 005 attempt lifecycle.

    AAS-DATA-005 models one attempt sequence per run, so a snapshot is one
    attempt covering the whole universe. Per-symbol outcomes are carried as
    event details rather than as separate attempts.
    """

    per_symbol = {
        outcome.symbol: {
            "attempts": outcome.attempts,
            "error_class": outcome.error_class,
            "row_count": outcome.row_count,
            "status": outcome.status,
        }
        for outcome in result.outcomes
    }
    started = CollectionRunEvent(
        run_id=run_id,
        event_type=RunEventType.ATTEMPT_STARTED,
        occurred_at_utc=result.observed_at,
        attempt_number=1,
        details={
            "snapshot_id": result.snapshot_id,
            "symbols": sorted(per_symbol),
            **_standing_grant_evidence(result),
        },
    )
    if result.published:
        return (
            started,
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.ATTEMPT_SUCCEEDED,
                occurred_at_utc=result.observed_at,
                attempt_number=1,
                details={"per_symbol": per_symbol, "snapshot_id": result.snapshot_id},
            ),
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.RUN_SUCCEEDED,
                occurred_at_utc=result.observed_at,
                details={"snapshot_id": result.snapshot_id},
            ),
        )
    failed_classes = sorted(
        {outcome.error_class for outcome in result.outcomes if outcome.error_class}
    )
    return (
        started,
        CollectionRunEvent(
            run_id=run_id,
            event_type=RunEventType.ATTEMPT_FAILED,
            occurred_at_utc=result.observed_at,
            attempt_number=1,
            error_class=failed_classes[0] if failed_classes else "run_not_published",
            details={"per_symbol": per_symbol, "snapshot_id": result.snapshot_id},
        ),
        CollectionRunEvent(
            run_id=run_id,
            event_type=RunEventType.RUN_FAILED,
            occurred_at_utc=result.observed_at,
            error_class="partial_run_not_published",
            details={"snapshot_id": result.snapshot_id},
        ),
    )


def _unique_transmissions(result: CollectionResult) -> tuple[CapturedTransmission, ...]:
    """Return each immutable transmission once in deterministic capture order."""

    unique: dict[str, CapturedTransmission] = {}
    for captured in result.captured:
        for transmission in captured.transmissions:
            existing = unique.get(transmission.snapshot_id)
            if (
                existing is not None
                and existing.raw_call.body_sha256 != transmission.raw_call.body_sha256
            ):
                raise PublicationError(
                    f"transmission identity has conflicting content: {transmission.snapshot_id}"
                )
            unique.setdefault(transmission.snapshot_id, transmission)
    return tuple(unique.values())


def build_collection_receipts(
    run_id: str,
    result: CollectionResult,
) -> tuple[CollectionReceipt, ...]:
    """Reference each immutable raw capture from the 005 receipt table."""

    digest = receipt_sha256(result.receipt)
    return tuple(
        (
            CollectionReceipt(
                run_id=run_id,
                attempt_number=1,
                source_snapshot_id=transmission.snapshot_id,
                observed_window_start=result.observed_at,
                observed_window_end=result.observed_at,
                row_count=0 if transmission.row_count is None else transmission.row_count,
                byte_count=transmission.raw_call.byte_length,
                receipt_sha256=digest,
            )
        )
        for transmission in _unique_transmissions(result)
    )


def build_usage_records(
    run_id: str,
    result: CollectionResult,
    *,
    recorded_at_utc: datetime,
) -> tuple[CollectionUsageRecord, ...]:
    """Record reserved and actual provider cost as 005 usage evidence."""

    reserved = sum((outcome.reserved_usd for outcome in result.outcomes), Decimal(0))
    actual = sum((outcome.actual_usd for outcome in result.outcomes), Decimal(0))
    quantum = Decimal("0.000001")
    return (
        CollectionUsageRecord(
            run_id=run_id,
            usage_seq=1,
            metric="provider_cost_reserved_usd",
            quantity=reserved.quantize(quantum),
            unit="USD",
            recorded_at_utc=recorded_at_utc,
            evidence={"snapshot_id": result.snapshot_id, **_standing_grant_evidence(result)},
        ),
        CollectionUsageRecord(
            run_id=run_id,
            usage_seq=2,
            metric="provider_cost_actual_usd",
            quantity=actual.quantize(quantum),
            unit="USD",
            recorded_at_utc=recorded_at_utc,
            evidence={"snapshot_id": result.snapshot_id, **_standing_grant_evidence(result)},
        ),
    )


def build_source_registrations(
    result: CollectionResult,
    config: CollectorConfig,
) -> tuple[SourceSnapshotRegistration, ...]:
    """Project each immutable raw capture as a registrable 005 source snapshot."""

    registrations: list[SourceSnapshotRegistration] = []
    for transmission in _unique_transmissions(result):
        snapshot = build_source_snapshot(
            transmission.raw_call,
            snapshot_id=transmission.snapshot_id,
            row_count=transmission.row_count,
            validation_status=transmission.validation_status,
        )
        files = (
            SourceSnapshotFile(
                f"{transmission.raw_call.symbol}.json",
                transmission.raw_call.byte_length,
                transmission.raw_call.body_sha256,
            ),
        )
        registrations.append(
            SourceSnapshotRegistration(
                snapshot=snapshot,
                tree_sha256=source_tree_digest(files),
                files=files,
                manifest={
                    "config_sha256": config.digest(),
                    "schema_id": SCHEMA_ID,
                    "snapshot_id": result.snapshot_id,
                    "symbol": transmission.raw_call.symbol,
                    "transmission_snapshot_id": transmission.snapshot_id,
                },
            )
        )
    return tuple(registrations)


def register_collection_lifecycle(  # noqa: PLR0913 - one lifecycle spans distinct registries
    result: CollectionResult,
    config: CollectorConfig,
    *,
    collection_registry: CollectionRegistry,
    metadata_registry: MetadataRegistry,
    plan_id: str,
    run_id: str,
    recorded_at_utc: datetime,
) -> None:
    """Persist the whole 005 lifecycle for one snapshot, idempotently.

    Every registry operation is immutable and idempotent.  If a process dies
    between their individual transactions, a retry validates the projections
    already present and appends only the missing event suffix, receipts, and
    usage rows.  It therefore converges to one complete lifecycle without
    deleting or compensating durable evidence.
    """

    plan = build_run_plan(plan_id, config, created_at_utc=recorded_at_utc, result=result)
    collection_registry.register_plan(plan)
    collection_registry.start_run(build_run(run_id, plan_id, created_at_utc=recorded_at_utc))
    for registration in build_source_registrations(result, config):
        metadata_registry.register_source_snapshot(registration)
    # Resume after the current event rather than replaying from the beginning.
    # append_event is idempotent only for the latest event; selecting the
    # missing suffix also covers crashes after attempt_started or
    # attempt_succeeded/failed.
    expected_events = build_run_events(run_id, result)
    state = collection_registry.current_run_state(run_id)
    remaining_events = expected_events
    if state is not None and state.state is not None:
        matching = [
            index for index, event in enumerate(expected_events) if event.event_type is state.state
        ]
        if not matching:
            raise CollectorError(
                f"stored lifecycle state conflicts with the receipt: {state.state.value}"
            )
        remaining_events = expected_events[matching[-1] + 1 :]
    terminal_event = expected_events[-1]
    for event in remaining_events:
        if event is terminal_event:
            continue
        collection_registry.append_event(event)
    for receipt in build_collection_receipts(run_id, result):
        collection_registry.record_receipt(receipt)
    for record in build_usage_records(run_id, result, recorded_at_utc=recorded_at_utc):
        collection_registry.record_usage(record)
    if terminal_event in remaining_events:
        collection_registry.append_event(terminal_event)


#: Every column the 003/005 lifecycle writes, per table. A schema that is
#: missing any of these is incompatible even when the table name exists.
REQUIRED_LIFECYCLE_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "source_snapshots": (
        "snapshot_id",
        "provider",
        "dataset",
        "content_sha256",
        "tree_sha256",
        "validation_status",
    ),
    "collection_run_plans": (
        "plan_id",
        "provider",
        "dataset",
        "mode",
        "parameters_json",
        "plan_sha256",
    ),
    "collection_runs": ("run_id", "plan_id", "created_at_utc"),
    "collection_run_events": ("run_id", "event_type", "attempt_number", "occurred_at_utc"),
    "collection_run_receipts": ("run_id", "attempt_number", "source_snapshot_id", "receipt_sha256"),
    "collection_usage_records": ("run_id", "usage_seq", "metric", "quantity", "unit"),
}
