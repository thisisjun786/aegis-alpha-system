from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Final, Protocol, assert_never, cast

from aegis_alpha.data.fmp_normalize import Provenance, normalize_records
from aegis_alpha.data.fmp_rate_limit import RateLimiter
from aegis_alpha.data.fmp_rate_types import (
    DurableRequestState,
    DurableRetry,
    DurableTerminalRequest,
    EntitlementError,
    FailureClass,
    RateResumeState,
    RetryCeilingError,
    RetryObligation,
    TerminalRequestDisposition,
    UnexpectedStatusError,
    classify_failure,
)
from aegis_alpha.data.fmp_response_security import (
    CredentialLeakError,
    response_contains_credential,
)
from aegis_alpha.data.fmp_windows import CollectorContractError
from aegis_alpha.data.serialization import canonical_json_bytes

PROVIDER_ERROR_KEYS: Final = frozenset({"Note", "Error Message", "Errors", "error-message"})
PROVIDER_RECORD_DATA_KEYS: Final = frozenset(
    {
        "historical",
        "historicalStockList",
        "stockList",
        "symbol",
        "date",
        "data",
        "results",
    }
)
DURABLE_RESPONSE_KEYS: Final = frozenset(
    (
        "attempt_record_sha256",
        "attempt_seq",
        "content_sha256",
        "page",
        "raw_byte_length",
        "request_fingerprint",
        "requested_at_utc",
        "response_headers",
        "retrieved_at_utc",
        "run_identity",
        "source_uri",
        "status_code",
        "symbol",
    )
)
_HTTP_SUCCESS_MIN: Final = 200
_HTTP_REDIRECT_MIN: Final = 300


@dataclass(frozen=True, slots=True)
class DurableResponse:
    request_fingerprint: str
    status_code: int
    response_headers: Mapping[str, str]
    body: bytes
    requested_at_utc: datetime
    retrieved_at_utc: datetime
    provenance: bytes


@dataclass(frozen=True, slots=True)
class RejectedResponseAttempt:
    """Credential-free accounting projection for a securely rejected body."""

    status_code: int
    raw_byte_length: int
    request_attempt_index: int
    failure: FailureClass | None


@dataclass(frozen=True, slots=True)
class ReconciledAttempts:
    rate_state: RateResumeState
    responses: tuple[DurableResponse, ...]
    request_states: tuple[DurableRequestState, ...]
    next_request_not_before_utc: datetime | None


@dataclass(frozen=True, slots=True)
class DurableResponseEvidence:
    attempt: Mapping[str, object]
    provenance: bytes
    record: Mapping[str, object]


class AttemptRequest(Protocol):
    @property
    def request_fingerprint(self) -> str: ...


class ProviderResponse(Protocol):
    """Response fields required to validate and normalize provider observations."""

    @property
    def status_code(self) -> int: ...

    @property
    def headers(self) -> Mapping[str, str]: ...

    @property
    def body(self) -> bytes: ...

    @property
    def raw_headers(self) -> tuple[tuple[str, str], ...]: ...

    @property
    def retrieved_at_utc(self) -> datetime: ...

    @property
    def content_sha256(self) -> str: ...


class TransportFailureKind(StrEnum):
    NETWORK_ERROR = "network_error"
    TIMEOUT = "timeout"


@dataclass(frozen=True, slots=True)
class TransportFailureAttempt:
    kind: TransportFailureKind
    request_attempt_index: int
    retry: RetryObligation | None


@dataclass(frozen=True, slots=True)
class ResponseAttempt:
    response: ProviderResponse
    request_attempt_index: int
    retry: RetryObligation | None


type AttemptOutcome = TransportFailureAttempt | ResponseAttempt | RejectedResponseAttempt


@dataclass(frozen=True, slots=True)
class SymbolObservationRequest:
    dataset: str
    symbol: str
    source_receipt_id: str


@dataclass(frozen=True, slots=True)
class SymbolObservationResult:
    rows: tuple[Mapping[str, object], ...]
    unknown_fields: tuple[str, ...]
    is_empty: bool


type CollectedSymbolRows = tuple[tuple[Mapping[str, object], ...], tuple[bytes, ...]]


def resume_request_state(state: DurableRequestState) -> int:
    """Honor one durable request state before any resumed transport."""

    match state:
        case DurableRetry(request_attempt_index=index):
            return index + 1
        case DurableTerminalRequest(disposition=disposition):
            match disposition:
                case TerminalRequestDisposition.CREDENTIAL_REJECTED:
                    raise CredentialLeakError
                case TerminalRequestDisposition.ENTITLEMENT:
                    raise EntitlementError(
                        "provider returned an entitlement failure; the run is blocked"
                    )
                case TerminalRequestDisposition.RETRY_CEILING:
                    raise RetryCeilingError("request failed 5 times; the run is blocked")
                case TerminalRequestDisposition.UNEXPECTED_STATUS:
                    raise UnexpectedStatusError(state.status_code or 0)
                case unreachable:
                    assert_never(unreachable)
        case unreachable:
            assert_never(unreachable)


def reconcile_durable_response(
    raw_root: Path, evidence: DurableResponseEvidence
) -> DurableResponse:
    """Reconcile one capturable response against its body and provenance."""

    attempt = evidence.attempt
    record = evidence.record
    digest = str(record["content_sha256"])
    blob = raw_root / "fmp" / "blobs" / "sha256" / digest[:2] / f"{digest}.raw"
    body = blob.read_bytes()
    if (
        record["request_fingerprint"] != attempt["request_fingerprint"]
        or record["attempt_record_sha256"]
        != hashlib.sha256(canonical_json_bytes(attempt)).hexdigest()
        or record["status_code"] != attempt["status_code"]
        or digest != attempt["content_sha256"]
        or record["raw_byte_length"] != attempt["raw_byte_length"]
        or len(body) != int(str(record["raw_byte_length"]))
        or hashlib.sha256(body).hexdigest() != digest
    ):
        raise CollectorContractError(
            "durable response provenance conflicts with its unique attempt"
        )
    return DurableResponse(
        request_fingerprint=str(record["request_fingerprint"]),
        status_code=int(str(record["status_code"])),
        response_headers=cast("Mapping[str, str]", record["response_headers"]),
        body=body,
        requested_at_utc=datetime.fromisoformat(str(record["requested_at_utc"])),
        retrieved_at_utc=datetime.fromisoformat(str(record["retrieved_at_utc"])),
        provenance=evidence.provenance,
    )


def classify_response(response: ProviderResponse) -> FailureClass | None:
    """Classify one response without changing usage state."""

    if _HTTP_SUCCESS_MIN <= response.status_code < _HTTP_REDIRECT_MIN:
        return None
    return classify_failure(response.status_code)


def sanitized_credential_rejection(
    response: ProviderResponse, credential: str, attempt_index: int
) -> RejectedResponseAttempt | None:
    """Return accounting-only metadata when a body contains its credential."""

    if not response_contains_credential(response, credential):
        return None
    try:
        failure = classify_response(response)
    except UnexpectedStatusError:
        failure = None
    return RejectedResponseAttempt(
        status_code=response.status_code,
        raw_byte_length=len(response.body),
        request_attempt_index=attempt_index,
        failure=failure,
    )


def classify_and_account_response(
    response: ProviderResponse, limiter: RateLimiter
) -> FailureClass | None:
    """Classify and account one response before any cutoff can interrupt the run."""

    try:
        failure = classify_response(response)
    except UnexpectedStatusError:
        limiter.after_response(byte_count=len(response.body))
        raise
    limiter.after_response(byte_count=len(response.body), failure=failure)
    return failure


def _reject_provider_error_object(parsed: Mapping[str, object]) -> None:
    """Fail closed on HTTP-200 FMP error objects that would parse as no-data."""

    if any(key in parsed for key in PROVIDER_RECORD_DATA_KEYS):
        return
    for key in PROVIDER_ERROR_KEYS:
        value = parsed.get(key)
        if isinstance(value, str) and value.strip():
            snippet = value.strip()[:80]
            raise CollectorContractError(
                f"provider error object {key!r}={snippet!r} is not an empty dataset"
            )


def parse_provider_records(response: ProviderResponse) -> list[Mapping[str, object]]:
    """Parse one JSON-array provider response into object records."""

    content_type = next(
        (value for name, value in response.headers.items() if name.casefold() == "content-type"),
        "",
    )
    if "json" not in content_type.casefold():
        raise CollectorContractError("provider response is not a JSON content type")
    try:
        parsed = json.loads(response.body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise CollectorContractError("provider response body is not valid JSON") from None
    if isinstance(parsed, Mapping):
        _reject_provider_error_object(parsed)
        raise CollectorContractError("provider response root must be a JSON array")
    if not isinstance(parsed, list):
        raise CollectorContractError("provider response root must be a JSON array")
    records: list[Mapping[str, object]] = []
    for record in parsed:
        if not isinstance(record, Mapping):
            raise CollectorContractError("provider response records must be JSON objects")
        records.append(record)
    return records


def normalize_symbol_observation(
    request: SymbolObservationRequest,
    response: ProviderResponse,
) -> SymbolObservationResult:
    """Normalize a symbol-only endpoint response without adding date parameters."""

    records = parse_provider_records(response)
    normalized = normalize_records(
        dataset=request.dataset,
        records=records,
        provenance=Provenance(
            source_receipt_id=request.source_receipt_id,
            raw_content_sha256=response.content_sha256,
            retrieved_at_utc=response.retrieved_at_utc,
        ),
        symbol=request.symbol,
    )
    return SymbolObservationResult(
        rows=normalized.rows,
        unknown_fields=normalized.unknown_fields,
        is_empty=not records,
    )
