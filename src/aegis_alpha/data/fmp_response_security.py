"""Credential-safe raw response inspection and evidence header redaction."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Final, Protocol

_MAX_RAW_RESPONSE_BYTES: Final = 32 * 1024 * 1024


class CredentialLeakError(RuntimeError):
    """Credential bytes were securely rejected before durable evidence."""

    def __init__(self) -> None:
        super().__init__("credential material detected in provider or evidence bytes")


@dataclass(frozen=True, slots=True)
class CollectorResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes
    requested_at_utc: datetime
    retrieved_at_utc: datetime
    raw_headers: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.requested_at_utc.tzinfo is None or self.retrieved_at_utc.tzinfo is None:
            raise ValueError("collector timestamps must be timezone-aware")
        if len(self.body) > _MAX_RAW_RESPONSE_BYTES:
            raise ValueError("provider response exceeds the collector byte limit")

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()


class SecurityScannableResponse(Protocol):
    @property
    def body(self) -> bytes: ...

    @property
    def headers(self) -> Mapping[str, str]: ...

    @property
    def raw_headers(self) -> tuple[tuple[str, str], ...]: ...


def response_contains_credential(response: SecurityScannableResponse, credential: str) -> bool:
    """Scan body plus every ordered raw header name and value."""

    if not credential:
        return False
    needle = credential.encode()
    if needle in response.body:
        return True
    headers = response.raw_headers or tuple(response.headers.items())
    return any(credential in name or credential in value for name, value in headers)


def assert_credential_absent(credential: str, *payloads: bytes) -> None:
    """Reject credential material before evidence crosses a durable boundary."""

    if credential and any(credential.encode() in payload for payload in payloads):
        raise CredentialLeakError


def redact_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Redact a normalized mapping at a logging or evidence boundary."""

    return redact_raw_headers(headers.items())


def redact_raw_headers(headers: Iterable[tuple[str, str]]) -> dict[str, str]:
    """Redact ordered raw headers while collapsing only the safe projection."""

    redacted: dict[str, str] = {}
    for name, value in headers:
        folded = name.casefold()
        redacted[folded] = (
            "[REDACTED]"
            if folded in {"apikey", "authorization", "x-api-key", "cookie", "set-cookie"}
            else value
        )
    return redacted
