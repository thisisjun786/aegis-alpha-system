"""Provider requests and answers shared by the native US collectors (SEC, FRED/ALFRED).

A request is a provider, an endpoint and its exact parameters, nothing else: no
credential, no contact header and no day it is asked. Its fingerprint hashes
``aas-<provider>-request-v1`` with the endpoint and the canonical parameter JSON, so asking
the same question on another day is another attempt of the same request.

An answer is the HTTP status, the retained headers, the body and the instants the call
started and ended. Classifying an answer only routes the collector; the bytes are always
retained and the promotion mappers judge them.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Final, cast

from aegis_alpha.data.opendart import canonical

COMPLETED: Final = "COMPLETED"
NO_DATA: Final = "NO_DATA"
FAILED: Final = "FAILED"
OUTCOMES: Final = frozenset({COMPLETED, NO_DATA, FAILED})
_PROVIDER: Final = re.compile(r"[a-z][a-z0-9]*")
_ENDPOINT: Final = re.compile(r"[a-z][a-z0-9_]*")
_HTTP_MIN, _HTTP_MAX = 100, 599


@dataclass(frozen=True, slots=True)
class Request:
    """One provider question: a provider, an endpoint and its exact text parameters."""

    provider: str
    endpoint: str
    parameters_json: str

    def __post_init__(self) -> None:
        if _PROVIDER.fullmatch(self.provider) is None or _ENDPOINT.fullmatch(self.endpoint) is None:
            raise ValueError("a request names a lowercase provider and endpoint")
        try:
            parameters = json.loads(self.parameters_json)
        except (TypeError, json.JSONDecodeError):
            raise ValueError("request parameters must be JSON") from None
        if not isinstance(parameters, dict) or any(
            not isinstance(value, str) for value in parameters.values()
        ):
            raise ValueError("request parameters must be a JSON object of text values")
        if canonical(parameters) != self.parameters_json:
            raise ValueError("request parameters must be canonical JSON")

    @classmethod
    def of(cls, provider: str, endpoint: str, parameters: Mapping[str, str]) -> Request:
        return cls(provider, endpoint, canonical(dict(parameters)))

    @property
    def parameters(self) -> dict[str, str]:
        return cast("dict[str, str]", json.loads(self.parameters_json))

    @property
    def document(self) -> dict[str, str]:
        """The request as receipts and source rows record it."""
        return {"endpoint": self.endpoint, "parameters_json": self.parameters_json}

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(
            canonical(
                [f"aas-{self.provider}-request-v1", self.endpoint, self.parameters_json]
            ).encode()
        ).hexdigest()

    @classmethod
    def from_document(cls, provider: str, document: object) -> Request:
        if not isinstance(document, Mapping):
            raise ValueError("a request document is a JSON object")  # noqa: TRY004 -- malformed-content ValueError contract
        body = cast("Mapping[str, object]", document)
        if set(body) != {"endpoint", "parameters_json"}:
            raise ValueError("a request document holds exactly its endpoint and parameters")
        endpoint, parameters = body["endpoint"], body["parameters_json"]
        if not isinstance(endpoint, str) or not isinstance(parameters, str):
            raise ValueError("a request document names its endpoint and parameters as text")  # noqa: TRY004 -- malformed-content ValueError contract
        return cls(provider, endpoint, parameters)


@dataclass(frozen=True, slots=True)
class Response:
    """One provider answer: HTTP status, retained headers, bytes and the call's instants."""

    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes
    requested_at: datetime
    retrieved_at: datetime

    def __post_init__(self) -> None:
        if type(self.status) is not int or not _HTTP_MIN <= self.status <= _HTTP_MAX:
            raise ValueError("invalid HTTP status")
        if type(self.body) is not bytes:
            raise TypeError("a response body is bytes")
        for moment in (self.requested_at, self.retrieved_at):
            if moment.tzinfo is None or moment.utcoffset() is None:
                raise ValueError("response instants must be timezone-aware")
        if self.retrieved_at < self.requested_at:
            raise ValueError("a response is retrieved after it is requested")
