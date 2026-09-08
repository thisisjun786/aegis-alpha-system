"""Finite invocation admission layered over durable Qveris page accounting."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING

from aegis_alpha.data.qveris_contracts import credit_value

if TYPE_CHECKING:
    from aegis_alpha.data.qveris_billing import QverisPort
    from aegis_alpha.data.qveris_client import QverisResponse


@dataclass(slots=True)
class InvocationBudget:
    """Reserve exact quotes before a durable intent or paid request exists.

    Reservations are never refunded within the invocation: uncertain attempts
    consume the full quote. The acquisition store owns restart accounting.
    """

    max_calls: int
    max_credits: Decimal
    reserved_calls: int = field(default=0, init=False)
    reserved_credits: Decimal = field(default=Decimal(0), init=False)

    def __post_init__(self) -> None:
        if type(self.max_calls) is not int or self.max_calls < 1:
            raise ValueError("max_calls must be a positive integer")
        self.max_credits = credit_value(self.max_credits)

    def reserve(self, quote: Decimal) -> None:
        amount = credit_value(quote)
        if self.reserved_calls >= self.max_calls:
            raise RuntimeError("INVOCATION_CALL_LIMIT: execute was not attempted")
        if self.reserved_credits + amount > self.max_credits:
            raise RuntimeError("INVOCATION_CREDIT_LIMIT: execute was not attempted")
        self.reserved_calls += 1
        self.reserved_credits += amount


class RequestBudgetPort:
    """Bound all HTTP attempts separately from paid tool executions."""

    def __init__(self, client: QverisPort, max_requests: int, seconds: float) -> None:
        if type(max_requests) is not int or max_requests < 1:
            raise ValueError("HTTP request limit must be positive")
        if (
            isinstance(seconds, bool)
            or not isinstance(seconds, (int, float))
            or not math.isfinite(seconds)
            or seconds <= 0
        ):
            raise ValueError("request time limit must be finite and positive")
        self.client = client
        self.max_requests = max_requests
        self.deadline = time.monotonic() + seconds
        self.http_requests = 0
        self.paid_executions = 0

    @property
    def account_key(self) -> str:
        return self.client.account_key

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        if self.http_requests >= self.max_requests or time.monotonic() >= self.deadline:
            raise RuntimeError("INVOCATION_HTTP_LIMIT: request was not attempted")
        self.http_requests += 1
        if path == "/tools/execute":
            self.paid_executions += 1
        return self.client.request(path, body=body, query=query)
