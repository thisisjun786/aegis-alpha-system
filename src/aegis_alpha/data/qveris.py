"""Finite invocation admission layered over durable Qveris page accounting."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from aegis_alpha.data.qveris_contracts import credit_value


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
