from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, localcontext

import pytest

from aegis_alpha.data.qveris import InvocationBudget
from aegis_alpha.data.qveris_client import QverisResponse


def test_exact_wire_quote_above_cap_cannot_round_into_budget() -> None:
    # Given an exact wire amount exceeding the cap beyond Decimal's default precision.
    at = datetime(2026, 1, 1, tzinfo=UTC)
    response = QverisResponse(
        200,
        (),
        b'{"amount":0.100000000000000000000000000000001}',
        at,
        at,
    )
    amount = response.document()["amount"]
    assert isinstance(amount, Decimal)
    budget = InvocationBudget(2, Decimal("0.1"))
    assert amount > budget.max_credits
    # When admission checks that quote in the ordinary arithmetic context.
    with localcontext() as context:
        context.prec = 28
        with pytest.raises(RuntimeError):
            budget.reserve(amount)
    # Then no reservation can precede a paid execution.
    assert budget.reserved_calls == 0
    assert budget.reserved_credits == Decimal(0)


@pytest.mark.parametrize("cap", [Decimal("0.1"), Decimal(1)])
def test_rounded_aggregate_is_rejected_without_changing_reserved_quotes(cap: Decimal) -> None:
    # Given a prior exact quote and a new amount that would disappear in addition.
    budget = InvocationBudget(2, cap)
    budget.reserve(Decimal("0.1"))
    # When adding it would produce an inexact aggregate, even below a larger cap.
    with localcontext() as context:
        context.prec = 28
        with pytest.raises(RuntimeError):
            budget.reserve(Decimal("1e-40"))
    # Then the original admitted quote remains exact and the attempt is not reserved.
    assert budget.reserved_calls == 1
    assert budget.reserved_credits == Decimal("0.1")
