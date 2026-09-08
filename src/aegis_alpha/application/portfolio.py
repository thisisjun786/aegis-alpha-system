from __future__ import annotations

from fractions import Fraction

from aegis_alpha.application.contracts import PortfolioRequest


def compose_portfolio(request: PortfolioRequest) -> dict[str, object]:
    """Combine funded local allocations with exact arithmetic and explicit residual cash."""
    budgets = {budget.module: Fraction(budget.weight) for budget in request.budgets}
    positions: dict[str, Fraction] = {}
    modules: list[dict[str, object]] = []
    for allocation in sorted(request.modules, key=lambda item: item.module):
        budget = budgets[allocation.module]
        invested = Fraction()
        contributions: list[dict[str, object]] = []
        for target in sorted(allocation.targets, key=lambda item: item.instrument_id):
            contribution = budget * Fraction(target.weight)
            positions[target.instrument_id] = (
                positions.get(target.instrument_id, Fraction()) + contribution
            )
            invested += contribution
            contributions.append(
                {"instrument_id": target.instrument_id, "weight": float(contribution)}
            )
        modules.append(
            {
                "module": allocation.module.value,
                "strategy_id": allocation.strategy_id,
                "strategy_version": allocation.strategy_version,
                "budget": float(budget),
                "cash_weight": float(budget - invested),
                "contributions": contributions,
            }
        )
    return {
        "schema_version": request.schema_version,
        "as_of": request.as_of.isoformat(),
        "positions": [
            {"instrument_id": instrument, "weight": float(weight)}
            for instrument, weight in sorted(positions.items())
            if weight
        ],
        "cash_weight": float(1 - sum(positions.values(), Fraction())),
        "modules": modules,
        "execution_enabled": False,
    }
