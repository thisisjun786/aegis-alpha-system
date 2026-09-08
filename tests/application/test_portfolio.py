from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal, localcontext

from aegis_alpha.application.contracts import parse_request
from aegis_alpha.application.portfolio import compose_portfolio
from aegis_alpha.modules.catalog import module_catalog
from tests.application.test_application_contracts import EXAMPLE


def test_independent_allocation_oracle_and_lineage() -> None:
    output = compose_portfolio(parse_request(EXAMPLE))
    assert output["positions"] == [
        {"instrument_id": "asset:equity-basket", "weight": 0.58},
        {"instrument_id": "asset:protective-basket", "weight": 0.1},
        {"instrument_id": "asset:stock-b", "weight": 0.15},
    ]
    assert output["cash_weight"] == 0.17  # noqa: PLR2004 -- independent hand-calculated oracle
    assert output["schema_version"] == 1
    assert output["as_of"] == "2026-09-05"
    assert output["execution_enabled"] is False
    assert output["modules"] == [
        {
            "module": "aegis",
            "strategy_id": "demo-aegis",
            "strategy_version": "1",
            "budget": 0.6,
            "cash_weight": 0.12,
            "contributions": [{"instrument_id": "asset:equity-basket", "weight": 0.48}],
        },
        {
            "module": "alpha",
            "strategy_id": "demo-alpha",
            "strategy_version": "1",
            "budget": 0.25,
            "cash_weight": 0.0,
            "contributions": [
                {"instrument_id": "asset:equity-basket", "weight": 0.1},
                {"instrument_id": "asset:stock-b", "weight": 0.15},
            ],
        },
        {
            "module": "hedge",
            "strategy_id": "demo-hedge",
            "strategy_version": "1",
            "budget": 0.1,
            "cash_weight": 0.0,
            "contributions": [{"instrument_id": "asset:protective-basket", "weight": 0.1}],
        },
    ]
    assert json.loads(json.dumps(output, allow_nan=False)) == output


def test_order_and_decimal_context_do_not_change_output() -> None:
    request = parse_request(EXAMPLE)
    shuffled = replace(
        request,
        budgets=tuple(reversed(request.budgets)),
        modules=tuple(
            replace(m, targets=tuple(reversed(m.targets))) for m in reversed(request.modules)
        ),
    )
    expected = json.dumps(compose_portfolio(request))
    with localcontext() as context:
        context.prec = 2
        assert json.dumps(compose_portfolio(shuffled)) == expected


def test_empty_targets_and_zero_budgets_leave_cash_without_redistribution() -> None:
    request = parse_request(EXAMPLE)
    empty = replace(request, modules=tuple(replace(m, targets=()) for m in request.modules))
    assert compose_portfolio(empty)["cash_weight"] == 1
    assert compose_portfolio(empty)["positions"] == []
    zero = replace(request, budgets=tuple(replace(b, weight=Decimal(0)) for b in request.budgets))
    assert compose_portfolio(zero)["cash_weight"] == 1
    assert compose_portfolio(zero)["positions"] == []
    partial = replace(
        request, modules=(replace(request.modules[0], targets=()), *request.modules[1:])
    )
    assert compose_portfolio(partial)["cash_weight"] == 0.65  # noqa: PLR2004 -- .25 + .10 invested


def test_catalog_is_declarative_and_returns_independent_json_values() -> None:
    catalog = module_catalog()
    assert [item["module"] for item in catalog] == ["aegis", "alpha", "hedge"]
    assert len({item["responsibility"] for item in catalog}) == len(catalog)
    assert all(item["strategy_execution"] == "not_implemented" for item in catalog)
    assert all(item["instrument_type_validation"] is False for item in catalog)
    assert json.loads(json.dumps(catalog, allow_nan=False)) == catalog
    catalog[0]["module"] = "changed"
    assert module_catalog()[0]["module"] == "aegis"
