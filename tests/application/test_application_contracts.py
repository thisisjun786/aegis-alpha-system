from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast

import pytest

from aegis_alpha.application.contracts import (
    ContractError,
    InstrumentId,
    ModuleAllocation,
    ModuleBudget,
    ModuleId,
    PortfolioRequest,
    TargetWeight,
    parse_request,
)

# Independent synthetic fixture from the agreed wire contract, shared only by these tests.
EXAMPLE = """{"schema_version":1,"as_of":"2026-09-05",
"budgets":{"aegis":0.6,"alpha":0.25,"hedge":0.1},"modules":[
{"module":"aegis","strategy_id":"demo-aegis","strategy_version":"1",
"as_of":"2026-09-05","targets":[{"instrument_id":"asset:equity-basket","weight":0.8}]},
{"module":"alpha","strategy_id":"demo-alpha","strategy_version":"1",
"as_of":"2026-09-05","targets":[{"instrument_id":"asset:equity-basket","weight":0.4},
{"instrument_id":"asset:stock-b","weight":0.6}]},
{"module":"hedge","strategy_id":"demo-hedge","strategy_version":"1",
"as_of":"2026-09-05","targets":[{"instrument_id":"asset:protective-basket","weight":1}]}]}"""


def test_models_are_frozen_and_nested_collections_are_tuples() -> None:
    request = parse_request(EXAMPLE)
    assert isinstance(request.modules, tuple)
    assert isinstance(request.budgets, tuple)
    assert isinstance(request.modules[0].targets, tuple)
    assert request.budgets[0].weight == Decimal("0.6")
    assert request.as_of == date(2026, 9, 5)
    with pytest.raises(FrozenInstanceError):
        request.as_of = date(2026, 9, 6)  # ty: ignore[invalid-assignment] -- mutation probe
    with pytest.raises(FrozenInstanceError):
        request.modules[0].targets[0].weight = Decimal(1)  # ty: ignore[invalid-assignment]


@pytest.mark.parametrize(
    ("before", "after", "message"),
    [
        ('"schema_version":1', '"schema_version":true', "schema_version"),
        ('"schema_version":1', '"schema_version":1.0', "schema_version"),
        ('"schema_version":1', '"schema_version":2', "schema_version"),
        ('"schema_version":1', '"schema_version":1,"extra":0', "exactly"),
        ('"schema_version":1', '"schema_version":1,"schema_version":1', "duplicate"),
        ('"aegis":0.6', '"aegis":0.6,"aegis":0.1', "duplicate"),
        ('"aegis":0.6', '"unknown":0.6', "exactly"),
        ('"aegis":0.6,', "", "exactly"),
        ('"aegis":0.6', '"aegis":0.9', "budgets exceed"),
        ('"module":"aegis"', '"module":"invalid"', "module must"),
        ('"module":"alpha"', '"module":"aegis"', "once"),
        ('"module":"aegis"', '"module":"aegis","extra":0', "exactly"),
        ('"module":"aegis"', '"module":"aegis","module":"aegis"', "duplicate"),
        ('"weight":0.8', '"weight":true', "finite number"),
        ('"weight":0.8', '"weight":"0.8"', "finite number"),
        ('"weight":0.8', '"weight":null', "finite number"),
        ('"weight":0.8', '"weight":-0.1', "between"),
        ('"weight":0.8', '"weight":1.01', "between"),
        ('"weight":0.8', '"weight":NaN', "finite"),
        ('"weight":0.8', '"weight":Infinity', "finite"),
        ('"weight":0.8', '"weight":-Infinity', "finite"),
        ('"weight":0.8', '"weight":1e999', "between"),
        ('"weight":0.8', '"weight":0.8,"extra":0', "exactly"),
        ('"weight":0.8', '"weight":0.8,"weight":0.8', "duplicate"),
        ('"weight":0.4', '"weight":0.5', "target weights exceed"),
        ('"asset:stock-b"', '"asset:equity-basket"', "duplicate instrument"),
        ('"demo-aegis"', '"../bad"', "identifier"),
        ('"demo-aegis"', '""', "identifier"),
        ('"asset:equity-basket"', '"bad\\nidentifier"', "identifier"),
        ('"strategy_version":"1"', '"strategy_version":1', "identifier"),
        ('"2026-09-05"', '"2026-02-30"', "calendar date"),
        ('"2026-09-05"', '"20260905"', "YYYY-MM-DD"),
        ('"2026-09-05"', '"2026-09-05T00:00:00"', "YYYY-MM-DD"),
    ],
)
def test_rejects_bad_wire_fields(before: str, after: str, message: str) -> None:
    with pytest.raises(ContractError, match=message):
        parse_request(EXAMPLE.replace(before, after))


@pytest.mark.parametrize("text", ["", "{", "null", "[]", "42", '{"schema_version":1}'])
def test_rejects_bad_documents(text: str) -> None:
    with pytest.raises(ContractError):
        parse_request(text)


def test_rejects_missing_modules_and_mismatched_date() -> None:
    document = json.loads(EXAMPLE)
    document["modules"].pop()
    with pytest.raises(ContractError, match="once"):
        parse_request(json.dumps(document))
    with pytest.raises(ContractError, match="match portfolio"):
        parse_request(EXAMPLE.replace('"2026-09-05"', '"2026-09-06"', 1))


@pytest.mark.parametrize("bad", [True, "0.5", None, float("nan"), float("inf"), -1, 2])
def test_direct_weight_constructors_reject_invalid_values(bad: object) -> None:
    # Casts deliberately send invalid runtime inputs through public constructor boundaries.
    weight = cast("Decimal", bad)
    with pytest.raises(ContractError):
        TargetWeight(InstrumentId("asset:x"), weight)
    with pytest.raises(ContractError):
        ModuleBudget(ModuleId.AEGIS, weight)


@pytest.mark.parametrize("bad", [[], {}, ("not-a-model",)])
def test_direct_constructors_reject_mutable_or_untyped_collections(bad: object) -> None:
    request = parse_request(EXAMPLE)
    with pytest.raises(ContractError, match="immutable tuple"):
        replace(request, modules=cast("tuple[ModuleAllocation, ...]", bad))
    with pytest.raises(ContractError, match="immutable tuple"):
        replace(request, budgets=cast("tuple[ModuleBudget, ...]", bad))
    with pytest.raises(ContractError, match="immutable tuple"):
        replace(request.modules[0], targets=cast("tuple[TargetWeight, ...]", bad))


@pytest.mark.parametrize("bad", ["2026-09-05", datetime(2026, 9, 5, tzinfo=UTC), None])
def test_direct_constructors_reject_non_date_values(bad: object) -> None:
    request = parse_request(EXAMPLE)
    with pytest.raises(ContractError, match="without a time"):
        replace(request, as_of=cast("date", bad))
    with pytest.raises(ContractError, match="without a time"):
        replace(request.modules[0], as_of=cast("date", bad))


def test_direct_constructor_enforces_complete_request_and_module_enum() -> None:
    request = parse_request(EXAMPLE)
    with pytest.raises(ContractError, match="once"):
        replace(request, modules=request.modules[:-1])
    with pytest.raises(ContractError, match="ModuleId"):
        replace(request.modules[0], module=cast("ModuleId", "aegis"))
    with pytest.raises(ContractError, match="ModuleId"):
        ModuleBudget(cast("ModuleId", "aegis"), Decimal(0))
    with pytest.raises(ContractError, match="schema_version"):
        PortfolioRequest(
            schema_version=True,
            as_of=request.as_of,
            budgets=request.budgets,
            modules=request.modules,
        )
