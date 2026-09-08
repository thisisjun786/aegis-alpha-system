from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, DecimalException
from enum import StrEnum
from fractions import Fraction
from typing import NewType, cast

InstrumentId = NewType("InstrumentId", str)
StrategyId = NewType("StrategyId", str)
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")


class ContractError(ValueError):
    """Malformed or inconsistent funded allocation preview input."""


class ModuleId(StrEnum):
    AEGIS = "aegis"
    ALPHA = "alpha"
    HEDGE = "hedge"


def _identifier(value: object) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ContractError("identifier must contain 1-128 safe ASCII characters")
    return value


def _weight(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ContractError("weight must be a finite number, not a boolean")
    number = Decimal(str(value))
    if not number.is_finite() or not 0 <= number <= 1:
        raise ContractError("weight must be finite and between 0 and 1")
    return number


def _date(value: object) -> None:
    if type(value) is not date:
        raise ContractError("as_of must be a date without a time")


def _module(value: object) -> ModuleId:
    if not isinstance(value, str):
        raise ContractError("module must be aegis, alpha or hedge")
    try:
        return ModuleId(value)
    except ValueError as error:
        raise ContractError("module must be aegis, alpha or hedge") from error


def _tuple_of[T](value: object, kind: type[T]) -> tuple[T, ...]:
    if not isinstance(value, tuple) or any(not isinstance(item, kind) for item in value):
        raise ContractError(f"expected immutable tuple of {kind.__name__}")
    # The element checks above establish this immutable generic boundary.
    return cast("tuple[T, ...]", value)


def _total(weights: tuple[Decimal, ...]) -> Fraction:
    return sum((Fraction(weight) for weight in weights), Fraction())


@dataclass(frozen=True, slots=True)
class TargetWeight:
    instrument_id: InstrumentId
    weight: Decimal

    def __post_init__(self) -> None:
        _identifier(self.instrument_id)
        object.__setattr__(self, "weight", _weight(self.weight))


@dataclass(frozen=True, slots=True)
class ModuleBudget:
    module: ModuleId
    weight: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.module, ModuleId):
            raise ContractError("budget module must be a ModuleId")
        object.__setattr__(self, "weight", _weight(self.weight))


@dataclass(frozen=True, slots=True)
class ModuleAllocation:
    module: ModuleId
    strategy_id: StrategyId
    strategy_version: str
    as_of: date
    targets: tuple[TargetWeight, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.module, ModuleId):
            raise ContractError("allocation module must be a ModuleId")
        _identifier(self.strategy_id)
        _identifier(self.strategy_version)
        _date(self.as_of)
        targets = _tuple_of(self.targets, TargetWeight)
        if len({target.instrument_id for target in targets}) != len(targets):
            raise ContractError("duplicate instrument_id in module targets")
        if _total(tuple(target.weight for target in targets)) > 1:
            raise ContractError("module target weights exceed 1")


@dataclass(frozen=True, slots=True)
class PortfolioRequest:
    schema_version: int
    as_of: date
    budgets: tuple[ModuleBudget, ...]
    modules: tuple[ModuleAllocation, ...]

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ContractError("schema_version must be integer 1")
        _date(self.as_of)
        budgets = _tuple_of(self.budgets, ModuleBudget)
        modules = _tuple_of(self.modules, ModuleAllocation)
        for group in (budgets, modules):
            if len(group) != len(ModuleId) or {item.module for item in group} != set(ModuleId):
                raise ContractError(
                    "budgets and modules must each contain aegis, alpha, hedge once"
                )
        if _total(tuple(budget.weight for budget in budgets)) > 1:
            raise ContractError("portfolio budgets exceed 1")
        if any(module.as_of != self.as_of for module in modules):
            raise ContractError("module as_of must match portfolio as_of")


def _object(value: object, fields: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ContractError(
            f"object must contain exactly these fields: {', '.join(sorted(fields))}"
        )
    # Exact field matching above proves all keys are strings.
    return cast("dict[str, object]", value)


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ContractError("expected a JSON array")
    # JSON arrays contain arbitrary objects; only the container shape is asserted here.
    return cast("list[object]", value)


def _parse_date(value: object) -> date:
    if not isinstance(value, str) or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value) is None:
        raise ContractError("as_of must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ContractError("as_of must be a valid calendar date") from error


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ContractError("duplicate JSON object field")
        result[key] = value
    return result


def _constant(_value: str) -> object:
    raise ContractError("JSON numbers must be finite")


def _parse_target(value: object) -> TargetWeight:
    row = _object(value, {"instrument_id", "weight"})
    return TargetWeight(InstrumentId(_identifier(row["instrument_id"])), _weight(row["weight"]))


def _parse_allocation(value: object) -> ModuleAllocation:
    row = _object(value, {"module", "strategy_id", "strategy_version", "as_of", "targets"})
    return ModuleAllocation(
        module=_module(row["module"]),
        strategy_id=StrategyId(_identifier(row["strategy_id"])),
        strategy_version=_identifier(row["strategy_version"]),
        as_of=_parse_date(row["as_of"]),
        targets=tuple(_parse_target(target) for target in _array(row["targets"])),
    )


def parse_request(text: str) -> PortfolioRequest:
    """Parse the complete three-module schema; all input failures are ContractError."""
    if not isinstance(text, str):
        raise ContractError("request must be JSON text")
    try:
        decoded: object = json.loads(
            text, parse_float=Decimal, object_pairs_hook=_pairs, parse_constant=_constant
        )
    except (ValueError, DecimalException, RecursionError) as error:
        raise ContractError(f"invalid JSON: {error}") from error
    row = _object(decoded, {"schema_version", "as_of", "budgets", "modules"})
    budgets = _object(row["budgets"], {module.value for module in ModuleId})
    return PortfolioRequest(
        # The public constructor validates the exact integer type and supported version.
        schema_version=cast("int", row["schema_version"]),
        as_of=_parse_date(row["as_of"]),
        budgets=tuple(ModuleBudget(_module(key), _weight(value)) for key, value in budgets.items()),
        modules=tuple(_parse_allocation(value) for value in _array(row["modules"])),
    )
