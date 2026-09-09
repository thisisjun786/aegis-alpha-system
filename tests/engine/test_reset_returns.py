"""Independent arithmetic and boundary checks on fabricated return inputs."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast

import pytest

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.errors import ContractDefinitionError
from aegis_alpha.engine.proxy import ReturnSeries
from aegis_alpha.engine.reset_returns import ResetCosts, ResetRecipe, build_reset_returns

ANCHOR = date(2020, 1, 2)
DATES = (date(2020, 1, 3), date(2020, 1, 6))
SESSIONS = (ANCHOR, *DATES)


def underlying() -> ReturnSeries:
    return ReturnSeries(
        instrument_id="SYNTHETIC",
        currency="UNIT",
        return_kind="price_return",
        close_convention="caller_close",
        net_of_fees=False,
        anchor_date=ANCHOR,
        dates=DATES,
        returns=(0.1, -0.1),
        source_sha256="a" * 64,
    )


def costs() -> ResetCosts:
    return ResetCosts(
        ANCHOR,
        DATES,
        (0.0, 0.0),
        (0.0, 0.0),
        (0.0, 0.0),
        "b" * 64,
        "fraction_of_starting_nav",
        "zero_sensitivity",
    )


@pytest.mark.parametrize("bad", [True, "1", float("nan"), float("inf"), -float("inf"), 0])
def test_invalid_multiplier(bad: object) -> None:
    with pytest.raises(ContractDefinitionError, match="multiplier"):
        ResetRecipe(cast("float", bad), "explicit")


@pytest.mark.parametrize("bad", ["", " spaced", "line\nbreak", None])
def test_invalid_reason(bad: object) -> None:
    with pytest.raises(ContractDefinitionError, match="reason"):
        ResetRecipe(2, cast("str", bad))


@pytest.mark.parametrize("bad", [(), DATES[::-1], (DATES[0], DATES[0]), (ANCHOR, DATES[0])])
def test_cost_date_order(bad: tuple[date, ...]) -> None:
    with pytest.raises(ContractDefinitionError):
        replace(costs(), dates=bad)


@pytest.mark.parametrize("field", ["financing_drag", "collateral_return", "expense_drag"])
@pytest.mark.parametrize("bad", [(float("nan"), 0), (True, 0), (0,), "00"])
def test_cost_numeric_and_shape_rejection(field: str, bad: object) -> None:
    with pytest.raises(ContractDefinitionError):
        replace(costs(), **{field: bad})


@pytest.mark.parametrize("field", ["financing_drag", "expense_drag"])
def test_negative_drags_rejected(field: str) -> None:
    with pytest.raises(ContractDefinitionError, match="nonnegative"):
        replace(costs(), basis="explicit_assumptions", **{field: (-0.1, 0)})


@pytest.mark.parametrize("field", ["financing_drag", "collateral_return", "expense_drag"])
def test_zero_sensitivity_cannot_hide_nonzero_terms(field: str) -> None:
    with pytest.raises(ContractDefinitionError, match="all cost terms zero"):
        replace(costs(), **{field: (0.1, 0)})


@pytest.mark.parametrize(
    ("field", "value"),
    [("basis", "unknown"), ("convention", "annual_rate"), ("source_sha256", "A" * 64)],
)
def test_cost_metadata_rejected(field: str, value: str) -> None:
    with pytest.raises(ContractDefinitionError):
        replace(costs(), **{field: value})


@pytest.mark.parametrize("sessions", [SESSIONS[:-1], (ANCHOR, date(2020, 1, 4), DATES[1])])
def test_missing_or_shifted_session_rejected(sessions: tuple[date, ...]) -> None:
    with pytest.raises(ContractDefinitionError, match="exactly match"):
        build_reset_returns(
            underlying(), costs(), ResetRecipe(2, "test"), expected_sessions=sessions
        )


def test_cost_anchor_and_date_mismatch_rejected() -> None:
    for changed in (
        replace(costs(), anchor_date=date(2020, 1, 1)),
        replace(costs(), dates=(DATES[0], date(2020, 1, 7))),
    ):
        with pytest.raises(ContractDefinitionError, match="cost dates"):
            build_reset_returns(
                underlying(), changed, ResetRecipe(2, "test"), expected_sessions=SESSIONS
            )


def test_datetime_rejected_at_boundary() -> None:
    bad = datetime(2020, 1, 2, tzinfo=UTC)
    with pytest.raises(ContractDefinitionError, match="not datetime"):
        replace(costs(), anchor_date=bad)
    with pytest.raises(ContractDefinitionError, match="not datetime"):
        build_reset_returns(
            underlying(), costs(), ResetRecipe(2, "test"), expected_sessions=(bad, *DATES)
        )


def test_netted_underlying_rejected_even_when_costs_zero() -> None:
    with pytest.raises(ContractDefinitionError, match="exclude fees"):
        build_reset_returns(
            replace(underlying(), net_of_fees=True),
            costs(),
            ResetRecipe(2, "test"),
            expected_sessions=SESSIONS,
        )


@pytest.mark.parametrize("returns", [(-0.5, 0), (-0.6, 0), (1e308, 0)])
def test_nonpositive_factor_or_overflow_rejected(returns: tuple[float, ...]) -> None:
    with pytest.raises(ContractDefinitionError, match="2020-01-03"):
        build_reset_returns(
            replace(underlying(), returns=returns),
            costs(),
            ResetRecipe(2, "test"),
            expected_sessions=SESSIONS,
        )


def test_index_overflow_rejected() -> None:
    with pytest.raises(ContractDefinitionError, match="reset index"):
        build_reset_returns(
            replace(underlying(), returns=(1e307, 0)),
            costs(),
            ResetRecipe(1, "test"),
            expected_sessions=SESSIONS,
        )


def test_reset_path_differs_from_levered_total_return() -> None:
    result = build_reset_returns(
        underlying(), costs(), ResetRecipe(2, "test"), expected_sessions=SESSIONS
    )
    assert [p.index_value for p in result.points] == pytest.approx([100, 120, 96])


def test_distinct_signed_cost_terms_against_decimal_cash_account() -> None:
    chosen_costs = replace(
        costs(),
        basis="explicit_assumptions",
        financing_drag=(0.003, 0.004),
        collateral_return=(0.001, -0.001),
        expense_drag=(0.002, 0.003),
    )
    result = build_reset_returns(
        replace(underlying(), returns=(0.02, -0.01)),
        chosen_costs,
        ResetRecipe(2, "test"),
        expected_sessions=SESSIONS,
    )
    expected = Decimal(100) * Decimal("1.036") * Decimal("0.972")
    assert result.points[-1].index_value == pytest.approx(float(expected))
    assert [p.period_return for p in result.points] == pytest.approx([0, 0.036, -0.028])


def test_financing_is_prescaled_not_multiplied_again() -> None:
    result = build_reset_returns(
        replace(underlying(), returns=(0, 0)),
        replace(costs(), basis="explicit_assumptions", financing_drag=(0.002, 0)),
        ResetRecipe(3, "test"),
        expected_sessions=SESSIONS,
    )
    assert result.points[1].period_return == pytest.approx(-0.002)


def test_inverse_and_serialized_basis_and_flags() -> None:
    result = build_reset_returns(
        underlying(),
        replace(costs(), basis="explicit_assumptions"),
        ResetRecipe(-0.5, "test"),
        expected_sessions=SESSIONS,
    )
    assert result.points[1].period_return == pytest.approx(-0.05)
    document = json.loads(canonical_json_bytes(result))
    assert document["costs"]["basis"] == "explicit_assumptions"
    assert document["research_only"] is True
    assert document["non_executable"] is True
    assert document["source_pins_verified"] is False
    assert document["point_in_time_verified"] is False
    for attribute in (
        "research_only",
        "non_executable",
        "source_pins_verified",
        "point_in_time_verified",
    ):
        with pytest.raises(FrozenInstanceError):
            setattr(result, attribute, False)


def test_mutable_input_arrays_are_copied() -> None:
    array = [0.0, 0.0]
    changed = replace(costs(), financing_drag=array)
    array[0] = 99
    assert changed.financing_drag == (0.0, 0.0)
