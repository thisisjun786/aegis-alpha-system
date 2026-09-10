"""Independent synthetic period arithmetic and strict research splice contracts."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, asdict, fields, replace
from datetime import UTC, date, datetime, timedelta
from typing import cast

import pytest

from aegis_alpha.engine.errors import ContractDefinitionError
from aegis_alpha.engine.proxy import ProxyPoint, ProxyRecipe, ReturnSeries, build_proxy_returns

ANCHOR = date(2020, 1, 1)
SWITCH = date(2020, 1, 3)
DAYS = (date(2020, 1, 2), SWITCH, date(2020, 1, 6), date(2020, 1, 7))


def _inputs() -> tuple[ReturnSeries, ReturnSeries, ProxyRecipe]:
    donor = ReturnSeries(
        instrument_id="SYN_D",
        currency="USD",
        return_kind="total_return",
        close_convention="NYSE_16:00",
        net_of_fees=True,
        anchor_date=ANCHOR,
        dates=DAYS,
        returns=(0.1, -0.2, 9.0, 8.0),
        source_sha256="a" * 64,
    )
    target = ReturnSeries(
        instrument_id="SYN_T",
        currency="USD",
        return_kind="total_return",
        close_convention="NYSE_16:00",
        net_of_fees=True,
        anchor_date=SWITCH,
        dates=DAYS[2:],
        returns=(0.25, -0.1),
        source_sha256="b" * 64,
    )
    return donor, target, ProxyRecipe("SYN_T", "SYN_D", SWITCH, 0, "already_net", "Synthetic case")


def test_hand_compounding_selects_periods_at_the_shared_seam() -> None:
    donor, target, recipe = _inputs()
    result = build_proxy_returns(donor, target, recipe)
    # 100 * 1.1 = 110; * .8 = 88; * 1.25 = 110; * .9 = 99.
    assert [p.index_value for p in result.points] == pytest.approx([100, 110, 88, 110, 99])
    assert [p.period_return for p in result.points] == [0, 0.1, -0.2, 0.25, -0.1]
    assert [p.date for p in result.points] == [ANCHOR, *DAYS]
    assert [p.source_kind for p in result.points] == [
        "anchor",
        "donor",
        "donor",
        "target",
        "target",
    ]
    assert result.points[0] == ProxyPoint(ANCHOR, 100.0, 0.0, "anchor")


def test_target_observed_end_boundary_ignores_earlier_target_returns() -> None:
    donor, target, recipe = _inputs()
    target = replace(target, anchor_date=ANCHOR, dates=DAYS, returns=(7.0, 6.0, 0.25, -0.1))
    assert [
        p.index_value for p in build_proxy_returns(donor, target, recipe).points
    ] == pytest.approx([100, 110, 88, 110, 99])


def test_future_target_mutation_leaves_entire_earlier_prefix_unchanged() -> None:
    donor, target, recipe = _inputs()
    original = build_proxy_returns(donor, target, recipe)
    changed = build_proxy_returns(donor, replace(target, returns=(0.25, 4.0)), recipe)
    assert original.points[:-1] == changed.points[:-1]
    assert original.points[-1].index_value == pytest.approx(99)
    assert changed.points[-1].index_value == pytest.approx(550)


def test_annual_expense_uses_calendar_days_only_on_gross_donor() -> None:
    donor, target, recipe = _inputs()
    # Four Gregorian years are 1461 days = 4 fee-years, so drag is exactly .9**4.
    start = date(2016, 1, 1)
    switch = date(2020, 1, 1)
    donor = replace(donor, net_of_fees=False, anchor_date=start, dates=(switch,), returns=(0.2,))
    target = replace(target, anchor_date=switch, dates=(date(2024, 1, 1),), returns=(0.5,))
    recipe = replace(recipe, switch_date=switch, annual_fee=0.1, fee_model="annual_expense")
    result = build_proxy_returns(donor, target, recipe)
    # 120 * .6561 = 78.732; target remains net: 78.732 * 1.5 = 118.098.
    assert [p.index_value for p in result.points] == pytest.approx([100, 78.732, 118.098])
    assert result.points[1].period_return == pytest.approx(-0.21268)
    assert result.points[2].period_return == pytest.approx(0.5)


def test_weekend_fee_drag_counts_three_calendar_days() -> None:
    donor, target, recipe = _inputs()
    donor = replace(donor, net_of_fees=False, anchor_date=SWITCH, dates=(DAYS[2],), returns=(0.0,))
    target = replace(target, anchor_date=DAYS[2], dates=(DAYS[3],), returns=(0.25,))
    recipe = replace(recipe, switch_date=DAYS[2], annual_fee=0.1, fee_model="annual_expense")
    result = build_proxy_returns(donor, target, recipe)
    # Independent high-precision evaluation of exp(ln(.9) * 3 / 365.25).
    assert result.points[1].index_value == pytest.approx(99.91349902246620, rel=1e-13)
    assert result.points[2].index_value == pytest.approx(124.89187377808275, rel=1e-13)


def test_zero_expense_sensitivity_is_explicit_and_preserves_gross_returns() -> None:
    donor, target, recipe = _inputs()
    recipe = replace(recipe, fee_model="zero_expense_sensitivity", reason="Explicit zero cost case")
    result = build_proxy_returns(replace(donor, net_of_fees=False), target, recipe)
    assert result.points[-1].index_value == pytest.approx(99)
    assert result.fee_model == "zero_expense_sensitivity"
    assert result.reason == "Explicit zero cost case"


@pytest.mark.parametrize(
    ("fee_model", "net", "fee"),
    [
        ("already_net", False, 0),
        ("annual_expense", True, 0.01),
        ("zero_expense_sensitivity", True, 0),
    ],
)
def test_donor_fee_basis_rejects_double_fee_and_silent_gross_pass(
    fee_model: str, *, net: bool, fee: float
) -> None:
    donor, target, recipe = _inputs()
    with pytest.raises(ContractDefinitionError, match="donor net_of_fees"):
        build_proxy_returns(
            replace(donor, net_of_fees=net),
            target,
            replace(recipe, fee_model=fee_model, annual_fee=fee),
        )


@pytest.mark.parametrize("fee", [True, None, 10**1000, float("nan"), float("inf"), -0.1, 0, 1])
def test_annual_expense_rejects_invalid_fee(fee: object) -> None:
    _, _, recipe = _inputs()
    with pytest.raises(ContractDefinitionError, match="annual_fee"):
        replace(recipe, fee_model="annual_expense", annual_fee=cast("float", fee))


@pytest.mark.parametrize("mode", ["already_net", "zero_expense_sensitivity"])
def test_zero_fee_modes_reject_nonzero_expense(mode: str) -> None:
    _, _, recipe = _inputs()
    with pytest.raises(ContractDefinitionError, match="require fee 0"):
        replace(recipe, fee_model=mode, annual_fee=0.01)


@pytest.mark.parametrize(
    ("name", "value"),
    [("currency", "EUR"), ("return_kind", "price_return"), ("close_convention", "UTC_00:00")],
)
def test_mismatched_series_labels_are_rejected(name: str, value: str) -> None:
    donor, target, recipe = _inputs()
    with pytest.raises(ContractDefinitionError, match=name):
        build_proxy_returns(donor, replace(target, **{name: value}), recipe)


@pytest.mark.parametrize("name", ["donor_id", "target_id"])
def test_recipe_identity_mismatch_rejected(name: str) -> None:
    donor, target, recipe = _inputs()
    with pytest.raises(ContractDefinitionError, match="IDs"):
        build_proxy_returns(donor, target, replace(recipe, **{name: "SYN_OTHER"}))


def test_gross_target_is_rejected() -> None:
    donor, target, recipe = _inputs()
    with pytest.raises(ContractDefinitionError, match="target must be net"):
        build_proxy_returns(donor, replace(target, net_of_fees=False), recipe)


@pytest.mark.parametrize("side", ["donor", "target"])
def test_missing_seam_rejects_gaps_and_straddling_periods(side: str) -> None:
    donor, target, recipe = _inputs()
    if side == "donor":
        donor = replace(donor, dates=(DAYS[0], DAYS[2]), returns=(0.1, 0.2))
    else:
        target = replace(target, anchor_date=DAYS[0])
    with pytest.raises(ContractDefinitionError, match="switch boundary"):
        build_proxy_returns(donor, target, recipe)
    with pytest.raises(ContractDefinitionError, match="switch boundary"):
        build_proxy_returns(*_inputs()[:2], replace(recipe, switch_date=date(2020, 1, 4)))


def test_both_segments_require_at_least_one_period() -> None:
    donor, target, recipe = _inputs()
    with pytest.raises(ContractDefinitionError, match="one donor period"):
        build_proxy_returns(
            replace(donor, anchor_date=SWITCH, dates=DAYS[2:], returns=(0, 0)), target, recipe
        )
    target = replace(target, anchor_date=ANCHOR, dates=DAYS[:2], returns=(0, 0))
    with pytest.raises(ContractDefinitionError, match="one target period"):
        build_proxy_returns(donor, target, recipe)


@pytest.mark.parametrize("value", [True, None, 10**1000, float("nan"), float("inf"), -1, -2, "0.1"])
def test_bad_returns_rejected_at_construction(value: object) -> None:
    donor, _, _ = _inputs()
    with pytest.raises(ContractDefinitionError, match="returns"):
        replace(donor, returns=cast("tuple[float, ...]", (value, 0, 0, 0)))


@pytest.mark.parametrize("value", [True, None, "2020-01-02", datetime(2020, 1, 2, tzinfo=UTC)])
def test_dates_anchor_and_switch_reject_non_dates(value: object) -> None:
    donor, _, recipe = _inputs()
    invalid = cast("date", value)
    with pytest.raises(ContractDefinitionError, match="must be a date"):
        replace(donor, dates=(invalid, *DAYS[1:]))
    with pytest.raises(ContractDefinitionError, match="must be a date"):
        replace(donor, anchor_date=invalid)
    with pytest.raises(ContractDefinitionError, match="must be a date"):
        replace(recipe, switch_date=invalid)


@pytest.mark.parametrize("anchor", [DAYS[0], SWITCH])
def test_anchor_must_precede_first_end(anchor: date) -> None:
    donor, _, _ = _inputs()
    with pytest.raises(ContractDefinitionError, match="precede"):
        replace(donor, anchor_date=anchor)


@pytest.mark.parametrize("dates", [(DAYS[0], DAYS[0]), (SWITCH, DAYS[0])])
def test_duplicate_or_decreasing_dates_rejected(dates: tuple[date, ...]) -> None:
    donor, _, _ = _inputs()
    with pytest.raises(ContractDefinitionError, match="increasing and unique"):
        replace(donor, dates=dates, returns=(0, 0))


@pytest.mark.parametrize("value", [None, "", {}, True])
@pytest.mark.parametrize("name", ["dates", "returns"])
def test_invalid_sequence_containers(name: str, value: object) -> None:
    donor, _, _ = _inputs()
    with pytest.raises(ContractDefinitionError, match="sequence"):
        replace(donor, **{name: value})


def test_empty_and_misaligned_series_rejected() -> None:
    donor, _, _ = _inputs()
    with pytest.raises(ContractDefinitionError, match="nonempty and aligned"):
        replace(donor, dates=(), returns=())
    with pytest.raises(ContractDefinitionError, match="nonempty and aligned"):
        replace(donor, returns=(0,))


@pytest.mark.parametrize("value", ["", " padded", "padded ", "a\nb", None, 3])
def test_identity_and_exact_text_reject_invalid_values(value: object) -> None:
    donor, _, recipe = _inputs()
    for name in ("instrument_id", "currency", "return_kind", "close_convention"):
        with pytest.raises(ContractDefinitionError, match="exact printable text"):
            replace(donor, **{name: value})
    for name in ("target_id", "donor_id", "fee_model", "reason"):
        with pytest.raises(ContractDefinitionError, match="exact printable text"):
            replace(recipe, **{name: value})


@pytest.mark.parametrize("value", ["a" * 63, "A" * 64, "g" * 64, "a" * 64 + "\n", None])
def test_hash_requires_lowercase_sha256(value: object) -> None:
    donor, _, _ = _inputs()
    with pytest.raises(ContractDefinitionError, match="SHA256"):
        replace(donor, source_sha256=cast("str", value))


def test_unknown_basis_fee_model_and_nonboolean_net_rejected() -> None:
    donor, _, recipe = _inputs()
    with pytest.raises(ContractDefinitionError, match="return_kind"):
        replace(donor, return_kind="adjusted")
    with pytest.raises(ContractDefinitionError, match="fee_model"):
        replace(recipe, fee_model="none")
    with pytest.raises(ContractDefinitionError, match="boolean"):
        replace(donor, net_of_fees=cast("bool", 1))


def test_index_overflow_and_underflow_rejected() -> None:
    donor, target, recipe = _inputs()
    with pytest.raises(ContractDefinitionError, match="proxy index_value"):
        build_proxy_returns(replace(donor, returns=(1e308, 0, 0, 0)), target, recipe)
    dates = tuple(ANCHOR + timedelta(days=i) for i in range(1, 401))
    donor = replace(donor, dates=dates, returns=(-0.99,) * len(dates))
    target = replace(
        target, anchor_date=dates[-1], dates=(dates[-1] + timedelta(days=1),), returns=(0,)
    )
    with pytest.raises(ContractDefinitionError, match="proxy index_value"):
        build_proxy_returns(donor, target, replace(recipe, switch_date=dates[-1]))


def test_input_copies_and_all_value_models_are_frozen() -> None:
    donor, target, recipe = _inputs()
    dates, returns = list(donor.dates), list(donor.returns)
    donor = replace(
        donor, dates=cast("tuple[date, ...]", dates), returns=cast("tuple[float, ...]", returns)
    )
    dates.clear()
    returns.clear()
    result = build_proxy_returns(donor, target, recipe)
    points = list(result.points)
    copied = replace(result, points=cast("tuple[ProxyPoint, ...]", points))
    points.clear()
    assert copied == result
    assert isinstance(donor.dates, tuple)
    assert all(isinstance(value, float) for value in donor.returns)
    for model, name in (
        (donor, "currency"),
        (recipe, "reason"),
        (result, "points"),
        (result.points[0], "date"),
    ):
        with pytest.raises(FrozenInstanceError):
            setattr(model, name, None)


def test_exact_output_contract_and_iso_serialization() -> None:
    donor, target, recipe = _inputs()
    result = build_proxy_returns(donor, target, recipe)
    assert [f.name for f in fields(ReturnSeries)] == [
        "instrument_id",
        "currency",
        "return_kind",
        "close_convention",
        "net_of_fees",
        "anchor_date",
        "dates",
        "returns",
        "source_sha256",
    ]
    assert [f.name for f in fields(ProxyRecipe)] == [
        "target_id",
        "donor_id",
        "switch_date",
        "annual_fee",
        "fee_model",
        "reason",
    ]
    assert asdict(result) == {
        "target_id": "SYN_T",
        "donor_id": "SYN_D",
        "donor_source_sha256": "a" * 64,
        "target_source_sha256": "b" * 64,
        "switch_date": SWITCH,
        "currency": "USD",
        "return_kind": "total_return",
        "close_convention": "NYSE_16:00",
        "annual_fee": 0,
        "fee_model": "already_net",
        "reason": "Synthetic case",
        "points": tuple(
            {
                "date": day,
                "index_value": pytest.approx(level),
                "period_return": ret,
                "source_kind": kind,
            }
            for day, level, ret, kind in zip(
                (ANCHOR, *DAYS),
                (100, 110, 88, 110, 99),
                (0, 0.1, -0.2, 0.25, -0.1),
                ("anchor", "donor", "donor", "target", "target"),
                strict=True,
            )
        ),
        "research_only": True,
        "non_executable": True,
        "point_in_time_verified": False,
        "source_pins_verified": False,
    }
    payload = json.loads(json.dumps(asdict(result), default=str))
    assert payload["switch_date"] == "2020-01-03"
    assert [date.fromisoformat(p["date"]) for p in payload["points"]] == [ANCHOR, *DAYS]
    assert result.research_only is True
    assert result.non_executable is True
    assert result.point_in_time_verified is False
    assert result.source_pins_verified is False
