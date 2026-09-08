"""Named derived operations, explicit macro limits, and point-in-time refusals."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest
from engine_support import DAY, GATES, strategy

from aegis_alpha.engine import (
    DerivedCashflows,
    DerivedInputBinding,
    DerivedSeriesSpec,
    MacroPoint,
    MacroSignalSpec,
    ReplayBlockedError,
    derive_series,
    evaluate_signals,
)


def derived_spec(*, plus: bool = False) -> DerivedSeriesSpec:
    bindings = (
        DerivedInputBinding("synthetic-prices", "v1", "price_source", "price"),
        DerivedInputBinding("synthetic-flows", "v1", "flow_source", "addend_a"),
    )
    if plus:
        bindings += (DerivedInputBinding("synthetic-extra", "v1", "extra_source", "addend_b"),)
    return DerivedSeriesSpec(
        series_id="caller_yield",
        operation="trailing_sum_plus_trailing_sum_over_price"
        if plus
        else "trailing_sum_over_price",
        trailing_months=2,
        consumes_capital=True,
        consumes_totalreturn=False,
        input_bindings=bindings,
        signal_lag_months=(0,),
        signal_thresholds=(0.3,),
        reference_provenance="fabricated",
    )


def cashflows() -> DerivedCashflows:
    return DerivedCashflows(
        fields={
            "price": ((DAY, 10, DAY),),
            "addend_a": ((date(2026, 2, 28), 1, DAY), (DAY, 3, DAY)),
            "addend_b": ((DAY, 2, DAY),),
        },
        identities={
            "price_source": ("synthetic-prices", "v1"),
            "flow_source": ("synthetic-flows", "v1"),
            "extra_source": ("synthetic-extra", "v1"),
        },
    )


@pytest.mark.parametrize(("plus", "expected"), [(False, 0.4), (True, 0.6)])
def test_named_derived_arithmetic(*, plus: bool, expected: float) -> None:
    point = derive_series(derived_spec(plus=plus), cashflows(), as_of=DAY, gates=GATES)
    assert point.value == pytest.approx(expected)
    assert point.as_of == DAY


@pytest.mark.parametrize("case", ["identity", "field", "future", "nan", "price"])
def test_invalid_derived_inputs_fail(case: str) -> None:
    inputs = cashflows()
    with pytest.raises(ReplayBlockedError):  # noqa: PT012 -- constructing invalid observations can also reject
        if case == "identity":
            inputs = replace(inputs, identities={})
        elif case == "field":
            inputs = replace(inputs, fields={"price": ((DAY, 10, DAY),)})
        elif case == "future":
            inputs = replace(
                inputs, fields={**inputs.fields, "addend_a": ((DAY, 1, date(2026, 4, 1)),)}
            )
        elif case == "nan":
            inputs = replace(
                inputs, fields={**inputs.fields, "addend_a": ((DAY, float("nan"), DAY),)}
            )
        else:
            inputs = replace(inputs, fields={**inputs.fields, "price": ((DAY, -10, DAY),)})
        derive_series(derived_spec(), inputs, as_of=DAY, gates=GATES)


@pytest.mark.parametrize(
    ("comparison", "thresholds", "expected"),
    [("LT", (2.0,), False), ("LTE", (2.0,), True), ("LT_SOFT_HARD", (1.0, 3.0), True)],
)
def test_explicit_macro_threshold_and_mode(
    comparison: str, thresholds: tuple[float, ...], *, expected: bool
) -> None:
    mode = "caller_mode" if comparison == "LT_SOFT_HARD" else None
    config = {} if mode is None else {mode: {"kind": "threshold_mode", "mode": "HARD"}}
    record = replace(strategy(), signals_config=config)
    spec = MacroSignalSpec("caller_macro", (0,), "EXACT", comparison, thresholds, mode)
    result = evaluate_signals(
        strategy=record,
        macro={"caller_macro": (MacroPoint(DAY, 2, DAY),)},
        features={},
        signal_date=DAY,
        specs=(spec,),
        as_of=DAY,
        gates=GATES,
    )
    assert result.flags == {"caller_macro": expected}
    with pytest.raises(ReplayBlockedError, match="bucket"):
        evaluate_signals(
            strategy=record,
            macro={},
            features={},
            signal_date=DAY,
            specs=(spec,),
            as_of=DAY,
            gates=GATES,
        )


def test_missing_price_month_cannot_relabel_a_later_cashflow_window() -> None:
    inputs = cashflows()
    inputs = replace(inputs, fields={**inputs.fields, "price": ((date(2026, 2, 28), 10, DAY),)})
    with pytest.raises(ReplayBlockedError, match="price bucket"):
        derive_series(derived_spec(), inputs, as_of=DAY, gates=GATES)


def test_derived_series_rejects_alternate_macro_supplier() -> None:
    from engine_support import bundle, contract, request  # noqa: PLC0415 -- replay boundary fixture

    from aegis_alpha.engine import replay  # noqa: PLC0415 -- replay boundary fixture

    value = replace(
        contract(),
        derived_series=(derived_spec(),),
        macro_signals=(MacroSignalSpec("caller_yield", (0,), "EXACT", "LT", (0.3,)),),
    )
    inputs = replace(request(), macro={"caller_yield": (MacroPoint(DAY, 0, DAY),)})
    with pytest.raises(ReplayBlockedError, match="cannot also be supplied"):
        replay(bundle(value), inputs)


def test_signal_name_collision_rejected_before_replay() -> None:
    from engine_support import contract  # noqa: PLC0415 -- contract fixture

    from aegis_alpha.engine import ContractDefinitionError  # noqa: PLC0415 -- boundary error

    record = replace(
        strategy(),
        signals_config={"collision": {"kind": "negative_abs_momentum", "enabled": False}},
    )
    with pytest.raises(ContractDefinitionError, match="collide"):
        replace(
            contract(),
            pack=(record,),
            macro_signals=(MacroSignalSpec("collision", (0,), "EXACT", "LT", (0.0,)),),
        )


def test_last_business_day_derived_price_feeds_calendar_month_signal() -> None:
    from engine_support import bundle, contract, request  # noqa: PLC0415 -- full replay fixture

    from aegis_alpha.engine import replay  # noqa: PLC0415 -- full replay fixture

    spec = derived_spec()
    value = replace(
        contract(),
        derived_series=(spec,),
        macro_signals=(MacroSignalSpec("caller_yield", (0,), "EXACT", "LT", (0.3,)),),
    )
    inputs = replace(
        request(),
        derived_inputs={
            "caller_yield": {
                "fields": {
                    "price": ((date(2026, 2, 27), 10, date(2026, 2, 27)),),
                    "addend_a": ((date(2026, 2, 15), 4, date(2026, 2, 15)),),
                },
                "identities": {
                    "price_source": ("synthetic-prices", "v1"),
                    "flow_source": ("synthetic-flows", "v1"),
                },
            }
        },
    )
    result = replay(bundle(value), inputs)
    assert result.signals["synthetic-choice"] == {"caller_yield": False}
    assert result.ensemble == {"ASSET_A": 1.0}
