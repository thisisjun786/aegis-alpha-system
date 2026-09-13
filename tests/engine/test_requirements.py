"""Registered execution requirements, with independent synthetic expectations."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, asdict, replace

import pytest

from aegis_alpha.engine import load_bundle, replay, serialize_bundle, sha256_bytes
from aegis_alpha.engine.errors import ContractDefinitionError
from aegis_alpha.engine.models import (
    DerivedInputBinding,
    DerivedSeriesSpec,
    EngineContract,
    FeatureMatrixSpec,
    MacroSignalSpec,
    MomentumScoreSpec,
)
from aegis_alpha.engine.requirements import InputRequirement, derive_execution_definition
from tests.engine.engine_support import bundle, contract, raw_bundle, request, strategy


def rich_contract() -> EngineContract:
    """A second fabricated strategy with non-positional horizons and named inputs."""
    record = replace(
        strategy(),
        name="synthetic-other",
        cash_asset="CASH_Y",
        offensive_config={
            "strategy_type": "absolute_defensive",
            "assets": ["ASSET_Z"],
            "top_n": 1,
            "scoring": {"method": "momentum_score", "horizon": 7, "score_name": "blend"},
            "reference_asset": "REF_Y",
        },
        defensive_config={"assets": ["DEF_Y"]},
        canary_config={
            "canary_mode": "AND",
            "assets": ["CANARY_ON", "CANARY_OFF", "ASSET_Z"],
            "enabled": [True, False, True],
            "scoring": {"method": "moving_average", "horizon": 6},
        },
        signals_config={
            "negative": {
                "kind": "negative_abs_momentum",
                "enabled": True,
                "threshold": 1,
                "scoring": {"method": "return_rate", "horizon": 3},
            }
        },
    )
    value = contract()
    return replace(
        value,
        pack=(record,),
        feature_matrix=FeatureMatrixSpec(
            momentum_scores=(MomentumScoreSpec("blend", (7, 3), (2.0, 1.0), 3.0),),
            moving_average_months=(6,),
            ma_window_includes_current_month=False,
            return_months=(7, 3),
            includes_latest_price=True,
        ),
        calendar=replace(value.calendar, current_month_drop_before_day=9, history_observations=8),
        macro_signals=(
            MacroSignalSpec("MACRO_Y", (2, 0), "OR", "LT", (0.25,)),
            MacroSignalSpec("YIELD_Y", (1,), "EXACT", "LTE", (0.5,)),
        ),
        derived_series=(
            DerivedSeriesSpec(
                series_id="YIELD_Y",
                operation="trailing_sum_over_price",
                trailing_months=4,
                consumes_capital=True,
                consumes_totalreturn=False,
                input_bindings=(
                    DerivedInputBinding("synthetic-prices", "2", "PRICE_Y", "price"),
                    DerivedInputBinding("synthetic-flows", "3", "FLOW_Y", "addend_a"),
                ),
                signal_lag_months=(1,),
                signal_thresholds=(0.5,),
                reference_provenance="synthetic",
            ),
        ),
        ensemble_membership_reference="ensemble:" + "b" * 64,
    )


def test_registered_bundle_exposes_required_price_identities() -> None:
    source = bundle(contract())
    result = derive_execution_definition(source)
    assert result.price_asset_ids == ("ASSET_A", "ASSET_B", "REF_X")
    assert result.asset_ids == ("ASSET_A", "ASSET_B", "REF_X")
    assert result.cash_asset_ids == ("CASH_X",)
    assert (result.bundle_id, result.bundle_version) == ("synthetic-probe", "1")
    assert (result.source_sha256, result.contract_sha256) == (
        source.source_sha256,
        source.contract_sha256,
    )
    assert result.input_requirements == (
        InputRequirement("prices", ("ASSET_A", "ASSET_B", "REF_X"), 3),
        InputRequirement("membership", (source.contract.ensemble_membership_reference,)),
    )
    assert asdict(result.feature_matrix) == {
        "momentum_scores": (),
        "moving_average_months": (),
        "ma_window_includes_current_month": True,
        "return_months": (2,),
        "includes_latest_price": True,
    }
    assert result.macro_signals == result.derived_series == ()
    assert asdict(result.calendar) == {
        "evaluation_snap": "calendar_month_end",
        "signal_date": "prior_calendar_month_end",
        "current_month_drop_before_day": 1,
        "history_observations": 3,
        "canonical_selection_authority": "synthetic",
        "evaluation_authority": "synthetic",
        "signal_authority": "synthetic",
        "history_authority": "synthetic",
    }
    assert asdict(result.stale_gates) == {
        "price_stale_after_days": 50,
        "macro_stale_after_days": 50,
    }
    # Derivation is observational: legacy replay still consumes only these prices.
    assert dict(replay(source, request()).ensemble) == {"ASSET_A": 1.0}
    assert serialize_bundle(source) == raw_bundle(contract())


def test_second_strategy_has_independent_requirements() -> None:
    result = derive_execution_definition(bundle(rich_contract()))
    assert result.price_asset_ids == ("ASSET_Z", "CANARY_ON", "REF_Y")
    assert result.asset_ids == ("ASSET_Z", "CANARY_ON", "DEF_Y", "REF_Y")
    assert result.cash_asset_ids == ("CASH_Y",)
    assert result.feature_matrix.return_months == (7, 3)
    assert result.feature_matrix.moving_average_months == (6,)
    assert result.feature_matrix.ma_window_includes_current_month is False
    assert asdict(result.feature_matrix.momentum_scores[0]) == {
        "name": "blend",
        "return_months": (7, 3),
        "weights": (2.0, 1.0),
        "divisor": 3.0,
    }
    assert (
        result.calendar.current_month_drop_before_day,
        result.calendar.history_observations,
    ) == (9, 8)
    assert result.input_requirements == (
        InputRequirement("prices", ("ASSET_Z", "CANARY_ON", "REF_Y"), 8),
        InputRequirement("macro", ("MACRO_Y",), lag_months=(2, 0), lag_combination="OR"),
        InputRequirement(
            "derived",
            ("YIELD_Y",),
            lag_months=(1,),
            trailing_months=4,
            input_bindings=(
                DerivedInputBinding("synthetic-prices", "2", "PRICE_Y", "price"),
                DerivedInputBinding("synthetic-flows", "3", "FLOW_Y", "addend_a"),
            ),
            basis="capital",
        ),
        InputRequirement("membership", ("ensemble:" + "b" * 64,)),
    )
    assert tuple(signal.series_id for signal in result.macro_signals) == ("MACRO_Y", "YIELD_Y")
    assert result.derived_series[0].operation == "trailing_sum_over_price"
    assert result.ensemble_membership_reference == "ensemble:" + "b" * 64


def test_pack_union_deduplicates_without_losing_cash_feature_role() -> None:
    first = contract()
    other = rich_contract().pack[0]
    other = replace(other, cash_asset="ASSET_A")
    result = derive_execution_definition(bundle(replace(first, pack=(*first.pack, other))))
    assert result.price_asset_ids == (
        "ASSET_A",
        "ASSET_B",
        "ASSET_Z",
        "CANARY_ON",
        "REF_X",
        "REF_Y",
    )
    assert result.cash_asset_ids == ("ASSET_A", "CASH_X")
    assert "CANARY_OFF" not in result.asset_ids
    assert "DEF_Y" in result.asset_ids


@pytest.mark.parametrize("value", [contract(), rich_contract()])
def test_bare_bundle_never_resolves_external_conventions(value: EngineContract) -> None:
    source = bundle(value)
    result = derive_execution_definition(source)
    assert result.required_convention_roles == ("calendar", "basis", "cost", "execution")
    assert result.unresolved_convention_roles == ("calendar", "basis", "cost", "execution")
    assert result.input_requirements[0].basis is None
    assert result.executable is False
    assert derive_execution_definition(source) == result
    raw = serialize_bundle(source)
    assert (
        derive_execution_definition(load_bundle(raw, sha256_bytes(raw), "synthetic-probe", "1"))
        == result
    )
    with pytest.raises(FrozenInstanceError):
        setattr(result, "executable", True)  # noqa: B010 -- exercise frozen runtime boundary
    with pytest.raises(FrozenInstanceError):
        setattr(result.input_requirements[0], "basis", "total_return")  # noqa: B010


@pytest.mark.parametrize(("includes_current", "expected"), [(True, 6), (False, 7)])
def test_moving_average_warmup_uses_current_month_rule(
    *, includes_current: bool, expected: int
) -> None:
    value = contract()
    value = replace(
        value,
        feature_matrix=FeatureMatrixSpec(
            momentum_scores=(),
            moving_average_months=(6,),
            ma_window_includes_current_month=includes_current,
            return_months=(2,),
            includes_latest_price=True,
        ),
    )
    result = derive_execution_definition(bundle(value))
    # Preserve the actual configured cap, even when it is insufficient for replay.
    assert (
        result.input_requirements[0].minimum_observations,
        result.calendar.history_observations,
    ) == (expected, 3)
    assert result.executable is False


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("evaluation_snap", "weekly", 'evaluation_snap must be "calendar_month_end"'),
        ("signal_date", "same_day", 'signal_date must be "prior_calendar_month_end"'),
        ("current_month_drop_before_day", 32, "current_month_drop_before_day must be within 1..31"),
        ("history_observations", 0, "history_observations must be positive"),
    ],
)
def test_malformed_serialized_cadence_is_rejected(field: str, value: object, reason: str) -> None:
    payload = json.loads(raw_bundle(contract()))
    payload["contract"]["calendar"][field] = value
    raw = json.dumps(payload).encode()
    with pytest.raises(ContractDefinitionError) as caught:
        load_bundle(raw, sha256_bytes(raw), "synthetic-probe", "1")
    assert caught.value.reason == reason


@pytest.mark.parametrize("field", ["scoring", "reference_asset", "strategy_type", "assets"])
def test_missing_required_rule_is_rejected_at_parser(field: str) -> None:
    payload = json.loads(raw_bundle(contract()))
    del payload["contract"]["pack"][0]["offensive_config"][field]
    raw = json.dumps(payload).encode()
    with pytest.raises(ContractDefinitionError) as caught:
        load_bundle(raw, sha256_bytes(raw), "synthetic-probe", "1")
    assert caught.value.reason == f"offensive_config requires explicit {field}"


def test_incompatible_derived_price_consumption_is_rejected() -> None:
    payload = json.loads(raw_bundle(rich_contract()))
    payload["contract"]["derived_series"][0]["consumes_totalreturn"] = True
    raw = json.dumps(payload).encode()
    with pytest.raises(ContractDefinitionError) as caught:
        load_bundle(raw, sha256_bytes(raw), "synthetic-probe", "1")
    assert caught.value.reason == "derived series must consume capital prices only"
