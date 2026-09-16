"""Known-answer calculations and missing/stale input refusals."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal

import pytest
from engine_support import DAY, GATES, bundle, contract, request, strategy

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine import (
    AssetFeatures,
    EnsembleMembership,
    FeatureBuildRequest,
    FeatureMatrixSpec,
    MacroPoint,
    MembershipRow,
    MomentumScoreSpec,
    PricePoint,
    ReplayBlockedError,
    SignalSnapshot,
    allocate,
    build_feature_matrix,
    combine_ensemble,
    evaluate_signals,
    membership_hash,
    replay,
)
from aegis_alpha.engine.calendar import lag_month
from aegis_alpha.engine.errors import BlockReason


def feature(asset: str, score: float) -> AssetFeatures:
    return AssetFeatures(asset, DAY, 10.0, {2: score}, {2: score + 1}, {"fabricated": score})


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("equal_weight", {"ASSET_A": 0.5, "ASSET_B": 0.5}),
        ("relative", {"ASSET_A": 0.5, "ASSET_B": 0.5}),
        ("absolute", {"ASSET_A": 1.0}),
        ("absolute_defensive", {"ASSET_A": 1.0}),
        ("relative_absolute", {"ASSET_A": 0.5, "DEF_X": 0.5}),
        ("relative_absolute_defensive", {"ASSET_A": 0.5, "DEF_X": 0.5}),
        ("relative_absolute_cash", {"ASSET_A": 0.5, "CASH_X": 0.5}),
    ],
)
def test_allocation_modes(kind: str, expected: dict[str, float]) -> None:
    record = strategy()
    record = replace(
        record,
        offensive_config={**record.offensive_config, "strategy_type": kind, "top_n": 2},
        defensive_config={"assets": ["DEF_X"]},
    )
    rows = {
        "ASSET_A": feature("ASSET_A", 0.4),
        "ASSET_B": feature("ASSET_B", -0.2),
        "REF_X": feature("REF_X", 0),
    }
    assert (
        dict(allocate(record, rows, SignalSnapshot(flags={}, canary=False, master_switch=False)))
        == expected
    )


def test_sparse_horizons_and_weighted_formula() -> None:
    spec = FeatureMatrixSpec(
        momentum_scores=(MomentumScoreSpec("fabricated", (2, 4), (2, 1), 3),),
        moving_average_months=(2, 4),
        ma_window_includes_current_month=True,
        return_months=(2, 4),
        includes_latest_price=True,
    )
    dates = (date(2025, 11, 30), date(2025, 12, 31), date(2026, 1, 31), date(2026, 2, 28), DAY)
    points = tuple(
        PricePoint(day, price, day) for day, price in zip(dates, (10, 12, 15, 18, 30), strict=True)
    )
    row = build_feature_matrix({"A": points}, FeatureBuildRequest(spec, DAY, GATES, 1, 5))["A"]
    assert row.returns == {2: 1.0, 4: 2.0}
    assert row.ma_ratios[2] == pytest.approx(1.25)
    assert row.ma_ratios[4] == pytest.approx(1.6)
    assert row.momentum["fabricated"] == pytest.approx(4 / 3)


@pytest.mark.parametrize(
    "case", ["short_ma", "missing_month", "future", "stale", "missing_asset", "wrong_cutoff"]
)
def test_invalid_market_inputs_block(case: str) -> None:
    value, inputs = contract(), request()
    if case == "short_ma":
        value = replace(
            value,
            feature_matrix=FeatureMatrixSpec(
                momentum_scores=(),
                moving_average_months=(4,),
                ma_window_includes_current_month=True,
                return_months=(),
                includes_latest_price=True,
            ),
        )
    elif case == "missing_month":
        prices = dict(inputs.prices)
        prices["ASSET_A"] = (PricePoint(date(2025, 12, 31), 10, DAY), *prices["ASSET_A"][1:])
        inputs = replace(inputs, prices=prices)
    elif case in ("future", "stale"):
        observed = date(2026, 4, 1) if case == "future" else date(2025, 1, 1)
        prices = dict(inputs.prices)
        prices["ASSET_A"] = tuple(replace(p, observed_on=observed) for p in prices["ASSET_A"])
        inputs = replace(inputs, prices=prices)
    elif case == "missing_asset":
        inputs = replace(inputs, prices={k: v for k, v in inputs.prices.items() if k != "ASSET_A"})
    else:
        inputs = replace(inputs, fixture_as_of=date(2026, 3, 30))
    with pytest.raises(ReplayBlockedError):
        replay(bundle(value), inputs)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), True])
def test_nonfinite_observations_rejected(bad: float) -> None:
    with pytest.raises(ReplayBlockedError):
        PricePoint(DAY, bad, DAY)
    with pytest.raises(ReplayBlockedError):
        MacroPoint(DAY, bad, DAY)


@pytest.mark.parametrize("bad", [0.0, -1.0])
def test_nonpositive_prices_rejected(bad: float) -> None:
    with pytest.raises(ReplayBlockedError):
        PricePoint(DAY, bad, DAY)


@pytest.mark.parametrize("missing", [False, True])
def test_named_negative_signal_is_executed_or_refuses_missing_score(*, missing: bool) -> None:
    record = replace(
        strategy(),
        signals_config={
            "caller_signal": {
                "kind": "negative_abs_momentum",
                "enabled": True,
                "threshold": 1,
                "scoring": {"method": "return_rate", "horizon": 2},
            }
        },
    )
    rows = {"ASSET_A": feature("ASSET_A", -0.1), "ASSET_B": feature("ASSET_B", 0.2)}
    if missing:
        del rows["ASSET_A"]
        with pytest.raises(ReplayBlockedError, match="missing signal"):
            evaluate_signals(
                strategy=record,
                macro={},
                features=rows,
                signal_date=DAY,
                specs=(),
                as_of=DAY,
                gates=GATES,
            )
    else:
        result = evaluate_signals(
            strategy=record,
            macro={},
            features=rows,
            signal_date=DAY,
            specs=(),
            as_of=DAY,
            gates=GATES,
        )
        assert result.flags == {"caller_signal": True}
        assert result.master_switch is True


@pytest.mark.parametrize(("mode", "expected"), [("OR", True), ("AND", False)])
def test_canary_combinations_and_missing_input(mode: str, *, expected: bool) -> None:
    record = replace(
        strategy(),
        canary_config={
            "canary_mode": mode,
            "assets": ["A", "B"],
            "enabled": [True, True],
            "scoring": {"method": "return_rate", "horizon": 2},
        },
    )
    rows = {"A": feature("A", -0.1), "B": feature("B", 0.2)}
    result = evaluate_signals(
        strategy=record, macro={}, features=rows, signal_date=DAY, specs=(), as_of=DAY, gates=GATES
    )
    assert result.canary is expected
    del rows["A"]
    with pytest.raises(ReplayBlockedError, match="missing canary"):
        evaluate_signals(
            strategy=record,
            macro={},
            features=rows,
            signal_date=DAY,
            specs=(),
            as_of=DAY,
            gates=GATES,
        )


def test_ensemble_weights_and_identity() -> None:
    rows = (MembershipRow("A", Decimal(2)), MembershipRow("B", Decimal(1)))
    membership = EnsembleMembership(rows, membership_hash(rows))
    result = combine_ensemble({"A": {"X": 1.0}, "B": {"Y": 1.0}}, membership)
    assert result == {"X": pytest.approx(2 / 3), "Y": pytest.approx(1 / 3)}
    with pytest.raises(ReplayBlockedError, match="missing"):
        combine_ensemble({"A": {"X": 1.0}}, membership)
    with pytest.raises(ValueError, match="digest"):
        EnsembleMembership(rows, "a" * 64)


def test_recent_ingestion_does_not_make_old_price_buckets_current() -> None:
    inputs = request()
    prices = {
        name: tuple(replace(p, as_of=p.as_of.replace(year=2025), observed_on=DAY) for p in points)
        for name, points in inputs.prices.items()
    }
    with pytest.raises(ReplayBlockedError, match="price bucket"):
        replay(bundle(contract()), replace(inputs, prices=prices))


def test_nonfinite_computed_features_cannot_reach_allocation() -> None:
    with pytest.raises(ReplayBlockedError, match="finite"):
        feature("A", float("inf"))


def test_calendar_horizon_is_bounded_without_an_unbounded_loop() -> None:
    with pytest.raises(ReplayBlockedError, match="supported dates"):
        lag_month(DAY, 10**100)


@pytest.mark.parametrize(
    ("decision", "knowledge", "drop", "latest"),
    [
        (date(2026, 1, 29), date(2026, 2, 1), 1, date(2026, 1, 29)),
        (date(2025, 12, 31), date(2026, 1, 2), 1, date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 1, 16), 15, date(2025, 12, 31)),
        (date(2026, 1, 29), date(2026, 1, 28), 1, date(2026, 1, 29)),
    ],
)
def test_knowledge_date_never_selects_economic_price_buckets(
    decision: date, knowledge: date, drop: int, latest: date
) -> None:
    dates = (lag_month(latest, 2), lag_month(latest, 1), latest)
    points = tuple(
        PricePoint(day, close, knowledge) for day, close in zip(dates, (10, 12, 15), strict=True)
    )
    # Both a dropped current month and future economic prices would change the score.
    if latest < decision:
        points += (PricePoint(decision, 999, knowledge),)
    points += (PricePoint(decision + timedelta(days=1), 9999, knowledge),)
    build = FeatureBuildRequest(contract().feature_matrix, decision, GATES, drop, 3)
    before = canonical_json_bytes(build)
    row = build_feature_matrix({"A": points}, build, knowledge_as_of=knowledge)["A"]
    assert (row.as_of, row.latest_price, row.returns) == (latest, 15, {2: 0.5})
    assert canonical_json_bytes(build) == before
    assert all(point.observed_on == knowledge for point in points)


@pytest.mark.parametrize("reason", [BlockReason.POST_CUTOFF, BlockReason.STALE_PRICE])
def test_explicit_knowledge_retains_price_guards(reason: BlockReason) -> None:
    inputs = request()
    knowledge = DAY + timedelta(days=1)
    observed = (
        knowledge + timedelta(days=1)
        if reason == BlockReason.POST_CUTOFF
        else DAY - timedelta(days=50)
    )
    prices = {
        name: tuple(replace(point, observed_on=observed) for point in points)
        for name, points in inputs.prices.items()
    }
    if reason == BlockReason.STALE_PRICE:
        assert replay(bundle(contract()), replace(inputs, prices=prices)).ensemble == {
            "ASSET_A": 1.0
        }
    with pytest.raises(ReplayBlockedError) as blocked:
        replay(bundle(contract()), replace(inputs, prices=prices), knowledge_as_of=knowledge)
    assert blocked.value.reason == reason


def test_default_knowledge_preserves_request_receipt_and_feature_bytes() -> None:
    source, inputs = bundle(contract()), request()
    original = canonical_json_bytes(inputs)
    receipt = replay(source, inputs)
    assert receipt.as_of == DAY
    assert receipt.ensemble == {"ASSET_A": 1.0}
    assert canonical_json_bytes(receipt) == canonical_json_bytes(
        replay(source, inputs, knowledge_as_of=None)
    )
    assert canonical_json_bytes(receipt) == canonical_json_bytes(
        replay(source, inputs, knowledge_as_of=DAY)
    )
    assert canonical_json_bytes(inputs) == original
    build = FeatureBuildRequest(contract().feature_matrix, DAY, GATES, 1, 3)
    assert build_feature_matrix(inputs.prices, build) == build_feature_matrix(
        inputs.prices, build, knowledge_as_of=DAY
    )
    with pytest.raises(ReplayBlockedError) as blocked:
        replay(source, replace(inputs, fixture_as_of=DAY - timedelta(days=1)), knowledge_as_of=DAY)
    assert blocked.value.reason == BlockReason.AS_OF_MISMATCH
