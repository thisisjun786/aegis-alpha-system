"""Independent arithmetic and prefix-invariance checks on fabricated inputs."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Literal, cast

import pytest

from aegis_alpha.engine.errors import ContractDefinitionError
from aegis_alpha.engine.risk import (
    DrawdownPolicy,
    RiskWindow,
    annualized_volatility,
    apply_drawdown_guard,
    inverse_volatility,
    period_returns,
    sample_covariance,
    target_volatility_scale,
    two_asset_minimum_variance,
)

DECISION = date(2020, 1, 6)
SESSIONS = (date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 6))
CLOSES = {"A": (100.0, 110.0, 99.0, 108.9), "B": (50.0, 55.0, 49.5, 54.45)}


def window(**overrides: object) -> RiskWindow:
    kwargs = {
        "decision_date": DECISION,
        "sessions": SESSIONS,
        "closes": CLOSES,
        "lookback_sessions": 3,
    }
    kwargs.update(overrides)
    return RiskWindow(**kwargs)


def test_risk_window_trims_to_trailing_lookback_and_sorts_assets() -> None:
    longer_sessions = (date(2019, 12, 30), *SESSIONS)
    longer_closes = {"A": (90.0, *CLOSES["A"]), "B": (45.0, *CLOSES["B"])}
    trimmed = RiskWindow(DECISION, longer_sessions, longer_closes, 3)
    assert trimmed.sessions == SESSIONS
    assert trimmed.closes == {"A": CLOSES["A"], "B": CLOSES["B"]}
    assert trimmed.asset_ids == ("A", "B")


def test_risk_window_rejects_future_session() -> None:
    with pytest.raises(ContractDefinitionError, match="decision_date"):
        window(sessions=(*SESSIONS[:-1], date(2020, 1, 7)), decision_date=date(2020, 1, 6))


def test_risk_window_rejects_non_increasing_or_datetime_sessions() -> None:
    with pytest.raises(ContractDefinitionError, match="strictly increasing"):
        window(sessions=(SESSIONS[0], SESSIONS[0], SESSIONS[2], SESSIONS[3]))
    with pytest.raises(ContractDefinitionError, match="not datetime"):
        window(sessions=(datetime(2020, 1, 1, 0, 0, tzinfo=UTC), *SESSIONS[1:]))


def test_risk_window_rejects_misaligned_or_nonpositive_closes() -> None:
    with pytest.raises(ContractDefinitionError, match="align"):
        window(closes={"A": CLOSES["A"][:-1], "B": CLOSES["B"]})
    with pytest.raises(ContractDefinitionError):
        window(closes={"A": (100.0, 110.0, 0.0, 108.9), "B": CLOSES["B"]})
    with pytest.raises(ContractDefinitionError):
        window(closes={"A": (100.0, 110.0, float("nan"), 108.9), "B": CLOSES["B"]})


def test_risk_window_requires_lookback_plus_one_sessions() -> None:
    with pytest.raises(ContractDefinitionError, match="lookback_sessions"):
        window(lookback_sessions=5)
    with pytest.raises(ContractDefinitionError, match="positive integer"):
        window(lookback_sessions=0)
    with pytest.raises(ContractDefinitionError, match="positive integer"):
        window(lookback_sessions=True)


def test_risk_window_rejects_empty_or_non_mapping_closes() -> None:
    with pytest.raises(ContractDefinitionError, match="nonempty mapping"):
        window(closes={})


def test_risk_window_copies_mutable_caller_inputs() -> None:
    sessions = list(SESSIONS)
    closes = {"A": list(CLOSES["A"]), "B": list(CLOSES["B"])}
    built = RiskWindow(
        DECISION,
        cast("tuple[date, ...]", sessions),
        cast("dict[str, tuple[float, ...]]", closes),
        3,
    )
    sessions.append(date(2020, 1, 7))
    closes["A"].append(999.0)
    closes["C"] = [1.0, 2.0, 3.0, 4.0]
    assert built.sessions == SESSIONS
    assert built.closes["A"] == CLOSES["A"]
    assert built.asset_ids == ("A", "B")


def test_period_returns_arithmetic() -> None:
    result = period_returns(window())
    assert result["A"] == pytest.approx((0.1, -0.1, 0.1))
    assert result["B"] == pytest.approx((0.1, -0.1, 0.1))


def test_period_returns_prefix_invariance_under_future_price_changes() -> None:
    base = window()
    changed = window(closes={"A": (100.0, 110.0, 99.0, 5000.0), "B": CLOSES["B"]})
    assert period_returns(base)["A"][:2] == period_returns(changed)["A"][:2]


def test_sample_covariance_hand_arithmetic_and_sorted_order() -> None:
    returns = {"B": (0.1, -0.1, 0.1), "A": (0.2, 0.0, -0.1)}
    result = sample_covariance(returns)
    a_values = returns["A"]
    b_values = returns["B"]
    mean_a_val = sum(a_values) / 3
    mean_b_val = sum(b_values) / 3
    expected_var_a = sum((v - mean_a_val) ** 2 for v in a_values) / 2
    expected_var_b = sum((v - mean_b_val) ** 2 for v in b_values) / 2
    expected_cov = (
        sum((a - mean_a_val) * (b - mean_b_val) for a, b in zip(a_values, b_values, strict=True))
        / 2
    )
    assert result[0][0] == pytest.approx(expected_var_a)
    assert result[1][1] == pytest.approx(expected_var_b)
    assert result[0][1] == pytest.approx(expected_cov)
    assert result[1][0] == pytest.approx(expected_cov)


def test_sample_covariance_rejects_short_or_unequal_or_empty() -> None:
    with pytest.raises(ContractDefinitionError, match="at least two"):
        sample_covariance({"A": (0.1,)})
    with pytest.raises(ContractDefinitionError, match="equal length"):
        sample_covariance({"A": (0.1, 0.2), "B": (0.1, 0.2, 0.3)})
    with pytest.raises(ContractDefinitionError, match="nonempty mapping"):
        sample_covariance({})


def test_inverse_volatility_weights_and_zero_variance_rejection() -> None:
    weights = inverse_volatility({"A": 4.0, "B": 1.0})
    assert weights["A"] == pytest.approx(1 / 3)
    assert weights["B"] == pytest.approx(2 / 3)
    assert sum(weights.values()) == pytest.approx(1.0)
    with pytest.raises(ContractDefinitionError):
        inverse_volatility({"A": 0.0, "B": 1.0})
    with pytest.raises(ContractDefinitionError):
        inverse_volatility({"A": -1.0, "B": 1.0})


def test_annualized_volatility_hand_arithmetic() -> None:
    returns = [0.01, -0.02, 0.015, 0.005]
    mean = sum(returns) / 4
    variance = sum((v - mean) ** 2 for v in returns) / 3
    expected = (variance**0.5) * (252**0.5)
    assert annualized_volatility(returns, periods_per_year=252) == pytest.approx(expected)


def test_annualized_volatility_requires_two_finite_returns_and_positive_periods() -> None:
    with pytest.raises(ContractDefinitionError, match="at least two"):
        annualized_volatility([0.01], periods_per_year=252)
    with pytest.raises(ContractDefinitionError):
        annualized_volatility([0.01, float("nan")], periods_per_year=252)
    with pytest.raises(ContractDefinitionError, match="positive integer"):
        annualized_volatility([0.01, 0.02], periods_per_year=0)


def test_target_volatility_scale_clips_to_ceiling_and_requires_positive_finite() -> None:
    assert target_volatility_scale(0.1, 0.2, 1.0) == pytest.approx(1.0)
    assert target_volatility_scale(0.2, 0.1, 1.0) == pytest.approx(0.5)
    with pytest.raises(ContractDefinitionError, match="at most 1"):
        target_volatility_scale(0.2, 0.1, 1.5)
    with pytest.raises(ContractDefinitionError):
        target_volatility_scale(0.0, 0.1, 1.0)
    with pytest.raises(ContractDefinitionError):
        target_volatility_scale(0.1, -0.1, 1.0)


def test_two_asset_minimum_variance_matches_hand_arithmetic() -> None:
    weight_a, weight_b = two_asset_minimum_variance(4.0, 9.0, 1.0, 0.0, 1.0)
    assert weight_a == pytest.approx(8 / 11)
    assert weight_b == pytest.approx(3 / 11)
    assert weight_a + weight_b == pytest.approx(1.0)


def test_two_asset_minimum_variance_rejects_non_psd_inputs() -> None:
    with pytest.raises(ContractDefinitionError, match="positive-definite"):
        two_asset_minimum_variance(4.0, 9.0, 10.0, 0.0, 1.0)


def test_two_asset_minimum_variance_rejects_perfectly_correlated_equal_variance() -> None:
    # var_a == var_b == cov_ab drives the minimum-variance denominator to zero;
    # that boundary is caught by the positive-definite check first.
    with pytest.raises(ContractDefinitionError, match="positive-definite"):
        two_asset_minimum_variance(4.0, 4.0, 4.0, 0.0, 1.0)


def test_two_asset_minimum_variance_rejects_malformed_bounds() -> None:
    with pytest.raises(ContractDefinitionError, match="0 <= lower_a <= upper_a <= 1"):
        two_asset_minimum_variance(4.0, 9.0, 1.0, 0.9, 0.5)
    with pytest.raises(ContractDefinitionError, match="0 <= lower_a <= upper_a <= 1"):
        two_asset_minimum_variance(4.0, 9.0, 1.0, -0.1, 1.0)


def test_two_asset_minimum_variance_clamps_unconstrained_optimum_to_bounds() -> None:
    # Unconstrained optimum is 8/11 (~0.727); [0, .5] is feasible and clamps
    # both weights to the box edge instead of being rejected as infeasible.
    weight_a, weight_b = two_asset_minimum_variance(4.0, 9.0, 1.0, 0.0, 0.5)
    assert weight_a == pytest.approx(0.5)
    assert weight_b == pytest.approx(0.5)


def test_two_asset_minimum_variance_clamps_to_lower_bound_too() -> None:
    weight_a, weight_b = two_asset_minimum_variance(4.0, 9.0, 1.0, 0.8, 1.0)
    assert weight_a == pytest.approx(0.8)
    assert weight_b == pytest.approx(0.2)


DRAWDOWN_SESSIONS = (date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 4))
DRAWDOWN_CLOSES = (100.0, 80.0, 95.0, 90.0)


def test_drawdown_running_reset_matches_worked_example() -> None:
    policy = DrawdownPolicy(0.1, "running", 2, reset_on_reentry=True)
    points = apply_drawdown_guard(DRAWDOWN_SESSIONS, DRAWDOWN_CLOSES, policy)
    highs = [p.high for p in points]
    invested = [p.invested for p in points]
    reentered = [p.reentered for p in points]
    assert highs == pytest.approx([100.0, 100.0, 100.0, 95.0])
    assert invested == [True, False, True, True]
    assert reentered == [False, False, True, False]
    assert points[2].drawdown == pytest.approx(0.05)


def test_drawdown_running_no_reset_keeps_original_high() -> None:
    policy = DrawdownPolicy(0.1, "running", 2, reset_on_reentry=False)
    points = apply_drawdown_guard(DRAWDOWN_SESSIONS, DRAWDOWN_CLOSES, policy)
    highs = [p.high for p in points]
    invested = [p.invested for p in points]
    assert highs == pytest.approx([100.0, 100.0, 100.0, 100.0])
    # Without a reset, the running high never drops from 100, so the exact
    # threshold boundary at close=90 (1 - 90/100 == threshold) re-triggers exit.
    assert invested == [True, False, True, False]
    assert points[2].reentered is True
    assert points[3].reentered is False


def test_drawdown_exit_and_reentry_use_multiplicative_threshold_boundary() -> None:
    # 1 - 90 / 100 is 0.09999999999999998 in double precision, so a division
    # based `drawdown >= threshold` comparison misses the exact boundary.
    # Comparing close against high * (1 - threshold) does not.
    policy = DrawdownPolicy(0.1, "running", 2, reset_on_reentry=True)
    reset_points = apply_drawdown_guard(DRAWDOWN_SESSIONS, DRAWDOWN_CLOSES, policy)
    no_reset_points = apply_drawdown_guard(
        DRAWDOWN_SESSIONS, DRAWDOWN_CLOSES, replace(policy, reset_on_reentry=False)
    )
    assert [p.invested for p in reset_points] == [True, False, True, True]
    assert [p.invested for p in no_reset_points] == [True, False, True, False]


def test_drawdown_rolling_bounds_window_to_lookback_sessions() -> None:
    policy = DrawdownPolicy(0.1, "rolling", 2, reset_on_reentry=True)
    points = apply_drawdown_guard(DRAWDOWN_SESSIONS, DRAWDOWN_CLOSES, policy)
    highs = [p.high for p in points]
    assert highs == pytest.approx([100.0, 100.0, 95.0, 95.0])


def test_drawdown_start_invested_and_source_semantics_flag() -> None:
    policy = DrawdownPolicy(0.1, "running", 2, reset_on_reentry=True)
    points = apply_drawdown_guard(DRAWDOWN_SESSIONS, DRAWDOWN_CLOSES, policy)
    assert points[0].invested is True
    assert points[0].reentered is False
    for point in points:
        assert point.source_semantics_certified is False


def test_drawdown_guard_prefix_invariance_under_future_price_changes() -> None:
    policy = DrawdownPolicy(0.1, "running", 2, reset_on_reentry=True)
    prefix = apply_drawdown_guard(DRAWDOWN_SESSIONS[:3], DRAWDOWN_CLOSES[:3], policy)
    extended_sessions = (*DRAWDOWN_SESSIONS, date(2020, 1, 5))
    extended_closes = (*DRAWDOWN_CLOSES, 1.0)
    extended = apply_drawdown_guard(extended_sessions, extended_closes, policy)
    assert prefix == extended[:3]


def test_drawdown_policy_rejects_invalid_threshold_and_convention() -> None:
    with pytest.raises(ContractDefinitionError, match="between 0 and 1"):
        DrawdownPolicy(1.0, "running", 2, reset_on_reentry=True)
    with pytest.raises(ContractDefinitionError, match="between 0 and 1"):
        DrawdownPolicy(0.0, "running", 2, reset_on_reentry=True)
    bad_convention = cast('Literal["rolling", "running"]', "invalid")
    with pytest.raises(ContractDefinitionError, match="rolling or running"):
        DrawdownPolicy(0.1, bad_convention, 2, reset_on_reentry=True)
    with pytest.raises(ContractDefinitionError, match="positive integer"):
        DrawdownPolicy(0.1, "rolling", 0, reset_on_reentry=True)
    with pytest.raises(ContractDefinitionError, match="boolean"):
        DrawdownPolicy(0.1, "running", 2, reset_on_reentry=cast("bool", 1))


def test_apply_drawdown_guard_rejects_misaligned_or_nonpositive_closes() -> None:
    policy = DrawdownPolicy(0.1, "running", 2, reset_on_reentry=True)
    with pytest.raises(ContractDefinitionError, match="align"):
        apply_drawdown_guard(DRAWDOWN_SESSIONS, DRAWDOWN_CLOSES[:-1], policy)
    with pytest.raises(ContractDefinitionError):
        apply_drawdown_guard(DRAWDOWN_SESSIONS, (100.0, 0.0, 95.0, 90.0), policy)


def test_apply_drawdown_guard_requires_validated_policy() -> None:
    with pytest.raises(ContractDefinitionError, match="validated DrawdownPolicy"):
        apply_drawdown_guard(DRAWDOWN_SESSIONS, DRAWDOWN_CLOSES, cast("DrawdownPolicy", None))


@pytest.mark.parametrize("magnitude", [1e155, 1e308])
def test_annualized_volatility_overflow_uses_contract_error(magnitude: float) -> None:
    with pytest.raises(ContractDefinitionError, match="finite"):
        annualized_volatility([magnitude, -magnitude], periods_per_year=1)


def test_annualization_count_overflow_uses_contract_error() -> None:
    with pytest.raises(ContractDefinitionError, match="periods_per_year"):
        annualized_volatility([0, 0], periods_per_year=10**1000)
