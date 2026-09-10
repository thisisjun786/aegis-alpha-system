"""Synthetic evidence tests for comparison, missingness and stable identities."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime
from typing import cast

import pytest

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.errors import ContractDefinitionError
from aegis_alpha.engine.etf_candidates import (
    ComparisonPolicy,
    ETFProfile,
    TrackingMeasure,
    compare_etfs,
)

START = date(2020, 1, 2)
END = date(2020, 12, 31)
AS_OF = date(2021, 1, 5)


def profile(identity: str = "REFERENCE", fee: float = 20) -> ETFProfile:
    return ETFProfile(
        identity,
        "EXPOSURE",
        "UNIT",
        False,  # noqa: FBT003 -- positional synthetic immutable profile
        1,
        "none",
        fee,
        date(2010, 1, 1),
        AS_OF,
        "a" * 64,
        TrackingMeasure(START, END, 0.03, "b" * 64),
        1000,
    )


def policy() -> ComparisonPolicy:
    return ComparisonPolicy(AS_OF, 10, 100, 2, 0.04, START, END)


@pytest.mark.parametrize("field", ["fee_bps", "liquidity", "leverage"])
@pytest.mark.parametrize("value", [True, "1", float("nan"), float("inf")])
def test_invalid_profile_numbers(field: str, value: object) -> None:
    with pytest.raises(ContractDefinitionError):
        replace(profile(), **{field: value})


@pytest.mark.parametrize("field", ["fee_bps", "liquidity"])
def test_negative_evidence_is_not_missing(field: str) -> None:
    with pytest.raises(ContractDefinitionError, match="nonnegative"):
        replace(profile(), **{field: -1})


@pytest.mark.parametrize("field", ["instrument_id", "exposure_id", "currency", "reset"])
def test_empty_identity_rejected(field: str) -> None:
    with pytest.raises(ContractDefinitionError, match="text"):
        replace(profile(), **{field: " "})


@pytest.mark.parametrize(
    "field", ["fee_bps", "inception", "as_of", "source_hash", "tracking", "liquidity"]
)
@pytest.mark.parametrize("missing_reference", [False, True])
def test_missing_evidence_constructible_and_not_ranked(
    field: str, *, missing_reference: bool
) -> None:
    current, candidate = profile(), profile("CANDIDATE", 5)
    if missing_reference:
        current = replace(current, **{field: None})
    else:
        candidate = replace(candidate, **{field: None})
    result = compare_etfs(current, [candidate], policy())
    assert result.decisions[0].status == "insufficient_evidence"
    who = "reference" if missing_reference else "candidate"
    assert result.decisions[0].reason == f"{who}_missing_{field}"
    assert result.decisions[0].fee_saving_bps is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("exposure_id", "OTHER"),
        ("currency", "OTHER"),
        ("hedged", True),
        ("leverage", -1),
        ("leverage", 2),
        ("reset", "daily"),
    ],
)
def test_cheaper_wrong_exposure_is_excluded_first(field: str, value: object) -> None:
    candidate = replace(profile("OTHER", 0), **{field: value})
    result = compare_etfs(replace(profile(), source_hash=None), [candidate], policy())
    assert result.decisions[0].status == "excluded"
    assert result.decisions[0].reason == "exposure_mismatch"


@pytest.mark.parametrize("reference", [False, True])
def test_future_and_stale_profiles_not_ranked(*, reference: bool) -> None:
    for day, reason in [(date(2021, 1, 6), "future_profile"), (END, "stale_profile")]:
        current, candidate = profile(), profile("CANDIDATE", 5)
        if reference:
            current = replace(current, as_of=day)
        else:
            candidate = replace(candidate, as_of=day)
        result = compare_etfs(current, [candidate], replace(policy(), max_profile_age_days=0))
        who = "reference" if reference else "candidate"
        assert result.decisions[0].reason == f"{who}_{reason}"
        assert result.decisions[0].status == "insufficient_evidence"


def test_policy_window_rejects_two_equally_stale_measurements() -> None:
    changed = TrackingMeasure(date(2019, 1, 1), date(2019, 12, 31), 0.01, "c" * 64)
    result = compare_etfs(
        replace(profile(), tracking=changed),
        [replace(profile("CANDIDATE", 5), tracking=changed)],
        policy(),
    )
    assert result.decisions[0].reason == "reference_tracking_window_mismatch"


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("liquidity", 50, "candidate_liquidity_below_minimum"),
        ("fee_bps", 19, "fee_saving_below_minimum"),
    ],
)
def test_threshold_exclusions(field: str, value: float, reason: str) -> None:
    result = compare_etfs(profile(), [replace(profile("CANDIDATE", 5), **{field: value})], policy())
    assert result.decisions[0].status == "excluded"
    assert result.decisions[0].reason == reason


@pytest.mark.parametrize(
    ("error", "reason"),
    [(0.05, "tracking_error_above_maximum"), (0.035, "tracking_worse_than_reference")],
)
def test_lower_fee_cannot_hide_tracking_deterioration(error: float, reason: str) -> None:
    candidate = profile("CANDIDATE", 5)
    candidate = replace(candidate, tracking=TrackingMeasure(START, END, error, "b" * 64))
    result = compare_etfs(profile(), [candidate], policy())
    assert result.decisions[0].reason == reason


def test_reference_liquidity_is_checked() -> None:
    result = compare_etfs(replace(profile(), liquidity=1), [profile("CANDIDATE", 5)], policy())
    assert result.decisions[0].reason == "reference_liquidity_below_minimum"


def test_stable_identity_rename_ambiguity_rejected() -> None:
    with pytest.raises(ContractDefinitionError, match="duplicate stable"):
        compare_etfs(profile(), [profile("REFERENCE", 5)], policy())
    with pytest.raises(ContractDefinitionError, match="duplicate stable"):
        compare_etfs(profile(), [profile("CANDIDATE", 5), profile("CANDIDATE", 7)], policy())


def test_input_order_and_mutation_cannot_change_match_ranking() -> None:
    candidates = [profile("Z", 5), profile("A", 5), profile("C", 4)]
    result = compare_etfs(profile(), candidates, policy())
    candidates.clear()
    assert [row.instrument_id for row in result.decisions] == ["C", "A", "Z"]
    assert tuple(row.instrument_id for row in result.candidates) == ("Z", "A", "C")
    assert [row.fee_saving_bps for row in result.decisions] == [16, 15, 15]
    for field in ("research_only", "automatic_replacement", "source_pins_verified"):
        with pytest.raises(FrozenInstanceError):
            setattr(result, field, False)


def test_empty_and_incomplete_reference_report() -> None:
    result = compare_etfs(replace(profile(), tracking=None), [], policy())
    assert result.decisions == ()
    assert result.reference_status == "missing_tracking"


def test_zero_fee_and_tracking_are_valid_evidence() -> None:
    candidate = replace(profile("CANDIDATE", 0), tracking=TrackingMeasure(START, END, 0, "b" * 64))
    result = compare_etfs(profile(), [candidate], policy())
    assert result.decisions[0].status == "matched"
    document = json.loads(canonical_json_bytes(result))
    assert document["source_pins_verified"] is False
    assert document["automatic_replacement"] is False
    assert document["research_only"] is True
    assert document["candidates"][0]["tracking"]["source_hash"] == "b" * 64


@pytest.mark.parametrize("value", ["A" * 64, "short", "", 1])
def test_invalid_hash_rejected(value: object) -> None:
    with pytest.raises(ContractDefinitionError, match="SHA256"):
        replace(profile(), source_hash=cast("str", value))


def test_dates_and_boolean_contracts() -> None:
    with pytest.raises(ContractDefinitionError, match="date"):
        replace(profile(), as_of=datetime(2021, 1, 5, tzinfo=UTC))
    with pytest.raises(ContractDefinitionError, match="inception"):
        replace(profile(), inception=date(2022, 1, 1))
    with pytest.raises(ContractDefinitionError, match="tracking"):
        replace(profile(), as_of=date(2020, 1, 1))
    with pytest.raises(ContractDefinitionError, match="boolean"):
        replace(profile(), hedged=cast("bool", 1))
    with pytest.raises(ContractDefinitionError, match="integer"):
        replace(policy(), max_profile_age_days=True)


def test_equal_fee_is_not_a_lower_cost_match() -> None:
    result = compare_etfs(
        profile(), [profile("CANDIDATE", 20)], replace(policy(), min_fee_saving_bps=0)
    )
    assert result.decisions[0].status == "excluded"
    assert result.decisions[0].reason == "fee_saving_below_minimum"


def test_every_candidate_has_one_disposition_and_tracking_breaks_fee_tie() -> None:
    better = replace(profile("Z", 5), tracking=TrackingMeasure(START, END, 0.01, "c" * 64))
    candidates = [
        profile("A", 5),
        better,
        replace(profile("MISSING", 1), tracking=None),
        replace(profile("FOREIGN", 0), currency="OTHER"),
    ]
    result = compare_etfs(profile(), candidates, policy())
    assert [(row.instrument_id, row.status) for row in result.decisions] == [
        ("Z", "matched"),
        ("A", "matched"),
        ("FOREIGN", "excluded"),
        ("MISSING", "insufficient_evidence"),
    ]
    assert {row.instrument_id for row in result.decisions} == {
        row.instrument_id for row in candidates
    }


@pytest.mark.parametrize("bad", ["profiles", [None], {"candidate": "profile"}])
def test_untyped_candidates_fail_at_boundary(bad: object) -> None:
    with pytest.raises(ContractDefinitionError, match="ETFProfile"):
        compare_etfs(profile(), cast("list[ETFProfile]", bad), policy())


def test_default_tracking_basis_and_positional_backward_compatibility() -> None:
    measure = TrackingMeasure(START, END, 0.03, "b" * 64)
    assert measure.basis == "net_total_return"
    matching_policy = ComparisonPolicy(AS_OF, 10, 100, 2, 0.04, START, END)
    assert matching_policy.tracking_basis == "net_total_return"


def test_empty_tracking_basis_rejected() -> None:
    with pytest.raises(ContractDefinitionError, match="text"):
        replace(TrackingMeasure(START, END, 0.03, "b" * 64), basis=" ")
    with pytest.raises(ContractDefinitionError, match="text"):
        replace(policy(), tracking_basis=" ")


@pytest.mark.parametrize("reference", [False, True])
def test_tracking_basis_mismatch_checked_before_thresholds(*, reference: bool) -> None:
    mismatched = TrackingMeasure(
        START, END, 0.03, "b" * 64, basis="fund_market_tr_after_expenses_vs_index_gross_tr"
    )
    current, candidate = profile(), profile("CANDIDATE", 5)
    if reference:
        current = replace(current, tracking=mismatched)
    else:
        candidate = replace(candidate, tracking=mismatched)
    result = compare_etfs(current, [candidate], policy())
    who = "reference" if reference else "candidate"
    assert result.decisions[0].status == "insufficient_evidence"
    assert result.decisions[0].reason == f"{who}_tracking_basis_mismatch"
    assert result.decisions[0].fee_saving_bps is None


def test_custom_matching_tracking_basis_is_accepted() -> None:
    custom = "fund_market_tr_after_expenses_vs_index_gross_tr"
    measure = TrackingMeasure(START, END, 0.01, "c" * 64, basis=custom)
    result = compare_etfs(
        replace(profile(), tracking=measure),
        [replace(profile("CANDIDATE", 5), tracking=measure)],
        replace(policy(), tracking_basis=custom),
    )
    assert result.decisions[0].status == "matched"
