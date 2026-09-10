"""Tests for aegis_alpha.engine.research: grid expansion, scoring, holdout."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from typing import Literal, cast

import pytest

from aegis_alpha.engine.errors import ContractDefinitionError
from aegis_alpha.engine.research import (
    EvaluationFailure,
    HoldoutReceipt,
    ParameterGrid,
    SelectionPolicy,
    TrialOutcome,
    TrialParameters,
    TrialScores,
    TrialSpec,
    evaluate_holdout,
    generate_trials,
    score_trials,
)

_PARENT_HASH = "a" * 64
_TRAIN_END = date(2020, 1, 1)
_VALIDATION_END = date(2020, 6, 1)
_TEST_END = date(2020, 12, 1)
_EXPECTED_MOMENTUM_HORIZON = 6
_MINIMUM_LARGE_GRID_SIZE = 4

_UNIVERSE_A = ("ETF_A", "ETF_B")
_UNIVERSE_B = ("ETF_A", "ETF_C")
_TYPES_A: tuple[tuple[str, str], ...] = (("ETF_A", "ETF"), ("ETF_B", "ETF"))
_TYPES_B: tuple[tuple[str, str], ...] = (("ETF_A", "ETF"), ("ETF_C", "ETF"))


def _types_for(*universes: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    merged: dict[str, str] = {}
    for universe in universes:
        for instrument_id in universe:
            merged[instrument_id] = "ETF"
    return tuple(sorted(merged.items()))


def _grid(**overrides: object) -> ParameterGrid:
    fields: dict[str, object] = {
        "etf_universes": (_UNIVERSE_A, _UNIVERSE_B),
        "instrument_types": _types_for(_UNIVERSE_A, _UNIVERSE_B),
        "momentum_horizons": (3, 6),
        "absolute_filters": (True, False),
        "trend_filters": (True,),
        "weightings": ("equal", "inverse_volatility"),
        "volatility_caps": (0.1, 0.2),
    }
    fields.update(overrides)
    return ParameterGrid(**fields)


def _generate(grid: ParameterGrid | None = None, **overrides: object) -> tuple[TrialSpec, ...]:
    fields: dict[str, object] = {
        "grid": grid or _grid(),
        "parent_hash": _PARENT_HASH,
        "seed": 7,
        "train_end": _TRAIN_END,
        "validation_end": _VALIDATION_END,
        "test_end": _TEST_END,
        "cost_ref": "cost-v1",
        "max_trials": 1000,
    }
    fields.update(overrides)
    return generate_trials(**fields)


def _outcome(
    spec: TrialSpec,
    split: Literal["train", "validation", "test"],
    *,
    metric_value: float = 1.0,
    status: Literal["complete", "failed"] = "complete",
    error: str | None = None,
) -> TrialOutcome:
    if status == "failed":
        return TrialOutcome(
            trial_sha256=spec.canonical_sha256,
            split=split,
            status="failed",
            metrics=(),
            turnover=None,
            costs=None,
            error=error or "declared failure",
        )
    return TrialOutcome(
        trial_sha256=spec.canonical_sha256,
        split=split,
        status="complete",
        metrics=(("sharpe", metric_value),),
        turnover=0.2,
        costs=0.01,
        error=None,
    )


class _RecordingEvaluator:
    """Spies on every (trial, split) call and returns a scripted outcome."""

    def __init__(self, script: dict[tuple[str, str], TrialOutcome] | None = None) -> None:
        self.calls: list[tuple[str, str]] = []
        self._script = script or {}
        self._default_metric = 1.0

    def __call__(
        self, spec: TrialSpec, split: Literal["train", "validation", "test"]
    ) -> TrialOutcome:
        self.calls.append((spec.canonical_sha256, split))
        scripted = self._script.get((spec.canonical_sha256, split))
        if scripted is not None:
            return scripted
        return _outcome(spec, split, metric_value=self._default_metric)


def _single_trial_grid() -> ParameterGrid:
    return _grid(
        etf_universes=(_UNIVERSE_A,),
        instrument_types=_TYPES_A,
        momentum_horizons=(3,),
        absolute_filters=(True,),
        trend_filters=(True,),
        weightings=("equal",),
        volatility_caps=(0.1,),
    )


def _two_trial_grid() -> ParameterGrid:
    return _grid(
        etf_universes=(_UNIVERSE_A,),
        instrument_types=_TYPES_A,
        momentum_horizons=(3, 6),
        absolute_filters=(True,),
        trend_filters=(True,),
        weightings=("equal",),
        volatility_caps=(0.1,),
    )


# --- ParameterGrid / TrialParameters -----------------------------------


def test_parameter_grid_dedupes_and_sorts_axes() -> None:
    grid = ParameterGrid(
        etf_universes=(_UNIVERSE_A, _UNIVERSE_A, _UNIVERSE_B),
        instrument_types=_types_for(_UNIVERSE_A, _UNIVERSE_B),
        momentum_horizons=(6, 3, 3),
        absolute_filters=(True, True, False),
        trend_filters=(True,),
        weightings=("equal", "equal"),
        volatility_caps=(0.2, 0.1),
    )
    assert grid.etf_universes == (("ETF_A", "ETF_B"), ("ETF_A", "ETF_C"))
    assert grid.momentum_horizons == (3, 6)
    assert grid.absolute_filters == (False, True)
    assert grid.weightings == ("equal",)
    assert grid.volatility_caps == (0.1, 0.2)
    assert grid.size == 2 * 2 * 2 * 1 * 1 * 2


def test_parameter_grid_rejects_unknown_weighting_label() -> None:
    with pytest.raises(ContractDefinitionError):
        _grid(weightings=("equal", "risk_parity"))


def test_parameter_grid_rejects_duplicate_instrument_in_one_universe() -> None:
    with pytest.raises(ContractDefinitionError):
        _grid(etf_universes=(("ETF_A", "ETF_A"),), instrument_types=(("ETF_A", "ETF"),))


def test_parameter_grid_rejects_empty_axis() -> None:
    with pytest.raises(ContractDefinitionError):
        _grid(momentum_horizons=())


def test_parameter_grid_rejects_non_etf_instrument_type() -> None:
    with pytest.raises(ContractDefinitionError):
        _grid(instrument_types=(("ETF_A", "STOCK"), ("ETF_B", "ETF"), ("ETF_C", "ETF")))


def test_parameter_grid_rejects_missing_instrument_type() -> None:
    with pytest.raises(ContractDefinitionError):
        _grid(instrument_types=(("ETF_A", "ETF"), ("ETF_B", "ETF")))  # missing IWM


def test_parameter_grid_rejects_extra_instrument_type() -> None:
    with pytest.raises(ContractDefinitionError):
        _grid(instrument_types=(*_types_for(_UNIVERSE_A, _UNIVERSE_B), ("GLD", "ETF")))


def test_parameter_grid_rejects_duplicate_same_instrument_type_entry() -> None:
    with pytest.raises(ContractDefinitionError):
        _grid(
            etf_universes=(_UNIVERSE_A,),
            instrument_types=(("ETF_A", "ETF"), ("ETF_A", "ETF"), ("ETF_B", "ETF")),
        )


def test_parameter_grid_rejects_duplicate_conflicting_instrument_type_entry() -> None:
    with pytest.raises(ContractDefinitionError):
        _grid(
            etf_universes=(_UNIVERSE_A,),
            instrument_types=(("ETF_A", "STOCK"), ("ETF_A", "ETF"), ("ETF_B", "ETF")),
        )


def test_parameter_grid_rejects_oversized_instrument_type_pair() -> None:
    with pytest.raises(ContractDefinitionError):
        _grid(
            etf_universes=(_UNIVERSE_A,),
            instrument_types=(("ETF_A", "ETF", "extra"), ("ETF_B", "ETF")),
        )


def test_trial_parameters_freezes_input_universe_against_later_mutation() -> None:
    universe = ["ETF_A", "ETF_B"]
    parameters = TrialParameters(
        etf_universe=tuple(universe),
        instrument_types=_TYPES_A,
        momentum_horizon=3,
        absolute_filter=True,
        trend_filter=True,
        weighting="equal",
        volatility_cap=0.1,
    )
    universe.append("ETF_C")
    assert parameters.etf_universe == ("ETF_A", "ETF_B")


def test_trial_parameters_rejects_instrument_types_not_matching_universe() -> None:
    with pytest.raises(ContractDefinitionError):
        TrialParameters(
            etf_universe=("ETF_A", "ETF_B"),
            instrument_types=(("ETF_A", "ETF"),),
            momentum_horizon=3,
            absolute_filter=True,
            trend_filter=True,
            weighting="equal",
            volatility_cap=0.1,
        )


def test_trial_parameters_rejects_non_etf_type() -> None:
    with pytest.raises(ContractDefinitionError):
        TrialParameters(
            etf_universe=("ETF_A", "ETF_B"),
            instrument_types=(("ETF_A", "STOCK"), ("ETF_B", "ETF")),
            momentum_horizon=3,
            absolute_filter=True,
            trend_filter=True,
            weighting="equal",
            volatility_cap=0.1,
        )


def test_trial_parameters_rejects_duplicate_same_instrument_type_entry() -> None:
    with pytest.raises(ContractDefinitionError):
        TrialParameters(
            etf_universe=("ETF_A", "ETF_B"),
            instrument_types=(("ETF_A", "ETF"), ("ETF_A", "ETF"), ("ETF_B", "ETF")),
            momentum_horizon=3,
            absolute_filter=True,
            trend_filter=True,
            weighting="equal",
            volatility_cap=0.1,
        )


def test_trial_parameters_rejects_duplicate_conflicting_instrument_type_entry() -> None:
    with pytest.raises(ContractDefinitionError):
        TrialParameters(
            etf_universe=("ETF_A", "ETF_B"),
            instrument_types=(("ETF_A", "STOCK"), ("ETF_A", "ETF"), ("ETF_B", "ETF")),
            momentum_horizon=3,
            absolute_filter=True,
            trend_filter=True,
            weighting="equal",
            volatility_cap=0.1,
        )


def test_trial_parameters_rejects_oversized_instrument_type_pair() -> None:
    with pytest.raises(ContractDefinitionError):
        TrialParameters(
            etf_universe=("ETF_A", "ETF_B"),
            instrument_types=cast(
                "tuple[tuple[str, str], ...]", (("ETF_A", "ETF", "extra"), ("ETF_B", "ETF"))
            ),
            momentum_horizon=3,
            absolute_filter=True,
            trend_filter=True,
            weighting="equal",
            volatility_cap=0.1,
        )


# --- generate_trials -----------------------------------------------------


def test_generate_trials_is_deterministic_and_deduplicated() -> None:
    grid = _grid()
    first = _generate(grid)
    second = _generate(grid)
    assert [spec.canonical_sha256 for spec in first] == [spec.canonical_sha256 for spec in second]
    assert len(first) == grid.size
    assert len({spec.canonical_sha256 for spec in first}) == len(first)


def test_generate_trials_preserves_original_parameters() -> None:
    trials = _generate(
        _grid(
            etf_universes=(_UNIVERSE_A,),
            instrument_types=_TYPES_A,
            momentum_horizons=(6,),
            absolute_filters=(True,),
            trend_filters=(False,),
            weightings=("inverse_volatility",),
            volatility_caps=(0.15,),
        )
    )
    assert len(trials) == 1
    parameters = trials[0].parameter_grid
    assert parameters.etf_universe == _UNIVERSE_A
    assert parameters.instrument_types == _TYPES_A
    assert parameters.momentum_horizon == _EXPECTED_MOMENTUM_HORIZON
    assert parameters.absolute_filter is True
    assert parameters.trend_filter is False
    assert parameters.weighting == "inverse_volatility"
    assert parameters.volatility_cap == pytest.approx(0.15)


def test_generate_trials_guards_oversized_grid_before_expansion() -> None:
    grid = _grid()
    assert grid.size > _MINIMUM_LARGE_GRID_SIZE
    with pytest.raises(ContractDefinitionError):
        _generate(grid, max_trials=1)


def test_generate_trials_rejects_out_of_order_split_dates() -> None:
    with pytest.raises(ContractDefinitionError):
        _generate(train_end=_VALIDATION_END, validation_end=_TRAIN_END)


def test_generate_trials_rejects_nonpositive_max_trials() -> None:
    with pytest.raises(ContractDefinitionError):
        _generate(max_trials=0)


def test_generate_trials_rejects_non_iso_date_string_round_trip() -> None:
    with pytest.raises(ContractDefinitionError):
        TrialSpec.from_document(
            {
                **_generate(_single_trial_grid())[0].to_document(),
                "train_end": "2020-1-1",
            }
        )


# --- TrialSpec / TrialOutcome documents ----------------------------------


def test_trial_spec_round_trips_through_document() -> None:
    spec = _generate()[0]
    document = spec.to_document()
    restored = TrialSpec.from_document(document)
    assert restored.canonical_sha256 == spec.canonical_sha256
    assert restored.parameter_grid.etf_universe == spec.parameter_grid.etf_universe
    assert restored.parameter_grid.instrument_types == spec.parameter_grid.instrument_types


def test_trial_spec_from_document_rejects_unknown_field() -> None:
    document = dict(_generate()[0].to_document())
    document["extra_field"] = "unexpected"
    with pytest.raises(ContractDefinitionError):
        TrialSpec.from_document(document)


def test_trial_spec_from_document_rejects_missing_field() -> None:
    document = dict(_generate()[0].to_document())
    del document["cost_ref"]
    with pytest.raises(ContractDefinitionError):
        TrialSpec.from_document(document)


def test_trial_spec_from_document_rejects_tampered_hash() -> None:
    document = dict(_generate()[0].to_document())
    document["canonical_sha256"] = "0" * 64
    with pytest.raises(ContractDefinitionError):
        TrialSpec.from_document(document)


def test_trial_spec_from_document_rejects_omitted_instrument_types() -> None:
    document = dict(_generate()[0].to_document())
    parameter_grid = dict(cast("Mapping[str, object]", document["parameter_grid"]))
    del parameter_grid["instrument_types"]
    document["parameter_grid"] = parameter_grid
    with pytest.raises(ContractDefinitionError):
        TrialSpec.from_document(document)


def test_trial_spec_from_document_rejects_duplicate_instrument_type_entry() -> None:
    document = dict(_generate(_single_trial_grid())[0].to_document())
    parameter_grid = dict(cast("Mapping[str, object]", document["parameter_grid"]))
    parameter_grid["instrument_types"] = [["ETF_A", "ETF"], ["ETF_A", "STOCK"], ["ETF_B", "ETF"]]
    document["parameter_grid"] = parameter_grid
    with pytest.raises(ContractDefinitionError):
        TrialSpec.from_document(document)


def test_trial_spec_from_document_rejects_oversized_instrument_type_pair() -> None:
    document = dict(_generate(_single_trial_grid())[0].to_document())
    parameter_grid = dict(cast("Mapping[str, object]", document["parameter_grid"]))
    parameter_grid["instrument_types"] = [["ETF_A", "ETF", "extra"], ["ETF_B", "ETF"]]
    document["parameter_grid"] = parameter_grid
    with pytest.raises(ContractDefinitionError):
        TrialSpec.from_document(document)


def test_trial_outcome_complete_requires_nonnegative_turnover_and_costs() -> None:
    spec = _generate()[0]
    with pytest.raises(ContractDefinitionError):
        TrialOutcome(
            trial_sha256=spec.canonical_sha256,
            split="train",
            status="complete",
            metrics=(("sharpe", 1.0),),
            turnover=-0.1,
            costs=0.0,
            error=None,
        )


def test_trial_outcome_complete_rejects_error() -> None:
    spec = _generate()[0]
    with pytest.raises(ContractDefinitionError):
        TrialOutcome(
            trial_sha256=spec.canonical_sha256,
            split="train",
            status="complete",
            metrics=(("sharpe", 1.0),),
            turnover=0.1,
            costs=0.0,
            error="should not be here",
        )


def test_trial_outcome_failed_requires_nonempty_error_and_no_metrics() -> None:
    spec = _generate()[0]
    with pytest.raises(ContractDefinitionError):
        TrialOutcome(
            trial_sha256=spec.canonical_sha256,
            split="train",
            status="failed",
            metrics=(("sharpe", 1.0),),
            turnover=None,
            costs=None,
            error="oops",
        )
    with pytest.raises(ContractDefinitionError):
        TrialOutcome(
            trial_sha256=spec.canonical_sha256,
            split="train",
            status="failed",
            metrics=(),
            turnover=None,
            costs=None,
            error="",
        )


def test_trial_outcome_metrics_are_immutable_against_later_input_mutation() -> None:
    spec = _generate()[0]
    metrics: list[tuple[str, float]] = [("sharpe", 1.0)]
    outcome = TrialOutcome(
        trial_sha256=spec.canonical_sha256,
        split="train",
        status="complete",
        metrics=cast("tuple[tuple[str, float], ...]", metrics),
        turnover=0.1,
        costs=0.0,
        error=None,
    )
    metrics.append(("extra", 2.0))
    assert outcome.metrics == (("sharpe", 1.0),)


# --- score_trials ---------------------------------------------------------


def test_score_trials_calls_exactly_train_and_validation_never_test() -> None:
    trials = _generate(_two_trial_grid())
    evaluator = _RecordingEvaluator()
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    assert isinstance(scores, TrialScores)
    splits_seen = {split for _, split in evaluator.calls}
    assert splits_seen == {"train", "validation"}
    assert len(evaluator.calls) == 2 * len(trials)
    for spec in trials:
        assert evaluator.calls.count((spec.canonical_sha256, "train")) == 1
        assert evaluator.calls.count((spec.canonical_sha256, "validation")) == 1


def test_score_trials_picks_deterministic_maximize_winner() -> None:
    trials = _generate(_two_trial_grid())
    script = {
        (trials[0].canonical_sha256, "train"): _outcome(trials[0], "train", metric_value=1.0),
        (trials[0].canonical_sha256, "validation"): _outcome(
            trials[0], "validation", metric_value=1.0
        ),
        (trials[1].canonical_sha256, "train"): _outcome(trials[1], "train", metric_value=1.0),
        (trials[1].canonical_sha256, "validation"): _outcome(
            trials[1], "validation", metric_value=3.0
        ),
    }
    evaluator = _RecordingEvaluator(script)
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    assert scores.winner == trials[1].canonical_sha256


def test_score_trials_breaks_ties_deterministically_by_trial_hash() -> None:
    trials = _generate(_two_trial_grid())
    evaluator = _RecordingEvaluator()  # identical default metric for every trial
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    expected_winner = min(spec.canonical_sha256 for spec in trials)
    assert scores.winner == expected_winner


def test_score_trials_excludes_trial_with_failed_train_or_validation() -> None:
    trials = _generate(_two_trial_grid())
    script = {
        (trials[0].canonical_sha256, "train"): _outcome(trials[0], "train", status="failed"),
    }
    evaluator = _RecordingEvaluator(script)
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    assert scores.winner == trials[1].canonical_sha256


def test_score_trials_no_valid_trials_yields_none_winner() -> None:
    trials = _generate(_single_trial_grid())
    assert len(trials) == 1
    evaluator = _RecordingEvaluator(
        {(trials[0].canonical_sha256, "train"): _outcome(trials[0], "train", status="failed")}
    )
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    assert scores.winner is None


def test_score_trials_missing_selection_metric_is_a_contract_failure() -> None:
    trials = _generate(_single_trial_grid())

    def evaluator(s: TrialSpec, split: Literal["train", "validation", "test"]) -> TrialOutcome:
        return TrialOutcome(
            trial_sha256=s.canonical_sha256,
            split=split,
            status="complete",
            metrics=(("other_metric", 1.0),),
            turnover=0.1,
            costs=0.0,
            error=None,
        )

    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    with pytest.raises(ContractDefinitionError):
        score_trials(trials, evaluator, policy)


def test_score_trials_converts_evaluation_failure_into_failed_outcome() -> None:
    trials = _generate(_single_trial_grid())

    def evaluator(s: TrialSpec, split: Literal["train", "validation", "test"]) -> TrialOutcome:
        if split == "train":
            raise EvaluationFailure("no data available")
        return _outcome(s, split)

    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    train_outcome = next(o for o in scores.outcomes if o.split == "train")
    assert train_outcome.status == "failed"
    assert train_outcome.error == "no data available"
    assert scores.winner is None


def test_score_trials_propagates_unrelated_value_error() -> None:
    trials = _generate(_single_trial_grid())

    def evaluator(_spec: TrialSpec, _split: Literal["train", "validation", "test"]) -> TrialOutcome:
        raise ValueError("unrelated failure")

    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    with pytest.raises(ValueError, match="unrelated failure"):
        score_trials(trials, evaluator, policy)


def test_score_trials_propagates_type_error() -> None:
    trials = _generate(_single_trial_grid())

    def evaluator(_spec: TrialSpec, _split: Literal["train", "validation", "test"]) -> TrialOutcome:
        raise TypeError("programming fault")

    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    with pytest.raises(TypeError):
        score_trials(trials, evaluator, policy)


def test_score_trials_propagates_runtime_error() -> None:
    trials = _generate(_single_trial_grid())

    def evaluator(_spec: TrialSpec, _split: Literal["train", "validation", "test"]) -> TrialOutcome:
        raise RuntimeError("programming fault")

    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    with pytest.raises(RuntimeError):
        score_trials(trials, evaluator, policy)


def test_score_trials_rejects_outcome_with_mismatched_split() -> None:
    trials = _generate(_single_trial_grid())

    def evaluator(s: TrialSpec, split: Literal["train", "validation", "test"]) -> TrialOutcome:
        return _outcome(s, "validation" if split == "train" else "train")

    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    with pytest.raises(ContractDefinitionError):
        score_trials(trials, evaluator, policy)


def test_trial_scores_rejects_duplicate_trial_ids() -> None:
    trials = _generate(_single_trial_grid())
    spec = trials[0]
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    with pytest.raises(ContractDefinitionError):
        TrialScores(
            policy=policy,
            trials=(spec, spec),
            outcomes=(
                _outcome(spec, "train"),
                _outcome(spec, "validation"),
            ),
        )


def test_trial_scores_rejects_test_split_outcome() -> None:
    trials = _generate(_single_trial_grid())
    spec = trials[0]
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    with pytest.raises(ContractDefinitionError):
        TrialScores(
            policy=policy,
            trials=(spec,),
            outcomes=(
                _outcome(spec, "train"),
                _outcome(spec, "validation"),
                _outcome(spec, "test"),
            ),
        )


def test_trial_scores_rejects_partial_outcome_group() -> None:
    trials = _generate(_single_trial_grid())
    spec = trials[0]
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    with pytest.raises(ContractDefinitionError):
        TrialScores(policy=policy, trials=(spec,), outcomes=(_outcome(spec, "train"),))


def test_trial_scores_rejects_outcome_for_unknown_trial() -> None:
    trials = _generate(_single_trial_grid())
    other_trials = _generate(_single_trial_grid(), seed=99)
    spec, foreign_spec = trials[0], other_trials[0]
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    with pytest.raises(ContractDefinitionError):
        TrialScores(
            policy=policy,
            trials=(spec,),
            outcomes=(
                _outcome(spec, "train"),
                _outcome(spec, "validation"),
                _outcome(foreign_spec, "train"),
            ),
        )


def test_trial_scores_always_derives_its_own_winner() -> None:
    """Even a hand-built TrialScores recomputes winner; it is never trusted input."""
    trials = _generate(_two_trial_grid())
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    outcomes = (
        _outcome(trials[0], "train", metric_value=1.0),
        _outcome(trials[0], "validation", metric_value=1.0),
        _outcome(trials[1], "train", metric_value=1.0),
        _outcome(trials[1], "validation", metric_value=5.0),
    )
    scores = TrialScores(policy=policy, trials=trials, outcomes=outcomes)
    assert scores.winner == trials[1].canonical_sha256


# --- evaluate_holdout ------------------------------------------------------


def test_evaluate_holdout_calls_test_exactly_once_for_winner() -> None:
    trials = _generate(_single_trial_grid())
    evaluator = _RecordingEvaluator()
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    evaluator.calls.clear()
    receipt = evaluate_holdout(scores, evaluator, [])
    assert evaluator.calls == [(scores.winner, "test")]
    assert isinstance(receipt, HoldoutReceipt)
    assert receipt.trial_sha256 == scores.winner
    assert receipt.outcome.split == "test"
    assert receipt.contaminated is False
    assert receipt.prior_receipt_hashes == ()
    assert receipt.research_only is True
    assert receipt.history_complete_verified is False


def test_evaluate_holdout_raises_when_no_winner() -> None:
    trials = _generate(_single_trial_grid())
    evaluator = _RecordingEvaluator()
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(
        trials,
        lambda s, split: _outcome(s, split, status="failed"),
        policy,
    )
    with pytest.raises(ContractDefinitionError):
        evaluate_holdout(scores, evaluator, [])


def test_evaluate_holdout_flags_contamination_from_same_parent_hash() -> None:
    trials = _generate(_single_trial_grid())
    evaluator = _RecordingEvaluator()
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    first_receipt = evaluate_holdout(scores, evaluator, [])
    second_receipt = evaluate_holdout(scores, evaluator, [first_receipt])
    assert second_receipt.contaminated is True
    assert second_receipt.prior_receipt_hashes == (first_receipt.canonical_sha256,)


def test_evaluate_holdout_contamination_holds_even_for_failed_prior_outcome() -> None:
    trials = _generate(_single_trial_grid())
    evaluator = _RecordingEvaluator()
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    failed_receipt = evaluate_holdout(
        scores,
        lambda s, split: _outcome(s, split, status="failed"),
        [],
    )
    assert failed_receipt.contaminated is False
    new_receipt = evaluate_holdout(scores, evaluator, [failed_receipt])
    assert new_receipt.contaminated is True


def test_holdout_receipt_round_trips_through_document() -> None:
    trials = _generate(_single_trial_grid())
    evaluator = _RecordingEvaluator()
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    receipt = evaluate_holdout(scores, evaluator, [])
    document = receipt.to_document()
    restored = HoldoutReceipt.from_document(document)
    assert restored.canonical_sha256 == receipt.canonical_sha256
    assert restored.research_only is True
    assert restored.history_complete_verified is False


def test_holdout_receipt_from_document_rejects_tampered_hash() -> None:
    trials = _generate(_single_trial_grid())
    evaluator = _RecordingEvaluator()
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    receipt = evaluate_holdout(scores, evaluator, [])
    document = dict(receipt.to_document())
    document["contaminated"] = not document["contaminated"]
    with pytest.raises(ContractDefinitionError):
        HoldoutReceipt.from_document(document)


def test_holdout_receipt_from_document_rejects_flipped_research_only_flag() -> None:
    trials = _generate(_single_trial_grid())
    evaluator = _RecordingEvaluator()
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    receipt = evaluate_holdout(scores, evaluator, [])
    document = dict(receipt.to_document())
    document["research_only"] = False
    with pytest.raises(ContractDefinitionError):
        HoldoutReceipt.from_document(document)


def test_holdout_receipt_from_document_rejects_flipped_history_complete_flag() -> None:
    trials = _generate(_single_trial_grid())
    evaluator = _RecordingEvaluator()
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    receipt = evaluate_holdout(scores, evaluator, [])
    document = dict(receipt.to_document())
    document["history_complete_verified"] = True
    with pytest.raises(ContractDefinitionError):
        HoldoutReceipt.from_document(document)


def test_evaluate_holdout_rejects_unvalidated_prior_receipt() -> None:
    trials = _generate(_single_trial_grid())
    evaluator = _RecordingEvaluator()
    policy = SelectionPolicy(metric="sharpe", direction="maximize")
    scores = score_trials(trials, evaluator, policy)
    bad_receipts = cast("Sequence[HoldoutReceipt]", [{"not": "a receipt"}])
    with pytest.raises(ContractDefinitionError):
        evaluate_holdout(scores, evaluator, bad_receipts)


def test_trial_scores_rejects_wholly_omitted_trial_outcomes() -> None:
    trials = _generate()[:2]
    with pytest.raises(ContractDefinitionError, match="every trial"):
        TrialScores(
            policy=SelectionPolicy("sharpe", "maximize"),
            trials=trials,
            outcomes=(
                _outcome(trials[0], "train"),
                _outcome(trials[0], "validation"),
            ),
        )
