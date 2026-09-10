"""Pure trial generation, scoring, and holdout evaluation for research candidates.

Every input is an explicit caller-supplied value: no private strategy defaults,
no ETF universe, cost, or risk assumptions baked in. This module never imports
`engine.risk`; weighting is recorded purely as a caller label. Nothing here
calls a provider, opens a database, or executes an order. Every universe
instrument must carry an explicit, caller-declared `instrument_types` entry
equal to `"ETF"`; no other instrument type is accepted, and no type is ever
inferred from a symbol. TrialSpec and HoldoutReceipt documents use exact-key
serialization so a decoder fails closed on any missing or unexpected field.
Their canonical hashes are derived, never caller-supplied, and detect
tampering (integrity); a hash match is not proof the document was produced by
a genuine evaluation run (authenticity is outside this pure contract).
HoldoutReceipt additionally carries fixed `research_only=True` and
`history_complete_verified=False` markers that a decoder cannot flip.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from itertools import product
from typing import Final, Literal
from typing import cast as _cast

from aegis_alpha.data.serialization import content_sha256
from aegis_alpha.engine.errors import ContractDefinitionError
from aegis_alpha.engine.numbers import require_finite, require_positive

_SHA256_HEX: Final = re.compile(r"^[0-9a-f]{64}$")
_WEIGHTING_LABELS: Final = frozenset({"equal", "inverse_volatility"})
_SPLITS: Final = ("train", "validation", "test")
_STATUSES: Final = ("complete", "failed")
_DIRECTIONS: Final = ("maximize", "minimize")
_METRIC_PAIR_LENGTH: Final = 2
_TYPE_PAIR_LENGTH: Final = 2
_ETF_INSTRUMENT_TYPE: Final = "ETF"

Split = Literal["train", "validation", "test"]


class EvaluationFailure(ValueError):  # noqa: N818 -- exact name mandated by the audited plan
    """A trusted evaluator's declared failure to produce a trial outcome.

    This is the only exception score_trials/evaluate_holdout convert into a
    failed TrialOutcome. Any other exception, including an unrelated
    ValueError, propagates uncaught.
    """


def _date(value: object, name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ContractDefinitionError(f"{name} must be a date, not datetime")
    return value


def _nonempty_str(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractDefinitionError(f"{name} must be a nonempty string")
    return value


def _sha256_hex(value: object, name: str) -> str:
    if not isinstance(value, str) or not _SHA256_HEX.fullmatch(value):
        raise ContractDefinitionError(f"{name} must be lowercase SHA256 hex")
    return value


def _require_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractDefinitionError(f"{name} must be an integer")
    return value


def _require_positive_int(value: object, name: str) -> int:
    converted = _require_int(value, name)
    if converted <= 0:
        raise ContractDefinitionError(f"{name} must be a positive integer")
    return converted


def _require_sequence(value: object, name: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ContractDefinitionError(f"{name} must be a non-string sequence")
    return value


def _require_nonempty_sequence(value: object, name: str) -> Sequence[object]:
    sequence = _require_sequence(value, name)
    if not sequence:
        raise ContractDefinitionError(f"{name} must be nonempty")
    return sequence


def _require_strict_split_dates(train_end: date, validation_end: date, test_end: date) -> None:
    if not train_end < validation_end < test_end:
        raise ContractDefinitionError(
            "train_end, validation_end, and test_end must be strictly increasing"
        )


def _tuple_of_instrument_ids(value: object, name: str) -> tuple[str, ...]:
    items = _require_nonempty_sequence(value, name)
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        instrument_id = _nonempty_str(item, f"{name} item")
        if instrument_id in seen:
            raise ContractDefinitionError(f"{name} must not contain duplicate instrument ids")
        seen.add(instrument_id)
        result.append(instrument_id)
    return tuple(result)


def _dedupe_universe_axis(value: object) -> tuple[tuple[str, ...], ...]:
    raw = _require_nonempty_sequence(value, "etf_universes")
    resolved = [_tuple_of_instrument_ids(item, "etf_universes item") for item in raw]
    seen: set[tuple[str, ...]] = set()
    unique: list[tuple[str, ...]] = []
    for universe in resolved:
        if universe in seen:
            continue
        seen.add(universe)
        unique.append(universe)
    return tuple(sorted(unique))


def _dedupe_bool_axis(value: object, name: str) -> tuple[bool, ...]:
    raw = _require_nonempty_sequence(value, name)
    values: set[bool] = set()
    for item in raw:
        if not isinstance(item, bool):
            raise ContractDefinitionError(f"{name} must contain only booleans")
        values.add(item)
    return tuple(sorted(values))


def _dedupe_positive_int_axis(value: object, name: str) -> tuple[int, ...]:
    raw = _require_nonempty_sequence(value, name)
    values: set[int] = set()
    for item in raw:
        values.add(_require_positive_int(item, name))
    return tuple(sorted(values))


def _dedupe_positive_float_axis(value: object, name: str) -> tuple[float, ...]:
    raw = _require_nonempty_sequence(value, name)
    values: set[float] = set()
    for item in raw:
        values.add(require_positive(item, field=name))
    return tuple(sorted(values))


def _dedupe_weighting_axis(value: object) -> tuple[str, ...]:
    raw = _require_nonempty_sequence(value, "weightings")
    values: set[str] = set()
    for item in raw:
        if not isinstance(item, str) or item not in _WEIGHTING_LABELS:
            raise ContractDefinitionError("weightings must contain only known labels")
        values.add(item)
    return tuple(sorted(values))


def _require_type_pairs(value: object, name: str) -> tuple[tuple[str, str], ...]:
    """Validate a sequence of exactly-2 (instrument_id, type) pairs.

    Duplicate instrument ids are rejected here, before any dict-based
    canonicalization could silently let a later pair overwrite an earlier
    one (same or conflicting type value). An oversized or undersized pair is
    rejected by exact length, never truncated.
    """
    items = _require_sequence(value, name)
    seen_ids: set[str] = set()
    result: list[tuple[str, str]] = []
    for item in items:
        if (
            not isinstance(item, Sequence)
            or isinstance(item, (str, bytes, bytearray))
            or len(item) != _TYPE_PAIR_LENGTH
        ):
            raise ContractDefinitionError(f"each {name} entry must be an (id, type) pair")
        instrument_id_raw, type_value = item
        instrument_id = _nonempty_str(instrument_id_raw, f"{name} id")
        if instrument_id in seen_ids:
            raise ContractDefinitionError(f"{name} must not contain duplicate instrument ids")
        seen_ids.add(instrument_id)
        if not isinstance(type_value, str) or type_value != _ETF_INSTRUMENT_TYPE:
            raise ContractDefinitionError(
                f"{name}[{instrument_id!r}] must be exactly {_ETF_INSTRUMENT_TYPE!r}"
            )
        result.append((instrument_id, type_value))
    return tuple(result)


def _require_instrument_type_map(
    value: object, universe_ids: frozenset[str]
) -> tuple[tuple[str, str], ...]:
    name = "instrument_types"
    pairs = (
        _require_type_pairs(list(value.items()), name)
        if isinstance(value, Mapping)
        else _require_type_pairs(value, name)
    )
    if {instrument_id for instrument_id, _ in pairs} != universe_ids:
        raise ContractDefinitionError(
            f"{name} must declare exactly the instrument ids used by etf_universes"
        )
    return tuple(sorted(pairs))


def _require_metrics_axis(value: object) -> tuple[tuple[str, float], ...]:
    items = _require_sequence(value, "metrics")
    seen: set[str] = set()
    result: list[tuple[str, float]] = []
    for item in items:
        if (
            not isinstance(item, Sequence)
            or isinstance(item, (str, bytes, bytearray))
            or len(item) != _METRIC_PAIR_LENGTH
        ):
            raise ContractDefinitionError("each metric must be a (name, value) pair")
        name, raw_value = item
        metric_name = _nonempty_str(name, "metric name")
        if metric_name in seen:
            raise ContractDefinitionError("metric names must be unique")
        seen.add(metric_name)
        result.append((metric_name, require_finite(raw_value, field=f"metric {metric_name}")))
    return tuple(result)


def _parse_date_field(value: object, name: str) -> date:
    """Parse a strict YYYY-MM-DD string, matching backtest_cli's `_day` convention."""
    if not isinstance(value, str):
        raise ContractDefinitionError(f"{name} must be an ISO date string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ContractDefinitionError(f"{name} must use YYYY-MM-DD") from error
    if parsed.isoformat() != value:
        raise ContractDefinitionError(f"{name} must use YYYY-MM-DD")
    return parsed


@dataclass(frozen=True, slots=True)
class TrialParameters:
    """One fully resolved point drawn from a ParameterGrid.

    weighting is recorded purely as a caller label; no allocation logic is
    invoked here or anywhere in this module. instrument_types is required and
    must declare exactly the ids in etf_universe, each mapped to "ETF"; it
    cannot be omitted from direct construction or from_document parsing, and
    it participates in the canonical hash via TrialSpec.
    """

    etf_universe: tuple[str, ...]
    instrument_types: tuple[tuple[str, str], ...]
    momentum_horizon: int
    absolute_filter: bool
    trend_filter: bool
    weighting: Literal["equal", "inverse_volatility"]
    volatility_cap: float

    def __post_init__(self) -> None:
        universe = _tuple_of_instrument_ids(self.etf_universe, "etf_universe")
        object.__setattr__(self, "etf_universe", universe)
        object.__setattr__(
            self,
            "instrument_types",
            _require_instrument_type_map(self.instrument_types, frozenset(universe)),
        )
        object.__setattr__(
            self,
            "momentum_horizon",
            _require_positive_int(self.momentum_horizon, "momentum_horizon"),
        )
        if not isinstance(self.absolute_filter, bool):
            raise ContractDefinitionError("absolute_filter must be a boolean")
        if not isinstance(self.trend_filter, bool):
            raise ContractDefinitionError("trend_filter must be a boolean")
        if self.weighting not in _WEIGHTING_LABELS:
            raise ContractDefinitionError("weighting must be a known label")
        object.__setattr__(
            self, "volatility_cap", require_positive(self.volatility_cap, field="volatility_cap")
        )

    def to_document(self) -> dict[str, object]:
        return {
            "etf_universe": list(self.etf_universe),
            "instrument_types": [list(pair) for pair in self.instrument_types],
            "momentum_horizon": self.momentum_horizon,
            "absolute_filter": self.absolute_filter,
            "trend_filter": self.trend_filter,
            "weighting": self.weighting,
            "volatility_cap": self.volatility_cap,
        }


_PARAMETER_FIELDS: Final = frozenset(
    {
        "etf_universe",
        "instrument_types",
        "momentum_horizon",
        "absolute_filter",
        "trend_filter",
        "weighting",
        "volatility_cap",
    }
)


def _parameters_from_document(document: object) -> TrialParameters:
    if not isinstance(document, Mapping) or set(document.keys()) != _PARAMETER_FIELDS:
        raise ContractDefinitionError("parameter_grid document has missing or unknown fields")
    universe = document["etf_universe"]
    if not isinstance(universe, Sequence) or isinstance(universe, (str, bytes, bytearray)):
        raise ContractDefinitionError("parameter_grid etf_universe must be a sequence")
    types_raw = document["instrument_types"]
    if not isinstance(types_raw, Sequence) or isinstance(types_raw, (str, bytes, bytearray)):
        raise ContractDefinitionError("parameter_grid instrument_types must be a sequence")
    instrument_types = tuple(
        tuple(pair)
        if isinstance(pair, Sequence) and not isinstance(pair, (str, bytes, bytearray))
        else pair
        for pair in types_raw
    )
    return TrialParameters(
        etf_universe=tuple(universe),
        instrument_types=instrument_types,
        momentum_horizon=document["momentum_horizon"],
        absolute_filter=document["absolute_filter"],
        trend_filter=document["trend_filter"],
        weighting=document["weighting"],
        volatility_cap=document["volatility_cap"],
    )


@dataclass(frozen=True, slots=True)
class ParameterGrid:
    """Caller-supplied axes to Cartesian-expand into TrialSpec candidates.

    Every axis is explicit; there are no private defaults for any field.
    instrument_types must declare exactly the instrument ids referenced across
    etf_universes, each mapped to the literal string "ETF"; any other declared
    type (for example "STOCK") is rejected, as is a missing or extra id.
    """

    etf_universes: tuple[tuple[str, ...], ...]
    instrument_types: tuple[tuple[str, str], ...]
    momentum_horizons: tuple[int, ...]
    absolute_filters: tuple[bool, ...]
    trend_filters: tuple[bool, ...]
    weightings: tuple[str, ...]
    volatility_caps: tuple[float, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "etf_universes", _dedupe_universe_axis(self.etf_universes))
        universe_ids = frozenset(
            instrument_id for universe in self.etf_universes for instrument_id in universe
        )
        object.__setattr__(
            self,
            "instrument_types",
            _require_instrument_type_map(self.instrument_types, universe_ids),
        )
        object.__setattr__(
            self,
            "momentum_horizons",
            _dedupe_positive_int_axis(self.momentum_horizons, "momentum_horizons"),
        )
        object.__setattr__(
            self,
            "absolute_filters",
            _dedupe_bool_axis(self.absolute_filters, "absolute_filters"),
        )
        object.__setattr__(
            self, "trend_filters", _dedupe_bool_axis(self.trend_filters, "trend_filters")
        )
        object.__setattr__(self, "weightings", _dedupe_weighting_axis(self.weightings))
        object.__setattr__(
            self,
            "volatility_caps",
            _dedupe_positive_float_axis(self.volatility_caps, "volatility_caps"),
        )

    @property
    def size(self) -> int:
        return (
            len(self.etf_universes)
            * len(self.momentum_horizons)
            * len(self.absolute_filters)
            * len(self.trend_filters)
            * len(self.weightings)
            * len(self.volatility_caps)
        )


@dataclass(frozen=True, slots=True)
class TrialSpec:
    """An immutable, exactly-hashable single research trial configuration."""

    parent_hash: str
    seed: int
    parameter_grid: TrialParameters
    train_end: date
    validation_end: date
    test_end: date
    cost_ref: str
    universe_hash: str
    canonical_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "parent_hash", _sha256_hex(self.parent_hash, "parent_hash"))
        object.__setattr__(self, "seed", _require_int(self.seed, "seed"))
        if not isinstance(self.parameter_grid, TrialParameters):
            raise ContractDefinitionError("parameter_grid must be a validated TrialParameters")
        train_end = _date(self.train_end, "train_end")
        validation_end = _date(self.validation_end, "validation_end")
        test_end = _date(self.test_end, "test_end")
        _require_strict_split_dates(train_end, validation_end, test_end)
        object.__setattr__(self, "train_end", train_end)
        object.__setattr__(self, "validation_end", validation_end)
        object.__setattr__(self, "test_end", test_end)
        object.__setattr__(self, "cost_ref", _nonempty_str(self.cost_ref, "cost_ref"))
        object.__setattr__(self, "universe_hash", _sha256_hex(self.universe_hash, "universe_hash"))
        object.__setattr__(self, "canonical_sha256", content_sha256(self._base_document()))

    def _base_document(self) -> dict[str, object]:
        return {
            "parent_hash": self.parent_hash,
            "seed": self.seed,
            "parameter_grid": self.parameter_grid.to_document(),
            "train_end": self.train_end.isoformat(),
            "validation_end": self.validation_end.isoformat(),
            "test_end": self.test_end.isoformat(),
            "cost_ref": self.cost_ref,
            "universe_hash": self.universe_hash,
        }

    def to_document(self) -> dict[str, object]:
        document = self._base_document()
        document["canonical_sha256"] = self.canonical_sha256
        return document

    @classmethod
    def from_document(cls, document: object) -> TrialSpec:
        if not isinstance(document, Mapping) or set(document.keys()) != _TRIAL_SPEC_FIELDS:
            raise ContractDefinitionError("trial spec document has missing or unknown fields")
        spec = cls(
            parent_hash=document["parent_hash"],
            seed=document["seed"],
            parameter_grid=_parameters_from_document(document["parameter_grid"]),
            train_end=_parse_date_field(document["train_end"], "train_end"),
            validation_end=_parse_date_field(document["validation_end"], "validation_end"),
            test_end=_parse_date_field(document["test_end"], "test_end"),
            cost_ref=document["cost_ref"],
            universe_hash=document["universe_hash"],
        )
        expected = document["canonical_sha256"]
        if not isinstance(expected, str) or expected != spec.canonical_sha256:
            raise ContractDefinitionError("trial spec canonical_sha256 does not match derived hash")
        return spec


_TRIAL_SPEC_FIELDS: Final = frozenset(
    {
        "parent_hash",
        "seed",
        "parameter_grid",
        "train_end",
        "validation_end",
        "test_end",
        "cost_ref",
        "universe_hash",
        "canonical_sha256",
    }
)


def generate_trials(  # noqa: PLR0913 -- every split/cost/parent axis is an explicit required input
    grid: ParameterGrid,
    *,
    parent_hash: str,
    seed: int,
    train_end: date,
    validation_end: date,
    test_end: date,
    cost_ref: str,
    max_trials: int,
) -> tuple[TrialSpec, ...]:
    """Cartesian-expand grid into deterministic, deduplicated TrialSpecs.

    The grid size is checked against max_trials before any Cartesian
    materialization; an oversized or empty grid never reaches itertools.product.
    """
    if not isinstance(grid, ParameterGrid):
        raise ContractDefinitionError("grid must be a validated ParameterGrid")
    parent = _sha256_hex(parent_hash, "parent_hash")
    seed_value = _require_int(seed, "seed")
    train = _date(train_end, "train_end")
    validation = _date(validation_end, "validation_end")
    test = _date(test_end, "test_end")
    _require_strict_split_dates(train, validation, test)
    cost = _nonempty_str(cost_ref, "cost_ref")
    max_candidates = _require_positive_int(max_trials, "max_trials")
    total = grid.size
    if total == 0:
        raise ContractDefinitionError("grid must produce at least one candidate")
    if total > max_candidates:
        raise ContractDefinitionError(
            f"grid would produce {total} candidates, exceeding max_trials={max_candidates}"
        )
    instrument_type_lookup = dict(grid.instrument_types)
    candidates: dict[str, TrialSpec] = {}
    for universe, horizon, absolute_filter, trend_filter, weighting, cap in product(
        grid.etf_universes,
        grid.momentum_horizons,
        grid.absolute_filters,
        grid.trend_filters,
        grid.weightings,
        grid.volatility_caps,
    ):
        trial_instrument_types = tuple(
            sorted(
                (instrument_id, instrument_type_lookup[instrument_id]) for instrument_id in universe
            )
        )
        parameters = TrialParameters(
            etf_universe=universe,
            instrument_types=trial_instrument_types,
            momentum_horizon=horizon,
            absolute_filter=absolute_filter,
            trend_filter=trend_filter,
            weighting=_cast('Literal["equal", "inverse_volatility"]', weighting),
            volatility_cap=cap,
        )
        spec = TrialSpec(
            parent_hash=parent,
            seed=seed_value,
            parameter_grid=parameters,
            train_end=train,
            validation_end=validation,
            test_end=test,
            cost_ref=cost,
            universe_hash=content_sha256(list(universe)),
        )
        candidates[spec.canonical_sha256] = spec
    return tuple(sorted(candidates.values(), key=lambda item: item.canonical_sha256))


def _validate_complete_outcome(
    outcome: TrialOutcome, metrics: tuple[tuple[str, float], ...]
) -> tuple[float, float]:
    if not metrics:
        raise ContractDefinitionError("a complete outcome requires at least one metric")
    if outcome.error is not None:
        raise ContractDefinitionError("a complete outcome must not carry an error")
    if outcome.turnover is None or outcome.costs is None:
        raise ContractDefinitionError("a complete outcome requires turnover and costs")
    turnover = require_finite(outcome.turnover, field="turnover")
    costs = require_finite(outcome.costs, field="costs")
    if turnover < 0 or costs < 0:
        raise ContractDefinitionError("turnover and costs must be nonnegative")
    return turnover, costs


def _validate_failed_outcome(outcome: TrialOutcome, metrics: tuple[tuple[str, float], ...]) -> None:
    if metrics:
        raise ContractDefinitionError("a failed outcome must not carry metrics")
    if outcome.turnover is not None or outcome.costs is not None:
        raise ContractDefinitionError("a failed outcome must not carry turnover or costs")
    if not isinstance(outcome.error, str) or not outcome.error.strip():
        raise ContractDefinitionError("a failed outcome requires a nonempty error")


@dataclass(frozen=True, slots=True)
class TrialOutcome:
    """One evaluator result for exactly one TrialSpec on exactly one split."""

    trial_sha256: str
    split: Split
    status: Literal["complete", "failed"]
    metrics: tuple[tuple[str, float], ...]
    turnover: float | None
    costs: float | None
    error: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "trial_sha256", _sha256_hex(self.trial_sha256, "trial_sha256"))
        if self.split not in _SPLITS:
            raise ContractDefinitionError("split must be train, validation, or test")
        if self.status not in _STATUSES:
            raise ContractDefinitionError("status must be complete or failed")
        metrics = _require_metrics_axis(self.metrics)
        if self.status == "complete":
            turnover, costs = _validate_complete_outcome(self, metrics)
            object.__setattr__(self, "turnover", turnover)
            object.__setattr__(self, "costs", costs)
        else:
            _validate_failed_outcome(self, metrics)
        object.__setattr__(self, "metrics", metrics)

    def to_document(self) -> dict[str, object]:
        return {
            "trial_sha256": self.trial_sha256,
            "split": self.split,
            "status": self.status,
            "metrics": [list(pair) for pair in self.metrics],
            "turnover": self.turnover,
            "costs": self.costs,
            "error": self.error,
        }

    @classmethod
    def from_document(cls, document: object) -> TrialOutcome:
        if not isinstance(document, Mapping) or set(document.keys()) != _OUTCOME_FIELDS:
            raise ContractDefinitionError("trial outcome document has missing or unknown fields")
        metrics_raw = document["metrics"]
        if not isinstance(metrics_raw, Sequence) or isinstance(
            metrics_raw, (str, bytes, bytearray)
        ):
            raise ContractDefinitionError("trial outcome metrics must be a sequence of pairs")
        metrics = tuple((pair[0], pair[1]) for pair in metrics_raw)
        return cls(
            trial_sha256=document["trial_sha256"],
            split=document["split"],
            status=document["status"],
            metrics=metrics,
            turnover=document["turnover"],
            costs=document["costs"],
            error=document["error"],
        )


_OUTCOME_FIELDS: Final = frozenset(
    {"trial_sha256", "split", "status", "metrics", "turnover", "costs", "error"}
)


TrialEvaluator = Callable[[TrialSpec, Split], TrialOutcome]


def _invoke_evaluator(evaluate: TrialEvaluator, spec: TrialSpec, split: Split) -> TrialOutcome:
    try:
        outcome = evaluate(spec, split)
    except EvaluationFailure as error:
        outcome = TrialOutcome(
            trial_sha256=spec.canonical_sha256,
            split=split,
            status="failed",
            metrics=(),
            turnover=None,
            costs=None,
            error=str(error) or "evaluation failed",
        )
    if not isinstance(outcome, TrialOutcome):
        raise ContractDefinitionError("evaluator must return a validated TrialOutcome")
    if outcome.trial_sha256 != spec.canonical_sha256:
        raise ContractDefinitionError(
            "evaluator outcome trial_sha256 must match the requested trial"
        )
    if outcome.split != split:
        raise ContractDefinitionError("evaluator outcome split must match the requested split")
    return outcome


@dataclass(frozen=True, slots=True)
class SelectionPolicy:
    """Fixed before scoring: the single metric and direction used to rank."""

    metric: str
    direction: Literal["maximize", "minimize"]

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", _nonempty_str(self.metric, "metric"))
        if self.direction not in _DIRECTIONS:
            raise ContractDefinitionError("direction must be maximize or minimize")


def _require_unique_trials(trials: object) -> tuple[TrialSpec, ...]:
    sequence = _require_sequence(trials, "trials")
    specs: list[TrialSpec] = []
    seen: set[str] = set()
    for item in sequence:
        if not isinstance(item, TrialSpec):
            raise ContractDefinitionError("trials must contain validated TrialSpec instances")
        if item.canonical_sha256 in seen:
            raise ContractDefinitionError("trials must not contain duplicate trial ids")
        seen.add(item.canonical_sha256)
        specs.append(item)
    return tuple(specs)


def _group_train_validation_outcomes(
    trial_ids: frozenset[str], outcomes: tuple[TrialOutcome, ...]
) -> dict[str, dict[str, TrialOutcome]]:
    """Group outcomes per trial, enforcing exactly train+validation, never test."""
    grouped: dict[str, dict[str, TrialOutcome]] = {}
    for outcome in outcomes:
        if outcome.trial_sha256 not in trial_ids:
            raise ContractDefinitionError("an outcome references a trial absent from trials")
        if outcome.split not in ("train", "validation"):
            raise ContractDefinitionError(
                "TrialScores outcomes must be train or validation only, never test"
            )
        per_trial = grouped.setdefault(outcome.trial_sha256, {})
        if outcome.split in per_trial:
            raise ContractDefinitionError(
                "an outcome duplicates an existing split for the same trial"
            )
        per_trial[outcome.split] = outcome
    if set(grouped) != trial_ids:
        raise ContractDefinitionError("every trial requires train and validation outcomes")
    for trial_sha256, per_trial in grouped.items():
        if set(per_trial.keys()) != {"train", "validation"}:
            raise ContractDefinitionError(
                f"trial {trial_sha256} must carry exactly one train and one validation outcome"
            )
    return grouped


def _rank_winner(
    policy: SelectionPolicy,
    trials: tuple[TrialSpec, ...],
    outcomes: tuple[TrialOutcome, ...],
) -> str | None:
    """Derive the deterministic validation winner; ties break on trial_sha256.

    A trial needs a complete train AND a complete validation outcome before it
    can be ranked. A validation outcome missing the selection metric is a
    contract failure, not a score of zero.
    """
    trial_ids = frozenset(spec.canonical_sha256 for spec in trials)
    grouped = _group_train_validation_outcomes(trial_ids, outcomes)
    candidates: list[tuple[float, str]] = []
    for spec in trials:
        per_trial = grouped.get(spec.canonical_sha256)
        if per_trial is None:
            continue
        train_outcome = per_trial["train"]
        validation_outcome = per_trial["validation"]
        if train_outcome.status != "complete" or validation_outcome.status != "complete":
            continue
        metric_values = dict(validation_outcome.metrics)
        if policy.metric not in metric_values:
            raise ContractDefinitionError(
                f"validation outcome for trial {spec.canonical_sha256} is missing "
                f"selection metric {policy.metric!r}"
            )
        score = metric_values[policy.metric]
        rank_key = -score if policy.direction == "maximize" else score
        candidates.append((rank_key, spec.canonical_sha256))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][1]


@dataclass(frozen=True, slots=True)
class TrialScores:
    """Immutable record of every trial/outcome pair and the derived winner.

    winner is never a caller-supplied value: it is always recomputed from
    policy, trials, and outcomes in __post_init__, so an inconsistent winner
    cannot be constructed. trials must have unique trial ids; outcomes must
    reference only those trials and carry exactly one train and one
    validation outcome per trial, never a test outcome.
    """

    policy: SelectionPolicy
    trials: tuple[TrialSpec, ...]
    outcomes: tuple[TrialOutcome, ...]
    winner: str | None = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.policy, SelectionPolicy):
            raise ContractDefinitionError("policy must be a validated SelectionPolicy")
        trials = _require_unique_trials(self.trials)
        outcomes = _require_sequence(self.outcomes, "outcomes")
        validated_outcomes: list[TrialOutcome] = []
        for item in outcomes:
            if not isinstance(item, TrialOutcome):
                raise ContractDefinitionError(
                    "outcomes must contain validated TrialOutcome instances"
                )
            validated_outcomes.append(item)
        object.__setattr__(self, "trials", trials)
        object.__setattr__(self, "outcomes", tuple(validated_outcomes))
        winner = _rank_winner(self.policy, trials, tuple(validated_outcomes))
        object.__setattr__(self, "winner", winner)


def score_trials(
    trials: Sequence[TrialSpec],
    evaluate: TrialEvaluator,
    policy: SelectionPolicy,
) -> TrialScores:
    """Evaluate exactly train and validation for every trial, never test."""
    if not isinstance(policy, SelectionPolicy):
        raise ContractDefinitionError("policy must be a validated SelectionPolicy")
    specs = _require_unique_trials(trials)
    outcomes: list[TrialOutcome] = []
    for spec in specs:
        outcomes.append(_invoke_evaluator(evaluate, spec, "train"))
        outcomes.append(_invoke_evaluator(evaluate, spec, "validation"))
    return TrialScores(policy=policy, trials=specs, outcomes=tuple(outcomes))


@dataclass(frozen=True, slots=True)
class HoldoutReceipt:
    """An immutable, exactly-hashable record of one holdout evaluation.

    Contamination is keyed on parent_hash alone: any prior receipt sharing the
    same parent_hash marks this receipt contaminated, including a prior
    receipt whose own outcome failed. This is a conservative flag over the
    *supplied* receipt history: it is hash-verified for internal consistency
    (integrity), not a cryptographic proof that the history is complete or
    that any receipt was produced by a genuine evaluation run (authenticity
    is outside this pure contract). research_only and history_complete_verified
    are fixed disposition markers, never caller-settable and never flippable
    through from_document.
    """

    parent_hash: str
    trial_sha256: str
    train_end: date
    validation_end: date
    test_end: date
    cost_ref: str
    universe_hash: str
    selection_policy: SelectionPolicy
    outcome: TrialOutcome
    contaminated: bool
    prior_receipt_hashes: tuple[str, ...]
    research_only: bool = field(default=True, init=False)
    history_complete_verified: bool = field(default=False, init=False)
    canonical_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "parent_hash", _sha256_hex(self.parent_hash, "parent_hash"))
        object.__setattr__(self, "trial_sha256", _sha256_hex(self.trial_sha256, "trial_sha256"))
        train_end = _date(self.train_end, "train_end")
        validation_end = _date(self.validation_end, "validation_end")
        test_end = _date(self.test_end, "test_end")
        _require_strict_split_dates(train_end, validation_end, test_end)
        object.__setattr__(self, "train_end", train_end)
        object.__setattr__(self, "validation_end", validation_end)
        object.__setattr__(self, "test_end", test_end)
        object.__setattr__(self, "cost_ref", _nonempty_str(self.cost_ref, "cost_ref"))
        object.__setattr__(self, "universe_hash", _sha256_hex(self.universe_hash, "universe_hash"))
        if not isinstance(self.selection_policy, SelectionPolicy):
            raise ContractDefinitionError("selection_policy must be a validated SelectionPolicy")
        if not isinstance(self.outcome, TrialOutcome):
            raise ContractDefinitionError("outcome must be a validated TrialOutcome")
        if self.outcome.trial_sha256 != self.trial_sha256:
            raise ContractDefinitionError(
                "outcome trial_sha256 must match the receipt trial_sha256"
            )
        if self.outcome.split != "test":
            raise ContractDefinitionError("a holdout receipt outcome must be the test split")
        if not isinstance(self.contaminated, bool):
            raise ContractDefinitionError("contaminated must be a boolean")
        prior = _require_sequence(self.prior_receipt_hashes, "prior_receipt_hashes")
        object.__setattr__(
            self,
            "prior_receipt_hashes",
            tuple(_sha256_hex(item, "prior_receipt_hashes item") for item in prior),
        )
        object.__setattr__(self, "research_only", True)
        object.__setattr__(self, "history_complete_verified", False)
        object.__setattr__(self, "canonical_sha256", content_sha256(self._base_document()))

    def _base_document(self) -> dict[str, object]:
        return {
            "parent_hash": self.parent_hash,
            "trial_sha256": self.trial_sha256,
            "train_end": self.train_end.isoformat(),
            "validation_end": self.validation_end.isoformat(),
            "test_end": self.test_end.isoformat(),
            "cost_ref": self.cost_ref,
            "universe_hash": self.universe_hash,
            "selection_policy": {
                "metric": self.selection_policy.metric,
                "direction": self.selection_policy.direction,
            },
            "outcome": self.outcome.to_document(),
            "contaminated": self.contaminated,
            "prior_receipt_hashes": list(self.prior_receipt_hashes),
            "research_only": self.research_only,
            "history_complete_verified": self.history_complete_verified,
        }

    def to_document(self) -> dict[str, object]:
        document = self._base_document()
        document["canonical_sha256"] = self.canonical_sha256
        return document

    @classmethod
    def from_document(cls, document: object) -> HoldoutReceipt:
        if not isinstance(document, Mapping) or set(document.keys()) != _HOLDOUT_RECEIPT_FIELDS:
            raise ContractDefinitionError("holdout receipt document has missing or unknown fields")
        if (
            document["research_only"] is not True
            or document["history_complete_verified"] is not False
        ):
            raise ContractDefinitionError(
                "holdout receipt research_only/history_complete_verified flags are fixed"
            )
        policy_document = document["selection_policy"]
        if (
            not isinstance(policy_document, Mapping)
            or set(policy_document.keys()) != _SELECTION_POLICY_FIELDS
        ):
            raise ContractDefinitionError(
                "holdout receipt selection_policy has missing or unknown fields"
            )
        prior_raw = document["prior_receipt_hashes"]
        if not isinstance(prior_raw, Sequence) or isinstance(prior_raw, (str, bytes, bytearray)):
            raise ContractDefinitionError("prior_receipt_hashes must be a sequence")
        receipt = cls(
            parent_hash=document["parent_hash"],
            trial_sha256=document["trial_sha256"],
            train_end=_parse_date_field(document["train_end"], "train_end"),
            validation_end=_parse_date_field(document["validation_end"], "validation_end"),
            test_end=_parse_date_field(document["test_end"], "test_end"),
            cost_ref=document["cost_ref"],
            universe_hash=document["universe_hash"],
            selection_policy=SelectionPolicy(
                metric=policy_document["metric"], direction=policy_document["direction"]
            ),
            outcome=TrialOutcome.from_document(document["outcome"]),
            contaminated=document["contaminated"],
            prior_receipt_hashes=tuple(prior_raw),
        )
        expected = document["canonical_sha256"]
        if not isinstance(expected, str) or expected != receipt.canonical_sha256:
            raise ContractDefinitionError(
                "holdout receipt canonical_sha256 does not match derived hash"
            )
        return receipt


_SELECTION_POLICY_FIELDS: Final = frozenset({"metric", "direction"})
_HOLDOUT_RECEIPT_FIELDS: Final = frozenset(
    {
        "parent_hash",
        "trial_sha256",
        "train_end",
        "validation_end",
        "test_end",
        "cost_ref",
        "universe_hash",
        "selection_policy",
        "outcome",
        "contaminated",
        "prior_receipt_hashes",
        "research_only",
        "history_complete_verified",
        "canonical_sha256",
    }
)


def _require_prior_receipts(value: object) -> tuple[HoldoutReceipt, ...]:
    sequence = _require_sequence(value, "previous_receipts")
    receipts: list[HoldoutReceipt] = []
    for item in sequence:
        if not isinstance(item, HoldoutReceipt):
            raise ContractDefinitionError(
                "previous_receipts must contain validated HoldoutReceipt instances"
            )
        receipts.append(item)
    return tuple(receipts)


def evaluate_holdout(
    scores: TrialScores,
    evaluate: TrialEvaluator,
    previous_receipts: Sequence[HoldoutReceipt],
) -> HoldoutReceipt:
    """Evaluate the test split exactly once for the validation winner.

    Raises when scores has no winner. Every supplied previous_receipts entry
    is already a hash-verified HoldoutReceipt by construction; any receipt
    sharing the winner's parent_hash marks the new receipt contaminated
    regardless of that prior receipt's own outcome status.
    """
    if not isinstance(scores, TrialScores):
        raise ContractDefinitionError("scores must be a validated TrialScores")
    if scores.winner is None:
        raise ContractDefinitionError("no validation winner is available for holdout evaluation")
    winner_spec = next(
        (spec for spec in scores.trials if spec.canonical_sha256 == scores.winner), None
    )
    if winner_spec is None:
        raise ContractDefinitionError("the validation winner trial is missing from scores.trials")
    priors = _require_prior_receipts(previous_receipts)
    outcome = _invoke_evaluator(evaluate, winner_spec, "test")
    contaminated = any(prior.parent_hash == winner_spec.parent_hash for prior in priors)
    return HoldoutReceipt(
        parent_hash=winner_spec.parent_hash,
        trial_sha256=winner_spec.canonical_sha256,
        train_end=winner_spec.train_end,
        validation_end=winner_spec.validation_end,
        test_end=winner_spec.test_end,
        cost_ref=winner_spec.cost_ref,
        universe_hash=winner_spec.universe_hash,
        selection_policy=scores.policy,
        outcome=outcome,
        contaminated=contaminated,
        prior_receipt_hashes=tuple(prior.canonical_sha256 for prior in priors),
    )
