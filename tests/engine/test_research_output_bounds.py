"""Aggregate generation limits must run before Cartesian materialization."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import date
from typing import Never

import pytest

from aegis_alpha.engine import research
from aegis_alpha.engine.errors import ContractDefinitionError
from aegis_alpha.engine.research import ParameterGrid, TrialSpec, generate_trials


def grid_for(universe: tuple[str, ...], horizons: tuple[int, ...]) -> ParameterGrid:
    return ParameterGrid(
        etf_universes=(universe,),
        instrument_types=tuple((item, "ETF") for item in universe),
        momentum_horizons=horizons,
        absolute_filters=(True,),
        trend_filters=(False,),
        weightings=("equal",),
        volatility_caps=(0.15,),
    )


def generate(grid: ParameterGrid, cost_ref: str = "synthetic-cost-v1") -> tuple[TrialSpec, ...]:
    return generate_trials(
        grid,
        parent_hash="a" * 64,
        seed=7,
        train_end=date(2020, 12, 31),
        validation_end=date(2021, 12, 31),
        test_end=date(2022, 12, 31),
        cost_ref=cost_ref,
        max_trials=256,
    )


def forbid_materialization(*_axes: Sequence[tuple[str, ...] | int | str | float]) -> Never:
    pytest.fail("Cartesian materialization reached before aggregate rejection")


@pytest.mark.parametrize(
    ("universe", "cost_ref"),
    [
        (("X" * 300_000,), "synthetic-cost-v1"),
        (("ETF-A",), "C" * 300_000),
        (tuple(f"ETF-{index}" for index in range(400)), "synthetic-cost-v1"),
    ],
    ids=["repeated-long-id", "repeated-long-cost", "aggregate-instruments"],
)
def test_rejects_before_product_when_aggregate_output_is_oversized(
    monkeypatch: pytest.MonkeyPatch, universe: tuple[str, ...], cost_ref: str
) -> None:
    # Given: at most 256 trials, with a small input that amplifies on expansion.
    grid = grid_for(universe, tuple(range(1, 257)))
    monkeypatch.setattr(research, "product", forbid_materialization)

    # When / Then: reject at the shared boundary, without allocating any trials.
    with pytest.raises(ContractDefinitionError):
        generate(grid, cost_ref)


@pytest.mark.parametrize(
    ("instrument_id", "byte_limit"),
    [("A", 512), ("I" + "\x01" * 8192, 65_536), ("\U00010000" * 8192, 65_536)],
    ids=["structural-overhead", "json-control-escapes", "utf8-multibyte"],
)
def test_rejects_before_product_when_encoding_exceeds_byte_limit(
    monkeypatch: pytest.MonkeyPatch, instrument_id: str, byte_limit: int
) -> None:
    # Given: tiny scaled budgets distinguish byte size from string length.
    grid = grid_for((instrument_id,), (1, 2))
    monkeypatch.setattr(research, "_MAX_GENERATED_BYTES", byte_limit)
    monkeypatch.setattr(research, "product", forbid_materialization)

    # When / Then: layout, JSON escaping and UTF-8 expansion are budgeted.
    with pytest.raises(ContractDefinitionError):
        generate(grid)


def test_rejects_before_product_when_horizon_has_many_digits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: structural overhead alone fits, but a numeric scalar does not.
    grid = grid_for(("A",), (10**2000,))
    monkeypatch.setattr(research, "_MAX_GENERATED_BYTES", 2500)
    monkeypatch.setattr(research, "product", forbid_materialization)

    # When / Then: numeric encoding is included, not assumed fixed width.
    with pytest.raises(ContractDefinitionError):
        generate(grid)


def test_accepts_when_deduplicated_instrument_count_equals_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: overlapping universes contribute one plus three occurrences.
    grid = replace(
        grid_for(("A", "B", "C"), (1,)),
        etf_universes=(("A",), ("A", "B", "C"), ("A",)),
        momentum_horizons=(1, 1),
    )
    monkeypatch.setattr(research, "_MAX_GENERATED_INSTRUMENTS", 4)

    # When: expand normalized axes exactly at the aggregate occurrence limit.
    trials = generate(grid)

    # Then: duplicates do not consume the budget; shared IDs count per trial.
    assert {trial.parameter_grid.etf_universe for trial in trials} == {
        ("A",),
        ("A", "B", "C"),
    }


def test_rejects_before_product_when_instrument_count_is_one_over_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: three instruments repeated across two horizons means six entries.
    grid = grid_for(("A", "B", "C"), (1, 2))
    monkeypatch.setattr(research, "_MAX_GENERATED_INSTRUMENTS", 5)
    monkeypatch.setattr(research, "product", forbid_materialization)

    # When / Then: count occurrences rather than only distinct input IDs.
    with pytest.raises(ContractDefinitionError):
        generate(grid)
