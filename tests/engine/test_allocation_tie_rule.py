"""The research tie rule is the engine ordering that already exists.

Snowball's public documentation and frontend carry no server tie rule, so
momentum-tie-canonical-id-asc-v1 is our own deterministic research policy rather
than a parity claim. engine.allocation._top already implements it. These are
characterization tests over that behaviour so a later edit cannot quietly make the
boundary depend on input order.
"""

from __future__ import annotations

import itertools

from aegis_alpha.engine.allocation import _top


def test_an_exact_score_tie_breaks_on_ascending_identifier() -> None:
    scores = {"ZZZ": 1.0, "AAA": 1.0, "MMM": 1.0}
    assert _top(("ZZZ", "AAA", "MMM"), scores, 2) == ("AAA", "MMM")


def test_the_boundary_between_selected_and_dropped_is_the_identifier() -> None:
    # AAA and BBB tie on the score that decides the last seat; BBB loses on identity.
    scores = {"CCC": 2.0, "AAA": 1.0, "BBB": 1.0}
    assert _top(("BBB", "AAA", "CCC"), scores, 2) == ("CCC", "AAA")


def test_no_permutation_of_the_input_changes_the_selection() -> None:
    scores = {"AAA": 1.0, "BBB": 1.0, "CCC": 1.0, "DDD": 0.5}
    selections = {_top(order, scores, 2) for order in itertools.permutations(scores)}
    assert selections == {("AAA", "BBB")}


def test_a_higher_score_always_outranks_a_lower_identifier() -> None:
    scores = {"ZZZ": 2.0, "AAA": 1.0}
    assert _top(("AAA", "ZZZ"), scores, 1) == ("ZZZ",)


def test_ties_are_exact_and_a_tiny_difference_still_decides() -> None:
    # No epsilon and no display rounding: a real difference wins on score alone.
    scores = {"ZZZ": 1.0, "AAA": 1.0 - 1e-12}
    assert _top(("AAA", "ZZZ"), scores, 1) == ("ZZZ",)


def test_requesting_more_than_the_universe_returns_the_whole_ordering() -> None:
    scores = {"BBB": 1.0, "AAA": 1.0}
    assert _top(("BBB", "AAA"), scores, 5) == ("AAA", "BBB")
