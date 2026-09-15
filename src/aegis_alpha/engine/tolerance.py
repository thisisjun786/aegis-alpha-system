"""Numeric tolerances shared by target export and replay.

Preparation must not call accounting, so the value both sides compare against
lives here rather than in either of them.
"""

from __future__ import annotations

import sys

# Replay absorbs a relative self-financing error of 1e-12. Stay an order of
# magnitude inside it so the rounding that follows an accepted overshoot still
# fits, and so an unbounded entry count cannot widen the accepted band past what
# replay will take.
MAXIMUM_LONG_ONLY_SUM_TOLERANCE = 1e-13


def long_only_sum_tolerance(count: int) -> float:
    """Admit binary64 normalization rounding in a long-only target sum.

    Each normalized weight is rounded on its own, so the exact sum of the stored
    doubles can miss one by about the weight count in units of epsilon. Scaling
    with the count keeps the accepted overshoot inside the relative amount the
    self-financing check in replay absorbs. A constant equal to that amount does
    not: at a large account the two boundaries meet, and an envelope that passed
    export then failed replay with a self-financing error.

    The count is caller-supplied and unbounded, and zero weights inflate it without
    contributing any rounding, so the scaled value is capped. Past the cap a target
    is refused rather than exported into a replay that would reject it.
    """
    return min(count * sys.float_info.epsilon, MAXIMUM_LONG_ONLY_SUM_TOLERANCE)
