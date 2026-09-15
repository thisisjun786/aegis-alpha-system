"""Numeric tolerances shared by target export and replay.

Preparation must not call accounting, so the value both sides compare against
lives here rather than in either of them.
"""

from __future__ import annotations

import sys


def long_only_sum_tolerance(count: int) -> float:
    """Admit binary64 normalization rounding in a long-only target sum.

    Each normalized weight is rounded on its own, so the exact sum of the stored
    doubles can miss one by about the weight count in units of epsilon. Scaling
    with the count keeps the accepted overshoot far inside the relative amount the
    self-financing check in replay absorbs. A constant equal to that amount does
    not: at a large account the two boundaries meet, and an envelope that passed
    export then failed replay with a self-financing error.
    """
    return count * sys.float_info.epsilon
