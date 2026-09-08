"""Scoped approval bypass for fully validated marker-only finalization."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from aegis_alpha.data.fmp_collector import FmpCollector


@contextmanager
def marker_finalization_context(
    collector: FmpCollector,
    *,
    marker_recovery: bool,
) -> Iterator[None]:
    if not marker_recovery:
        yield
        return
    if collector._lock is not None:  # noqa: SLF001
        collector._lock.require_ownership()  # noqa: SLF001
    approval_check = collector._approval_check  # noqa: SLF001
    collector._approval_check = None  # noqa: SLF001
    try:
        yield
    finally:
        collector._approval_check = approval_check  # noqa: SLF001
