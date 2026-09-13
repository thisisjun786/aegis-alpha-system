from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from aegis_alpha.compute_resources import (
    CPU_ENV,
    HOST_CPU_ENV,
    HOST_MEMORY_ENV,
    LOCK_ENV,
    MEMORY_ENV,
    ComputeBudget,
    ComputeResourceError,
    compute_lease,
    compute_lock_path,
    resolve_compute_budget,
)

_COMPUTE_ENV = frozenset({CPU_ENV, HOST_CPU_ENV, HOST_MEMORY_ENV, MEMORY_ENV, LOCK_ENV})


def compute_status() -> dict[str, object]:
    if not _COMPUTE_ENV.intersection(os.environ):
        return {
            "configured": False,
            "mode": "serial_default",
            "required_environment": [HOST_CPU_ENV, HOST_MEMORY_ENV, LOCK_ENV],
        }
    budget = resolve_compute_budget()
    lock_file = compute_lock_path()
    return {
        "configured": True,
        "mode": "shared_compute_budget",
        "budget": budget.to_dict(),
        "lock_file": str(lock_file),
    }


@contextmanager
def price_compute(*, excluded_locks: tuple[Path, ...] = ()) -> Iterator[ComputeBudget | None]:
    """Keep legacy unconfigured reads serial; configured bulk reads share one compute lease."""
    if not _COMPUTE_ENV.intersection(os.environ):
        yield None
        return
    # Resolve again after queueing so current memory headroom governs this computation.
    lock_file = compute_lock_path()
    # Compare names even before lock creation, and identities for existing aliases.
    # This only rejects: compute_lease still owns all descriptor/path/ownership admission.
    if any(
        lock_file == target
        or (lock_file.exists() and target.exists() and lock_file.samefile(target))
        for target in excluded_locks
    ):
        raise ComputeResourceError("compute lock aliases a workspace storage lock")
    resolve_compute_budget()
    with compute_lease(lock_file):
        yield resolve_compute_budget()
