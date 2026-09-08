from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from fractions import Fraction
from pathlib import Path
from threading import Event

import pytest

from aegis_alpha import compute_resources
from aegis_alpha.application.compute_cli import compute_status, price_compute
from aegis_alpha.compute_resources import ComputeResourceError, VisibleLimits, compute_lease

_ENV = (
    "AAS_HOST_CPU_LIMIT",
    "AAS_HOST_MEMORY_LIMIT_BYTES",
    "AAS_CPU_LIMIT",
    "AAS_MEMORY_LIMIT_BYTES",
    "AAS_COMPUTE_LOCK_FILE",
)
_MIB = 1024 * 1024


def _clear(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENV:
        monkeypatch.delenv(name, raising=False)


def test_unconfigured_serial_and_partial_configuration_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear(monkeypatch)
    assert compute_status()["mode"] == "serial_default"
    with price_compute() as budget:
        assert budget is None
    monkeypatch.setenv("AAS_CPU_LIMIT", "2")
    with pytest.raises(ComputeResourceError, match="AAS_HOST_CPU_LIMIT"):
        compute_status()
    with pytest.raises(ComputeResourceError, match=r"AAS_.*requires"), price_compute():
        pytest.fail("partial resource configuration cannot enter computation")


def test_price_budget_rechecks_headroom_after_waiting_for_shared_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("AAS_HOST_CPU_LIMIT", "20")
    monkeypatch.setenv("AAS_HOST_MEMORY_LIMIT_BYTES", str(512 * _MIB))
    monkeypatch.setenv("AAS_COMPUTE_LOCK_FILE", str(tmp_path / "compute.lock"))
    probed = Event()
    available = [256 * _MIB]

    def inspect(_pid: int = 0) -> VisibleLimits:
        probed.set()
        return VisibleLimits(Fraction(4), available[0])

    monkeypatch.setattr(compute_resources, "inspect_visible_limits", inspect)

    def read_budget() -> int | None:
        with price_compute() as budget:
            assert budget is not None
            return budget.memory_headroom_bytes

    with ThreadPoolExecutor(max_workers=1) as pool:
        with compute_lease(tmp_path / "compute.lock"):
            task = pool.submit(read_budget)
            assert probed.wait(timeout=5)
            assert not task.done()
            available[0] = 64 * _MIB
        assert task.result(timeout=5) == 64 * _MIB
