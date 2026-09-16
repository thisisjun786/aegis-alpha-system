"""Capacity discovery and cross-process lease boundaries on synthetic kernel files."""
# ruff: noqa: PLR2004, SLF001
# Numeric synthetic capacities and injected proc roots make the boundary observable.

from __future__ import annotations

import json
import multiprocessing
import os
import subprocess
import sys
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from threading import Event, Thread
from typing import TYPE_CHECKING

import pytest

from aegis_alpha import compute_resources as resources
from aegis_alpha.compute_resources import (
    ComputeBudget,
    ComputeCancelledError,
    ComputeResourceError,
    compute_lease,
    compute_lock_path,
    resolve_compute_budget,
)

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event as ProcessEvent

GIB = 1024**3
MIB = 1024**2


@pytest.fixture
def capacity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, str], Path]:
    proc = tmp_path / "proc"
    cgroup = tmp_path / "cgroup"
    (proc / "self").mkdir(parents=True)
    (cgroup / "slice/job").mkdir(parents=True)
    (proc / "self/cgroup").write_text("0::/slice/job\n")
    (proc / "self/mountinfo").write_text(f"1 0 0:31 / {cgroup} rw - cgroup2 cgroup rw\n")
    (proc / "meminfo").write_text(
        f"MemTotal: {16 * GIB // 1024} kB\nMemAvailable: {12 * GIB // 1024} kB\n"
    )
    for directory in (cgroup, cgroup / "slice", cgroup / "slice/job"):
        (directory / "cpu.max").write_text("max 100000")
        (directory / "cpuset.cpus.effective").write_text("0-19")
        (directory / "memory.max").write_text("max")
        (directory / "memory.current").write_text("0")
    monkeypatch.setattr(resources, "_PROC_ROOT", proc)
    monkeypatch.setattr(os, "sched_getaffinity", lambda _pid: set(range(20)))
    return {resources.HOST_CPU_ENV: "20", resources.HOST_MEMORY_ENV: str(16 * GIB)}, cgroup


def test_resolve_host_affinity_quotas_and_operator_minimum(
    capacity: tuple[dict[str, str], Path],
) -> None:
    env, cgroup = capacity
    assert resolve_compute_budget(env).cpu_limit == 20
    (cgroup / "slice/cpu.max").write_text("250000 100000")
    (cgroup / "slice/job/cpu.max").write_text("600000 100000")
    budget = resolve_compute_budget(env)
    assert budget.cpu_limit == Fraction(5, 2)
    assert budget.duckdb_threads == 3
    assert json.loads(json.dumps(budget.to_dict()))["cpu_limit"] == "5/2"
    env[resources.CPU_ENV] = "1.75"
    assert resolve_compute_budget(env).cpu_limit == Fraction(7, 4)
    (cgroup / "slice/job/cpuset.cpus.effective").write_text("7")
    assert resolve_compute_budget(env).cpu_limit == 1


def test_fraction_below_one_retains_quota_with_one_lane(
    capacity: tuple[dict[str, str], Path],
) -> None:
    env, cgroup = capacity
    (cgroup / "slice/cpu.max").write_text("25000 100000")
    budget = resolve_compute_budget(env)
    assert budget.cpu_limit == Fraction(1, 4)
    assert budget.hash_workers == 1


def test_memory_uses_parent_headroom_and_operator_ceiling(
    capacity: tuple[dict[str, str], Path],
) -> None:
    env, cgroup = capacity
    (cgroup / "slice/memory.max").write_text(str(4 * GIB))
    (cgroup / "slice/memory.current").write_text(str(3 * GIB))
    budget = resolve_compute_budget(env)
    assert budget.memory_headroom_bytes == GIB
    assert budget.memory_limit_bytes == GIB * 4 // 5
    assert budget.duckdb_memory_limit_bytes < budget.memory_limit_bytes
    env[resources.MEMORY_ENV] = str(128 * MIB)
    assert resolve_compute_budget(env).memory_headroom_bytes == 128 * MIB
    (cgroup / "slice/memory.high").write_text(str(3 * GIB))
    with pytest.raises(ComputeResourceError, match="memory budget"):
        resolve_compute_budget(env)


def test_private_namespace_host_bounds_are_never_inferred(
    capacity: tuple[dict[str, str], Path],
) -> None:
    env, cgroup = capacity
    # The visible root claims unlimited resources, but supplied host ancestors are tighter.
    (resources._PROC_ROOT / "self/cgroup").write_text("0::/\n")
    env[resources.HOST_CPU_ENV] = "1.5"
    env[resources.HOST_MEMORY_ENV] = str(256 * MIB)
    budget = resolve_compute_budget(env)
    assert budget.cpu_limit == Fraction(3, 2)
    assert budget.memory_headroom_bytes == 256 * MIB
    for name in (resources.HOST_CPU_ENV, resources.HOST_MEMORY_ENV):
        without = dict(env)
        without.pop(name)
        with pytest.raises(ComputeResourceError, match=name):
            resolve_compute_budget(without)
    # A mounted ancestor prefix is mapped to the actual visible directory, not /sys/fs/cgroup.
    (resources._PROC_ROOT / "self/cgroup").write_text("0::/delegated/slice/job\n")
    (resources._PROC_ROOT / "self/mountinfo").write_text(
        f"1 0 0:31 /delegated {cgroup} rw - cgroup2 cgroup rw\n"
    )
    assert resolve_compute_budget(env).cpu_limit == Fraction(3, 2)


@pytest.mark.parametrize(
    "value", ["", "0", "-1", "NaN", "Infinity", "true", " 2", "2 ", "2/3", "1e2"]
)
def test_invalid_cpu_override_refuses(capacity: tuple[dict[str, str], Path], value: str) -> None:
    env, _ = capacity
    env[resources.CPU_ENV] = value
    with pytest.raises(ComputeResourceError, match=resources.CPU_ENV):
        resolve_compute_budget(env)


@pytest.mark.parametrize("value", ["", "0", "-1", "1.5", "NaN", "2GiB"])
def test_invalid_memory_override_refuses(capacity: tuple[dict[str, str], Path], value: str) -> None:
    env, _ = capacity
    env[resources.MEMORY_ENV] = value
    with pytest.raises(ComputeResourceError, match=resources.MEMORY_ENV):
        resolve_compute_budget(env)


def test_unknown_or_unreadable_visible_capacity_refuses(
    capacity: tuple[dict[str, str], Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    env, cgroup = capacity
    original = Path.read_text

    def denied(path: Path, *args: object, **kwargs: object) -> str:
        if path == cgroup / "slice/cpu.max":
            raise PermissionError("synthetic inaccessible ancestor")
        return original(path, *args, **kwargs)  # ty: ignore[invalid-argument-type] # injected I/O error

    monkeypatch.setattr(Path, "read_text", denied)
    with pytest.raises(ComputeResourceError, match="cannot be read"):
        resolve_compute_budget(env)
    monkeypatch.setattr(Path, "read_text", original)
    (resources._PROC_ROOT / "self/cgroup").write_text("1:cpu:/legacy\n")
    with pytest.raises(ComputeResourceError, match="cgroup v2"):
        resolve_compute_budget(env)


def test_memory_bounds_worker_and_queue_counts() -> None:
    budget = ComputeBudget(Fraction(20), 6 * MIB)
    assert budget.hash_workers == 3
    assert budget.max_in_flight == 6
    with pytest.raises(ComputeResourceError, match="positive Fraction"):
        ComputeBudget(cpu_limit=True, memory_limit_bytes=GIB)  # ty: ignore[invalid-argument-type] # bool boundary
    with pytest.raises(ComputeResourceError, match="memory budget"):
        ComputeBudget(Fraction(1), memory_limit_bytes=True)


def test_import_is_stdlib_only() -> None:
    code = (
        "import sys; import aegis_alpha.compute_resources; "
        "print([x for x in ('duckdb','pyarrow','sqlalchemy') if x in sys.modules])"
    )
    result = subprocess.run(  # noqa: S603 - fixed local import smoke
        [sys.executable, "-S", "-c", code],
        env={
            **os.environ,
            "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "[]"


def _hold_lease(path: str, ready: ProcessEvent, release: ProcessEvent) -> None:
    with compute_lease(Path(path)):
        ready.set()
        if not release.wait(10):
            raise RuntimeError("test lease holder timed out")


def test_cross_process_contention_cancellation_and_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "compute.lock"
    context = multiprocessing.get_context("spawn")
    ready, release = context.Event(), context.Event()
    child = context.Process(target=_hold_lease, args=(str(path), ready, release))
    child.start()
    try:
        assert ready.wait(5)
        blocked, cancel = Event(), Event()
        original = resources.fcntl.flock

        def observe(fd: int, operation: int) -> None:
            try:
                original(fd, operation)
            except BlockingIOError:
                blocked.set()
                raise

        monkeypatch.setattr(resources.fcntl, "flock", observe)

        def cancel_waiter() -> None:
            if blocked.wait(5):
                cancel.set()

        canceller = Thread(target=cancel_waiter)
        canceller.start()
        with (
            pytest.raises(ComputeCancelledError, match="cancelled"),
            compute_lease(path, cancel_event=cancel),
        ):
            pytest.fail("contending waiter must not enter")
        canceller.join(5)
        assert not canceller.is_alive()
    finally:
        release.set()
        child.join(5)
        if child.is_alive():
            child.kill()
            child.join(5)
    assert child.exitcode == 0
    inode = path.stat().st_ino
    with pytest.raises(RuntimeError, match="synthetic"), compute_lease(path):
        raise RuntimeError("synthetic work failure")
    with compute_lease(path):
        assert path.stat().st_ino == inode


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "parent", "replacement", "git"])
def test_unsafe_lease_paths_refuse(tmp_path: Path, kind: str) -> None:
    path = tmp_path / "compute.lock"
    other = tmp_path / "other"
    other.write_bytes(b"preserve")
    if kind == "symlink":
        path.symlink_to(other)
    elif kind == "hardlink":
        path.hardlink_to(other)
    elif kind == "parent":
        target = tmp_path / "real"
        target.mkdir()
        alias = tmp_path / "alias"
        alias.symlink_to(target, target_is_directory=True)
        path = alias / "compute.lock"
    elif kind == "git":
        (tmp_path / ".git").mkdir()
    with pytest.raises(ValueError, match=r"lock|file|aliases|Git"), compute_lease(path):  # noqa: PT012 - test acquisition and release refusals
        assert kind == "replacement"
        other.replace(path)
    if kind != "replacement":
        assert other.read_bytes() == b"preserve"


def test_lock_environment_is_explicit_and_side_effect_free(tmp_path: Path) -> None:
    path = tmp_path / "compute.lock"
    assert compute_lock_path({resources.LOCK_ENV: str(path)}) == path
    assert not path.exists()
    with pytest.raises(ComputeResourceError, match=resources.LOCK_ENV):
        compute_lock_path({})
    cancelled = Event()
    cancelled.set()
    with (
        pytest.raises(ComputeCancelledError, match="cancelled"),
        compute_lease(path, cancel_event=cancelled),
    ):
        pytest.fail("cancelled acquisition must not enter")
    assert not path.exists()


def test_host_pid_inspection_and_exact_ceiling_propagation(
    capacity: tuple[dict[str, str], Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    env, cgroup = capacity
    target = resources._PROC_ROOT / "123"
    target.mkdir()
    (target / "cgroup").write_text("0::/slice/job\n")
    (cgroup / "slice/cpu.max").write_text("100000 300000")
    inspected: list[int] = []

    def affinity(pid: int) -> set[int]:
        inspected.append(pid)
        return set(range(8))

    monkeypatch.setattr(os, "sched_getaffinity", affinity)
    visible = resources.inspect_visible_limits(123)
    assert inspected == [123]
    assert visible.cpu_limit == Fraction(1, 3)
    propagated = visible.to_host_environment()
    assert propagated[resources.HOST_CPU_ENV] == "1/3"
    budget = resolve_compute_budget({**env, **propagated})
    assert budget.cpu_limit == Fraction(1, 3)


def test_broad_mount_keeps_ancestor_limits_visible(capacity: tuple[dict[str, str], Path]) -> None:
    env, cgroup = capacity
    nested = cgroup / "bind"
    nested.mkdir()
    (nested / "cpu.max").write_text("max 100000")
    (cgroup / "slice/cpu.max").write_text("150000 100000")
    mountinfo = resources._PROC_ROOT / "self/mountinfo"
    mountinfo.write_text(
        mountinfo.read_text() + f"2 0 0:31 /slice/job {nested} rw - cgroup2 cgroup rw\n"
    )
    assert resolve_compute_budget(env).cpu_limit == Fraction(3, 2)


def test_a_reserve_reduces_only_the_materialization_allowance() -> None:
    """Reserving held bytes must not move the engine share."""
    budget = ComputeBudget(Fraction(1), 512 * 1024 * 1024)
    held = replace(budget, reserved_bytes=12_096)
    assert held.available_bytes == budget.available_bytes - 12_096
    assert held.duckdb_memory_limit_bytes == budget.duckdb_memory_limit_bytes
    assert held.duckdb_threads == budget.duckdb_threads


def test_a_component_split_divides_what_is_not_held() -> None:
    """A split must not charge retained bytes again against each smaller slice."""
    plain = ComputeBudget(Fraction(1), 64 * 1024 * 1024)
    # With nothing retained this is the plain division it replaces.
    assert plain.component(2).memory_limit_bytes == plain.memory_limit_bytes // 2
    assert plain.component(2).available_bytes == plain.available_bytes // 2
    # A large allocation holding 80 MiB still has 48 MiB free, so a halved slice
    # must remain usable. Charging the reserve again would leave it owing more
    # than its whole non-DuckDB share.
    held = replace(ComputeBudget(Fraction(1), 512 * 1024 * 1024), reserved_bytes=80 * 1024 * 1024)
    assert held.available_bytes == 48 * 1024 * 1024
    assert held.component(2).reserved_bytes == 0
    # A slice can never exceed its share of what the caller actually has free.
    assert 0 < held.component(2).available_bytes <= held.available_bytes // 2
    # Nearly everything retained leaves nothing to divide, and the floor refuses it.
    starved = replace(held, reserved_bytes=held.available_bytes + held.reserved_bytes - 1)
    with pytest.raises(ComputeResourceError, match="bounded hash worker"):
        starved.component(2)


def test_a_reserve_that_leaves_no_allowance_is_refused() -> None:
    """Admitting nothing is a configuration error, not a silent success."""
    budget = ComputeBudget(Fraction(1), 512 * 1024 * 1024)
    with pytest.raises(ComputeResourceError, match="no materialization allowance"):
        replace(budget, reserved_bytes=budget.available_bytes)
