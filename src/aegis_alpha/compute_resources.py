"""Explicit shared compute budgets and leases; imports have no runtime side effects.

Host ceilings are mandatory for automatic allocation: a private cgroup namespace
cannot prove that visible ancestors describe every effective host restriction.
"""

from __future__ import annotations

import fcntl
import math
import os
import re
import stat
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from fractions import Fraction
from pathlib import Path, PurePosixPath
from threading import Event
from typing import BinaryIO

from aegis_alpha.data.descriptor_tree import DescriptorTree

HOST_CPU_ENV = "AAS_HOST_CPU_LIMIT"
HOST_MEMORY_ENV = "AAS_HOST_MEMORY_LIMIT_BYTES"
CPU_ENV = "AAS_CPU_LIMIT"
MEMORY_ENV = "AAS_MEMORY_LIMIT_BYTES"
LOCK_ENV = "AAS_COMPUTE_LOCK_FILE"
HASH_CHUNK_BYTES = 1024 * 1024
_HASH_WORKER_BYTES = 2 * HASH_CHUNK_BYTES
_POLL_SECONDS = 0.05
_PROC_ROOT = Path("/proc")


class ComputeResourceError(ValueError):
    """Capacity or shared lock configuration is missing or unsafe."""


class ComputeCancelledError(RuntimeError):
    """Cooperative compute cancellation; descriptors are drained before return."""


def check_cancelled(cancel_event: Event | None) -> None:
    if cancel_event is not None and not isinstance(cancel_event, Event):
        raise TypeError("cancel_event must be threading.Event")
    if cancel_event is not None and cancel_event.is_set():
        raise ComputeCancelledError("compute cancelled")


@dataclass(frozen=True, slots=True)
class ComputeBudget:
    cpu_limit: Fraction
    memory_limit_bytes: int
    memory_headroom_bytes: int | None = None
    # What the caller already holds live in Python across the work being admitted.
    # It reduces the materialization allowance without moving DuckDB's own share,
    # so a step can never charge against bytes an earlier step is still using.
    reserved_bytes: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.cpu_limit, Fraction) or self.cpu_limit <= 0:
            raise ComputeResourceError("cpu_limit must be a positive Fraction")
        if type(self.memory_limit_bytes) is not int or self.memory_limit_bytes < _HASH_WORKER_BYTES:
            raise ComputeResourceError("memory budget cannot support one bounded hash worker")
        if self.memory_headroom_bytes is not None and (
            type(self.memory_headroom_bytes) is not int
            or self.memory_headroom_bytes < self.memory_limit_bytes
        ):
            raise ComputeResourceError("memory headroom must cover the compute allocation")
        if type(self.reserved_bytes) is not int or self.reserved_bytes < 0:
            raise ComputeResourceError("reserved bytes must be a non-negative integer")
        if self.reserved_bytes >= self.memory_limit_bytes - self.duckdb_memory_limit_bytes:
            raise ComputeResourceError("retained state leaves no materialization allowance")

    @property
    def hash_workers(self) -> int:
        return min(math.ceil(self.cpu_limit), self.memory_limit_bytes // _HASH_WORKER_BYTES)

    @property
    def max_in_flight(self) -> int:
        return 2 * self.hash_workers

    @property
    def duckdb_threads(self) -> int:
        return self.hash_workers

    @property
    def duckdb_memory_limit_bytes(self) -> int:
        # Leave space for Python/Arrow and file buffers outside DuckDB's allocator.
        return self.memory_limit_bytes * 3 // 4

    @property
    def available_bytes(self) -> int:
        """Materialization bytes outside DuckDB that are not already held live."""
        return self.memory_limit_bytes - self.duckdb_memory_limit_bytes - self.reserved_bytes

    def component(self, divisor: int) -> ComputeBudget:
        """Carve a sub-allocation out of what is not already held live.

        The slice is sized from the free materialization allowance rather than from
        total memory, so what the caller retains is accounted once here instead of
        being charged again inside every slice. Subtracting retention from total
        memory first would pass only a quarter of it through to the Python share,
        letting a slice materialize bytes the caller is still holding.

        With nothing retained this reproduces the plain division it replaces. When
        too little is free the slice falls below the hash-worker floor and is
        refused, which is the honest answer to having nothing to work with.
        """
        if type(divisor) is not int or divisor < 1:
            raise ComputeResourceError("component divisor must be a positive integer")
        # available_bytes is a quarter of memory_limit_bytes, so scale back up.
        return replace(
            self,
            memory_limit_bytes=4 * self.available_bytes // divisor,
            reserved_bytes=0,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "cpu_limit": str(self.cpu_limit),
            "cpu_slots": math.ceil(self.cpu_limit),
            "memory_limit_bytes": self.memory_limit_bytes,
            "memory_headroom_bytes": self.memory_headroom_bytes,
            "hash_workers": self.hash_workers,
            "max_in_flight": self.max_in_flight,
            "duckdb_threads": self.duckdb_threads,
            "duckdb_memory_limit_bytes": self.duckdb_memory_limit_bytes,
        }


def _positive_decimal(name: str, value: object) -> Fraction:
    if not isinstance(value, str) or re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value) is None:
        raise ComputeResourceError(f"{name} requires an exact positive decimal")
    result = Fraction(value)
    if result <= 0:
        raise ComputeResourceError(f"{name} must be positive")
    return result


def _integer(name: str, value: str, *, zero: bool = False) -> int:
    if re.fullmatch(r"[0-9]+", value) is None:
        raise ComputeResourceError(f"{name} requires integer bytes/counts")
    result = int(value)
    if result < (0 if zero else 1):
        raise ComputeResourceError(f"{name} has an invalid count")
    return result


def _memory_env(env: Mapping[str, str], name: str) -> int:
    value = env.get(name)
    if not isinstance(value, str):
        raise ComputeResourceError(f"{name} is required as positive integer bytes")
    return _integer(name, value)


def _read(path: Path, *, optional: bool = False) -> str | None:
    # These are kernel-provided proc/cgroup files; /proc/self is intentionally a kernel alias.
    try:
        return path.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        if optional:
            return None
        raise ComputeResourceError("compute capacity file is unavailable") from None
    except (OSError, UnicodeError) as error:
        raise ComputeResourceError("compute capacity file cannot be read") from error


def _visible_cgroup(pid: int) -> tuple[Path, Path]:
    member_path = "self" if pid == 0 else str(pid)
    membership = _read(_PROC_ROOT / member_path / "cgroup") or ""
    groups = [line[3:] for line in membership.splitlines() if line.startswith("0::")]
    if len(groups) != 1:
        raise ComputeResourceError("automatic compute allocation requires readable cgroup v2")
    member = PurePosixPath(groups[0])
    if not member.is_absolute() or ".." in member.parts:
        raise ComputeResourceError("cgroup membership is outside the visible namespace")
    candidates: list[tuple[int, Path, Path]] = []
    for line in (_read(_PROC_ROOT / "self/mountinfo") or "").splitlines():
        before, separator, after = line.partition(" - ")
        if not separator or (after.split() or [""])[0] != "cgroup2":
            continue
        fields = before.split()
        if len(fields) < 6:  # noqa: PLR2004 - mountinfo mandatory field count
            raise ComputeResourceError("invalid cgroup mountinfo")
        root, mount = (_unescape(fields[index]) for index in (3, 4))
        if (
            not Path(mount).is_absolute()
            or ".." in Path(mount).parts
            or not PurePosixPath(root).is_absolute()
            or ".." in PurePosixPath(root).parts
        ):
            raise ComputeResourceError("invalid cgroup mount path")
        try:
            relative = member.relative_to(root)
        except ValueError:
            continue
        candidates.append((len(PurePosixPath(root).parts), Path(mount) / relative, Path(mount)))
    if not candidates:
        raise ComputeResourceError("cgroup ancestry is unavailable; cannot allocate automatically")
    # Prefer the broadest visible hierarchy; a nested bind mount can hide known ancestors.
    _, current, mount = min(candidates, key=lambda item: (item[0], len(item[2].parts)))
    return current, mount


def _unescape(value: str) -> str:
    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), value)


def _cpuset(value: str, affinity: set[int]) -> set[int]:
    admitted: set[int] = set()
    for part in value.split(","):
        if re.fullmatch(r"[0-9]+(?:-[0-9]+)?", part) is None:
            raise ComputeResourceError("invalid effective cpuset")
        ends = part.split("-")
        first, last = int(ends[0]), int(ends[-1])
        if last < first:
            raise ComputeResourceError("invalid effective cpuset range")
        admitted.update(cpu for cpu in affinity if first <= cpu <= last)
    return admitted


def _memory_headroom(current: Path) -> list[int]:
    values: list[int] = []
    for name in ("memory.max", "memory.high"):
        value = _read(current / name, optional=True)
        if value is not None and value != "max":
            limit = _integer(name, value, zero=True)
            used = _integer("memory.current", _read(current / "memory.current") or "", zero=True)
            values.append(max(0, limit - used))
    return values


def _visible_limits(affinity: set[int], pid: int) -> tuple[int, list[Fraction], list[int]]:
    current, mount = _visible_cgroup(pid)
    quotas: list[Fraction] = []
    headrooms: list[int] = []
    while True:
        if not current.is_dir():
            raise ComputeResourceError("visible cgroup directory disappeared")
        if (cpus := _read(current / "cpuset.cpus.effective", optional=True)) is not None:
            affinity = _cpuset(cpus, affinity)
        if (quota := _read(current / "cpu.max", optional=True)) is not None:
            parts = quota.split()
            if len(parts) != 2:  # noqa: PLR2004 - cpu.max is quota and period
                raise ComputeResourceError("invalid cpu.max")
            period = _integer("cpu.max period", parts[1])
            if parts[0] != "max":
                quotas.append(Fraction(_integer("cpu.max quota", parts[0]), period))
        headrooms.extend(_memory_headroom(current))
        if current == mount:
            break
        current = current.parent
    if not affinity:
        raise ComputeResourceError("no CPU remains in affinity/effective cpuset")
    return len(affinity), quotas, headrooms


@dataclass(frozen=True, slots=True)
class VisibleLimits:
    """An observation, never proof that the caller can see hidden host ancestors."""

    cpu_limit: Fraction
    memory_headroom_bytes: int

    def to_host_environment(self) -> dict[str, str]:
        """Installer-only propagation after the installer establishes host visibility."""
        return {HOST_CPU_ENV: str(self.cpu_limit), HOST_MEMORY_ENV: str(self.memory_headroom_bytes)}


def inspect_visible_limits(pid: int = 0) -> VisibleLimits:
    """Inspect a target PID from the observer's proc/cgroup namespace, without writes.

    For Docker host attestation, the installer must execute this on the actual
    host with the probe's host PID. Calling it inside a container is insufficient.
    """
    if type(pid) is not int or pid < 0:
        raise ComputeResourceError("pid must be a nonnegative integer")
    try:
        affinity = set(os.sched_getaffinity(pid))
    except (AttributeError, OSError) as error:
        raise ComputeResourceError("CPU affinity is unavailable") from error
    cpus, quotas, headrooms = _visible_limits(affinity, pid)
    memory = dict(
        re.findall(
            r"^(MemTotal|MemAvailable):\s+([0-9]+) kB$",
            _read(_PROC_ROOT / "meminfo") or "",
            re.MULTILINE,
        )
    )
    if set(memory) != {"MemTotal", "MemAvailable"}:
        raise ComputeResourceError("memory headroom is unavailable")
    total = _integer("MemTotal", memory["MemTotal"]) * 1024
    available = _integer("MemAvailable", memory["MemAvailable"], zero=True) * 1024
    if available > total:
        raise ComputeResourceError("memory availability exceeds total")
    return VisibleLimits(
        min(Fraction(cpus), *quotas) if quotas else Fraction(cpus),
        min(total, available, *headrooms),
    )


def _host_cpu(value: object) -> Fraction:
    if isinstance(value, str) and re.fullmatch(r"[0-9]+/[0-9]+", value):
        numerator, denominator = value.split("/")
        return Fraction(_integer(HOST_CPU_ENV, numerator), _integer(HOST_CPU_ENV, denominator))
    return _positive_decimal(HOST_CPU_ENV, value)


def resolve_compute_budget(environ: Mapping[str, str] | None = None) -> ComputeBudget:
    """Resolve an allocation from explicit host ceilings, visible limits and operator caps."""
    env = os.environ if environ is None else environ
    host_cpu = _host_cpu(env.get(HOST_CPU_ENV))
    host_memory = _memory_env(env, HOST_MEMORY_ENV)
    visible = inspect_visible_limits()
    cpu_limits = [host_cpu, visible.cpu_limit]
    memory_limits = [host_memory, visible.memory_headroom_bytes]
    if CPU_ENV in env:
        cpu_limits.append(_positive_decimal(CPU_ENV, env[CPU_ENV]))
    if MEMORY_ENV in env:
        memory_limits.append(_memory_env(env, MEMORY_ENV))
    headroom = min(memory_limits)
    # This is an allocation ceiling, not a kernel limit on every native allocation.
    return ComputeBudget(min(cpu_limits), headroom * 4 // 5, headroom)


def compute_lock_path(environ: Mapping[str, str] | None = None) -> Path:
    env = os.environ if environ is None else environ
    value = env.get(LOCK_ENV)
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ComputeResourceError(f"{LOCK_ENV} requires an absolute external lock path")
    path = Path(value)
    _validate_lock_path(path)
    return path


def _validate_lock_path(path: Path) -> None:
    if not isinstance(path, Path) or not path.is_absolute() or ".." in path.parts or not path.name:
        raise ComputeResourceError("compute lock must be an exact absolute file path")
    if any((parent / ".git").exists() for parent in path.parents):
        raise ComputeResourceError("compute lock must be outside Git")


def _lock_identity(tree: DescriptorTree, handle: BinaryIO) -> tuple[int, int]:
    information = os.fstat(handle.fileno())
    if (
        information.st_uid != os.geteuid()
        or information.st_nlink != 1
        or stat.S_IMODE(information.st_mode) & 0o022
    ):
        raise ComputeResourceError("compute lock must be singly linked, owned and private-writable")
    with DescriptorTree.open_path(tree.logical_root) as visible:
        if visible.identity != tree.identity:
            raise ComputeResourceError("compute lock directory was replaced")
    return information.st_dev, information.st_ino


@contextmanager
def compute_lease(lock_file: Path, *, cancel_event: Event | None = None) -> Iterator[None]:
    """Queue CPU work behind one persistent descriptor-bound lock; never unlink the lock."""
    _validate_lock_path(lock_file)
    check_cancelled(cancel_event)
    with DescriptorTree.open_path(lock_file.parent) as tree:
        parent = os.fstat(tree.descriptor)
        if parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) & 0o022:
            raise ComputeResourceError("compute lock parent must be owned and private-writable")
        with tree.binary_writer(lock_file.name, append=True, truncate=False) as handle:
            identity = _lock_identity(tree, handle)
            wait_event = cancel_event if cancel_event is not None else Event()
            try:
                while True:
                    check_cancelled(cancel_event)
                    try:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        wait_event.wait(_POLL_SECONDS)
                _check_lock_binding(tree, handle, lock_file.name, identity)
                check_cancelled(cancel_event)
                try:
                    yield
                finally:
                    _check_lock_binding(tree, handle, lock_file.name, identity)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _check_lock_binding(
    tree: DescriptorTree, handle: BinaryIO, name: str, identity: tuple[int, int]
) -> None:
    visible = tree.stat(name)
    if (visible.st_dev, visible.st_ino) != identity or _lock_identity(tree, handle) != identity:
        raise ComputeResourceError("compute lock file was replaced")
