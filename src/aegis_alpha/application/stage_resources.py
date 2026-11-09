"""Per-stage resource numbers for the maintain report; reading them never fails a run.

Each stage records its elapsed seconds, the process's peak resident set during the stage
and, where the process's cgroup v2 directory is readable, the cgroup's peak and current
memory, its anonymous and file-backed parts and its swap. A number that cannot be read is
None. Peaks are per stage: the process high-water mark is reset through ``clear_refs`` and
the cgroup peaks through a per-descriptor ``reset`` of ``memory.peak`` and
``memory.swap.peak`` (Linux 6.12 and later); without a reset the peak is None rather than a
peak of some earlier stage.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

from aegis_alpha.compute_resources import _visible_cgroup

_PROC_SELF: Final = Path("/proc/self")
_PEAKS: Final = ("memory.peak", "memory.swap.peak")
_ERRORS: Final = (OSError, ValueError)


def _cgroup() -> Path | None:
    try:
        return _visible_cgroup(0)[0]
    except _ERRORS:
        return None


def _integer(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="ascii").strip())
    except _ERRORS:
        return None


def _keyed(path: Path, keys: tuple[str, ...], *, scale: int = 1) -> dict[str, int | None]:
    values: dict[str, int | None] = dict.fromkeys(keys)
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except _ERRORS:
        return values
    for line in lines:
        name, _, rest = line.partition(":" if ":" in line else " ")
        if name in values:
            try:
                values[name] = int(rest.split()[0]) * scale
            except (*_ERRORS, IndexError):
                values[name] = None
    return values


def _reset_peak(path: Path) -> int | None:
    """A descriptor whose later reads give the peak since now, or None."""
    try:
        descriptor = os.open(path, os.O_RDWR | os.O_CLOEXEC)
    except _ERRORS:
        return None
    try:
        os.write(descriptor, b"reset\n")
    except _ERRORS:
        os.close(descriptor)
        return None
    return descriptor


def _read_peak(descriptor: int | None) -> int | None:
    if descriptor is None:
        return None
    try:
        return int(os.pread(descriptor, 64, 0).decode("ascii").strip())
    except (*_ERRORS, UnicodeError):
        return None
    finally:
        os.close(descriptor)


@dataclass(slots=True)
class StageProbe:
    """One stage's measurement, opened by ``StageMeter.start``; ``finish`` closes it."""

    proc: Path
    cgroup: Path | None
    clock: Callable[[], float]
    started: float
    rss_reset: bool
    peaks: dict[str, int | None] = field(default_factory=dict)

    def finish(self) -> dict[str, object]:
        elapsed = round(self.clock() - self.started, 3)
        status = _keyed(self.proc / "status", ("VmHWM",), scale=1024)
        peaks = {name: _read_peak(self.peaks.get(name)) for name in _PEAKS}
        cgroup = self.cgroup
        stat = (
            _keyed(cgroup / "memory.stat", ("anon", "file"))
            if cgroup is not None
            else {"anon": None, "file": None}
        )
        return {
            "elapsed_seconds": elapsed,
            "process_peak_rss_bytes": status["VmHWM"] if self.rss_reset else None,
            "cgroup_peak_bytes": peaks["memory.peak"],
            "cgroup_current_bytes": None if cgroup is None else _integer(cgroup / "memory.current"),
            "cgroup_anon_bytes": stat["anon"],
            "cgroup_file_bytes": stat["file"],
            "cgroup_swap_bytes": None
            if cgroup is None
            else _integer(cgroup / "memory.swap.current"),
            "cgroup_swap_peak_bytes": peaks["memory.swap.peak"],
        }


@dataclass(slots=True)
class StageMeter:
    """Opens a probe per stage over this process and its cgroup (found on first use)."""

    proc: Path = _PROC_SELF
    cgroup: Path | None = None
    find_cgroup: bool = True
    clock: Callable[[], float] = time.monotonic

    def start(self) -> StageProbe:
        if self.find_cgroup:
            self.cgroup, self.find_cgroup = _cgroup(), False
        try:
            (self.proc / "clear_refs").write_bytes(b"5")
            rss_reset = True
        except _ERRORS:
            rss_reset = False
        cgroup = self.cgroup
        peaks = {} if cgroup is None else {name: _reset_peak(cgroup / name) for name in _PEAKS}
        return StageProbe(self.proc, cgroup, self.clock, self.clock(), rss_reset, peaks)
