"""Per-stage resource numbers of the maintain report, from synthetic proc and cgroup files."""

from __future__ import annotations

from pathlib import Path

from aegis_alpha.application.stage_resources import StageMeter


def _clock(*values: float):  # noqa: ANN202 -- a scripted monotonic clock
    times = iter(values)
    return lambda: next(times)


def test_a_stage_reads_its_process_peak_and_cgroup_memory(tmp_path: Path) -> None:
    proc, cgroup = tmp_path / "proc", tmp_path / "cgroup"
    proc.mkdir()
    cgroup.mkdir()
    (proc / "status").write_text("Name:\tpython\nVmHWM:\t    2048 kB\nVmRSS:\t    1024 kB\n")
    (cgroup / "memory.current").write_text("5000\n")
    (cgroup / "memory.swap.current").write_text("7\n")
    (cgroup / "memory.stat").write_text("anon 3000\nfile 1500\nanon_thp 0\n")
    meter = StageMeter(proc=proc, cgroup=cgroup, find_cgroup=False, clock=_clock(10.0, 12.5))

    numbers = meter.start().finish()

    assert numbers == {
        "elapsed_seconds": 2.5,
        "process_peak_rss_bytes": 2048 * 1024,
        "cgroup_peak_bytes": None,  # no per-descriptor peak reset in a plain directory
        "cgroup_current_bytes": 5000,
        "cgroup_anon_bytes": 3000,
        "cgroup_file_bytes": 1500,
        "cgroup_swap_bytes": 7,
        "cgroup_swap_peak_bytes": None,
    }


def test_numbers_that_cannot_be_read_are_null(tmp_path: Path) -> None:
    meter = StageMeter(
        proc=tmp_path / "absent", cgroup=tmp_path / "gone", find_cgroup=False,
        clock=_clock(1.0, 1.25),
    )  # fmt: skip

    numbers = meter.start().finish()

    assert numbers.pop("elapsed_seconds") == 0.25  # noqa: PLR2004 -- the scripted clock
    assert set(numbers.values()) == {None}
