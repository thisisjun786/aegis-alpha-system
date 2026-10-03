"""Deterministic file-granular sharding of the collected test set.

``--test-shard INDEX/COUNT`` keeps the test files of one shard out of COUNT. Every
shard collects the same items under the same marker selection, so every shard
computes the same partition: the union of all shards is the unsharded run and no
file runs twice. A whole file stays in one shard, so module fixtures are built once.

Files are balanced by the measured seconds in ``shard_weights.json``. A file the
table does not list is estimated from its item count at the table's mean
seconds per test, so a new file needs no table edit to be scheduled. Refresh the
table with ``--test-durations-out`` from an unsharded run of the same lane, or join
the per-shard files every CI ``tests`` job publishes::

    python -m tests.sharding merge durations-1.json durations-2.json ...
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

WEIGHTS = Path(__file__).with_name("shard_weights.json")
_ROOT = WEIGHTS.parents[1]
_SHARD = re.compile(r"([1-9][0-9]{0,2})/([1-9][0-9]{0,2})\Z")
_MILLISECONDS = 1000
_DEFAULT_TEST_MILLISECONDS = 1000
_SHARD_SUMMARY = pytest.StashKey[str]()
_EMPTY_SHARD = pytest.StashKey[bool]()


def parse_shard(value: str) -> tuple[int, int]:
    """Parse a one-based ``INDEX/COUNT`` shard selector."""
    match = _SHARD.fullmatch(value)
    if match is None:
        raise argparse.ArgumentTypeError("expected INDEX/COUNT such as 2/4")
    index, count = int(match[1]), int(match[2])
    if index > count:
        raise argparse.ArgumentTypeError("shard index must not exceed the shard count")
    return index, count


def file_of(nodeid: str) -> str:
    return nodeid.split("::", 1)[0]


def read_seconds(path: Path) -> dict[str, float]:
    """A validated table of measured seconds per test file."""
    raw = json.loads(path.read_text())
    if not isinstance(raw, dict):
        raise TypeError("shard weights must be a JSON object")
    table: dict[str, float] = {}
    for name, seconds in raw.items():
        if not isinstance(name, str) or not name.startswith("tests/") or not name.endswith(".py"):
            raise ValueError(f"shard weight key must be a test file path: {name!r}")
        if isinstance(seconds, bool) or not isinstance(seconds, int | float) or seconds < 0:
            raise ValueError(f"shard weight must be non-negative seconds: {name}")
        table[name] = seconds
    return table


def load_weights(path: Path = WEIGHTS) -> dict[str, int]:
    """Measured seconds per test file, as integer milliseconds."""
    return {name: round(seconds * _MILLISECONDS) for name, seconds in read_seconds(path).items()}


def write_seconds(path: Path, seconds: Mapping[str, float]) -> None:
    """Write seconds per test file in the weight table format."""
    table = {name: round(value, 1) for name, value in sorted(seconds.items())}
    path.write_text(json.dumps(table, indent=2, sort_keys=True) + "\n")


def merge(paths: Iterable[Path]) -> dict[str, float]:
    """Join per-shard duration tables; shards are disjoint, so a file appears once."""
    merged: dict[str, float] = {}
    for path in paths:
        for name, seconds in read_seconds(path).items():
            if name in merged:
                raise ValueError(f"test file measured by more than one shard: {name}")
            merged[name] = seconds
    return merged


def estimates(counts: Mapping[str, int], weights: Mapping[str, int]) -> dict[str, int]:
    """Milliseconds per collected file, measured where known and estimated otherwise."""
    known = [name for name in counts if name in weights]
    known_tests = sum(counts[name] for name in known)
    per_test = (
        sum(weights[name] for name in known) // known_tests
        if known_tests
        else _DEFAULT_TEST_MILLISECONDS
    )
    return {name: weights.get(name, counts[name] * per_test) for name in counts}


def partition(counts: Mapping[str, int], weights: Mapping[str, int], count: int) -> list[list[str]]:
    """Longest-first greedy balance of files over ``count`` shards, ties broken by name."""
    if count < 1:
        raise ValueError("shard count must be positive")
    cost = estimates(counts, weights)
    shards: list[list[str]] = [[] for _ in range(count)]
    loads = [0] * count
    for name in sorted(cost, key=lambda name: (-cost[name], name)):
        target = min(range(count), key=lambda shard: (loads[shard], shard))
        shards[target].append(name)
        loads[target] += cost[name]
    return [sorted(files) for files in shards]


def select(
    nodeids: Iterable[str], weights: Mapping[str, int], index: int, count: int
) -> frozenset[str]:
    """The test files that shard ``index`` of ``count`` runs."""
    counts = Counter(file_of(nodeid) for nodeid in nodeids)
    return frozenset(partition(counts, weights, count)[index - 1])


class DurationRecorder:
    """Sum setup, call and teardown seconds per test file for the weight table."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.seconds: defaultdict[str, float] = defaultdict(float)

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        self.seconds[file_of(report.nodeid)] += report.duration

    def pytest_sessionfinish(self) -> None:
        write_seconds(self.path, self.seconds)


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("aas-sharding", "deterministic test sharding")
    group.addoption(
        "--test-shard",
        type=parse_shard,
        default=None,
        metavar="INDEX/COUNT",
        help="run only the test files of one deterministic shard (one-based)",
    )
    group.addoption(
        "--test-durations-out",
        type=Path,
        default=None,
        metavar="PATH",
        help="write measured seconds per test file in the shard weight format",
    )


def pytest_configure(config: pytest.Config) -> None:
    path = config.getoption("test_durations_out")
    if path is not None:
        config.pluginmanager.register(DurationRecorder(path), "aas-duration-recorder")


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    # trylast: shard only what marker and keyword selection have already kept.
    shard = config.getoption("test_shard")
    if shard is None:
        return
    index, count = shard
    files = select((item.nodeid for item in items), load_weights(), index, count)
    kept = [item for item in items if file_of(item.nodeid) in files]
    dropped = [item for item in items if file_of(item.nodeid) not in files]
    if dropped:
        config.hook.pytest_deselected(items=dropped)
    items[:] = kept
    config.stash[_SHARD_SUMMARY] = (
        f"test shard {index}/{count}: {len(files)} files, {len(kept)} tests"
    )
    # More shards than selected files leaves this one nothing by design.
    config.stash[_EMPTY_SHARD] = bool(dropped) and not kept


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    # An empty shard of a non-empty selection is a pass; an empty selection stays exit 5.
    if exitstatus == pytest.ExitCode.NO_TESTS_COLLECTED and session.config.stash.get(
        _EMPTY_SHARD, False
    ):
        session.exitstatus = pytest.ExitCode.OK


def pytest_report_collectionfinish(config: pytest.Config) -> str | None:
    return config.stash.get(_SHARD_SUMMARY, None)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m tests.sharding")
    commands = parser.add_subparsers(dest="command", required=True)
    joined = commands.add_parser("merge", help="join per-shard duration files into the weights")
    joined.add_argument("inputs", nargs="+", type=Path, metavar="DURATIONS")
    joined.add_argument(
        "--out", type=Path, default=WEIGHTS, help="table to write (default: the checked-in weights)"
    )
    arguments = parser.parse_args(argv)
    merged = merge(arguments.inputs)
    if arguments.out.exists():
        # A shard that wrote or uploaded nothing would silently drop measured weights;
        # only a weighed file that no longer exists may leave the table.
        unmeasured = sorted(
            name
            for name in read_seconds(arguments.out)
            if name not in merged and (_ROOT / name).exists()
        )
        if unmeasured:
            parser.error(
                f"the inputs measure none of {len(unmeasured)} weighed test files that still "
                f"exist, such as {unmeasured[0]}; pass every shard's durations"
            )
    write_seconds(arguments.out, merged)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
