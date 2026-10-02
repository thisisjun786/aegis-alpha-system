"""Deterministic file shards: complete, disjoint, balanced and stable across runners."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.sharding import WEIGHTS, estimates, load_weights, parse_shard, partition, select

_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(("value", "expected"), [("1/1", (1, 1)), ("2/4", (2, 4))])
def test_shard_selector_is_one_based(value: str, expected: tuple[int, int]) -> None:
    assert parse_shard(value) == expected


@pytest.mark.parametrize("value", ["", "0/4", "5/4", "1/0", "2", "1/4/2", " 1/4", "01/4"])
def test_malformed_shard_selector_is_refused(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        parse_shard(value)


def test_partition_is_complete_disjoint_and_independent_of_input_order() -> None:
    counts = {
        f"tests/x/test_{name}.py": size for name, size in zip("abcdefgh", range(1, 9), strict=True)
    }
    weights = {"tests/x/test_a.py": 9000, "tests/x/test_b.py": 0}
    shards = partition(counts, weights, 3)
    files = [name for shard in shards for name in shard]
    assert sorted(files) == sorted(counts)
    assert len(files) == len(set(files))
    assert partition(dict(reversed(counts.items())), weights, 3) == shards


def test_measured_weight_wins_and_unknown_files_use_the_measured_rate() -> None:
    counts = {"tests/a/test_slow.py": 2, "tests/a/test_new.py": 4, "tests/a/test_fast.py": 8}
    weights = {"tests/a/test_slow.py": 8000, "tests/a/test_fast.py": 2000}
    assert estimates(counts, weights) == {
        "tests/a/test_slow.py": 8000,
        "tests/a/test_new.py": 4 * (10000 // 10),
        "tests/a/test_fast.py": 2000,
    }
    # One heavy file is balanced against many light ones rather than by test count.
    assert partition(counts, weights, 2) == [
        ["tests/a/test_slow.py"],
        ["tests/a/test_fast.py", "tests/a/test_new.py"],
    ]


def test_more_shards_than_files_leaves_empty_shards() -> None:
    shards = partition({"tests/test_only.py": 3}, {}, 3)
    assert shards == [["tests/test_only.py"], [], []]
    assert select(["tests/test_only.py::test_x"], {}, 2, 3) == frozenset()


@pytest.mark.parametrize(
    "table",
    [[], {"tests/test_a.py": -1}, {"tests/test_a.py": True}, {"src/a.py": 1}, {"tests/a": 1}],
)
def test_weight_table_shape_is_validated(tmp_path: Path, table: object) -> None:
    path = tmp_path / "weights.json"
    path.write_text(json.dumps(table))
    with pytest.raises((TypeError, ValueError)):
        load_weights(path)


def test_checked_in_weights_name_existing_test_files() -> None:
    weights = load_weights(WEIGHTS)
    assert weights
    assert all((_ROOT / name).is_file() for name in weights)


def _collect(*arguments: str) -> list[str]:
    environment = {key: value for key, value in os.environ.items() if key != "PYTEST_ADDOPTS"}
    result = subprocess.run(  # noqa: S603 -- fixed pytest argv against this checkout
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
            "-m",
            "not database",
            "tests/engine",
            "tests/identity",
            "tests/metadata",
            *arguments,
        ],
        cwd=_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return [line for line in result.stdout.splitlines() if "::" in line]


def test_collected_shards_reassemble_the_unsharded_selection() -> None:
    everything = _collect()
    shards = [_collect("--test-shard", f"{index}/3") for index in (1, 2, 3)]
    assert sorted(nodeid for shard in shards for nodeid in shard) == sorted(everything)
    files = [{nodeid.split("::", 1)[0] for nodeid in shard} for shard in shards]
    assert sum(len(shard) for shard in files) == len(set().union(*files))


def test_durations_are_recorded_in_the_weight_format(tmp_path: Path) -> None:
    out = tmp_path / "durations.json"
    environment = {key: value for key, value in os.environ.items() if key != "PYTEST_ADDOPTS"}
    result = subprocess.run(  # noqa: S603 -- fixed pytest argv against this checkout
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            f"--basetemp={tmp_path / 'inner'}",
            "--test-durations-out",
            str(out),
            "tests/engine/test_bundle.py",
        ],
        cwd=_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert list(load_weights(out)) == ["tests/engine/test_bundle.py"]
