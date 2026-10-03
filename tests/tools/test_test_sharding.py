"""Deterministic file shards: complete, disjoint, balanced and stable across runners."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.sharding import (
    WEIGHTS,
    estimates,
    load_weights,
    main,
    merge,
    parse_shard,
    partition,
    select,
)

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


@pytest.mark.parametrize("count", range(1, 7))
def test_selected_shards_cover_every_file_exactly_once(count: int) -> None:
    # Uneven sizes, partial weights and more shards than some directories have files.
    counts = {f"tests/d{index % 3}/test_{index:02d}.py": index % 7 + 1 for index in range(17)}
    weights = {name: (index * 1300) % 9000 for index, name in enumerate(counts) if index % 2}
    nodeids = [f"{name}::test_{case}" for name, size in counts.items() for case in range(size)]
    shards = [select(nodeids, weights, index, count) for index in range(1, count + 1)]
    assert frozenset().union(*shards) == frozenset(counts)
    for left in range(count):
        for right in range(left + 1, count):
            assert not shards[left] & shards[right], (left + 1, right + 1)


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


def test_checked_in_weight_table_loads() -> None:
    # Keys for removed or renamed files are ignored: only collected files are weighed.
    assert load_weights(WEIGHTS)


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


@pytest.mark.parametrize(
    ("arguments", "status"), [(("--test-shard", "2/2"), 0), (("-k", "nothing_matches_this"), 5)]
)
def test_empty_shard_passes_but_an_empty_selection_does_not(
    tmp_path: Path, arguments: tuple[str, ...], status: int
) -> None:
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
            "tests/engine/test_bundle.py",
            *arguments,
        ],
        cwd=_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == status, result.stdout + result.stderr


@pytest.mark.parametrize("via", ["argv", "PYTEST_ADDOPTS"])
def test_durations_are_recorded_in_the_weight_format(tmp_path: Path, via: str) -> None:
    # CI passes the option through PYTEST_ADDOPTS so the lane script stays unchanged.
    out = tmp_path / "durations.json"
    environment = {key: value for key, value in os.environ.items() if key != "PYTEST_ADDOPTS"}
    option = ["--test-durations-out", str(out)]
    if via == "PYTEST_ADDOPTS":
        environment["PYTEST_ADDOPTS"] = f"--test-durations-out={out}"
        option = []
    result = subprocess.run(  # noqa: S603 -- fixed pytest argv against this checkout
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            f"--basetemp={tmp_path / 'inner'}",
            *option,
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


def test_shard_durations_merge_into_the_weight_table(tmp_path: Path) -> None:
    first, second, out = (
        tmp_path / "durations-1.json",
        tmp_path / "durations-2.json",
        tmp_path / "w.json",
    )
    first.write_text(json.dumps({"tests/b/test_b.py": 2.25, "tests/a/test_a.py": 0}))
    second.write_text(json.dumps({"tests/c/test_c.py": 61.04}))
    assert main(["merge", "--out", str(out), str(first), str(second)]) == 0
    assert out.read_text() == (
        '{\n  "tests/a/test_a.py": 0,\n  "tests/b/test_b.py": 2.2,\n'
        '  "tests/c/test_c.py": 61.0\n}\n'
    )
    assert load_weights(out) == {
        "tests/a/test_a.py": 0,
        "tests/b/test_b.py": 2200,
        "tests/c/test_c.py": 61000,
    }


def test_merge_refuses_a_file_measured_by_two_shards(tmp_path: Path) -> None:
    first, second = tmp_path / "durations-1.json", tmp_path / "durations-2.json"
    first.write_text(json.dumps({"tests/a/test_a.py": 1.0}))
    second.write_text(json.dumps({"tests/a/test_a.py": 2.0}))
    with pytest.raises(ValueError, match="more than one shard"):
        merge([first, second])


def test_merge_validates_each_input(tmp_path: Path) -> None:
    bad = tmp_path / "durations-1.json"
    bad.write_text(json.dumps({"src/a.py": 1.0}))
    with pytest.raises(ValueError, match="test file path"):
        merge([bad])


def test_merge_refuses_to_drop_the_weight_of_a_file_that_still_exists(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # A shard that wrote or uploaded nothing must not erase measured weights; a weighed file
    # that was deleted or renamed leaves the table.
    out, shard = tmp_path / "weights.json", tmp_path / "durations-1.json"
    original = '{\n  "tests/engine/test_bundle.py": 3.0,\n  "tests/gone/test_removed.py": 9.0\n}\n'
    out.write_text(original)
    shard.write_text(json.dumps({"tests/a/test_new.py": 1.0}))
    with pytest.raises(SystemExit) as refused:
        main(["merge", "--out", str(out), str(shard)])
    assert refused.value.code
    assert "such as tests/engine/test_bundle.py" in capsys.readouterr().err
    assert out.read_text() == original
    shard.write_text(json.dumps({"tests/engine/test_bundle.py": 4.0, "tests/a/test_new.py": 1.0}))
    assert main(["merge", "--out", str(out), str(shard)]) == 0
    assert load_weights(out) == {"tests/a/test_new.py": 1000, "tests/engine/test_bundle.py": 4000}


def test_merge_drops_a_weighed_file_the_lane_no_longer_selects(tmp_path: Path) -> None:
    # A file whose tests are all database-marked still exists but is never measured by the
    # database-free lane again, so it must not block every later refresh.
    database_only = "tests/metadata/test_migrations.py"
    assert (_ROOT / database_only).exists()
    out, shard = tmp_path / "weights.json", tmp_path / "durations-1.json"
    out.write_text(json.dumps({database_only: 5.0, "tests/engine/test_bundle.py": 3.0}))
    shard.write_text(json.dumps({"tests/engine/test_bundle.py": 4.0}))
    assert main(["merge", "--out", str(out), str(shard)]) == 0
    assert load_weights(out) == {"tests/engine/test_bundle.py": 4000}
