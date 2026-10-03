"""xdist runs every file whole on one worker and every declared serial group on one worker."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import pytest

from tests.scheduling import scope_of

_ROOT = Path(__file__).resolve().parents[2]
_GROUPED = ("tests/data/test_qveris_budget.py", "tests/data/test_qveris_billing.py")
_WHOLE = ("tests/engine/test_bundle.py", "tests/engine/test_requirements.py")


@pytest.mark.parametrize(
    ("nodeid", "scope"),
    [
        ("tests/a/test_x.py::test_one", "tests/a/test_x.py"),
        ("tests/a/test_x.py::Case::test_one[p]", "tests/a/test_x.py"),
        ("tests/a/test_x.py::test_one@lease", "lease"),
        ("tests/a/test_x.py::test_one[a@b]@lease", "lease"),
        ("tests/a/test_x.py::test_one[a@b]", "tests/a/test_x.py"),
    ],
)
def test_scope_is_the_group_when_marked_and_the_file_otherwise(nodeid: str, scope: str) -> None:
    assert scope_of(nodeid) == scope


def test_workers_take_whole_files_and_whole_groups(tmp_path: Path) -> None:
    environment = {key: value for key, value in os.environ.items() if key != "PYTEST_ADDOPTS"}
    result = subprocess.run(  # noqa: S603 -- fixed pytest argv against this checkout
        [
            sys.executable,
            "-m",
            "pytest",
            "-v",
            "-p",
            "no:cacheprovider",
            "-n",
            "4",
            "--dist",
            "loadgroup",
            f"--basetemp={tmp_path / 'inner'}",
            *_GROUPED,
            *_WHOLE,
        ],
        cwd=_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    workers: defaultdict[str, set[str]] = defaultdict(set)
    passed = re.compile(r"^\[(gw\d+)\] \[[ \d]+%\] PASSED (\S+)", re.MULTILINE)
    for worker, nodeid in passed.findall(result.stdout):
        workers[nodeid.split("::", 1)[0]].add(worker)
    assert set(workers) == {*_GROUPED, *_WHOLE}
    assert all(len(found) == 1 for found in workers.values()), workers
    assert workers[_GROUPED[0]] == workers[_GROUPED[1]]
