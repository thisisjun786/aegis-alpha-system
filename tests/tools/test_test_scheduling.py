"""xdist runs every file whole on one worker and every declared serial group on one worker."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import pytest

from aegis_alpha.data.qveris_store import QverisStore
from tests.scheduling import scope_of
from tests.serial import QVERIS_LEASE_GROUP

_ROOT = Path(__file__).resolve().parents[2]
# Synthetic files only: a probe over real test files would take their host-wide resources
# (the Qveris account lease) while the outer run's serial group may hold them.
_GROUPED = ("test_grouped_a.py", "test_grouped_b.py")
_WHOLE = ("test_whole_a.py", "test_whole_b.py")
_TESTS_PER_FILE = 8


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
    probe = tmp_path / "probe"
    probe.mkdir()
    (probe / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    cases = "".join(
        f"\n\ndef test_{index}() -> None:\n    pass\n" for index in range(_TESTS_PER_FILE)
    )
    for name in _GROUPED:
        mark = 'import pytest\n\npytestmark = pytest.mark.xdist_group("probe")\n'
        (probe / name).write_text(mark + cases, encoding="utf-8")
    for name in _WHOLE:
        (probe / name).write_text(cases, encoding="utf-8")
    environment = {key: value for key, value in os.environ.items() if key != "PYTEST_ADDOPTS"}
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(None, (os.fspath(_ROOT), environment.get("PYTHONPATH")))
    )
    result = subprocess.run(  # noqa: S603 -- fixed pytest argv against generated files
        [
            sys.executable,
            "-m",
            "pytest",
            "-v",
            "-c",
            "pytest.ini",
            f"--rootdir={probe}",
            "-p",
            "no:cacheprovider",
            "-p",
            "tests.scheduling",
            "-n",
            "4",
            "--dist",
            "loadgroup",
            f"--basetemp={tmp_path / 'inner'}",
            *_GROUPED,
            *_WHOLE,
        ],
        cwd=probe,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    workers: defaultdict[str, set[str]] = defaultdict(set)
    passed = re.compile(r"^\[(gw\d+)\] \[[ \d]+%\] PASSED (\S+)", re.MULTILINE)
    nodeids = passed.findall(result.stdout)
    assert len(nodeids) == _TESTS_PER_FILE * len((*_GROUPED, *_WHOLE)), result.stdout
    for worker, nodeid in nodeids:
        workers[nodeid.split("::", 1)[0]].add(worker)
    assert set(workers) == {*_GROUPED, *_WHOLE}
    assert all(len(found) == 1 for found in workers.values()), workers
    assert workers[_GROUPED[0]] == workers[_GROUPED[1]]


def test_a_test_outside_the_lease_group_cannot_take_the_qveris_lease(tmp_path: Path) -> None:
    # The guard refuses before the store binds, so this probe never holds the lease.
    store = QverisStore(tmp_path / "evidence", "synthetic-unmarked-account")
    with pytest.raises(pytest.fail.Exception, match=QVERIS_LEASE_GROUP), store:
        pass
    assert store.tree is None
