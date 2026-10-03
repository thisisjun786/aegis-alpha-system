"""Each test process runs in its own isolated home, and a run aimed at live state is refused."""

from __future__ import annotations

import os
import pwd
import subprocess
import sys
from pathlib import Path

import pytest

from aegis_alpha.storage.paths import resolve_home
from tests.isolation import OWNER, XDG_HOMES, isolate, live_roots, live_state_refusal

_ROOT = Path(__file__).resolve().parents[2]
_USAGE_ERROR = 4


def test_process_home_and_xdg_roots_live_in_one_isolation_tree() -> None:
    tree = Path(os.environ["HOME"]).parent
    assert tree.name.startswith("aas-pytest-")
    assert Path.home() == tree / "home"
    assert all(Path(os.environ[name]).parent == tree for name in XDG_HOMES)
    assert os.environ[OWNER] == os.fspath(tree)
    # The default store and data root are this process's own unless a caller named them;
    # one inside another test process's tree (an xdist controller's) is never kept.
    for name, default in (("AAS_HOME", "aas-home"), ("AAS_DATA_ROOT", "data")):
        root = Path(os.environ[name])
        assert root == tree / default or not root.parent.name.startswith("aas-pytest-"), name
    assert resolve_home() == Path(os.environ["AAS_HOME"])
    operator = Path(pwd.getpwuid(os.getuid()).pw_dir)
    assert operator not in (tree, *tree.parents)
    assert live_state_refusal(os.environ, live_roots(os.fspath(operator))) is None


def test_isolation_keeps_caller_roots_but_always_moves_home(tmp_path: Path) -> None:
    environ = {"HOME": "/elsewhere", "XDG_DATA_HOME": "/elsewhere/data", "AAS_HOME": "/mounted"}
    isolate(tmp_path, environ)
    assert environ["HOME"] == os.fspath(tmp_path / "home")
    assert environ["XDG_DATA_HOME"] == os.fspath(tmp_path / "xdg-data")
    assert environ["AAS_HOME"] == "/mounted"
    assert environ["AAS_DATA_ROOT"] == os.fspath(tmp_path / "data")
    assert all((tmp_path / name).is_dir() for name in ("home", "aas-home", "data"))
    assert environ[OWNER] == os.fspath(tmp_path)


def test_a_worker_replaces_its_parents_roots_but_keeps_a_callers(tmp_path: Path) -> None:
    parent = tmp_path / "aas-pytest-parent"
    worker = tmp_path / "aas-pytest-worker"
    worker.mkdir()
    environ = {
        OWNER: os.fspath(parent),
        "AAS_HOME": os.fspath(parent / "aas-home"),
        "AAS_DATA_ROOT": "/mounted/data",
    }
    isolate(worker, environ)
    assert environ["AAS_HOME"] == os.fspath(worker / "aas-home")
    assert environ["AAS_DATA_ROOT"] == "/mounted/data"
    assert environ[OWNER] == os.fspath(worker)


@pytest.mark.parametrize(
    ("name", "relative"),
    [
        ("HOME", "."),
        ("AAS_HOME", ".aas"),
        ("AAS_HOME", ".aas/nested"),
        ("AAS_DATA_ROOT", ".local/share/aegis-alpha/data"),
        ("AAS_DATA_CONFIG", ".local/share/aegis-alpha/runtime.json"),
    ],
)
def test_guard_names_the_variable_that_reaches_live_state(
    tmp_path: Path, name: str, relative: str
) -> None:
    operator = tmp_path / "operator"
    environ = {"HOME": os.fspath(tmp_path / "isolated"), name: os.fspath(operator / relative)}
    refusal = live_state_refusal(environ, live_roots(os.fspath(operator)))
    assert refusal is not None
    assert "refuse to run against live state" in refusal
    assert (f"{name}/" if name == "HOME" else f"{name}=") in refusal


def test_guard_follows_symlinks_into_live_state(tmp_path: Path) -> None:
    operator = tmp_path / "operator"
    (operator / ".aas").mkdir(parents=True)
    alias = tmp_path / "alias"
    alias.symlink_to(operator / ".aas")
    environ = {"HOME": os.fspath(tmp_path / "isolated"), "AAS_HOME": os.fspath(alias)}
    assert live_state_refusal(environ, live_roots(os.fspath(operator))) is not None


def test_container_state_mount_is_live() -> None:
    environ = {"HOME": "/nonexistent-home", "AAS_DATA_ROOT": "/state/aas/store"}
    assert live_state_refusal(environ, live_roots()) is not None


def _pytest(operator: Path, *arguments: str, **overrides: str) -> subprocess.CompletedProcess[str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTEST_ADDOPTS", "AAS_HOME", "AAS_DATA_ROOT"}
    }
    environment.update(HOME=os.fspath(operator), **overrides)
    return subprocess.run(  # noqa: S603 -- fixed pytest argv against this checkout
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *arguments],
        cwd=_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )


def _snapshot(root: Path) -> dict[str, tuple[int, int]]:
    return {
        os.fspath(path.relative_to(root)): (path.stat().st_mtime_ns, path.stat().st_size)
        for path in (root, *root.rglob("*"))
    }


@pytest.fixture
def operator(tmp_path: Path) -> Path:
    """A stand-in for the operator's home whose ~/.aas holds live state."""
    home = tmp_path / "operator"
    live = home / ".aas"
    live.mkdir(parents=True)
    (live / "state.sqlite3").write_bytes(b"live")
    return home


@pytest.mark.parametrize(
    ("name", "relative"),
    [("AAS_HOME", ".aas"), ("AAS_DATA_ROOT", ".local/share/aegis-alpha/data")],
)
def test_a_run_pointed_at_live_state_stops_before_any_test(
    operator: Path, tmp_path: Path, name: str, relative: str
) -> None:
    before = _snapshot(operator / ".aas")
    result = _pytest(
        operator,
        f"--basetemp={tmp_path / 'inner'}",
        "tests/tools/test_test_isolation.py::test_process_home_and_xdg_roots_live_in_one_isolation_tree",
        **{name: os.fspath(operator / relative)},
    )
    assert result.returncode == _USAGE_ERROR, result.stdout + result.stderr
    assert f"{name}={operator / relative}" in result.stdout + result.stderr
    assert " passed" not in result.stdout
    assert _snapshot(operator / ".aas") == before


def test_a_run_started_from_the_operator_home_never_reaches_it(
    operator: Path, tmp_path: Path
) -> None:
    before = _snapshot(operator)
    result = _pytest(
        operator,
        "-n",
        "2",
        f"--basetemp={tmp_path / 'inner'}",
        "tests/tools/test_test_isolation.py::test_process_home_and_xdg_roots_live_in_one_isolation_tree",
        "tests/storage/test_workspace.py",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert _snapshot(operator) == before
