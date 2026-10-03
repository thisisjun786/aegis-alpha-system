"""Build one research installation per process, then hand each test a private copy.

The research fixtures initialize a workspace, import a stored request and register
observation panels (and sleeves) before any test body runs. That work is identical for
every test that reads it, so it runs once under the same pinned clock and compute
limits, and each test receives an ordinary ``copytree`` of the whole fixture root at its
own ``tmp_path``. A test that writes to its copy changes only that copy.

The cache is keyed by name rather than held in a module-scoped fixture, because the
fixtures that use it are imported by name into other modules, where a second fixture
name would not resolve.
"""

from __future__ import annotations

import json
import shutil
import time
from typing import TYPE_CHECKING, Any, cast

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

type Document = dict[str, Any]

_TEMPLATES: dict[str, tuple[Path, bytes]] = {}


def compute_limits(patch: pytest.MonkeyPatch, lock: Path) -> None:
    """The limits every research module's autouse ``compute_environment`` sets."""
    patch.setenv("AAS_HOST_CPU_LIMIT", "1")
    patch.setenv("AAS_HOST_MEMORY_LIMIT_BYTES", str(1024 * 1024 * 1024))
    patch.setenv("AAS_CPU_LIMIT", "1")
    patch.setenv("AAS_MEMORY_LIMIT_BYTES", str(512 * 1024 * 1024))
    patch.setenv("AAS_COMPUTE_LOCK_FILE", str(lock))


def copy_template(
    name: str,
    factory: pytest.TempPathFactory,
    root: Path,
    clock_ns: int,
    build: Callable[[Path], list[Document]],
) -> list[Document]:
    """Copy the named template into ``root``, building it on first use.

    ``build`` receives the template root, writes the installation at ``root / "home"``
    and returns the JSON documents the fixture hands back. Each call returns freshly
    decoded documents, so a test that edits one never edits another test's.
    """
    if name not in _TEMPLATES:
        source = factory.mktemp(name)
        lock = factory.mktemp(name + "-lock") / "compute.lock"
        # The build may run during any test's setup; pin what a fresh fixture pinned
        # rather than inherit that test's clock or environment.
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(time, "time_ns", lambda: clock_ns)
            compute_limits(patch, lock)
            documents = build(source)
        # Every workspace handle is closed before any ordinary copy is taken.
        assert not any(path.name.endswith(("-wal", "-shm", ".wal")) for path in source.rglob("*"))
        _TEMPLATES[name] = (source, json.dumps(documents).encode())
    source, raw = _TEMPLATES[name]
    # copytree creates new regular files with copy2 modes, never links to the template.
    _ = shutil.copytree(source, root, dirs_exist_ok=True)
    return cast("list[Document]", json.loads(raw))
