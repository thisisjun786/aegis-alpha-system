from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from aegis_alpha.application.container_install import _preflight_owned, _write_owned


def test_owned_wrapper_preserves_shebang_executes_and_updates_without_clobber(
    tmp_path: Path,
) -> None:
    command = tmp_path / "bin/aas"
    initial = b'#!/usr/bin/env python3\n# AAS_MANAGED_CONTAINER_V1 test-id\nprint("first")\n'
    _write_owned(command, initial, "test-id", executable=True)
    result = subprocess.run([str(command)], capture_output=True, text=True, check=False)  # noqa: S603 -- created synthetic executable only
    assert result.returncode == 0
    assert result.stdout.strip() == "first"
    _write_owned(command, initial, "test-id", executable=True)
    updated = initial.replace(b'"first"', b'"second"')
    _write_owned(command, updated, "test-id", executable=True)
    assert command.read_bytes() == updated
    backups = tuple(command.parent.glob("aas.previous-*"))
    assert len(backups) == 1
    assert backups[0].read_bytes() == initial
    assert os.stat(command).st_mode & 0o777 == 0o700  # noqa: PTH116,PLR2004 -- executable permission contract


def test_unowned_binary_and_symlink_are_preserved(tmp_path: Path) -> None:
    target = tmp_path / "aas"
    target.write_bytes(b"unrelated tool\n")
    with pytest.raises(ValueError, match="unowned"):
        _write_owned(target, b"new", "test-id", executable=True)
    assert target.read_bytes() == b"unrelated tool\n"
    alias = tmp_path / "alias"
    alias.symlink_to(target)
    with pytest.raises(ValueError, match="non-aliased"):
        _preflight_owned(alias, "test-id")
    assert target.read_bytes() == b"unrelated tool\n"


def test_symlink_parent_cannot_create_files_outside_installation(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match=r"directory|symlink|relative"):
        _write_owned(alias / "new/aas", b"new", "test-id", executable=True)
    assert not (outside / "new").exists()
