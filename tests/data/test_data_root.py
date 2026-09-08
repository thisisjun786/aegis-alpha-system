from __future__ import annotations

import os
from pathlib import Path

import pytest

from aegis_alpha.data.data_root import (
    DATA_ROOT_ENV_VAR,
    DataRoot,
    DataRootError,
    configured_data_root,
    resolve_data_root,
)


def test_unset_root_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(DATA_ROOT_ENV_VAR, raising=False)
    with pytest.raises(DataRootError, match=DATA_ROOT_ENV_VAR):
        configured_data_root()


def test_valid_root_derives_immutable_namespaces(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "data-root").resolve()
    root.mkdir()
    monkeypatch.setenv(DATA_ROOT_ENV_VAR, os.fspath(root))

    resolved = configured_data_root()

    assert isinstance(resolved, DataRoot)
    assert resolved.root == root
    assert resolved.raw == root / "raw"
    assert resolved.normalized == root / "normalized"
    assert resolved.canonical == root / "canonical"
    assert resolved.identity == root / "identity"
    assert resolved.owner_receipts == root / "owner-receipts"
    assert resolved.backups == root / "backups"
    with pytest.raises(AttributeError):
        resolved.root = root  # ty: ignore[invalid-assignment]


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("", id="empty"),
        pytest.param("data-root", id="relative"),
        pytest.param("/tmp/~/data-root", id="tilde"),  # noqa: S108 - rejected spelling
        pytest.param("/tmp/./data-root", id="dot"),  # noqa: S108 - rejected spelling
        pytest.param("/tmp/data-root/../data-root", id="dotdot"),  # noqa: S108 - rejected spelling
    ],
)
def test_lexically_noncanonical_root_is_rejected(value: str) -> None:
    with pytest.raises(DataRootError, match=DATA_ROOT_ENV_VAR):
        resolve_data_root(value)


def test_duplicate_and_trailing_separators_are_rejected(tmp_path: Path) -> None:
    root = (tmp_path / "data-root").resolve()
    root.mkdir()
    duplicate = os.fspath(root).replace("/", "//", 1)
    with pytest.raises(DataRootError, match="duplicate"):
        resolve_data_root(duplicate)
    with pytest.raises(DataRootError, match="trailing"):
        resolve_data_root(os.fspath(root) + "/")


def test_missing_and_non_directory_roots_are_rejected(tmp_path: Path) -> None:
    missing = (tmp_path / "missing").resolve()
    with pytest.raises(DataRootError, match="existing directory"):
        resolve_data_root(missing)

    file_path = (tmp_path / "file").resolve()
    file_path.write_text("not a directory", encoding="utf-8")
    with pytest.raises(DataRootError, match="directory"):
        resolve_data_root(file_path)


def test_symlink_component_is_rejected(tmp_path: Path) -> None:
    real_root = (tmp_path / "real-root").resolve()
    real_root.mkdir()
    alias = (tmp_path / "alias").resolve()
    try:
        alias.symlink_to(real_root, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"symlink test unavailable: {error}")
    with pytest.raises(DataRootError, match="symlink"):
        resolve_data_root(alias)
