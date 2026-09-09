# ruff: noqa: PLR2004 -- explicit expected protocol values.
from __future__ import annotations

import json
from pathlib import Path

import pytest

from aegis_alpha.console.registry import ConflictError, Registry
from aegis_alpha.storage.locks import file_lock


def body(path: Path) -> dict[str, object]:
    return {"name": "연구 원본", "path": str(path), "kind": "directory", "note": "확인 대기"}


def test_registry_persists_and_rejects_lost_update(tmp_path: Path) -> None:
    resource = tmp_path / "source"
    resource.mkdir()
    registry = Registry(tmp_path / "registry")
    created = registry.create(body(resource))
    updated = registry.update(
        str(created["id"]), {"name": "검토 자료", "note": "<script>literal</script>", "revision": 1}
    )
    assert updated["revision"] == 2
    assert Registry(registry.root).resources()[0]["note"] == "<script>literal</script>"
    with pytest.raises(ConflictError):
        registry.update(str(created["id"]), {"name": "old", "note": "", "revision": 1})
    with pytest.raises(ConflictError):
        registry.create(body(resource))
    assert (registry.root / "resources.json").stat().st_mode & 0o777 == 0o600


def test_registry_rejects_alias_checkout_and_unknown_fields(tmp_path: Path) -> None:
    registry = Registry(tmp_path / "registry")
    source = tmp_path / "source"
    source.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(source, target_is_directory=True)
    with pytest.raises(ValueError, match=r".+"):
        registry.create(body(alias))
    with pytest.raises(ValueError, match=r".+"):
        registry.create({**body(source), "extra": "bad"})
    (source / ".git").mkdir()
    with pytest.raises(ValueError, match="Git"):
        registry.create(body(source))
    assert registry.resources() == []


def test_registry_busy_and_corrupt_are_not_overwritten(tmp_path: Path) -> None:
    registry = Registry(tmp_path / "registry")
    with file_lock(registry.root / ".registry.lock"), pytest.raises(RuntimeError):
        registry.resources()
    path = registry.root / "resources.json"
    path.write_text(json.dumps({"version": 2, "resources": []}))
    path.chmod(0o600)
    with pytest.raises(ValueError, match=r".+"):
        registry.resources()
    assert json.loads(path.read_text())["version"] == 2


def test_removed_pointer_does_not_erase_note(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    registry = Registry(tmp_path / "registry")
    registry.create(body(source))
    source.rmdir()
    assert registry.resources()[0]["status"] == "unavailable"
    assert registry.resources()[0]["note"] == "확인 대기"
