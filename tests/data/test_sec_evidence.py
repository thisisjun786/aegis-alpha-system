"""Adversarial SEC filesystem boundaries, with task-owned temporary data."""

from __future__ import annotations

from pathlib import Path

import pytest

from aegis_alpha.data import sec_evidence
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.sec_collector import DestinationError, validate_destination
from aegis_alpha.data.sec_evidence import (
    file_pin,
    publish_bytes,
    read_bytes,
    read_document,
    verify_pin,
)


def test_immutable_publication_and_hash(tmp_path: Path) -> None:
    path = tmp_path / "run/evidence.json"
    assert publish_bytes(path, b'{"a":1}') is True
    assert publish_bytes(path, b'{"a":1}') is False
    pin = file_pin(tmp_path, path)
    verify_pin(tmp_path, pin)
    with pytest.raises(FileExistsError, match="conflicts"):
        publish_bytes(path, b'{"a":2}')
    assert read_document(path) == {"a": 1}


@pytest.mark.parametrize("kind", ["leaf", "ancestor"])
def test_symlink_io_and_destinations_refuse(tmp_path: Path, kind: str) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "evidence").write_bytes(b"original")
    if kind == "leaf":
        selected = tmp_path / "evidence"
        selected.symlink_to(outside / "evidence")
    else:
        alias = tmp_path / "alias"
        alias.symlink_to(outside, target_is_directory=True)
        selected = alias / "evidence"
    with pytest.raises(DestinationError, match="symlink"):
        validate_destination("SEC destination", selected)
    with pytest.raises(ValueError, match=r"file|aliases|directory"):
        publish_bytes(selected, b"changed")
    assert (outside / "evidence").read_bytes() == b"original"


def test_replaced_directory_is_detected_during_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "run/evidence.json"
    publish_bytes(path, b'{"a":1}')
    original = DescriptorTree.read_bytes
    mutated: list[bool] = []

    def replace_parent(self: DescriptorTree, relative: str, **kwargs: int) -> bytes:
        payload = original(self, relative, **kwargs)
        path.parent.rename(tmp_path / "old-run")
        path.parent.mkdir()
        path.write_bytes(payload)
        mutated.append(True)
        return payload

    monkeypatch.setattr(DescriptorTree, "read_bytes", replace_parent)
    with pytest.raises(ValueError, match="directory changed"):
        read_bytes(path)
    assert mutated == [True]


@pytest.mark.parametrize("payload", [b'{"x":1,"x":2}', b'{"x":NaN}', b"[]"])
def test_invalid_marker_json_refuses(tmp_path: Path, payload: bytes) -> None:
    path = tmp_path / "marker.json"
    path.write_bytes(payload)
    with pytest.raises(ValueError, match=r"duplicate|JSON|range"):
        read_document(path)


def test_failed_exclusive_temp_creation_preserves_foreign_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foreign = tmp_path / ".sec-collision.tmp"
    foreign.write_bytes(b"foreign")
    monkeypatch.setattr(sec_evidence.secrets, "token_hex", lambda _size: "collision")
    with pytest.raises(ValueError, match="cannot be opened"):
        publish_bytes(tmp_path / "new.json", b"new")
    assert foreign.read_bytes() == b"foreign"
