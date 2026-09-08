from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from aegis_alpha.data.fmp_owner_authority import load_owner_approval_authority

NOW = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)
COLLECTOR_UID = 2000
AUTHORITY_UID = 1000


def _write_authority(path: Path, **changes: object) -> Path:
    private_key = Ed25519PrivateKey.generate()
    document: dict[str, object] = {
        "schema": "aegis-alpha/fmp-owner-approval-authority",
        "version": 1,
        "authority_id": "synthetic-owner-authority",
        "key_id": "ed25519:synthetic-2026-08",
        "public_key_encoding": "raw-ed25519-hex",
        "public_key": private_key.public_key().public_bytes_raw().hex(),
        "valid_from_utc": "2026-08-18T12:00:00.000000Z",
        "valid_until_utc": "2026-08-20T12:00:00.000000Z",
    }
    document.update(changes)
    path.parent.mkdir(mode=0o700, exist_ok=True)
    path.write_text(json.dumps(document), encoding="utf-8")
    path.chmod(0o600)
    return path


def _load(path: Path, *, collector_uid: int = COLLECTOR_UID) -> None:
    load_owner_approval_authority(
        path,
        NOW,
        collector_uid=collector_uid,
        ownership_reader=lambda _metadata: AUTHORITY_UID,
    )


def test_independently_owned_public_only_authority_loads(tmp_path: Path) -> None:
    path = _write_authority(tmp_path / "authority" / "key.json")
    try:
        _load(path)
    finally:
        path.unlink()
        path.parent.rmdir()


@pytest.mark.parametrize(
    ("collector_uid", "message"),
    [(0, "must not run as root"), (AUTHORITY_UID, "independently owned")],
)
def test_root_or_same_owner_collector_is_rejected(
    tmp_path: Path, collector_uid: int, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _load(_write_authority(tmp_path / "authority.json"), collector_uid=collector_uid)


@pytest.mark.parametrize("mode", [0o620, 0o606])
def test_group_or_world_writable_authority_is_rejected(tmp_path: Path, mode: int) -> None:
    path = _write_authority(tmp_path / "authority.json")
    path.chmod(mode)
    with pytest.raises(ValueError, match="group/world writable"):
        _load(path)


def test_group_writable_authority_directory_is_rejected(tmp_path: Path) -> None:
    directory = tmp_path / "authority"
    path = _write_authority(directory / "key.json")
    directory.chmod(0o720)
    with pytest.raises(ValueError, match="group/world writable"):
        _load(path)


@pytest.mark.parametrize("entry", ["symlink", "fifo"])
def test_symlink_and_nonregular_authority_are_rejected(tmp_path: Path, entry: str) -> None:
    path = tmp_path / "authority.json"
    if entry == "symlink":
        target = _write_authority(tmp_path / "target.json")
        path.symlink_to(target)
    else:
        os.mkfifo(path, mode=0o600)
    with pytest.raises(ValueError, match=r"symlinks|wrong type"):
        _load(path)


def test_private_key_and_unknown_fields_are_rejected(tmp_path: Path) -> None:
    path = _write_authority(tmp_path / "authority.json", private_key="forbidden")
    with pytest.raises(ValueError, match="fields are not exact"):
        _load(path)


def test_duplicate_key_and_oversized_authority_are_rejected(tmp_path: Path) -> None:
    path = _write_authority(tmp_path / "authority.json")
    path.write_bytes(b'{"version":1,"version":1}')
    with pytest.raises(ValueError, match="duplicate"):
        _load(path)
    path.write_bytes(b"{" + b" " * 4096 + b"}")
    with pytest.raises(ValueError, match="too large"):
        _load(path)
