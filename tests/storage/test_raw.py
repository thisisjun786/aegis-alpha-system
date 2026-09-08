from __future__ import annotations

from pathlib import Path

import pytest

from aegis_alpha.storage.raw import put_raw, put_raw_file, verify_raw


def test_no_clobber_raw_hash_and_tamper(tmp_path: Path) -> None:
    raw = b'{"synthetic": true}'
    reference = put_raw(tmp_path, raw)
    assert put_raw(tmp_path, raw) == reference
    relative, digest, size = reference
    verify_raw(tmp_path, relative, digest, size)
    (tmp_path / relative).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="different"):
        put_raw(tmp_path, raw)
    with pytest.raises(ValueError, match="checksum"):
        verify_raw(tmp_path, relative, digest, size)


def test_raw_escapes_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="reference"):
        verify_raw(tmp_path, "../anything", "a" * 64, 1)


def test_streamed_snapshot_replay_and_tamper(tmp_path: Path) -> None:
    source = tmp_path / "snapshot"
    payload = b"synthetic\x00" * 150_000
    source.write_bytes(payload)
    source.chmod(0o600)
    root = tmp_path / "raw"
    root.mkdir(mode=0o700)
    expected = put_raw(root, payload)
    assert put_raw_file(root, source) == expected
    assert source.read_bytes() == payload
    relative, digest, size = expected
    assert (root / relative).stat().st_nlink == 1
    verify_raw(root, relative, digest, size)
    (root / relative).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        put_raw_file(root, source)
    assert not list(root.glob(".*.tmp"))


def test_streamed_snapshot_alias_refused(tmp_path: Path) -> None:
    source = tmp_path / "snapshot"
    source.write_bytes(b"synthetic")
    source.chmod(0o600)
    alias = tmp_path / "alias"
    alias.symlink_to(source)
    with pytest.raises(ValueError, match="not linked"):
        put_raw_file(tmp_path, alias)
