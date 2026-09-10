from __future__ import annotations

import os
from pathlib import Path

import pytest

from aegis_alpha.storage.raw import put_raw, put_raw_file, verify_raw

_COMPETING_LINKS = 2


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


@pytest.mark.parametrize("conflicting", [False, True])
def test_concurrent_raw_writer_preserves_winner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, conflicting: bool
) -> None:
    payload = b"synthetic"
    winner = b"different" if conflicting else payload
    competitor = tmp_path / "competitor"
    competitor.write_bytes(winner)
    competitor.chmod(0o600)
    link = os.link
    targets: list[str] = []

    def compete(
        source: str,
        destination: str,
        *,
        src_dir_fd: int,
        dst_dir_fd: int,
        follow_symlinks: bool,
    ) -> None:
        targets.append(destination)
        link(competitor, tmp_path / destination)
        link(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(os, "link", compete)
    if conflicting:
        with pytest.raises(ValueError, match="different bytes"):
            put_raw(tmp_path, payload)
    else:
        relative, _, size = put_raw(tmp_path, payload)
        assert relative == targets[0]
        assert size == len(payload)
    assert len(targets) == 1
    assert (tmp_path / targets[0]).read_bytes() == winner
    assert (tmp_path / targets[0]).stat().st_nlink == _COMPETING_LINKS
    assert not list(tmp_path.rglob(".*.tmp"))


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
