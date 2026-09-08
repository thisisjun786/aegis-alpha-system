from __future__ import annotations

from pathlib import Path

import pytest

from aegis_alpha.storage.raw import put_raw, verify_raw


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
