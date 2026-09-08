from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from aegis_alpha.data.contracts import SourceSnapshot, ValidationStatus
from aegis_alpha.data.raw_store import ContentAddressedRawStore


def snapshot_for(
    payload: bytes, *, snapshot_id: str = "snapshot-001", retrieved_at: datetime
) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=snapshot_id,
        schema_version=1,
        provider="synthetic",
        dataset="daily_prices",
        source_uri="fixture://daily-prices",
        request_fingerprint="sha256:" + "1" * 64,
        parameters={"symbol": "AAPL"},
        requested_at_utc=retrieved_at,
        retrieved_at_utc=retrieved_at,
        content_type="application/json",
        encoding="utf-8",
        compression=None,
        raw_byte_length=len(payload),
        content_sha256=hashlib.sha256(payload).hexdigest(),
        parser_name="synthetic-json",
        parser_version="1.0.0",
        validation_status=ValidationStatus.PASS,
    )


def test_capture_rejects_bytes_that_do_not_match_provenance(tmp_path: Path) -> None:
    declared = snapshot_for(b'{"close": 100}', retrieved_at=datetime(2026, 7, 29, tzinfo=UTC))
    store = ContentAddressedRawStore(tmp_path)

    with pytest.raises(ValueError, match="content SHA-256 mismatch"):
        store.capture(declared, b'{"close": 101}')

    assert list(tmp_path.rglob("*")) == []


def test_capture_reuses_identical_blob_and_preserves_changed_bytes(tmp_path: Path) -> None:
    first_payload = b'{"close": 100}'
    changed_payload = b'{"close": 101}'
    store = ContentAddressedRawStore(tmp_path)

    first = store.capture(
        snapshot_for(first_payload, retrieved_at=datetime(2026, 7, 29, 0, 0, tzinfo=UTC)),
        first_payload,
    )
    repeated = store.capture(
        snapshot_for(
            first_payload,
            snapshot_id="snapshot-002",
            retrieved_at=datetime(2026, 7, 29, 0, 1, tzinfo=UTC),
        ),
        first_payload,
    )
    changed = store.capture(
        snapshot_for(
            changed_payload,
            snapshot_id="snapshot-003",
            retrieved_at=datetime(2026, 7, 29, 0, 2, tzinfo=UTC),
        ),
        changed_payload,
    )

    assert first == repeated
    assert changed != first
    assert first.read_bytes() == first_payload
    assert changed.read_bytes() == changed_payload
    assert sorted(path for path in tmp_path.rglob("*.raw")) == sorted([first, changed])


def test_snapshot_id_cannot_be_reused_for_different_provenance(tmp_path: Path) -> None:
    payload = b'{"close": 100}'
    store = ContentAddressedRawStore(tmp_path)
    store.capture(
        snapshot_for(payload, retrieved_at=datetime(2026, 7, 29, 0, 0, tzinfo=UTC)),
        payload,
    )

    with pytest.raises(ValueError, match="snapshot ID already has different provenance"):
        store.capture(
            snapshot_for(payload, retrieved_at=datetime(2026, 7, 29, 0, 1, tzinfo=UTC)),
            payload,
        )


def test_interrupted_write_does_not_publish_final_or_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = b'{"close": 100}'
    store = ContentAddressedRawStore(tmp_path)

    def fail_fsync(_file_descriptor: int) -> None:
        raise OSError("simulated fsync failure")

    monkeypatch.setattr(os, "fsync", fail_fsync)

    with pytest.raises(OSError, match="simulated fsync failure"):
        store.capture(
            snapshot_for(payload, retrieved_at=datetime(2026, 7, 29, 0, 0, tzinfo=UTC)),
            payload,
        )

    assert list(tmp_path.rglob("*.raw")) == []
    assert list(tmp_path.rglob("*.json")) == []
    assert list(tmp_path.rglob("*.tmp")) == []
