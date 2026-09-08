"""The registered SEC marker bytes remain authoritative across finalization races."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import (
    collection_run_events,
    collection_run_receipts,
    collection_watermarks,
)
from aegis_alpha.data import sec_finalization
from aegis_alpha.data.sec_collector import CollectorConfig, CollectorError
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.metadata.schema import dataset_artifacts, dataset_versions, source_snapshots
from tests.data.test_sec_runtime import _config, _rows, _run

if TYPE_CHECKING:
    from sqlalchemy import Engine


def test_marker_swap_after_verification_rolls_back_all_finalization(
    clean_postgres: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = sec_finalization.verified_marker
    trusted_hashes: list[str] = []

    def swap_after_verification(
        registry: CollectionRegistry,
        config: CollectorConfig,
        *,
        run_id: str,
        request_sha256: str,
    ) -> object:
        verified = original(registry, config, run_id=run_id, request_sha256=request_sha256)
        path = config.dataset_root / "finalization.json"
        payload = path.read_bytes()
        trusted_hashes.append(hashlib.sha256(payload).hexdigest())
        replacement = json.loads(payload)
        replacement["disagreement_count"] += 1
        temporary = path.with_suffix(".swapped")
        temporary.write_bytes(canonical_json_bytes(replacement))
        temporary.replace(path)
        return verified

    monkeypatch.setattr(sec_finalization, "verified_marker", swap_after_verification)
    config = _config(tmp_path)
    with pytest.raises(CollectorError, match="runtime failed"):
        _run(clean_postgres, config)
    assert len(trusted_hashes) == 1
    assert _rows(clean_postgres, dataset_versions) == []
    assert _rows(clean_postgres, dataset_artifacts) == []
    assert _rows(clean_postgres, collection_run_receipts) == []
    assert _rows(clean_postgres, collection_watermarks) == []
    assert [row["event_type"] for row in _rows(clean_postgres, collection_run_events)] == [
        "attempt_started"
    ]
    run_source = next(
        row
        for row in _rows(clean_postgres, source_snapshots)
        if row["snapshot_id"] == config.run_identity + "-source"
    )
    assert run_source["content_sha256"] == trusted_hashes[0]


def test_success_uses_registered_marker_hash_for_all_finalization_records(
    clean_postgres: Engine, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    outcome = _run(clean_postgres, config)
    marker_path = config.dataset_root / "runs" / outcome.run_id / "finalization.json"
    expected = hashlib.sha256(marker_path.read_bytes()).hexdigest()
    run_source = next(
        row
        for row in _rows(clean_postgres, source_snapshots)
        if row["snapshot_id"] == outcome.run_id + "-source"
    )
    marker_artifact = next(
        row
        for row in _rows(clean_postgres, dataset_artifacts)
        if row["relative_path"] == "finalization.json"
    )
    receipt = _rows(clean_postgres, collection_run_receipts)[0]
    assert run_source["content_sha256"] == expected
    assert marker_artifact["content_sha256"] == expected
    assert marker_artifact["size_bytes"] == marker_path.stat().st_size
    assert receipt["receipt_sha256"] == expected
    assert receipt["byte_count"] == marker_path.stat().st_size
    for event in _rows(clean_postgres, collection_run_events)[1:]:
        assert event["details_json"] == {"sec_finalization_sha256": expected}
