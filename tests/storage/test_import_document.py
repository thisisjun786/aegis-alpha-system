from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from aegis_alpha.storage.import_document import parse_import, read_import


def payload() -> dict[str, object]:
    return {
        "schema_version": "aas-market-import-v1",
        "dataset_id": "synthetic-macro",
        "version": "1",
        "generation_id": "synthetic-generation",
        "operation_id": "synthetic-import",
        "parent_id": None,
        "domain": "macro_observations",
        "provider": "synthetic",
        "publication_at_us": 1,
        "normalizer_version": "synthetic-v1",
        "transform_sha256": "a" * 64,
        "rows": [{"series_id": "synthetic-series", "value": "1.25"}],
    }


def test_import_preserves_bytes_and_assigns_source_hash(tmp_path: Path) -> None:
    raw = json.dumps(payload()).encode()
    path = tmp_path / "rows.json"
    path.write_bytes(raw)
    parsed = read_import(path, hashlib.sha256(raw).hexdigest())
    assert parsed.payload == raw
    assert parsed.rows[0]["source_snapshot_id"] == parsed.source_id
    with pytest.raises(ValueError, match="SHA-256"):
        read_import(path, "a" * 64)


def test_import_rejects_forged_source_unknown_and_duplicate_fields() -> None:
    document = payload()
    document["sql"] = "drop table arbitrary"
    with pytest.raises(ValueError, match="unknown"):
        parse_import(json.dumps(document).encode())
    document = payload()
    document["rows"] = [{"source_snapshot_id": "forged"}]
    with pytest.raises(ValueError, match="lineage"):
        parse_import(json.dumps(document).encode())
    raw = json.dumps(payload()).replace('"version": "1"', '"version": "1", "version": "2"').encode()
    with pytest.raises(ValueError, match="duplicate"):
        parse_import(raw)
