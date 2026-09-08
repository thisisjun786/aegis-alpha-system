"""Changed bytes, forged lineage and escaping paths cannot enter the FMP catalog."""

from __future__ import annotations

# ruff: noqa: F811 -- imported pytest fixtures are consumed by their parameter names
import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import select
from test_fmp_catalog import catalog_counts, lifecycle, uncataloged  # noqa: F401 -- shared fixture
from test_fmp_daily_budget import (  # noqa: F401
    SUCCESS_CALLS,
    DailyHarness,
    DailyTransport,
    daily_harness,
)

from aegis_alpha.data.fmp_catalog import (
    recover_completed_collections,
    register_completed_collection,
)
from aegis_alpha.data.fmp_catalog_io import FmpCatalogError
from aegis_alpha.data.fmp_collector import CollectorRequest, CollectorResponse
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.metadata.registry import MetadataConflictError
from aegis_alpha.metadata.schema import quality_results, source_snapshots


def _documents(
    harness: DailyHarness, run_id: str
) -> tuple[Path, dict[str, object], Path, dict[str, object]]:
    root = harness.root / "raw/fmp/runs" / run_id
    marker = json.loads((root / "publication.json").read_bytes())
    completion = json.loads((root / "completion.json").read_bytes())
    receipt_path = Path(completion["receipt_path"])
    return root, marker, receipt_path, json.loads(receipt_path.read_bytes())


def _rebind(
    root: Path, marker: dict[str, object], receipt_path: Path, receipt: dict[str, object]
) -> None:
    marker_bytes = canonical_json_bytes(marker)
    (root / "publication.json").write_bytes(marker_bytes)
    marker_sha = hashlib.sha256(marker_bytes).hexdigest()
    receipt["publication_marker_sha256"] = marker_sha
    receipt_bytes = canonical_json_bytes(receipt)
    receipt_path.write_bytes(receipt_bytes)
    (root / "completion.json").write_bytes(
        canonical_json_bytes(
            {
                "marker_sha256": marker_sha,
                "plan_id": marker["plan_id"],
                "receipt_path": str(receipt_path),
                "receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
                "run_id": marker["run_id"],
            }
        )
    )


@pytest.mark.parametrize("kind", ["raw", "output", "receipt", "attempt", "provenance", "chunk"])
def test_tampered_evidence_refuses_without_catalog_or_lifecycle_changes(
    daily_harness: DailyHarness,
    uncataloged: str,
    kind: str,
) -> None:
    root, marker, receipt_path, receipt = _documents(daily_harness, uncataloged)
    if kind == "receipt":
        target = receipt_path
    elif kind == "output":
        target = Path(cast("list[dict[str, str]]", marker["artifacts"])[0]["path"])
    elif kind == "attempt":
        target = root / "attempts/00000001.json"
    elif kind == "provenance":
        ref = json.loads((root / "provenance/00000001.provenance.json").read_bytes())
        sha = ref["provenance_sha256"]
        target = daily_harness.root / "raw/fmp/provenance/sha256" / sha[:2] / f"{sha}.json"
    else:
        field = "raw_content_addresses" if kind == "raw" else "request_metadata_chunk_addresses"
        sha = cast("list[str]", receipt[field])[0].removeprefix("sha256:")
        directory, suffix = ("blobs", "raw") if kind == "raw" else ("receipt-metadata", "json")
        target = daily_harness.root / "raw/fmp" / directory / "sha256" / sha[:2] / f"{sha}.{suffix}"
    before = lifecycle(daily_harness.engine)
    target.write_bytes(target.read_bytes() + b" ")
    result = recover_completed_collections(
        daily_harness.engine, daily_harness.root / "raw", daily_harness.root / "normalized"
    )
    assert result["status"] == "catalog_pending"
    assert result["failures"]
    assert catalog_counts(daily_harness.engine) == (0, 0, 0, 0, 0)
    assert lifecycle(daily_harness.engine) == before


@pytest.mark.parametrize("kind", ["escape", "duplicate", "symlink", "raw_refs", "plan"])
def test_rebound_manifest_still_cannot_bypass_catalog_admission(
    daily_harness: DailyHarness,
    uncataloged: str,
    kind: str,
) -> None:
    root, marker, receipt_path, receipt = _documents(daily_harness, uncataloged)
    artifacts = cast("list[dict[str, str]]", marker["artifacts"])
    if kind in {"escape", "symlink"}:
        original = Path(artifacts[0]["path"])
        foreign = daily_harness.root / "foreign.parquet"
        foreign.write_bytes(original.read_bytes())
        if kind == "escape":
            artifacts[0]["path"] = str(foreign)
        else:
            original.unlink()
            original.symlink_to(foreign)
    elif kind == "duplicate":
        artifacts.append(dict(artifacts[0]))
    elif kind == "raw_refs":
        receipt["raw_content_addresses"] = []
    else:
        receipt["plan_sha256"] = "0" * 64
    _rebind(root, marker, receipt_path, receipt)
    with pytest.raises((ValueError, OSError)):
        register_completed_collection(
            daily_harness.engine,
            daily_harness.root / "raw",
            daily_harness.root / "normalized",
            uncataloged,
        )
    assert catalog_counts(daily_harness.engine) == (0, 0, 0, 0, 0)


def test_empty_success_registers_source_and_receipt_without_invented_dataset(
    daily_harness: DailyHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = DailyTransport.__call__

    def future_listing(
        self: DailyTransport, request: CollectorRequest, credential: str
    ) -> CollectorResponse:
        response = original(self, request, credential)
        if request.endpoint == "/stable/actively-trading-list":
            return replace(response, body=b'[{"symbol":"SYNTH.A","ipoDate":"2026-08-22"}]')
        return response

    monkeypatch.setattr(DailyTransport, "__call__", future_listing)
    code, report = daily_harness.run(SUCCESS_CALLS)
    assert code == 0, report
    assert catalog_counts(daily_harness.engine) == (1, 1, 0, 0, 1)


def test_cancelled_collection_cannot_be_cataloged(daily_harness: DailyHarness) -> None:
    code, report = daily_harness.run(3)
    assert code == 1
    with pytest.raises(FmpCatalogError, match="successful"):
        register_completed_collection(
            daily_harness.engine,
            daily_harness.root / "raw",
            daily_harness.root / "normalized",
            str(report["run_id"]),
        )
    assert catalog_counts(daily_harness.engine) == (0, 0, 0, 0, 0)


def test_partial_coverage_evidence_is_preserved_without_eligibility(
    daily_harness: DailyHarness,
    uncataloged: str,
) -> None:
    root, marker, path, receipt = _documents(daily_harness, uncataloged)
    receipt["blocked_symbols"] = ["synth.missing"]
    receipt["quality_results"] = [{"kind": "missing_coverage", "symbol": "synth.missing"}]
    _rebind(root, marker, path, receipt)
    result = register_completed_collection(
        daily_harness.engine,
        daily_harness.root / "raw",
        daily_harness.root / "normalized",
        uncataloged,
    )
    assert result["data_eligibility_changed"] is False
    with daily_harness.engine.connect() as connection:
        manifest = connection.scalar(select(source_snapshots.c.manifest_json))
        assert manifest is not None
        assert manifest["receipt"]["blocked_symbols"] == ["synth.missing"]
        checks = connection.execute(select(quality_results)).mappings().all()
        assert checks
        for row in checks:
            assert row["status"] == "BLOCKED"
            assert (
                json.loads(row["details_json"][0])["quality_results"] == receipt["quality_results"]
            )


def test_changed_receipt_cannot_replace_registered_source(daily_harness: DailyHarness) -> None:
    code, report = daily_harness.run(SUCCESS_CALLS)
    assert code == 0, report
    run_id = str(report["collection_run_id"])
    root, marker, path, receipt = _documents(daily_harness, run_id)
    before = catalog_counts(daily_harness.engine)
    receipt["blocked_symbols"] = ["changed-after-registration"]
    _rebind(root, marker, path, receipt)
    with pytest.raises(MetadataConflictError):
        register_completed_collection(
            daily_harness.engine,
            daily_harness.root / "raw",
            daily_harness.root / "normalized",
            run_id,
        )
    assert catalog_counts(daily_harness.engine) == before
