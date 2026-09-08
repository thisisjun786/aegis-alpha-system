"""Disagreement sidecar is diagnostic-only and never mutates FMP or SEC sources."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from sec_collector_support import (
    FIXTURE_ROOT,
    RecordingIdentity,
    load_named_identity,
    make_collector,
    receipt_view,
)

from aegis_alpha.collection.records import CollectionMode, RunEventType
from aegis_alpha.data.sec_disagreement import (
    ComparableFact,
    compare_disagreements,
    load_fmp_comparison,
    sec_comparable_facts,
)
from aegis_alpha.data.sec_identity import AdmissionState

SEC_REVENUE = 1_000_000
FMP_REVENUE = 999_999


def test_sidecar_records_disagreement_without_mutating_fmp_bytes(tmp_path: Path) -> None:
    fmp_path = tmp_path / "fmp_comparison.json"
    original = (FIXTURE_ROOT / "fmp_comparison.json").read_bytes()
    fmp_path.write_bytes(original)
    identity = RecordingIdentity(load_named_identity("identity_admitted.json"))
    collector, _clock, _control = make_collector(tmp_path, mode=CollectionMode.BACKFILL)
    fmp_facts = load_fmp_comparison(fmp_path)

    outcome = collector.collect(
        identity=identity,
        fmp_facts=fmp_facts,
        fmp_source_bytes={fmp_path: original},
    )

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert outcome.disagreement_count == 1
    assert fmp_path.read_bytes() == original
    assert json.loads(original) == json.loads(fmp_path.read_bytes())
    sidecar = tmp_path / "data" / "normalized" / "sec" / "sec-normalized-v1" / "disagreements"
    assert any(sidecar.rglob("*.parquet"))
    fmp_raw = tmp_path / "raw" / "fmp"
    assert not fmp_raw.exists()
    assert "insert" not in {name for name, _value in identity.calls}


def test_compare_does_not_rewrite_input_fact_tuples() -> None:
    sec = (ComparableFact("0000990001", "revenue", "2024-12-31", SEC_REVENUE, "sec-1", "sec"),)
    fmp = (ComparableFact("0000990001", "revenue", "2024-12-31", FMP_REVENUE, "fmp-1", "fmp"),)
    observed = datetime(2026, 8, 18, tzinfo=UTC)

    rows = compare_disagreements(sec, fmp, observed_at_utc=observed)

    assert len(rows) == 1
    assert sec[0].value == SEC_REVENUE
    assert fmp[0].value == FMP_REVENUE
    assert rows[0].sec_snapshot_id == "sec-1"
    assert rows[0].fmp_snapshot_id == "fmp-1"


def test_matching_values_produce_no_sidecar_rows() -> None:
    fact = ComparableFact("0000990001", "dividend", "2024-11-15", 0.25, "snap", "sec")
    rows = compare_disagreements(
        (fact,), (fact,), observed_at_utc=datetime(2026, 8, 18, tzinfo=UTC)
    )
    assert rows == ()


def test_sec_comparable_projection_does_not_mutate_fact_rows() -> None:
    row = {
        "cik": "0000990001",
        "tag": "Revenues",
        "period_end": "2024-12-31",
        "value": SEC_REVENUE,
    }
    original = dict(row)

    facts = sec_comparable_facts((row,), snapshot_id="run-1")

    assert row == original
    assert facts[0].field == "revenue"


def test_successful_collect_leaves_skip_receipt_when_no_cik(tmp_path: Path) -> None:
    identity = RecordingIdentity(load_named_identity("identity_no_cik.json"))
    collector, _clock, _control = make_collector(tmp_path)

    outcome = collector.collect(identity=identity)

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert outcome.calls_attempted == 0
    assert outcome.skips[0].state is AdmissionState.SKIPPED_NO_CIK
    receipt = receipt_view(tmp_path / "receipts" / "sec.receipt.json")
    skips = receipt["skips"]
    assert isinstance(skips, list)
    first_skip = skips[0]
    assert isinstance(first_skip, Mapping)
    assert cast("Mapping[str, object]", first_skip)["state"] == "skipped_no_cik"
    assert receipt["calls_attempted"] == 0
    assert identity.calls
    assert all(name != "insert" for name, _value in identity.calls)
