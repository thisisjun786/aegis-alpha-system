"""Synthetic SEC collector: first-wave datasets, 005 modes, identity isolation."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from sec_collector_support import (
    FIXTURE_ROOT,
    PADDED_CIK,
    SOURCE_CIK,
    FakeControlPlane,
    RecordingIdentity,
    fixture_bodies,
    fixture_companyfact_tags,
    load_named_identity,
    make_collector,
    read_dataset_column,
    receipt_view,
)

from aegis_alpha.collection.records import CollectionMode, RunEventType, WatermarkAdvance
from aegis_alpha.collection.registry import CollectionRegistry, CollectionStateError
from aegis_alpha.data.sec_collector import DATASET, companyfacts_stream, submissions_stream
from aegis_alpha.data.sec_identity import Admission, AdmissionState
from aegis_alpha.data.sec_normalize import (
    FACTS_DATASET,
    PROVIDER,
    SUBMISSIONS_DATASET,
    THIRTEEN_F_DATASET,
    NormalizationContext,
    extract_cik_source,
    normalize_companyfacts,
    normalize_submissions,
    parse_json_object,
)
from aegis_alpha.data.sec_transport import DatasetKind

if TYPE_CHECKING:
    from sqlalchemy import Engine

FIRST_WAVE_CALLS = 2
SUBMISSIONS_ONLY_CALLS = 1


def test_probe_collects_submissions_facts_and_13f_index(tmp_path: Path) -> None:
    identity = RecordingIdentity(load_named_identity("identity_admitted.json"))
    collector, _clock, control = make_collector(tmp_path, mode=CollectionMode.PROBE)

    outcome = collector.collect(identity=identity)

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert outcome.calls_attempted == FIRST_WAVE_CALLS
    assert outcome.watermarks_advanced == ()
    assert isinstance(control, FakeControlPlane)
    assert {event.event_type for event in control.events} >= {
        RunEventType.ATTEMPT_STARTED,
        RunEventType.ATTEMPT_SUCCEEDED,
        RunEventType.RUN_SUCCEEDED,
    }
    version = tmp_path / "data" / "normalized" / "sec" / "sec-normalized-v1"
    submissions_ciks = read_dataset_column(version, SUBMISSIONS_DATASET, "cik")
    submissions_sources = read_dataset_column(version, SUBMISSIONS_DATASET, "cik_source")
    fact_tags = {str(tag) for tag in read_dataset_column(version, FACTS_DATASET, "tag")}
    thirteen_f_forms = read_dataset_column(version, THIRTEEN_F_DATASET, "form")
    fixture_tags = fixture_companyfact_tags()
    assert "0000990001" in submissions_ciks
    assert SOURCE_CIK in submissions_sources
    assert "13F-HR" in thirteen_f_forms
    assert "Revenues" in fixture_tags
    assert "StockholdersEquityNoteStockSplitConversionRatio" in fixture_tags
    assert fixture_tags <= fact_tags
    raw_submissions = tmp_path / "raw" / "sec" / "submissions"
    assert any(raw_submissions.rglob("*.raw"))
    assert outcome.identity_writes == 0


def test_backfill_advances_watermarks_incremental_skips_fresh_facts(tmp_path: Path) -> None:
    identity = RecordingIdentity(load_named_identity("identity_admitted.json"))
    first, _clock, control = make_collector(tmp_path, mode=CollectionMode.BACKFILL)
    first_outcome = first.collect(identity=identity)
    assert first_outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert any(
        dataset == DATASET and stream == submissions_stream(PADDED_CIK)
        for dataset, stream, _value in first_outcome.watermarks_advanced
    )
    assert first_outcome.calls_attempted == FIRST_WAVE_CALLS

    second, _clock2, _control2 = make_collector(
        tmp_path / "second",
        mode=CollectionMode.INCREMENTAL,
        control=control,
    )
    second_outcome = second.collect(identity=identity)
    assert second_outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert second_outcome.calls_attempted == SUBMISSIONS_ONLY_CALLS


def test_conflict_skip_makes_zero_calls_and_writes_no_identity(tmp_path: Path) -> None:
    identity = RecordingIdentity(load_named_identity("identity_conflict.json"))
    collector, _clock, _control = make_collector(tmp_path)

    outcome = collector.collect(identity=identity)

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert outcome.calls_attempted == 0
    assert all(item.state is AdmissionState.SKIPPED_CONFLICT for item in outcome.admissions)
    assert outcome.identity_writes == 0
    assert not (tmp_path / "raw" / "sec" / "submissions").exists()
    assert all(name != "insert" for name, _value in identity.calls)
    receipt = receipt_view(tmp_path / "receipts" / "sec.receipt.json")
    assert receipt["identity_writes"] == 0


def test_normalize_companyfacts_keeps_every_fixture_tag_in_stable_order() -> None:
    payload = parse_json_object(
        (FIXTURE_ROOT / "companyfacts.json").read_bytes(), label="companyfacts"
    )
    fixture_tags = fixture_companyfact_tags()
    rows = normalize_companyfacts(
        payload,
        NormalizationContext(
            cik_source=extract_cik_source(payload),
            instrument_id="inst-synth-common",
            issuer_id="issuer-synth",
            snapshot_id="snap",
            raw_content_sha256="a" * 64,
        ),
    )
    tags = [str(row["tag"]) for row in rows]

    assert "Revenues" in fixture_tags
    assert fixture_tags <= set(tags)
    assert tags == sorted(tags)
    assert len(rows) == len(fixture_tags)


def test_normalize_companyfacts_keeps_revenues_when_fixture_key_order_is_reversed() -> None:
    payload = json.loads((FIXTURE_ROOT / "companyfacts.json").read_text(encoding="utf-8"))
    us_gaap = payload["facts"]["us-gaap"]
    payload["facts"]["us-gaap"] = {tag: us_gaap[tag] for tag in reversed(list(us_gaap))}
    rows = normalize_companyfacts(
        payload,
        NormalizationContext(
            cik_source=extract_cik_source(payload),
            instrument_id="inst-synth-common",
            issuer_id="issuer-synth",
            snapshot_id="snap",
            raw_content_sha256="a" * 64,
        ),
    )
    tags = {str(row["tag"]) for row in rows}
    assert "Revenues" in tags
    assert fixture_companyfact_tags() <= tags


def test_normalized_cik_is_padded_raw_payload_keeps_unpadded_source() -> None:
    payload = parse_json_object(
        (FIXTURE_ROOT / "submissions.json").read_bytes(), label="submissions"
    )
    assert extract_cik_source(payload) == SOURCE_CIK
    rows = normalize_submissions(
        payload,
        NormalizationContext(
            cik_source=extract_cik_source(payload),
            instrument_id="inst-synth-common",
            issuer_id="issuer-synth",
            snapshot_id="snap",
            raw_content_sha256="a" * 64,
        ),
    )
    assert rows[0]["cik"] == PADDED_CIK
    assert rows[0]["cik_source"] == SOURCE_CIK
    raw = json.loads((FIXTURE_ROOT / "submissions.json").read_text(encoding="utf-8"))
    assert raw["cik"] == SOURCE_CIK


def test_payload_cik_mismatch_fails_closed_and_keeps_unpadded_source(tmp_path: Path) -> None:
    payload = json.loads((FIXTURE_ROOT / "submissions.json").read_text(encoding="utf-8"))
    assert payload["cik"] == SOURCE_CIK
    payload["cik"] = "123456"
    bodies = fixture_bodies()
    bodies[(DatasetKind.SUBMISSIONS, PADDED_CIK)] = json.dumps(payload).encode()
    identity = RecordingIdentity(load_named_identity("identity_admitted.json"))
    collector, _clock, control = make_collector(tmp_path, bodies=bodies)

    outcome = collector.collect(identity=identity)

    assert outcome.terminal_event is RunEventType.RUN_FAILED
    assert isinstance(control, FakeControlPlane)
    event_types = [event.event_type for event in control.events]
    assert RunEventType.ATTEMPT_STARTED in event_types
    assert RunEventType.ATTEMPT_FAILED in event_types
    assert RunEventType.RUN_FAILED in event_types
    assert RunEventType.RUN_SUCCEEDED not in event_types
    assert not (tmp_path / "data" / "normalized").exists()
    raw = json.loads((FIXTURE_ROOT / "submissions.json").read_text(encoding="utf-8"))
    assert raw["cik"] == SOURCE_CIK


def test_publish_collision_emits_attempt_failed_not_orphan_started(tmp_path: Path) -> None:
    receipt = tmp_path / "receipts" / "sec.receipt.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_bytes(b'{"collision":true}')
    identity = RecordingIdentity(load_named_identity("identity_admitted.json"))
    collector, _clock, control = make_collector(tmp_path)

    outcome = collector.collect(identity=identity)

    assert outcome.terminal_event is RunEventType.RUN_FAILED
    assert isinstance(control, FakeControlPlane)
    event_types = [event.event_type for event in control.events]
    assert RunEventType.ATTEMPT_STARTED in event_types
    assert RunEventType.ATTEMPT_FAILED in event_types
    assert RunEventType.RUN_FAILED in event_types
    assert RunEventType.ATTEMPT_SUCCEEDED not in event_types
    assert RunEventType.RUN_SUCCEEDED not in event_types
    state = control.current_run_state(outcome.run_id)
    assert state is not None
    assert state.state is RunEventType.RUN_FAILED
    assert not list((tmp_path / "data").rglob("*.parquet"))


def test_fmp_guard_error_emits_run_failed(tmp_path: Path) -> None:
    fmp_path = tmp_path / "fmp-comparison.json"
    fmp_path.write_bytes(b"original-fmp-bytes")

    class MutatingIdentity(RecordingIdentity):
        def admit(self, instrument_id: str, as_of: datetime) -> Admission:
            fmp_path.write_bytes(b"mutated-fmp-bytes")
            return super().admit(instrument_id, as_of)

    identity = MutatingIdentity(load_named_identity("identity_admitted.json"))
    collector, _clock, control = make_collector(tmp_path)

    outcome = collector.collect(
        identity=identity, fmp_source_bytes={fmp_path: b"original-fmp-bytes"}
    )

    assert outcome.terminal_event is RunEventType.RUN_FAILED
    assert isinstance(control, FakeControlPlane)
    event_types = [event.event_type for event in control.events]
    assert RunEventType.ATTEMPT_FAILED in event_types
    assert RunEventType.RUN_FAILED in event_types
    assert RunEventType.ATTEMPT_SUCCEEDED not in event_types


def test_tickers_in_submissions_are_not_identity_writes(tmp_path: Path) -> None:
    identity = RecordingIdentity(load_named_identity("identity_admitted.json"))
    collector, _clock, _control = make_collector(tmp_path)

    outcome = collector.collect(identity=identity)

    receipt = receipt_view(tmp_path / "receipts" / "sec.receipt.json")
    assert "SYNTH" not in json.dumps(receipt)
    assert outcome.identity_writes == 0
    assert all(name != "insert" for name, _value in identity.calls)


def test_real_005_uses_plan_dataset_and_skips_unchanged_backfill(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    identity = RecordingIdentity(load_named_identity("identity_admitted.json"))
    registry = CollectionRegistry(clean_postgres)
    first, _clock, _control = make_collector(
        tmp_path, mode=CollectionMode.BACKFILL, control=registry
    )
    first_outcome = first.collect(identity=identity)

    assert first_outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    current = registry.latest_watermark(PROVIDER, DATASET, submissions_stream(PADDED_CIK))
    assert current is not None
    assert current.dataset == DATASET
    assert current.stream == submissions_stream(PADDED_CIK)
    facts = registry.latest_watermark(PROVIDER, DATASET, companyfacts_stream(PADDED_CIK))
    assert facts is not None
    assert registry.latest_watermark(PROVIDER, f"submissions:{PADDED_CIK}", PADDED_CIK) is None
    with pytest.raises(CollectionStateError, match="provider/dataset"):
        registry.advance_watermark(
            WatermarkAdvance(
                provider=PROVIDER,
                dataset=f"submissions:{PADDED_CIK}",
                stream=PADDED_CIK,
                run_id=first_outcome.run_id,
                watermark_value=current.watermark_value,
                watermark_position=current.watermark_position,
            )
        )

    second, _clock2, _control2 = make_collector(
        tmp_path / "second", mode=CollectionMode.BACKFILL, control=registry
    )
    second_outcome = second.collect(identity=identity)

    assert second_outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert second_outcome.watermarks_advanced == ()
    replay = registry.latest_watermark(PROVIDER, DATASET, submissions_stream(PADDED_CIK))
    assert replay is not None
    assert replay.run_id == first_outcome.run_id
