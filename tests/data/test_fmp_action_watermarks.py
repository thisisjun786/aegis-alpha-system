"""Future corporate-action dates are evidence, not price-window cursors."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from aegis_alpha.collection.records import CollectionMode, CurrentWatermark
from aegis_alpha.data.fmp_collection_work import collect_manifest
from aegis_alpha.data.fmp_collector import (
    CollectorConfig,
    CollectorRequest,
    CollectorResponse,
    FmpCollector,
)
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_rate_limit import RateLimiter, TierArtifact
from aegis_alpha.data.fmp_windows import ManifestSource, UniverseEntry, UniverseManifest
from tests.data.test_fmp_collector_engine import FakeControlPlane

AS_OF = date(2025, 3, 10)
NOW = datetime(2025, 3, 10, 12, tzinfo=UTC)
PAST = date(2025, 3, 1)
FUTURE = date(2025, 3, 20)


def _collector(
    root: Path,
    dataset: str,
    prior: date | None,
    dates: tuple[date, ...],
    *,
    operator_from: date | None = date(2025, 2, 1),
) -> tuple[FmpCollector, FakeControlPlane, list[CollectorRequest]]:
    plane = FakeControlPlane()
    if prior is not None:
        plane.watermarks[("fmp", dataset, "SYNTH")] = CurrentWatermark(
            provider="fmp",
            dataset=dataset,
            stream="SYNTH",
            watermark_seq=1,
            run_id="fmp-run-old",
            watermark_value=prior.isoformat(),
            watermark_position=datetime.combine(prior, datetime.min.time(), tzinfo=UTC),
            recorded_at_utc=NOW,
        )
    calls: list[CollectorRequest] = []

    def transport(request: CollectorRequest, _credential: str) -> CollectorResponse:
        calls.append(request)
        if request.endpoint == "/stable/profile":
            records = [{"symbol": "SYNTH", "cik": "0000000001"}]
        else:
            fields = (
                {"dividend": 1.0, "adjDividend": 1.0}
                if dataset == "fmp_dividends"
                else {"numerator": 2.0, "denominator": 1.0}
            )
            records = [{"symbol": "SYNTH", "date": value.isoformat(), **fields} for value in dates]
        return CollectorResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            body=json.dumps(records).encode(),
            requested_at_utc=NOW,
            retrieved_at_utc=NOW,
        )

    collector = FmpCollector(
        config=CollectorConfig(
            raw_store_root=root / "raw",
            dataset_root=root / "normalized",
            receipt_path=root / "receipt.json",
            as_of=AS_OF,
            mode=CollectionMode.INCREMENTAL,
            max_calls=5,
            run_identity="fmp-run-action-fixture",
            operator_from=operator_from,
        ),
        transport=transport,
        control_plane=plane,
        limiter=RateLimiter(
            tier=TierArtifact(3000, None, None),
            max_calls=5,
            clock=lambda: 0.0,
            sleep=lambda _seconds: None,
            run_seed=1,
        ),
        credential="synthetic-only",
        clock=lambda: NOW,
    )
    return collector, plane, calls


def _manifest() -> UniverseManifest:
    return UniverseManifest(
        generated_at_utc=NOW,
        sources=(ManifestSource("/stable/actively-trading-list", NOW, "a" * 64),),
        entries=(UniverseEntry("SYNTH", None, None, active=True),),
    )


@pytest.mark.parametrize("selection", [DatasetSelection.DIVIDENDS, DatasetSelection.SPLITS])
@pytest.mark.parametrize("prior", [None, date(2025, 2, 28), PAST, FUTURE])
def test_action_snapshot_keeps_future_rows_and_never_rewinds_old_watermark(
    tmp_path: Path,
    selection: DatasetSelection,
    prior: date | None,
) -> None:
    collector, plane, calls = _collector(tmp_path, selection.value, prior, (PAST, FUTURE))
    before = dict(plane.watermarks)
    batches = list(collect_manifest(collector, _manifest(), selection))
    actions = [batch for batch in batches if batch.dataset == selection.value]
    assert len(actions) == 1
    assert {row["date"] for row in actions[0].rows} == {PAST, FUTURE}
    expected = None if prior is not None and prior >= PAST else (selection.value, "SYNTH", PAST)
    assert actions[0].advance == expected
    assert plane.watermarks == before
    assert [request.endpoint for request in calls] == [
        "/stable/profile",
        "/stable/" + selection.value.removeprefix("fmp_"),
    ]
    assert calls[1].parameters == {"symbol": "SYNTH"}


@pytest.mark.parametrize("selection", [DatasetSelection.DIVIDENDS, DatasetSelection.SPLITS])
def test_future_only_actions_do_not_advance_watermark(
    tmp_path: Path, selection: DatasetSelection
) -> None:
    collector, _plane, _calls = _collector(tmp_path, selection.value, None, (FUTURE,))
    batch = list(collect_manifest(collector, _manifest(), selection))[-1]
    assert batch.rows[0]["date"] == FUTURE
    assert batch.advance is None


def test_future_price_watermark_still_fails_closed(tmp_path: Path) -> None:
    collector, _plane, calls = _collector(tmp_path, "fmp_price_eod_full", FUTURE, ())
    with pytest.raises(ValueError, match="watermark cannot exceed"):
        list(collect_manifest(collector, _manifest(), DatasetSelection.PROBE))
    assert calls == []


@pytest.mark.parametrize("selection", [DatasetSelection.DIVIDENDS, DatasetSelection.SPLITS])
def test_action_snapshot_with_existing_cursor_needs_no_backfill_start(
    tmp_path: Path,
    selection: DatasetSelection,
) -> None:
    collector, _plane, calls = _collector(
        tmp_path, selection.value, FUTURE, (FUTURE,), operator_from=None
    )
    batches = list(collect_manifest(collector, _manifest(), selection))
    assert batches[-1].rows[0]["date"] == FUTURE
    assert batches[-1].advance is None
    assert len(calls) == 2  # noqa: PLR2004 -- profile and action snapshot


@pytest.mark.parametrize(
    "entry",
    [
        UniverseEntry("SYNTH", FUTURE, None, active=True),
        UniverseEntry("SYNTH", None, date(2025, 1, 1), active=False),
    ],
)
def test_action_snapshot_keeps_manifest_empty_range_refusal(
    tmp_path: Path, entry: UniverseEntry
) -> None:
    collector, _plane, calls = _collector(tmp_path, "fmp_dividends", FUTURE, (FUTURE,))
    manifest = replace(_manifest(), entries=(entry,))
    assert list(collect_manifest(collector, manifest, DatasetSelection.DIVIDENDS)) == []
    assert calls == []
    assert collector.quality_results[0].kind.value == "empty_range_skipped"


@pytest.mark.parametrize("selection", [DatasetSelection.DIVIDENDS, DatasetSelection.SPLITS])
def test_empty_action_snapshot_preserves_existing_watermark(
    tmp_path: Path, selection: DatasetSelection
) -> None:
    collector, plane, calls = _collector(tmp_path, selection.value, FUTURE, ())
    before = dict(plane.watermarks)
    batches = list(collect_manifest(collector, _manifest(), selection))
    assert [batch.dataset for batch in batches] == ["fmp_profile"]
    assert plane.watermarks == before
    assert len(calls) == 2  # noqa: PLR2004 -- profile and empty action snapshot
