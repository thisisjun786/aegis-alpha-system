"""AAS-DATA-008C identity, restatement, and deterministic replay tests.

All evidence is synthetic and provider-neutral; zero provider calls are made.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path

import pytest
from finimpulse_collector_support import (
    LATER_OBSERVED_AT,
    OBSERVED_AT,
    SYNTHETIC_CREDENTIAL,
    RecordingTransport,
    build_raw_call,
    build_response,
    make_config,
    make_gate_evidence,
    make_identity_export,
    no_sleep,
    receipt_view,
    snapshot_items,
    synthetic_pair,
)

from aegis_alpha.data.finimpulse_collector import (
    CollectionResult,
    ContractError,
    IdentityEvidenceError,
    IdentityExport,
    IdentityState,
    RestatementClass,
    RestatementFinding,
    classify_restatements,
    collect_snapshot,
    logical_content_hash,
    replay_from_raw,
)


def collect(  # noqa: PLR0913 - each argument varies one synthetic snapshot input
    tmp_path: Path,
    snapshot: str,
    *,
    snapshot_id: str = "snap-001",
    symbols: tuple[str, ...] = ("AAPL", "PLAB"),
    identity: IdentityExport | None = None,
    predecessor_rows: Sequence[Mapping[str, object]] = (),
    observed_at: datetime = OBSERVED_AT,
    predecessor_snapshot_id: str | None = None,
) -> CollectionResult:
    resolved_identity = make_identity_export(symbols=symbols) if identity is None else identity
    offline, capability = synthetic_pair(RecordingTransport(snapshot, requested_at=observed_at))
    return collect_snapshot(
        snapshot_id=snapshot_id,
        config=make_config(
            symbols,
            predecessor_snapshot_id=predecessor_snapshot_id,
            identity_export_sha256=resolved_identity.export_sha256,
            identity_as_of=resolved_identity.as_of_utc,
        ),
        credential=SYNTHETIC_CREDENTIAL,
        transport=offline,
        raw_store_root=tmp_path / "raw",
        dataset_root=tmp_path / "dataset",
        identity_export=resolved_identity,
        gate_evidence=make_gate_evidence(tmp_path / "gates", symbols=symbols),
        predecessor_rows=predecessor_rows,
        observed_at=observed_at,
        sleeper=no_sleep,
        synthetic=capability,
    )


def test_unresolved_ticker_stays_unmapped_and_is_quarantined(tmp_path: Path) -> None:
    identity = make_identity_export({"AAPL": ["INST-AAPL"], "PLAB": []})

    result = collect(tmp_path, "baseline", identity=identity)

    states = {decision.provider_symbol: decision.state for decision in result.identity_decisions}
    assert states["AAPL"] is IdentityState.RESOLVED
    assert states["PLAB"] is IdentityState.UNRESOLVED
    unmapped = [row for row in result.rows if row["provider_symbol"] == "PLAB"]
    assert unmapped
    assert all(row["instrument_id"] is None for row in unmapped)
    quarantined = receipt_view(result.receipt)["identity"]["quarantined"]
    assert {entry["provider_symbol"] for entry in quarantined} == {"PLAB"}


def test_ambiguous_mapping_is_quarantined_and_never_guessed(tmp_path: Path) -> None:
    identity = make_identity_export({"AAPL": ["INST-AAPL"], "PLAB": ["INST-A", "INST-B"]})

    result = collect(tmp_path, "baseline", identity=identity)

    decision = next(item for item in result.identity_decisions if item.provider_symbol == "PLAB")
    assert decision.state is IdentityState.AMBIGUOUS
    assert decision.instrument_id is None
    assert receipt_view(result.receipt)["identity"]["counts"]["AMBIGUOUS"] == 1


def test_identity_lookup_uses_the_pinned_export_effective_time(tmp_path: Path) -> None:
    """The receipt pins the export's own effective time, not wall-clock now."""

    identity = make_identity_export({"AAPL": ["INST-AAPL"], "PLAB": ["INST-PLAB"]})

    result = collect(tmp_path, "baseline", identity=identity)

    pinned = receipt_view(result.receipt)["pinned_replay_inputs"]
    assert pinned["identity_export_sha256"] == identity.export_sha256
    assert pinned["identity_as_of_utc"] == "2026-07-31T12:00:00Z"


def test_mapping_correction_appends_a_new_snapshot_without_rewriting(tmp_path: Path) -> None:
    first = collect(
        tmp_path,
        "baseline",
        identity=make_identity_export({"AAPL": []}),
        symbols=("AAPL",),
    )
    assert first.manifest is not None
    original = {
        partition.relative_path: (tmp_path / "dataset" / partition.relative_path).read_bytes()
        for partition in first.manifest.partitions
    }

    second = collect(
        tmp_path,
        "baseline",
        snapshot_id="snap-002",
        symbols=("AAPL",),
        identity=make_identity_export({"AAPL": ["INST-AAPL"]}),
    )

    assert second.manifest is not None
    assert all(row["instrument_id"] == "INST-AAPL" for row in second.rows)
    for relative_path, payload in original.items():
        assert (tmp_path / "dataset" / relative_path).read_bytes() == payload


def test_symbol_absent_from_the_pinned_export_is_unresolved() -> None:
    decision = make_identity_export({"MSFT": ["INST-MSFT"]}).resolve("AAPL")

    assert decision.state is IdentityState.UNRESOLVED
    assert decision.instrument_id is None


def classify(tmp_path: Path, later_snapshot: str) -> tuple[RestatementFinding, ...]:
    baseline = collect(tmp_path / "a", "baseline", symbols=("AAPL",))
    later = collect(
        tmp_path / "b",
        later_snapshot,
        snapshot_id="snap-002",
        symbols=("AAPL",),
        predecessor_rows=tuple(baseline.rows),
        observed_at=LATER_OBSERVED_AT,
        predecessor_snapshot_id="snap-001",
    )
    return later.restatements


def test_open_period_current_revision_is_expected(tmp_path: Path) -> None:
    findings = classify(tmp_path, "expected_revision")

    revisions = [
        finding
        for finding in findings
        if finding.classification is RestatementClass.EXPECTED_CURRENT_REVISION
    ]
    assert revisions
    assert all(not finding.is_restatement for finding in revisions)


def test_rolling_revision_counts_are_expected(tmp_path: Path) -> None:
    findings = classify(tmp_path, "rolling_window")

    rolling = [
        finding
        for finding in findings
        if finding.classification is RestatementClass.EXPECTED_ROLLING_WINDOW
    ]
    assert rolling
    assert all(not finding.is_restatement for finding in rolling)


def test_lookback_inconsistency_is_restatement_evidence(tmp_path: Path) -> None:
    findings = classify(tmp_path, "lookback_inconsistency")

    flagged = [
        finding
        for finding in findings
        if finding.classification is RestatementClass.RESTATEMENT_LOOKBACK_INCONSISTENCY
    ]
    assert flagged
    assert flagged[0].is_restatement
    assert "thirty_days_ago" in flagged[0].changed_fields


def test_post_actualization_change_is_restatement_evidence(tmp_path: Path) -> None:
    findings = classify(tmp_path, "post_actualization")

    flagged = [
        finding
        for finding in findings
        if finding.classification is RestatementClass.RESTATEMENT_POST_ACTUALIZATION
    ]
    assert flagged
    assert flagged[0].is_restatement


def test_duplicate_natural_key_is_an_anomaly() -> None:
    row = {
        "provider_symbol": "AAPL",
        "estimate_type": "eps_trend",
        "period_date": "2026-09-30",
        "date_type": "quarter",
        "current": 1.7,
    }

    findings = classify_restatements([], [row, dict(row)])

    classes = {finding.classification for finding in findings}
    assert RestatementClass.ANOMALY_DUPLICATE_ROW in classes


def test_new_and_missing_keys_are_classified() -> None:
    previous = {
        "provider_symbol": "AAPL",
        "estimate_type": "eps_trend",
        "period_date": "2026-06-30",
        "date_type": "quarter",
        "current": 1.5,
    }
    current = {**previous, "period_date": "2026-09-30"}

    findings = classify_restatements([previous], [current])

    classes = {finding.classification for finding in findings}
    assert classes == {RestatementClass.NEW_KEY, RestatementClass.MISSING_KEY}


def trend_row(**values: object) -> dict[str, object]:
    row: dict[str, object] = {
        "provider_symbol": "AAPL",
        "estimate_type": "eps_trend",
        "period_date": "2026-09-30",
        "date_type": "quarter",
        "current": None,
        "seven_days_ago": None,
        "thirty_days_ago": None,
        "sixty_days_ago": None,
        "ninety_days_ago": None,
    }
    row.update(values)
    return row


def test_seven_day_ladder_shift_with_consistent_history_is_expected() -> None:
    """A capture seven days later must reproduce the earlier values, shifted."""

    previous = trend_row(current=1.70, seven_days_ago=1.69, thirty_days_ago=1.66)
    current = trend_row(current=1.75, seven_days_ago=1.70, thirty_days_ago=1.66)

    findings = classify_restatements([previous], [current])

    assert [finding.classification for finding in findings] == [
        RestatementClass.EXPECTED_CURRENT_REVISION
    ]
    assert not findings[0].is_restatement


def test_aligned_ladder_with_altered_history_is_restatement_evidence() -> None:
    """A 30-day shift aligns 30->60 and 60->90, so those values must survive.

    Only ages that land on another rung of the sparse ladder are provably
    contradicted; the collector never infers a value it did not capture.
    """

    previous = trend_row(current=1.70, thirty_days_ago=1.66, sixty_days_ago=1.62)
    current = trend_row(
        current=1.80,
        thirty_days_ago=1.70,
        sixty_days_ago=1.40,
        ninety_days_ago=1.62,
    )

    findings = classify_restatements([previous], [current])

    assert findings[0].classification is RestatementClass.RESTATEMENT_LOOKBACK_INCONSISTENCY
    assert findings[0].is_restatement
    assert "sixty_days_ago" in findings[0].changed_fields


def test_unaligned_lookback_age_is_not_treated_as_a_contradiction() -> None:
    """A 7-day shift leaves the 30-day rung at an age nothing else captured.

    That value is unconstrained by prior evidence, so it is not flagged as a
    restatement merely because it moved.
    """

    previous = trend_row(current=1.70, seven_days_ago=1.69, thirty_days_ago=1.66)
    current = trend_row(current=1.75, seven_days_ago=1.70, thirty_days_ago=1.64)

    findings = classify_restatements([previous], [current])

    assert findings[0].classification is RestatementClass.EXPECTED_CURRENT_REVISION
    assert not findings[0].is_restatement


def test_restatement_findings_are_recorded_in_the_receipt(tmp_path: Path) -> None:
    baseline = collect(tmp_path / "a", "baseline", symbols=("AAPL",))
    later = collect(
        tmp_path / "b",
        "lookback_inconsistency",
        snapshot_id="snap-002",
        symbols=("AAPL",),
        predecessor_rows=tuple(baseline.rows),
        observed_at=LATER_OBSERVED_AT,
        predecessor_snapshot_id="snap-001",
    )

    restatements = receipt_view(later.receipt)["restatements"]
    assert restatements["predecessor_snapshot_id"] == "snap-001"
    assert restatements["counts"]["RESTATEMENT_LOOKBACK_INCONSISTENCY"] >= 1
    assert restatements["findings"]


def test_logical_replay_from_raw_content_reproduces_the_logical_hash(tmp_path: Path) -> None:
    identity = make_identity_export({"AAPL": ["INST-AAPL"], "PLAB": []})
    result = collect(tmp_path, "baseline", identity=identity)
    assert result.manifest is not None

    raw_calls = [
        build_raw_call(
            symbol,
            build_response(
                symbol,
                snapshot_items("baseline", symbol),
                cost=float(cost),
            ),
        )
        for symbol, cost in (("AAPL", 0.0014), ("PLAB", 0.0011))
    ]
    replayed = replay_from_raw(
        raw_calls,
        receipt=result.receipt,
        config=make_config(
            identity_export_sha256=identity.export_sha256,
            identity_as_of=identity.as_of_utc,
        ),
        identity_export=identity,
    )

    assert logical_content_hash(replayed) == result.manifest.logical_content_sha256


def test_replay_rejects_raw_content_that_does_not_match_the_receipt(tmp_path: Path) -> None:
    result = collect(tmp_path, "baseline", symbols=("AAPL",))
    tampered_items = snapshot_items("baseline", "AAPL")
    tampered_items[0]["current"] = 9.99
    raw_calls = [build_raw_call("AAPL", build_response("AAPL", tampered_items, cost=0.0014))]

    identity = make_identity_export(symbols=("AAPL",))
    with pytest.raises(ContractError, match="raw content hash mismatch"):
        replay_from_raw(
            raw_calls,
            receipt=result.receipt,
            config=make_config(
                ("AAPL",),
                identity_export_sha256=identity.export_sha256,
            ),
            identity_export=identity,
        )


def test_replay_rejects_a_config_that_was_not_pinned(tmp_path: Path) -> None:
    result = collect(tmp_path, "baseline", symbols=("AAPL",))
    raw_calls = [
        build_raw_call(
            "AAPL", build_response("AAPL", snapshot_items("baseline", "AAPL"), cost=0.0014)
        )
    ]

    identity = make_identity_export(symbols=("AAPL",))
    with pytest.raises(ContractError, match="does not match the config pinned"):
        replay_from_raw(
            raw_calls,
            receipt=result.receipt,
            config=make_config(("MSFT",)),
            identity_export=identity,
        )


def test_replay_rejects_an_identity_export_that_was_not_pinned(tmp_path: Path) -> None:
    """Registry drift cannot change replay output: a different export is fatal."""

    result = collect(tmp_path, "baseline", symbols=("AAPL",))
    raw_calls = [
        build_raw_call(
            "AAPL", build_response("AAPL", snapshot_items("baseline", "AAPL"), cost=0.0014)
        )
    ]
    drifted = make_identity_export({"AAPL": ["INST-DRIFTED"]})

    with pytest.raises(IdentityEvidenceError, match="does not match the export pinned"):
        replay_from_raw(
            raw_calls,
            receipt=result.receipt,
            config=make_config(
                ("AAPL",),
                identity_export_sha256=make_identity_export(symbols=("AAPL",)).export_sha256,
            ),
            identity_export=drifted,
        )
