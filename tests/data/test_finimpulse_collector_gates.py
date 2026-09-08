"""AAS-DATA-008C round-1 review fixes: gates, evidence capture, cost, atomicity.

Every test here runs against synthetic fixtures with zero provider calls,
zero credential access, and zero cost.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from finimpulse_collector_support import (
    OBSERVED_AT,
    SYNTHETIC_CREDENTIAL,
    RecordingTransport,
    build_response,
    make_config,
    make_gate_evidence,
    make_identity_export,
    no_sleep,
    receipt_view,
    snapshot_items,
    synthetic_pair,
    write_gate_evidence,
    write_identity_export,
)

from aegis_alpha.data.finimpulse_collector import (
    REQUIRED_GB_GATES,
    CollectionResult,
    ContractError,
    GateError,
    IdentityEvidenceError,
    PublicationError,
    build_collection_receipts,
    build_source_registrations,
    collect_snapshot,
    committed_marker_path,
    committed_snapshot_ids,
    is_retryable_status,
    load_committed_manifest,
    load_gate_evidence,
    load_identity_export,
    publish_snapshot,
    read_partition_rows,
    recover_uncommitted_partitions,
    require_gb_gates,
    sha256_hex,
)

HTTP_TOO_MANY_REQUESTS = 429
HTTP_BAD_GATEWAY = 502
HTTP_FORBIDDEN = 403
HTTP_NOT_FOUND = 404


def run(  # noqa: PLR0913 - each argument varies one frozen evidence input
    tmp_path: Path,
    *,
    transport: RecordingTransport | None = None,
    gate_overrides: dict[str, str] | None = None,
    gate_budget: str = "0.10",
    gate_expires: datetime | None = None,
    symbols: tuple[str, ...] = ("AAPL", "PLAB"),
    max_retries: int = 3,
    dataset_root: Path | None = None,
) -> CollectionResult:
    identity = make_identity_export(symbols=symbols)
    gate_evidence = make_gate_evidence(
        tmp_path / "gates",
        symbols=symbols,
        budget_usd=gate_budget,
        decisions=gate_overrides,
        **({} if gate_expires is None else {"expires_at": gate_expires}),
    )
    offline, capability = synthetic_pair(
        RecordingTransport("baseline") if transport is None else transport
    )
    return collect_snapshot(
        snapshot_id="snap-gate-001",
        config=make_config(
            symbols,
            identity_export_sha256=identity.export_sha256,
            identity_as_of=identity.as_of_utc,
            max_retries=max_retries,
        ),
        credential=SYNTHETIC_CREDENTIAL,
        transport=offline,
        raw_store_root=tmp_path / "raw",
        dataset_root=(tmp_path / "dataset") if dataset_root is None else dataset_root,
        identity_export=identity,
        gate_evidence=gate_evidence,
        observed_at=OBSERVED_AT,
        sleeper=no_sleep,
        synthetic=capability,
    )


# AAS008C-R1-F1 — G-B owner gates


@pytest.mark.parametrize("blocked_gate", REQUIRED_GB_GATES)
def test_any_non_positive_gate_blocks_with_zero_calls(
    blocked_gate: str,
    tmp_path: Path,
) -> None:
    transport = RecordingTransport("baseline")

    with pytest.raises(GateError):
        run(tmp_path, transport=transport, gate_overrides={blocked_gate: "PENDING"})

    assert transport.calls == []
    assert not list(tmp_path.rglob("*.parquet"))
    assert not (tmp_path / "raw").exists()


def test_missing_gate_decision_blocks_the_run(tmp_path: Path) -> None:
    document = json.loads((write_gate_evidence(tmp_path / "gates")[0]).read_text(encoding="utf-8"))
    document["decisions"] = [
        entry for entry in document["decisions"] if entry["gate"] != "purge_capability_available"
    ]
    path = tmp_path / "partial.json"
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(payload)

    evidence = load_gate_evidence(path, expected_sha256=sha256_hex(payload))

    with pytest.raises(GateError, match="are not decided"):
        require_gb_gates(evidence, config=make_config(), now=OBSERVED_AT)


def test_stale_gate_authorization_blocks_with_zero_calls(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline")

    with pytest.raises(GateError, match="expired"):
        run(
            tmp_path,
            transport=transport,
            gate_expires=datetime(2026, 1, 1, tzinfo=UTC),
        )

    assert transport.calls == []


def test_gate_artifact_tampering_is_detected(tmp_path: Path) -> None:
    path, digest = write_gate_evidence(tmp_path / "gates")
    path.write_bytes(path.read_bytes().replace(b"PERMITTED", b"REJECTED "))

    with pytest.raises(GateError, match="does not match the authorized digest"):
        load_gate_evidence(path, expected_sha256=digest)


def test_absent_gate_artifact_is_fatal(tmp_path: Path) -> None:
    with pytest.raises(GateError, match="is absent"):
        load_gate_evidence(tmp_path / "missing.json", expected_sha256="0" * 64)


def test_budget_above_the_approved_gate_budget_blocks(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline")

    with pytest.raises(GateError, match="approved bounded-validation budget"):
        run(tmp_path, transport=transport, gate_budget="0.01")

    assert transport.calls == []


def test_gate_authorization_must_cover_the_exact_universe(tmp_path: Path) -> None:
    identity = make_identity_export(symbols=("AAPL",))
    gate_evidence = make_gate_evidence(tmp_path / "gates", symbols=("MSFT",))
    transport = RecordingTransport("baseline")
    offline, capability = synthetic_pair(transport)

    with pytest.raises(GateError, match="does not cover this universe"):
        collect_snapshot(
            snapshot_id="snap-gate-002",
            config=make_config(
                ("AAPL",),
                identity_export_sha256=identity.export_sha256,
            ),
            credential=SYNTHETIC_CREDENTIAL,
            transport=offline,
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "dataset",
            identity_export=identity,
            gate_evidence=gate_evidence,
            observed_at=OBSERVED_AT,
            sleeper=no_sleep,
            synthetic=capability,
        )

    assert transport.calls == []


def test_receipt_records_the_gate_artifact_digest(tmp_path: Path) -> None:
    result = run(tmp_path)

    gates = receipt_view(result.receipt)["gb_gate_evidence"]
    assert len(gates["artifact_sha256"]) == 64  # noqa: PLR2004 - SHA-256 hex length
    assert {entry["gate"] for entry in gates["decisions"]} == set(REQUIRED_GB_GATES)


# AAS008C-R1-F2 — capture every body before parsing


def test_error_response_body_is_captured_as_blocked_evidence(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline", error_statuses={"PLAB": [HTTP_NOT_FOUND]})

    result = run(tmp_path, transport=transport)

    assert result.published is False
    blobs = list((tmp_path / "raw" / "blobs" / "sha256").rglob("*.raw"))
    assert blobs
    provenance = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (tmp_path / "raw" / "snapshots").glob("*.json")
    ]
    blocked = [entry for entry in provenance if entry["validation_status"] == "BLOCKED"]
    assert blocked, "the non-200 body must be captured as BLOCKED evidence"


def test_invalid_response_is_captured_yet_never_published(tmp_path: Path) -> None:
    poisoned = build_response("AAPL", snapshot_items("baseline", "AAPL"), cost=0.0014)
    poisoned["result"]["symbol"] = "MSFT"
    transport = RecordingTransport("baseline", overrides={"AAPL": poisoned})

    with pytest.raises(ContractError):
        run(tmp_path, transport=transport)

    provenance = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (tmp_path / "raw" / "snapshots").glob("*.json")
    ]
    assert any(entry["validation_status"] == "BLOCKED" for entry in provenance)
    assert not list((tmp_path / "dataset").rglob("*.parquet"))


def test_receipt_lists_every_transmission_capture(tmp_path: Path) -> None:
    transport = RecordingTransport(
        "baseline",
        error_statuses={"PLAB": [HTTP_BAD_GATEWAY]},
    )

    result = run(tmp_path, transport=transport)

    calls = {entry["symbol"]: entry for entry in receipt_view(result.receipt)["calls"]}
    assert len(calls["PLAB"]["transmission_snapshot_ids"]) >= 2  # noqa: PLR2004 - retry then success
    expected = {
        snapshot_id
        for outcome in result.outcomes
        for snapshot_id in outcome.transmission_snapshot_ids
    }
    registrations = build_source_registrations(result, make_config())
    receipts = build_collection_receipts("run-transmissions", result)
    assert {registration.snapshot.snapshot_id for registration in registrations} == expected
    assert {receipt.source_snapshot_id for receipt in receipts} == expected


def test_identical_retry_bodies_keep_distinct_attempt_identities(tmp_path: Path) -> None:
    transport = RecordingTransport(
        "baseline",
        error_statuses={"PLAB": [HTTP_BAD_GATEWAY, HTTP_BAD_GATEWAY]},
    )

    result = run(tmp_path, transport=transport)

    plab = next(outcome for outcome in result.outcomes if outcome.symbol == "PLAB")
    assert plab.attempts == 3  # noqa: PLR2004 - two failures then success
    assert len(plab.transmission_snapshot_ids) == plab.attempts
    assert len(set(plab.transmission_snapshot_ids)) == plab.attempts
    assert all(
        f"tx-{attempt:04d}-" in snapshot_id
        for attempt, snapshot_id in enumerate(plab.transmission_snapshot_ids, start=1)
    )
    receipts = build_collection_receipts("run-identical-retries", result)
    assert len(receipts) == sum(outcome.attempts for outcome in result.outcomes)


# AAS008C-R1-F3 — per-transmission cost and retry policy


def test_only_rate_limit_and_server_errors_are_retryable() -> None:
    assert is_retryable_status(HTTP_TOO_MANY_REQUESTS) is True
    assert is_retryable_status(HTTP_BAD_GATEWAY) is True
    assert is_retryable_status(HTTP_FORBIDDEN) is False
    assert is_retryable_status(HTTP_NOT_FOUND) is False


def test_non_retryable_status_is_terminal_after_one_transmission(tmp_path: Path) -> None:
    transport = RecordingTransport(
        "baseline",
        error_statuses={"AAPL": [HTTP_FORBIDDEN, HTTP_FORBIDDEN]},
    )

    result = run(tmp_path, transport=transport)

    # AAPL is attempted exactly once: a 403 is deterministic, so retrying it
    # would spend the limit with no chance of a different outcome.
    assert transport.calls.count("AAPL") == 1
    failed = next(outcome for outcome in result.outcomes if outcome.symbol == "AAPL")
    assert failed.attempts == 1
    assert failed.error_class == "provider_rejected"
    assert result.published is False


def test_every_retry_transmission_consumes_its_own_reservation(tmp_path: Path) -> None:
    """A retried call reserves each transmission, so retries cannot overrun."""

    transport = RecordingTransport("baseline", failures={"AAPL": 2})

    result = run(tmp_path, transport=transport)

    outcome = next(item for item in result.outcomes if item.symbol == "AAPL")
    assert outcome.attempts == 3  # noqa: PLR2004 - two retried 429s then success
    assert outcome.reserved_usd == 3 * make_config().call_reservation_usd


def test_retries_cannot_exceed_the_client_side_limit(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline", failures={"AAPL": 99})

    result = run(tmp_path, transport=transport, symbols=("AAPL",), max_retries=99)

    assert result.published is False
    cost = receipt_view(result.receipt)["cost"]
    assert Decimal(str(cost["reserved_usd"])) <= Decimal(str(cost["client_side_limit_usd"]))


def test_uncharged_transport_failure_releases_its_reservation(tmp_path: Path) -> None:
    """A request that never reached the provider is proven uncharged."""

    transport = RecordingTransport("baseline", transport_errors={"AAPL": 1})

    result = run(tmp_path, transport=transport)

    outcome = next(item for item in result.outcomes if item.symbol == "AAPL")
    assert outcome.completed is True
    assert outcome.reserved_usd == make_config().call_reservation_usd


# AAS008C-R1-F4 — atomic publication


def test_failed_second_partition_publishes_no_partition_at_all(tmp_path: Path) -> None:
    result = run(tmp_path)
    assert result.manifest is not None
    rows = list(result.rows)
    dataset_root = tmp_path / "atomic"
    blocker = dataset_root / "estimate_type=eps_revisions"
    blocker.parent.mkdir(parents=True, exist_ok=True)
    blocker.write_bytes(b"not a directory")

    with pytest.raises(OSError):  # noqa: PT011 - any link failure must roll back
        publish_snapshot(
            dataset_root,
            rows,
            snapshot_id="snap-atomic-001",
            observed_at=OBSERVED_AT,
            config=make_config(),
        )

    published = [path for path in dataset_root.rglob("*.parquet") if ".staging" not in path.parts]
    assert published == []


def test_staging_directory_is_discarded_after_publication(tmp_path: Path) -> None:
    result = run(tmp_path)

    assert result.published is True
    assert not (tmp_path / "dataset" / ".staging").exists()


def test_committed_marker_makes_the_snapshot_discoverable(tmp_path: Path) -> None:
    result = run(tmp_path)

    assert result.manifest is not None
    assert committed_snapshot_ids(tmp_path / "dataset") == ("snap-gate-001",)
    manifest = load_committed_manifest(tmp_path / "dataset", "snap-gate-001")
    assert manifest is not None
    assert manifest["logical_content_sha256"] == result.manifest.logical_content_sha256


def test_a_crash_before_the_marker_leaves_an_invisible_snapshot(tmp_path: Path) -> None:
    """Simulate a crash between partition commit and the atomic marker rename."""

    result = run(tmp_path)
    assert result.manifest is not None
    committed_marker_path(tmp_path / "dataset", "snap-gate-001").unlink()

    # Readers honour the marker, so the snapshot is not discoverable at all.
    assert committed_snapshot_ids(tmp_path / "dataset") == ()
    assert load_committed_manifest(tmp_path / "dataset", "snap-gate-001") is None
    with pytest.raises(PublicationError, match="not committed and must not be read"):
        read_partition_rows(tmp_path / "dataset", result.manifest)


def test_crash_recovery_removes_uncommitted_partition_debris(tmp_path: Path) -> None:
    result = run(tmp_path)
    assert result.manifest is not None
    committed_marker_path(tmp_path / "dataset", "snap-gate-001").unlink()

    removed = recover_uncommitted_partitions(tmp_path / "dataset", "snap-gate-001")

    assert removed
    assert not [
        path for path in (tmp_path / "dataset").rglob("*.parquet") if ".staging" not in path.parts
    ]


def test_recovery_never_touches_a_committed_snapshot(tmp_path: Path) -> None:
    result = run(tmp_path)
    assert result.manifest is not None

    removed = recover_uncommitted_partitions(tmp_path / "dataset", "snap-gate-001")

    assert removed == ()
    assert read_partition_rows(tmp_path / "dataset", result.manifest)


# AAS008C-R2-F2 — capture strictly precedes validation


def test_valid_200_body_is_captured_before_it_is_parsed(tmp_path: Path) -> None:
    """The accepted body exists as evidence under its transmission key too."""

    run(tmp_path, symbols=("AAPL",))

    provenance = {
        path.stem: json.loads(path.read_text(encoding="utf-8"))
        for path in (tmp_path / "raw" / "snapshots").glob("*.json")
    }
    transmission_keys = [key for key in provenance if ".tx-" in key]
    assert transmission_keys, "a 200 body must be captured before validation"
    assert all(provenance[key]["validation_status"] == "PASS" for key in transmission_keys)


def test_credential_reflecting_body_is_captured_without_disclosure(tmp_path: Path) -> None:
    """A reflected credential fails closed after capture and is never echoed."""

    reflected = build_response("AAPL", snapshot_items("baseline", "AAPL"), cost=0.0014)
    reflected["result"]["items"][0]["date_type"] = "quarter"
    reflected["echo"] = SYNTHETIC_CREDENTIAL
    transport = RecordingTransport("baseline", overrides={"AAPL": reflected})

    with pytest.raises(ContractError) as failure:
        run(tmp_path, transport=transport, symbols=("AAPL",))

    assert "reflected credential material" in str(failure.value)
    assert SYNTHETIC_CREDENTIAL not in str(failure.value)
    provenance = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (tmp_path / "raw" / "snapshots").glob("*.json")
    ]
    assert provenance, "the reflecting body must still be captured as evidence"
    assert all(entry["validation_status"] == "BLOCKED" for entry in provenance)
    assert not list((tmp_path / "dataset").rglob("*.parquet"))


# AAS008C-R2-F3 — charged non-200 reconciliation


def test_charged_over_reservation_429_halts_before_retry(tmp_path: Path) -> None:
    """A charged 429 above its reservation stops the run before retrying."""

    transport = RecordingTransport(
        "baseline",
        error_statuses={"AAPL": [HTTP_TOO_MANY_REQUESTS]},
        error_cost=0.05,
    )

    result = run(tmp_path, transport=transport, symbols=("AAPL",))

    assert transport.calls == ["AAPL"]
    assert result.published is False
    receipt = receipt_view(result.receipt)
    assert "exceeded its reservation" in str(receipt["publication"]["halt_reason"])
    assert Decimal(str(receipt["cost"]["provider_reported_total_usd"])) == Decimal("0.05")


def test_charged_non200_is_counted_in_receipt_totals(tmp_path: Path) -> None:
    transport = RecordingTransport(
        "baseline",
        error_statuses={"AAPL": [HTTP_BAD_GATEWAY]},
        error_cost=0.0005,
    )

    result = run(tmp_path, transport=transport, symbols=("AAPL",), max_retries=0)

    receipt = receipt_view(result.receipt)
    assert Decimal(str(receipt["cost"]["provider_reported_total_usd"])) == Decimal("0.0005")
    assert result.published is False


# AAS008C-R1-F6 — immutable identity export


def test_identity_export_tampering_is_detected(tmp_path: Path) -> None:
    path, digest = write_identity_export(tmp_path / "evidence", {"AAPL": ["INST-AAPL"]})
    path.write_bytes(path.read_bytes().replace(b"INST-AAPL", b"INST-OTHER"))

    with pytest.raises(IdentityEvidenceError, match="does not match the authorized digest"):
        load_identity_export(path, expected_sha256=digest)


def test_absent_identity_export_is_fatal(tmp_path: Path) -> None:
    with pytest.raises(IdentityEvidenceError, match="is absent"):
        load_identity_export(tmp_path / "missing.json", expected_sha256="0" * 64)


def test_collection_rejects_an_export_that_was_not_pinned(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline")
    offline, capability = synthetic_pair(transport)
    identity = make_identity_export({"AAPL": ["INST-AAPL"], "PLAB": []})

    with pytest.raises(IdentityEvidenceError, match="does not match the supplied"):
        collect_snapshot(
            snapshot_id="snap-identity-001",
            config=make_config(identity_export_sha256="b" * 64),
            credential=SYNTHETIC_CREDENTIAL,
            transport=offline,
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "dataset",
            identity_export=identity,
            gate_evidence=make_gate_evidence(tmp_path / "gates"),
            observed_at=OBSERVED_AT,
            sleeper=no_sleep,
            synthetic=capability,
        )

    assert transport.calls == []
