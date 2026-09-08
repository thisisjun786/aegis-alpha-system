"""AAS-DATA-008C collector contract, capture, budget, and publication tests.

G-A discipline: every test runs against committed synthetic fixtures with zero
provider calls, zero credential access, and zero cost.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Final

import pyarrow.parquet as pq
import pytest
from finimpulse_authority_support import AUTHORITY_NOW, make_standing_authority, write_revocation
from finimpulse_collector_support import (
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
    ESTIMATE_TYPES,
    NORMALIZED_SCHEMA,
    SCHEMA_ID,
    SCHEMA_VERSION,
    BudgetError,
    BudgetLedger,
    CollectionResult,
    CollectorConfig,
    CollectorError,
    ContractError,
    IdentityExport,
    PublicationError,
    RawCall,
    Transport,
    build_collection_receipts,
    build_run,
    build_run_events,
    build_run_plan,
    build_usage_records,
    classify_http_error,
    collect_snapshot,
    logical_content_hash,
    parse_response,
    publish_snapshot,
    read_partition_rows,
    retry_delay_seconds,
    sha256_hex,
    validate_receipt,
    validate_symbols,
)
from aegis_alpha.data.finimpulse_recurring_authority import (
    RecurringAuthorityError,
    VerifiedRecurringAuthority,
    verify_recurring_authority,
)

_PACED_CALLS_PER_MINUTE: Final = 30
EXPECTED_BASELINE_ROWS = 6
EXPECTED_AAPL_ROWS = 4
HTTP_TOO_MANY_REQUESTS = 429
HTTP_BAD_GATEWAY = 502
HTTP_NOT_FOUND = 404


def run_baseline(  # noqa: PLR0913 - each argument varies one synthetic run input
    tmp_path: Path,
    *,
    config: CollectorConfig | None = None,
    transport: Transport | None = None,
    snapshot_id: str = "snap-001",
    identity: IdentityExport | None = None,
    dataset_root: Path | None = None,
) -> CollectionResult:
    resolved_identity = (
        make_identity_export(symbols=("AAPL", "PLAB")) if identity is None else identity
    )
    resolved_config = (
        make_config(
            identity_export_sha256=resolved_identity.export_sha256,
            identity_as_of=resolved_identity.as_of_utc,
        )
        if config is None
        else config
    )
    offline, capability = synthetic_pair(
        RecordingTransport("baseline") if transport is None else transport
    )
    return collect_snapshot(
        snapshot_id=snapshot_id,
        config=resolved_config,
        credential=SYNTHETIC_CREDENTIAL,
        transport=offline,
        raw_store_root=tmp_path / "raw",
        dataset_root=(tmp_path / "dataset") if dataset_root is None else dataset_root,
        identity_export=(
            make_identity_export(symbols=resolved_config.universe) if identity is None else identity
        ),
        gate_evidence=make_gate_evidence(
            tmp_path / "gates",
            symbols=resolved_config.universe,
            budget_usd=format(resolved_config.budget_usd, "f"),
        ),
        observed_at=OBSERVED_AT,
        sleeper=no_sleep,
        synthetic=capability,
    )


def test_completed_run_publishes_versioned_partitions_and_receipt(tmp_path: Path) -> None:
    identity = make_identity_export({"AAPL": ["INST-AAPL"], "PLAB": ["INST-PLAB"]})

    result = run_baseline(tmp_path, identity=identity)

    assert result.published is True
    assert result.manifest is not None
    assert len(result.rows) == EXPECTED_BASELINE_ROWS
    assert {partition.estimate_type for partition in result.manifest.partitions} == set(
        ESTIMATE_TYPES
    )
    for partition in result.manifest.partitions:
        assert partition.relative_path.startswith(f"estimate_type={partition.estimate_type}/")
        assert "/year=2026/month=07/snapshot_id=snap-001/" in partition.relative_path
        assert (tmp_path / "dataset" / partition.relative_path).is_file()
    assert result.manifest.dataset_version.startswith(f"{SCHEMA_ID}.v{SCHEMA_VERSION}.")
    validate_receipt(result.receipt)


def test_published_partitions_match_the_frozen_normalized_schema(tmp_path: Path) -> None:
    result = run_baseline(
        tmp_path, identity=make_identity_export({"AAPL": ["INST-AAPL"], "PLAB": []})
    )

    assert result.manifest is not None
    for partition in result.manifest.partitions:
        table = pq.read_table(tmp_path / "dataset" / partition.relative_path)
        assert table.schema.names == NORMALIZED_SCHEMA.names
        assert table.schema.types == NORMALIZED_SCHEMA.types


def test_raw_bytes_are_captured_content_addressed_before_parsing(tmp_path: Path) -> None:
    result = run_baseline(tmp_path)

    blobs = sorted((tmp_path / "raw" / "blobs" / "sha256").rglob("*.raw"))
    assert len(blobs) == len(result.outcomes)
    for outcome in result.outcomes:
        assert outcome.content_sha256 is not None
        stored = tmp_path / "raw" / "blobs" / "sha256" / outcome.content_sha256[:2]
        assert (stored / f"{outcome.content_sha256}.raw").is_file()
        assert outcome.raw_snapshot_id is not None
        assert outcome.raw_snapshot_id.startswith(f"snap-001.{outcome.symbol}.tx-0001-")
        assert (tmp_path / "raw" / "snapshots" / f"snap-001.{outcome.symbol}.json").is_file()


def test_second_snapshot_appends_without_rewriting_prior_partitions(tmp_path: Path) -> None:
    first = run_baseline(tmp_path, snapshot_id="snap-001")
    assert first.manifest is not None
    first_bytes = {
        partition.relative_path: (tmp_path / "dataset" / partition.relative_path).read_bytes()
        for partition in first.manifest.partitions
    }

    second = run_baseline(
        tmp_path,
        snapshot_id="snap-002",
        transport=RecordingTransport("baseline"),
    )

    assert second.manifest is not None
    second_paths = {partition.relative_path for partition in second.manifest.partitions}
    assert second_paths.isdisjoint(first_bytes)
    for relative_path, payload in first_bytes.items():
        assert (tmp_path / "dataset" / relative_path).read_bytes() == payload


def test_publishing_the_same_snapshot_partition_twice_is_refused(tmp_path: Path) -> None:
    result = run_baseline(tmp_path)

    with pytest.raises(PublicationError, match="refusing to clobber"):
        publish_snapshot(
            tmp_path / "dataset",
            list(result.rows),
            snapshot_id="snap-001",
            observed_at=OBSERVED_AT,
            config=make_config(),
        )


def test_partial_run_publishes_nothing_and_records_the_failure(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline", failures={"PLAB": 99})

    result = run_baseline(tmp_path, transport=transport)

    assert result.published is False
    assert result.manifest is None
    assert not list((tmp_path / "dataset").rglob("*.parquet"))
    failed = [outcome for outcome in result.outcomes if not outcome.completed]
    assert [outcome.symbol for outcome in failed] == ["PLAB"]
    assert failed[0].error_class == "rate_limited"
    assert receipt_view(result.receipt)["publication"]["published"] is False


def test_retry_succeeds_within_the_bounded_policy(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline", failures={"PLAB": 2})

    result = run_baseline(tmp_path, transport=transport)

    assert result.published is True
    attempts = {outcome.symbol: outcome.attempts for outcome in result.outcomes}
    assert attempts["PLAB"] == 3  # noqa: PLR2004 - two synthetic 429s then success


def test_retry_delay_is_bounded_and_jittered() -> None:
    config = make_config()

    delays = [retry_delay_seconds(attempt, config) for attempt in range(1, 4)]

    assert all(
        0 < delay <= config.backoff_seconds * 2 ** (index) for index, delay in enumerate(delays)
    )
    with pytest.raises(ValueError, match="attempt must be positive"):
        retry_delay_seconds(0, config)


def test_http_error_classes_separate_rate_limit_and_availability() -> None:
    assert classify_http_error(HTTP_TOO_MANY_REQUESTS) == "rate_limited"
    assert classify_http_error(HTTP_BAD_GATEWAY) == "provider_unavailable"
    assert classify_http_error(HTTP_NOT_FOUND) == "provider_rejected"


def test_preflight_insufficient_limit_makes_zero_calls(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline")
    config = make_config(budget_usd="0.001")

    with pytest.raises(BudgetError, match="zero provider calls"):
        run_baseline(tmp_path, config=config, transport=transport)

    assert transport.calls == []
    assert not list(tmp_path.rglob("*.parquet"))


def test_midrun_reservation_shortfall_never_reserves_the_unfundable_call() -> None:
    """A call whose reservation exceeds the remaining limit is never funded.

    Preflight already refuses an underfunded universe, so this guard is the
    mid-run defense that must hold even if remaining capacity is consumed.
    """

    config = make_config(("AAPL", "PLAB"), budget_usd="0.0075")
    ledger = BudgetLedger(config)

    first = ledger.reserve("AAPL")
    with pytest.raises(BudgetError, match="the call was never made"):
        ledger.reserve("PLAB")

    assert first == config.call_reservation_usd
    assert ledger.remaining_usd < config.call_reservation_usd


def test_underfunded_universe_is_refused_before_any_call(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline")
    config = make_config(("AAPL", "PLAB"), budget_usd="0.0075")

    with pytest.raises(BudgetError, match="zero provider calls"):
        run_baseline(tmp_path, config=config, transport=transport)

    assert transport.calls == []


def test_charge_above_its_reservation_halts_before_any_further_call(tmp_path: Path) -> None:
    over_charged = build_response(
        "AAPL",
        snapshot_items("baseline", "AAPL"),
        cost=0.05,
    )
    transport = RecordingTransport("baseline", overrides={"AAPL": over_charged})

    result = run_baseline(tmp_path, transport=transport)

    assert transport.calls == ["AAPL"]
    assert result.published is False
    assert "exceeded its reservation" in str(
        receipt_view(result.receipt)["publication"]["halt_reason"]
    )


def test_rows_beyond_the_requested_limit_are_rejected(tmp_path: Path) -> None:
    """Cost stays inside the reservation so the row-count breach is isolated."""

    items = snapshot_items("baseline", "AAPL")
    identity = make_identity_export(symbols=("AAPL",))
    transport = RecordingTransport(
        "baseline",
        limit=1,
        overrides={"AAPL": build_response("AAPL", items, cost=0.0002, limit=1)},
    )
    config = make_config(
        ("AAPL",),
        page_limit=1,
        identity_export_sha256=identity.export_sha256,
    )

    with pytest.raises(ContractError, match="more rows than the requested limit"):
        run_baseline(tmp_path, config=config, transport=transport, identity=identity)


def test_receipt_reconciles_reserved_against_actual_cost(tmp_path: Path) -> None:
    result = run_baseline(tmp_path)

    cost = receipt_view(result.receipt)["cost"]
    assert cost["reserved_vs_actual_reconciled"] is True
    assert cost["within_client_side_limit"] is True
    assert cost["provider_side_account_cap"] == "NOT_OBSERVED"
    assert cost["maximum_loss_bound"] == "UNKNOWN_WITHOUT_ACCOUNT_CAP_EVIDENCE"
    assert Decimal(str(cost["provider_reported_total_usd"])) == Decimal("0.0025")


def test_budget_ledger_release_restores_capacity() -> None:
    ledger = BudgetLedger(make_config(("AAPL",), budget_usd="0.01"))
    reservation = ledger.reserve("AAPL")

    ledger.release(reservation)

    assert ledger.remaining_usd == Decimal("0.01")


def test_credential_like_response_fields_are_never_persisted(tmp_path: Path) -> None:
    poisoned = build_response("AAPL", snapshot_items("baseline", "AAPL"), cost=0.0014)
    poisoned["api_key"] = "leaked"
    transport = RecordingTransport("baseline", overrides={"AAPL": poisoned})

    with pytest.raises(ContractError, match="credential-like field"):
        run_baseline(tmp_path, transport=transport)


def test_response_must_echo_the_request_projection() -> None:
    response = build_response("AAPL", snapshot_items("baseline", "AAPL"), cost=0.0014)
    response["data"]["symbol"] = "MSFT"

    with pytest.raises(ContractError, match="does not echo the request"):
        parse_response(build_raw_call("AAPL", response))


def test_duplicate_record_identity_in_one_response_is_rejected() -> None:
    items = snapshot_items("baseline", "AAPL")
    response = build_response("AAPL", [items[0], items[0]], cost=0.0014)

    with pytest.raises(ContractError, match="duplicate record identity"):
        parse_response(build_raw_call("AAPL", response))


def test_unknown_record_field_is_rejected() -> None:
    items = snapshot_items("baseline", "AAPL")
    items[0]["unexpected"] = 1
    response = build_response("AAPL", items, cost=0.0014)

    with pytest.raises(ContractError, match="unexpected eps_trend record fields"):
        parse_response(build_raw_call("AAPL", response))


def test_negative_provider_cost_is_rejected() -> None:
    response = build_response("AAPL", snapshot_items("baseline", "AAPL"), cost=-1.0)

    with pytest.raises(ContractError, match="finite nonnegative amount"):
        parse_response(build_raw_call("AAPL", response))


def test_symbol_validation_rejects_duplicate_and_malformed_tickers() -> None:
    assert validate_symbols(["aapl"]) == ("AAPL",)
    with pytest.raises(Exception, match="unique"):
        validate_symbols(["AAPL", "AAPL"])
    with pytest.raises(Exception, match="credential-free ticker syntax"):
        validate_symbols(["not a ticker"])


def test_receipt_validation_rejects_nonzero_eligibility(tmp_path: Path) -> None:
    result = run_baseline(tmp_path)
    tampered = receipt_view(result.receipt)
    tampered["eligibility"]["backtest"] = True

    with pytest.raises(ContractError, match="zero eligibility"):
        validate_receipt(tampered)


def test_receipt_records_blocked_purge_capability(tmp_path: Path) -> None:
    result = run_baseline(tmp_path)

    receipt = receipt_view(result.receipt)
    assert receipt["purge_capability"]["status"] == "BLOCKED"
    assert receipt["rate_limit"]["semantics_status"] == "NOT_RECONFIRMED"
    assert receipt["rate_limit"]["throttle_configured"] is False


def test_logical_hash_ignores_row_order_but_not_content(tmp_path: Path) -> None:
    result = run_baseline(tmp_path)
    rows = list(result.rows)

    assert logical_content_hash(rows) == logical_content_hash(list(reversed(rows)))

    mutated = [dict(row) for row in rows]
    mutated[0]["current"] = 99.0
    assert logical_content_hash(mutated) != logical_content_hash(rows)


def test_published_rows_read_back_with_the_manifest_logical_hash(tmp_path: Path) -> None:
    result = run_baseline(
        tmp_path, identity=make_identity_export({"AAPL": ["INST-AAPL"], "PLAB": []})
    )

    assert result.manifest is not None
    rows = read_partition_rows(tmp_path / "dataset", result.manifest)
    assert logical_content_hash(rows) == result.manifest.logical_content_sha256


def test_manifest_partition_hashes_match_the_written_files(tmp_path: Path) -> None:
    result = run_baseline(tmp_path)

    assert result.manifest is not None
    for partition in result.manifest.partitions:
        payload = (tmp_path / "dataset" / partition.relative_path).read_bytes()
        assert partition.content_sha256 == sha256_hex(payload)


def test_collection_control_plane_projections_are_built_for_the_run(tmp_path: Path) -> None:
    result = run_baseline(tmp_path)

    plan = build_run_plan("plan-008c-001", make_config(), created_at_utc=OBSERVED_AT)
    run = build_run("run-008c-001", plan.plan_id, created_at_utc=OBSERVED_AT)
    events = build_run_events(run.run_id, result)
    receipts = build_collection_receipts(run.run_id, result)
    usage = build_usage_records(run.run_id, result, recorded_at_utc=OBSERVED_AT)

    assert plan.mode.value == "probe"
    assert plan.provider == "finimpulse"
    assert events[-1].event_type.value == "run_succeeded"
    expected_transmissions = {
        snapshot_id
        for outcome in result.outcomes
        for snapshot_id in outcome.transmission_snapshot_ids
    }
    assert {receipt.source_snapshot_id for receipt in receipts} == expected_transmissions
    assert [record.metric for record in usage] == [
        "provider_cost_reserved_usd",
        "provider_cost_actual_usd",
    ]


def test_failed_run_projects_a_terminal_failure_event(tmp_path: Path) -> None:
    result = run_baseline(tmp_path, transport=RecordingTransport("baseline", failures={"PLAB": 99}))

    events = build_run_events("run-008c-002", result)

    assert events[-1].event_type.value == "run_failed"
    assert events[-1].error_class == "partial_run_not_published"


def test_rerunning_a_snapshot_reuses_captured_raw_without_new_calls(tmp_path: Path) -> None:
    """Idempotency: an already-captured symbol is never called or charged again."""

    first = run_baseline(tmp_path)
    assert first.published is True

    replay_transport = RecordingTransport("baseline")
    second = run_baseline(
        tmp_path,
        transport=replay_transport,
        dataset_root=tmp_path / "dataset-2",
    )

    assert replay_transport.calls == []
    assert second.manifest is not None
    assert first.manifest is not None
    assert second.manifest.logical_content_sha256 == first.manifest.logical_content_sha256
    assert all(outcome.actual_usd == Decimal(0) for outcome in second.outcomes)


def test_partially_captured_snapshot_only_calls_the_missing_symbol(tmp_path: Path) -> None:
    failing = RecordingTransport("baseline", failures={"PLAB": 99})
    first = run_baseline(tmp_path, transport=failing)
    assert first.published is False

    resumed = RecordingTransport("baseline")
    second = run_baseline(tmp_path, transport=resumed)

    assert resumed.calls == ["PLAB"]
    assert second.published is True


def test_missing_credential_is_refused_before_any_capture(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline")

    offline, capability = synthetic_pair(transport)

    with pytest.raises(CollectorError, match="zero provider calls"):
        collect_snapshot(
            snapshot_id="snap-001",
            config=make_config(),
            credential="",
            transport=offline,
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "dataset",
            identity_export=make_identity_export(),
            gate_evidence=make_gate_evidence(tmp_path / "gates"),
            observed_at=OBSERVED_AT,
            sleeper=no_sleep,
            synthetic=capability,
        )

    assert transport.calls == []


class _ControllableClock:
    """Advanceable UTC clock so mid-flight expiry is deterministic."""

    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def __call__(self) -> datetime:
        return self.moment


class _FakeMonotonic:
    """Deterministic limiter clock; tests never sleep for real."""

    def __init__(self) -> None:
        self.seconds = 0.0
        self.waits: list[float] = []

    def time(self) -> float:
        return self.seconds

    def sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.seconds += seconds


def _verified_authority(
    tmp_path: Path, *, calls_per_minute: int = 60
) -> VerifiedRecurringAuthority:
    fixture = make_standing_authority(
        tmp_path / "authority",
        destinations=tmp_path,
        document_changes={"calls_per_minute": calls_per_minute},
    )
    return verify_recurring_authority(
        fixture.payload, fixture.signature, fixture.owner_authority, now=AUTHORITY_NOW
    )


def test_signed_calls_per_minute_paces_every_outbound_transport_call(tmp_path: Path) -> None:
    """A signed sub-60 rate must wait at the request boundary, including retries."""

    transport = RecordingTransport("baseline", failures={"AAPL": 1})
    identity = make_identity_export(symbols=("AAPL", "PLAB"))
    offline, capability = synthetic_pair(transport)
    clock = _FakeMonotonic()
    limiter_waits: list[float] = []

    def sleeper(seconds: float) -> None:
        if seconds >= 1.0:
            limiter_waits.append(seconds)
            clock.sleep(seconds)

    result = collect_snapshot(
        snapshot_id="snap-rate-001",
        config=make_config(
            ("AAPL", "PLAB"),
            identity_export_sha256=identity.export_sha256,
            identity_as_of=identity.as_of_utc,
            max_retries=1,
        ),
        credential=SYNTHETIC_CREDENTIAL,
        transport=offline,
        raw_store_root=tmp_path / "raw",
        dataset_root=tmp_path / "dataset",
        identity_export=identity,
        gate_evidence=make_gate_evidence(tmp_path / "gates"),
        observed_at=OBSERVED_AT,
        sleeper=sleeper,
        synthetic=capability,
        standing_authority=_verified_authority(tmp_path, calls_per_minute=30),
        request_clock=lambda: AUTHORITY_NOW,
        monotonic_clock=clock.time,
    )

    assert result.published is True
    assert transport.calls == ["AAPL", "AAPL", "PLAB"]
    assert limiter_waits == [2.0, 2.0]
    receipt = receipt_view(result.receipt)
    assert receipt["rate_limit"]["throttle_configured"] is True
    assert receipt["standing_authority"]["calls_per_minute"] == _PACED_CALLS_PER_MINUTE


def test_mid_flight_revocation_stops_subsequent_transport_calls(tmp_path: Path) -> None:
    """Authority is revalidated immediately before every outbound call."""

    authority = _verified_authority(tmp_path)
    clock = _ControllableClock(AUTHORITY_NOW)
    identity = make_identity_export(symbols=("AAPL", "PLAB"))
    inner = RecordingTransport("baseline")

    def revoke_after_first(body: Mapping[str, object], credential: str) -> RawCall:
        raw_call = inner(body, credential)
        if len(inner.calls) == 1:
            write_revocation(authority, clock.moment)
            clock.moment = clock.moment + timedelta(seconds=1)
        return raw_call

    offline, capability = synthetic_pair(revoke_after_first)

    with pytest.raises(RecurringAuthorityError, match="revoked"):
        collect_snapshot(
            snapshot_id="snap-revoke-mid-001",
            config=make_config(
                ("AAPL", "PLAB"),
                identity_export_sha256=identity.export_sha256,
                identity_as_of=identity.as_of_utc,
            ),
            credential=SYNTHETIC_CREDENTIAL,
            transport=offline,
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "dataset",
            identity_export=identity,
            gate_evidence=make_gate_evidence(tmp_path / "gates"),
            observed_at=OBSERVED_AT,
            sleeper=no_sleep,
            synthetic=capability,
            standing_authority=authority,
            request_clock=clock,
        )

    assert inner.calls == ["AAPL"]


def test_mid_flight_signing_key_expiry_stops_subsequent_transport_calls(tmp_path: Path) -> None:
    """A run that outlives the signing-key window must not keep calling."""

    authority = _verified_authority(tmp_path)
    clock = _ControllableClock(AUTHORITY_NOW)
    identity = make_identity_export(symbols=("AAPL", "PLAB"))
    inner = RecordingTransport("baseline")

    def expire_after_first(body: Mapping[str, object], credential: str) -> RawCall:
        raw_call = inner(body, credential)
        if len(inner.calls) == 1:
            clock.moment = authority.key_valid_until_utc
        return raw_call

    offline, capability = synthetic_pair(expire_after_first)

    with pytest.raises(RecurringAuthorityError, match="signing key is expired"):
        collect_snapshot(
            snapshot_id="snap-expire-mid-001",
            config=make_config(
                ("AAPL", "PLAB"),
                identity_export_sha256=identity.export_sha256,
                identity_as_of=identity.as_of_utc,
            ),
            credential=SYNTHETIC_CREDENTIAL,
            transport=offline,
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "dataset",
            identity_export=identity,
            gate_evidence=make_gate_evidence(tmp_path / "gates"),
            observed_at=OBSERVED_AT,
            sleeper=no_sleep,
            synthetic=capability,
            standing_authority=authority,
            request_clock=clock,
        )

    assert inner.calls == ["AAPL"]


class _NoOpLimiter:
    """Injected limiter that never waits; must be rejected under standing scope."""

    def __init__(self) -> None:
        self.calls_attempted = 0

    def before_request(self) -> None:
        self.calls_attempted += 1


def test_revocation_during_limiter_wait_aborts_before_transport(tmp_path: Path) -> None:
    """Expiry/revocation after pacing must be observed immediately before transport."""

    authority = _verified_authority(tmp_path, calls_per_minute=30)
    clock = _ControllableClock(AUTHORITY_NOW)
    identity = make_identity_export(symbols=("AAPL", "PLAB"))
    inner = RecordingTransport("baseline")
    revoke_at: list[float] = []

    def revoke_during_wait(seconds: float) -> None:
        del seconds
        write_revocation(authority, clock.moment)
        clock.moment = clock.moment + timedelta(seconds=1)
        revoke_at.append(clock.moment.timestamp())

    offline, capability = synthetic_pair(inner)

    with pytest.raises(RecurringAuthorityError, match="revoked"):
        collect_snapshot(
            snapshot_id="snap-revoke-wait-001",
            config=make_config(
                ("AAPL", "PLAB"),
                identity_export_sha256=identity.export_sha256,
                identity_as_of=identity.as_of_utc,
            ),
            credential=SYNTHETIC_CREDENTIAL,
            transport=offline,
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "dataset",
            identity_export=identity,
            gate_evidence=make_gate_evidence(tmp_path / "gates"),
            observed_at=OBSERVED_AT,
            sleeper=revoke_during_wait,
            synthetic=capability,
            standing_authority=authority,
            request_clock=clock,
        )

    assert inner.calls == ["AAPL"]
    assert revoke_at


def test_injected_rate_limiter_is_rejected_when_standing_authority_is_present(
    tmp_path: Path,
) -> None:
    """Direct callers cannot replace the signed StandingRateLimiter."""

    authority = _verified_authority(tmp_path, calls_per_minute=30)
    identity = make_identity_export(symbols=("AAPL",))
    transport = RecordingTransport("baseline")
    offline, capability = synthetic_pair(transport)
    injected = _NoOpLimiter()

    with pytest.raises(CollectorError, match="cannot replace the standing rate limiter"):
        collect_snapshot(
            snapshot_id="snap-limiter-inject-001",
            config=make_config(
                ("AAPL",),
                identity_export_sha256=identity.export_sha256,
                identity_as_of=identity.as_of_utc,
            ),
            credential=SYNTHETIC_CREDENTIAL,
            transport=offline,
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "dataset",
            identity_export=identity,
            gate_evidence=make_gate_evidence(tmp_path / "gates"),
            observed_at=OBSERVED_AT,
            sleeper=no_sleep,
            synthetic=capability,
            standing_authority=authority,
            request_clock=lambda: AUTHORITY_NOW,
            rate_limiter=injected,
        )

    assert transport.calls == []
    assert injected.calls_attempted == 0


def test_non_synthetic_live_collection_requires_standing_authority(tmp_path: Path) -> None:
    """Direct live collection without standing_authority fails at the core boundary."""

    identity = make_identity_export(symbols=("AAPL",))
    transport = RecordingTransport("baseline")

    with pytest.raises(CollectorError, match="standing authority is required"):
        collect_snapshot(
            snapshot_id="snap-live-no-scope-001",
            config=make_config(
                ("AAPL",),
                identity_export_sha256=identity.export_sha256,
                identity_as_of=identity.as_of_utc,
            ),
            credential=SYNTHETIC_CREDENTIAL,
            transport=transport,
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "dataset",
            identity_export=identity,
            gate_evidence=make_gate_evidence(tmp_path / "gates", symbols=("AAPL",)),
            observed_at=OBSERVED_AT,
            sleeper=no_sleep,
        )

    assert transport.calls == []
    assert not (tmp_path / "raw").exists()


def test_direct_over_budget_call_is_bound_to_signed_max_spend(tmp_path: Path) -> None:
    """config.budget_usd must not exceed the signed max_spend_micros at collect_snapshot."""

    authority = _verified_authority(tmp_path)
    identity = make_identity_export(symbols=("AAPL",))
    transport = RecordingTransport("baseline")
    offline, capability = synthetic_pair(transport)

    with pytest.raises(CollectorError, match="max_spend_micros"):
        collect_snapshot(
            snapshot_id="snap-over-budget-001",
            config=make_config(
                ("AAPL",),
                budget_usd="1.00",
                identity_export_sha256=identity.export_sha256,
                identity_as_of=identity.as_of_utc,
            ),
            credential=SYNTHETIC_CREDENTIAL,
            transport=offline,
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "dataset",
            identity_export=identity,
            gate_evidence=make_gate_evidence(
                tmp_path / "gates",
                symbols=("AAPL",),
                budget_usd="1.00",
            ),
            observed_at=OBSERVED_AT,
            sleeper=no_sleep,
            synthetic=capability,
            standing_authority=authority,
            request_clock=lambda: AUTHORITY_NOW,
        )

    assert transport.calls == []


def test_direct_foreign_root_call_is_bound_to_signed_destinations(tmp_path: Path) -> None:
    """Direct callers cannot send paid work to roots outside the signed destinations."""

    authority = _verified_authority(tmp_path)
    identity = make_identity_export(symbols=("AAPL",))
    transport = RecordingTransport("baseline")
    offline, capability = synthetic_pair(transport)
    foreign = tmp_path / "foreign"

    with pytest.raises(CollectorError, match="do not match the signed standing authority roots"):
        collect_snapshot(
            snapshot_id="snap-foreign-root-001",
            config=make_config(
                ("AAPL",),
                identity_export_sha256=identity.export_sha256,
                identity_as_of=identity.as_of_utc,
            ),
            credential=SYNTHETIC_CREDENTIAL,
            transport=offline,
            raw_store_root=foreign / "raw",
            dataset_root=foreign / "dataset",
            identity_export=identity,
            gate_evidence=make_gate_evidence(tmp_path / "gates", symbols=("AAPL",)),
            observed_at=OBSERVED_AT,
            sleeper=no_sleep,
            synthetic=capability,
            standing_authority=authority,
            request_clock=lambda: AUTHORITY_NOW,
        )

    assert transport.calls == []
    assert not foreign.exists()


def test_standing_grant_is_projected_into_receipt_and_lifecycle_evidence(
    tmp_path: Path,
) -> None:
    """Receipts and 005 projections name the standing grant that authorized paid calls."""

    authority = _verified_authority(tmp_path, calls_per_minute=30)
    identity = make_identity_export(symbols=("AAPL", "PLAB"))
    offline, capability = synthetic_pair(RecordingTransport("baseline"))
    clock = _FakeMonotonic()

    result = collect_snapshot(
        snapshot_id="snap-grant-evidence-001",
        config=make_config(
            ("AAPL", "PLAB"),
            identity_export_sha256=identity.export_sha256,
            identity_as_of=identity.as_of_utc,
        ),
        credential=SYNTHETIC_CREDENTIAL,
        transport=offline,
        raw_store_root=tmp_path / "raw",
        dataset_root=tmp_path / "dataset",
        identity_export=identity,
        gate_evidence=make_gate_evidence(tmp_path / "gates"),
        observed_at=OBSERVED_AT,
        sleeper=clock.sleep,
        synthetic=capability,
        standing_authority=authority,
        request_clock=lambda: AUTHORITY_NOW,
        monotonic_clock=clock.time,
    )

    receipt = receipt_view(result.receipt)
    standing = receipt["standing_authority"]
    assert standing["payload_sha256"] == authority.payload_sha256
    assert standing["signature_sha256"] == authority.signature_sha256
    assert standing["authority_artifact_sha256"] == authority.authority_artifact_sha256
    assert standing["calls_per_minute"] == _PACED_CALLS_PER_MINUTE
    assert standing["max_spend_micros"] == authority.max_spend_micros
    assert receipt["rate_limit"]["throttle_configured"] is True
    plan = build_run_plan(
        "snap-grant-evidence-001.plan",
        make_config(
            ("AAPL", "PLAB"),
            identity_export_sha256=identity.export_sha256,
            identity_as_of=identity.as_of_utc,
        ),
        created_at_utc=OBSERVED_AT,
        result=result,
    )
    assert plan.parameters["standing_authority_payload_sha256"] == authority.payload_sha256
    assert plan.parameters["standing_authority_signature_sha256"] == authority.signature_sha256
    events = build_run_events("snap-grant-evidence-001.run", result)
    assert events[0].details["standing_authority_payload_sha256"] == authority.payload_sha256
    usage = build_usage_records(
        "snap-grant-evidence-001.run",
        result,
        recorded_at_utc=OBSERVED_AT,
    )
    for record in usage:
        assert record.evidence["standing_authority_payload_sha256"] == authority.payload_sha256
        assert record.evidence["standing_authority_signature_sha256"] == authority.signature_sha256
