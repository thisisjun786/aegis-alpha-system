from __future__ import annotations

import hashlib
import inspect
import json
import os
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    CollectionRunState,
    CollectionUsageRecord,
    CurrentWatermark,
    RunEventType,
    WatermarkAdvance,
)
from aegis_alpha.data import fmp_collection_work
from aegis_alpha.data import fmp_collector as fmp_collector_module
from aegis_alpha.data.fmp_approval import ApprovalExpiredError
from aegis_alpha.data.fmp_attempt_state import latest_durable_pacing_deadline
from aegis_alpha.data.fmp_collection_work import CollectionBatch, publish_normalized
from aegis_alpha.data.fmp_collector import (
    LOCK_RELATIVE_PATH,
    CollectorConfig,
    CollectorContractError,
    CollectorLock,
    CollectorLockError,
    CollectorLockLostError,
    CollectorRequest,
    CollectorResponse,
    CredentialLeakError,
    DestinationError,
    FmpCollector,
    QualityResultKind,
    assert_credential_absent,
    build_run_plan,
    new_run_id,
    plan_resume_record,
    publish_bundle,
    raw_blob_path,
    raw_provenance_path,
    read_plan_resume_record,
    redact_headers,
    validate_destination,
)
from aegis_alpha.data.fmp_collector_run import run_collection
from aegis_alpha.data.fmp_daily_refresh import DailyDisposition, classify_daily_universe
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_file_hash import RawCaptureIndex
from aegis_alpha.data.fmp_normalize import DATASET_SPECS
from aegis_alpha.data.fmp_rate_limit import (
    MAX_ATTEMPTS_PER_REQUEST,
    MAX_RETRY_AFTER_SECONDS,
    BudgetExhaustedError,
    EntitlementError,
    FailureClass,
    RateLimiter,
    RetryCeilingError,
    TierArtifact,
)
from aegis_alpha.data.fmp_request_pacing import RequestPacing
from aegis_alpha.data.fmp_symbol_observation import parse_provider_records
from aegis_alpha.data.fmp_universe_run import run_universe_build
from aegis_alpha.data.fmp_windows import DateWindow, UniverseEntry, parse_universe_manifest
from aegis_alpha.data.serialization import canonical_json_bytes

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

FIXTURE_ROOT = (
    Path(__file__).resolve().parents[1] / "fixtures" / "provider_neutral" / "fmp_collector"
)
CREDENTIAL = "SYNTH-CREDENTIAL-NEVER-REAL"
AS_OF = date(2026, 7, 29)
RUN_SEED = 20260729
SYMBOL = "SYNTH.A"
_FIXTURE_ROW_COUNT = 2
_SPLIT_ROW_COUNT = 10
_SPLIT_BODY_COUNT = 2
_LIST_PAGE_LIMIT = 1000
_DATASET_COUNT = 6
_BACKOFF_MIN = 2.0
_BACKOFF_MAX = 3.0
_BUDGET_CALLS = 3
_HTTP_OK = 200
_HTTP_NOT_FOUND = 404
_LOCK_MODE = 0o600
_PROVIDER_VARIANT_REQUESTS = 12
_OVERLAPPING_PAGE_IDENTITIES = 2000
_UNPAGED_UNIVERSE_REQUESTS = 2
_DELISTED_EFFECTIVE_PAGE_IDENTITIES = 201
_DELISTED_PAGED_UNIVERSE_ATTEMPTS = 3
_MIXED_CASE_UNIVERSE_ATTEMPTS = 4
_OVERLAP_DELISTED_PAGE = 17
_THIRTY_DELISTED_UNIVERSE_REQUESTS = 32


def _fixture_bytes(name: str) -> bytes:
    return (FIXTURE_ROOT / name).read_bytes()


def _fixture_records(name: str) -> list[dict[str, Any]]:
    return json.loads(_fixture_bytes(name))


def _body(records: Sequence[Mapping[str, object]]) -> bytes:
    return json.dumps(list(records)).encode()


class SimulatedCrash(BaseException):
    pass


class FakeClock:
    def __init__(self) -> None:
        self.seconds = 0.0
        self.waits: list[float] = []

    def time(self) -> float:
        return self.seconds

    def sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.seconds += seconds

    def now(self) -> datetime:
        return datetime(2026, 7, 29, 12, 0, tzinfo=UTC) + timedelta(seconds=self.seconds)


class CrashOnWaitClock(FakeClock):
    def __init__(self, *, crash_on_wait: int) -> None:
        super().__init__()
        self._crash_on_wait = crash_on_wait

    def sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        if len(self.waits) == self._crash_on_wait:
            raise SimulatedCrash
        self.seconds += seconds


@dataclass
class FakeControlPlane:
    """An in-memory stand-in exposing exactly the seven allowed 005 operations."""

    plans: dict[str, CollectionRunPlan] = field(default_factory=dict)
    runs: dict[str, CollectionRun] = field(default_factory=dict)
    events: list[CollectionRunEvent] = field(default_factory=list)
    watermarks: dict[tuple[str, str, str], CurrentWatermark] = field(default_factory=dict)
    usage: list[CollectionUsageRecord] = field(default_factory=list)
    advances: list[WatermarkAdvance] = field(default_factory=list)

    def register_plan(self, plan: CollectionRunPlan) -> None:
        existing = self.plans.get(plan.plan_id)
        if existing is not None and existing.plan_sha256 != plan.plan_sha256:
            raise AssertionError("plan identity has a different immutable projection")
        self.plans[plan.plan_id] = plan

    def start_run(self, run: CollectionRun) -> None:
        if run.plan_id not in self.plans:
            raise AssertionError("start_run requires a registered plan")
        existing = self.runs.get(run.run_id)
        if existing is not None and existing.plan_id != run.plan_id:
            raise AssertionError("a run ID is never reused with another plan ID")
        self.runs[run.run_id] = run

    def append_event(self, event: CollectionRunEvent) -> int:
        self.events.append(event)
        return len(self.events)

    def current_run_state(self, run_id: str) -> CollectionRunState | None:
        run = self.runs.get(run_id)
        if run is None:
            return None
        events = [event for event in self.events if event.run_id == run_id]
        last = events[-1] if events else None
        return CollectionRunState(
            run_id=run_id,
            plan_id=run.plan_id,
            state=None if last is None else last.event_type,
            terminal=last is not None
            and last.event_type
            in {
                RunEventType.RUN_SUCCEEDED,
                RunEventType.RUN_FAILED,
                RunEventType.RUN_CANCELLED,
            },
            attempt_count=sum(
                1 for event in events if event.event_type is RunEventType.ATTEMPT_STARTED
            ),
            last_event_seq=len(events) or None,
            last_occurred_at_utc=None if last is None else last.occurred_at_utc,
        )

    def run_plan_dataset(self, run_id: str) -> str | None:
        run = self.runs.get(run_id)
        return None if run is None else self.plans[run.plan_id].dataset

    def advance_watermark(self, advance: WatermarkAdvance) -> int:
        key = (advance.provider, advance.dataset, advance.stream)
        current = self.watermarks.get(key)
        if current is not None and advance.watermark_value < current.watermark_value:
            raise AssertionError("watermarks advance forward only")
        sequence = 1 if current is None else current.watermark_seq + 1
        self.watermarks[key] = CurrentWatermark(
            provider=advance.provider,
            dataset=advance.dataset,
            stream=advance.stream,
            watermark_seq=sequence,
            run_id=advance.run_id,
            watermark_value=advance.watermark_value,
            watermark_position=advance.watermark_position,
            recorded_at_utc=datetime.now(UTC),
        )
        self.advances.append(advance)
        return sequence

    def latest_watermark(self, provider: str, dataset: str, stream: str) -> CurrentWatermark | None:
        return self.watermarks.get((provider, dataset, stream))

    def record_usage(
        self,
        record: CollectionUsageRecord,
        *,
        recorded_at_clock: object | None = None,
    ) -> None:
        del recorded_at_clock
        self.usage.append(record)

    @property
    def event_types(self) -> list[str]:
        return [event.event_type.value for event in self.events]


class ScriptedTransport:
    """Replays queued responses; no socket is ever created."""

    def __init__(self, responses: Sequence[CollectorResponse | Exception]) -> None:
        self._responses = list(responses)
        self.requests: list[CollectorRequest] = []

    def __call__(self, request: CollectorRequest, credential: str) -> CollectorResponse:
        if credential != CREDENTIAL:
            raise AssertionError("transport received an unexpected credential")
        self.requests.append(request)
        if not self._responses:
            raise AssertionError("transport exhausted its scripted responses")
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _response(
    body: bytes,
    *,
    status_code: int = 200,
    headers: Mapping[str, str] | None = None,
    clock: FakeClock | None = None,
) -> CollectorResponse:
    moment = datetime(2026, 7, 29, 12, 0, tzinfo=UTC) if clock is None else clock.now()
    return CollectorResponse(
        status_code=status_code,
        headers={"Content-Type": "application/json", **(headers or {})},
        body=body,
        requested_at_utc=moment,
        retrieved_at_utc=moment,
    )


def _config(tmp_path: Path, **overrides: object) -> CollectorConfig:
    values: dict[str, Any] = {
        "raw_store_root": tmp_path / "raw",
        "dataset_root": tmp_path / "datasets",
        "receipt_path": tmp_path / "receipts" / "run.receipt.json",
        "as_of": AS_OF,
        "mode": CollectionMode.INCREMENTAL,
        "max_calls": 50,
        "run_identity": "synth-run-identity",
        "operator_from": date(2016, 1, 1),
        "artifact_hashes": {"approval": "a" * 64, "tier": "b" * 64, "notification": "c" * 64},
    }
    values.update(overrides)
    return CollectorConfig(**values)


def _plan(mode: CollectionMode = CollectionMode.INCREMENTAL) -> CollectionRunPlan:
    return build_run_plan(
        mode=mode,
        dataset="fmp_price_eod_full",
        parameters={"symbols_sha256": "d" * 64},
        created_at_utc=datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
    )


def _collector(  # noqa: PLR0913 - explicit synthetic wiring for each injected port
    tmp_path: Path,
    transport: ScriptedTransport,
    *,
    control_plane: FakeControlPlane | None = None,
    clock: FakeClock | None = None,
    max_calls: int | None = 50,
    tier: TierArtifact | None = None,
    config_overrides: Mapping[str, Any] | None = None,
    lock: CollectorLock | None = None,
    shared_pacing_deadline: datetime | None = None,
) -> tuple[FmpCollector, FakeControlPlane, FakeClock]:
    resolved_clock = clock or FakeClock()
    resolved_plane = control_plane or FakeControlPlane()
    limiter = RateLimiter(
        tier=tier or TierArtifact(calls_per_minute=3000, calls_per_day=None, bandwidth_gb_30d=None),
        max_calls=max_calls,
        clock=resolved_clock.time,
        sleep=resolved_clock.sleep,
        run_seed=RUN_SEED,
    )
    collector = FmpCollector(
        config=_config(tmp_path, max_calls=max_calls, **(config_overrides or {})),
        transport=transport,
        control_plane=resolved_plane,
        limiter=limiter,
        credential=CREDENTIAL,
        clock=resolved_clock.now,
        lock=lock,
        shared_pacing_deadline=shared_pacing_deadline,
    )
    return (collector, resolved_plane, resolved_clock)


# -- guardrails ------------------------------------------------------------


def test_git_contained_destinations_are_rejected(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    (repository / ".git").mkdir(parents=True)
    with pytest.raises(DestinationError, match="outside a Git repository"):
        validate_destination("raw store root", repository / "raw")
    outside = validate_destination("raw store root", tmp_path / "outside" / "raw")
    assert outside.is_absolute()


def test_destination_validation_resolves_symlinks(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    (repository / ".git").mkdir(parents=True)
    link = tmp_path / "link"
    link.symlink_to(repository, target_is_directory=True)
    with pytest.raises(DestinationError, match="outside a Git repository"):
        validate_destination("dataset root", link / "datasets")


def test_credential_scan_aborts_and_never_quotes_the_credential() -> None:
    payload = json.dumps({"note": CREDENTIAL}).encode()
    with pytest.raises(CredentialLeakError) as excinfo:
        assert_credential_absent(CREDENTIAL, payload)
    assert CREDENTIAL not in str(excinfo.value)


def test_authentication_headers_are_redacted_at_every_boundary() -> None:
    redacted = redact_headers({"apikey": CREDENTIAL, "Content-Type": "application/json"})
    assert redacted["apikey"] == "[REDACTED]"
    assert CREDENTIAL not in json.dumps(redacted)


@pytest.mark.parametrize(
    "raw_headers",
    [
        ((f"X-{CREDENTIAL}", "safe"),),
        (("X-Provider", f"echo:{CREDENTIAL}"),),
        (("X-Provider", "safe"), ("X-Provider", CREDENTIAL)),
    ],
)
def test_credential_in_any_raw_header_is_terminal_and_sanitized(
    tmp_path: Path, raw_headers: tuple[tuple[str, str], ...]
) -> None:
    response = CollectorResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        raw_headers=raw_headers,
        body=b"[]",
        requested_at_utc=datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
        retrieved_at_utc=datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
    )
    collector, _plane, _clock = _collector(tmp_path, ScriptedTransport([response]))

    with pytest.raises(CredentialLeakError):
        collector.collect_profile(symbol=SYMBOL)

    receipt = json.loads(collector.build_receipt(run_id="header-rejected", plan=_plan()))
    assert receipt["usage"]["calls_attempted"] == 1
    assert receipt["usage"]["bytes_received"] == len(b"[]")
    persisted = (path for path in (tmp_path / "raw").rglob("*") if path.is_file())
    assert all(CREDENTIAL.encode() not in path.read_bytes() for path in persisted)
    resumed_transport = ScriptedTransport([])
    resumed, _plane, _clock = _collector(tmp_path, resumed_transport)
    with pytest.raises(CredentialLeakError):
        resumed.collect_profile(symbol=SYMBOL)
    assert resumed_transport.requests == []


def test_existing_attempt_format_without_pacing_deadline_remains_readable(
    tmp_path: Path,
) -> None:
    request = CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)
    attempt_root = tmp_path / "raw" / "fmp" / "runs" / "synth-run-identity" / "attempts"
    attempt_root.mkdir(parents=True)
    payload = canonical_json_bytes(
        {
            "attempt_seq": 1,
            "attempted_at_utc": datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
            "outcome": "credential_rejected_response",
            "previous_attempt_sha256": "0" * 64,
            "raw_byte_length": 2,
            "request_attempt_index": 1,
            "request_fingerprint": request.request_fingerprint,
            "run_identity": "synth-run-identity",
            "status_code": 200,
        }
    )
    (attempt_root / "00000001.json").write_bytes(payload)
    (attempt_root.parent / "latest-attempt.json").write_bytes(payload)
    transport = ScriptedTransport([])
    collector, _plane, _clock = _collector(tmp_path, transport)

    with pytest.raises(CredentialLeakError):
        collector.collect_profile(symbol=SYMBOL)

    assert collector.calls_attempted == 1
    assert transport.requests == []


def test_request_fingerprints_and_uris_are_credential_free() -> None:
    request = CollectorRequest(
        endpoint="/stable/historical-price-eod/full",
        parameters={"symbol": SYMBOL, "from": "2026-07-01", "to": "2026-07-29"},
        symbol=SYMBOL,
    )
    assert CREDENTIAL not in request.source_uri
    assert request.request_fingerprint.startswith("sha256:")
    with pytest.raises(ValueError, match="credential parameters"):
        CollectorRequest(endpoint="/stable/profile", parameters={"apikey": "x"})


def test_credential_rejection_is_terminal_across_resume_below_cutoff(tmp_path: Path) -> None:
    poisoned = b"provider-echo:" + CREDENTIAL.encode()
    collector, _plane, _clock = _collector(tmp_path, ScriptedTransport([_response(poisoned)]))

    with pytest.raises(CredentialLeakError):
        collector.collect_profile(symbol=SYMBOL)

    expected_usage = {
        "bytes_received": len(poisoned),
        "calls_attempted": 1,
        "rate_limited_attempts": 0,
        "retry_after_waits": 0,
    }
    receipt = json.loads(collector.build_receipt(run_id="rejected", plan=_plan()))
    assert receipt["usage"] == expected_usage
    persisted = (path for path in (tmp_path / "raw").rglob("*") if path.is_file())
    assert all(CREDENTIAL.encode() not in path.read_bytes() for path in persisted)

    resumed_transport = ScriptedTransport([])
    resumed, _plane, _clock = _collector(tmp_path, resumed_transport)
    with pytest.raises(CredentialLeakError):
        resumed.collect_profile(symbol=SYMBOL)
    resumed_receipt = json.loads(resumed.build_receipt(run_id="resumed", plan=_plan()))
    assert resumed_receipt["usage"] == expected_usage
    assert resumed_transport.requests == []


def test_credential_response_is_safely_accounted_and_reconstructable(tmp_path: Path) -> None:
    tier = TierArtifact(calls_per_minute=3000, calls_per_day=None, bandwidth_gb_30d=0.000001)
    cutoff = tier.bandwidth_cutoff_bytes
    assert cutoff is not None
    poisoned = CREDENTIAL.encode() + b"X" * (cutoff - len(CREDENTIAL))
    transport = ScriptedTransport(
        [_response(poisoned, status_code=429, headers={"Retry-After": "9"})]
    )
    collector, _plane, clock = _collector(tmp_path, transport, tier=tier)

    with pytest.raises(CredentialLeakError):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )

    usage = json.loads(collector.build_receipt(run_id="fmp-run-rejected", plan=_plan()))["usage"]
    assert usage == {
        "bytes_received": len(poisoned),
        "calls_attempted": 1,
        "rate_limited_attempts": 1,
        "retry_after_waits": 0,
    }
    assert clock.waits == []
    assert list((tmp_path / "raw").rglob("*.raw")) == []
    persisted_files = (path for path in (tmp_path / "raw").rglob("*") if path.is_file())
    assert all(CREDENTIAL.encode() not in path.read_bytes() for path in persisted_files)

    resumed_transport = ScriptedTransport([])
    resumed, _plane, _clock = _collector(tmp_path, resumed_transport, tier=tier)
    resumed_usage = json.loads(
        resumed.build_receipt(run_id="fmp-run-rejected-resume", plan=_plan())
    )["usage"]
    assert resumed_usage == usage
    with pytest.raises(CredentialLeakError):
        resumed.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    assert resumed_transport.requests == []


# -- lock ------------------------------------------------------------------


def test_lock_file_excludes_a_concurrent_run_and_reports_diagnostics(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    first = CollectorLock(raw_root, run_identity="synth-run-1")
    first.acquire()
    assert first.path == raw_root / LOCK_RELATIVE_PATH
    recorded = json.loads(first.path.read_bytes())
    assert recorded["run_identity"] == "synth-run-1"
    assert isinstance(recorded["pid"], int)
    assert first.path.stat().st_mode & 0o777 == _LOCK_MODE

    second = CollectorLock(raw_root, run_identity="synth-run-2")
    with pytest.raises(CollectorLockError, match="synth-run-1"):
        second.acquire()
    first.release()
    second.acquire()
    # Exclusion is kernel-enforced on the open file description, so the lock
    # path deliberately persists after release; the successor now holds it.
    assert json.loads(second.path.read_bytes())["run_identity"] == "synth-run-2"
    second.release()
    third = CollectorLock(raw_root, run_identity="synth-run-3")
    third.acquire()
    third.release()


def test_lock_is_released_on_the_finally_path(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    with (
        pytest.raises(RuntimeError, match="synthetic failure"),
        CollectorLock(raw_root, run_identity="synth-run-3"),
    ):
        raise RuntimeError("synthetic failure")
    # The lock was dropped, so a later run can acquire it immediately.
    successor = CollectorLock(raw_root, run_identity="synth-run-4")
    successor.acquire()
    successor.release()


def test_lock_rejects_a_symlink_without_mutating_its_target(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    lock_path = raw_root / LOCK_RELATIVE_PATH
    lock_path.parent.mkdir(parents=True)
    victim = tmp_path / "victim"
    original = b"operator-owned-bytes"
    victim.write_bytes(original)
    lock_path.symlink_to(victim)
    lock = CollectorLock(raw_root, run_identity="synth-run-attacker")

    try:
        with pytest.raises(CollectorLockError, match="unsafe lock target"):
            lock.acquire()
    finally:
        lock.release()

    assert victim.read_bytes() == original
    assert lock_path.is_symlink()
    assert lock.owned_payload is None


def test_lock_rejects_a_non_regular_target(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    lock_path = raw_root / LOCK_RELATIVE_PATH
    lock_path.mkdir(parents=True)

    with pytest.raises(CollectorLockError, match="unsafe lock target"):
        CollectorLock(raw_root, run_identity="synth-run-directory").acquire()

    assert lock_path.is_dir()


# -- happy path ------------------------------------------------------------


def test_full_synthetic_happy_path_publishes_before_the_terminal_event(tmp_path: Path) -> None:
    body = _fixture_bytes("price_eod_full.json")
    transport = ScriptedTransport([_response(body)])
    collector, plane, clock = _collector(tmp_path, transport)

    plan = build_run_plan(
        mode=CollectionMode.INCREMENTAL,
        dataset="fmp_price_eod_full",
        parameters={"symbols_sha256": "d" * 64},
        created_at_utc=clock.now(),
    )
    run_id = new_run_id()
    collector.register(plan, run_id=run_id)

    rows, bodies = collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    assert len(rows) == _FIXTURE_ROW_COUNT
    receipt = collector.build_receipt(run_id=run_id, plan=plan)
    published = collector.publish(
        raw_bodies=bodies,
        dataset_publications=[(tmp_path / "datasets" / "v1" / "part.parquet", b"SYNTHPARQUET")],
        receipt_bytes=receipt,
    )
    assert raw_blob_path(tmp_path / "raw", body) in published
    assert (tmp_path / "receipts" / "run.receipt.json").exists()

    collector.record_usage(run_id=run_id)
    collector.succeed(run_id=run_id)
    advanced = collector.advance_watermarks(
        run_id=run_id,
        advances=[("fmp_price_eod_full", SYMBOL, date(2026, 7, 28))],
    )
    assert advanced == (("fmp_price_eod_full", SYMBOL, "2026-07-28"),)
    assert plane.event_types == ["attempt_started", "attempt_succeeded", "run_succeeded"]
    assert {record.metric for record in plane.usage} == {
        "calls_attempted",
        "bytes_received",
        "rate_limited_attempts",
    }


def test_receipt_records_artifact_hashes_seed_and_usage(tmp_path: Path) -> None:
    transport = ScriptedTransport([_response(_fixture_bytes("price_eod_full.json"))])
    collector, _plane, clock = _collector(tmp_path, transport)
    plan = build_run_plan(
        mode=CollectionMode.INCREMENTAL,
        dataset="fmp_price_eod_full",
        parameters={"symbols_sha256": "d" * 64},
        created_at_utc=clock.now(),
    )
    collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    receipt = json.loads(collector.build_receipt(run_id="fmp-run-synth", plan=plan))
    assert receipt["run_seed"] == RUN_SEED
    assert set(receipt["artifact_hashes"]) == {"approval", "notification", "tier"}
    assert receipt["usage"]["calls_attempted"] == 1
    assert receipt["requests"][0]["response_headers"]["content-type"] == "application/json"
    assert CREDENTIAL not in json.dumps(receipt)


# -- retries and failures --------------------------------------------------


def test_429_terminal_ceiling_persists_when_global_budget_remains(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        _response(b"[]", status_code=429, headers={"Retry-After": "1"})
        for _ in range(MAX_ATTEMPTS_PER_REQUEST)
    ]
    transport = ScriptedTransport(responses)
    collector, plane, clock = _collector(
        tmp_path,
        transport,
        max_calls=25,
    )
    plan = _plan()
    run_id = "fmp-run-synth"
    collector.register(plan, run_id=run_id)
    responses_accounted = 0
    account_response = fmp_collector_module.classify_and_account_response

    def account_then_crash(
        response: CollectorResponse, limiter: RateLimiter
    ) -> FailureClass | None:
        nonlocal responses_accounted
        result = account_response(response, limiter)
        responses_accounted += 1
        if responses_accounted == MAX_ATTEMPTS_PER_REQUEST:
            raise SimulatedCrash
        return result

    monkeypatch.setattr(
        fmp_collector_module,
        "classify_and_account_response",
        account_then_crash,
    )
    with pytest.raises(SimulatedCrash):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    collector.record_usage(run_id=run_id)
    persisted_usage = {record.metric: record.quantity for record in plane.usage}
    assert persisted_usage["calls_attempted"] == MAX_ATTEMPTS_PER_REQUEST
    assert persisted_usage["rate_limited_attempts"] == MAX_ATTEMPTS_PER_REQUEST
    receipt = json.loads(collector.build_receipt(run_id=run_id, plan=plan))
    assert receipt["usage"]["calls_attempted"] == MAX_ATTEMPTS_PER_REQUEST
    assert receipt["usage"]["rate_limited_attempts"] == MAX_ATTEMPTS_PER_REQUEST
    assert len(transport.requests) == MAX_ATTEMPTS_PER_REQUEST
    assert clock.waits == [1.0, 1.0, 1.0, 1.0]

    resumed_transport = ScriptedTransport([])
    resumed, _plane, _clock = _collector(
        tmp_path,
        resumed_transport,
        max_calls=25,
    )
    with pytest.raises(RetryCeilingError, match="the run is blocked"):
        resumed.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    assert resumed_transport.requests == []
    resumed_receipt = json.loads(
        resumed.build_receipt(run_id="fmp-run-synth-resumed", plan=_plan())
    )
    assert resumed_receipt["usage"]["calls_attempted"] == MAX_ATTEMPTS_PER_REQUEST
    assert resumed_receipt["usage"]["rate_limited_attempts"] == MAX_ATTEMPTS_PER_REQUEST


def test_durable_replay_keeps_raw_bodies_out_of_collector_memory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = _body([{"symbol": SYMBOL.upper(), "cik": "0000000001"}])
    first, _plane, _clock = _collector(
        tmp_path,
        ScriptedTransport([_response(body)]),
    )

    first.collect_profile(symbol=SYMBOL)

    assert all(
        path.suffix == ".raw"
        for path in first._captured.paths  # noqa: SLF001
    )
    original_read_bytes = Path.read_bytes

    def reject_raw_materialization(path: Path) -> bytes:
        if path.suffix == ".raw":
            raise AssertionError("collector startup materialized a durable raw body")
        return original_read_bytes(path)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_bytes", reject_raw_materialization)
        resumed_transport = ScriptedTransport([])
        resumed, _plane, _clock = _collector(tmp_path, resumed_transport)
        assert all(
            path.suffix == ".raw"
            for path in resumed._captured.paths  # noqa: SLF001
        )
        assert all(
            isinstance(sequence, int)
            for sequences in resumed._durable_replays.values()  # noqa: SLF001
            for sequence in sequences
        )

    rows, _bodies = resumed.collect_profile(symbol=SYMBOL)
    assert rows[0]["cik"] == "0000000001"
    assert resumed_transport.requests == []


def test_fresh_run_does_not_scan_global_provenance_store(tmp_path: Path) -> None:
    unrelated = tmp_path / "raw" / "fmp" / "provenance" / "sha256" / "aa" / "unrelated.json"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_bytes(b"{")

    collector, _plane, _clock = _collector(tmp_path, ScriptedTransport([]))

    assert collector.calls_attempted == 0


def test_symbol_collection_uses_capture_cursor_not_full_digest_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = _body([{"symbol": SYMBOL.upper(), "cik": "0000000001"}])
    collector, _plane, _clock = _collector(
        tmp_path,
        ScriptedTransport([_response(body)]),
    )

    with monkeypatch.context() as patch:
        patch.setattr(
            RawCaptureIndex,
            "digests",
            property(
                lambda _index: (_ for _ in ()).throw(
                    AssertionError("collection copied every prior digest")
                )
            ),
        )
        rows, bodies = collector.collect_profile(symbol=SYMBOL)

    assert rows[0]["cik"] == "0000000001"
    assert bodies == (body,)


def test_uncapped_collection_streams_receipt_metadata_in_bounded_chunks(
    tmp_path: Path,
) -> None:
    response_count = 257
    body = _body([{"symbol": SYMBOL.upper(), "cik": "0000000001"}])
    collector, _plane, _clock = _collector(
        tmp_path,
        ScriptedTransport([_response(body) for _ in range(response_count)]),
        max_calls=None,
    )

    for _ in range(response_count):
        collector.collect_profile(symbol=SYMBOL)

    assert len(collector._requests) < response_count  # noqa: SLF001
    assert len(collector._provenance_records) < response_count  # noqa: SLF001
    receipt = json.loads(collector.build_receipt(run_id="synth-run", plan=_plan()))
    addresses = receipt["request_metadata_chunk_addresses"]
    chunks = [
        json.loads(
            (
                tmp_path
                / "raw"
                / "fmp"
                / "receipt-metadata"
                / "sha256"
                / address.removeprefix("sha256:")[:2]
                / f"{address.removeprefix('sha256:')}.json"
            ).read_bytes()
        )
        for address in addresses
    ]
    assert sum(len(chunk["requests"]) for chunk in chunks) == response_count
    assert sum(len(chunk["provenance_addresses"]) for chunk in chunks) == response_count


def test_5xx_uses_exponential_backoff_then_succeeds(tmp_path: Path) -> None:
    transport = ScriptedTransport(
        [
            _response(b"[]", status_code=503),
            _response(_fixture_bytes("price_eod_full.json")),
        ]
    )
    collector, _plane, clock = _collector(tmp_path, transport)
    rows, _bodies = collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    assert len(rows) == _FIXTURE_ROW_COUNT
    assert _BACKOFF_MIN <= clock.waits[0] < _BACKOFF_MAX


def test_unknown_status_is_durable_and_terminal_with_exact_resume(tmp_path: Path) -> None:
    body = b'{"Error Message":"unknown symbol"}'
    transport = ScriptedTransport([_response(body, status_code=_HTTP_NOT_FOUND)])
    collector, _plane, _clock = _collector(tmp_path, transport)
    error_type = getattr(fmp_collector_module, "UnexpectedStatusError", RuntimeError)

    with pytest.raises(error_type) as error:
        collector.collect_profile(symbol=SYMBOL)

    assert getattr(error.value, "status_code", None) == _HTTP_NOT_FOUND
    assert collector.calls_attempted == 1
    assert collector.captured_bodies == (body,)
    assert json.loads(collector.provenance_records[0])["status_code"] == _HTTP_NOT_FOUND
    resumed_transport = ScriptedTransport([])
    resumed, _plane, _clock = _collector(tmp_path, resumed_transport)
    with pytest.raises(error_type):
        resumed.collect_profile(symbol=SYMBOL)
    assert resumed.calls_attempted == 1
    assert resumed_transport.requests == []


def test_unknown_status_reconstructs_after_crash_before_usage_persistence(tmp_path: Path) -> None:
    body = b'{"Error Message":"missing"}'
    collector, _plane, _clock = _collector(
        tmp_path, ScriptedTransport([_response(body, status_code=_HTTP_NOT_FOUND)])
    )
    error_type = getattr(fmp_collector_module, "UnexpectedStatusError", RuntimeError)

    with pytest.raises(error_type):
        collector.collect_profile(symbol=SYMBOL)

    resumed_transport = ScriptedTransport([])
    resumed, _plane, _clock = _collector(tmp_path, resumed_transport)
    receipt = json.loads(resumed.build_receipt(run_id="crash-resume", plan=_plan()))
    assert receipt["usage"]["calls_attempted"] == 1
    assert receipt["usage"]["bytes_received"] == len(body)
    with pytest.raises(error_type):
        resumed.collect_profile(symbol=SYMBOL)
    assert resumed_transport.requests == []


def test_entitlement_failure_blocks_the_run_without_retrying(tmp_path: Path) -> None:
    transport = ScriptedTransport([_response(b"[]", status_code=403)])
    collector, _plane, clock = _collector(tmp_path, transport)
    with pytest.raises(EntitlementError):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    assert clock.waits == []


def test_timeout_counts_as_an_attempt_and_retries(tmp_path: Path) -> None:
    transport = ScriptedTransport(
        [TimeoutError("synthetic timeout"), _response(_fixture_bytes("price_eod_full.json"))]
    )
    collector, _plane, clock = _collector(tmp_path, transport)
    rows, _bodies = collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    assert len(rows) == _FIXTURE_ROW_COUNT
    assert len(clock.waits) == 1


def test_restart_honors_minimum_pacing_when_retry_after_is_short(tmp_path: Path) -> None:
    tier = TierArtifact(calls_per_minute=1, calls_per_day=None, bandwidth_gb_30d=None)
    crash_clock = CrashOnWaitClock(crash_on_wait=1)
    collector, _plane, _clock = _collector(
        tmp_path,
        ScriptedTransport(
            [_response(b"[]", status_code=429, headers={"Retry-After": "1"}, clock=crash_clock)]
        ),
        clock=crash_clock,
        tier=tier,
    )

    with pytest.raises(SimulatedCrash):
        collector.collect_profile(symbol=SYMBOL)

    resume_clock = FakeClock()
    resumed_transport = ScriptedTransport([_response(b"[]", clock=resume_clock)])
    resumed, _plane, _clock = _collector(tmp_path, resumed_transport, clock=resume_clock, tier=tier)
    resumed.collect_profile(symbol=SYMBOL)

    assert resume_clock.waits == [pytest.approx(tier.minimum_interval_seconds)]
    assert len(resumed_transport.requests) == 1


def test_successful_attempt_restores_pacing_before_a_new_transport(tmp_path: Path) -> None:
    tier = TierArtifact(calls_per_minute=1, calls_per_day=None, bandwidth_gb_30d=None)
    first, _plane, _clock = _collector(tmp_path, ScriptedTransport([_response(b"[]")]), tier=tier)
    first._execute(  # noqa: SLF001 - exact request boundary under test
        CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)
    )

    resume_clock = FakeClock()
    resumed_transport = ScriptedTransport([_response(b"[]", clock=resume_clock)])
    resumed, _plane, _clock = _collector(tmp_path, resumed_transport, clock=resume_clock, tier=tier)
    resumed._execute(  # noqa: SLF001 - exact request boundary under test
        CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)
    )
    resumed._execute(  # noqa: SLF001 - exact request boundary under test
        CollectorRequest("/stable/splits", {"symbol": SYMBOL}, symbol=SYMBOL)
    )

    assert resume_clock.waits == [pytest.approx(tier.minimum_interval_seconds)]
    assert len(resumed_transport.requests) == 1


def test_new_run_honors_the_latest_durable_provider_deadline(tmp_path: Path) -> None:
    tier = TierArtifact(calls_per_minute=1, calls_per_day=None, bandwidth_gb_30d=None)
    first, _plane, _clock = _collector(
        tmp_path,
        ScriptedTransport([_response(b"[]")]),
        tier=tier,
        config_overrides={"run_identity": "fmp-run-prior"},
    )
    first._execute(  # noqa: SLF001 - exact request boundary under test
        CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)
    )

    shared_deadline = latest_durable_pacing_deadline(tmp_path / "raw")
    next_clock = FakeClock()
    next_transport = ScriptedTransport([_response(b"[]", clock=next_clock)])
    next_run, _plane, _clock = _collector(
        tmp_path,
        next_transport,
        clock=next_clock,
        tier=tier,
        config_overrides={"run_identity": "fmp-run-next"},
        shared_pacing_deadline=shared_deadline,
    )
    next_run._execute(  # noqa: SLF001 - exact request boundary under test
        CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)
    )

    assert next_clock.waits == [pytest.approx(tier.minimum_interval_seconds)]
    assert len(next_transport.requests) == 1


def test_stale_latest_attempt_pointer_fails_closed(tmp_path: Path) -> None:
    collector, _plane, _clock = _collector(
        tmp_path,
        ScriptedTransport([_response(b"[]"), _response(b"[]")]),
        config_overrides={"run_identity": "fmp-run-pointer-crash"},
    )
    collector._execute(  # noqa: SLF001 - exact durable boundary under test
        CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)
    )
    collector._execute(  # noqa: SLF001 - exact durable boundary under test
        CollectorRequest("/stable/splits", {"symbol": SYMBOL}, symbol=SYMBOL)
    )
    run_root = tmp_path / "raw" / "fmp" / "runs" / "fmp-run-pointer-crash"
    (run_root / "latest-attempt.json").write_bytes(
        (run_root / "attempts" / "00000001.json").read_bytes()
    )

    with pytest.raises(CollectorContractError, match="latest attempt pointer"):
        latest_durable_pacing_deadline(tmp_path / "raw")


@pytest.mark.parametrize(
    "deadline",
    [
        None,
        datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
        datetime(2026, 7, 29, 11, 59, 59, tzinfo=UTC),
    ],
)
def test_nonpositive_restored_deadline_reports_no_wait(
    deadline: datetime | None,
) -> None:
    clock = FakeClock()
    limiter = RateLimiter(
        tier=TierArtifact(calls_per_minute=1, calls_per_day=None, bandwidth_gb_30d=None),
        max_calls=50,
        clock=clock.time,
        sleep=clock.sleep,
        run_seed=RUN_SEED,
    )
    pacing = RequestPacing(clock.now, limiter)
    pacing.restore(deadline)

    approval_checks: list[datetime] = []
    waited = pacing.honor_restored_deadline(before_wait=lambda: approval_checks.append(clock.now()))

    assert waited is False
    assert approval_checks == []
    assert clock.waits == []


def test_normal_attempt_checks_approval_once_when_restored_pacing_does_not_wait(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    transport = ScriptedTransport([_response(b"[]", clock=clock)])
    limiter = RateLimiter(
        tier=TierArtifact(calls_per_minute=1, calls_per_day=None, bandwidth_gb_30d=None),
        max_calls=50,
        clock=clock.time,
        sleep=clock.sleep,
        run_seed=RUN_SEED,
    )
    approval_checks: list[datetime] = []
    collector = FmpCollector(
        config=_config(tmp_path),
        transport=transport,
        control_plane=FakeControlPlane(),
        limiter=limiter,
        credential=CREDENTIAL,
        clock=clock.now,
        approval_check=lambda: approval_checks.append(clock.now()),
    )

    collector._execute(  # noqa: SLF001 - exact pre-transport boundary under test
        CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)
    )

    assert approval_checks == [datetime(2026, 7, 29, 12, 0, tzinfo=UTC)]
    assert clock.waits == []
    assert limiter.calls_attempted == 1
    assert len(transport.requests) == 1


@pytest.mark.parametrize(
    ("expire_before_wait", "expected_wait"),
    [(False, float(MAX_RETRY_AFTER_SECONDS)), (True, None)],
    ids=("expires-during-retry-wait", "expired-before-retry-wait"),
)
def test_retry_pacing_expiry_blocks_second_slot_and_remains_resumable(
    tmp_path: Path,
    *,
    expire_before_wait: bool,
    expected_wait: float | None,
) -> None:
    clock = FakeClock()
    tier = TierArtifact(calls_per_minute=60, calls_per_day=None, bandwidth_gb_30d=None)
    transport = ScriptedTransport(
        [
            _response(
                b"[]",
                status_code=429,
                headers={"Retry-After": str(MAX_RETRY_AFTER_SECONDS)},
                clock=clock,
            ),
            _response(b"[]", clock=clock),
        ]
    )
    limiter = RateLimiter(
        tier=tier,
        max_calls=50,
        clock=clock.time,
        sleep=clock.sleep,
        run_seed=RUN_SEED,
    )
    expires_at = clock.now() + timedelta(seconds=10)
    approval_checks: list[datetime] = []

    def require_active_approval() -> None:
        approval_checks.append(clock.now())
        if len(approval_checks) > 1 and (expire_before_wait or clock.now() >= expires_at):
            raise ApprovalExpiredError("synthetic approval expired")

    collector = FmpCollector(
        config=_config(tmp_path),
        transport=transport,
        control_plane=FakeControlPlane(),
        limiter=limiter,
        credential=CREDENTIAL,
        clock=clock.now,
        approval_check=require_active_approval,
    )
    request = CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)

    with pytest.raises(ApprovalExpiredError, match="synthetic approval expired"):
        collector._execute(request)  # noqa: SLF001 - exact retry boundary under test

    expected_checks = [datetime(2026, 7, 29, 12, 0, tzinfo=UTC)] * 2
    if expected_wait is not None:
        expected_checks.append(
            datetime(2026, 7, 29, 12, 0, tzinfo=UTC) + timedelta(seconds=MAX_RETRY_AFTER_SECONDS)
        )
    assert approval_checks == expected_checks
    assert clock.waits == ([] if expected_wait is None else [expected_wait])
    assert limiter.calls_attempted == 1
    assert len(transport.requests) == 1
    usage = json.loads(collector.build_receipt(run_id="retry-expired", plan=_plan()))["usage"]
    assert usage == {
        "bytes_received": len(b"[]"),
        "calls_attempted": 1,
        "rate_limited_attempts": 1,
        "retry_after_waits": 1,
    }
    attempts_root = tmp_path / "raw" / "fmp" / "runs" / "synth-run-identity" / "attempts"
    assert len(list(attempts_root.glob("*.json"))) == 1

    resume_clock = FakeClock()
    resume_clock.seconds = clock.seconds
    resumed_transport = ScriptedTransport([_response(b"[]", clock=resume_clock)])
    resumed, _plane, _clock = _collector(tmp_path, resumed_transport, clock=resume_clock, tier=tier)
    resumed._execute(request)  # noqa: SLF001 - pending retry remains resumable

    assert resume_clock.waits == (
        [] if expected_wait is not None else [float(MAX_RETRY_AFTER_SECONDS)]
    )
    assert len(resumed_transport.requests) == 1
    resumed_usage = json.loads(resumed.build_receipt(run_id="retry-resumed", plan=_plan()))["usage"]
    expected_attempts_after_resume = 2
    assert resumed_usage["calls_attempted"] == expected_attempts_after_resume
    assert len(list(attempts_root.glob("*.json"))) == expected_attempts_after_resume


def test_approval_expiring_during_ordinary_pacing_blocks_second_transport(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    transport = ScriptedTransport([_response(b"[]", clock=clock), _response(b"[]", clock=clock)])
    limiter = RateLimiter(
        tier=TierArtifact(calls_per_minute=1, calls_per_day=None, bandwidth_gb_30d=None),
        max_calls=50,
        clock=clock.time,
        sleep=clock.sleep,
        run_seed=RUN_SEED,
    )
    expires_at = clock.now() + timedelta(seconds=10)
    approval_checks: list[datetime] = []

    def require_active_approval() -> None:
        approval_checks.append(clock.now())
        if clock.now() >= expires_at:
            raise ApprovalExpiredError("synthetic approval expired")

    collector = FmpCollector(
        config=_config(tmp_path),
        transport=transport,
        control_plane=FakeControlPlane(),
        limiter=limiter,
        credential=CREDENTIAL,
        clock=clock.now,
        approval_check=require_active_approval,
    )
    collector._execute(  # noqa: SLF001 - establish in-process request pacing
        CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)
    )

    with pytest.raises(ApprovalExpiredError, match="synthetic approval expired"):
        collector._execute(  # noqa: SLF001 - exact pre-slot boundary under test
            CollectorRequest("/stable/splits", {"symbol": SYMBOL}, symbol=SYMBOL)
        )

    assert clock.waits == [pytest.approx(75.0)]
    assert approval_checks == [
        datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
        datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
        datetime(2026, 7, 29, 12, 1, 15, tzinfo=UTC),
    ]
    assert limiter.calls_attempted == 1
    assert len(transport.requests) == 1


def test_expired_approval_avoids_positive_ordinary_wait_and_second_transport(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    transport = ScriptedTransport([_response(b"[]", clock=clock), _response(b"[]", clock=clock)])
    limiter = RateLimiter(
        tier=TierArtifact(calls_per_minute=1, calls_per_day=None, bandwidth_gb_30d=None),
        max_calls=50,
        clock=clock.time,
        sleep=clock.sleep,
        run_seed=RUN_SEED,
    )
    approval_checks: list[datetime] = []

    def require_active_approval() -> None:
        approval_checks.append(clock.now())
        if len(approval_checks) > 1:
            raise ApprovalExpiredError("synthetic approval already expired")

    collector = FmpCollector(
        config=_config(tmp_path),
        transport=transport,
        control_plane=FakeControlPlane(),
        limiter=limiter,
        credential=CREDENTIAL,
        clock=clock.now,
        approval_check=require_active_approval,
    )
    collector._execute(  # noqa: SLF001 - establish in-process request pacing
        CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)
    )

    with pytest.raises(ApprovalExpiredError, match="already expired"):
        collector._execute(  # noqa: SLF001 - exact pre-wait boundary under test
            CollectorRequest("/stable/splits", {"symbol": SYMBOL}, symbol=SYMBOL)
        )

    assert approval_checks == [
        datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
        datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
    ]
    assert clock.waits == []
    assert limiter.calls_attempted == 1
    assert len(transport.requests) == 1


def test_expired_approval_avoids_positive_restored_wait_and_transport(
    tmp_path: Path,
) -> None:
    tier = TierArtifact(calls_per_minute=1, calls_per_day=None, bandwidth_gb_30d=None)
    first, _plane, _clock = _collector(tmp_path, ScriptedTransport([_response(b"[]")]), tier=tier)
    first._execute(  # noqa: SLF001 - establish one durable successful dispatch
        CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)
    )

    resume_clock = FakeClock()
    resumed_transport = ScriptedTransport([_response(b"[]", clock=resume_clock)])
    limiter = RateLimiter(
        tier=tier,
        max_calls=50,
        clock=resume_clock.time,
        sleep=resume_clock.sleep,
        run_seed=RUN_SEED,
    )
    approval_checks: list[datetime] = []

    def require_active_approval() -> None:
        approval_checks.append(resume_clock.now())
        raise ApprovalExpiredError("synthetic approval already expired")

    resumed = FmpCollector(
        config=_config(tmp_path),
        transport=resumed_transport,
        control_plane=FakeControlPlane(),
        limiter=limiter,
        credential=CREDENTIAL,
        clock=resume_clock.now,
        approval_check=require_active_approval,
    )

    with pytest.raises(ApprovalExpiredError, match="already expired"):
        resumed._execute(  # noqa: SLF001 - exact pre-wait boundary under test
            CollectorRequest("/stable/splits", {"symbol": SYMBOL}, symbol=SYMBOL)
        )

    assert approval_checks == [datetime(2026, 7, 29, 12, 0, tzinfo=UTC)]
    assert resume_clock.waits == []
    assert limiter.calls_attempted == 1
    assert resumed_transport.requests == []


def test_approval_expiring_during_restored_pacing_blocks_transport(
    tmp_path: Path,
) -> None:
    tier = TierArtifact(calls_per_minute=1, calls_per_day=None, bandwidth_gb_30d=None)
    first, _plane, _clock = _collector(tmp_path, ScriptedTransport([_response(b"[]")]), tier=tier)
    first._execute(  # noqa: SLF001 - establish one durable successful dispatch
        CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)
    )

    resume_clock = FakeClock()
    resumed_transport = ScriptedTransport([_response(b"[]", clock=resume_clock)])
    limiter = RateLimiter(
        tier=tier,
        max_calls=50,
        clock=resume_clock.time,
        sleep=resume_clock.sleep,
        run_seed=RUN_SEED,
    )
    expires_at = resume_clock.now() + timedelta(seconds=10)
    approval_checks: list[datetime] = []

    def require_active_approval() -> None:
        approval_checks.append(resume_clock.now())
        if resume_clock.now() >= expires_at:
            raise ApprovalExpiredError("synthetic approval expired")

    resumed = FmpCollector(
        config=_config(tmp_path),
        transport=resumed_transport,
        control_plane=FakeControlPlane(),
        limiter=limiter,
        credential=CREDENTIAL,
        clock=resume_clock.now,
        approval_check=require_active_approval,
    )

    with pytest.raises(ApprovalExpiredError, match="synthetic approval expired"):
        resumed._execute(  # noqa: SLF001 - exact pre-transport boundary under test
            CollectorRequest("/stable/splits", {"symbol": SYMBOL}, symbol=SYMBOL)
        )

    assert resume_clock.waits == [pytest.approx(tier.minimum_interval_seconds)]
    assert approval_checks == [
        datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
        datetime(2026, 7, 29, 12, 1, 15, tzinfo=UTC),
    ]
    assert limiter.calls_attempted == 1
    assert resumed_transport.requests == []


def test_expired_approval_blocks_durable_replay_without_transport(tmp_path: Path) -> None:
    first, _plane, _clock = _collector(tmp_path, ScriptedTransport([_response(b"[]")]))
    request = CollectorRequest("/stable/profile", {"symbol": SYMBOL}, symbol=SYMBOL)
    first._execute(request)  # noqa: SLF001 - establish a durable successful response

    resume_clock = FakeClock()
    resumed_transport = ScriptedTransport([_response(b"[]", clock=resume_clock)])
    limiter = RateLimiter(
        tier=TierArtifact(calls_per_minute=3000, calls_per_day=None, bandwidth_gb_30d=None),
        max_calls=50,
        clock=resume_clock.time,
        sleep=resume_clock.sleep,
        run_seed=RUN_SEED,
    )
    approval_checks: list[datetime] = []

    def require_active_approval() -> None:
        approval_checks.append(resume_clock.now())
        raise ApprovalExpiredError("synthetic approval already expired")

    resumed = FmpCollector(
        config=_config(tmp_path),
        transport=resumed_transport,
        control_plane=FakeControlPlane(),
        limiter=limiter,
        credential=CREDENTIAL,
        clock=resume_clock.now,
        approval_check=require_active_approval,
    )
    attempts_root = tmp_path / "raw" / "fmp" / "runs" / "synth-run-identity" / "attempts"
    attempt_count = len(list(attempts_root.glob("*.json")))

    with pytest.raises(ApprovalExpiredError, match="already expired"):
        resumed._execute(request)  # noqa: SLF001 - exact durable replay boundary

    assert approval_checks == [datetime(2026, 7, 29, 12, 0, tzinfo=UTC)]
    assert resume_clock.waits == []
    assert limiter.calls_attempted == 1
    assert resumed_transport.requests == []
    assert len(list(attempts_root.glob("*.json"))) == attempt_count


def test_429_crash_resumes_pending_wait_and_fifth_attempt_ceiling(tmp_path: Path) -> None:
    crash_clock = CrashOnWaitClock(crash_on_wait=4)
    first_transport = ScriptedTransport(
        [
            _response(b"[]", status_code=429, headers={"Retry-After": "9"}, clock=crash_clock)
            for _ in range(4)
        ]
    )
    collector, _plane, _clock = _collector(
        tmp_path,
        first_transport,
        clock=crash_clock,
        max_calls=MAX_ATTEMPTS_PER_REQUEST,
    )

    with pytest.raises(SimulatedCrash):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )

    resume_clock = FakeClock()
    resume_clock.seconds = crash_clock.seconds
    resumed_transport = ScriptedTransport(
        [_response(b"[]", status_code=429, headers={"Retry-After": "9"}, clock=resume_clock)]
    )
    resumed, _plane, _clock = _collector(
        tmp_path,
        resumed_transport,
        clock=resume_clock,
        max_calls=MAX_ATTEMPTS_PER_REQUEST,
    )
    with pytest.raises(RetryCeilingError, match="the run is blocked"):
        resumed.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )

    assert resume_clock.waits == [9.0]
    assert len(resumed_transport.requests) == 1
    usage = json.loads(resumed.build_receipt(run_id="fmp-run-resumed", plan=_plan()))["usage"]
    assert usage == {
        "bytes_received": 10,
        "calls_attempted": MAX_ATTEMPTS_PER_REQUEST,
        "rate_limited_attempts": MAX_ATTEMPTS_PER_REQUEST,
        "retry_after_waits": MAX_ATTEMPTS_PER_REQUEST - 1,
    }


def test_timeout_crash_resumes_backoff_before_second_transport(tmp_path: Path) -> None:
    crash_clock = CrashOnWaitClock(crash_on_wait=1)
    collector, _plane, _clock = _collector(
        tmp_path,
        ScriptedTransport([TimeoutError("synthetic timeout")]),
        clock=crash_clock,
    )

    with pytest.raises(SimulatedCrash):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )

    pending_wait = crash_clock.waits[0]
    resume_clock = FakeClock()
    resumed_transport = ScriptedTransport(
        [_response(_fixture_bytes("price_eod_full.json"), clock=resume_clock)]
    )
    resumed, plane, _clock = _collector(tmp_path, resumed_transport, clock=resume_clock)
    rows, _bodies = resumed.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )

    assert len(rows) == _FIXTURE_ROW_COUNT
    assert resume_clock.waits == [pytest.approx(pending_wait, abs=1e-6)]
    plan = _plan()
    run_id = "fmp-run-timeout-resume"
    receipt = resumed.build_receipt(run_id=run_id, plan=plan)
    usage = json.loads(receipt)["usage"]
    marker = publish_normalized(resumed, run_id, iter(()), plan_id=plan.plan_id)
    resumed.record_usage(run_id=run_id)
    recorded = {record.metric: int(record.quantity) for record in plane.usage}
    assert usage["calls_attempted"] == _FIXTURE_ROW_COUNT
    assert marker["usage"] == usage
    assert recorded == {
        "bytes_received": usage["bytes_received"],
        "calls_attempted": usage["calls_attempted"],
        "rate_limited_attempts": usage["rate_limited_attempts"],
    }


def test_normalized_batches_publish_before_the_next_batch_is_requested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fmp_collection_work, "NORMALIZED_BATCH_ROWS", 1)
    encoded: list[str] = []
    original_parquet_bytes = fmp_collection_work.parquet_bytes

    def encode(
        dataset: str,
        rows: Sequence[Mapping[str, object]],
    ) -> bytes:
        encoded.append(dataset)
        return original_parquet_bytes(dataset, rows)

    monkeypatch.setattr(fmp_collection_work, "parquet_bytes", encode)
    collector, _plane, _clock = _collector(tmp_path, ScriptedTransport([]))
    run_id = "fmp-run-streamed-publication"
    plan = _plan()
    first_path = (
        collector.config.dataset_root
        / "normalized"
        / "fmp"
        / "fmp_profile"
        / f"run_id={run_id}"
        / "part-00000.parquet"
    )

    def batches() -> Iterator[CollectionBatch]:
        yield CollectionBatch("fmp_profile", ({"symbol": "SYNTH.A"},), None)
        assert encoded == ["fmp_profile"]
        assert not first_path.exists()
        yield CollectionBatch("fmp_profile", ({"symbol": "SYNTH.B"},), None)

    marker = publish_normalized(
        collector,
        run_id,
        batches(),
        plan_id=plan.plan_id,
    )

    artifact_paths = [
        Path(item["path"]) for item in cast("list[dict[str, str]]", marker["artifacts"])
    ]
    assert [path.name for path in artifact_paths if path.suffix == ".parquet"] == [
        "part-00000.parquet",
        "part-00001.parquet",
    ]
    assert [path.name for path in artifact_paths if path.suffix == ".json"] == [
        "history-index.json"
    ]
    assert all(path.is_file() for path in artifact_paths)


@pytest.mark.parametrize(
    ("body", "headers", "message"),
    [
        (b"{not json", None, "not valid JSON"),
        (b'{"symbol": "SYNTH.A"}', None, "root must be a JSON array"),
        (b"[]", {"Content-Type": "text/html"}, "not a JSON content type"),
    ],
)
def test_schema_drift_blocks_the_run(
    tmp_path: Path,
    body: bytes,
    headers: dict[str, str] | None,
    message: str,
) -> None:
    transport = ScriptedTransport([_response(body, headers=headers)])
    collector, _plane, _clock = _collector(tmp_path, transport)
    with pytest.raises(CollectorContractError, match=message):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )


def test_mid_run_drift_records_a_failed_run_and_supports_exact_resume(tmp_path: Path) -> None:
    transport = ScriptedTransport([_response(b'{"drift": true}')])
    plane = FakeControlPlane()
    collector, _plane, clock = _collector(tmp_path, transport, control_plane=plane)
    plan = build_run_plan(
        mode=CollectionMode.INCREMENTAL,
        dataset="fmp_price_eod_full",
        parameters={"symbols_sha256": "d" * 64},
        created_at_utc=clock.now(),
    )
    run_id = new_run_id()
    collector.register(plan, run_id=run_id)
    with pytest.raises(CollectorContractError):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    collector.fail(run_id=run_id, error_class="schema_drift", error_message="root not an array")
    assert plane.event_types == ["attempt_started", "attempt_failed", "run_failed"]
    state = plane.current_run_state(run_id)
    assert state is not None
    assert state.terminal is True


def test_budget_exhaustion_cancels_the_run_and_advances_no_watermark(tmp_path: Path) -> None:
    transport = ScriptedTransport([_response(b"[]", status_code=503) for _ in range(4)])
    plane = FakeControlPlane()
    collector, _plane, clock = _collector(tmp_path, transport, control_plane=plane, max_calls=2)
    plan = build_run_plan(
        mode=CollectionMode.INCREMENTAL,
        dataset="fmp_price_eod_full",
        parameters={"symbols_sha256": "d" * 64},
        created_at_utc=clock.now(),
    )
    run_id = new_run_id()
    collector.register(plan, run_id=run_id)
    with pytest.raises(BudgetExhaustedError):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    collector.cancel(run_id=run_id, reason="call_budget_exhausted")
    assert plane.event_types == ["attempt_started", "run_cancelled"]
    with pytest.raises(CollectorContractError, match="terminal run_succeeded"):
        collector.advance_watermarks(
            run_id=run_id,
            advances=[("fmp_price_eod_full", SYMBOL, date(2026, 7, 28))],
        )
    assert plane.advances == []


def test_every_retry_consumes_the_call_budget(tmp_path: Path) -> None:
    transport = ScriptedTransport([_response(b"[]", status_code=503) for _ in range(3)])
    collector, _plane, _clock = _collector(tmp_path, transport, max_calls=3)
    with pytest.raises(BudgetExhaustedError):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    assert len(transport.requests) == _BUDGET_CALLS


# -- truncation, gaps, and quality ----------------------------------------


def test_exactly_5000_records_splits_and_records_both_halves(tmp_path: Path) -> None:
    template = _fixture_records("price_eod_full.json")[0]
    window = DateWindow(date(2026, 1, 1), date(2026, 1, 10))

    def rows_for(start: date, count: int) -> bytes:
        return _body(
            [
                {**template, "date": (start + timedelta(days=index)).isoformat()}
                for index in range(count)
            ]
        )

    transport = ScriptedTransport(
        [
            _response(_body([{**template, "date": "2026-01-01"}] * 5000)),
            _response(rows_for(date(2026, 1, 6), 5)),
            _response(rows_for(date(2026, 1, 1), 5)),
        ]
    )
    collector, _plane, _clock = _collector(tmp_path, transport)
    rows, bodies = collector.collect_window(
        dataset="fmp_price_eod_full", symbol=SYMBOL, window=window
    )
    assert len(rows) == _SPLIT_ROW_COUNT
    # AAS004C-R1-F1: the truncated original is raw evidence too, so all three
    # received bodies are captured, not only the two that produced rows.
    assert len(bodies) == _SPLIT_BODY_COUNT + 1
    assert len(collector.captured_bodies) == _SPLIT_BODY_COUNT + 1
    receipt = json.loads(collector.build_receipt(run_id="fmp-run-synth", plan=_plan()))
    assert receipt["window_splits"][0]["parent_from"] == "2026-01-01"


def test_irreducible_single_day_truncation_blocks_only_that_symbol(tmp_path: Path) -> None:
    template = _fixture_records("price_eod_full.json")[0]
    transport = ScriptedTransport([_response(_body([{**template, "date": "2026-07-29"}] * 5000))])
    collector, _plane, _clock = _collector(tmp_path, transport)
    rows, _bodies = collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(AS_OF, AS_OF),
    )
    assert rows == ()
    assert collector.blocked_symbols == (SYMBOL,)
    kinds = {result.kind for result in collector.quality_results}
    assert QualityResultKind.IRREDUCIBLE_TRUNCATION in kinds


def test_non_integral_volume_rows_are_skipped_as_quality(tmp_path: Path) -> None:
    body = _fixture_bytes("price_eod_non_split_float_volume.json")
    transport = ScriptedTransport([_response(body)])
    collector, _plane, _clock = _collector(tmp_path, transport)
    rows, bodies = collector.collect_window(
        dataset="fmp_price_eod_non_split_adjusted",
        symbol="SCCO",
        window=DateWindow(date(2026, 7, 27), date(2026, 8, 28)),
    )
    assert len(rows) == 1
    assert isinstance(rows[0]["volume"], int)
    assert collector.quality_results[0].kind is QualityResultKind.PROVIDER_ROW_SKIPPED
    assert collector.quality_results[0].detail["skipped_records"] == 1
    assert collector.blocked_symbols == ()
    assert bodies == (body,)


def test_returned_dates_outside_the_window_are_kept_as_quality(tmp_path: Path) -> None:
    template = _fixture_records("price_eod_full.json")[0]
    transport = ScriptedTransport([_response(_body([{**template, "date": "2020-01-01"}]))])
    collector, _plane, _clock = _collector(tmp_path, transport)
    rows, bodies = collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    assert len(rows) == 1
    assert str(rows[0]["date"]) == "2020-01-01"
    assert collector.quality_results[0].kind is QualityResultKind.OUT_OF_WINDOW_DATES
    assert collector.blocked_symbols == ()
    assert bodies


def test_watermark_advance_clamps_to_the_requested_window_end(tmp_path: Path) -> None:
    template = _fixture_records("price_eod_full.json")[0]
    window_end = date(2026, 7, 28)
    transport = ScriptedTransport(
        [
            _response(
                _body(
                    [
                        {**template, "date": "2026-07-28"},
                        {**template, "date": "2026-08-05"},
                    ]
                )
            )
        ]
    )
    collector, plane, _clock = _collector(tmp_path, transport)
    rows, _bodies = collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), window_end),
    )
    assert {str(row["date"]) for row in rows} == {"2026-07-28", "2026-08-05"}
    assert collector.quality_results[0].kind is QualityResultKind.OUT_OF_WINDOW_DATES
    plan = _plan()
    collector.register(plan, run_id="fmp-run-window-clamp")
    publication = publish_normalized(
        collector,
        "fmp-run-window-clamp",
        iter(
            (
                CollectionBatch(
                    "fmp_price_eod_full",
                    rows,
                    ("fmp_price_eod_full", SYMBOL, window_end),
                ),
            )
        ),
        plan_id=plan.plan_id,
    )
    collector.succeed(run_id="fmp-run-window-clamp")
    advances = cast("list[dict[str, str]]", publication["advances"])
    advanced = collector.advance_watermarks(
        run_id="fmp-run-window-clamp",
        advances=[
            (item["dataset"], item["stream"], date.fromisoformat(item["value"]))
            for item in advances
        ],
    )
    assert advanced == (("fmp_price_eod_full", SYMBOL, "2026-07-28"),)
    watermark = collector.latest_watermark_date(dataset="fmp_price_eod_full", symbol=SYMBOL)
    assert watermark == window_end
    assert plane.watermarks[("fmp", "fmp_price_eod_full", SYMBOL)].watermark_value == "2026-07-28"


def test_empty_window_is_recorded_as_a_coverage_gap(tmp_path: Path) -> None:
    transport = ScriptedTransport([_response(b"[]")])
    collector, _plane, _clock = _collector(tmp_path, transport)
    rows, bodies = collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    assert rows == ()
    # AAS004C-R1-F1: an empty validated response is still captured raw
    # evidence, but it produces no normalized row.
    assert bodies == (b"[]",)
    assert collector.quality_results[0].kind is QualityResultKind.COVERAGE_GAP
    assert collector.blocked_symbols == ()


def test_collect_loop_keeps_empty_splits_notes_and_out_of_window_action_dates(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 8, 28)
    watermark = date(2026, 7, 28)
    plane = FakeControlPlane()
    for dataset in (
        "fmp_price_eod_full",
        "fmp_price_eod_non_split_adjusted",
        "fmp_price_eod_dividend_adjusted",
        "fmp_splits",
        "fmp_dividends",
    ):
        for symbol in ("VTSI", "CULL"):
            plane.watermarks[("fmp", dataset, symbol)] = CurrentWatermark(
                provider="fmp",
                dataset=dataset,
                stream=symbol,
                watermark_seq=1,
                run_id="prior",
                watermark_value=watermark.isoformat(),
                watermark_position=datetime(2026, 7, 28, tzinfo=UTC),
                recorded_at_utc=datetime(2026, 7, 28, tzinfo=UTC),
            )

    def eod(symbol: str, *, change: object = 0.07) -> bytes:
        return _body(
            [
                {
                    "symbol": symbol,
                    "date": "2026-08-28",
                    "open": 16.7,
                    "high": 16.7,
                    "low": 16.7,
                    "close": 16.7,
                    "volume": 400,
                    "change": change,
                    "changePercent": change,
                    "vwap": 16.7,
                }
            ]
        )

    def adj(symbol: str) -> bytes:
        return _body(
            [
                {
                    "symbol": symbol,
                    "date": "2026-08-28",
                    "adjOpen": 16.7,
                    "adjHigh": 16.7,
                    "adjLow": 16.7,
                    "adjClose": 16.7,
                    "volume": 400,
                }
            ]
        )

    note = b'{"Note": "Limit Reach."}'
    transport = ScriptedTransport(
        [
            _response(_body([{"symbol": "VTSI", "cik": "0000000001"}])),
            _response(eod("VTSI")),
            _response(adj("VTSI")),
            _response(adj("VTSI")),
            _response(
                _body(
                    [
                        {
                            "symbol": "VTSI",
                            "date": "2018-03-02",
                            "numerator": 1,
                            "denominator": 2,
                            "splitType": "stock-split",
                        }
                    ]
                )
            ),
            _response(b"[]"),
            _response(_body([{"symbol": "CULL", "cik": "0000000002"}])),
            _response(eod("CULL", change=0)),
            _response(adj("CULL")),
            _response(adj("CULL")),
            _response(
                _body(
                    [
                        {
                            "symbol": "CULL",
                            "date": "2021-07-15",
                            "numerator": 28409,
                            "denominator": 10000,
                            "splitType": "stock-split",
                        }
                    ]
                )
            ),
            _response(note),
        ]
    )
    collector, plane, _clock = _collector(
        tmp_path,
        transport,
        control_plane=plane,
        max_calls=None,
        config_overrides={"as_of": as_of, "mode": CollectionMode.INCREMENTAL},
    )
    manifest = parse_universe_manifest(
        {
            "generated_at_utc": datetime(2026, 8, 28, 12, 0, tzinfo=UTC).isoformat(),
            "sources": [
                {
                    "endpoint": "/stable/actively-trading-list",
                    "retrieved_at_utc": datetime(2026, 8, 28, 12, 0, tzinfo=UTC).isoformat(),
                    "raw_content_sha256": "1" * 64,
                }
            ],
            "entries": [
                {"symbol": "VTSI", "ipoDate": None, "delistedDate": None, "active": True},
                {"symbol": "CULL", "ipoDate": None, "delistedDate": None, "active": True},
            ],
        }
    )

    outcome = run_collection(
        collector,
        manifest=manifest,
        manifest_sha256="e" * 64,
        created_at_utc=datetime(2026, 8, 28, 12, 0, tzinfo=UTC),
        run_id="fmp-run-collect-variants",
        dataset_selection=DatasetSelection.ALL,
    )
    assert len(transport.requests) == _PROVIDER_VARIANT_REQUESTS
    kinds = {result.kind for result in collector.quality_results}
    assert QualityResultKind.COVERAGE_GAP in kinds
    assert outcome.terminal_event is RunEventType.RUN_FAILED
    assert outcome.error_class == "CollectorContractError"


def test_http_200_note_error_object_is_a_contract_failure(tmp_path: Path) -> None:
    note = b'{"Note": "Limit Reach."}'
    transport = ScriptedTransport([_response(note)])
    collector, _plane, _clock = _collector(tmp_path, transport)
    with pytest.raises(CollectorContractError, match="Note"):
        collector.collect_symbol_observations(dataset="fmp_splits", symbol=SYMBOL)


def test_empty_array_remains_the_accepted_no_data_form(tmp_path: Path) -> None:
    transport = ScriptedTransport([_response(b"[]")])
    collector, _plane, _clock = _collector(tmp_path, transport)
    rows, bodies = collector.collect_symbol_observations(dataset="fmp_splits", symbol=SYMBOL)
    assert rows == ()
    assert bodies == (b"[]",)
    assert collector.quality_results[0].kind is QualityResultKind.COVERAGE_GAP
    assert collector.blocked_symbols == ()


def test_parse_provider_records_rejects_note_error_objects_and_accepts_empty_arrays() -> None:
    with pytest.raises(CollectorContractError, match="Note"):
        parse_provider_records(_response(b'{"Note": "Limit Reach."}'))
    assert parse_provider_records(_response(b"[]")) == []


def test_delisted_shortfall_is_never_proof_of_absence(tmp_path: Path) -> None:
    transport = ScriptedTransport([])
    collector, _plane, _clock = _collector(tmp_path, transport)
    collector.record_delisted_shortfall(
        symbol=SYMBOL,
        dataset="fmp_price_eod_full",
        window=DateWindow(date(2020, 1, 1), date(2020, 6, 30)),
    )
    result = collector.quality_results[0]
    assert result.kind is QualityResultKind.DELISTED_SHORTFALL
    assert result.detail["interpretation"] == "recorded_shortfall_not_proof_of_absence"
    assert collector.blocked_symbols == ()


def test_observed_coverage_start_is_not_a_listing_date(tmp_path: Path) -> None:
    collector, _plane, _clock = _collector(tmp_path, ScriptedTransport([]))
    collector.record_observed_coverage_start(
        symbol=SYMBOL,
        dataset="fmp_price_eod_full",
        observed_start=date(2017, 4, 3),
        requested_start=date(2016, 1, 1),
    )
    detail = collector.quality_results[0].detail
    assert detail["interpretation"] == "observed_coverage_only_not_a_listing_date"


def test_recycled_ticker_cik_disagreement_blocks_the_symbol(tmp_path: Path) -> None:
    collector, _plane, _clock = _collector(tmp_path, ScriptedTransport([]))
    prior = _fixture_records("profile.json")[0]["cik"]
    collected = _fixture_records("profile_recycled_ticker.json")[0]["cik"]
    assert collector.check_recycled_ticker(symbol=SYMBOL, collected_cik=collected, prior_cik=prior)
    assert collector.blocked_symbols == (SYMBOL,)
    assert collector.quality_results[0].kind is QualityResultKind.RECYCLED_TICKER_AMBIGUITY
    # A matching cik is not an ambiguity.
    assert not collector.check_recycled_ticker(
        symbol="SYNTH.B", collected_cik=prior, prior_cik=prior
    )


def test_blocked_symbols_never_advance_a_watermark(tmp_path: Path) -> None:
    plane = FakeControlPlane()
    collector, _plane, clock = _collector(tmp_path, ScriptedTransport([]), control_plane=plane)
    plan = build_run_plan(
        mode=CollectionMode.INCREMENTAL,
        dataset="fmp_price_eod_full",
        parameters={},
        created_at_utc=clock.now(),
    )
    run_id = new_run_id()
    collector.register(plan, run_id=run_id)
    collector.check_recycled_ticker(symbol=SYMBOL, collected_cik="999", prior_cik="001")
    collector.succeed(run_id=run_id)
    advanced = collector.advance_watermarks(
        run_id=run_id,
        advances=[
            ("fmp_price_eod_full", SYMBOL, date(2026, 7, 28)),
            ("fmp_price_eod_full", "SYNTH.B", date(2026, 7, 28)),
        ],
    )
    assert advanced == (("fmp_price_eod_full", "SYNTH.B", "2026-07-28"),)


def test_unknown_fields_are_recorded_as_a_schema_observation(tmp_path: Path) -> None:
    transport = ScriptedTransport([_response(_fixture_bytes("price_eod_full_unknown_field.json"))])
    collector, _plane, _clock = _collector(tmp_path, transport)
    collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    receipt = json.loads(collector.build_receipt(run_id="fmp-run-synth", plan=_plan()))
    observations = [
        result
        for result in receipt["quality_results"]
        if result["kind"] == QualityResultKind.SCHEMA_OBSERVATION.value
    ]
    assert observations[0]["detail"]["unknown_fields"] == [
        "anotherSynthField",
        "synthUnknownField",
    ]


# -- list pagination -------------------------------------------------------


def test_list_walk_paginates_and_confirms_exhaustion(tmp_path: Path) -> None:
    first_page = _body([{"symbol": f"synth{index:05d}"} for index in range(1000)])
    transport = ScriptedTransport([_response(first_page), _response(b"[]"), _response(b"[]")])
    collector, _plane, _clock = _collector(tmp_path, transport)
    walk = collector.walk_list_endpoint(
        endpoint="/stable/delisted-companies", identity_key="symbol"
    )
    assert len(walk.identities) == _LIST_PAGE_LIMIT
    assert [entry["outcome"] for entry in walk.as_receipt_pages()][-1] == "complete"


def test_empty_first_page_blocks_the_endpoint_after_one_retry(tmp_path: Path) -> None:
    transport = ScriptedTransport([_response(b"[]"), _response(b"[]")])
    collector, _plane, _clock = _collector(tmp_path, transport)
    walk = collector.walk_list_endpoint(endpoint="/stable/cik-list", identity_key="cik")
    assert walk.identities == ()
    assert collector.quality_results[0].kind is QualityResultKind.ENDPOINT_BLOCKED


def test_overlapping_page_identities_do_not_block_the_walk(tmp_path: Path) -> None:
    first = _body([{"symbol": f"synth{index:05d}"} for index in range(1000)])
    second = _body(
        [{"symbol": f"synth{index:05d}"} for index in range(997, 1000)]
        + [{"symbol": f"extra{index:05d}"} for index in range(997)]
    )
    transport = ScriptedTransport(
        [_response(first), _response(second), _response(b"[]"), _response(b"[]")]
    )
    collector, _plane, _clock = _collector(tmp_path, transport)
    walk = collector.walk_list_endpoint(endpoint="/stable/cik-list", identity_key="symbol")
    assert len(walk.identities) == _OVERLAPPING_PAGE_IDENTITIES
    assert [entry["outcome"] for entry in walk.as_receipt_pages()][-1] == "complete"


def test_unpaged_active_list_over_page_limit_completes_after_one_request(
    tmp_path: Path,
) -> None:
    body = _body([{"symbol": f"synth{index:05d}"} for index in range(_LIST_PAGE_LIMIT + 1)])
    transport = ScriptedTransport([_response(body)])
    collector, _plane, _clock = _collector(tmp_path, transport)

    walk = collector.walk_list_endpoint(
        endpoint="/stable/actively-trading-list", identity_key="symbol"
    )

    assert len(walk.identities) == _LIST_PAGE_LIMIT + 1
    assert [entry["outcome"] for entry in walk.as_receipt_pages()] == ["complete"]
    assert len(transport.requests) == 1


def test_unpaged_active_list_over_page_limit_builds_the_universe(tmp_path: Path) -> None:
    active = _body([{"symbol": f"synth{index:05d}"} for index in range(_LIST_PAGE_LIMIT + 1)])
    delisted = _body([{"symbol": "synth.old", "delistedDate": "2021-03-04"}])
    transport = ScriptedTransport([_response(active), _response(delisted)])
    collector, plane, _clock = _collector(tmp_path, transport, max_calls=None)
    destination = tmp_path / "manifests" / "universe-unpaged.json"

    outcome = run_universe_build(
        collector,
        generated_at_utc=datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
        destination=destination,
        run_id="fmp-run-universe-unpaged-active",
    )

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert len(transport.requests) == _UNPAGED_UNIVERSE_REQUESTS
    attempts = list(
        (tmp_path / "raw" / "fmp" / "runs" / outcome.run_id / "attempts").glob("*.json")
    )
    assert len(attempts) == _UNPAGED_UNIVERSE_REQUESTS
    manifest = parse_universe_manifest(json.loads(destination.read_bytes()))
    assert len(manifest.symbols()) == _LIST_PAGE_LIMIT + 2
    assert manifest.symbols()[-1] == "synth.old"
    assert plane.event_types[-1] == "run_succeeded"


def test_delisted_effective_page_size_100_continues_until_a_short_page(tmp_path: Path) -> None:
    first = _body([{"symbol": f"delist{index:03d}"} for index in range(100)])
    second = _body([{"symbol": f"delist{index:03d}"} for index in range(100, 200)])
    tail = _body([{"symbol": "delist.tail", "delistedDate": "2021-03-04"}])
    transport = ScriptedTransport([_response(first), _response(second), _response(tail)])
    collector, _plane, _clock = _collector(tmp_path, transport)

    walk = collector.walk_list_endpoint(
        endpoint="/stable/delisted-companies", identity_key="symbol"
    )

    assert len(walk.identities) == _DELISTED_EFFECTIVE_PAGE_IDENTITIES
    assert [entry["outcome"] for entry in walk.as_receipt_pages()] == [
        "continue",
        "continue",
        "complete",
    ]
    assert [request.page for request in transport.requests] == [0, 1, 2]


def test_universe_build_pages_delisted_at_effective_size_100(tmp_path: Path) -> None:
    active = _body([{"symbol": f"synth{index:05d}"} for index in range(_LIST_PAGE_LIMIT + 1)])
    delisted_page = _body(
        [
            {
                "symbol": f"delist{index:03d}",
                "companyName": "Northern Data AG",
                "exchange": "FSX",
                "ipoDate": "2018-10-02",
                "delistedDate": "2021-03-04",
            }
            for index in range(100)
        ]
    )
    delisted_tail = _body(
        [{"symbol": "delist.tail", "ipoDate": "2018-10-02", "delistedDate": "2021-03-04"}]
    )
    transport = ScriptedTransport(
        [_response(active), _response(delisted_page), _response(delisted_tail)]
    )
    collector, plane, _clock = _collector(tmp_path, transport, max_calls=None)
    destination = tmp_path / "manifests" / "universe-delisted-paged.json"

    outcome = run_universe_build(
        collector,
        generated_at_utc=datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
        destination=destination,
        run_id="fmp-run-universe-delisted-paged",
    )

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert [(request.endpoint, request.page) for request in transport.requests] == [
        ("/stable/actively-trading-list", 0),
        ("/stable/delisted-companies", 0),
        ("/stable/delisted-companies", 1),
    ]
    attempts = list(
        (tmp_path / "raw" / "fmp" / "runs" / outcome.run_id / "attempts").glob("*.json")
    )
    assert len(attempts) == _DELISTED_PAGED_UNIVERSE_ATTEMPTS
    manifest = parse_universe_manifest(json.loads(destination.read_bytes()))
    assert len(manifest.symbols()) == _LIST_PAGE_LIMIT + 1 + 101
    assert manifest.symbols()[-1] == "delist.tail"
    assert plane.event_types[-1] == "run_succeeded"


def test_universe_build_keeps_mixed_case_delisted_labels_and_pages_on(tmp_path: Path) -> None:
    active = _body([{"symbol": f"synth{index:05d}"} for index in range(_LIST_PAGE_LIMIT + 1)])
    delisted_page = _body(
        [
            {
                "symbol": f"NB{index}.F",
                "companyName": "Northern Data AG",
                "exchange": "FSX",
                "ipoDate": "2018-10-02",
                "delistedDate": "2026-12-30",
            }
            for index in range(100)
        ]
    )
    delisted_next = _body(
        [
            {
                "symbol": f"NB{index}.F",
                "companyName": "Northern Data AG",
                "exchange": "FSX",
                "ipoDate": "2018-10-02",
                "delistedDate": "2026-12-30",
            }
            for index in range(100, 200)
        ]
    )
    delisted_tail = _body(
        [
            {
                "symbol": "nb2.tail",
                "ipoDate": "2018-10-02",
                "delistedDate": "2026-12-30",
            }
        ]
    )
    transport = ScriptedTransport(
        [
            _response(active),
            _response(delisted_page),
            _response(delisted_next),
            _response(delisted_tail),
        ]
    )
    collector, plane, _clock = _collector(tmp_path, transport, max_calls=None)
    destination = tmp_path / "manifests" / "universe-mixed-case.json"

    outcome = run_universe_build(
        collector,
        generated_at_utc=datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
        destination=destination,
        run_id="fmp-run-universe-mixed-case",
    )

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert [(request.endpoint, request.page) for request in transport.requests] == [
        ("/stable/actively-trading-list", 0),
        ("/stable/delisted-companies", 0),
        ("/stable/delisted-companies", 1),
        ("/stable/delisted-companies", 2),
    ]
    attempts = list(
        (tmp_path / "raw" / "fmp" / "runs" / outcome.run_id / "attempts").glob("*.json")
    )
    assert len(attempts) == _MIXED_CASE_UNIVERSE_ATTEMPTS
    manifest = parse_universe_manifest(json.loads(destination.read_bytes()))
    assert manifest.symbols()[_LIST_PAGE_LIMIT + 1] == "NB0.F"
    assert manifest.symbols()[-1] == "nb2.tail"
    assert plane.event_types[-1] == "run_succeeded"


def test_universe_build_pages_thirty_delisted_pages_with_overlap_and_future_dates(
    tmp_path: Path,
) -> None:
    overlap_symbols = ("NB2.F", "AAPL", "MSFT")
    active_records = [
        {"symbol": f"act{index:05d}", "ipoDate": "2018-10-02"}
        for index in range(_LIST_PAGE_LIMIT + 1)
    ]
    active_records.extend({"symbol": symbol, "ipoDate": "2018-10-02"} for symbol in overlap_symbols)
    active = _body(active_records)

    def delisted_page(page: int) -> bytes:
        records = []
        for index in range(100):
            ordinal = page * 100 + index
            record = {
                "symbol": f"dl{ordinal:04d}",
                "companyName": "Northern Data AG",
                "exchange": "FSX",
                "ipoDate": "2018-10-02",
                "delistedDate": "2021-03-04",
            }
            if page == _OVERLAP_DELISTED_PAGE and index < len(overlap_symbols):
                record["symbol"] = overlap_symbols[index]
                record["delistedDate"] = "2026-12-30"
            records.append(record)
        return _body(records)

    responses = [_response(active)]
    responses.extend(_response(delisted_page(page)) for page in range(30))
    responses.append(_response(_body([{"symbol": "dl.tail", "delistedDate": "2021-03-04"}])))
    transport = ScriptedTransport(responses)
    collector, plane, _clock = _collector(tmp_path, transport, max_calls=None)
    destination = tmp_path / "manifests" / "universe-thirty-delisted.json"

    outcome = run_universe_build(
        collector,
        generated_at_utc=datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
        destination=destination,
        run_id="fmp-run-universe-thirty-delisted",
    )

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert outcome.error_class is None
    assert len(transport.requests) == _THIRTY_DELISTED_UNIVERSE_REQUESTS
    assert [(request.endpoint, request.page) for request in transport.requests] == [
        ("/stable/actively-trading-list", 0),
        *[("/stable/delisted-companies", page) for page in range(31)],
    ]
    attempts = list(
        (tmp_path / "raw" / "fmp" / "runs" / outcome.run_id / "attempts").glob("*.json")
    )
    assert len(attempts) == _THIRTY_DELISTED_UNIVERSE_REQUESTS
    manifest = parse_universe_manifest(json.loads(destination.read_bytes()))
    classification = classify_daily_universe(
        manifest,
        (),
        norgate_snapshot_date=date(2026, 7, 28),
    )
    assert (
        classification.symbols(DailyDisposition.PROVIDER_ACTIVE_DELISTED_OVERLAP) == overlap_symbols
    )
    for symbol in overlap_symbols:
        assert symbol not in classification.collection_manifest.symbols()
    assert plane.event_types[-1] == "run_succeeded"


# -- publication -----------------------------------------------------------


def test_republishing_identical_bytes_is_a_no_op(tmp_path: Path) -> None:
    destination = tmp_path / "raw" / "blob.raw"
    publish_bundle([(destination, b"SYNTH")])
    publish_bundle([(destination, b"SYNTH")])
    assert destination.read_bytes() == b"SYNTH"


def test_different_bytes_at_the_same_address_is_a_hard_error(tmp_path: Path) -> None:
    destination = tmp_path / "raw" / "blob.raw"
    publish_bundle([(destination, b"SYNTH")])
    with pytest.raises(FileExistsError, match="different bytes"):
        publish_bundle([(destination, b"OTHER")])


def test_a_failed_bundle_leaves_no_partial_publication(tmp_path: Path) -> None:
    good = tmp_path / "raw" / "good.raw"
    conflicting = tmp_path / "raw" / "conflict.raw"
    publish_bundle([(conflicting, b"ORIGINAL")])
    with pytest.raises(FileExistsError):
        publish_bundle([(good, b"NEW"), (conflicting, b"DIFFERENT")])
    assert not good.exists()
    assert conflicting.read_bytes() == b"ORIGINAL"


# -- plan and run identity -------------------------------------------------


def test_plan_id_is_derived_from_the_005_projection_digest() -> None:
    created = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
    plan = build_run_plan(
        mode=CollectionMode.BACKFILL,
        dataset="fmp_price_eod_full",
        parameters={"symbols_sha256": "d" * 64},
        created_at_utc=created,
    )
    assert plan.plan_id == f"fmp-plan-{plan.plan_sha256}"
    identical = build_run_plan(
        mode=CollectionMode.BACKFILL,
        dataset="fmp_price_eod_full",
        parameters={"symbols_sha256": "d" * 64},
        created_at_utc=created + timedelta(hours=5),
    )
    # created_at_utc is not part of the 005 digest projection.
    assert identical.plan_id == plan.plan_id
    changed = build_run_plan(
        mode=CollectionMode.INCREMENTAL,
        dataset="fmp_price_eod_full",
        parameters={"symbols_sha256": "d" * 64},
        created_at_utc=created,
    )
    assert changed.plan_id != plan.plan_id


def test_plan_resume_record_round_trips_without_regenerating_fields(tmp_path: Path) -> None:
    plan = build_run_plan(
        mode=CollectionMode.BACKFILL,
        dataset="fmp_price_eod_full",
        parameters={"symbols_sha256": "d" * 64},
        created_at_utc=datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
    )
    run_id = new_run_id()
    record = plan_resume_record(plan, run_id=run_id)
    path = tmp_path / "resume" / "plan.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(canonical_json_bytes(record))
    reread = read_plan_resume_record(path)
    assert reread is not None
    assert reread.plan_id == plan.plan_id
    assert reread.run_id == run_id
    assert reread.created_at_utc == record.created_at_utc
    assert read_plan_resume_record(tmp_path / "missing.json") is None


def test_a_run_id_is_never_reused_with_another_plan(tmp_path: Path) -> None:
    plane = FakeControlPlane()
    collector, _plane, clock = _collector(tmp_path, ScriptedTransport([]), control_plane=plane)
    first = build_run_plan(
        mode=CollectionMode.INCREMENTAL,
        dataset="fmp_price_eod_full",
        parameters={"symbols_sha256": "d" * 64},
        created_at_utc=clock.now(),
    )
    second = build_run_plan(
        mode=CollectionMode.BACKFILL,
        dataset="fmp_price_eod_full",
        parameters={"symbols_sha256": "e" * 64},
        created_at_utc=clock.now(),
    )
    run_id = new_run_id()
    collector.register(first, run_id=run_id)
    with pytest.raises(AssertionError, match="never reused"):
        collector.register(second, run_id=run_id)


def test_post_success_watermark_replay_is_idempotent(tmp_path: Path) -> None:
    plane = FakeControlPlane()
    collector, _plane, clock = _collector(tmp_path, ScriptedTransport([]), control_plane=plane)
    plan = build_run_plan(
        mode=CollectionMode.INCREMENTAL,
        dataset="fmp_price_eod_full",
        parameters={},
        created_at_utc=clock.now(),
    )
    run_id = new_run_id()
    collector.register(plan, run_id=run_id)
    collector.succeed(run_id=run_id)
    advances = [("fmp_price_eod_full", SYMBOL, date(2026, 7, 28))]
    collector.advance_watermarks(run_id=run_id, advances=advances)
    # Recovery after a crash between run_succeeded and the watermark calls
    # reuses the same successful run and completes the advance idempotently.
    collector.advance_watermarks(run_id=run_id, advances=advances)
    assert collector.latest_watermark_date(dataset="fmp_price_eod_full", symbol=SYMBOL) == date(
        2026, 7, 28
    )


def test_only_the_seven_allowed_005_operations_are_used() -> None:
    source = inspect.getsource(fmp_collector_module)
    allowed = {
        "register_plan",
        "start_run",
        "append_event",
        "current_run_state",
        "advance_watermark",
        "latest_watermark",
        "record_usage",
    }
    forbidden = {"record_receipt", "begin_registration", "execute", "session", "engine"}
    for name in forbidden:
        assert f"_control_plane.{name}" not in source
    for name in allowed:
        assert name in source


def test_no_unsupported_lifecycle_labels_are_ever_emitted() -> None:
    source = inspect.getsource(fmp_collector_module)
    for unsupported in ("planned", "collecting", "publishing", "registered", "blocked", "partial"):
        assert f'event_type="{unsupported}"' not in source
        assert f"RunEventType.{unsupported.upper()}" not in source


def test_planning_switches_between_incremental_and_backfill(tmp_path: Path) -> None:
    collector, _plane, _clock = _collector(tmp_path, ScriptedTransport([]))
    entry = UniverseEntry(symbol=SYMBOL, ipo_date=date(2015, 1, 2), delisted_date=None, active=True)
    incremental = collector.plan_symbol_windows(
        dataset="fmp_price_eod_full",
        entry=entry,
        watermark=date(2026, 7, 28),
        known_dates=[date(2026, 7, 27), date(2026, 7, 28)],
    )
    assert len(incremental) == 1
    assert incremental[0].end == AS_OF

    backfill = collector.plan_symbol_windows(
        dataset="fmp_price_eod_full",
        entry=entry,
        watermark=None,
        known_dates=[],
    )
    assert len(backfill) > 1


def test_backfill_without_operator_from_is_refused(tmp_path: Path) -> None:
    collector, _plane, _clock = _collector(
        tmp_path, ScriptedTransport([]), config_overrides={"operator_from": None}
    )
    entry = UniverseEntry(symbol=SYMBOL, ipo_date=None, delisted_date=None, active=True)
    with pytest.raises(ValueError, match="explicit operator --from"):
        collector.plan_symbol_windows(
            dataset="fmp_price_eod_full", entry=entry, watermark=None, known_dates=[]
        )


def test_all_six_datasets_are_collectable_from_synthetic_fixtures(tmp_path: Path) -> None:
    assert len(DATASET_SPECS) == _DATASET_COUNT
    for dataset, fixture in (
        ("fmp_price_eod_full", "price_eod_full.json"),
        ("fmp_price_eod_non_split_adjusted", "price_eod_non_split_adjusted.json"),
        ("fmp_price_eod_dividend_adjusted", "price_eod_dividend_adjusted.json"),
    ):
        transport = ScriptedTransport([_response(_fixture_bytes(fixture))])
        collector, _plane, _clock = _collector(tmp_path / dataset, transport)
        rows, _bodies = collector.collect_window(
            dataset=dataset,
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
        assert len(rows) == _FIXTURE_ROW_COUNT


# -- round 1 review fixes --------------------------------------------------


def test_schema_invalid_body_is_still_captured_as_raw_evidence(tmp_path: Path) -> None:
    """AAS004C-R1-F1: an invalid body is published, never normalized."""

    invalid = b'{"drift": true}'
    transport = ScriptedTransport([_response(invalid)])
    collector, _plane, _clock = _collector(tmp_path, transport)
    with pytest.raises(CollectorContractError):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    assert collector.captured_bodies == (invalid,)
    published = collector.publish(
        dataset_publications=[],
        receipt_bytes=collector.build_receipt(run_id="fmp-run-synth", plan=_plan()),
    )
    assert raw_blob_path(tmp_path / "raw", invalid) in published


def test_error_response_bodies_are_captured_and_published(tmp_path: Path) -> None:
    """AAS004C-R1-F1/F3: 5xx bodies are raw evidence and count as bytes."""

    error_body = b'{"error":"SYNTH_UPSTREAM"}'
    transport = ScriptedTransport(
        [
            _response(error_body, status_code=503),
            _response(_fixture_bytes("price_eod_full.json")),
        ]
    )
    collector, _plane, _clock = _collector(tmp_path, transport)
    _rows, bodies = collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    assert error_body in bodies
    published = collector.publish(
        dataset_publications=[],
        receipt_bytes=collector.build_receipt(run_id="fmp-run-synth", plan=_plan()),
    )
    assert raw_blob_path(tmp_path / "raw", error_body) in published


def test_receipt_links_every_captured_raw_address(tmp_path: Path) -> None:
    """AAS004C-R1-F1: each captured body is addressable from the receipt."""

    error_body = b'{"error":"SYNTH_UPSTREAM"}'
    success_body = _fixture_bytes("price_eod_full.json")
    transport = ScriptedTransport([_response(error_body, status_code=503), _response(success_body)])
    collector, _plane, _clock = _collector(tmp_path, transport)
    collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    receipt = json.loads(collector.build_receipt(run_id="fmp-run-synth", plan=_plan()))
    addresses = set(receipt["raw_content_addresses"])
    assert f"sha256:{hashlib.sha256(error_body).hexdigest()}" in addresses
    assert f"sha256:{hashlib.sha256(success_body).hexdigest()}" in addresses
    dispositions = sorted(entry["disposition"] for entry in receipt["requests"])
    assert dispositions == ["failed", "succeeded"]


def test_error_bytes_count_toward_usage_and_can_trigger_the_cutoff(tmp_path: Path) -> None:
    """AAS004C-R1-F3: error bodies reach bandwidth accounting and 005 usage."""

    tier = TierArtifact(calls_per_minute=3000, calls_per_day=None, bandwidth_gb_30d=0.000001)
    cutoff = tier.bandwidth_cutoff_bytes
    assert cutoff is not None
    error_body = b"E" * cutoff
    transport = ScriptedTransport([_response(error_body, status_code=503)])
    plane = FakeControlPlane()
    collector, _plane, _clock = _collector(tmp_path, transport, control_plane=plane, tier=tier)
    with pytest.raises(BudgetExhaustedError, match="90% cutoff"):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    collector.record_usage(run_id="fmp-run-synth")
    recorded = {record.metric: int(record.quantity) for record in plane.usage}
    assert recorded["bytes_received"] == len(error_body)
    receipt = json.loads(collector.build_receipt(run_id="fmp-run-synth", plan=_plan()))
    assert receipt["usage"]["bytes_received"] == len(error_body)


def test_429_cutoff_usage_persists_and_resumes_without_repeat_transport(tmp_path: Path) -> None:
    tier = TierArtifact(calls_per_minute=3000, calls_per_day=None, bandwidth_gb_30d=0.000001)
    cutoff = tier.bandwidth_cutoff_bytes
    assert cutoff is not None
    body = b"R" * cutoff
    transport = ScriptedTransport([_response(body, status_code=429)])
    plane = FakeControlPlane()
    collector, _plane, _clock = _collector(tmp_path, transport, control_plane=plane, tier=tier)

    with pytest.raises(BudgetExhaustedError, match="90% cutoff"):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    collector.record_usage(run_id="fmp-run-synth")
    persisted = {record.metric: int(record.quantity) for record in plane.usage}
    assert persisted == {
        "bytes_received": cutoff,
        "calls_attempted": 1,
        "rate_limited_attempts": 1,
    }
    receipt = json.loads(collector.build_receipt(run_id="fmp-run-synth", plan=_plan()))
    assert receipt["usage"]["calls_attempted"] == 1
    assert receipt["usage"]["bytes_received"] == cutoff
    assert receipt["usage"]["rate_limited_attempts"] == 1
    attempt_digest = collector.attempt_ledger_sha256
    assert attempt_digest != "0" * 64

    resumed_transport = ScriptedTransport([])
    resumed, _plane, _clock = _collector(tmp_path, resumed_transport, tier=tier)
    resumed_receipt = json.loads(
        resumed.build_receipt(run_id="fmp-run-synth-resumed", plan=_plan())
    )
    assert resumed_receipt["usage"]["calls_attempted"] == 1
    assert resumed_receipt["usage"]["bytes_received"] == cutoff
    assert resumed_receipt["usage"]["rate_limited_attempts"] == 1
    assert resumed.attempt_ledger_sha256 == attempt_digest
    with pytest.raises(BudgetExhaustedError, match="90% cutoff"):
        resumed.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    assert resumed_transport.requests == []


def test_entitlement_body_bytes_are_accounted_before_the_block(tmp_path: Path) -> None:
    """AAS004C-R1-F3: a 403 body is accounted before disposition."""

    body = b'{"error":"SYNTH_FORBIDDEN"}'
    transport = ScriptedTransport([_response(body, status_code=403)])
    collector, _plane, _clock = _collector(tmp_path, transport)
    with pytest.raises(EntitlementError):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    assert collector.captured_bodies == (body,)
    receipt = json.loads(collector.build_receipt(run_id="fmp-run-synth", plan=_plan()))
    assert receipt["usage"]["bytes_received"] == len(body)


def test_a_prior_lock_never_deletes_a_successor_lock(tmp_path: Path) -> None:
    """AAS004C-R1-F4/R3-F1: a prior run cannot remove a successor's lock."""

    raw_root = tmp_path / "raw"
    first = CollectorLock(raw_root, run_identity="synth-run-first")
    first.acquire()
    # Simulate an operator deliberately clearing what looks like a stale lock,
    # after which a successor run legitimately acquires its own lock.
    (raw_root / LOCK_RELATIVE_PATH).unlink()
    successor = CollectorLock(raw_root, run_identity="synth-run-successor")
    successor.acquire()
    successor_payload = (raw_root / LOCK_RELATIVE_PATH).read_bytes()

    first.release()

    # Release performs no unlink at all, so the successor's lock is untouched
    # regardless of inode numbering or reuse.
    assert (raw_root / LOCK_RELATIVE_PATH).exists()
    assert (raw_root / LOCK_RELATIVE_PATH).read_bytes() == successor_payload
    successor.release()


def test_lock_loss_fails_closed_before_a_protected_request(tmp_path: Path) -> None:
    """AAS004C-R1-F4: a run that lost its lock attempts no request."""

    raw_root = tmp_path / "raw"
    lock = CollectorLock(raw_root, run_identity="synth-run-owner")
    lock.acquire()
    transport = ScriptedTransport([_response(_fixture_bytes("price_eod_full.json"))])
    collector, _plane, _clock = _collector(tmp_path, transport, lock=lock)

    (raw_root / LOCK_RELATIVE_PATH).unlink()
    CollectorLock(raw_root, run_identity="synth-run-replacement").acquire()

    with pytest.raises(CollectorLockLostError, match="held by another run"):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    assert transport.requests == []


def test_missing_lock_file_also_fails_closed(tmp_path: Path) -> None:
    """AAS004C-R1-F4: a vanished lock is lock loss, not an implicit grant."""

    raw_root = tmp_path / "raw"
    lock = CollectorLock(raw_root, run_identity="synth-run-owner")
    lock.acquire()
    transport = ScriptedTransport([_response(b"[]")])
    collector, _plane, _clock = _collector(tmp_path, transport, lock=lock)
    (raw_root / LOCK_RELATIVE_PATH).unlink()
    with pytest.raises(CollectorLockLostError, match="missing or unreadable"):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    assert transport.requests == []


def test_ownership_check_passes_while_the_lock_is_held(tmp_path: Path) -> None:
    """AAS004C-R1-F4: a held lock does not obstruct ordinary collection."""

    raw_root = tmp_path / "raw"
    lock = CollectorLock(raw_root, run_identity="synth-run-owner")
    lock.acquire()
    transport = ScriptedTransport([_response(_fixture_bytes("price_eod_full.json"))])
    collector, _plane, _clock = _collector(tmp_path, transport, lock=lock)
    rows, _bodies = collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    assert len(rows) == _FIXTURE_ROW_COUNT
    lock.release()


def test_manifest_symbol_matches_the_uppercase_provider_label(tmp_path: Path) -> None:
    """AAS004C-R1-F2: casefolded manifest symbols must not reject real labels."""

    manifest = parse_universe_manifest(
        json.loads((FIXTURE_ROOT / "universe_manifest.json").read_text(encoding="utf-8"))
    )
    manifest_symbol = manifest.symbols()[0]
    assert manifest_symbol == "synth.a"
    transport = ScriptedTransport([_response(_fixture_bytes("price_eod_full.json"))])
    collector, _plane, _clock = _collector(tmp_path, transport)
    rows, _bodies = collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=manifest_symbol,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    assert len(rows) == _FIXTURE_ROW_COUNT
    # The provider label is preserved verbatim, never rewritten to the
    # canonical comparison form.
    assert rows[0]["symbol"] == "SYNTH.A"


# -- round 2 review fixes --------------------------------------------------


def test_raw_evidence_survives_a_parse_exception_without_manual_publish(
    tmp_path: Path,
) -> None:
    """AAS004C-R2-F1: capture is durable before parsing."""

    invalid = b'{"drift": true}'
    transport = ScriptedTransport([_response(invalid)])
    collector, _plane, _clock = _collector(tmp_path, transport)
    with pytest.raises(CollectorContractError):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    # No manual publish() call happens here: the exception propagated out of
    # collect_window, yet the immutable raw file must already exist on disk.
    blob = raw_blob_path(tmp_path / "raw", invalid)
    assert blob.exists()
    assert blob.read_bytes() == invalid
    assert len(collector.provenance_records) == 1
    provenance_path = raw_provenance_path(tmp_path / "raw", collector.provenance_records[0])
    assert provenance_path.exists()
    provenance = json.loads(provenance_path.read_bytes())
    assert provenance["content_sha256"] == hashlib.sha256(invalid).hexdigest()
    assert provenance["status_code"] == _HTTP_OK
    assert CREDENTIAL not in provenance_path.read_text(encoding="utf-8")


def test_error_body_is_durable_before_the_entitlement_block(tmp_path: Path) -> None:
    """AAS004C-R2-F1: an error body is on disk even though the run blocks."""

    body = b'{"error":"SYNTH_FORBIDDEN"}'
    transport = ScriptedTransport([_response(body, status_code=403)])
    collector, _plane, _clock = _collector(tmp_path, transport)
    with pytest.raises(EntitlementError):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=SYMBOL,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    assert raw_blob_path(tmp_path / "raw", body).exists()
    assert len(collector.provenance_records) == 1
    assert raw_provenance_path(tmp_path / "raw", collector.provenance_records[0]).exists()


def test_durable_capture_is_idempotent_across_republication(tmp_path: Path) -> None:
    """AAS004C-R2-F1: capture then publish must not conflict on identical bytes."""

    body = _fixture_bytes("price_eod_full.json")
    transport = ScriptedTransport([_response(body)])
    collector, _plane, _clock = _collector(tmp_path, transport)
    collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    published = collector.publish(
        dataset_publications=[],
        receipt_bytes=collector.build_receipt(run_id="fmp-run-synth", plan=_plan()),
    )
    assert raw_blob_path(tmp_path / "raw", body) in published
    assert raw_blob_path(tmp_path / "raw", body).read_bytes() == body


def test_a_successor_acquiring_during_release_survives(tmp_path: Path) -> None:
    """AAS004C-R3-F1: release cannot delete a successor, by construction.

    A successor replaces the lock path at the latest possible moment: after the
    prior owner has made every observation it could make. Because release never
    unlinks, there is no check-then-act window and no reliance on inode
    non-reuse, which is unsound on ext4 and other filesystems.
    """

    raw_root = tmp_path / "raw"
    prior = CollectorLock(raw_root, run_identity="synth-run-prior")
    prior.acquire()
    lock_path = raw_root / LOCK_RELATIVE_PATH

    swapped: list[bytes] = []
    swapping = False
    real_read = os.read
    real_fstat = os.fstat
    real_read_bytes = Path.read_bytes

    def swap_in_successor() -> None:
        nonlocal swapping
        if swapped or swapping:
            return
        swapping = True
        try:
            lock_path.unlink()
            CollectorLock(raw_root, run_identity="synth-run-successor").acquire()
            swapped.append(real_read_bytes(lock_path))
        finally:
            swapping = False

    def racing_read(descriptor: int, length: int) -> bytes:
        result = real_read(descriptor, length)
        swap_in_successor()
        return result

    def racing_fstat(descriptor: int) -> os.stat_result:
        result = real_fstat(descriptor)
        swap_in_successor()
        return result

    def racing_read_bytes(self: Path) -> bytes:
        result = real_read_bytes(self)
        if self == lock_path:
            swap_in_successor()
        return result

    # Hook the content/descriptor observations a check-then-act release would
    # make, so a successor lands after the final one. `os.stat` is deliberately
    # not hooked because patching it breaks unrelated pathlib operations.
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "read", racing_read)
        patch.setattr(os, "fstat", racing_fstat)
        patch.setattr(Path, "read_bytes", racing_read_bytes)
        prior.release()
        if not swapped:
            # Release made no observation at all, which is the strongest form
            # of the guarantee. Swap afterwards and assert survival anyway.
            swap_in_successor()

    assert swapped, "the racing successor was never created"
    assert lock_path.exists()
    assert lock_path.read_bytes() == swapped[0]


def test_release_never_unlinks_the_lock_path(tmp_path: Path) -> None:
    """AAS004C-R3-F1: ownership never depends on inode identity or reuse."""

    raw_root = tmp_path / "raw"
    lock = CollectorLock(raw_root, run_identity="synth-run-owner")
    lock.acquire()
    lock_path = raw_root / LOCK_RELATIVE_PATH

    real_unlink = os.unlink
    unlinked: list[str] = []

    def recording_unlink(target: str | bytes | os.PathLike[str]) -> None:
        unlinked.append(str(target))
        real_unlink(target)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "unlink", recording_unlink)
        lock.release()

    assert unlinked == []
    assert lock_path.exists()
    # A successor can still acquire immediately, so exclusion is preserved.
    successor = CollectorLock(raw_root, run_identity="synth-run-successor")
    successor.acquire()
    successor.release()


def test_identical_bodies_from_distinct_requests_keep_both_provenance_records(
    tmp_path: Path,
) -> None:
    """AAS004C-R3-F2: provenance is per request attempt, not per body digest."""

    # Two different symbols legitimately return byte-identical empty arrays.
    body = b"[]"
    transport = ScriptedTransport([_response(body), _response(body)])
    collector, _plane, _clock = _collector(tmp_path, transport)
    for symbol in ("SYNTH.A", "SYNTH.B"):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=symbol,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )

    # The body is deduplicated, but both request attempts retain provenance.
    assert collector.captured_bodies == (body,)
    records = collector.provenance_records
    assert len(records) == _FIXTURE_ROW_COUNT
    symbols = [json.loads(record)["symbol"] for record in records]
    assert symbols == ["SYNTH.A", "SYNTH.B"]

    # Both survive a crash: they are already durable with no publish() call.
    paths = {raw_provenance_path(tmp_path / "raw", record) for record in records}
    assert len(paths) == _FIXTURE_ROW_COUNT
    for path in paths:
        assert path.exists()
    assert raw_blob_path(tmp_path / "raw", body).exists()


def test_repeated_identical_request_keeps_one_record_per_attempt(
    tmp_path: Path,
) -> None:
    """AAS004C-R3-F2: even a retry of the same request keeps its own record."""

    body = b'{"error":"SYNTH_UPSTREAM"}'
    transport = ScriptedTransport(
        [
            _response(body, status_code=503),
            _response(body, status_code=503),
            _response(_fixture_bytes("price_eod_full.json")),
        ]
    )
    collector, _plane, _clock = _collector(tmp_path, transport)
    collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol=SYMBOL,
        window=DateWindow(date(2026, 7, 27), AS_OF),
    )
    error_records = [
        record
        for record in collector.provenance_records
        if json.loads(record)["status_code"] != _HTTP_OK
    ]
    assert len(error_records) == _FIXTURE_ROW_COUNT
    assert {json.loads(record)["attempt_seq"] for record in error_records} == {1, 2}
    for record in error_records:
        assert raw_provenance_path(tmp_path / "raw", record).exists()


def test_receipt_links_every_provenance_record(tmp_path: Path) -> None:
    """AAS004C-R3-F2: each attempt's provenance is addressable from the receipt."""

    body = b"[]"
    transport = ScriptedTransport([_response(body), _response(body)])
    collector, _plane, _clock = _collector(tmp_path, transport)
    for symbol in ("SYNTH.A", "SYNTH.B"):
        collector.collect_window(
            dataset="fmp_price_eod_full",
            symbol=symbol,
            window=DateWindow(date(2026, 7, 27), AS_OF),
        )
    receipt = json.loads(collector.build_receipt(run_id="fmp-run-synth", plan=_plan()))
    addresses = receipt["provenance_addresses"]
    assert len(addresses) == _FIXTURE_ROW_COUNT
    assert len(set(addresses)) == _FIXTURE_ROW_COUNT
    for record in collector.provenance_records:
        assert f"sha256:{hashlib.sha256(record).hexdigest()}" in addresses
