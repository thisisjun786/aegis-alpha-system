from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

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
from aegis_alpha.data import (
    fmp_collector,
    fmp_collector_run,
    fmp_universe_run,
    fmp_universe_work,
)
from aegis_alpha.data.fmp_approval import ApprovalExpiredError
from aegis_alpha.data.fmp_collection_work import CollectionBatch
from aegis_alpha.data.fmp_collector import (
    CollectorConfig,
    CollectorRequest,
    CollectorResponse,
    FmpCollector,
    build_run_plan,
)
from aegis_alpha.data.fmp_collector_run import run_collection
from aegis_alpha.data.fmp_collector_state import marker_path
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_rate_limit import RateLimiter, TierArtifact, UsageLedger
from aegis_alpha.data.fmp_symbol_observation import (
    SymbolObservationRequest,
    SymbolObservationResult,
)
from aegis_alpha.data.fmp_universe_recovery import COMPLETED_NAME, publish_required
from aegis_alpha.data.fmp_universe_run import run_universe_build
from aegis_alpha.data.fmp_windows import (
    CollectorContractError,
    UniverseManifest,
    parse_universe_manifest,
)

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
_CREDENTIAL = "SYNTHETIC-ONLY-CREDENTIAL"
_MAX_CALLS = 8
_REPLAY_APPROVAL_CHECKS = 3
_REPLAY_REQUESTS = 2
_REGISTRATION_ATTEMPT_STARTED_CHECK = 4
_USAGE_METRICS = 3
_SECOND_USAGE_METRIC_CHECK = 2


class _StopBeforeFinalization(BaseException):
    pass


def _collect_then_stop(
    _collector: FmpCollector,
    _run_id: str,
    batches: Iterable[CollectionBatch],
    *,
    plan_id: str,
) -> None:
    del plan_id
    for _batch in batches:
        pass
    raise _StopBeforeFinalization


@dataclass
class _MutableClock:
    current: datetime = NOW

    def now(self) -> datetime:
        return self.current


@dataclass(frozen=True)
class _LaterBoundaryExpectation:
    """Expected durable projection when the named check observes expiry."""

    expire_on_check: int
    usage_count: int
    receipt_exists: bool
    completion_exists: bool
    events: tuple[str, ...]


@dataclass
class _MemoryControlPlane:
    """Socket-free durable-state stand-in with the FMP driver port shape."""

    plans: dict[str, CollectionRunPlan] = field(default_factory=dict)
    runs: dict[str, CollectionRun] = field(default_factory=dict)
    events: list[CollectionRunEvent] = field(default_factory=list)
    usage: dict[tuple[str, int], CollectionUsageRecord] = field(default_factory=dict)
    watermarks: dict[tuple[str, str, str], CurrentWatermark] = field(default_factory=dict)

    def register_plan(self, plan: CollectionRunPlan) -> None:
        existing = self.plans.get(plan.plan_id)
        if existing is not None and existing.plan_sha256 != plan.plan_sha256:
            raise AssertionError("plan identity changed")
        self.plans[plan.plan_id] = plan

    def start_run(self, run: CollectionRun) -> None:
        if run.plan_id not in self.plans:
            raise AssertionError("run requires its registered plan")
        existing = self.runs.get(run.run_id)
        if existing is not None and existing.plan_id != run.plan_id:
            raise AssertionError("run identity changed")
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
                RunEventType.RUN_CANCELLED,
                RunEventType.RUN_FAILED,
                RunEventType.RUN_SUCCEEDED,
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
            raise AssertionError("watermark moved backwards")
        sequence = 1 if current is None else current.watermark_seq + 1
        self.watermarks[key] = CurrentWatermark(
            provider=advance.provider,
            dataset=advance.dataset,
            stream=advance.stream,
            watermark_seq=sequence,
            run_id=advance.run_id,
            watermark_value=advance.watermark_value,
            watermark_position=advance.watermark_position,
            recorded_at_utc=NOW,
        )
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
        key = (record.run_id, record.usage_seq)
        existing = self.usage.get(key)
        if existing is not None and existing != record:
            raise AssertionError("usage identity changed")
        self.usage[key] = record

    def run_usage_records(self, run_id: str) -> tuple[CollectionUsageRecord, ...]:
        return tuple(
            record
            for (record_run_id, _sequence), record in sorted(self.usage.items())
            if record_run_id == run_id
        )

    def event_types(self, run_id: str) -> tuple[str, ...]:
        return tuple(event.event_type.value for event in self.events if event.run_id == run_id)


@dataclass
class _StrictMemoryControlPlane(_MemoryControlPlane):
    """A local port stand-in that preserves the registry's first-event invariant."""

    def start_run(self, run: CollectionRun) -> None:
        existing = self.runs.get(run.run_id)
        if existing is not None and existing != run:
            raise AssertionError("run identity changed")
        super().start_run(run)

    def append_event(self, event: CollectionRunEvent) -> int:
        prior = [item for item in self.events if item.run_id == event.run_id]
        if not prior:
            if (
                event.event_type is not RunEventType.ATTEMPT_STARTED
                or event.attempt_number != 1
                or event.retry_of_attempt is not None
            ):
                raise AssertionError("a run must begin with attempt_started")
        else:
            previous = prior[-1]
            allowed = (
                (
                    previous.event_type is RunEventType.ATTEMPT_STARTED
                    and (
                        (
                            event.event_type
                            in {RunEventType.ATTEMPT_SUCCEEDED, RunEventType.ATTEMPT_FAILED}
                            and event.attempt_number == previous.attempt_number
                        )
                        or event.event_type is RunEventType.RUN_CANCELLED
                    )
                )
                or (
                    previous.event_type is RunEventType.ATTEMPT_FAILED
                    and (
                        (
                            event.event_type is RunEventType.ATTEMPT_STARTED
                            and event.attempt_number == (previous.attempt_number or 0) + 1
                            and event.retry_of_attempt == previous.attempt_number
                        )
                        or event.event_type in {RunEventType.RUN_FAILED, RunEventType.RUN_CANCELLED}
                    )
                )
                or (
                    previous.event_type is RunEventType.ATTEMPT_SUCCEEDED
                    and event.event_type is RunEventType.RUN_SUCCEEDED
                )
            )
            if not allowed:
                raise AssertionError("illegal lifecycle transition")
        return super().append_event(event)


class _ScriptedTransport:
    """Returns local response fixtures and never creates a socket."""

    def __init__(self, bodies: Sequence[bytes]) -> None:
        self._bodies = list(bodies)
        self.calls: list[CollectorRequest] = []

    def __call__(self, request: CollectorRequest, credential: str) -> CollectorResponse:
        if credential != _CREDENTIAL:
            raise AssertionError("unexpected credential")
        self.calls.append(request)
        if not self._bodies:
            raise AssertionError("unexpected transport invocation")
        return CollectorResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            body=self._bodies.pop(0),
            requested_at_utc=NOW,
            retrieved_at_utc=NOW,
        )


def _collector(  # noqa: PLR0913 - test fixture exposes independent run controls
    tmp_path: Path,
    control_plane: _MemoryControlPlane,
    transport: _ScriptedTransport,
    run_id: str,
    *,
    approval_check: Callable[[], None] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> FmpCollector:
    return FmpCollector(
        config=CollectorConfig(
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "normalized",
            receipt_path=tmp_path / "receipts" / "run.json",
            as_of=NOW.date(),
            mode=CollectionMode.BACKFILL,
            max_calls=_MAX_CALLS,
            run_identity=run_id,
            operator_from=date(2026, 7, 27),
            artifact_hashes={"approval": "a" * 64, "tier": "b" * 64},
        ),
        transport=transport,
        control_plane=control_plane,
        limiter=RateLimiter(
            tier=TierArtifact(calls_per_minute=3000, calls_per_day=None, bandwidth_gb_30d=None),
            max_calls=_MAX_CALLS,
            clock=lambda: 0.0,
            sleep=lambda _seconds: None,
            run_seed=7,
        ),
        credential=_CREDENTIAL,
        clock=(lambda: NOW) if clock is None else clock,
        approval_check=approval_check,
    )


def _collection_manifest() -> UniverseManifest:
    return parse_universe_manifest(
        {
            "generated_at_utc": NOW.isoformat(),
            "sources": [
                {
                    "endpoint": "/stable/actively-trading-list",
                    "retrieved_at_utc": NOW.isoformat(),
                    "raw_content_sha256": "c" * 64,
                }
            ],
            "entries": [
                {
                    "symbol": "synth.a",
                    "ipoDate": None,
                    "delistedDate": None,
                    "active": True,
                }
            ],
        }
    )


def _collection_bodies() -> tuple[bytes, bytes]:
    return (
        b'[{"symbol":"SYNTH.A","cik":"0000000001"}]',
        json.dumps(
            [
                {
                    "symbol": "SYNTH.A",
                    "date": "2026-07-28",
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                    "volume": 10,
                }
            ]
        ).encode(),
    )


def _prepare_collection_marker(
    tmp_path: Path,
    control_plane: _MemoryControlPlane,
    run_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as patch:
        patch.setattr(
            FmpCollector,
            "record_usage",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(_StopBeforeFinalization()),
        )
        with pytest.raises(_StopBeforeFinalization):
            run_collection(
                _collector(
                    tmp_path,
                    control_plane,
                    _ScriptedTransport(_collection_bodies()),
                    run_id,
                ),
                manifest=_collection_manifest(),
                manifest_sha256="d" * 64,
                created_at_utc=NOW,
                run_id=run_id,
            )


def test_collection_expiry_after_final_replay_blocks_fresh_publication_and_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = _MemoryControlPlane()
    run_id = "fmp-run-collection-replay-expiry"
    with monkeypatch.context() as patch:
        patch.setattr(
            fmp_collector_run,
            "publish_normalized",
            _collect_then_stop,
        )
        with pytest.raises(_StopBeforeFinalization):
            run_collection(
                _collector(
                    tmp_path,
                    control_plane,
                    _ScriptedTransport(_collection_bodies()),
                    run_id,
                ),
                manifest=_collection_manifest(),
                manifest_sha256="d" * 64,
                created_at_utc=NOW,
                run_id=run_id,
            )

    approval_checks: list[int] = []

    def expire_after_replays() -> None:
        approval_checks.append(len(approval_checks) + 1)
        if len(approval_checks) == _REPLAY_APPROVAL_CHECKS:
            raise ApprovalExpiredError("synthetic approval expired after final replay")

    resumed_transport = _ScriptedTransport(())
    resumed = _collector(
        tmp_path,
        control_plane,
        resumed_transport,
        run_id,
        approval_check=expire_after_replays,
    )
    with pytest.raises(ApprovalExpiredError, match="after final replay"):
        run_collection(
            resumed,
            manifest=_collection_manifest(),
            manifest_sha256="d" * 64,
            created_at_utc=NOW,
            run_id=run_id,
        )

    assert approval_checks == [1, 2, 3]
    assert resumed_transport.calls == []
    assert not list((tmp_path / "normalized").rglob("*.parquet"))
    assert not marker_path(resumed, run_id).exists()
    assert not (tmp_path / "receipts" / "run.json").exists()
    assert control_plane.event_types(run_id) == ("attempt_started",)
    assert control_plane.usage == {}
    assert control_plane.watermarks == {}

    recovery_transport = _ScriptedTransport(())
    recovered = run_collection(
        _collector(tmp_path, control_plane, recovery_transport, run_id),
        manifest=_collection_manifest(),
        manifest_sha256="d" * 64,
        created_at_utc=NOW,
        run_id=run_id,
    )
    assert recovery_transport.calls == []
    assert recovered.terminal_event is RunEventType.RUN_SUCCEEDED
    assert control_plane.event_types(run_id) == (
        "attempt_started",
        "attempt_succeeded",
        "run_succeeded",
    )
    assert len(control_plane.usage) == _USAGE_METRICS
    assert len(control_plane.watermarks) == 1


def test_collection_expiry_after_new_provider_slot_cancels_without_publication(
    tmp_path: Path,
) -> None:
    control_plane = _MemoryControlPlane()
    run_id = "fmp-run-collection-new-slot-expiry"
    transport = _ScriptedTransport((b'[{"symbol":"SYNTH.A","cik":"0000000001"}]',))

    def expire_after_new_slot() -> None:
        if transport.calls:
            raise ApprovalExpiredError("synthetic approval expired after new provider slot")

    collector = _collector(
        tmp_path,
        control_plane,
        transport,
        run_id,
        approval_check=expire_after_new_slot,
    )
    outcome = run_collection(
        collector,
        manifest=_collection_manifest(),
        manifest_sha256="d" * 64,
        created_at_utc=NOW,
        run_id=run_id,
        dataset_selection=DatasetSelection.PROFILE,
    )

    assert outcome.terminal_event is RunEventType.RUN_CANCELLED
    assert len(transport.calls) == 1
    assert not marker_path(collector, run_id).exists()
    assert not list((tmp_path / "normalized").rglob("*.parquet"))
    assert not (tmp_path / "receipts" / "run.json").exists()
    assert control_plane.event_types(run_id) == ("attempt_started", "run_cancelled")
    assert len(control_plane.usage) == _USAGE_METRICS
    assert control_plane.watermarks == {}


@pytest.mark.parametrize(
    "expectation",
    [
        _LaterBoundaryExpectation(
            expire_on_check=1,
            usage_count=0,
            receipt_exists=False,
            completion_exists=False,
            events=("attempt_started",),
        ),
        _LaterBoundaryExpectation(
            expire_on_check=4,
            usage_count=_USAGE_METRICS,
            receipt_exists=False,
            completion_exists=False,
            events=("attempt_started",),
        ),
        _LaterBoundaryExpectation(
            expire_on_check=5,
            usage_count=_USAGE_METRICS,
            receipt_exists=True,
            completion_exists=False,
            events=("attempt_started",),
        ),
        _LaterBoundaryExpectation(
            expire_on_check=6,
            usage_count=_USAGE_METRICS,
            receipt_exists=True,
            completion_exists=True,
            events=("attempt_started",),
        ),
        _LaterBoundaryExpectation(
            expire_on_check=7,
            usage_count=_USAGE_METRICS,
            receipt_exists=True,
            completion_exists=True,
            events=("attempt_started", "attempt_succeeded"),
        ),
        _LaterBoundaryExpectation(
            expire_on_check=8,
            usage_count=_USAGE_METRICS,
            receipt_exists=True,
            completion_exists=True,
            events=("attempt_started", "attempt_succeeded", "run_succeeded"),
        ),
    ],
    ids=("usage", "receipt", "completion", "attempt-success", "run-success", "watermark"),
)
def test_collection_marker_recovery_needs_no_later_provider_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    expectation: _LaterBoundaryExpectation,
) -> None:
    control_plane = _MemoryControlPlane()
    run_id = f"fmp-run-collection-later-{expectation.expire_on_check}"
    _prepare_collection_marker(tmp_path, control_plane, run_id, monkeypatch)
    marker = marker_path(
        _collector(tmp_path, control_plane, _ScriptedTransport(()), run_id), run_id
    )
    marker_bytes = marker.read_bytes()
    approval_checks: list[int] = []

    def expire_at_boundary() -> None:
        approval_checks.append(len(approval_checks) + 1)
        if len(approval_checks) == expectation.expire_on_check:
            raise ApprovalExpiredError("synthetic approval expired at durable boundary")

    resumed_transport = _ScriptedTransport(())
    recovered = run_collection(
        _collector(
            tmp_path,
            control_plane,
            resumed_transport,
            run_id,
            approval_check=expire_at_boundary,
        ),
        manifest=_collection_manifest(),
        manifest_sha256="d" * 64,
        created_at_utc=NOW,
        run_id=run_id,
    )

    assert recovered.terminal_event is RunEventType.RUN_SUCCEEDED
    assert approval_checks == []
    assert resumed_transport.calls == []
    assert marker.read_bytes() == marker_bytes
    assert len(control_plane.usage) == _USAGE_METRICS
    assert (tmp_path / "receipts" / "run.json").is_file()
    assert (marker.parent / "completion.json").is_file()
    assert control_plane.event_types(run_id) == (
        "attempt_started",
        "attempt_succeeded",
        "run_succeeded",
    )
    assert control_plane.watermarks


def test_universe_expiry_after_final_replay_blocks_fresh_publication_and_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = _MemoryControlPlane()
    run_id = "fmp-run-universe-replay-expiry"
    destination = tmp_path / "manifests" / "universe.json"
    original_publish_bundle = fmp_universe_work.publish_bundle

    def stop_before_manifest(
        publications: Sequence[tuple[Path, bytes]],
    ) -> tuple[Path, ...]:
        if any(path == destination for path, _content in publications):
            raise _StopBeforeFinalization
        return original_publish_bundle(publications)

    with monkeypatch.context() as patch:
        patch.setattr(fmp_universe_work, "publish_bundle", stop_before_manifest)
        with pytest.raises(_StopBeforeFinalization):
            run_universe_build(
                _collector(
                    tmp_path,
                    control_plane,
                    _ScriptedTransport(
                        (
                            b'[{"symbol":"synth.active","ipoDate":"2020-01-02"}]',
                            b'[{"symbol":"synth.old","delistedDate":"2021-03-04"}]',
                        )
                    ),
                    run_id,
                ),
                generated_at_utc=NOW,
                destination=destination,
                run_id=run_id,
            )

    approval_checks: list[int] = []

    def expire_after_replays() -> None:
        approval_checks.append(len(approval_checks) + 1)
        if len(approval_checks) == _REPLAY_APPROVAL_CHECKS:
            raise ApprovalExpiredError("synthetic approval expired after final replay")

    resumed_transport = _ScriptedTransport(())
    resumed = _collector(
        tmp_path,
        control_plane,
        resumed_transport,
        run_id,
        approval_check=expire_after_replays,
    )
    with pytest.raises(ApprovalExpiredError, match="after final replay"):
        run_universe_build(
            resumed,
            generated_at_utc=NOW,
            destination=destination,
            run_id=run_id,
        )

    assert approval_checks == [1, 2, 3]
    assert resumed_transport.calls == []
    assert not destination.exists()
    assert not marker_path(resumed, run_id).exists()
    assert control_plane.event_types(run_id) == ("attempt_started",)
    assert control_plane.usage == {}
    assert control_plane.watermarks == {}

    recovery_transport = _ScriptedTransport(())
    recovered = run_universe_build(
        _collector(tmp_path, control_plane, recovery_transport, run_id),
        generated_at_utc=NOW,
        destination=destination,
        run_id=run_id,
    )
    assert recovery_transport.calls == []
    assert recovered.terminal_event is RunEventType.RUN_SUCCEEDED
    assert control_plane.event_types(run_id) == (
        "attempt_started",
        "attempt_succeeded",
        "run_succeeded",
    )
    assert len(control_plane.usage) == _USAGE_METRICS


def test_universe_expiry_blocks_recovery_completion_marker_and_valid_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = _MemoryControlPlane()
    run_id = "fmp-run-universe-recovery-expiry"
    destination = tmp_path / "manifests" / "universe.json"
    initial_transport = _ScriptedTransport(
        (
            b'[{"symbol":"synth.active","ipoDate":"2020-01-02"}]',
            b'[{"symbol":"synth.old","delistedDate":"2021-03-04"}]',
        )
    )
    initial = _collector(tmp_path, control_plane, initial_transport, run_id)
    outcome = run_universe_build(
        initial,
        generated_at_utc=NOW,
        destination=destination,
        run_id=run_id,
    )
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    plan = control_plane.plans[outcome.plan_id]
    publish_required(initial, plan, run_id, destination)
    completed = marker_path(initial, run_id).parent / COMPLETED_NAME
    assert not completed.exists()
    events = control_plane.event_types(run_id)
    usage = dict(control_plane.usage)
    completion_started = False
    original_publish_completed = fmp_universe_run.publish_completed

    def mark_recovery_completion(
        collector: FmpCollector,
        recovery_plan: CollectionRunPlan,
        recovery_run_id: str,
        recovery_destination: Path,
    ) -> None:
        nonlocal completion_started
        completion_started = True
        original_publish_completed(
            collector,
            recovery_plan,
            recovery_run_id,
            recovery_destination,
        )

    def expire() -> None:
        if completion_started:
            raise ApprovalExpiredError("synthetic approval expired before recovery completion")

    resumed_transport = _ScriptedTransport(())
    with monkeypatch.context() as patch:
        patch.setattr(fmp_universe_run, "publish_completed", mark_recovery_completion)
        with pytest.raises(ApprovalExpiredError, match="before recovery completion"):
            run_universe_build(
                _collector(
                    tmp_path,
                    control_plane,
                    resumed_transport,
                    run_id,
                    approval_check=expire,
                ),
                generated_at_utc=NOW,
                destination=destination,
                run_id=run_id,
            )

    assert resumed_transport.calls == []
    assert not completed.exists()
    assert control_plane.event_types(run_id) == events
    assert control_plane.usage == usage

    recovery_transport = _ScriptedTransport(())
    recovered = run_universe_build(
        _collector(tmp_path, control_plane, recovery_transport, run_id),
        generated_at_utc=NOW,
        destination=destination,
        run_id=run_id,
    )
    assert recovered.terminal_event is RunEventType.RUN_SUCCEEDED
    assert recovery_transport.calls == []
    assert completed.exists()


def test_collection_zero_call_replay_parse_failure_after_expiry_writes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = _MemoryControlPlane()
    run_id = "fmp-run-collection-replay-parse-expiry"
    with monkeypatch.context() as patch:
        patch.setattr(
            fmp_collector_run,
            "publish_normalized",
            _collect_then_stop,
        )
        with pytest.raises(_StopBeforeFinalization):
            run_collection(
                _collector(
                    tmp_path,
                    control_plane,
                    _ScriptedTransport(_collection_bodies()),
                    run_id,
                ),
                manifest=_collection_manifest(),
                manifest_sha256="d" * 64,
                created_at_utc=NOW,
                run_id=run_id,
            )

    clock = _MutableClock()
    approval_checks: list[int] = []
    parsed = 0
    original_parse_provider_records = fmp_collector.parse_provider_records
    original_normalize_symbol_observation = fmp_collector.normalize_symbol_observation

    def record_replay_parse() -> None:
        nonlocal parsed
        parsed += 1
        if parsed == _REPLAY_REQUESTS:
            clock.current = NOW + timedelta(minutes=1)
            raise ValueError("synthetic durable replay parse failure")

    def expire_after_final_replay(
        response: CollectorResponse,
    ) -> list[Mapping[str, object]]:
        records = original_parse_provider_records(response)
        record_replay_parse()
        return records

    def expire_after_profile_replay(
        request: SymbolObservationRequest,
        response: CollectorResponse,
    ) -> SymbolObservationResult:
        observation = original_normalize_symbol_observation(request, response)
        record_replay_parse()
        return observation

    def require_current_approval() -> None:
        approval_checks.append(len(approval_checks) + 1)
        if clock.now() > NOW:
            raise ApprovalExpiredError("synthetic approval expired after durable replays")

    resumed_transport = _ScriptedTransport(())
    resumed = _collector(
        tmp_path,
        control_plane,
        resumed_transport,
        run_id,
        approval_check=require_current_approval,
        clock=clock.now,
    )
    with monkeypatch.context() as patch:
        patch.setattr(fmp_collector, "parse_provider_records", expire_after_final_replay)
        patch.setattr(
            fmp_collector,
            "normalize_symbol_observation",
            expire_after_profile_replay,
        )
        with pytest.raises(ApprovalExpiredError, match="after durable replays"):
            run_collection(
                resumed,
                manifest=_collection_manifest(),
                manifest_sha256="d" * 64,
                created_at_utc=NOW,
                run_id=run_id,
            )

    assert parsed == _REPLAY_REQUESTS
    assert approval_checks == [1, 2, 3]
    assert resumed_transport.calls == []
    assert not marker_path(resumed, run_id).exists()
    assert not list((tmp_path / "normalized").rglob("*.parquet"))
    assert not (tmp_path / "receipts" / "run.json").exists()
    assert control_plane.event_types(run_id) == ("attempt_started",)
    assert control_plane.usage == {}
    assert control_plane.watermarks == {}


def test_collection_new_slot_parse_failure_still_records_terminal_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = _MemoryControlPlane()
    run_id = "fmp-run-collection-new-slot-parse-failure"
    clock = _MutableClock(NOW + timedelta(days=1))

    def fail_parse(
        _request: SymbolObservationRequest,
        _response: CollectorResponse,
    ) -> SymbolObservationResult:
        raise ValueError("synthetic new-slot parse failure")

    with monkeypatch.context() as patch:
        patch.setattr(fmp_collector, "normalize_symbol_observation", fail_parse)
        outcome = run_collection(
            _collector(
                tmp_path,
                control_plane,
                _ScriptedTransport((b'[{"symbol":"SYNTH.A","cik":"0000000001"}]',)),
                run_id,
                clock=clock.now,
            ),
            manifest=_collection_manifest(),
            manifest_sha256="d" * 64,
            created_at_utc=NOW,
            run_id=run_id,
            dataset_selection=DatasetSelection.PROFILE,
        )

    assert outcome.terminal_event is RunEventType.RUN_FAILED
    assert control_plane.event_types(run_id) == (
        "attempt_started",
        "attempt_failed",
        "run_failed",
    )
    assert len(control_plane.usage) == _USAGE_METRICS
    assert {record.recorded_at_utc for record in control_plane.usage.values()} == {clock.current}


def test_universe_zero_call_replay_manifest_failure_after_expiry_writes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = _MemoryControlPlane()
    run_id = "fmp-run-universe-replay-parse-expiry"
    destination = tmp_path / "manifests" / "universe.json"
    original_publish_bundle = fmp_universe_work.publish_bundle

    def stop_before_manifest(
        publications: Sequence[tuple[Path, bytes]],
    ) -> tuple[Path, ...]:
        if any(path == destination for path, _content in publications):
            raise _StopBeforeFinalization
        return original_publish_bundle(publications)

    with monkeypatch.context() as patch:
        patch.setattr(fmp_universe_work, "publish_bundle", stop_before_manifest)
        with pytest.raises(_StopBeforeFinalization):
            run_universe_build(
                _collector(
                    tmp_path,
                    control_plane,
                    _ScriptedTransport(
                        (
                            b'[{"symbol":"synth.active","ipoDate":"2020-01-02"}]',
                            b'[{"symbol":"synth.old","delistedDate":"2021-03-04"}]',
                        )
                    ),
                    run_id,
                ),
                generated_at_utc=NOW,
                destination=destination,
                run_id=run_id,
            )

    clock = _MutableClock()
    approval_checks: list[int] = []

    def fail_manifest_after_replays(*_args: object, **_kwargs: object) -> object:
        clock.current = NOW + timedelta(minutes=1)
        raise ValueError("synthetic durable replay manifest failure")

    def require_current_approval() -> None:
        approval_checks.append(len(approval_checks) + 1)
        if clock.now() > NOW:
            raise ApprovalExpiredError("synthetic approval expired after durable replays")

    resumed_transport = _ScriptedTransport(())
    resumed = _collector(
        tmp_path,
        control_plane,
        resumed_transport,
        run_id,
        approval_check=require_current_approval,
        clock=clock.now,
    )
    with monkeypatch.context() as patch:
        patch.setattr(fmp_universe_work, "build_universe_manifest", fail_manifest_after_replays)
        with pytest.raises(ApprovalExpiredError, match="after durable replays"):
            run_universe_build(
                resumed,
                generated_at_utc=NOW,
                destination=destination,
                run_id=run_id,
            )

    assert approval_checks == [1, 2, 3]
    assert resumed_transport.calls == []
    assert not destination.exists()
    assert not marker_path(resumed, run_id).exists()
    assert control_plane.event_types(run_id) == ("attempt_started",)
    assert control_plane.usage == {}
    assert control_plane.watermarks == {}


def test_collection_repairs_expired_registration_before_first_event(tmp_path: Path) -> None:
    control_plane = _StrictMemoryControlPlane()
    run_id = "fmp-run-collection-registration-repair"
    approval_checks: list[int] = []

    def expire_before_attempt_started() -> None:
        approval_checks.append(len(approval_checks) + 1)
        if len(approval_checks) == _REGISTRATION_ATTEMPT_STARTED_CHECK:
            raise ApprovalExpiredError("synthetic approval expired before attempt_started")

    first_transport = _ScriptedTransport(())
    with pytest.raises(ApprovalExpiredError, match="before attempt_started"):
        run_collection(
            _collector(
                tmp_path,
                control_plane,
                first_transport,
                run_id,
                approval_check=expire_before_attempt_started,
            ),
            manifest=_collection_manifest(),
            manifest_sha256="d" * 64,
            created_at_utc=NOW,
            run_id=run_id,
        )

    assert approval_checks == [1, 2, 3, 4]
    assert first_transport.calls == []
    assert len(control_plane.plans) == 1
    assert tuple(control_plane.runs) == (run_id,)
    assert control_plane.event_types(run_id) == ()

    recovery_clock = _MutableClock(NOW + timedelta(days=1))
    recovery_transport = _ScriptedTransport(_collection_bodies())
    recovered = run_collection(
        _collector(
            tmp_path,
            control_plane,
            recovery_transport,
            "fmp-run-collection-registration-candidate",
            clock=recovery_clock.now,
        ),
        manifest=_collection_manifest(),
        manifest_sha256="d" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-collection-registration-candidate",
    )

    assert recovered.run_id == run_id
    assert control_plane.runs[run_id].created_at_utc == NOW
    assert control_plane.event_types(run_id) == (
        "attempt_started",
        "attempt_succeeded",
        "run_succeeded",
    )
    assert len(recovery_transport.calls) == _REPLAY_REQUESTS


def test_universe_repairs_expired_registration_before_first_event(tmp_path: Path) -> None:
    control_plane = _StrictMemoryControlPlane()
    run_id = "fmp-run-universe-registration-repair"
    destination = tmp_path / "manifests" / "universe.json"
    approval_checks: list[int] = []

    def expire_before_attempt_started() -> None:
        approval_checks.append(len(approval_checks) + 1)
        if len(approval_checks) == _REGISTRATION_ATTEMPT_STARTED_CHECK:
            raise ApprovalExpiredError("synthetic approval expired before attempt_started")

    first_transport = _ScriptedTransport(())
    with pytest.raises(ApprovalExpiredError, match="before attempt_started"):
        run_universe_build(
            _collector(
                tmp_path,
                control_plane,
                first_transport,
                run_id,
                approval_check=expire_before_attempt_started,
            ),
            generated_at_utc=NOW,
            destination=destination,
            run_id=run_id,
        )

    assert approval_checks == [1, 2, 3, 4]
    assert first_transport.calls == []
    assert len(control_plane.plans) == 1
    assert tuple(control_plane.runs) == (run_id,)
    assert control_plane.event_types(run_id) == ()

    recovery_clock = _MutableClock(NOW + timedelta(days=1))
    recovery_transport = _ScriptedTransport(
        (
            b'[{"symbol":"synth.active","ipoDate":"2020-01-02"}]',
            b'[{"symbol":"synth.old","delistedDate":"2021-03-04"}]',
        )
    )
    recovered = run_universe_build(
        _collector(
            tmp_path,
            control_plane,
            recovery_transport,
            "fmp-run-universe-registration-candidate",
            clock=recovery_clock.now,
        ),
        generated_at_utc=NOW,
        destination=destination,
        run_id="fmp-run-universe-registration-candidate",
    )

    assert recovered.run_id == run_id
    assert control_plane.runs[run_id].created_at_utc == NOW
    assert control_plane.event_types(run_id) == (
        "attempt_started",
        "attempt_succeeded",
        "run_succeeded",
    )
    assert len(recovery_transport.calls) == _REPLAY_REQUESTS


@pytest.mark.parametrize("expire_on_check", [2, 3])
def test_collection_marker_recovery_ignores_expiry_between_usage_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    expire_on_check: int,
) -> None:
    control_plane = _MemoryControlPlane()
    run_id = f"fmp-run-collection-partial-usage-{expire_on_check}"
    _prepare_collection_marker(tmp_path, control_plane, run_id, monkeypatch)
    clock = _MutableClock()
    approval_checks: list[int] = []

    def expire_between_usage_rows() -> None:
        approval_checks.append(len(approval_checks) + 1)
        if len(approval_checks) == expire_on_check:
            raise ApprovalExpiredError("synthetic approval expired between usage rows")

    resumed_transport = _ScriptedTransport(())
    recovered = run_collection(
        _collector(
            tmp_path,
            control_plane,
            resumed_transport,
            run_id,
            approval_check=expire_between_usage_rows,
            clock=clock.now,
        ),
        manifest=_collection_manifest(),
        manifest_sha256="d" * 64,
        created_at_utc=NOW,
        run_id=run_id,
    )

    assert recovered.terminal_event is RunEventType.RUN_SUCCEEDED
    assert approval_checks == []
    assert resumed_transport.calls == []
    assert len(control_plane.usage) == _USAGE_METRICS
    assert {record.recorded_at_utc for record in control_plane.usage.values()} == {NOW}


def test_partial_usage_retry_stamps_missing_deltas_at_recovery_time(
    tmp_path: Path,
) -> None:
    control_plane = _MemoryControlPlane()
    checks: list[int] = []

    def expire_before_second_metric() -> None:
        checks.append(len(checks) + 1)
        if len(checks) == _SECOND_USAGE_METRIC_CHECK:
            raise ApprovalExpiredError("synthetic approval expired before second metric")

    run_id = "fmp-run-partial-usage-retry-time"
    ledger = UsageLedger(
        calls_attempted=1,
        bytes_received=5,
        retry_after_waits=0,
        rate_limited_attempts=0,
    )
    collector = _collector(
        tmp_path,
        control_plane,
        _ScriptedTransport(()),
        run_id,
        approval_check=expire_before_second_metric,
    )
    persist_at = NOW + timedelta(hours=1)
    recovery_at = NOW + timedelta(days=1)

    with pytest.raises(ApprovalExpiredError, match="before second metric"):
        collector.record_usage(
            run_id=run_id,
            recorded_at_utc=persist_at,
            ledger=ledger,
            require_bound_approval=True,
        )

    _collector(
        tmp_path,
        control_plane,
        _ScriptedTransport(()),
        run_id,
    ).record_usage(
        run_id=run_id,
        recorded_at_utc=recovery_at,
        ledger=ledger,
    )

    assert len(control_plane.usage) == _USAGE_METRICS
    assert {record.recorded_at_utc for record in control_plane.usage.values()} == {
        persist_at,
        recovery_at,
    }


def test_usage_metrics_are_timestamped_at_each_insert_boundary(
    tmp_path: Path,
) -> None:
    control_plane = _MemoryControlPlane()
    clock = _MutableClock(NOW)

    def advance_before_metric() -> None:
        clock.current += timedelta(hours=1)

    collector = _collector(
        tmp_path,
        control_plane,
        _ScriptedTransport(()),
        "fmp-run-per-metric-insert-time",
        approval_check=advance_before_metric,
        clock=clock.now,
    )
    collector.record_usage(
        run_id="fmp-run-per-metric-insert-time",
        recorded_at_utc=NOW,
        ledger=UsageLedger(
            calls_attempted=1,
            bytes_received=5,
            retry_after_waits=0,
            rate_limited_attempts=0,
        ),
        require_bound_approval=True,
    )

    assert [record.recorded_at_utc for record in control_plane.usage.values()] == [
        NOW + timedelta(hours=index) for index in range(1, _USAGE_METRICS + 1)
    ]


def test_collection_recovers_failure_after_expiry_between_failure_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = _StrictMemoryControlPlane()
    run_id = "fmp-run-collection-failure-recovery"
    clock = _MutableClock()
    persist_at = NOW + timedelta(hours=1)
    recovery_at = NOW + timedelta(days=1)

    def expire_after_attempt_failure() -> None:
        if control_plane.event_types(run_id)[-1:] == ("attempt_failed",):
            clock.current = recovery_at
            raise ApprovalExpiredError("synthetic approval expired between failure events")

    def fail_before_transport(*_args: object, **_kwargs: object) -> object:
        if clock.current == NOW:
            clock.current = persist_at
        raise ValueError("synthetic collection failure")

    first_transport = _ScriptedTransport(())
    with monkeypatch.context() as patch:
        patch.setattr(fmp_collector_run, "collect_manifest", fail_before_transport)
        with pytest.raises(ApprovalExpiredError, match="between failure events"):
            run_collection(
                _collector(
                    tmp_path,
                    control_plane,
                    first_transport,
                    run_id,
                    approval_check=expire_after_attempt_failure,
                    clock=clock.now,
                ),
                manifest=_collection_manifest(),
                manifest_sha256="d" * 64,
                created_at_utc=NOW,
                run_id=run_id,
            )

        recovery_transport = _ScriptedTransport(())
        recovered = run_collection(
            _collector(
                tmp_path,
                control_plane,
                recovery_transport,
                run_id,
                clock=clock.now,
            ),
            manifest=_collection_manifest(),
            manifest_sha256="d" * 64,
            created_at_utc=NOW,
            run_id=run_id,
        )

    events = tuple(event for event in control_plane.events if event.run_id == run_id)
    assert first_transport.calls == []
    assert recovery_transport.calls == []
    assert recovered.terminal_event is RunEventType.RUN_FAILED
    assert tuple(event.event_type for event in events) == (
        RunEventType.ATTEMPT_STARTED,
        RunEventType.ATTEMPT_FAILED,
        RunEventType.RUN_FAILED,
    )
    assert events[1].occurred_at_utc == persist_at
    assert events[2].occurred_at_utc == recovery_at
    assert {record.recorded_at_utc for record in control_plane.usage.values()} == {persist_at}


def test_universe_recovers_failure_after_expiry_between_failure_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = _StrictMemoryControlPlane()
    run_id = "fmp-run-universe-failure-recovery"
    destination = tmp_path / "manifests" / "universe.json"
    clock = _MutableClock()
    recovery_at = NOW + timedelta(days=1)

    def expire_after_attempt_failure() -> None:
        if control_plane.event_types(run_id)[-1:] == ("attempt_failed",):
            clock.current = recovery_at
            raise ApprovalExpiredError("synthetic approval expired between failure events")

    def fail_before_transport(*_args: object, **_kwargs: object) -> object:
        raise ValueError("synthetic universe failure")

    first_transport = _ScriptedTransport(())
    with monkeypatch.context() as patch:
        patch.setattr(
            fmp_universe_run,
            "publish_universe_manifest",
            fail_before_transport,
        )
        with pytest.raises(ApprovalExpiredError, match="between failure events"):
            run_universe_build(
                _collector(
                    tmp_path,
                    control_plane,
                    first_transport,
                    run_id,
                    approval_check=expire_after_attempt_failure,
                    clock=clock.now,
                ),
                generated_at_utc=NOW,
                destination=destination,
                run_id=run_id,
            )

        recovery_transport = _ScriptedTransport(())
        recovered = run_universe_build(
            _collector(
                tmp_path,
                control_plane,
                recovery_transport,
                run_id,
                clock=clock.now,
            ),
            generated_at_utc=NOW,
            destination=destination,
            run_id=run_id,
        )

    events = tuple(event for event in control_plane.events if event.run_id == run_id)
    assert first_transport.calls == []
    assert recovery_transport.calls == []
    assert recovered.terminal_event is RunEventType.RUN_FAILED
    assert tuple(event.event_type for event in events) == (
        RunEventType.ATTEMPT_STARTED,
        RunEventType.ATTEMPT_FAILED,
        RunEventType.RUN_FAILED,
    )
    assert events[1].occurred_at_utc == NOW
    assert events[2].occurred_at_utc == recovery_at


def test_failed_run_is_idempotent_and_rejects_nonfailure_state(tmp_path: Path) -> None:
    control_plane = _StrictMemoryControlPlane()
    run_id = "fmp-run-failure-lifecycle-validation"
    collector = _collector(tmp_path, control_plane, _ScriptedTransport(()), run_id)
    plan = build_run_plan(
        mode=CollectionMode.BACKFILL,
        dataset=DatasetSelection.PROFILE.plan_dataset,
        parameters={},
        created_at_utc=NOW,
    )
    collector.register(plan, run_id=run_id)
    collector.fail(
        run_id=run_id,
        error_class="SyntheticFailure",
        error_message="first terminal failure",
    )
    failed_events = tuple(control_plane.events)
    collector.fail(
        run_id=run_id,
        error_class="DifferentFailure",
        error_message="must not create another immutable event",
    )
    assert tuple(control_plane.events) == failed_events

    successful_run_id = "fmp-run-failure-lifecycle-success"
    successful = _collector(tmp_path, control_plane, _ScriptedTransport(()), successful_run_id)
    successful_plan = build_run_plan(
        mode=CollectionMode.BACKFILL,
        dataset=DatasetSelection.PROFILE.plan_dataset,
        parameters={"different": True},
        created_at_utc=NOW,
    )
    successful.register(successful_plan, run_id=successful_run_id)
    successful.succeed(run_id=successful_run_id)
    succeeded_events = tuple(control_plane.events)
    with pytest.raises(CollectorContractError, match="cannot record failure"):
        successful.fail(
            run_id=successful_run_id,
            error_class="SyntheticFailure",
            error_message="must fail closed",
        )
    assert tuple(control_plane.events) == succeeded_events
