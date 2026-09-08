"""Synthetic helpers for AAS-DATA-012 G-A collector tests. Zero network."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

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
from aegis_alpha.collection.registry import CollectionStateError
from aegis_alpha.data.fred_alfred_collector import (
    CollectorConfig,
    CollectorRequest,
    CollectorResponse,
    ControlPlanePort,
    FredAlfredCollector,
)
from aegis_alpha.data.fred_alfred_rate_limit import RateLimiter

FIXTURE_ROOT = (
    Path(__file__).resolve().parents[1] / "fixtures" / "provider_neutral" / "fred_alfred_collector"
)
CREDENTIAL = "SYNTH-FRED-KEY-NEVER-REAL"
NOW = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)
MAX_CALLS = 25


def fixture_bytes(name: str) -> bytes:
    return (FIXTURE_ROOT / name).read_bytes()


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
        return NOW + timedelta(seconds=self.seconds)


@dataclass
class FakeControlPlane:
    plans: dict[str, CollectionRunPlan] = field(default_factory=dict)
    runs: dict[str, CollectionRun] = field(default_factory=dict)
    events: list[CollectionRunEvent] = field(default_factory=list)
    watermarks: dict[tuple[str, str, str], CurrentWatermark] = field(default_factory=dict)
    usage: list[CollectionUsageRecord] = field(default_factory=list)
    advances: list[WatermarkAdvance] = field(default_factory=list)

    def register_plan(self, plan: CollectionRunPlan) -> None:
        self.plans[plan.plan_id] = plan

    def start_run(self, run: CollectionRun) -> None:
        if run.plan_id not in self.plans:
            raise AssertionError("start_run requires a registered plan")
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

    def advance_watermark(self, advance: WatermarkAdvance) -> int:
        run = self.runs.get(advance.run_id)
        if run is None:
            raise ValueError("unknown run_id for watermark advance")
        plan = self.plans.get(run.plan_id)
        if plan is None:
            raise ValueError("unknown plan_id for watermark advance")
        if plan.provider != advance.provider or plan.dataset != advance.dataset:
            raise CollectionStateError("watermark provider/dataset does not match the run plan")
        state = self.current_run_state(advance.run_id)
        if state is None or state.state is not RunEventType.RUN_SUCCEEDED:
            raise CollectionStateError("watermark advance requires a terminal run_succeeded event")
        key = (advance.provider, advance.dataset, advance.stream)
        current = self.watermarks.get(key)
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

    def record_usage(self, record: CollectionUsageRecord) -> None:
        self.usage.append(record)

    @property
    def event_types(self) -> list[str]:
        return [event.event_type.value for event in self.events]


class ScriptedTransport:
    """Replay fixture bytes. A socket is never created."""

    def __init__(
        self,
        responses: Mapping[tuple[str | None, ...], CollectorResponse | Exception],
    ) -> None:
        self._responses = dict(responses)
        self.requests: list[CollectorRequest] = []
        self.credentials: list[str] = []

    def __call__(self, request: CollectorRequest, credential: str) -> CollectorResponse:
        self.requests.append(request)
        self.credentials.append(credential)
        offset = request.parameters.get("offset", "0")
        vintage = request.parameters.get("realtime_start")
        keyed: tuple[str | None, ...] = (
            request.endpoint,
            request.series_id,
            vintage,
            offset,
        )
        legacy: tuple[str | None, ...] = (request.endpoint, request.series_id, vintage)
        if keyed in self._responses:
            item = self._responses[keyed]
        elif offset in {None, "0"} and legacy in self._responses:
            item = self._responses[legacy]
        else:
            raise AssertionError(f"no scripted response for {keyed}")
        if isinstance(item, Exception):
            raise item
        return item


def response_from_fixture(
    name: str,
    *,
    status_code: int = 200,
    clock: FakeClock | None = None,
) -> CollectorResponse:
    moment = NOW if clock is None else clock.now()
    return CollectorResponse(
        status_code=status_code,
        headers={"content-type": "application/json"},
        body=fixture_bytes(name),
        requested_at_utc=moment,
        retrieved_at_utc=moment,
    )


def default_script(
    *,
    clock: FakeClock | None = None,
    fail_series: frozenset[str] = frozenset(),
) -> ScriptedTransport:
    mapping: dict[tuple[str | None, ...], CollectorResponse | Exception] = {}
    for series_id in ("T10Y2Y", "T10Y3M", "DGS10", "DGS2"):
        mapping[("/fred/series", series_id, None)] = response_from_fixture(
            f"series_{series_id}.json", clock=clock
        )
        mapping[("/fred/series/vintagedates", series_id, None)] = response_from_fixture(
            f"vintagedates_{series_id}.json", clock=clock
        )
        vintages = json.loads(fixture_bytes(f"vintagedates_{series_id}.json"))["vintage_dates"]
        for vintage in vintages:
            if series_id in fail_series:
                mapping[("/fred/series/observations", series_id, vintage)] = response_from_fixture(
                    f"observations_{series_id}_{vintage}.json",
                    status_code=500,
                    clock=clock,
                )
            else:
                mapping[("/fred/series/observations", series_id, vintage)] = response_from_fixture(
                    f"observations_{series_id}_{vintage}.json", clock=clock
                )
    return ScriptedTransport(mapping)


def make_config(  # noqa: PLR0913 - explicit synthetic wiring for each collector input
    tmp_path: Path,
    *,
    mode: CollectionMode = CollectionMode.INCREMENTAL,
    series_ids: Sequence[str] = ("T10Y2Y", "T10Y3M", "DGS10", "DGS2"),
    max_calls: int = MAX_CALLS,
    observation_start: date | None = date(2024, 1, 1),
    receipt_name: str = "run.receipt.json",
) -> CollectorConfig:
    return CollectorConfig(
        raw_store_root=tmp_path / "raw",
        dataset_root=tmp_path / "normalized",
        receipt_path=tmp_path / "receipts" / receipt_name,
        mode=mode,
        max_calls=max_calls,
        run_identity="synth-fred-alfred-run-0001",
        series_ids=tuple(series_ids),
        observation_start=observation_start,
    )


def make_collector(  # noqa: PLR0913 - explicit synthetic wiring for each injected port
    tmp_path: Path,
    transport: ScriptedTransport,
    *,
    control_plane: ControlPlanePort | None = None,
    clock: FakeClock | None = None,
    config: CollectorConfig | None = None,
    max_calls: int = MAX_CALLS,
) -> tuple[FredAlfredCollector, ControlPlanePort, FakeClock]:
    resolved_clock = clock or FakeClock()
    resolved_plane = control_plane or FakeControlPlane()
    limiter = RateLimiter(
        max_calls=max_calls,
        clock=resolved_clock.time,
        sleep=resolved_clock.sleep,
    )
    collector = FredAlfredCollector(
        config=config or make_config(tmp_path, max_calls=max_calls),
        transport=transport,
        control_plane=resolved_plane,
        limiter=limiter,
        credential=CREDENTIAL,
        clock=resolved_clock.now,
    )
    return (collector, resolved_plane, resolved_clock)


def observation_keys(rows: Sequence[Mapping[str, Any]]) -> list[tuple[object, ...]]:
    return [
        (
            row["series_id"],
            row["observation_date"],
            row["realtime_start"],
            row["realtime_end"],
        )
        for row in rows
    ]
