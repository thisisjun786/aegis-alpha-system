"""Shared G-A helpers for the SEC verifier collector. Zero network I/O."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

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
from aegis_alpha.data.sec_collector import CollectorConfig, ControlPlanePort, SecCollector
from aegis_alpha.data.sec_identity import Admission, IdentitySnapshot, load_identity_snapshot
from aegis_alpha.data.sec_rate_limit import RateLimiter
from aegis_alpha.data.sec_transport import DatasetKind, make_fixture_transport

FIXTURE_ROOT = (
    Path(__file__).resolve().parents[1] / "fixtures" / "provider_neutral" / "sec_collector"
)
AS_OF = datetime(2026, 8, 18, tzinfo=UTC)
PADDED_CIK = "0000990001"
SOURCE_CIK = "990001"
MAX_CALLS = 8


def synthetic_user_agent() -> str:
    local, host, suffix = "sec-agent", "example", "invalid"
    return f"AegisAlpha {local}@{host}.{suffix}"


class FakeClock:
    """A deterministic clock; tests never sleep for real."""

    def __init__(self) -> None:
        self.seconds = 0.0
        self.waits: list[float] = []

    def time(self) -> float:
        return self.seconds

    def sleep(self, seconds: float) -> None:
        if seconds < 0:
            raise AssertionError("sleep cannot be negative")
        self.waits.append(seconds)
        self.seconds += seconds

    def now(self) -> datetime:
        return datetime(2026, 8, 18, 12, 0, tzinfo=UTC) + timedelta(seconds=self.seconds)


@dataclass
class FakeControlPlane:
    plans: dict[str, CollectionRunPlan] = field(default_factory=dict)
    runs: dict[str, CollectionRun] = field(default_factory=dict)
    events: list[CollectionRunEvent] = field(default_factory=list)
    watermarks: dict[tuple[str, str, str], CurrentWatermark] = field(default_factory=dict)
    usage: list[CollectionUsageRecord] = field(default_factory=list)

    def register_plan(self, plan: CollectionRunPlan) -> None:
        self.plans[plan.plan_id] = plan

    def start_run(self, run: CollectionRun) -> None:
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
            in {RunEventType.RUN_SUCCEEDED, RunEventType.RUN_FAILED, RunEventType.RUN_CANCELLED},
            attempt_count=sum(
                1 for event in events if event.event_type is RunEventType.ATTEMPT_STARTED
            ),
            last_event_seq=None if not events else len(events),
            last_occurred_at_utc=None if last is None else last.occurred_at_utc,
        )

    def advance_watermark(self, advance: WatermarkAdvance) -> int:
        key = (advance.provider, advance.dataset, advance.stream)
        current = self.watermarks.get(key)
        seq = 1 if current is None else current.watermark_seq + 1
        self.watermarks[key] = CurrentWatermark(
            provider=advance.provider,
            dataset=advance.dataset,
            stream=advance.stream,
            watermark_seq=seq,
            run_id=advance.run_id,
            watermark_value=advance.watermark_value,
            watermark_position=advance.watermark_position,
            recorded_at_utc=advance.watermark_position,
        )
        return seq

    def latest_watermark(self, provider: str, dataset: str, stream: str) -> CurrentWatermark | None:
        return self.watermarks.get((provider, dataset, stream))

    def record_usage(self, record: CollectionUsageRecord) -> None:
        self.usage.append(record)


class RecordingIdentity:
    """007 double that records every call and has no write surface."""

    def __init__(self, snapshot: IdentitySnapshot) -> None:
        self.snapshot = snapshot
        self.calls: list[tuple[str, object]] = []

    def requested_instruments(self) -> tuple[str, ...]:
        self.calls.append(("requested_instruments", None))
        return self.snapshot.requested_instruments()

    def admit(self, instrument_id: str, as_of: datetime) -> Admission:
        self.calls.append(("admit", instrument_id))
        return self.snapshot.admit(instrument_id, as_of)

    def insert(self, *_args: object, **_kwargs: object) -> None:
        self.calls.append(("insert", _args))
        raise AssertionError("013 must not write identity tables")


def load_named_identity(name: str) -> IdentitySnapshot:
    return load_identity_snapshot(FIXTURE_ROOT / name)


def fixture_bodies() -> dict[tuple[DatasetKind, str], bytes]:
    return {
        (DatasetKind.SUBMISSIONS, PADDED_CIK): (FIXTURE_ROOT / "submissions.json").read_bytes(),
        (DatasetKind.COMPANYFACTS, PADDED_CIK): (FIXTURE_ROOT / "companyfacts.json").read_bytes(),
    }


def make_collector(  # noqa: PLR0913 - each argument is one synthetic harness knob
    tmp_path: Path,
    *,
    mode: CollectionMode = CollectionMode.PROBE,
    max_calls: int = MAX_CALLS,
    clock: FakeClock | None = None,
    control: ControlPlanePort | None = None,
    user_agent: str | None = None,
    bodies: Mapping[tuple[DatasetKind, str], bytes] | None = None,
) -> tuple[SecCollector, FakeClock, ControlPlanePort]:
    resolved_clock = FakeClock() if clock is None else clock
    resolved_control = FakeControlPlane() if control is None else control
    collector = SecCollector(
        config=CollectorConfig(
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "data",
            receipt_path=tmp_path / "receipts" / "sec.receipt.json",
            mode=mode,
            max_calls=max_calls,
            run_identity="sec-ga-test",
            as_of=AS_OF,
        ),
        transport=make_fixture_transport(
            fixture_bodies() if bodies is None else bodies,
            clock=resolved_clock.now,
        ),
        control_plane=resolved_control,
        limiter=RateLimiter(
            max_calls=max_calls,
            clock=resolved_clock.time,
            sleep=resolved_clock.sleep,
        ),
        clock=resolved_clock.now,
        user_agent=user_agent,
    )
    return collector, resolved_clock, resolved_control


def receipt_view(path: Path) -> Mapping[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def fixture_companyfact_tags() -> frozenset[str]:
    payload = json.loads((FIXTURE_ROOT / "companyfacts.json").read_text(encoding="utf-8"))
    facts = payload["facts"]
    if not isinstance(facts, dict):
        raise TypeError("companyfacts fixture facts must be an object")
    tags: set[str] = set()
    for taxonomy in facts.values():
        if not isinstance(taxonomy, dict):
            raise TypeError("companyfacts fixture taxonomy must be an object")
        tags.update(str(tag) for tag in taxonomy)
    return frozenset(tags)


def read_dataset_column(root: Path, dataset: str, column: str) -> list[object]:
    """Read every partition. A single rglob hit is locale-dependent and incomplete."""

    paths = sorted((root / dataset).rglob("*.parquet"))
    values: list[object] = []
    for path in paths:
        values.extend(pq.read_table(path).column(column).to_pylist())
    return values
