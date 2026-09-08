"""Durable FRED accounting followed by atomic catalog and terminal registration."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import fields, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionReceipt,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    CollectionUsageRecord,
    RunEventType,
    WatermarkAdvance,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.data.contracts import SourceSnapshot, ValidationStatus
from aegis_alpha.data.fred_alfred_collector import (
    CollectorConfig,
    CollectorError,
    CollectorOutcome,
    build_run_plan,
)
from aegis_alpha.data.fred_alfred_evidence import (
    capture_source,
    dataset_registration,
    publish_evidence,
    read_evidence,
    register_source,
    verify_publication,
)
from aegis_alpha.data.fred_alfred_rate_limit import UsageLedger
from aegis_alpha.data.fred_alfred_recurring_authority import VerifiedRecurringAuthority
from aegis_alpha.data.fred_alfred_recurring_errors import DailyBudgetError
from aegis_alpha.data.fred_alfred_series import (
    PLAN_DATASET,
    PROVIDER,
    watermark_dataset,
    watermark_stream,
)
from aegis_alpha.data.fred_alfred_usage_budget import (
    DailyUsageBudget,
    admit_requested_calls,
    record_run_consumption,
    settled_consumption_matches,
    utc_day_window,
)
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.metadata.registry import MetadataRegistry

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine


class FredRuntime:
    def __init__(
        self,
        config: CollectorConfig,
        engine: Engine,
        authority: VerifiedRecurringAuthority,
        clock: Callable[[], datetime],
    ) -> None:
        self.config, self.engine, self.authority, self.clock = config, engine, authority, clock
        self.registry = CollectionRegistry(engine)
        self.metadata = MetadataRegistry(engine)
        self.snapshots: list[SourceSnapshot] = []
        self.reservation: DailyUsageBudget | None = None
        self.admission_path = config.receipt_path.with_name(
            config.receipt_path.name + ".admission.json"
        )
        self.ready_path = config.receipt_path.with_name(config.receipt_path.name + ".ready.json")
        identity = {item.name: getattr(config, item.name) for item in fields(config)}
        for name in ("raw_store_root", "dataset_root", "receipt_path"):
            identity[name] = str(identity[name])
        self.config_digest = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        self.recorded_at: datetime | None = None

    def plan(self, created: datetime) -> CollectionRunPlan:
        return build_run_plan(
            mode=self.config.mode,
            series_ids=self.config.series_ids,
            created_at_utc=created,
            extra_parameters={
                "max_calls": self.config.max_calls,
                "observation_start": None
                if self.config.observation_start is None
                else self.config.observation_start.isoformat(),
                "runtime_config_sha256": self.config_digest,
            },
        )

    def require_request(self) -> None:
        now = self.clock()
        self.authority.require_request(now)
        if self.reservation is None or utc_day_window(now)[0] != self.reservation.day_start_utc:
            raise DailyBudgetError("FRED request lacks a current UTC-day reservation")

    def start(self, plan: CollectionRunPlan, run_id: str) -> None:
        with self.registry.begin_registration() as connection:
            self.registry.register_plan(plan, connection=connection)
            if not self.registry.create_or_observe_run(
                CollectionRun(run_id, plan.plan_id, plan.created_at_utc), connection=connection
            ):
                raise CollectorError("existing FRED run requires verified recovery evidence")
            self.registry.append_event(
                CollectionRunEvent(
                    run_id, RunEventType.ATTEMPT_STARTED, plan.created_at_utc, attempt_number=1
                ),
                connection=connection,
            )
        self.reservation = admit_requested_calls(
            engine=self.engine,
            authority=self.authority,
            requested_calls=self.config.max_calls,
            now=self.clock(),
            actual_run_id=run_id,
            clock=self.clock,
        )
        publish_evidence(
            self.admission_path,
            canonical_json_bytes(
                {
                    "config_sha256": self.config_digest,
                    "run_id": run_id,
                    "plan_id": plan.plan_id,
                    "created_at_utc": plan.created_at_utc,
                    "reservation": self.reservation,
                }
            ),
        )

    def capture(self, snapshot: SourceSnapshot, path: Path) -> None:
        register_source(self.metadata, self.config.raw_store_root, snapshot, path)
        self.snapshots.append(snapshot)

    def aggregate_source(
        self, plan: CollectionRunPlan, run_id: str, *, publish: bool = True
    ) -> SourceSnapshot:
        body = canonical_json_bytes(
            {"run_id": run_id, "plan_id": plan.plan_id, "sources": self.snapshots}
        )
        digest = hashlib.sha256(body).hexdigest()
        snapshot = SourceSnapshot(
            snapshot_id=f"fred-run-{hashlib.sha256(run_id.encode()).hexdigest()}",
            schema_version=1,
            provider=PROVIDER,
            dataset=PLAN_DATASET,
            source_uri="urn:aas:fred:run:" + run_id,
            request_fingerprint="sha256:" + digest,
            parameters={"run_id": run_id},
            requested_at_utc=plan.created_at_utc,
            retrieved_at_utc=max(
                [plan.created_at_utc, *(s.retrieved_at_utc for s in self.snapshots)]
            ),
            content_type="application/json",
            encoding="utf-8",
            compression=None,
            raw_byte_length=len(body),
            content_sha256=digest,
            parser_name="fred-run-sources",
            parser_version="1",
            validation_status=ValidationStatus.PASS,
        )
        if publish:
            capture_source(self.config.raw_store_root, snapshot, body)
        return snapshot

    def finalize(
        self, plan: CollectionRunPlan, outcome: CollectorOutcome, usage: UsageLedger
    ) -> CollectorOutcome:
        if self.reservation is None:
            raise CollectorError("FRED finalization requires a durable reservation")
        if outcome.receipt_path is not None:
            receipt = read_evidence(outcome.receipt_path)
            document = json.loads(receipt)
            self.recorded_at = datetime.fromisoformat(document["collected_at_utc"])
            publish_evidence(
                self.ready_path,
                canonical_json_bytes(
                    {
                        "config_sha256": self.config_digest,
                        "receipt_sha256": hashlib.sha256(receipt).hexdigest(),
                    }
                ),
            )
        else:
            self.recorded_at = self.clock()
        source = self.aggregate_source(plan, outcome.run_id)
        if outcome.receipt_path is not None:
            verify_publication(self.config)
        catalog = dataset_registration(
            self.config, outcome, (*self.snapshots, source), self.recorded_at
        )
        if self._terminal_run(plan, outcome):
            raise CollectorError("terminal FRED run requires persisted replay verification")
        # Known HTTP consumption is durable before the all-or-nothing publication
        # finalization. A failed finalization must not erase already-attempted calls.
        self._account(outcome, usage, self.reservation, self.recorded_at)
        terminal_at = self.clock()
        advanced: list[tuple[str, str]] = []
        with self.registry.begin_registration() as connection:
            if self._terminal_run(plan, outcome, connection=connection):
                raise CollectorError("FRED run became terminal before finalization")
            source_path = (
                self.config.raw_store_root
                / "blobs"
                / "sha256"
                / source.content_sha256[:2]
                / f"{source.content_sha256}.raw"
            )
            register_source(
                self.metadata,
                self.config.raw_store_root,
                source,
                source_path,
                connection=connection,
            )
            self.registry.record_receipt(
                CollectionReceipt(
                    outcome.run_id,
                    1,
                    source.snapshot_id,
                    row_count=len(outcome.rows),
                    byte_count=usage.bytes_received,
                    receipt_sha256=(
                        hashlib.sha256(receipt).hexdigest()
                        if outcome.receipt_path is not None
                        else source.content_sha256
                    ),
                ),
                connection=connection,
            )
            if catalog is not None:
                self.metadata.register_dataset(
                    catalog,
                    connection=connection,
                    precommit_guard=lambda: verify_publication(self.config),
                )
            success = outcome.terminal_event is RunEventType.RUN_SUCCEEDED
            self.registry.append_event(
                CollectionRunEvent(
                    outcome.run_id,
                    RunEventType.ATTEMPT_SUCCEEDED if success else RunEventType.ATTEMPT_FAILED,
                    terminal_at,
                    attempt_number=1,
                    error_class=None if success else "CollectorError",
                    error_message=None if success else "collection failed",
                ),
                connection=connection,
            )
            self.registry.append_event(
                CollectionRunEvent(
                    outcome.run_id,
                    outcome.terminal_event,
                    terminal_at,
                    error_class=None if success else "CollectorError",
                    error_message=None if success else "collection failed",
                ),
                connection=connection,
            )
            if success:
                advanced = self._watermarks(outcome, connection)
        return replace(
            outcome, watermarks_advanced=tuple(advanced), calls_attempted=usage.calls_attempted
        )

    def _account(
        self,
        outcome: CollectorOutcome,
        usage: UsageLedger,
        reservation: DailyUsageBudget,
        recorded: datetime,
    ) -> None:
        if utc_day_window(self.clock())[0] != reservation.day_start_utc:
            record = CollectionUsageRecord(
                outcome.run_id,
                1,
                "calls_attempted",
                Decimal(usage.calls_attempted),
                "call",
                recorded,
            )
            if not settled_consumption_matches(self.engine, record, reservation):
                raise DailyBudgetError(
                    "FRED prior-day usage remains uncertain; reservation retained"
                )
        else:
            record_run_consumption(
                engine=self.engine,
                database_settlement=True,
                run_id=outcome.run_id,
                calls_attempted=usage.calls_attempted,
                now=recorded,
                reservation=reservation,
            )
        self.registry.record_usage(
            CollectionUsageRecord(
                outcome.run_id,
                2,
                "bytes_received",
                Decimal(usage.bytes_received),
                "byte",
                recorded,
            ),
        )

    def _terminal_run(
        self,
        plan: CollectionRunPlan,
        outcome: CollectorOutcome,
        *,
        connection: Connection | None = None,
    ) -> bool:
        state = self.registry.current_run_state(outcome.run_id, connection=connection)
        if state is None or state.plan_id != plan.plan_id:
            raise CollectorError("FRED recovery run binding differs")
        if state.terminal and state.state is not outcome.terminal_event:
            raise CollectorError("FRED finalization conflicts with terminal state")
        return state.terminal

    def _watermarks(
        self, outcome: CollectorOutcome, connection: Connection
    ) -> list[tuple[str, str]]:
        advanced: list[tuple[str, str]] = []
        if self.config.mode is CollectionMode.PROBE:
            return advanced
        for series in outcome.series_outcomes:
            if series.succeeded and series.vintage_watermark is not None:
                position = datetime.combine(
                    series.vintage_watermark, datetime.min.time(), tzinfo=UTC
                )
                current = self.registry.latest_watermark(
                    PROVIDER,
                    watermark_dataset(series.series_id),
                    watermark_stream(series.series_id),
                )
                if current is not None and position <= current.watermark_position:
                    continue
                self.registry.advance_watermark(
                    WatermarkAdvance(
                        PROVIDER,
                        watermark_dataset(series.series_id),
                        watermark_stream(series.series_id),
                        outcome.run_id,
                        series.vintage_watermark.isoformat(),
                        position,
                    ),
                    connection=connection,
                )
                advanced.append((series.series_id, series.vintage_watermark.isoformat()))
        return advanced
