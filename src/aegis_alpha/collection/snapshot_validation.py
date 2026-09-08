from __future__ import annotations

from collections import defaultdict

from sqlalchemy import Connection, RowMapping, select

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRunEvent,
    CollectionRunPlan,
    CollectionUsageRecord,
    RunEventType,
)
from aegis_alpha.collection.registry import CollectionStateError, _validate_transition
from aegis_alpha.collection.schema import (
    collection_run_events,
    collection_run_plans,
    collection_run_receipts,
    collection_runs,
    collection_usage_records,
    collection_watermarks,
)
from aegis_alpha.metadata.schema import source_snapshots


def _plans(connection: Connection) -> dict[str, CollectionRunPlan]:
    plans = {}
    for row in connection.execute(select(collection_run_plans)).mappings():
        plan = CollectionRunPlan(
            plan_id=row["plan_id"],
            schema_version=row["schema_version"],
            provider=row["provider"],
            dataset=row["dataset"],
            mode=CollectionMode(row["mode"]),
            requested_window_start=row["requested_window_start"],
            requested_window_end=row["requested_window_end"],
            parameters=row["parameters_json"],
            created_at_utc=row["created_at_utc"],
        )
        if plan.plan_sha256 != row["plan_sha256"]:
            raise CollectionStateError("snapshot plan digest does not match its definition")
        plans[plan.plan_id] = plan
    return plans


def _events(connection: Connection) -> dict[str, list[RowMapping]]:
    events: dict[str, list[RowMapping]] = defaultdict(list)
    rows = connection.execute(
        select(collection_run_events).order_by(
            collection_run_events.c.run_id, collection_run_events.c.event_seq
        )
    ).mappings()
    for row in rows:
        previous = events[row["run_id"]]
        if row["event_seq"] != len(previous) + 1:
            raise CollectionStateError("snapshot event sequence has a gap")
        event = CollectionRunEvent(
            run_id=row["run_id"],
            event_type=RunEventType(row["event_type"]),
            occurred_at_utc=row["occurred_at_utc"],
            attempt_number=row["attempt_number"],
            retry_of_attempt=row["retry_of_attempt"],
            error_class=row["error_class"],
            error_message=row["error_message"],
            details=row["details_json"],
        )
        last = previous[-1] if previous else None
        _validate_transition(last, event)
        if last is not None and event.occurred_at_utc < last["occurred_at_utc"]:
            raise CollectionStateError("snapshot events move backwards in time")
        previous.append(row)
    return events


def _receipts(
    connection: Connection, runs: dict[str, CollectionRunPlan], events: dict[str, list[RowMapping]]
) -> None:
    snapshots = {
        row["snapshot_id"]: (row["provider"], row["dataset"])
        for row in connection.execute(
            select(
                source_snapshots.c.snapshot_id,
                source_snapshots.c.provider,
                source_snapshots.c.dataset,
            )
        ).mappings()
    }
    for row in connection.execute(select(collection_run_receipts)).mappings():
        plan = runs[row["run_id"]]
        if snapshots.get(row["source_snapshot_id"]) != (plan.provider, plan.dataset):
            raise CollectionStateError("snapshot receipt source lineage disagrees with run plan")
        if not any(
            e["event_type"] == RunEventType.ATTEMPT_STARTED.value
            and e["attempt_number"] == row["attempt_number"]
            for e in events.get(row["run_id"], [])
        ):
            raise CollectionStateError("snapshot receipt references an unstarted attempt")


def _watermarks(
    connection: Connection, runs: dict[str, CollectionRunPlan], events: dict[str, list[RowMapping]]
) -> None:
    streams: dict[tuple[str, str, str], RowMapping] = {}
    seen_runs: set[tuple[str, str, str, str]] = set()
    rows = connection.execute(
        select(collection_watermarks).order_by(
            collection_watermarks.c.provider,
            collection_watermarks.c.dataset,
            collection_watermarks.c.stream,
            collection_watermarks.c.watermark_seq,
        )
    ).mappings()
    for row in rows:
        plan = runs[row["run_id"]]
        # Same explicit multi-dataset rule used by CollectionRegistry.advance_watermark.
        declared = plan.parameters.get("datasets", ())
        allowed = row["dataset"] == plan.dataset or (
            isinstance(declared, (tuple, list))
            and all(isinstance(item, str) for item in declared)
            and row["dataset"] in declared
        )
        history = events.get(row["run_id"], [])
        if plan.provider != row["provider"] or not allowed:
            raise CollectionStateError("snapshot watermark lineage disagrees with run plan")
        if not history or history[-1]["event_type"] != RunEventType.RUN_SUCCEEDED.value:
            raise CollectionStateError("snapshot watermark belongs to an unsuccessful run")
        key = (row["provider"], row["dataset"], row["stream"])
        previous = streams.get(key)
        expected = 1 if previous is None else previous["watermark_seq"] + 1
        if row["watermark_seq"] != expected or (
            previous is not None and row["watermark_position"] <= previous["watermark_position"]
        ):
            raise CollectionStateError("snapshot watermark sequence or position is not increasing")
        identity = (*key, row["run_id"])
        if identity in seen_runs:
            raise CollectionStateError("snapshot run advances the same stream more than once")
        seen_runs.add(identity)
        streams[key] = row


def validate_collection_snapshot(connection: Connection) -> None:
    """Validate restored history without replaying registration or altering any timestamp."""
    plans = _plans(connection)
    runs = {
        row["run_id"]: plans[row["plan_id"]]
        for row in connection.execute(select(collection_runs)).mappings()
    }
    events = _events(connection)
    _receipts(connection, runs, events)
    _watermarks(connection, runs, events)
    for row in connection.execute(select(collection_usage_records)).mappings():
        CollectionUsageRecord(
            run_id=row["run_id"],
            usage_seq=row["usage_seq"],
            metric=row["metric"],
            quantity=row["quantity"],
            unit=row["unit"],
            recorded_at_utc=row["recorded_at_utc"],
            evidence=row["evidence_json"],
        )
