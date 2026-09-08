"""Automatic zero-provider-call finalization of pending recurring markers."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final

from sqlalchemy import create_engine, exists, select

from aegis_alpha.collection.records import CollectionMode, RunEventType
from aegis_alpha.collection.schema import (
    collection_run_events,
    collection_run_plans,
    collection_runs,
)
from aegis_alpha.data.fmp_collector import CollectorRequest, CollectorResponse
from aegis_alpha.data.fmp_collector_command import RuntimeDependencies, run_authorized_command
from aegis_alpha.data.fmp_daily_refresh import build_daily_refresh_plan
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_normalize import PROVIDER
from aegis_alpha.data.fmp_recurring_authority import VerifiedRecurringAuthority
from aegis_alpha.data.fmp_recurring_errors import RecurringAuthorityError
from aegis_alpha.data.fmp_recurring_runs import recurring_attempt_index
from aegis_alpha.data.fmp_recurring_runtime import (
    RecurringOperation,
    authorize_recurring_operation,
)

_TERMINAL_EVENTS = tuple(
    event.value
    for event in (
        RunEventType.RUN_SUCCEEDED,
        RunEventType.RUN_FAILED,
        RunEventType.RUN_CANCELLED,
    )
)
_DATASET_COMMAND = {
    "fmp_daily_all": "collect",
    "fmp_universe": "build-universe",
}


@dataclass(frozen=True, slots=True)
class PendingMarkerRecovery:
    run_id: str
    command: str
    service_day: date
    created_at_utc: datetime
    attempt_index: int
    mode: CollectionMode = CollectionMode.INCREMENTAL
    output_path: Path | None = None
    manifest_path: Path | None = None
    operator_from: date | None = None
    shard: tuple[int, int] | None = None


_SHARD_COORDINATE_LENGTH: Final = 2


def _service_day_utc(created_at: datetime) -> date:
    return created_at.astimezone(UTC).date()


def _shard_from_parameters(
    parameters: Mapping[str, object] | None,
) -> tuple[int, int] | None:
    value = parameters.get("shard") if isinstance(parameters, Mapping) else None
    if value is None:
        return None
    if not isinstance(value, (list, tuple)) or len(value) != _SHARD_COORDINATE_LENGTH:
        raise RecurringAuthorityError("pending recurring shard coordinates are invalid") from None
    first, second = value[0], value[1]
    if (
        isinstance(first, bool)
        or isinstance(second, bool)
        or not isinstance(first, int)
        or not isinstance(second, int)
    ):
        raise RecurringAuthorityError("pending recurring shard coordinates are invalid") from None
    shard = (first, second)
    if shard[1] < 1 or shard[0] < 1 or shard[0] > shard[1]:
        raise RecurringAuthorityError("pending recurring shard coordinates are invalid") from None
    return shard


def pending_marker_recoveries(
    *,
    database_url: str,
    authority: VerifiedRecurringAuthority,
) -> tuple[PendingMarkerRecovery, ...]:
    terminal = exists(
        select(collection_run_events.c.run_id).where(
            collection_run_events.c.run_id == collection_runs.c.run_id,
            collection_run_events.c.event_type.in_(_TERMINAL_EVENTS),
        )
    )
    statement = (
        select(
            collection_runs.c.run_id,
            collection_run_plans.c.dataset,
            collection_run_plans.c.mode,
            collection_run_plans.c.parameters_json,
            collection_run_plans.c.created_at_utc,
        )
        .join(
            collection_run_plans,
            collection_run_plans.c.plan_id == collection_runs.c.plan_id,
        )
        .where(
            collection_run_plans.c.provider == PROVIDER,
            collection_run_plans.c.dataset.in_(tuple(_DATASET_COMMAND)),
            ~terminal,
        )
    )
    engine = create_engine(database_url, pool_pre_ping=True, hide_parameters=True)
    try:
        with engine.connect() as connection:
            rows = connection.execute(statement).all()
    finally:
        engine.dispose()
    pending = []
    for run_id, dataset, mode_value, parameters, created_at_utc in rows:
        hashes = parameters.get("artifact_hashes") if isinstance(parameters, Mapping) else None
        if (
            not isinstance(hashes, Mapping)
            or hashes.get("recurring_authority") != authority.payload_sha256
            or not marker_path_for(authority.raw_store_root, str(run_id)).is_file()
        ):
            continue
        shard = _shard_from_parameters(parameters)
        command = _DATASET_COMMAND[str(dataset)]
        try:
            mode = CollectionMode(mode_value)
        except ValueError:
            raise RecurringAuthorityError("pending recurring mode is invalid") from None
        output_path = None
        manifest_path = None
        operator_from = None
        if mode is CollectionMode.BACKFILL:
            recovery = parameters.get("recurring_recovery")
            contract = recovery.get("contract") if isinstance(recovery, Mapping) else None
            recovery_mode = recovery.get("mode") if isinstance(recovery, Mapping) else None
            service_day_text = (
                recovery.get("service_day") if isinstance(recovery, Mapping) else None
            )
            operator_from_text = (
                recovery.get("operator_from") if isinstance(recovery, Mapping) else None
            )
            output_path_text = (
                recovery.get("output_path") if isinstance(recovery, Mapping) else None
            )
            manifest_path_text = (
                recovery.get("manifest_path") if isinstance(recovery, Mapping) else None
            )
            if (
                not isinstance(recovery, Mapping)
                or contract != "fmp-recurring-recovery-v1"
                or recovery_mode != CollectionMode.BACKFILL.value
                or not isinstance(service_day_text, str)
                or not isinstance(operator_from_text, str)
                or not isinstance(output_path_text, str)
                or not isinstance(manifest_path_text, str)
            ):
                raise RecurringAuthorityError("pending backfill recovery inputs are unavailable")
            try:
                service_day = date.fromisoformat(service_day_text)
                operator_from = date.fromisoformat(operator_from_text)
            except ValueError:
                raise RecurringAuthorityError(
                    "pending backfill recovery dates are invalid"
                ) from None
            if service_day != operator_from:
                raise RecurringAuthorityError("pending backfill recovery identity is inconsistent")
            output_path = Path(output_path_text)
            manifest_path = Path(manifest_path_text)
        else:
            service_day = _service_day_utc(created_at_utc)
        pending.append(
            PendingMarkerRecovery(
                run_id=str(run_id),
                command=command,
                service_day=service_day,
                created_at_utc=created_at_utc,
                attempt_index=recurring_attempt_index(
                    authority,
                    command,
                    service_day,
                    str(run_id),
                    shard,
                ),
                mode=mode,
                output_path=output_path,
                manifest_path=manifest_path,
                operator_from=operator_from,
                shard=shard,
            )
        )
    return tuple(sorted(pending, key=lambda item: (item.created_at_utc, item.run_id)))


def marker_path_for(raw_store_root: Path, run_id: str) -> Path:
    return raw_store_root / "fmp" / "runs" / run_id / "publication.json"


def _forbid_provider_call(
    _request: CollectorRequest,
    _credential: str,
) -> CollectorResponse:
    raise RecurringAuthorityError("marker-only recovery attempted a provider call")


def finalize_pending_markers(  # noqa: PLR0913 - explicit recovery inputs
    *,
    authority: VerifiedRecurringAuthority,
    recurring_authority_path: Path,
    recurring_signature_path: Path,
    registry_path: Path,
    notification_path: Path,
    tier_path: Path,
    environment: Mapping[str, str],
    moment: datetime,
    credential: str,
    dependencies: RuntimeDependencies,
) -> tuple[str, ...]:
    database_url = environment["AAS_DATABASE_URL"]
    completed = []
    for candidate in pending_marker_recoveries(
        database_url=database_url,
        authority=authority,
    ):
        if candidate.mode is CollectionMode.BACKFILL:
            if (
                candidate.command != "collect"
                or candidate.output_path is None
                or candidate.manifest_path is None
                or candidate.operator_from is None
            ):
                raise RecurringAuthorityError("pending backfill recovery inputs are invalid")
            operation = RecurringOperation(
                command="collect",
                service_day=candidate.service_day,
                output_path=candidate.output_path,
                manifest_path=candidate.manifest_path,
                attempt_index=candidate.attempt_index,
                mode=CollectionMode.BACKFILL,
                selection=DatasetSelection.ALL,
                operator_from=candidate.operator_from,
            )
        else:
            plan = build_daily_refresh_plan(
                schedule_id=authority.schedule_id,
                service_day=candidate.service_day,
                output_root=authority.output_root,
                authority_payload_sha256=authority.payload_sha256,
            )
            operation = RecurringOperation(
                command=candidate.command,
                service_day=candidate.service_day,
                output_path=(
                    plan.receipt_path if candidate.command == "collect" else plan.universe_path
                ),
                manifest_path=(
                    plan.collection_manifest_path if candidate.command == "collect" else None
                ),
                attempt_index=candidate.attempt_index,
                shard=candidate.shard,
            )
        authorization = authorize_recurring_operation(
            operation=operation,
            recurring_authority_path=recurring_authority_path,
            recurring_signature_path=recurring_signature_path,
            registry_path=registry_path,
            notification_path=notification_path,
            tier_path=tier_path,
            environment=environment,
            moment=moment,
            approval_clock=dependencies.approval_clock,
            allow_historical_service_day=True,
        )
        if authorization.run_id != candidate.run_id:
            raise RecurringAuthorityError("pending marker selected a different recurring run")
        outcome, _restored_calls = run_authorized_command(
            arguments=argparse.Namespace(command=candidate.command),
            authorization=authorization,
            environment=environment,
            credential=credential,
            moment=candidate.created_at_utc,
            dependencies=replace(dependencies, transport=_forbid_provider_call),
            shard=candidate.shard,
        )
        if outcome.terminal_event is not RunEventType.RUN_SUCCEEDED:
            raise RecurringAuthorityError("pending marker finalization did not succeed")
        completed.append(candidate.run_id)
    return tuple(completed)
