from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunEvent,
    RunEventType,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.data import fmp_recurring_recovery
from aegis_alpha.data.fmp_collector import CollectorOutcome, build_run_plan
from aegis_alpha.data.fmp_collector_command import RuntimeDependencies
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_recurring_authority import VerifiedRecurringAuthority
from aegis_alpha.data.fmp_recurring_errors import RecurringAuthorityError
from aegis_alpha.data.fmp_recurring_recovery import (
    PendingMarkerRecovery,
    finalize_pending_markers,
    pending_marker_recoveries,
)
from aegis_alpha.data.fmp_recurring_runtime import RecurringOperation

if TYPE_CHECKING:
    from sqlalchemy import Engine

NOW = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)


def test_recovery_service_day_is_derived_in_utc() -> None:
    local_time = datetime.fromisoformat("2026-08-21T17:30:00-07:00")
    service_day = fmp_recurring_recovery._service_day_utc(local_time)  # noqa: SLF001

    assert service_day == datetime(2026, 8, 22, tzinfo=UTC).date()


def test_pending_marker_recovery_discovers_original_service_day(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    payload_sha256 = "a" * 64
    plan = build_run_plan(
        mode=CollectionMode.INCREMENTAL,
        dataset="fmp_daily_all",
        parameters={"artifact_hashes": {"recurring_authority": payload_sha256}},
        created_at_utc=NOW,
    )
    run_id = "fmp-run-pending-marker"
    registry = CollectionRegistry(clean_postgres)
    registry.register_plan(plan)
    registry.start_run(CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=NOW))
    registry.append_event(
        CollectionRunEvent(
            run_id=run_id,
            event_type=RunEventType.ATTEMPT_STARTED,
            occurred_at_utc=NOW,
            attempt_number=1,
        )
    )
    marker = tmp_path / "fmp" / "runs" / run_id / "publication.json"
    marker.parent.mkdir(parents=True)
    marker.write_bytes(b"{}")
    authority = cast(
        "VerifiedRecurringAuthority",
        cast(
            "object",
            SimpleNamespace(
                payload_sha256=payload_sha256,
                raw_store_root=tmp_path,
                run_identity=lambda *_args, **_kwargs: run_id,
            ),
        ),
    )

    pending = pending_marker_recoveries(
        database_url=clean_postgres.url.render_as_string(hide_password=False),
        authority=authority,
    )

    assert pending == (
        PendingMarkerRecovery(
            run_id=run_id,
            command="collect",
            service_day=NOW.date(),
            created_at_utc=NOW,
            attempt_index=0,
        ),
    )


def test_pending_marker_recovery_passes_shard_coordinates(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    payload_sha256 = "b" * 64
    plan = build_run_plan(
        mode=CollectionMode.INCREMENTAL,
        dataset="fmp_daily_all",
        parameters={
            "artifact_hashes": {"recurring_authority": payload_sha256},
            "shard": [7, 32],
        },
        created_at_utc=NOW,
    )
    run_id = "fmp-run-sharded-pending-marker-0-(7, 32)"
    registry = CollectionRegistry(clean_postgres)
    registry.register_plan(plan)
    registry.start_run(CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=NOW))
    registry.append_event(
        CollectionRunEvent(
            run_id=run_id,
            event_type=RunEventType.ATTEMPT_STARTED,
            occurred_at_utc=NOW,
            attempt_number=1,
        )
    )
    marker = tmp_path / "fmp" / "runs" / run_id / "publication.json"
    marker.parent.mkdir(parents=True)
    marker.write_bytes(b"{}")

    def identity(
        operation: str,
        service_day: date,
        *,
        attempt_index: int = 0,
        shard: tuple[int, int] | None = None,
    ) -> str:
        del operation, service_day
        return f"fmp-run-sharded-pending-marker-{attempt_index}-{shard}"

    authority = cast(
        "VerifiedRecurringAuthority",
        cast(
            "object",
            SimpleNamespace(
                payload_sha256=payload_sha256,
                raw_store_root=tmp_path,
                run_identity=identity,
            ),
        ),
    )

    pending = pending_marker_recoveries(
        database_url=clean_postgres.url.render_as_string(hide_password=False),
        authority=authority,
    )

    assert pending == (
        PendingMarkerRecovery(
            run_id=run_id,
            command="collect",
            service_day=NOW.date(),
            created_at_utc=NOW,
            attempt_index=0,
            shard=(7, 32),
        ),
    )


def test_pending_marker_recovery_rejects_invalid_shard_coordinates(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    payload_sha256 = "c" * 64
    plan = build_run_plan(
        mode=CollectionMode.INCREMENTAL,
        dataset="fmp_daily_all",
        parameters={
            "artifact_hashes": {"recurring_authority": payload_sha256},
            "shard": [33, 32],
        },
        created_at_utc=NOW,
    )
    run_id = "fmp-run-invalid-shard"
    registry = CollectionRegistry(clean_postgres)
    registry.register_plan(plan)
    registry.start_run(CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=NOW))
    marker = tmp_path / "fmp" / "runs" / run_id / "publication.json"
    marker.parent.mkdir(parents=True)
    marker.write_bytes(b"{}")
    authority = cast(
        "VerifiedRecurringAuthority",
        cast(
            "object",
            SimpleNamespace(
                payload_sha256=payload_sha256,
                raw_store_root=tmp_path,
                run_identity=lambda *_args, **_kwargs: run_id,
            ),
        ),
    )

    with pytest.raises(
        RecurringAuthorityError,
        match="pending recurring shard coordinates are invalid",
    ):
        pending_marker_recoveries(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            authority=authority,
        )


def test_pending_backfill_recovery_uses_persisted_historical_service_day(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    payload_sha256 = "a" * 64
    historical_day = NOW.date() - timedelta(days=30)
    plan = build_run_plan(
        mode=CollectionMode.BACKFILL,
        dataset="fmp_daily_all",
        parameters={
            "artifact_hashes": {"recurring_authority": payload_sha256},
            "recurring_recovery": {
                "contract": "fmp-recurring-recovery-v1",
                "manifest_path": str(tmp_path / "historical-manifest.json"),
                "mode": CollectionMode.BACKFILL.value,
                "operator_from": historical_day.isoformat(),
                "output_path": str(tmp_path / "historical-receipt.json"),
                "service_day": historical_day.isoformat(),
            },
        },
        created_at_utc=NOW,
    )
    run_id = "fmp-run-pending-historical-backfill"
    registry = CollectionRegistry(clean_postgres)
    registry.register_plan(plan)
    registry.start_run(CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=NOW))
    registry.append_event(
        CollectionRunEvent(
            run_id=run_id,
            event_type=RunEventType.ATTEMPT_STARTED,
            occurred_at_utc=NOW,
            attempt_number=1,
        )
    )
    marker = tmp_path / "fmp" / "runs" / run_id / "publication.json"
    marker.parent.mkdir(parents=True)
    marker.write_bytes(b"{}")

    def run_identity(
        _command: str,
        service_day: object,
        *,
        attempt_index: int,
        shard: tuple[int, int] | None = None,
    ) -> str:
        del shard
        if service_day == historical_day and attempt_index == 0:
            return run_id
        return f"other-{attempt_index}"

    authority = cast(
        "VerifiedRecurringAuthority",
        cast(
            "object",
            SimpleNamespace(
                payload_sha256=payload_sha256,
                raw_store_root=tmp_path,
                run_identity=run_identity,
            ),
        ),
    )

    pending = pending_marker_recoveries(
        database_url=clean_postgres.url.render_as_string(hide_password=False),
        authority=authority,
    )

    assert pending[0].service_day == historical_day
    assert pending[0].mode is CollectionMode.BACKFILL
    assert pending[0].operator_from == historical_day
    assert pending[0].output_path == tmp_path / "historical-receipt.json"
    assert pending[0].manifest_path == tmp_path / "historical-manifest.json"


def test_finalize_pending_marker_forbids_provider_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "fmp-run-pending-marker"
    candidate = PendingMarkerRecovery(run_id, "collect", NOW.date(), NOW, 1)
    authority = cast(
        "VerifiedRecurringAuthority",
        cast(
            "object",
            SimpleNamespace(
                schedule_id="fmp-daily-refresh-v1",
                output_root=tmp_path,
                payload_sha256="a" * 64,
            ),
        ),
    )
    observed: list[object] = []
    monkeypatch.setattr(
        fmp_recurring_recovery,
        "pending_marker_recoveries",
        lambda **_kwargs: (candidate,),
    )
    operations: list[RecurringOperation] = []

    def authorize(**keywords: object) -> SimpleNamespace:
        operations.append(cast("RecurringOperation", keywords["operation"]))
        return SimpleNamespace(run_id=run_id)

    monkeypatch.setattr(
        fmp_recurring_recovery,
        "authorize_recurring_operation",
        authorize,
    )

    def run(
        *,
        arguments: argparse.Namespace,
        dependencies: RuntimeDependencies,
        **_kwargs: object,
    ) -> tuple[CollectorOutcome, int]:
        observed.extend((arguments.command, dependencies.transport))
        return (
            CollectorOutcome(
                run_id,
                "plan",
                RunEventType.RUN_SUCCEEDED,
                (),
                None,
                (),
                (),
            ),
            4,
        )

    monkeypatch.setattr(fmp_recurring_recovery, "run_authorized_command", run)
    dependencies = RuntimeDependencies(
        transport=None,
        monotonic=lambda: 0.0,
        sleep=lambda _seconds: None,
        approval_clock=lambda: NOW,
        wall_clock=lambda: NOW,
    )

    completed = finalize_pending_markers(
        authority=authority,
        recurring_authority_path=tmp_path / "authority.json",
        recurring_signature_path=tmp_path / "authority.sig",
        registry_path=tmp_path / "registry.json",
        notification_path=tmp_path / "notification.json",
        tier_path=tmp_path / "tier.json",
        environment={"AAS_DATABASE_URL": "postgresql://unused"},
        moment=NOW + timedelta(days=1),
        credential="unused",
        dependencies=dependencies,
    )

    assert completed == (run_id,)
    assert operations[0].attempt_index == 1
    assert observed[0] == "collect"
    forbidden = cast("Callable[[object, str], object]", observed[1])
    with pytest.raises(RecurringAuthorityError, match="provider call"):
        forbidden(object(), "unused")

    historical_day = NOW.date() - timedelta(days=30)
    backfill_candidate = PendingMarkerRecovery(
        run_id=run_id,
        command="collect",
        service_day=historical_day,
        created_at_utc=NOW,
        attempt_index=1,
        mode=CollectionMode.BACKFILL,
        output_path=tmp_path / "historical-receipt.json",
        manifest_path=tmp_path / "historical-manifest.json",
        operator_from=historical_day,
    )
    monkeypatch.setattr(
        fmp_recurring_recovery,
        "pending_marker_recoveries",
        lambda **_kwargs: (backfill_candidate,),
    )
    operations.clear()

    completed = finalize_pending_markers(
        authority=authority,
        recurring_authority_path=tmp_path / "authority.json",
        recurring_signature_path=tmp_path / "authority.sig",
        registry_path=tmp_path / "registry.json",
        notification_path=tmp_path / "notification.json",
        tier_path=tmp_path / "tier.json",
        environment={"AAS_DATABASE_URL": "postgresql://unused"},
        moment=NOW + timedelta(days=1),
        credential="unused",
        dependencies=dependencies,
    )

    assert completed == (run_id,)
    assert operations[0] == RecurringOperation(
        command="collect",
        service_day=historical_day,
        output_path=tmp_path / "historical-receipt.json",
        manifest_path=tmp_path / "historical-manifest.json",
        attempt_index=1,
        mode=CollectionMode.BACKFILL,
        selection=DatasetSelection.ALL,
        operator_from=historical_day,
    )
