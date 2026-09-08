"""Resumable top-level lifecycle driver for AAS-DATA-004C."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import cast

from aegis_alpha.collection.records import RunEventType
from aegis_alpha.data.fmp_approval import (
    ApprovalExpiredError,
    fail_closed_if_unconsumed_expiry,
    require_approved_identity,
)
from aegis_alpha.data.fmp_collection_work import (
    collect_manifest,
    collection_parameters,
    publish_normalized,
)
from aegis_alpha.data.fmp_collector import (
    CollectorLockLostError,
    CollectorOutcome,
    FmpCollector,
    build_run_plan,
    plan_resume_record,
    publish_bundle,
)
from aegis_alpha.data.fmp_collector_state import (
    _validate_complete_artifact_inventory,
    marker_path,
    normalized_parts_exist,
    read_marker,
    resolve_resume,
)
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_marker_finalization import (
    PostSuccessWatermarkError,
    finalize_collection_marker,
)
from aegis_alpha.data.fmp_rate_limit import BudgetExhaustedError
from aegis_alpha.data.fmp_windows import UniverseManifest, partition_universe_manifest
from aegis_alpha.data.serialization import canonical_json_bytes


class LifecycleRecoveryRequiredError(RuntimeError):
    """A committed lifecycle boundary must be completed by exact resume."""


def _persist_usage_before_terminal(
    collector: FmpCollector,
    *,
    run_id: str,
    recorded_at_utc: datetime,
    transition: str,
    require_bound_approval: bool,
) -> None:
    try:
        collector.record_usage(
            run_id=run_id,
            recorded_at_utc=recorded_at_utc,
            require_bound_approval=require_bound_approval,
        )
    except ApprovalExpiredError:
        raise
    except Exception as error:
        raise LifecycleRecoveryRequiredError(
            f"usage persistence failed before {transition}; same-run recovery is required"
        ) from error


def run_collection(  # noqa: C901, PLR0913 - lifecycle boundaries remain explicit
    collector: FmpCollector,
    *,
    manifest: UniverseManifest,
    manifest_sha256: str,
    created_at_utc: datetime,
    run_id: str,
    dataset_selection: DatasetSelection = DatasetSelection.PROBE,
    approved_run_identity: str | None = None,
) -> CollectorOutcome:
    shard = collector.config.shard
    if shard is not None:
        manifest = partition_universe_manifest(manifest, shard_index=shard[0], shard_total=shard[1])
    plan = build_run_plan(
        mode=collector.config.mode,
        dataset=dataset_selection.plan_dataset,
        parameters=collection_parameters(collector, manifest, manifest_sha256, dataset_selection),
        created_at_utc=created_at_utc,
    )
    run_id, state, plan = resolve_resume(
        collector,
        plan,
        run_id,
        reuse_successful_candidate=True,
    )
    require_approved_identity(approved_run_identity, run_id)
    collector.resume_as(run_id)
    if state is not None and state.state is RunEventType.RUN_SUCCEEDED:
        _validate_complete_artifact_inventory(collector, run_id, state.plan_id)
    resume_path = (
        collector.config.raw_store_root / "fmp" / "plans" / f"{plan.plan_id}-{run_id}.json"
    )
    if state is None:
        collector.require_bound_approval()
        publish_bundle(
            [(resume_path, canonical_json_bytes(plan_resume_record(plan, run_id=run_id)))]
        )
    if state is None or state.state is None:
        collector.register(plan, run_id=run_id)
    calls_at_entry = collector.calls_attempted
    try:
        publication = read_marker(collector, run_id)
        marker_recovery = publication is not None
        if publication is None:
            publication = publish_normalized(
                collector,
                run_id,
                collect_manifest(collector, manifest, dataset_selection),
                plan_id=plan.plan_id,
            )
        advanced = finalize_collection_marker(
            collector,
            run_id=run_id,
            plan=plan,
            publication=publication,
            marker_recovery=marker_recovery,
        )
        return CollectorOutcome(
            run_id,
            plan.plan_id,
            RunEventType.RUN_SUCCEEDED,
            tuple(
                Path(item["path"])
                for item in cast("list[dict[str, str]]", publication["artifacts"])
            ),
            collector.receipt_destination(),
            collector.quality_results,
            advanced,
            collector.blocked_symbols,
        )
    except ApprovalExpiredError as error:
        current = collector._control_plane.current_run_state(run_id)  # noqa: SLF001
        fail_closed_if_unconsumed_expiry(
            error,
            consumed_new_slot=collector.calls_attempted != calls_at_entry,
            durable_boundary_committed=(
                marker_path(collector, run_id).is_file()
                or (
                    current is not None
                    and current.state
                    in {RunEventType.ATTEMPT_SUCCEEDED, RunEventType.RUN_SUCCEEDED}
                )
            ),
        )
        require_bound_approval = collector.calls_attempted == calls_at_entry
        _persist_usage_before_terminal(
            collector,
            run_id=run_id,
            recorded_at_utc=collector._clock(),  # noqa: SLF001
            transition="cancellation",
            require_bound_approval=require_bound_approval,
        )
        collector.cancel(
            run_id=run_id,
            reason=type(error).__name__,
            require_bound_approval=require_bound_approval,
        )
        return CollectorOutcome(
            run_id,
            plan.plan_id,
            RunEventType.RUN_CANCELLED,
            (),
            None,
            collector.quality_results,
            (),
            collector.blocked_symbols,
            type(error).__name__,
            error_class=type(error).__name__,
        )
    except (
        BudgetExhaustedError,
        CollectorLockLostError,
        KeyboardInterrupt,
    ) as error:
        if normalized_parts_exist(collector, run_id):
            raise LifecycleRecoveryRequiredError(
                "durable normalized part committed; same-run completion is required"
            ) from error
        require_bound_approval = collector.calls_attempted == calls_at_entry
        _persist_usage_before_terminal(
            collector,
            run_id=run_id,
            recorded_at_utc=collector._clock(),  # noqa: SLF001
            transition="cancellation",
            require_bound_approval=require_bound_approval,
        )
        collector.cancel(
            run_id=run_id,
            reason=type(error).__name__,
            require_bound_approval=require_bound_approval,
        )
        return CollectorOutcome(
            run_id,
            plan.plan_id,
            RunEventType.RUN_CANCELLED,
            (),
            None,
            collector.quality_results,
            (),
            collector.blocked_symbols,
            type(error).__name__,
            error_class=type(error).__name__,
        )
    except PostSuccessWatermarkError:
        raise
    except Exception as error:
        current = collector._control_plane.current_run_state(run_id)  # noqa: SLF001
        if current is not None and current.state is RunEventType.RUN_SUCCEEDED:
            raise PostSuccessWatermarkError("successful run requires recovery") from error
        if (
            (current is not None and current.state is RunEventType.ATTEMPT_SUCCEEDED)
            or marker_path(collector, run_id).is_file()
            or normalized_parts_exist(collector, run_id)
        ):
            raise LifecycleRecoveryRequiredError(
                "durable run boundary committed; same-run completion is required"
            ) from error
        name = type(error).__name__
        require_bound_approval = collector.calls_attempted == calls_at_entry
        _persist_usage_before_terminal(
            collector,
            run_id=run_id,
            recorded_at_utc=collector._clock(),  # noqa: SLF001
            transition="failure",
            require_bound_approval=require_bound_approval,
        )
        collector.fail(
            run_id=run_id,
            error_class=name,
            error_message=f"FMP collection terminated with {name}",
            require_bound_approval=require_bound_approval,
        )
        return CollectorOutcome(
            run_id,
            plan.plan_id,
            RunEventType.RUN_FAILED,
            (),
            None,
            collector.quality_results,
            (),
            collector.blocked_symbols,
            error_class=name,
        )
