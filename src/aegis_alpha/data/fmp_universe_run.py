"""Resumable budgeted active and delisted universe-list composition."""

from __future__ import annotations

from datetime import datetime
from functools import partial
from pathlib import Path

from aegis_alpha.collection.records import RunEventType
from aegis_alpha.data.fmp_approval import (
    ApprovalExpiredError,
    fail_closed_if_unconsumed_expiry,
    require_approved_identity,
)
from aegis_alpha.data.fmp_collector import (
    CollectorLockLostError,
    CollectorOutcome,
    FmpCollector,
    build_run_plan,
    plan_resume_record,
    publish_bundle,
)
from aegis_alpha.data.fmp_collector_run import LifecycleRecoveryRequiredError
from aegis_alpha.data.fmp_collector_state import (
    marker_ledger,
    marker_path,
    read_marker,
    resolve_resume,
)
from aegis_alpha.data.fmp_marker_recovery_context import marker_finalization_context
from aegis_alpha.data.fmp_rate_limit import BudgetExhaustedError
from aegis_alpha.data.fmp_universe_recovery import (
    publish_completed,
    publish_required,
    validate_pending,
)
from aegis_alpha.data.fmp_universe_work import (
    ACTIVE_LIST_ENDPOINT,
    DELISTED_LIST_ENDPOINT,
    publish_universe_manifest,
)
from aegis_alpha.data.serialization import canonical_json_bytes


def _outcome(  # noqa: PLR0913 - immutable outcome projection
    collector: FmpCollector,
    run_id: str,
    plan_id: str,
    terminal: RunEventType,
    *,
    artifact: Path | None = None,
    cancelled: str | None = None,
    error_class: str | None = None,
) -> CollectorOutcome:
    return CollectorOutcome(
        run_id=run_id,
        plan_id=plan_id,
        terminal_event=terminal,
        published_paths=() if artifact is None else (artifact,),
        receipt_path=None,
        quality_results=collector.quality_results,
        watermarks_advanced=(),
        cancel_reason=cancelled,
        artifact_path=artifact,
        error_class=error_class,
    )


def _cancel_for_interrupt(  # noqa: PLR0913 - terminal boundary inputs stay explicit
    collector: FmpCollector,
    run_id: str,
    plan_id: str,
    error: BaseException,
    *,
    recorded_at_utc: datetime,
    require_bound_approval: bool,
) -> CollectorOutcome:
    try:
        collector.record_usage(
            run_id=run_id,
            recorded_at_utc=recorded_at_utc,
            require_bound_approval=require_bound_approval,
        )
    except ApprovalExpiredError:
        raise
    except Exception as usage_error:
        raise LifecycleRecoveryRequiredError(
            "usage persistence failed before cancellation; same-run recovery is required"
        ) from usage_error
    collector.cancel(
        run_id=run_id,
        reason=type(error).__name__,
        require_bound_approval=require_bound_approval,
    )
    return _outcome(
        collector,
        run_id,
        plan_id,
        RunEventType.RUN_CANCELLED,
        cancelled=type(error).__name__,
        error_class=type(error).__name__,
    )


def run_universe_build(  # noqa: C901, PLR0912 - lifecycle boundaries remain explicit
    collector: FmpCollector,
    *,
    generated_at_utc: datetime,
    destination: Path,
    run_id: str,
    approved_run_identity: str | None = None,
) -> CollectorOutcome:
    plan = build_run_plan(
        mode=collector.config.mode,
        dataset="fmp_universe",
        parameters={
            "active_endpoint": ACTIVE_LIST_ENDPOINT,
            "delisted_endpoint": DELISTED_LIST_ENDPOINT,
            "max_calls": collector.config.max_calls,
            "artifact_hashes": dict(sorted(collector.config.artifact_hashes.items())),
            "destination": str(destination),
        },
        created_at_utc=generated_at_utc,
    )
    run_id, state, plan = resolve_resume(
        collector,
        plan,
        run_id,
        successful_recovery_validator=partial(validate_pending, destination=destination),
        reuse_successful_candidate=True,
    )
    require_approved_identity(approved_run_identity, run_id)
    collector.resume_as(run_id)
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
        marker = read_marker(collector, run_id)
        marker_recovery = marker is not None
        if marker is None:
            marker = publish_universe_manifest(
                collector, run_id, plan.plan_id, destination, plan.created_at_utc
            )
        ledger = marker_ledger(marker)
        with marker_finalization_context(collector, marker_recovery=marker_recovery):
            current = collector._control_plane.current_run_state(run_id)  # noqa: SLF001
            if current is not None and current.state is RunEventType.ATTEMPT_SUCCEEDED:
                collector.succeed(run_id=run_id)
            elif current is None or current.state is not RunEventType.RUN_SUCCEEDED:
                collector.record_usage(
                    run_id=run_id,
                    recorded_at_utc=collector._clock(),  # noqa: SLF001
                    ledger=ledger,
                )
                collector.succeed(run_id=run_id)
        if (
            state is not None
            and state.state is RunEventType.RUN_SUCCEEDED
            and validate_pending(collector, plan, run_id, state, destination=destination)
        ):
            publish_completed(collector, plan, run_id, destination)
        return _outcome(
            collector, run_id, plan.plan_id, RunEventType.RUN_SUCCEEDED, artifact=destination
        )
    except ApprovalExpiredError as error:
        current = collector._control_plane.current_run_state(run_id)  # noqa: SLF001
        fail_closed_if_unconsumed_expiry(
            error,
            consumed_new_slot=collector.calls_attempted != calls_at_entry,
            durable_boundary_committed=(
                marker_path(collector, run_id).is_file()
                or destination.is_file()
                or (
                    current is not None
                    and current.state
                    in {RunEventType.ATTEMPT_SUCCEEDED, RunEventType.RUN_SUCCEEDED}
                )
            ),
        )
        return _cancel_for_interrupt(
            collector,
            run_id,
            plan.plan_id,
            error,
            recorded_at_utc=collector._clock(),  # noqa: SLF001
            require_bound_approval=collector.calls_attempted == calls_at_entry,
        )
    except (
        BudgetExhaustedError,
        CollectorLockLostError,
        KeyboardInterrupt,
    ) as error:
        return _cancel_for_interrupt(
            collector,
            run_id,
            plan.plan_id,
            error,
            recorded_at_utc=collector._clock(),  # noqa: SLF001
            require_bound_approval=collector.calls_attempted == calls_at_entry,
        )
    except Exception as error:
        current = collector._control_plane.current_run_state(run_id)  # noqa: SLF001
        if current is not None and current.state is RunEventType.RUN_SUCCEEDED:
            publish_required(collector, plan, run_id, destination)
            raise LifecycleRecoveryRequiredError(
                "universe success committed; exact reconciliation is required"
            ) from error
        if (
            (current is not None and current.state is RunEventType.ATTEMPT_SUCCEEDED)
            or marker_path(collector, run_id).is_file()
            or destination.is_file()
        ):
            raise LifecycleRecoveryRequiredError(
                "durable universe boundary committed; same-run completion is required"
            ) from error
        name = type(error).__name__
        require_bound_approval = collector.calls_attempted == calls_at_entry
        try:
            collector.record_usage(
                run_id=run_id,
                recorded_at_utc=collector._clock(),  # noqa: SLF001
                require_bound_approval=require_bound_approval,
            )
        except ApprovalExpiredError:
            raise
        except Exception as usage_error:
            raise LifecycleRecoveryRequiredError(
                "usage persistence failed before failure; same-run recovery is required"
            ) from usage_error
        collector.fail(
            run_id=run_id,
            error_class=name,
            error_message=f"FMP universe build terminated with {name}",
            require_bound_approval=require_bound_approval,
        )
        return _outcome(collector, run_id, plan.plan_id, RunEventType.RUN_FAILED, error_class=name)
