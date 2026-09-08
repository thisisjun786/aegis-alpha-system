"""Pure standing-authority composition for one recurring FMP operation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from pathlib import Path

from aegis_alpha.collection.records import CollectionMode
from aegis_alpha.data.fmp_cli_artifacts import FmpPolicy, NotificationArtifact, PreconditionError
from aegis_alpha.data.fmp_collector import validate_destination
from aegis_alpha.data.fmp_daily_refresh import build_daily_refresh_plan
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_live_authorization import LiveAuthorization
from aegis_alpha.data.fmp_rate_types import TierArtifact, TrustedUsageSnapshot
from aegis_alpha.data.fmp_recurring_approval import BoundRecurringApproval
from aegis_alpha.data.fmp_recurring_authority import VerifiedRecurringAuthority
from aegis_alpha.data.fmp_recurring_scope import (
    RECURRING_SCOPE_CONTRACT,
    confine_recurring_path,
    recurring_scope_sha256,
)
from aegis_alpha.data.fmp_windows import UniverseManifest


@dataclass(frozen=True, slots=True)
class RecurringOperation:
    command: str
    service_day: date
    output_path: Path
    manifest_path: Path | None = None
    attempt_index: int = 0
    mode: CollectionMode = CollectionMode.INCREMENTAL
    selection: DatasetSelection = DatasetSelection.ALL
    operator_from: date | None = None
    shard: tuple[int, int] | None = None


def require_service_day(service_day: date, moment: datetime) -> None:
    today = moment.date()
    if service_day == today:
        return
    if service_day == today - timedelta(days=1):
        return
    raise PreconditionError("daily service day must match the current UTC date")


def _validate_operation(
    policy: FmpPolicy,
    operation: RecurringOperation,
    manifest: UniverseManifest | None,
    manifest_sha256: str | None,
) -> None:
    if not policy.scheduled_collection_allowed:
        raise PreconditionError("FMP policy does not permit scheduled collection")
    if operation.command not in {"build-universe", "collect"}:
        raise PreconditionError("recurring FMP operation is unsupported")
    if operation.command == "collect" and (manifest is None or manifest_sha256 is None):
        raise PreconditionError("daily collect requires its generated universe manifest")
    if operation.command == "build-universe" and (
        manifest is not None or manifest_sha256 is not None
    ):
        raise PreconditionError("daily universe build cannot accept a prior manifest")


def _validate_tier(
    authority: VerifiedRecurringAuthority,
    tier: TierArtifact,
    tier_sha256: str,
) -> None:
    """Bind the shipped tier to the signed authority; a null vendor cap accepts an overlay."""

    if tier.calls_per_minute != authority.calls_per_minute:
        raise PreconditionError("tier rate does not match signed recurring authority")
    if tier.calls_per_day is not None and tier.calls_per_day != authority.calls_per_day:
        raise PreconditionError("tier daily budget does not match signed recurring authority")
    if tier_sha256 != authority.tier_sha256:
        raise PreconditionError("tier digest does not match signed recurring authority")


def _validate_artifact_bindings(
    authority: VerifiedRecurringAuthority,
    policy: FmpPolicy,
    notification: NotificationArtifact,
) -> None:
    if policy.sha256 != authority.registry_sha256:
        raise PreconditionError("registry digest does not match signed recurring authority")
    if notification.sha256 != authority.notification_sha256:
        raise PreconditionError("notification digest does not match signed recurring authority")


def build_recurring_authorization(  # noqa: C901, PLR0913 - explicit signed inputs
    *,
    authority: VerifiedRecurringAuthority,
    policy: FmpPolicy,
    notification: NotificationArtifact,
    tier: TierArtifact,
    tier_sha256: str,
    usage: TrustedUsageSnapshot,
    operation: RecurringOperation,
    manifest: UniverseManifest | None,
    manifest_sha256: str | None,
    approval_clock: Callable[[], datetime],
    allow_historical_service_day: bool = False,
) -> LiveAuthorization:
    if not allow_historical_service_day:
        require_service_day(operation.service_day, approval_clock())
    _validate_artifact_bindings(authority, policy, notification)
    _validate_operation(
        policy=policy,
        operation=operation,
        manifest=manifest,
        manifest_sha256=manifest_sha256,
    )
    _validate_tier(authority, tier, tier_sha256)
    limiter_tier = replace(tier, calls_per_day=authority.calls_per_day)
    if operation.shard is not None:
        shard_index, shard_total = operation.shard
        if shard_total < 1 or not 1 <= shard_index <= shard_total:
            raise PreconditionError("collection shard coordinates are out of range")
        limiter_tier = replace(
            limiter_tier,
            calls_per_minute=max(1, authority.calls_per_minute // shard_total),
            calls_per_day=(authority.calls_per_day + shard_total - 1) // shard_total,
        )
    historical_backfill = allow_historical_service_day and operation.mode is CollectionMode.BACKFILL
    if historical_backfill:
        if operation.command != "collect":
            raise PreconditionError("standing backfill authority only covers collection")
        if operation.selection is not DatasetSelection.ALL:
            raise PreconditionError("standing backfill requires dataset selection all")
        if operation.operator_from != authority.backfill_from:
            raise PreconditionError("backfill start does not match signed recurring authority")
        if operation.manifest_path is None:
            raise PreconditionError("standing backfill requires a universe manifest")
        _ = confine_recurring_path(
            authority.output_root,
            operation.manifest_path,
            "backfill universe manifest",
        )
    else:
        plan = build_daily_refresh_plan(
            schedule_id=authority.schedule_id,
            service_day=operation.service_day,
            output_root=authority.output_root,
            authority_payload_sha256=authority.payload_sha256,
        )
        expected_output = (
            plan.universe_path if operation.command == "build-universe" else plan.receipt_path
        )
        if operation.output_path.resolve(strict=False) != expected_output.resolve(strict=False):
            raise PreconditionError("recurring operation output does not match the daily plan")
        if operation.command == "collect" and (
            operation.manifest_path is None
            or operation.manifest_path.resolve(strict=False)
            != plan.collection_manifest_path.resolve(strict=False)
        ):
            raise PreconditionError("recurring collection manifest does not match the daily plan")
    output_path = confine_recurring_path(
        authority.output_root, operation.output_path, "daily output"
    )
    manifest_path = (
        None
        if operation.manifest_path is None
        else confine_recurring_path(
            authority.output_root,
            operation.manifest_path,
            "recurring universe manifest",
        )
    )
    for label, destination in (
        ("raw store root", authority.raw_store_root),
        ("dataset root", authority.dataset_root),
        ("daily output", output_path),
    ):
        _ = validate_destination(label, destination)
    scope_sha256 = recurring_scope_sha256(
        authority_payload_sha256=authority.payload_sha256,
        attempt_index=operation.attempt_index,
        command=operation.command,
        dataset_selection=authority.dataset_selection,
        manifest_sha256=manifest_sha256,
        output_path=operation.output_path,
        schedule_id=authority.schedule_id,
        service_day=operation.service_day,
    )
    run_id = authority.run_identity(
        operation.command,
        operation.service_day,
        attempt_index=operation.attempt_index,
        shard=operation.shard,
    )
    selection = operation.selection if operation.command == "collect" else DatasetSelection.PROBE
    return LiveAuthorization(
        run_id=run_id,
        mode=operation.mode,
        selection=selection,
        operator_from=(operation.operator_from if historical_backfill else authority.backfill_from),
        output_path=output_path,
        raw_store_root=authority.raw_store_root,
        dataset_root=authority.dataset_root,
        manifest_path=manifest_path,
        manifest=manifest,
        manifest_sha256=manifest_sha256,
        tier=limiter_tier,
        usage_baseline=usage,
        artifact_hashes={
            "notification": notification.sha256,
            "policy": policy.sha256,
            "recurring_authority": authority.payload_sha256,
            "recurring_authority_signature": authority.signature_sha256,
            "recurring_scope_contract": RECURRING_SCOPE_CONTRACT,
            "recurring_scope_sha256": scope_sha256,
            "tier": tier_sha256,
        },
        approval=BoundRecurringApproval(
            authority=authority,
            service_day=operation.service_day,
            clock=approval_clock,
        ),
        max_calls=None,
        service_day=operation.service_day,
    )
