"""Fail-closed runtime authorization for one standing daily FMP operation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from aegis_alpha.collection.records import CollectionMode
from aegis_alpha.data.fmp_cli_artifacts import (
    PreconditionError,
    _read_json,
    load_fmp_policy,
    load_notification_artifact,
)
from aegis_alpha.data.fmp_daily_refresh import require_backfill_manifest_binding
from aegis_alpha.data.fmp_live_authorization import (
    OWNER_AUTHORITY_ENV,
    LiveAuthorization,
)
from aegis_alpha.data.fmp_owner_authority import load_owner_approval_authority
from aegis_alpha.data.fmp_rate_limit import TrustedUsageSnapshot, parse_tier_artifact
from aegis_alpha.data.fmp_recurring_artifacts import load_recurring_authority
from aegis_alpha.data.fmp_recurring_audit import publish_recurring_audit
from aegis_alpha.data.fmp_recurring_authority import (
    recurring_authority_issued_at,
    verify_recurring_authority,
)
from aegis_alpha.data.fmp_recurring_authorization import (
    RecurringOperation,
    build_recurring_authorization,
    require_service_day,
)
from aegis_alpha.data.fmp_recurring_runs import select_recurring_run_from_database
from aegis_alpha.data.fmp_usage_trust import (
    UsageExtensionContext,
    extend_trusted_fmp_usage_snapshot,
    load_trusted_fmp_usage_snapshot,
)
from aegis_alpha.data.fmp_windows import UniverseManifest, parse_universe_manifest


def authorize_recurring_operation(  # noqa: C901, PLR0913 - live recovery authorization stays explicit
    *,
    operation: RecurringOperation,
    recurring_authority_path: Path,
    recurring_signature_path: Path,
    registry_path: Path,
    notification_path: Path,
    tier_path: Path,
    environment: Mapping[str, str],
    moment: datetime,
    approval_clock: Callable[[], datetime],
    allow_historical_service_day: bool = False,
) -> LiveAuthorization:
    """Authenticate all external recurring inputs before adapter construction."""

    if not allow_historical_service_day:
        require_service_day(operation.service_day, moment)
    authority_path = environment.get(OWNER_AUTHORITY_ENV)
    if not authority_path:
        raise PreconditionError(f"{OWNER_AUTHORITY_ENV} is required")
    payload, signature = load_recurring_authority(
        recurring_authority_path, recurring_signature_path
    )
    trust = load_owner_approval_authority(
        Path(authority_path),
        recurring_authority_issued_at(payload),
    )
    authority = verify_recurring_authority(payload, signature, trust, now=moment)
    policy = load_fmp_policy(registry_path)
    notification = load_notification_artifact(notification_path)
    tier_document, tier_sha256 = _read_json("tier", tier_path)
    try:
        tier = parse_tier_artifact(tier_document)
    except ValueError as error:
        raise PreconditionError(str(error)) from error
    manifest: UniverseManifest | None = None
    manifest_sha256: str | None = None
    if operation.command == "collect":
        if operation.manifest_path is None:
            raise PreconditionError("daily collect requires a universe manifest path")
        manifest_document, manifest_sha256 = _read_json(
            "daily universe manifest", operation.manifest_path
        )
        manifest = parse_universe_manifest(manifest_document)
        if operation.mode is CollectionMode.BACKFILL:
            try:
                require_backfill_manifest_binding(
                    manifest,
                    norgate_security_master=authority.norgate_security_master,
                    norgate_security_master_sha256=authority.norgate_security_master_sha256,
                    norgate_security_master_row_count=authority.norgate_security_master_row_count,
                    norgate_snapshot_date=authority.norgate_snapshot_date,
                )
            except (OSError, ValueError) as error:
                raise PreconditionError(str(error)) from error
    usage = load_trusted_fmp_usage_snapshot(now=moment, environ=environment)
    database_url = environment.get("AAS_DATABASE_URL")
    if not database_url:
        raise PreconditionError("runtime control plane is unavailable")
    authorization = build_recurring_authorization(
        authority=authority,
        policy=policy,
        notification=notification,
        tier=tier,
        tier_sha256=tier_sha256,
        usage=usage,
        operation=operation,
        manifest=manifest,
        manifest_sha256=manifest_sha256,
        approval_clock=approval_clock,
        allow_historical_service_day=allow_historical_service_day,
    )

    def refresh_under_lock() -> LiveAuthorization:
        live_now = approval_clock()
        authority.require_request(live_now)
        selection = select_recurring_run_from_database(
            authority=authority,
            command=operation.command,
            service_day=operation.service_day,
            database_url=database_url,
            shard=operation.shard,
        )
        selected_operation = replace(operation, attempt_index=selection.attempt_index)
        refreshed_usage = extend_trusted_fmp_usage_snapshot(
            usage,
            context=UsageExtensionContext(
                database_url=database_url,
                raw_store_root=authority.raw_store_root,
            ),
            coverage_end_utc=usage.recorded_at,
            now=live_now,
        )
        refreshed = build_recurring_authorization(
            authority=authority,
            policy=policy,
            notification=notification,
            tier=tier,
            tier_sha256=tier_sha256,
            usage=refreshed_usage,
            operation=selected_operation,
            manifest=manifest,
            manifest_sha256=manifest_sha256,
            approval_clock=approval_clock,
            allow_historical_service_day=allow_historical_service_day,
        )
        publish_recurring_audit(
            authority=authority,
            authorization=refreshed,
            attempt_index=selection.attempt_index,
            service_day=operation.service_day,
            now=approval_clock(),
        )
        return refreshed

    def refresh_usage_under_lock() -> TrustedUsageSnapshot:
        live_now = approval_clock()
        authority.require_request(live_now)
        return extend_trusted_fmp_usage_snapshot(
            usage,
            context=UsageExtensionContext(
                database_url=database_url,
                raw_store_root=authority.raw_store_root,
            ),
            coverage_end_utc=usage.recorded_at,
            now=live_now,
        )

    return replace(
        authorization,
        refresh_under_lock=refresh_under_lock,
        refresh_usage_under_lock=refresh_usage_under_lock,
    )
