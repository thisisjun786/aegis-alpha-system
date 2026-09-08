"""Authorized adapter composition for the bounded FMP live path."""

from __future__ import annotations

import argparse
import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from sqlalchemy import create_engine

from aegis_alpha.collection.records import CollectionMode, RunEventType
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.data.fmp_attempt_state import latest_durable_pacing_deadline
from aegis_alpha.data.fmp_backfill_authorization import authorize_backfill_command
from aegis_alpha.data.fmp_catalog import FmpCatalogPendingError, register_completed_collection
from aegis_alpha.data.fmp_cli_artifacts import PreconditionError
from aegis_alpha.data.fmp_collector import (
    CollectorConfig,
    CollectorLock,
    CollectorOutcome,
    FmpCollector,
    Transport,
)
from aegis_alpha.data.fmp_collector_run import run_collection
from aegis_alpha.data.fmp_deferred_transport import deferred_live_transport
from aegis_alpha.data.fmp_live_authorization import (
    AuthorizationContext,
    LiveAuthorization,
    authorize_live_command,
)
from aegis_alpha.data.fmp_rate_limit import RateLimiter, TrustedUsageSnapshot
from aegis_alpha.data.fmp_universe_run import run_universe_build


@dataclass(frozen=True, slots=True)
class RuntimeDependencies:
    transport: Transport | None
    monotonic: Callable[[], float]
    sleep: Callable[[float], None]
    approval_clock: Callable[[], datetime]
    wall_clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    invocation_admission: Callable[[], None] | None = None


def _refresh_authorization_under_lock(
    authorization: LiveAuthorization,
    lock: CollectorLock,
) -> LiveAuthorization:
    refresh = authorization.refresh_under_lock
    if refresh is None:
        return authorization
    lock.require_ownership()
    refreshed = refresh()
    if refreshed.raw_store_root != authorization.raw_store_root:
        raise PreconditionError("under-lock authorization changed the signed raw root")
    lock.rebind_run_identity(refreshed.run_id)
    return replace(refreshed, refresh_under_lock=refresh)


def run_live_command(
    *,
    arguments: argparse.Namespace,
    environment: Mapping[str, str],
    credential: str,
    moment: datetime,
    dependencies: RuntimeDependencies,
) -> tuple[CollectorOutcome, int]:
    """Authorize raw inputs, then construct production adapters and execute."""

    context = AuthorizationContext(
        environment=environment,
        moment=moment,
        approval_clock=dependencies.approval_clock,
    )
    authorization = (
        authorize_backfill_command(arguments, context)
        if arguments.command == "collect" and arguments.mode == CollectionMode.BACKFILL.value
        else authorize_live_command(arguments, context)
    )
    return run_authorized_command(
        arguments=arguments,
        authorization=authorization,
        environment=environment,
        credential=credential,
        moment=moment,
        dependencies=dependencies,
    )


def run_authorized_command(  # noqa: PLR0913 - explicit runtime dependencies
    *,
    arguments: argparse.Namespace,
    authorization: LiveAuthorization,
    environment: Mapping[str, str],
    credential: str,
    moment: datetime,
    dependencies: RuntimeDependencies,
    shard: tuple[int, int] | None = None,
) -> tuple[CollectorOutcome, int]:
    """Construct production adapters after either one-shot or recurring authorization."""

    database_url = environment["AAS_DATABASE_URL"]
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        hide_parameters=True,
        connect_args={"connect_timeout": 5},
    )
    try:
        lock = CollectorLock(
            authorization.raw_store_root,
            run_identity=authorization.run_id,
            shard=shard,
        )
        with lock:
            authorization = _refresh_authorization_under_lock(authorization, lock)

            def refresh_daily_usage() -> TrustedUsageSnapshot:
                nonlocal authorization
                usage_refresh = authorization.refresh_usage_under_lock
                if usage_refresh is not None:
                    lock.require_ownership()
                    snapshot = usage_refresh()
                    authorization = replace(authorization, usage_baseline=snapshot)
                    return snapshot
                run_id = authorization.run_id
                authorization = _refresh_authorization_under_lock(authorization, lock)
                if authorization.run_id != run_id:
                    raise PreconditionError("daily usage refresh changed the active run identity")
                return authorization.usage_baseline

            shared_pacing_deadline = latest_durable_pacing_deadline(authorization.raw_store_root)
            limiter = RateLimiter(
                tier=authorization.tier,
                max_calls=authorization.max_calls,
                clock=dependencies.monotonic,
                sleep=dependencies.sleep,
                run_seed=int(hashlib.sha256(authorization.run_id.encode()).hexdigest()[:16], 16),
                usage_baseline=authorization.usage_baseline,
                utc_clock=dependencies.wall_clock,
                daily_usage_refresh=refresh_daily_usage,
                invocation_admission=dependencies.invocation_admission,
            )
            receipt_destination = authorization.output_path
            if shard is not None:
                receipt_destination = authorization.output_path.with_name(
                    f"{authorization.output_path.stem}"
                    f"-shard{shard[0]}of{shard[1]}"
                    f"{authorization.output_path.suffix}"
                )
            collector = FmpCollector(
                config=CollectorConfig(
                    raw_store_root=authorization.raw_store_root,
                    dataset_root=authorization.dataset_root,
                    receipt_path=authorization.output_path,
                    receipt_evidence_path=receipt_destination,
                    as_of=(
                        authorization.service_day
                        if authorization.service_day is not None
                        else moment.date()
                    ),
                    mode=authorization.mode,
                    max_calls=authorization.max_calls,
                    run_identity=authorization.run_id,
                    operator_from=authorization.operator_from,
                    artifact_hashes=authorization.artifact_hashes,
                    manifest_path=authorization.manifest_path,
                    shard=shard,
                ),
                transport=deferred_live_transport(
                    dependencies.transport, clock=dependencies.wall_clock
                ),
                control_plane=CollectionRegistry(engine),
                limiter=limiter,
                credential=credential,
                clock=dependencies.wall_clock,
                lock=lock,
                approval_check=authorization.approval.require_request,
                shared_pacing_deadline=shared_pacing_deadline,
            )
            if arguments.command == "build-universe":
                outcome = run_universe_build(
                    collector,
                    generated_at_utc=moment,
                    destination=authorization.output_path,
                    run_id=authorization.run_id,
                    approved_run_identity=authorization.run_id,
                )
            else:
                if authorization.manifest is None or authorization.manifest_sha256 is None:
                    raise ValueError("collect requires a validated universe manifest")
                outcome = run_collection(
                    collector,
                    manifest=authorization.manifest,
                    manifest_sha256=authorization.manifest_sha256,
                    created_at_utc=moment,
                    run_id=authorization.run_id,
                    dataset_selection=authorization.selection,
                    approved_run_identity=authorization.run_id,
                )
                if outcome.terminal_event is RunEventType.RUN_SUCCEEDED:
                    try:
                        register_completed_collection(
                            engine,
                            authorization.raw_store_root,
                            authorization.dataset_root,
                            outcome.run_id,
                        )
                    except Exception as error:
                        raise FmpCatalogPendingError(outcome.run_id, error) from error
            return outcome, collector.calls_attempted
    finally:
        engine.dispose()
