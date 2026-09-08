from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Final

from sqlalchemy import create_engine, make_url
from sqlalchemy.exc import ArgumentError, SQLAlchemyError

from aegis_alpha.collection.fmp_usage_checkpoint import (
    FmpUsageCheckpointService,
    VerifiedProviderUsageSnapshot,
)
from aegis_alpha.collection.provider_usage_repository import ProviderUsageRepository
from aegis_alpha.collection.usage_checkpoint import UsageRecordLeaf, usage_records_root
from aegis_alpha.data.fmp_rate_limit import (
    TrustedUsageSnapshot,
    UsageEvidenceUnavailableError,
)
from aegis_alpha.data.fmp_usage_authority import (
    FmpUsageAuthority,
    load_fmp_usage_authority,
)
from aegis_alpha.data.serialization import canonical_json_bytes

from .fmp_orphan_usage import DurableUsageReconciliation, load_durable_attempt_usage

DATABASE_URL_ENV: Final = "AAS_DATABASE_URL"
USAGE_AUTHORITY_PATH_ENV: Final = "AAS_FMP_USAGE_AUTHORITY_PATH"
_BLOCKED_MESSAGE: Final = (
    "live execution is BLOCKED_EXTERNAL: AAS-DATA-005 exposes no rolling usage "
    "aggregation and no separately-authorized usage snapshot exists"
)


@dataclass(frozen=True, slots=True)
class UsageExtensionContext:
    database_url: str
    raw_store_root: Path
    exclude_run_ids: tuple[str, ...] = ()


def require_current_usage_snapshot(
    snapshot: TrustedUsageSnapshot,
    *,
    now: datetime,
) -> TrustedUsageSnapshot:
    if snapshot.recorded_at != _utc(now):
        raise UsageEvidenceUnavailableError(
            "legacy preflight requires usage coverage at the current runtime instant"
        )
    return snapshot


def load_trusted_fmp_usage_snapshot(
    *,
    now: datetime,
    environ: Mapping[str, str] | None = None,
) -> TrustedUsageSnapshot:
    """Build the runtime baseline only from external authority and live-row proofs."""

    environment = os.environ if environ is None else environ
    try:
        return _load_verified_snapshot(now, environment)
    except (OSError, ValueError, LookupError, SQLAlchemyError):
        raise UsageEvidenceUnavailableError(_BLOCKED_MESSAGE) from None


def extend_trusted_fmp_usage_snapshot(
    snapshot: TrustedUsageSnapshot,
    *,
    context: UsageExtensionContext,
    coverage_end_utc: datetime,
    now: datetime,
) -> TrustedUsageSnapshot:
    """Add provider-lineage usage recorded after the signed checkpoint window."""

    coverage_end = _utc(coverage_end_utc)
    moment = _utc(now)
    if coverage_end != snapshot.recorded_at:
        raise ValueError("usage extension must begin at the signed checkpoint")
    if moment < coverage_end:
        raise ValueError("usage extension window cannot move backwards")
    day_start = moment.replace(hour=0, minute=0, second=0, microsecond=0)
    rolling_start = moment - timedelta(days=30)
    calls_start = max(coverage_end, day_start)
    bytes_start = max(coverage_end, rolling_start)
    query_start = min(day_start, rolling_start)
    try:
        url = make_url(context.database_url)
    except ArgumentError:
        raise UsageEvidenceUnavailableError(_BLOCKED_MESSAGE) from None
    if url.drivername != "postgresql+psycopg":
        raise UsageEvidenceUnavailableError(_BLOCKED_MESSAGE)
    engine = create_engine(
        context.database_url,
        pool_pre_ping=True,
        hide_parameters=True,
        connect_args={"connect_timeout": 5},
    )
    try:
        with engine.connect() as connection:
            rolling_leaves = tuple(
                ProviderUsageRepository.leaves(
                    connection,
                    provider="fmp",
                    coverage_start_utc=query_start,
                    coverage_end_utc=moment,
                )
            )
    except SQLAlchemyError:
        raise UsageEvidenceUnavailableError(_BLOCKED_MESSAGE) from None
    finally:
        engine.dispose()

    post_checkpoint_leaves = tuple(
        leaf for leaf in rolling_leaves if leaf.recorded_at_utc >= coverage_end
    )
    window_leaves = tuple(
        leaf for leaf in post_checkpoint_leaves if leaf.run_id not in context.exclude_run_ids
    )
    daily_leaves = tuple(leaf for leaf in window_leaves if leaf.recorded_at_utc >= calls_start)
    bandwidth_leaves = tuple(leaf for leaf in window_leaves if leaf.recorded_at_utc >= bytes_start)
    durable = load_durable_attempt_usage(
        context.raw_store_root,
        now=moment,
        exclude_run_ids=context.exclude_run_ids,
        reconciliation=DurableUsageReconciliation(
            recorded_calls_by_run={},
            recorded_bytes_by_run={},
            calls_start_utc=day_start,
            bytes_start_utc=rolling_start,
            exclude_start_utc=None,
        ),
    )
    durable_calls = {item.run_id: item.calls_used_today for item in durable.runs}
    durable_bytes = {item.run_id: item.bytes_used_30d for item in durable.runs}
    calls, orphan_calls = _window_and_durable_extension(
        post_checkpoint_leaves=daily_leaves,
        rolling_leaves=rolling_leaves,
        durable_by_run=durable_calls,
        metric="calls_attempted",
        unit="call",
        already_start_utc=day_start,
    )
    received_bytes, orphan_bytes = _window_and_durable_extension(
        post_checkpoint_leaves=bandwidth_leaves,
        rolling_leaves=rolling_leaves,
        durable_by_run=durable_bytes,
        metric="bytes_received",
        unit="byte",
        already_start_utc=rolling_start,
    )
    daily_floor = snapshot.calls_used_today if coverage_end.date() == moment.date() else 0
    bandwidth_floor = snapshot.bytes_used_30d if coverage_end > rolling_start else 0
    window_root = usage_records_root(window_leaves)
    integrity = hashlib.sha256(
        canonical_json_bytes(
            {
                "baseline_integrity_sha256": snapshot.integrity_sha256,
                "coverage_end_utc": coverage_end.isoformat(),
                "window_calls_attempted": calls,
                "window_bytes_received": received_bytes,
                "durable_orphan_calls_attempted": orphan_calls,
                "durable_orphan_bytes_received": orphan_bytes,
                "durable_attempt_count": durable.attempt_count,
                "durable_attempt_records_root_sha256": durable.records_root_sha256,
                "window_usage_record_count": len(window_leaves),
                "window_usage_records_root_sha256": window_root,
                "observed_until_utc": moment.isoformat(),
            }
        )
    ).hexdigest()
    return TrustedUsageSnapshot(
        source=(f"{snapshot.source}+fmp-live-usage:{window_root}:{durable.records_root_sha256}"),
        recorded_at_utc=moment.isoformat(),
        integrity_sha256=integrity,
        authority_verified=True,
        calls_used_today=daily_floor + calls + orphan_calls,
        bytes_used_30d=bandwidth_floor + received_bytes + orphan_bytes,
    )


def _window_and_durable_extension(  # noqa: PLR0913 - each input is one trusted source
    *,
    post_checkpoint_leaves: tuple[UsageRecordLeaf, ...],
    rolling_leaves: tuple[UsageRecordLeaf, ...],
    durable_by_run: Mapping[str, int],
    metric: str,
    unit: str,
    already_start_utc: datetime,
) -> tuple[int, int]:
    """Count each run's current-window spend once from attempts or terminal rows."""

    persisted_by_run = _integral_by_run(
        post_checkpoint_leaves,
        metric=metric,
        unit=unit,
        since_utc=datetime.min.replace(tzinfo=UTC),
    )
    already_recorded = _integral_by_run(
        rolling_leaves,
        metric=metric,
        unit=unit,
        since_utc=already_start_utc,
    )
    window_quantity = 0
    orphan_quantity = 0
    for run_id in set(persisted_by_run) | set(durable_by_run) | set(already_recorded):
        persisted = persisted_by_run.get(run_id, 0)
        durable = durable_by_run.get(run_id, 0)
        already = already_recorded.get(run_id, 0)
        # Key presence is authoritative: a durable zero (calls landed before
        # midnight, terminal row persisted after) must suppress the terminal
        # row rather than fall through to insert-date attribution.
        if run_id in durable_by_run:
            credited = min(persisted, durable) if persisted else 0
            window_quantity += credited
            leftover = max(durable - already, 0)
            if leftover:
                orphan_quantity += leftover
            continue
        window_quantity += persisted
    return window_quantity, orphan_quantity


def _integral_delta(
    leaves: tuple[UsageRecordLeaf, ...],
    *,
    metric: str,
    unit: str,
) -> int:
    quantity = Decimal(0)
    for leaf in leaves:
        if leaf.metric != metric:
            continue
        if leaf.unit != unit:
            raise UsageEvidenceUnavailableError(_BLOCKED_MESSAGE)
        quantity += leaf.quantity
    if quantity != quantity.to_integral_value() or quantity < 0:
        raise UsageEvidenceUnavailableError(_BLOCKED_MESSAGE)
    return int(quantity)


def _integral_by_run(
    leaves: tuple[UsageRecordLeaf, ...],
    *,
    metric: str,
    unit: str,
    since_utc: datetime,
) -> dict[str, int]:
    quantities: dict[str, Decimal] = {}
    for leaf in leaves:
        if leaf.metric != metric or leaf.recorded_at_utc < since_utc:
            continue
        if leaf.unit != unit:
            raise UsageEvidenceUnavailableError(_BLOCKED_MESSAGE)
        quantities[leaf.run_id] = quantities.get(leaf.run_id, Decimal(0)) + leaf.quantity
    if any(value != value.to_integral_value() or value < 0 for value in quantities.values()):
        raise UsageEvidenceUnavailableError(_BLOCKED_MESSAGE)
    return {run_id: int(value) for run_id, value in quantities.items()}


def _load_verified_snapshot(
    now: datetime,
    environment: Mapping[str, str],
) -> TrustedUsageSnapshot:
    moment = _utc(now)
    authority_path = environment.get(USAGE_AUTHORITY_PATH_ENV)
    database_url = environment.get(DATABASE_URL_ENV)
    if not authority_path or not database_url:
        raise ValueError("required trust environment is absent")
    authority = load_fmp_usage_authority(Path(authority_path), moment)
    try:
        url = make_url(database_url)
    except ArgumentError:
        raise ValueError("database URL is invalid") from None
    if url.drivername != "postgresql+psycopg":
        raise ValueError("database URL uses an unsupported driver")
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        hide_parameters=True,
        connect_args={"connect_timeout": 5},
    )
    try:
        rolling, daily = FmpUsageCheckpointService(engine).verify_latest_available(
            at_or_before_utc=moment,
            keyring=authority.keyring,
        )
        return _trusted_snapshot(authority, rolling, daily)
    finally:
        engine.dispose()


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _metric(snapshot: VerifiedProviderUsageSnapshot, metric: str, unit: str) -> int:
    matches = [item for item in snapshot.aggregates if item.metric == metric]
    if len(matches) != 1 or matches[0].unit != unit:
        raise ValueError("required usage metric/unit is absent or duplicated")
    quantity: Decimal = matches[0].quantity
    if quantity != quantity.to_integral_value():
        raise ValueError("usage quantity must be integral")
    return int(quantity)


def _trusted_snapshot(
    authority: FmpUsageAuthority,
    rolling: VerifiedProviderUsageSnapshot,
    daily: VerifiedProviderUsageSnapshot,
) -> TrustedUsageSnapshot:
    if rolling.coverage_end_utc != daily.coverage_end_utc:
        raise ValueError("usage checkpoint windows do not share one coverage end")
    proof = {
        "authority_artifact_sha256": authority.artifact_sha256,
        "authority_id": authority.authority_id,
        "key_id": authority.key_id,
        "checkpoints": [
            {
                "checkpoint_id": snapshot.checkpoint_id,
                "coverage_end_utc": snapshot.coverage_end_utc.isoformat(),
                "coverage_start_utc": snapshot.coverage_start_utc.isoformat(),
                "generated_at_utc": snapshot.generated_at_utc.isoformat(),
                "usage_records_root_sha256": snapshot.usage_records_root_sha256,
            }
            for snapshot in (rolling, daily)
        ],
    }
    source = (
        "fmp-signed-usage-checkpoints:"
        f"{rolling.checkpoint_id}:{daily.checkpoint_id}:"
        f"{authority.authority_id}:{authority.key_id}"
    )
    recorded_at = rolling.coverage_end_utc.isoformat()
    return TrustedUsageSnapshot(
        source=source,
        recorded_at_utc=recorded_at,
        integrity_sha256=hashlib.sha256(canonical_json_bytes(proof)).hexdigest(),
        authority_verified=True,
        calls_used_today=_metric(daily, "calls_attempted", "call"),
        bytes_used_30d=_metric(rolling, "bytes_received", "byte"),
    )
