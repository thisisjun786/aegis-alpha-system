"""Raw-input authorization boundary for a live FMP command."""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Final

from aegis_alpha.collection.records import CollectionMode
from aegis_alpha.data.fmp_approval import BoundApproval, RequestApproval, bind_verified_approval
from aegis_alpha.data.fmp_approval_scope import (
    CollectScope,
    CommonScope,
    UniverseScope,
    collect_scope_sha256,
    universe_scope_sha256,
)
from aegis_alpha.data.fmp_cli_artifacts import (
    AUTHORIZED_MAX_CALLS,
    PreconditionError,
    _read_json,
    load_fmp_policy,
    load_notification_artifact,
)
from aegis_alpha.data.fmp_collector import publish_bundle, validate_destination
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_owner_approval import (
    SCOPE_CONTRACT,
    claimed_run_identity,
    load_detached_approval,
    verify_owner_approval,
)
from aegis_alpha.data.fmp_owner_authority import load_owner_approval_authority
from aegis_alpha.data.fmp_rate_limit import TrustedUsageSnapshot
from aegis_alpha.data.fmp_rate_types import TierArtifact, parse_tier_artifact
from aegis_alpha.data.fmp_usage_trust import (
    UsageExtensionContext,
    extend_trusted_fmp_usage_snapshot,
    load_trusted_fmp_usage_snapshot,
)
from aegis_alpha.data.fmp_windows import UniverseManifest, parse_universe_manifest
from aegis_alpha.data.serialization import canonical_json_bytes

OWNER_AUTHORITY_ENV: Final = "AAS_FMP_OWNER_APPROVAL_AUTHORITY_PATH"


@dataclass(frozen=True, slots=True)
class AuthorizationContext:
    environment: Mapping[str, str]
    moment: datetime
    approval_clock: Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class OperationInputs:
    output_path: Path
    mode: CollectionMode
    selection: DatasetSelection
    operator_from: date | None


@dataclass(frozen=True, slots=True)
class LiveAuthorization:
    run_id: str
    mode: CollectionMode
    selection: DatasetSelection
    operator_from: date | None
    output_path: Path
    raw_store_root: Path
    dataset_root: Path
    manifest_path: Path | None
    manifest: UniverseManifest | None
    manifest_sha256: str | None
    tier: TierArtifact
    usage_baseline: TrustedUsageSnapshot
    artifact_hashes: Mapping[str, str]
    approval: RequestApproval
    max_calls: int | None
    service_day: date | None = None
    refresh_under_lock: Callable[[], LiveAuthorization] | None = None
    refresh_usage_under_lock: Callable[[], TrustedUsageSnapshot] | None = None


def _operator_from(arguments: argparse.Namespace, mode: CollectionMode, as_of: date) -> date | None:
    raw = getattr(arguments, "operator_from", None)
    if raw is not None:
        return date.fromisoformat(raw)
    return as_of if mode is CollectionMode.PROBE else None


def _operation_inputs(arguments: argparse.Namespace, moment: datetime) -> OperationInputs:
    match arguments.command:
        case "collect":
            mode = CollectionMode(arguments.mode)
            return OperationInputs(
                output_path=Path(arguments.receipt_path).resolve(strict=False),
                mode=mode,
                selection=DatasetSelection(arguments.dataset_selection),
                operator_from=_operator_from(arguments, mode, moment.date()),
            )
        case "build-universe":
            return OperationInputs(
                output_path=Path(arguments.universe_manifest_out).resolve(strict=False),
                mode=CollectionMode.PROBE,
                selection=DatasetSelection.PROBE,
                operator_from=None,
            )
        case _:
            raise PreconditionError("live FMP operation is unsupported")


def _resume_scope(  # noqa: PLR0913 - explicit persisted scope keys
    raw_store_root: Path,
    run_id: str,
    scope_sha256: str,
    *,
    scope_sha256_key: str = "approval_scope_sha256",
    scope_contract_key: str = "approval_scope_contract",
    expected_scope_contract: str = SCOPE_CONTRACT,
) -> None:
    root = raw_store_root / "fmp" / "plans"
    paths = sorted(root.glob(f"*-{run_id}.json")) if root.is_dir() else ()
    if len(paths) > 1:
        raise PreconditionError("signed run identity matches multiple resume records")
    if not paths:
        return
    try:
        raw = json.loads(paths[0].read_bytes())
    except (json.JSONDecodeError, OSError):
        raise PreconditionError("incomplete unsigned run cannot gain live authority") from None
    if not isinstance(raw, dict):
        raise PreconditionError("incomplete unsigned run cannot gain live authority")
    parameters = raw.get("parameters")
    hashes = parameters.get("artifact_hashes") if isinstance(parameters, dict) else None
    if not isinstance(hashes, dict):
        raise PreconditionError("incomplete unsigned run cannot gain live authority")
    if scope_sha256_key not in hashes or scope_contract_key not in hashes:
        raise PreconditionError("incomplete unsigned run cannot gain live authority")
    if (
        hashes[scope_sha256_key] != scope_sha256
        or hashes[scope_contract_key] != expected_scope_contract
    ):
        raise PreconditionError("resume record does not match the signed immutable scope")


def _persist_audit(
    root: Path,
    approval: BoundApproval,
    run_id: str,
    scope_sha256: str,
) -> None:
    verified = approval.approval
    document = {
        "authority_artifact_sha256": verified.authority_artifact_sha256,
        "authority_id": verified.authority_id,
        "key_id": verified.key_id,
        "owner_approval_payload_sha256": verified.payload_sha256,
        "owner_approval_signature_sha256": verified.signature_sha256,
        "run_identity": run_id,
        "scope_contract": SCOPE_CONTRACT,
        "scope_sha256": scope_sha256,
    }
    destination = (
        root / "fmp" / "runs" / run_id / f"owner-approval-audit-{verified.payload_sha256}.json"
    )
    approval.require_request()
    _ = publish_bundle([(destination, canonical_json_bytes(document))])


def _load_manifest_inputs(
    arguments: argparse.Namespace,
) -> tuple[Path | None, UniverseManifest | None, str | None]:
    if arguments.command != "collect":
        return None, None, None
    manifest_path = Path(arguments.universe_manifest).resolve(strict=False)
    manifest_document, manifest_sha256 = _read_json("universe manifest", manifest_path)
    return manifest_path, parse_universe_manifest(manifest_document), manifest_sha256


def _scope_and_manifest(
    arguments: argparse.Namespace,
    operation: OperationInputs,
    common: CommonScope,
    *,
    moment: datetime,
    output_path: Path,
) -> tuple[Path | None, UniverseManifest | None, str | None, str]:
    manifest_path, manifest, manifest_sha256 = _load_manifest_inputs(arguments)
    if arguments.command != "collect":
        scope_sha256 = universe_scope_sha256(UniverseScope(common=common, destination=output_path))
        return manifest_path, manifest, manifest_sha256, scope_sha256
    if manifest_path is None or manifest is None or manifest_sha256 is None:
        raise PreconditionError("collect requires a universe manifest")
    scope_sha256 = collect_scope_sha256(
        CollectScope(
            common=common,
            manifest_sha256=manifest_sha256,
            dataset_selection=operation.selection.value,
            datasets=operation.selection.datasets,
            mode=operation.mode.value,
            as_of=moment.date(),
            operator_from=operation.operator_from,
            receipt_path=output_path,
        )
    )
    return manifest_path, manifest, manifest_sha256, scope_sha256


def authorize_live_command(
    arguments: argparse.Namespace,
    context: AuthorizationContext,
) -> LiveAuthorization:
    """Authenticate exact live inputs before engine, transport, lock, or DB construction."""

    environment = context.environment
    moment = context.moment
    if type(arguments.max_calls) is not int or not 1 <= arguments.max_calls <= AUTHORIZED_MAX_CALLS:
        raise PreconditionError("--max-calls must be an integer from 1 through 25")
    if arguments.owner_approval is None or arguments.owner_approval_signature is None:
        raise PreconditionError(
            "non-backfill FMP commands require per-run owner approval artifacts"
        )
    operation = _operation_inputs(arguments, moment)
    output_path = operation.output_path
    raw_store_root = Path(arguments.raw_store_root).resolve(strict=False)
    dataset_root = Path(arguments.dataset_root).resolve(strict=False)
    for label, destination in (
        ("raw store root", raw_store_root),
        ("dataset root", dataset_root),
        ("command output path", output_path),
    ):
        _ = validate_destination(label, destination)
    policy = load_fmp_policy(Path(arguments.registry))
    if policy.scheduled_collection_allowed:
        raise PreconditionError("this executable permits manual one-shot collection only")
    notification = load_notification_artifact(Path(arguments.storage_notification))
    tier_document, tier_sha256 = _read_json("tier", Path(arguments.tier))
    try:
        tier = parse_tier_artifact(tier_document)
    except ValueError as error:
        raise PreconditionError(str(error)) from error
    approval_payload, signature = load_detached_approval(
        Path(arguments.owner_approval), Path(arguments.owner_approval_signature)
    )
    run_id = claimed_run_identity(approval_payload)
    common = CommonScope(
        run_identity=run_id,
        policy_sha256=policy.sha256,
        tier_sha256=tier_sha256,
        notification_sha256=notification.sha256,
        raw_store_root=raw_store_root,
        dataset_root=dataset_root,
        max_calls=arguments.max_calls,
    )
    manifest_path, manifest, manifest_sha256, scope_sha256 = _scope_and_manifest(
        arguments,
        operation,
        common,
        moment=moment,
        output_path=output_path,
    )
    authority_path = environment.get(OWNER_AUTHORITY_ENV)
    if not authority_path:
        raise PreconditionError(f"{OWNER_AUTHORITY_ENV} is required")
    authority = load_owner_approval_authority(Path(authority_path), moment)
    verified = verify_owner_approval(
        approval_payload,
        signature,
        authority,
        now=moment,
        expected_operation=arguments.command,
        expected_scope_sha256=scope_sha256,
        expected_max_calls=arguments.max_calls,
    )
    _resume_scope(raw_store_root, run_id, scope_sha256)
    usage = load_trusted_fmp_usage_snapshot(now=moment, environ=environment)
    database_url = environment.get("AAS_DATABASE_URL")
    if not database_url:
        raise PreconditionError("runtime control plane is unavailable")
    approval = bind_verified_approval(verified, run_id, clock=context.approval_clock)
    _persist_audit(raw_store_root, approval, run_id, scope_sha256)
    artifact_hashes = {
        "approval_scope_contract": SCOPE_CONTRACT,
        "approval_scope_sha256": scope_sha256,
        "notification": notification.sha256,
        "policy": policy.sha256,
        "tier": tier_sha256,
    }
    authorization = LiveAuthorization(
        run_id=run_id,
        mode=operation.mode,
        selection=operation.selection,
        operator_from=operation.operator_from,
        output_path=output_path,
        raw_store_root=raw_store_root,
        dataset_root=dataset_root,
        manifest_path=manifest_path,
        manifest=manifest,
        manifest_sha256=manifest_sha256,
        tier=tier,
        usage_baseline=usage,
        artifact_hashes=artifact_hashes,
        approval=approval,
        max_calls=arguments.max_calls,
    )

    def refresh_under_lock() -> LiveAuthorization:
        approval.require_request()
        refreshed_usage = extend_trusted_fmp_usage_snapshot(
            usage,
            context=UsageExtensionContext(
                database_url=database_url,
                raw_store_root=raw_store_root,
            ),
            coverage_end_utc=usage.recorded_at,
            now=context.approval_clock(),
        )
        return replace(
            authorization,
            usage_baseline=refreshed_usage,
            refresh_under_lock=None,
        )

    return replace(authorization, refresh_under_lock=refresh_under_lock)
