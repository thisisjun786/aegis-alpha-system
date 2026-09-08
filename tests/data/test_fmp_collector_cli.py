from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunPlan,
    CollectionUsageRecord,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import (
    collection_run_events,
    collection_run_plans,
    collection_runs,
    collection_usage_checkpoints,
    collection_usage_records,
    collection_watermarks,
)
from aegis_alpha.collection.usage_checkpoint_crypto import Ed25519PublicKeyring
from aegis_alpha.collection.usage_checkpoint_schema import SignedUsageCheckpoint
from aegis_alpha.data import fmp_approval as fmp_approval_module
from aegis_alpha.data import fmp_collector_cli as fmp_collector_cli_module
from aegis_alpha.data import fmp_live_authorization
from aegis_alpha.data import fmp_usage_authority as fmp_usage_authority_module
from aegis_alpha.data import fmp_usage_trust as fmp_usage_trust_module
from aegis_alpha.data.fmp_approval_scope import (
    CollectScope,
    CommonScope,
    UniverseScope,
    collect_scope_sha256,
    universe_scope_sha256,
)
from aegis_alpha.data.fmp_cli_artifacts import load_fmp_policy
from aegis_alpha.data.fmp_collector import (
    CollectorRequest,
    CollectorResponse,
    DestinationError,
    FmpCollector,
)
from aegis_alpha.data.fmp_collector_cli import (
    PRECONDITION_EXIT,
    ApprovalArtifact,
    NotificationArtifact,
    PreconditionError,
    license_classification,
    load_approval_artifact,
    load_notification_artifact,
    main,
    run_preflight,
)
from aegis_alpha.data.fmp_collector_command import RuntimeDependencies, run_live_command
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_owner_approval import (
    APPROVAL_CONTRACT,
    APPROVAL_VERSION,
    CONTRACT_REVISION,
)
from aegis_alpha.data.fmp_owner_authority import load_owner_approval_authority
from aegis_alpha.data.fmp_rate_limit import (
    BudgetExhaustedError,
    RateLimiter,
    TierArtifact,
    TrustedUsageSnapshot,
    UsageEvidenceUnavailableError,
)
from aegis_alpha.data.serialization import canonical_json_bytes

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy import Engine


class Destinations(TypedDict):
    raw_store_root: Path
    dataset_root: Path
    receipt_path: Path


FIXTURE_ROOT = (
    Path(__file__).resolve().parents[1] / "fixtures" / "provider_neutral" / "fmp_collector"
)
NOW = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
MAX_CALLS = 25
_SHA256_HEX_LENGTH = 64
_EXPECTED_DAILY_CALLS = 7
_EXPECTED_ROLLING_BYTES = 899
_EXPECTED_RETRY_RUN_CALLS = 5
_HTTP_SERVICE_UNAVAILABLE = 503
APPROVED_RUN_ID = "fmp-run-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


def _destinations(tmp_path: Path) -> Destinations:
    return {
        "raw_store_root": tmp_path / "raw",
        "dataset_root": tmp_path / "datasets",
        "receipt_path": tmp_path / "receipts" / "run.receipt.json",
    }


def _preflight(  # noqa: PLR0913 - one explicit argument per section 2 gate
    tmp_path: Path,
    *,
    registry: str = "registry_pending.json",
    approval: str | None = "owner_approval.json",
    notification: str = "storage_notification.json",
    tier: str = "tier_artifact.json",
    max_calls: int = MAX_CALLS,
    now: datetime = NOW,
) -> None:
    run_preflight(
        registry_path=FIXTURE_ROOT / registry,
        approval_path=None if approval is None else FIXTURE_ROOT / approval,
        notification_path=FIXTURE_ROOT / notification,
        tier_path=FIXTURE_ROOT / tier,
        max_calls=max_calls,
        now=now,
        **_destinations(tmp_path),
    )


@dataclass(frozen=True, slots=True)
class SigningContext:
    root: Path
    monkeypatch: pytest.MonkeyPatch
    now: datetime
    run_identity: str = APPROVED_RUN_ID
    expires_at_utc: datetime | None = None


def _signed_scope(arguments: argparse.Namespace, run_identity: str = APPROVED_RUN_ID) -> str:
    policy = load_fmp_policy(Path(arguments.registry))
    notification = load_notification_artifact(Path(arguments.storage_notification))
    common = CommonScope(
        run_identity=run_identity,
        policy_sha256=policy.sha256,
        tier_sha256=hashlib.sha256(Path(arguments.tier).read_bytes()).hexdigest(),
        notification_sha256=notification.sha256,
        raw_store_root=Path(arguments.raw_store_root).resolve(),
        dataset_root=Path(arguments.dataset_root).resolve(),
        max_calls=arguments.max_calls,
    )
    if arguments.command == "build-universe":
        return universe_scope_sha256(
            UniverseScope(
                common=common,
                destination=Path(arguments.universe_manifest_out).resolve(),
            )
        )
    selection = DatasetSelection(arguments.dataset_selection)
    operator_from = (
        arguments.now.date()
        if arguments.operator_from is None and arguments.mode == "probe"
        else None
        if arguments.operator_from is None
        else datetime.fromisoformat(arguments.operator_from).date()
    )
    return collect_scope_sha256(
        CollectScope(
            common=common,
            manifest_sha256=hashlib.sha256(
                Path(arguments.universe_manifest).read_bytes()
            ).hexdigest(),
            dataset_selection=selection.value,
            datasets=selection.datasets,
            mode=arguments.mode,
            as_of=arguments.now.date(),
            operator_from=operator_from,
            receipt_path=Path(arguments.receipt_path).resolve(),
        )
    )


def install_signed_approval(
    arguments: argparse.Namespace,
    context: SigningContext,
) -> dict[str, str]:
    arguments.now = context.now
    private_key = Ed25519PrivateKey.generate()
    authority_path = context.root / "authority" / "owner-key.json"
    authority_path.parent.mkdir(mode=0o700, exist_ok=True)
    authority_path.write_bytes(
        canonical_json_bytes(
            {
                "schema": "aegis-alpha/fmp-owner-approval-authority",
                "version": 1,
                "authority_id": "synthetic-owner-authority",
                "key_id": "ed25519:synthetic-2026-08",
                "public_key_encoding": "raw-ed25519-hex",
                "public_key": private_key.public_key().public_bytes_raw().hex(),
                "valid_from_utc": (context.now - timedelta(days=1)).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                "valid_until_utc": (context.now + timedelta(days=1)).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
            }
        )
    )
    authority_path.chmod(0o600)
    payload = canonical_json_bytes(
        {
            "contract": APPROVAL_CONTRACT,
            "version": APPROVAL_VERSION,
            "authority_id": "synthetic-owner-authority",
            "key_id": "ed25519:synthetic-2026-08",
            "approver": "synthetic-owner",
            "contract_revision": CONTRACT_REVISION,
            "policy_id": "fmp-operational-candidate-v1",
            "authorization_kind": "manual_one_shot",
            "run_identity": context.run_identity,
            "operation": arguments.command,
            "scope_sha256": _signed_scope(arguments, context.run_identity),
            "max_calls": arguments.max_calls,
            "issued_at_utc": (context.now - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "expires_at_utc": (
                context.expires_at_utc or context.now + timedelta(minutes=30)
            ).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
    )
    Path(arguments.owner_approval).write_bytes(payload)
    Path(arguments.owner_approval_signature).write_bytes(private_key.sign(payload))
    context.monkeypatch.setattr(
        fmp_live_authorization,
        "load_owner_approval_authority",
        lambda path, now: load_owner_approval_authority(
            path,
            now,
            collector_uid=2000,
            ownership_reader=lambda _metadata: 1000,
        ),
    )
    return {
        "FMP_API_KEY": "SYNTHETIC-CREDENTIAL",
        "AAS_FMP_OWNER_APPROVAL_AUTHORITY_PATH": str(authority_path),
    }


def _cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    extra: Sequence[str] = (),
    environ: dict[str, str] | None = None,
) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    destinations = _destinations(tmp_path)
    arguments = argparse.Namespace(
        command="collect",
        registry=FIXTURE_ROOT / "registry_pending.json",
        owner_approval=tmp_path / "approval.json",
        owner_approval_signature=tmp_path / "approval.sig",
        storage_notification=FIXTURE_ROOT / "storage_notification.json",
        tier=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=MAX_CALLS,
        raw_store_root=destinations["raw_store_root"],
        dataset_root=destinations["dataset_root"],
        receipt_path=destinations["receipt_path"],
        universe_manifest=FIXTURE_ROOT / "universe_manifest.json",
        mode="incremental",
        dataset_selection="probe",
        operator_from=None,
    )
    environment = install_signed_approval(
        arguments,
        SigningContext(root=tmp_path, monkeypatch=monkeypatch, now=NOW),
    )
    if environ is not None:
        environment.pop("FMP_API_KEY")
        environment.update(environ)
    argv = [
        "collect",
        "--registry",
        str(arguments.registry),
        "--owner-approval",
        str(arguments.owner_approval),
        "--owner-approval-signature",
        str(arguments.owner_approval_signature),
        "--storage-notification",
        str(arguments.storage_notification),
        "--tier",
        str(arguments.tier),
        "--max-calls",
        str(arguments.max_calls),
        "--raw-store-root",
        str(arguments.raw_store_root),
        "--dataset-root",
        str(arguments.dataset_root),
        "--receipt-path",
        str(arguments.receipt_path),
        "--universe-manifest",
        str(arguments.universe_manifest),
        "--mode",
        arguments.mode,
        *extra,
    ]
    code = main(argv, environ=environment, stdout=stdout, stderr=stderr, now=NOW)
    return (code, stdout.getvalue(), stderr.getvalue())


# -- P1 approval artifact --------------------------------------------------


def test_valid_approval_artifact_is_parsed_with_its_hash() -> None:
    approval = load_approval_artifact(FIXTURE_ROOT / "owner_approval.json")
    assert isinstance(approval, ApprovalArtifact)
    assert approval.max_calls == MAX_CALLS
    assert len(approval.sha256) == _SHA256_HEX_LENGTH
    assert approval.expires_at_utc > NOW


def test_missing_approval_artifact_means_zero_calls(tmp_path: Path) -> None:
    with pytest.raises(PreconditionError, match="no provider calls were attempted"):
        _preflight(tmp_path, approval=None)


def test_malformed_approval_artifact_means_zero_calls(tmp_path: Path) -> None:
    with pytest.raises(PreconditionError, match="missing required fields"):
        _preflight(tmp_path, approval="owner_approval_malformed.json")


def test_expired_approval_artifact_means_zero_calls(tmp_path: Path) -> None:
    with pytest.raises(PreconditionError, match="expired"):
        _preflight(
            tmp_path,
            registry="registry_cleared.json",
            approval="owner_approval_expired.json",
        )


def _write_pending_approval(path: Path, *, run_identity: str, max_calls: int = 4) -> Path:
    path.write_text(
        json.dumps(
            {
                "approver": "synthetic-owner",
                "contract_revision": "2026-07-29",
                "max_calls": max_calls,
                "run_identity": run_identity,
                "expires_at_utc": (NOW + timedelta(seconds=1)).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    return path


def test_budget_mismatch_means_zero_calls(tmp_path: Path) -> None:
    approval = _write_pending_approval(
        tmp_path / "approval.json",
        run_identity=APPROVED_RUN_ID,
        max_calls=MAX_CALLS - 1,
    )
    with pytest.raises(PreconditionError, match="must equal the run's --max-calls"):
        run_preflight(
            registry_path=FIXTURE_ROOT / "registry_cleared.json",
            approval_path=approval,
            notification_path=FIXTURE_ROOT / "storage_notification.json",
            tier_path=FIXTURE_ROOT / "tier_artifact.json",
            max_calls=MAX_CALLS,
            now=NOW,
            **_destinations(tmp_path),
        )


def test_mid_run_expiry_cancels_the_run() -> None:
    approval = load_approval_artifact(FIXTURE_ROOT / "owner_approval.json")
    approval.require_unexpired(NOW)
    with pytest.raises(PreconditionError, match="cancelled"):
        approval.require_unexpired(datetime(2026, 7, 31, tzinfo=UTC))


def test_approval_rejects_unknown_fields_without_echoing_values(tmp_path: Path) -> None:
    artifact = tmp_path / "approval.json"
    artifact.write_text(
        json.dumps(
            {
                "approver": "synthetic-owner",
                "contract_revision": "2026-07-29",
                "max_calls": MAX_CALLS,
                "run_identity": "synth-run-identity-0001",
                "expires_at_utc": "2026-07-30T00:00:00+00:00",
                "unknown": "SYNTH-SECRET-VALUE",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(PreconditionError, match="unknown fields") as error:
        load_approval_artifact(artifact)
    assert "SYNTH-SECRET-VALUE" not in str(error.value)


def test_approval_rejects_duplicate_keys_in_nested_objects(tmp_path: Path) -> None:
    artifact = tmp_path / "approval.json"
    artifact.write_text(
        '{"approver":"synthetic-owner","contract_revision":"2026-07-29",'
        '"max_calls":25,"run_identity":"synth-run-identity-0001",'
        '"expires_at_utc":"2026-07-30T00:00:00+00:00",'
        '"metadata":{"token":"first","token":"SYNTH-SECRET-VALUE"}}',
        encoding="utf-8",
    )
    with pytest.raises(PreconditionError, match="duplicate") as error:
        load_approval_artifact(artifact)
    assert "SYNTH-SECRET-VALUE" not in str(error.value)


def test_approval_is_required_and_accepted_once_the_registry_is_clear(tmp_path: Path) -> None:
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path, registry="registry_cleared.json")


def test_registry_clear_without_approval_means_zero_calls(tmp_path: Path) -> None:
    with pytest.raises(PreconditionError, match="--owner-approval is required"):
        _preflight(tmp_path, registry="registry_cleared.json", approval=None)


def test_registry_pending_without_artifacts_means_zero_calls(tmp_path: Path) -> None:
    assert (
        license_classification(FIXTURE_ROOT / "registry_pending.json") == "PENDING_CONTRACT_REVIEW"
    )
    with pytest.raises(PreconditionError, match="--owner-approval is required"):
        _preflight(tmp_path, approval=None)


def test_the_repository_registry_reports_post_004b_classification() -> None:
    repository_registry = (
        Path(__file__).resolve().parents[2] / "config" / ("data_authority_registry.json")
    )
    assert license_classification(repository_registry) == "PROPRIETARY_SUBSCRIPTION"


# -- P2 notification artifact ---------------------------------------------


def test_notification_artifact_is_validated_with_its_hash() -> None:
    notification = load_notification_artifact(FIXTURE_ROOT / "storage_notification.json")
    assert isinstance(notification, NotificationArtifact)
    assert notification.storage_aliases
    assert len(notification.sha256) == _SHA256_HEX_LENGTH


def test_notification_rejects_unknown_and_duplicate_fields(tmp_path: Path) -> None:
    unknown = tmp_path / "unknown-notification.json"
    unknown.write_text(
        json.dumps(
            {
                "notified_at_utc": "2026-07-29T00:00:00+00:00",
                "channel": "email:synthetic@example.invalid",
                "storage_aliases": ["synthetic.invalid"],
                "unknown": "SYNTH-SECRET-VALUE",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(PreconditionError, match="unknown fields") as unknown_error:
        load_notification_artifact(unknown)
    assert "SYNTH-SECRET-VALUE" not in str(unknown_error.value)

    duplicate = tmp_path / "duplicate-notification.json"
    duplicate.write_text(
        '{"notified_at_utc":"2026-07-29T00:00:00+00:00",'
        '"channel":"first","channel":"SYNTH-SECRET-VALUE",'
        '"storage_aliases":["synthetic.invalid"]}',
        encoding="utf-8",
    )
    with pytest.raises(PreconditionError, match="duplicate") as duplicate_error:
        load_notification_artifact(duplicate)
    assert "SYNTH-SECRET-VALUE" not in str(duplicate_error.value)


def test_malformed_notification_artifact_means_zero_calls(tmp_path: Path) -> None:
    broken = tmp_path / "notification.json"
    broken.write_text(json.dumps({"channel": "email:x", "storage_aliases": []}), encoding="utf-8")
    with pytest.raises(PreconditionError, match="storage-notification artifact"):
        run_preflight(
            registry_path=FIXTURE_ROOT / "registry_pending.json",
            approval_path=FIXTURE_ROOT / "owner_approval.json",
            notification_path=broken,
            tier_path=FIXTURE_ROOT / "tier_artifact.json",
            max_calls=MAX_CALLS,
            now=NOW,
            **_destinations(tmp_path),
        )


def test_missing_notification_artifact_means_zero_calls(tmp_path: Path) -> None:
    with pytest.raises(PreconditionError, match="missing"):
        run_preflight(
            registry_path=FIXTURE_ROOT / "registry_pending.json",
            approval_path=FIXTURE_ROOT / "owner_approval.json",
            notification_path=tmp_path / "absent.json",
            tier_path=FIXTURE_ROOT / "tier_artifact.json",
            max_calls=MAX_CALLS,
            now=NOW,
            **_destinations(tmp_path),
        )


def test_preflight_rejects_duplicate_tier_keys_before_usage_gate(tmp_path: Path) -> None:
    tier = tmp_path / "tier.json"
    tier.write_text(
        '{"calls_per_minute":750,"calls_per_minute":999,'
        '"calls_per_day":null,"bandwidth_gb_30d":null}',
        encoding="utf-8",
    )
    with pytest.raises(PreconditionError, match="duplicate"):
        run_preflight(
            registry_path=FIXTURE_ROOT / "registry_pending.json",
            approval_path=FIXTURE_ROOT / "owner_approval.json",
            notification_path=FIXTURE_ROOT / "storage_notification.json",
            tier_path=tier,
            max_calls=MAX_CALLS,
            now=NOW,
            **_destinations(tmp_path),
        )


def test_legacy_preflight_rejects_a_stale_usage_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        fmp_collector_cli_module,
        "load_trusted_fmp_usage_snapshot",
        lambda **_keywords: TrustedUsageSnapshot(
            source="stale-signed-checkpoint",
            recorded_at_utc=(NOW - timedelta(seconds=1)).isoformat(),
            integrity_sha256="a" * 64,
            authority_verified=True,
            calls_used_today=0,
            bytes_used_30d=0,
        ),
    )

    with pytest.raises(UsageEvidenceUnavailableError, match="current runtime instant"):
        _preflight(tmp_path)


# -- P4/P5 budget and usage gates -----------------------------------------


def test_max_calls_is_mandatory_and_positive(tmp_path: Path) -> None:
    with pytest.raises(PreconditionError, match="--max-calls is mandatory"):
        _preflight(tmp_path, max_calls=0)


def test_max_calls_above_authorized_ceiling_means_zero_calls(tmp_path: Path) -> None:
    with pytest.raises(PreconditionError, match="authorized ceiling of 25"):
        _preflight(tmp_path, registry="registry_cleared.json", max_calls=MAX_CALLS + 1)


def test_approval_budget_above_authorized_ceiling_is_rejected(tmp_path: Path) -> None:
    approval = _write_pending_approval(
        tmp_path / "approval.json",
        run_identity=APPROVED_RUN_ID,
        max_calls=MAX_CALLS + 1,
    )
    with pytest.raises(PreconditionError, match="authorized ceiling of 25"):
        load_approval_artifact(approval)


def test_exact_authorized_ceiling_reaches_the_usage_gate(tmp_path: Path) -> None:
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path, registry="registry_cleared.json", max_calls=MAX_CALLS)


def test_live_execution_fails_closed_without_trusted_usage_aggregation(tmp_path: Path) -> None:
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


@pytest.fixture
def fmp_usage_postgres(clean_postgres: Engine) -> Iterator[Engine]:
    yield clean_postgres
    with clean_postgres.begin() as connection:
        connection.exec_driver_sql(
            "ALTER TABLE collection_usage_checkpoints DISABLE TRIGGER "
            "collection_usage_checkpoints_append_only"
        )
        connection.execute(collection_usage_checkpoints.delete())
        connection.exec_driver_sql(
            "ALTER TABLE collection_usage_checkpoints ENABLE TRIGGER "
            "collection_usage_checkpoints_append_only"
        )


def _seed_signed_usage(  # noqa: C901, PLR0913 - adversarial PostgreSQL fixture builder
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    duplicate_daily: bool = False,
    daily_metric_failure: str | None = None,
    rolling_metric_failure: str | None = None,
) -> tuple[CollectionRegistry, Path]:
    registry = CollectionRegistry(engine)
    private_key = Ed25519PrivateKey.generate()
    authority_id = "external-fmp-usage-authority"
    key_id = "ed25519:2026-08"
    rolling_start = NOW - timedelta(days=30)
    day_start = NOW.replace(hour=0, minute=0, second=0, microsecond=0)

    def seed_run(
        run_id: str, provider: str, recorded_at: datetime, values: tuple[int, int]
    ) -> None:
        plan = CollectionRunPlan(
            plan_id=f"plan-{run_id}",
            schema_version=1,
            provider=provider,
            dataset="usage-preflight-test",
            mode=CollectionMode.PROBE,
            requested_window_start=None,
            requested_window_end=None,
            parameters={"synthetic": True, "run_id": run_id},
            created_at_utc=rolling_start - timedelta(days=1),
        )
        registry.register_plan(plan)
        registry.start_run(CollectionRun(run_id, plan.plan_id, plan.created_at_utc))
        records = [
            ("calls_attempted", Decimal(values[0]), "call"),
            ("bytes_received", Decimal(values[1]), "byte"),
        ]
        if provider == "fmp":
            if rolling_metric_failure == "missing":
                records.pop(1)
            elif rolling_metric_failure == "wrong-unit":
                records[1] = ("bytes_received", Decimal(values[1]), "octet")
            elif rolling_metric_failure == "duplicate":
                records.append(("bytes_received", Decimal(1), "octet"))
        if run_id == "fmp-today":
            if daily_metric_failure == "missing":
                records.pop(0)
            elif daily_metric_failure == "wrong-unit":
                records[0] = ("calls_attempted", Decimal(values[0]), "request")
            elif daily_metric_failure == "duplicate":
                records.append(("calls_attempted", Decimal(1), "request"))
        for usage_seq, (metric, quantity, unit) in enumerate(records, start=1):
            registry.record_usage(
                CollectionUsageRecord(
                    run_id=run_id,
                    usage_seq=usage_seq,
                    metric=metric,
                    quantity=quantity,
                    unit=unit,
                    recorded_at_utc=recorded_at,
                    evidence={"synthetic": True},
                )
            )

    seed_run("fmp-prior", "fmp", day_start - timedelta(days=1), (2, 799))
    seed_run("fmp-today", "fmp", day_start + timedelta(hours=1), (7, 100))
    seed_run("sec-today", "sec", day_start + timedelta(hours=1), (999, 999))
    keyring = Ed25519PublicKeyring(
        {(authority_id, key_id): private_key.public_key().public_bytes_raw()}
    )
    for checkpoint_id, start in (
        ("fmp-rolling-30d", rolling_start),
        ("fmp-current-day", day_start),
    ):
        candidate = registry.prepare_fmp_usage_checkpoint_candidate(
            checkpoint_id=checkpoint_id,
            coverage_start_utc=start,
            coverage_end_utc=NOW,
            authority_id=authority_id,
            key_id=key_id,
            generated_at_utc=NOW,
        )
        registry.register_usage_checkpoint(
            SignedUsageCheckpoint(
                candidate.checkpoint,
                private_key.sign(candidate.payload_bytes),
            ),
            candidate.leaves,
            keyring,
        )
    if duplicate_daily:
        candidate = registry.prepare_fmp_usage_checkpoint_candidate(
            checkpoint_id="fmp-current-day-duplicate",
            coverage_start_utc=day_start,
            coverage_end_utc=NOW,
            authority_id=authority_id,
            key_id=key_id,
            generated_at_utc=NOW,
        )
        registry.register_usage_checkpoint(
            SignedUsageCheckpoint(candidate.checkpoint, private_key.sign(candidate.payload_bytes)),
            candidate.leaves,
            keyring,
        )
    tmp_path.chmod(0o700)
    authority_path = tmp_path / "fmp-usage-authority.json"
    authority_path.write_text(
        json.dumps(
            {
                "schema": "aegis-alpha/fmp-usage-authority",
                "version": 1,
                "authority_id": authority_id,
                "key_id": key_id,
                "public_key_encoding": "raw-ed25519-hex",
                "public_key": private_key.public_key().public_bytes_raw().hex(),
                "valid_from_utc": "2026-07-28T12:00:00.000000Z",
                "valid_until_utc": "2026-07-30T12:00:00.000000Z",
            }
        ),
        encoding="utf-8",
    )
    authority_path.chmod(0o600)
    monkeypatch.setenv("AAS_DATABASE_URL", engine.url.render_as_string(hide_password=False))
    monkeypatch.setenv("AAS_FMP_USAGE_AUTHORITY_PATH", str(authority_path))
    return registry, authority_path


def test_preflight_builds_usage_only_from_two_current_verified_fmp_checkpoints(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)

    result = run_preflight(
        registry_path=FIXTURE_ROOT / "registry_pending.json",
        approval_path=FIXTURE_ROOT / "owner_approval.json",
        notification_path=FIXTURE_ROOT / "storage_notification.json",
        tier_path=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=MAX_CALLS,
        now=NOW,
        **_destinations(tmp_path),
    )

    assert result.usage_baseline.calls_used_today == _EXPECTED_DAILY_CALLS
    assert result.usage_baseline.bytes_used_30d == _EXPECTED_ROLLING_BYTES
    assert result.usage_baseline.authority_verified is True
    assert "fmp-current-day" in result.usage_baseline.source
    assert "fmp-rolling-30d" in result.usage_baseline.source


def test_verified_preflight_usage_enforces_historical_daily_and_bandwidth_caps(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    result = run_preflight(
        registry_path=FIXTURE_ROOT / "registry_pending.json",
        approval_path=FIXTURE_ROOT / "owner_approval.json",
        notification_path=FIXTURE_ROOT / "storage_notification.json",
        tier_path=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=MAX_CALLS,
        now=NOW,
        **_destinations(tmp_path),
    )
    limiter = RateLimiter(
        tier=TierArtifact(calls_per_minute=100, calls_per_day=10, bandwidth_gb_30d=0.000001),
        max_calls=10,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
        run_seed=1,
        usage_baseline=result.usage_baseline,
    )
    for _ in range(3):
        limiter.before_request()
    with pytest.raises(BudgetExhaustedError, match="daily cap"):
        limiter.before_request()
    with pytest.raises(BudgetExhaustedError, match="90% cutoff"):
        limiter.after_response(byte_count=1)


@pytest.mark.parametrize("failure", ["missing-db", "missing-artifact", "malformed-artifact"])
def test_preflight_usage_trust_inputs_fail_closed(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    _registry, authority_path = _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    if failure == "missing-db":
        monkeypatch.delenv("AAS_DATABASE_URL")
    elif failure == "missing-artifact":
        monkeypatch.setenv("AAS_FMP_USAGE_AUTHORITY_PATH", str(tmp_path / "missing.json"))
    else:
        authority_path.write_text("{}", encoding="utf-8")
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


def test_authority_json_parser_rejects_duplicates_at_every_object_level() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        fmp_usage_authority_module._strict_json_object(  # noqa: SLF001 - boundary regression
            b'{"version":1,"version":1}'
        )
    with pytest.raises(ValueError, match="duplicate"):
        fmp_usage_authority_module._strict_json_object(  # noqa: SLF001 - boundary regression
            b'{"outer":{"key":1,"key":2}}'
        )


def test_preflight_refuses_duplicate_top_level_authority_keys(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _registry, authority_path = _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    payload = authority_path.read_text(encoding="utf-8")
    authority_path.write_text(
        payload.replace('"version": 1,', '"version": 1, "version": 1,'), encoding="utf-8"
    )
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


@pytest.mark.parametrize("mode", [0o666, 0o620])
def test_preflight_refuses_group_or_world_writable_authority_files(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: int,
) -> None:
    _registry, authority_path = _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    authority_path.chmod(mode)
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


def test_preflight_refuses_wrong_owner_and_writable_parent_boundaries(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    real_euid = os.geteuid()
    monkeypatch.setattr(fmp_usage_authority_module.os, "geteuid", lambda: real_euid + 1)
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)
    monkeypatch.setattr(fmp_usage_authority_module.os, "geteuid", lambda: real_euid)
    tmp_path.chmod(0o720)
    try:
        with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
            _preflight(tmp_path)
    finally:
        tmp_path.chmod(0o700)


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_preflight_refuses_symlink_and_nonregular_authority_files(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    _registry, authority_path = _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    authority_path.unlink()
    if kind == "symlink":
        target = tmp_path / "authority-target.json"
        target.write_text("{}", encoding="utf-8")
        target.chmod(0o600)
        authority_path.symlink_to(target)
    else:
        os.mkfifo(authority_path, mode=0o600)
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


@pytest.mark.parametrize("version", [True, 1.0, 2])
def test_preflight_refuses_nonexact_authority_versions(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    version: object,
) -> None:
    _registry, authority_path = _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    document = json.loads(authority_path.read_bytes())
    document["version"] = version
    authority_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-07-28T12:00:00.000000+00:00",
        "2026-07-28T21:00:00.000000+09:00",
        "2026-07-28T12:00:00Z",
    ],
)
def test_preflight_refuses_noncanonical_authority_timestamps(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    timestamp: str,
) -> None:
    _registry, authority_path = _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    document = json.loads(authority_path.read_bytes())
    document["valid_from_utc"] = timestamp
    authority_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


@pytest.mark.parametrize("bounds", [("equal", "equal"), ("later", "earlier")])
def test_preflight_refuses_nonincreasing_authority_validity(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bounds: tuple[str, str],
) -> None:
    _registry, authority_path = _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    document = json.loads(authority_path.read_bytes())
    values = {
        "earlier": "2026-07-28T12:00:00.000000Z",
        "equal": "2026-07-29T12:00:00.000000Z",
        "later": "2026-07-30T12:00:00.000000Z",
    }
    document["valid_from_utc"] = values[bounds[0]]
    document["valid_until_utc"] = values[bounds[1]]
    authority_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


@pytest.mark.parametrize("field", ["authority_id", "key_id"])
@pytest.mark.parametrize(
    "identifier",
    ["has space", "unicode-ü", "control-\x01", "Uppercase", "-leading", "a" * 256],
)
def test_preflight_refuses_non_normalized_authority_identifiers(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    identifier: str,
    field: str,
) -> None:
    _registry, authority_path = _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    document = json.loads(authority_path.read_bytes())
    document[field] = identifier
    authority_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL") as raised:
        _preflight(tmp_path)
    assert identifier not in str(raised.value)


def test_preflight_missing_artifact_error_does_not_leak_its_path(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    sentinel = "secret-authority-location"
    monkeypatch.setenv("AAS_FMP_USAGE_AUTHORITY_PATH", str(tmp_path / sentinel))
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL") as raised:
        _preflight(tmp_path)
    assert sentinel not in str(raised.value)


def test_preflight_refuses_atomically_replaced_authority_key_without_leaking_it(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _registry, authority_path = _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    document = json.loads(authority_path.read_bytes())
    sentinel = "11" * 32
    document["public_key"] = sentinel
    replacement = tmp_path / "replacement.json"
    replacement.write_text(json.dumps(document), encoding="utf-8")
    replacement.chmod(0o600)
    replacement.replace(authority_path)
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL") as raised:
        _preflight(tmp_path)
    assert sentinel not in str(raised.value)


def test_preflight_refuses_persisted_quantity_tamper_after_signing(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    with fmp_usage_postgres.begin() as connection:
        connection.execute(
            collection_usage_records.update()
            .where(collection_usage_records.c.run_id == "fmp-today")
            .values(quantity=Decimal(8))
        )
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


def test_preflight_refuses_ambiguous_exact_checkpoint(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch, duplicate_daily=True)
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


@pytest.mark.parametrize("failure", ["missing", "wrong-unit", "duplicate"])
def test_preflight_refuses_missing_wrong_or_duplicate_usage_metric_units(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    _seed_signed_usage(
        fmp_usage_postgres,
        tmp_path,
        monkeypatch,
        daily_metric_failure=failure,
    )
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


@pytest.mark.parametrize("failure", ["missing", "wrong-unit", "duplicate"])
def test_preflight_refuses_missing_wrong_or_duplicate_rolling_byte_units(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    _seed_signed_usage(
        fmp_usage_postgres,
        tmp_path,
        monkeypatch,
        rolling_metric_failure=failure,
    )
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


@pytest.mark.parametrize("failure", ["stale", "wrong-key", "private-key", "usage-number"])
def test_preflight_refuses_untrusted_or_overpowered_authority_artifacts(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    _registry, authority_path = _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    document = json.loads(authority_path.read_text(encoding="utf-8"))
    if failure == "stale":
        document["valid_until_utc"] = "2026-07-29T11:59:59.000000Z"
    elif failure == "wrong-key":
        document["public_key"] = "00" * 32
    elif failure == "private-key":
        document["private_key"] = "forbidden"
    else:
        document["calls_used_today"] = 7
    authority_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


def test_preflight_refuses_missing_checkpoint_and_signature_tamper(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    with fmp_usage_postgres.begin() as connection:
        connection.exec_driver_sql(
            "ALTER TABLE collection_usage_checkpoints DISABLE TRIGGER "
            "collection_usage_checkpoints_append_only"
        )
        connection.execute(
            collection_usage_checkpoints.update()
            .where(collection_usage_checkpoints.c.checkpoint_id == "fmp-current-day")
            .values(signature=b"0" * 64)
        )
        connection.exec_driver_sql(
            "ALTER TABLE collection_usage_checkpoints ENABLE TRIGGER "
            "collection_usage_checkpoints_append_only"
        )
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)

    with fmp_usage_postgres.begin() as connection:
        connection.exec_driver_sql(
            "ALTER TABLE collection_usage_checkpoints DISABLE TRIGGER "
            "collection_usage_checkpoints_append_only"
        )
        connection.execute(
            collection_usage_checkpoints.delete().where(
                collection_usage_checkpoints.c.checkpoint_id == "fmp-current-day"
            )
        )
        connection.exec_driver_sql(
            "ALTER TABLE collection_usage_checkpoints ENABLE TRIGGER "
            "collection_usage_checkpoints_append_only"
        )
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL"):
        _preflight(tmp_path)


def test_preflight_refuses_an_unavailable_database_without_exposing_details(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)

    def unavailable(*_args: object, **_kwargs: object) -> None:
        raise SQLAlchemyError("synthetic database detail must stay internal")

    monkeypatch.setattr(fmp_usage_trust_module, "create_engine", unavailable)
    with pytest.raises(UsageEvidenceUnavailableError, match="BLOCKED_EXTERNAL") as raised:
        _preflight(tmp_path)
    assert "synthetic" not in str(raised.value)


def test_the_fail_closed_gate_is_reached_only_after_every_earlier_gate(tmp_path: Path) -> None:
    # A malformed tier artifact must be rejected before the usage gate,
    # proving the usage gate is last and no call could have happened.
    broken_tier = tmp_path / "tier.json"
    broken_tier.write_text(json.dumps({"calls_per_minute": 0}), encoding="utf-8")
    with pytest.raises(PreconditionError, match="missing required fields"):
        run_preflight(
            registry_path=FIXTURE_ROOT / "registry_pending.json",
            approval_path=FIXTURE_ROOT / "owner_approval.json",
            notification_path=FIXTURE_ROOT / "storage_notification.json",
            tier_path=broken_tier,
            max_calls=MAX_CALLS,
            now=NOW,
            **_destinations(tmp_path),
        )


# -- G1 destinations -------------------------------------------------------


def test_a_git_contained_destination_means_zero_calls(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    (repository / ".git").mkdir(parents=True)
    with pytest.raises(DestinationError, match="outside a Git repository"):
        run_preflight(
            registry_path=FIXTURE_ROOT / "registry_pending.json",
            approval_path=FIXTURE_ROOT / "owner_approval.json",
            notification_path=FIXTURE_ROOT / "storage_notification.json",
            tier_path=FIXTURE_ROOT / "tier_artifact.json",
            max_calls=MAX_CALLS,
            now=NOW,
            raw_store_root=repository / "raw",
            dataset_root=tmp_path / "datasets",
            receipt_path=tmp_path / "receipts" / "run.receipt.json",
        )


# -- CLI surface -----------------------------------------------------------


def test_missing_credential_means_zero_calls_and_exit_two(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    code, stdout, stderr = _cli(tmp_path, monkeypatch, environ={})
    assert code == PRECONDITION_EXIT
    assert stdout == ""
    assert "no provider calls were attempted" in stderr


def test_cli_fails_closed_on_the_usage_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    code, stdout, stderr = _cli(tmp_path, monkeypatch)
    assert code == PRECONDITION_EXIT
    assert stdout == ""
    assert "BLOCKED_EXTERNAL" in stderr
    assert "no provider calls were attempted" in stderr


def test_cli_parser_rejects_a_missing_approval() -> None:
    with pytest.raises(SystemExit, match="2"):
        main(["collect"])


def test_cli_rejects_a_malformed_universe_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stdout = io.StringIO()
    stderr = io.StringIO()
    destinations = _destinations(tmp_path)
    arguments = argparse.Namespace(
        command="collect",
        registry=FIXTURE_ROOT / "registry_pending.json",
        owner_approval=tmp_path / "approval.json",
        owner_approval_signature=tmp_path / "approval.sig",
        storage_notification=FIXTURE_ROOT / "storage_notification.json",
        tier=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=MAX_CALLS,
        raw_store_root=destinations["raw_store_root"],
        dataset_root=destinations["dataset_root"],
        receipt_path=destinations["receipt_path"],
        universe_manifest=FIXTURE_ROOT / "universe_manifest_duplicate_symbol.json",
        mode="incremental",
        dataset_selection="probe",
        operator_from=None,
    )
    environment = install_signed_approval(
        arguments,
        SigningContext(root=tmp_path, monkeypatch=monkeypatch, now=NOW),
    )
    code = main(
        [
            "collect",
            "--registry",
            str(arguments.registry),
            "--owner-approval",
            str(arguments.owner_approval),
            "--owner-approval-signature",
            str(arguments.owner_approval_signature),
            "--storage-notification",
            str(arguments.storage_notification),
            "--tier",
            str(arguments.tier),
            "--max-calls",
            str(arguments.max_calls),
            "--raw-store-root",
            str(arguments.raw_store_root),
            "--dataset-root",
            str(arguments.dataset_root),
            "--receipt-path",
            str(arguments.receipt_path),
            "--universe-manifest",
            str(arguments.universe_manifest),
            "--mode",
            arguments.mode,
        ],
        environ=environment,
        stdout=stdout,
        stderr=stderr,
        now=NOW,
    )
    assert code == PRECONDITION_EXIT
    assert "duplicate symbol" in stderr.getvalue()


def test_collect_help_requires_receipt_and_input_manifest(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit, match="0"):
        main(["collect", "--help"])
    help_text = capsys.readouterr().out
    assert "--receipt-path" in help_text
    assert "--owner-approval-signature" in help_text
    assert "--universe-manifest" in help_text
    assert "--universe-manifest-out" not in help_text


def test_build_universe_help_requires_only_manifest_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit, match="0"):
        main(["build-universe", "--help"])
    help_text = capsys.readouterr().out
    assert "--universe-manifest-out" in help_text
    assert "--owner-approval-signature" in help_text
    assert "--receipt-path" not in help_text
    assert "--universe-manifest " not in help_text


def test_build_universe_rejects_the_old_receipt_flag() -> None:
    with pytest.raises(SystemExit, match="2"):
        main(
            [
                "build-universe",
                "--owner-approval",
                "approval.json",
                "--storage-notification",
                "notification.json",
                "--tier",
                "tier.json",
                "--max-calls",
                "25",
                "--raw-store-root",
                "raw",
                "--dataset-root",
                "datasets",
                "--receipt-path",
                "universe.json",
            ]
        )


def test_build_universe_preflight_validates_the_manifest_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository = tmp_path / "repository"
    (repository / ".git").mkdir(parents=True)
    arguments = argparse.Namespace(
        command="build-universe",
        registry=FIXTURE_ROOT / "registry_cleared.json",
        owner_approval=tmp_path / "approval.json",
        owner_approval_signature=tmp_path / "approval.sig",
        storage_notification=FIXTURE_ROOT / "storage_notification.json",
        tier=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=MAX_CALLS,
        raw_store_root=tmp_path / "raw",
        dataset_root=tmp_path / "datasets",
        universe_manifest_out=repository / "universe.json",
    )
    environment = install_signed_approval(
        arguments,
        SigningContext(root=tmp_path, monkeypatch=monkeypatch, now=NOW),
    )
    stderr = io.StringIO()
    code = main(
        [
            "build-universe",
            "--registry",
            str(arguments.registry),
            "--owner-approval",
            str(arguments.owner_approval),
            "--owner-approval-signature",
            str(arguments.owner_approval_signature),
            "--storage-notification",
            str(arguments.storage_notification),
            "--tier",
            str(arguments.tier),
            "--max-calls",
            str(arguments.max_calls),
            "--raw-store-root",
            str(arguments.raw_store_root),
            "--dataset-root",
            str(arguments.dataset_root),
            "--universe-manifest-out",
            str(arguments.universe_manifest_out),
        ],
        environ=environment,
        stdout=io.StringIO(),
        stderr=stderr,
        now=NOW,
    )
    assert code == PRECONDITION_EXIT
    assert "command output path must be outside a Git repository" in stderr.getvalue()


def test_build_universe_is_budgeted_and_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stdout = io.StringIO()
    stderr = io.StringIO()
    destinations = _destinations(tmp_path)
    arguments = argparse.Namespace(
        command="build-universe",
        registry=FIXTURE_ROOT / "registry_pending.json",
        owner_approval=tmp_path / "approval.json",
        owner_approval_signature=tmp_path / "approval.sig",
        storage_notification=FIXTURE_ROOT / "storage_notification.json",
        tier=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=MAX_CALLS,
        raw_store_root=destinations["raw_store_root"],
        dataset_root=destinations["dataset_root"],
        universe_manifest_out=tmp_path / "manifests" / "universe.json",
    )
    environment = install_signed_approval(
        arguments,
        SigningContext(root=tmp_path, monkeypatch=monkeypatch, now=NOW),
    )
    code = main(
        [
            "build-universe",
            "--registry",
            str(arguments.registry),
            "--owner-approval",
            str(arguments.owner_approval),
            "--owner-approval-signature",
            str(arguments.owner_approval_signature),
            "--storage-notification",
            str(arguments.storage_notification),
            "--tier",
            str(arguments.tier),
            "--max-calls",
            str(arguments.max_calls),
            "--raw-store-root",
            str(arguments.raw_store_root),
            "--dataset-root",
            str(arguments.dataset_root),
            "--universe-manifest-out",
            str(arguments.universe_manifest_out),
        ],
        environ=environment,
        stdout=stdout,
        stderr=stderr,
        now=NOW,
    )
    assert code == PRECONDITION_EXIT
    assert "BLOCKED_EXTERNAL" in stderr.getvalue()


def test_build_universe_requires_max_calls() -> None:
    with pytest.raises(SystemExit):
        main(
            [
                "build-universe",
                "--owner-approval",
                "approval",
                "--storage-notification",
                "x",
                "--tier",
                "y",
            ],
            environ={"FMP_API_KEY": "SYNTH-CREDENTIAL"},
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            now=NOW,
        )


class EventApprovalClock:
    """Approval clock that holds still until a named test event advances it."""

    def __init__(self, current: datetime = NOW) -> None:
        self.current = current

    def now(self) -> datetime:
        return self.current

    def expire(self) -> None:
        self.current = NOW + timedelta(seconds=2)


class LiveClock:
    """Wall and monotonic test clock advanced only by transport or waiter."""

    def __init__(self) -> None:
        self.current = NOW + timedelta(hours=1)
        self.monotonic_seconds = 0.0
        self.waits: list[float] = []

    def now(self) -> datetime:
        return self.current

    def monotonic(self) -> float:
        return self.monotonic_seconds

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)
        self.monotonic_seconds += seconds

    def wait(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.advance(seconds)


class CliReplayTransport:
    def __init__(self) -> None:
        self.calls: list[CollectorRequest] = []

    def __call__(self, request: CollectorRequest, _credential: str) -> CollectorResponse:
        self.calls.append(request)
        symbol = request.symbol or "synth.none"
        requested_date = request.parameters.get("from", "2026-07-28")
        row: dict[str, object] = {
            "symbol": symbol.upper(),
            "date": requested_date,
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 100,
        }
        if request.endpoint == "/stable/dividends":
            row = {
                "symbol": symbol.upper(),
                "date": "2026-07-28",
                "dividend": 0.5,
                "adjDividend": 0.5,
            }
        body = json.dumps([row]).encode()
        return CollectorResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            body=body,
            requested_at_utc=NOW,
            retrieved_at_utc=NOW,
        )


def test_run_live_command_keeps_plan_moment_but_anchors_retry_to_live_time(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    destinations = _destinations(tmp_path)
    arguments = argparse.Namespace(
        command="collect",
        registry=FIXTURE_ROOT / "registry_cleared.json",
        owner_approval=tmp_path / "approval.json",
        owner_approval_signature=tmp_path / "approval.sig",
        storage_notification=FIXTURE_ROOT / "storage_notification.json",
        tier=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=6,
        raw_store_root=destinations["raw_store_root"],
        dataset_root=destinations["dataset_root"],
        receipt_path=destinations["receipt_path"],
        universe_manifest=FIXTURE_ROOT / "universe_manifest.json",
        mode="probe",
        dataset_selection=DatasetSelection.PROBE.value,
        operator_from=NOW.date().isoformat(),
    )
    environment = install_signed_approval(
        arguments,
        SigningContext(root=tmp_path, monkeypatch=monkeypatch, now=NOW),
    )
    environment["AAS_DATABASE_URL"] = os.environ["AAS_DATABASE_URL"]
    environment["AAS_FMP_USAGE_AUTHORITY_PATH"] = os.environ["AAS_FMP_USAGE_AUTHORITY_PATH"]
    clock = LiveClock()

    class RetryTransport(CliReplayTransport):
        def __call__(self, request: CollectorRequest, _credential: str) -> CollectorResponse:
            requested_at = clock.now()
            clock.advance(10.0)
            if not self.calls:
                self.calls.append(request)
                return CollectorResponse(
                    status_code=503,
                    headers={"content-type": "application/json"},
                    body=b"[]",
                    requested_at_utc=requested_at,
                    retrieved_at_utc=clock.now(),
                )
            response = super().__call__(request, _credential)
            return replace(
                response,
                requested_at_utc=requested_at,
                retrieved_at_utc=clock.now(),
            )

    transport = RetryTransport()

    outcome, calls = run_live_command(
        arguments=arguments,
        environment=environment,
        credential=environment["FMP_API_KEY"],
        moment=NOW,
        dependencies=RuntimeDependencies(
            transport=transport,
            monotonic=clock.monotonic,
            sleep=clock.wait,
            approval_clock=lambda: NOW,
            wall_clock=clock.now,
        ),
    )

    assert outcome.run_id == APPROVED_RUN_ID
    assert calls == len(transport.calls) == _EXPECTED_RETRY_RUN_CALLS
    first_attempt = json.loads(
        next((destinations["raw_store_root"] / "fmp" / "runs").rglob("00000001.json")).read_bytes()
    )
    first_provenance = next(
        json.loads(path.read_bytes())
        for path in (destinations["raw_store_root"] / "fmp" / "provenance").rglob("*.json")
        if json.loads(path.read_bytes())["status_code"] == _HTTP_SERVICE_UNAVAILABLE
    )
    retry_deadline = datetime.fromisoformat(first_attempt["retry_not_before_utc"])
    retrieved_at = datetime.fromisoformat(first_provenance["retrieved_at_utc"])
    assert retrieved_at > NOW
    assert retry_deadline - retrieved_at == timedelta(seconds=clock.waits[0])
    with fmp_usage_postgres.connect() as connection:
        created_at = connection.scalar(
            select(collection_run_plans.c.created_at_utc).where(
                collection_run_plans.c.plan_id == outcome.plan_id
            )
        )
    assert created_at == NOW


def test_cli_executes_the_offline_driver_against_the_real_registry(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    monkeypatch.setenv("FMP_API_KEY", "SYNTH-CLI-CREDENTIAL")
    destinations = _destinations(tmp_path)
    arguments = argparse.Namespace(
        command="collect",
        registry=FIXTURE_ROOT / "registry_cleared.json",
        owner_approval=tmp_path / "approval.json",
        owner_approval_signature=tmp_path / "approval.sig",
        storage_notification=FIXTURE_ROOT / "storage_notification.json",
        tier=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=6,
        raw_store_root=destinations["raw_store_root"],
        dataset_root=destinations["dataset_root"],
        receipt_path=destinations["receipt_path"],
        universe_manifest=FIXTURE_ROOT / "universe_manifest.json",
        mode="probe",
        dataset_selection=DatasetSelection.PROBE.value,
        operator_from=NOW.date().isoformat(),
    )
    for name, value in install_signed_approval(
        arguments, SigningContext(tmp_path, monkeypatch, NOW)
    ).items():
        monkeypatch.setenv(name, value)
    replay = CliReplayTransport()
    stdout = io.StringIO()
    stderr = io.StringIO()

    code = main(
        [
            "collect",
            "--registry",
            str(arguments.registry),
            "--owner-approval",
            str(arguments.owner_approval),
            "--owner-approval-signature",
            str(arguments.owner_approval_signature),
            "--storage-notification",
            str(FIXTURE_ROOT / "storage_notification.json"),
            "--tier",
            str(FIXTURE_ROOT / "tier_artifact.json"),
            "--max-calls",
            "6",
            "--raw-store-root",
            str(destinations["raw_store_root"]),
            "--dataset-root",
            str(destinations["dataset_root"]),
            "--receipt-path",
            str(destinations["receipt_path"]),
            "--universe-manifest",
            str(FIXTURE_ROOT / "universe_manifest.json"),
            "--mode",
            "probe",
            "--dataset-selection",
            "probe",
        ],
        stdout=stdout,
        stderr=stderr,
        now=NOW,
        transport=replay,
        monotonic=lambda: 0.0,
        sleep=lambda _seconds: None,
        approval_clock=lambda: NOW,
    )

    assert code == 0, stderr.getvalue() or stdout.getvalue()
    summary = json.loads(stdout.getvalue())
    assert summary["run_id"] == APPROVED_RUN_ID
    assert summary["provider_calls"] == len(replay.calls)
    assert summary["terminal_event"] == "run_succeeded"
    assert [(request.symbol, request.endpoint) for request in replay.calls] == [
        ("synth.a", "/stable/profile"),
        ("synth.a", "/stable/historical-price-eod/full"),
        ("synth.b", "/stable/profile"),
        ("synth.b", "/stable/historical-price-eod/full"),
    ]
    assert destinations["receipt_path"].is_file()
    with fmp_usage_postgres.connect() as connection:
        assert (
            connection.scalar(
                select(func.count())
                .select_from(collection_run_plans)
                .where(collection_run_plans.c.plan_id == summary["plan_id"])
            )
            == 1
        )
        assert (
            connection.scalar(
                select(func.count())
                .select_from(collection_runs)
                .where(collection_runs.c.run_id == APPROVED_RUN_ID)
            )
            == 1
        )
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == APPROVED_RUN_ID)
                .order_by(collection_run_events.c.event_seq)
            ).scalars()
        )
        streams = set(
            connection.execute(
                select(collection_watermarks.c.stream).where(
                    collection_watermarks.c.run_id == APPROVED_RUN_ID
                )
            ).scalars()
        )
    assert events == ["attempt_started", "attempt_succeeded", "run_succeeded"]
    assert streams == {"synth.a", "synth.b"}


def test_pending_approval_expiry_cancels_real_run_before_the_next_retry_call(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    monkeypatch.setenv("FMP_API_KEY", "SYNTH-PENDING-CREDENTIAL")
    destinations = _destinations(tmp_path)
    arguments = argparse.Namespace(
        command="collect",
        registry=FIXTURE_ROOT / "registry_pending.json",
        owner_approval=tmp_path / "approval.json",
        owner_approval_signature=tmp_path / "approval.sig",
        storage_notification=FIXTURE_ROOT / "storage_notification.json",
        tier=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=4,
        raw_store_root=destinations["raw_store_root"],
        dataset_root=destinations["dataset_root"],
        receipt_path=destinations["receipt_path"],
        universe_manifest=FIXTURE_ROOT / "universe_manifest.json",
        mode="probe",
        dataset_selection=DatasetSelection.PROBE.value,
        operator_from=None,
    )
    for name, value in install_signed_approval(
        arguments,
        SigningContext(tmp_path, monkeypatch, NOW, expires_at_utc=NOW + timedelta(seconds=1)),
    ).items():
        monkeypatch.setenv(name, value)
    clock = EventApprovalClock()

    class TimeoutTransport(CliReplayTransport):
        def __call__(self, request: CollectorRequest, _credential: str) -> CollectorResponse:
            self.calls.append(request)
            clock.expire()
            raise TimeoutError("synthetic timeout before approval expiry")

    replay = TimeoutTransport()
    stdout = io.StringIO()
    stderr = io.StringIO()

    code = main(
        [
            "collect",
            "--registry",
            str(FIXTURE_ROOT / "registry_pending.json"),
            "--owner-approval",
            str(arguments.owner_approval),
            "--owner-approval-signature",
            str(arguments.owner_approval_signature),
            "--storage-notification",
            str(FIXTURE_ROOT / "storage_notification.json"),
            "--tier",
            str(FIXTURE_ROOT / "tier_artifact.json"),
            "--max-calls",
            "4",
            "--raw-store-root",
            str(destinations["raw_store_root"]),
            "--dataset-root",
            str(destinations["dataset_root"]),
            "--receipt-path",
            str(destinations["receipt_path"]),
            "--universe-manifest",
            str(FIXTURE_ROOT / "universe_manifest.json"),
            "--mode",
            "probe",
        ],
        stdout=stdout,
        stderr=stderr,
        now=NOW,
        transport=replay,
        monotonic=lambda: 0.0,
        sleep=lambda _seconds: None,
        approval_clock=clock.now,
    )

    assert code == 1, stderr.getvalue()
    assert json.loads(stdout.getvalue())["run_id"] == APPROVED_RUN_ID
    assert len(replay.calls) == 1
    with fmp_usage_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == APPROVED_RUN_ID)
                .order_by(collection_run_events.c.event_seq)
            ).scalars()
        )
        calls = connection.scalar(
            select(collection_usage_records.c.quantity).where(
                collection_usage_records.c.run_id == APPROVED_RUN_ID,
                collection_usage_records.c.metric == "calls_attempted",
            )
        )
    assert events == ["attempt_started", "run_cancelled"]
    assert calls == 1


def test_build_universe_approval_expiry_cancels_before_retry_with_usage(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    monkeypatch.setenv("FMP_API_KEY", "SYNTH-PENDING-CREDENTIAL")
    destinations = _destinations(tmp_path)
    arguments = argparse.Namespace(
        command="build-universe",
        registry=FIXTURE_ROOT / "registry_pending.json",
        owner_approval=tmp_path / "approval.json",
        owner_approval_signature=tmp_path / "approval.sig",
        storage_notification=FIXTURE_ROOT / "storage_notification.json",
        tier=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=4,
        raw_store_root=destinations["raw_store_root"],
        dataset_root=destinations["dataset_root"],
        universe_manifest_out=tmp_path / "manifests" / "universe.json",
    )
    for name, value in install_signed_approval(
        arguments,
        SigningContext(tmp_path, monkeypatch, NOW, expires_at_utc=NOW + timedelta(seconds=1)),
    ).items():
        monkeypatch.setenv(name, value)
    clock = EventApprovalClock()

    class TimeoutTransport(CliReplayTransport):
        def __call__(self, request: CollectorRequest, _credential: str) -> CollectorResponse:
            self.calls.append(request)
            clock.expire()
            raise TimeoutError("synthetic universe timeout before approval expiry")

    replay = TimeoutTransport()
    stdout = io.StringIO()
    stderr = io.StringIO()

    code = main(
        [
            "build-universe",
            "--registry",
            str(FIXTURE_ROOT / "registry_pending.json"),
            "--owner-approval",
            str(arguments.owner_approval),
            "--owner-approval-signature",
            str(arguments.owner_approval_signature),
            "--storage-notification",
            str(FIXTURE_ROOT / "storage_notification.json"),
            "--tier",
            str(FIXTURE_ROOT / "tier_artifact.json"),
            "--max-calls",
            "4",
            "--raw-store-root",
            str(destinations["raw_store_root"]),
            "--dataset-root",
            str(destinations["dataset_root"]),
            "--universe-manifest-out",
            str(tmp_path / "manifests" / "universe.json"),
        ],
        stdout=stdout,
        stderr=stderr,
        now=NOW,
        transport=replay,
        monotonic=lambda: 0.0,
        sleep=lambda _seconds: None,
        approval_clock=clock.now,
    )

    assert code == 1, stderr.getvalue()
    summary = json.loads(stdout.getvalue())
    assert summary["run_id"] == APPROVED_RUN_ID
    assert summary["terminal_event"] == "run_cancelled"
    assert [(call.endpoint, call.page) for call in replay.calls] == [
        ("/stable/actively-trading-list", 0)
    ]
    assert not (tmp_path / "manifests" / "universe.json").exists()
    with fmp_usage_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == APPROVED_RUN_ID)
                .order_by(collection_run_events.c.event_seq)
            ).scalars()
        )
        calls = connection.scalar(
            select(collection_usage_records.c.quantity).where(
                collection_usage_records.c.run_id == APPROVED_RUN_ID,
                collection_usage_records.c.metric == "calls_attempted",
            )
        )
    assert events == ["attempt_started", "run_cancelled"]
    assert calls == 1


def test_expiry_between_registration_and_first_slot_blocks_the_slot(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    monkeypatch.setenv("FMP_API_KEY", "SYNTH-PENDING-CREDENTIAL")
    destinations = _destinations(tmp_path)
    arguments = argparse.Namespace(
        command="collect",
        registry=FIXTURE_ROOT / "registry_pending.json",
        owner_approval=tmp_path / "approval.json",
        owner_approval_signature=tmp_path / "approval.sig",
        storage_notification=FIXTURE_ROOT / "storage_notification.json",
        tier=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=4,
        raw_store_root=destinations["raw_store_root"],
        dataset_root=destinations["dataset_root"],
        receipt_path=destinations["receipt_path"],
        universe_manifest=FIXTURE_ROOT / "universe_manifest.json",
        mode="probe",
        dataset_selection=DatasetSelection.PROBE.value,
        operator_from=None,
    )
    for name, value in install_signed_approval(
        arguments,
        SigningContext(tmp_path, monkeypatch, NOW, expires_at_utc=NOW + timedelta(seconds=1)),
    ).items():
        monkeypatch.setenv(name, value)
    clock = EventApprovalClock()
    original_register = FmpCollector.register

    def register_then_expire(self: FmpCollector, plan: CollectionRunPlan, *, run_id: str) -> None:
        original_register(self, plan, run_id=run_id)
        clock.expire()

    monkeypatch.setattr(FmpCollector, "register", register_then_expire)
    replay = CliReplayTransport()
    stdout = io.StringIO()
    stderr = io.StringIO()

    code = main(
        [
            "collect",
            "--registry",
            str(FIXTURE_ROOT / "registry_pending.json"),
            "--owner-approval",
            str(arguments.owner_approval),
            "--owner-approval-signature",
            str(arguments.owner_approval_signature),
            "--storage-notification",
            str(FIXTURE_ROOT / "storage_notification.json"),
            "--tier",
            str(FIXTURE_ROOT / "tier_artifact.json"),
            "--max-calls",
            "4",
            "--raw-store-root",
            str(destinations["raw_store_root"]),
            "--dataset-root",
            str(destinations["dataset_root"]),
            "--receipt-path",
            str(destinations["receipt_path"]),
            "--universe-manifest",
            str(FIXTURE_ROOT / "universe_manifest.json"),
            "--mode",
            "probe",
        ],
        stdout=stdout,
        stderr=stderr,
        now=NOW,
        transport=replay,
        monotonic=lambda: 0.0,
        sleep=lambda _seconds: None,
        approval_clock=clock.now,
    )

    assert code == 1, stderr.getvalue()
    assert stdout.getvalue() == ""
    assert "ApprovalExpiredError" in stderr.getvalue()
    assert replay.calls == []
    with fmp_usage_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == APPROVED_RUN_ID)
                .order_by(collection_run_events.c.event_seq)
            ).scalars()
        )
        calls = connection.scalar(
            select(collection_usage_records.c.quantity).where(
                collection_usage_records.c.run_id == APPROVED_RUN_ID,
                collection_usage_records.c.metric == "calls_attempted",
            )
        )
    assert events == ["attempt_started"]
    assert calls is None


def test_expiry_between_no_wait_consecutive_slots_blocks_the_next_slot(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    monkeypatch.setenv("FMP_API_KEY", "SYNTH-PENDING-CREDENTIAL")
    destinations = _destinations(tmp_path)
    arguments = argparse.Namespace(
        command="collect",
        registry=FIXTURE_ROOT / "registry_pending.json",
        owner_approval=tmp_path / "approval.json",
        owner_approval_signature=tmp_path / "approval.sig",
        storage_notification=FIXTURE_ROOT / "storage_notification.json",
        tier=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=4,
        raw_store_root=destinations["raw_store_root"],
        dataset_root=destinations["dataset_root"],
        receipt_path=destinations["receipt_path"],
        universe_manifest=FIXTURE_ROOT / "universe_manifest.json",
        mode="probe",
        dataset_selection=DatasetSelection.PROBE.value,
        operator_from=None,
    )
    for name, value in install_signed_approval(
        arguments,
        SigningContext(tmp_path, monkeypatch, NOW, expires_at_utc=NOW + timedelta(seconds=1)),
    ).items():
        monkeypatch.setenv(name, value)
    clock = EventApprovalClock()
    monotonic_seconds = 0.0

    def monotonic() -> float:
        return monotonic_seconds

    class ExpireAfterFirstSlot(CliReplayTransport):
        def __call__(self, request: CollectorRequest, _credential: str) -> CollectorResponse:
            nonlocal monotonic_seconds
            response = super().__call__(request, _credential)
            clock.expire()
            monotonic_seconds = 1.0
            return response

    replay = ExpireAfterFirstSlot()
    stdout = io.StringIO()
    stderr = io.StringIO()

    code = main(
        [
            "collect",
            "--registry",
            str(FIXTURE_ROOT / "registry_pending.json"),
            "--owner-approval",
            str(arguments.owner_approval),
            "--owner-approval-signature",
            str(arguments.owner_approval_signature),
            "--storage-notification",
            str(FIXTURE_ROOT / "storage_notification.json"),
            "--tier",
            str(FIXTURE_ROOT / "tier_artifact.json"),
            "--max-calls",
            "4",
            "--raw-store-root",
            str(destinations["raw_store_root"]),
            "--dataset-root",
            str(destinations["dataset_root"]),
            "--receipt-path",
            str(destinations["receipt_path"]),
            "--universe-manifest",
            str(FIXTURE_ROOT / "universe_manifest.json"),
            "--mode",
            "probe",
        ],
        stdout=stdout,
        stderr=stderr,
        now=NOW,
        transport=replay,
        monotonic=monotonic,
        sleep=lambda _seconds: None,
        approval_clock=clock.now,
    )

    assert code == 1, stderr.getvalue()
    summary = json.loads(stdout.getvalue())
    assert summary["run_id"] == APPROVED_RUN_ID
    assert summary["terminal_event"] == "run_cancelled"
    assert len(replay.calls) == 1
    with fmp_usage_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == APPROVED_RUN_ID)
                .order_by(collection_run_events.c.event_seq)
            ).scalars()
        )
        calls = connection.scalar(
            select(collection_usage_records.c.quantity).where(
                collection_usage_records.c.run_id == APPROVED_RUN_ID,
                collection_usage_records.c.metric == "calls_attempted",
            )
        )
    assert events == ["attempt_started", "run_cancelled"]
    assert calls == 1


def test_pending_approval_rejects_noncanonical_identity_before_transport_construction(
    fmp_usage_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_signed_usage(fmp_usage_postgres, tmp_path, monkeypatch)
    monkeypatch.setenv("FMP_API_KEY", "SYNTH-PENDING-CREDENTIAL")
    destinations = _destinations(tmp_path)
    replay = CliReplayTransport()
    arguments = argparse.Namespace(
        command="collect",
        registry=FIXTURE_ROOT / "registry_pending.json",
        owner_approval=tmp_path / "approval.json",
        owner_approval_signature=tmp_path / "approval.sig",
        storage_notification=FIXTURE_ROOT / "storage_notification.json",
        tier=FIXTURE_ROOT / "tier_artifact.json",
        max_calls=4,
        raw_store_root=destinations["raw_store_root"],
        dataset_root=destinations["dataset_root"],
        receipt_path=destinations["receipt_path"],
        universe_manifest=FIXTURE_ROOT / "universe_manifest.json",
        mode="probe",
        dataset_selection=DatasetSelection.PROBE.value,
        operator_from=None,
    )
    for name, value in install_signed_approval(
        arguments, SigningContext(tmp_path, monkeypatch, NOW, run_identity="wrong-run")
    ).items():
        monkeypatch.setenv(name, value)

    code = main(
        [
            "collect",
            "--registry",
            str(FIXTURE_ROOT / "registry_pending.json"),
            "--owner-approval",
            str(arguments.owner_approval),
            "--owner-approval-signature",
            str(arguments.owner_approval_signature),
            "--storage-notification",
            str(FIXTURE_ROOT / "storage_notification.json"),
            "--tier",
            str(FIXTURE_ROOT / "tier_artifact.json"),
            "--max-calls",
            "4",
            "--raw-store-root",
            str(destinations["raw_store_root"]),
            "--dataset-root",
            str(destinations["dataset_root"]),
            "--receipt-path",
            str(destinations["receipt_path"]),
            "--universe-manifest",
            str(FIXTURE_ROOT / "universe_manifest.json"),
            "--mode",
            "probe",
        ],
        stdout=io.StringIO(),
        stderr=io.StringIO(),
        now=NOW,
        transport=replay,
        approval_clock=lambda: NOW,
    )

    assert code == PRECONDITION_EXIT
    assert replay.calls == []
    with fmp_usage_postgres.connect() as connection:
        assert (
            connection.scalar(
                select(func.count())
                .select_from(collection_runs)
                .where(collection_runs.c.run_id == "wrong-run")
            )
            == 0
        )


@pytest.mark.parametrize(
    "identity",
    [
        "synth-run-identity-0001",
        "fmp-run-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "fmp-run-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/../usage",
        "postgresql://synthetic.invalid/database",
        "AAS_DATABASE_URL=synthetic",
        " fmp-run-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ],
)
def test_pending_approval_identity_validation_is_strict(identity: str) -> None:
    approval = replace(
        load_approval_artifact(FIXTURE_ROOT / "owner_approval.json"),
        run_identity=identity,
    )
    with pytest.raises(PreconditionError, match="canonical fmp-run UUID"):
        fmp_approval_module.approved_candidate_run_id(approval)


def test_no_scheduler_or_purge_surface_exists() -> None:
    names = dir(fmp_collector_cli_module)
    for forbidden in ("schedule", "cron", "daemon", "purge", "tombstone"):
        assert not any(forbidden in name.casefold() for name in names)
    assert not (Path(__file__).resolve().parents[2] / "scripts" / "purge_fmp_data.py").exists()
