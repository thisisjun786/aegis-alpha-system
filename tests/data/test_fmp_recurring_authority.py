from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunPlan,
    CollectionUsageRecord,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.usage_checkpoint import UsageRecordLeaf
from aegis_alpha.collection.usage_checkpoint_crypto import (
    Ed25519PublicKeyring,
    TrustedEd25519PublicKey,
)
from aegis_alpha.data import fmp_daily_authority, fmp_recurring_runtime
from aegis_alpha.data.fmp_cli_artifacts import FmpPolicy, NotificationArtifact, PreconditionError
from aegis_alpha.data.fmp_collector import CollectorLock
from aegis_alpha.data.fmp_collector_command import _refresh_authorization_under_lock
from aegis_alpha.data.fmp_live_authorization import LiveAuthorization, _resume_scope
from aegis_alpha.data.fmp_orphan_usage import load_durable_attempt_usage
from aegis_alpha.data.fmp_owner_approval import OwnerApprovalAuthority
from aegis_alpha.data.fmp_rate_limit import BudgetExhaustedError, RateLimiter, parse_tier_artifact
from aegis_alpha.data.fmp_rate_types import TierArtifact, TrustedUsageSnapshot
from aegis_alpha.data.fmp_recurring_approval import BoundRecurringApproval
from aegis_alpha.data.fmp_recurring_audit import publish_recurring_audit
from aegis_alpha.data.fmp_recurring_authority import (
    MAX_PROVIDER_CALLS_PER_MINUTE,
    RECURRING_AUTHORITY_CONTRACT,
    RECURRING_AUTHORITY_VERSION,
    VerifiedRecurringAuthority,
    verify_recurring_authority,
)
from aegis_alpha.data.fmp_recurring_authorization import require_service_day
from aegis_alpha.data.fmp_recurring_errors import RecurringAuthorityError
from aegis_alpha.data.fmp_recurring_revocation import publish_recurring_revocation
from aegis_alpha.data.fmp_recurring_runs import RecurringRunSelection
from aegis_alpha.data.fmp_recurring_runtime import (
    RecurringOperation,
    authorize_recurring_operation,
    build_recurring_authorization,
)
from aegis_alpha.data.fmp_usage_trust import (
    UsageExtensionContext,
    _window_and_durable_extension,
    extend_trusted_fmp_usage_snapshot,
)
from aegis_alpha.data.serialization import canonical_json_bytes

if TYPE_CHECKING:
    from sqlalchemy import Engine

NOW = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
AUTHORITY_ID = "synthetic-owner-authority"
KEY_ID = "ed25519:synthetic-2026-08"
CALLS_PER_DAY = 5
SYNTHETIC_TIER_PATH = (
    Path(__file__).parent
    / "../fixtures/provider_neutral/fmp_collector/tier_synthetic_unlimited_daily.json"
)
SYNTHETIC_TIER_SHA256 = "45bce99b704017def9b8e00070cfee6034e5622159dc28683bfdb5713e8bc644"
SYNTHETIC_TIER_BANDWIDTH_GB_30D = 7
SYNTHETIC_TIER_CALLS_PER_MINUTE = 73


def _document(**changes: object) -> dict[str, object]:
    document: dict[str, object] = {
        "contract": RECURRING_AUTHORITY_CONTRACT,
        "version": RECURRING_AUTHORITY_VERSION,
        "authority_id": AUTHORITY_ID,
        "key_id": KEY_ID,
        "approver": "synthetic-owner",
        "policy_id": "fmp-operational-candidate-v1",
        "schedule_id": "fmp-daily-refresh-v1",
        "cadence": "daily",
        "dataset_selection": "all",
        "raw_store_root": "/data/aegis-alpha-system/raw",
        "dataset_root": "/data/aegis-alpha-system/normalized",
        "output_root": "/data/aegis-alpha-system/owner-receipts/fmp-daily",
        "norgate_security_master": (
            "/data/aegis-alpha-system/normalized/norgate/"
            "2026-07-29-us-platinum/full-v1/security_master.parquet"
        ),
        "norgate_security_master_sha256": "d" * 64,
        "norgate_security_master_row_count": 35603,
        "norgate_snapshot_date": "2026-07-28",
        "backfill_from": "2026-07-29",
        "calls_per_minute": 3000,
        "calls_per_day": CALLS_PER_DAY,
        "registry_sha256": "e" * 64,
        "notification_sha256": "f" * 64,
        "tier_sha256": "2" * 64,
        "issued_at_utc": "2026-08-21T11:59:00.000000Z",
        "valid_from_utc": "2026-08-21T12:00:00.000000Z",
    }
    document.update(changes)
    return document


def _authority(private_key: Ed25519PrivateKey) -> OwnerApprovalAuthority:
    valid_from = NOW - timedelta(days=1)
    valid_until = NOW + timedelta(days=370)
    return OwnerApprovalAuthority(
        authority_id=AUTHORITY_ID,
        key_id=KEY_ID,
        valid_from_utc=valid_from,
        valid_until_utc=valid_until,
        artifact_sha256="c" * 64,
        keyring=Ed25519PublicKeyring(
            {
                (AUTHORITY_ID, KEY_ID): TrustedEd25519PublicKey(
                    private_key.public_key().public_bytes_raw(),
                    valid_from_utc=valid_from,
                    valid_until_utc=valid_until,
                )
            }
        ),
    )


def _verified() -> VerifiedRecurringAuthority:
    private_key = Ed25519PrivateKey.generate()
    payload = canonical_json_bytes(_document())
    return verify_recurring_authority(
        payload,
        private_key.sign(payload),
        _authority(private_key),
        now=NOW,
    )


def _synthetic_null_tier() -> TierArtifact:
    return parse_tier_artifact(json.loads(SYNTHETIC_TIER_PATH.read_bytes()))


def _null_tier_authorization(**authority_changes: object) -> LiveAuthorization:
    private_key = Ed25519PrivateKey.generate()
    payload = canonical_json_bytes(
        _document(
            calls_per_minute=SYNTHETIC_TIER_CALLS_PER_MINUTE,
            tier_sha256=SYNTHETIC_TIER_SHA256,
            **authority_changes,
        )
    )
    verified = verify_recurring_authority(
        payload,
        private_key.sign(payload),
        _authority(private_key),
        now=NOW,
    )
    return build_recurring_authorization(
        authority=verified,
        policy=FmpPolicy(
            policy_id="fmp-operational-candidate-v1",
            license_classification="PROPRIETARY_SUBSCRIPTION",
            scheduled_collection_allowed=True,
            sha256="e" * 64,
        ),
        notification=NotificationArtifact(
            notified_at_utc=NOW,
            channel="synthetic",
            storage_aliases=("external-data-root",),
            sha256="f" * 64,
        ),
        tier=_synthetic_null_tier(),
        tier_sha256=SYNTHETIC_TIER_SHA256,
        usage=TrustedUsageSnapshot(
            source="signed-checkpoint",
            recorded_at_utc=NOW.isoformat(),
            integrity_sha256="1" * 64,
            authority_verified=True,
            calls_used_today=0,
            bytes_used_30d=0,
        ),
        operation=RecurringOperation(
            command="build-universe",
            service_day=NOW.date(),
            output_path=verified.output_root / "2026-08-21/universe-manifest.json",
        ),
        manifest=None,
        manifest_sha256=None,
        approval_clock=lambda: NOW,
    )


def test_signed_daily_authority_allows_repeatable_runs_without_run_cap() -> None:
    private_key = Ed25519PrivateKey.generate()
    payload = canonical_json_bytes(_document())
    verified = verify_recurring_authority(
        payload,
        private_key.sign(payload),
        _authority(private_key),
        now=NOW,
    )

    assert verified.calls_per_minute == MAX_PROVIDER_CALLS_PER_MINUTE
    assert verified.calls_per_day == CALLS_PER_DAY
    assert verified.dataset_selection == "all"
    assert verified.run_identity("build-universe", date(2026, 8, 21)).startswith("fmp-run-")
    assert verified.run_identity("collect", date(2026, 8, 21)) != verified.run_identity(
        "build-universe", date(2026, 8, 21)
    )
    verified.require_request(NOW)


def test_standing_authority_expires_with_its_signing_key() -> None:
    private_key = Ed25519PrivateKey.generate()
    document = _document()
    payload = canonical_json_bytes(document)
    verified = verify_recurring_authority(
        payload,
        private_key.sign(payload),
        _authority(private_key),
        now=NOW,
    )

    key_expiry = NOW + timedelta(days=370)
    verified.require_request(key_expiry - timedelta(microseconds=1))
    with pytest.raises(RecurringAuthorityError, match="signing key is expired"):
        verified.require_request(key_expiry)


@pytest.mark.parametrize(
    ("field", "message"),
    [
        ("registry_sha256", "registry digest"),
        ("notification_sha256", "notification digest"),
    ],
)
def test_recurring_authority_binds_runtime_policy_artifacts(
    field: str,
    message: str,
) -> None:
    with pytest.raises(PreconditionError, match=message):
        _null_tier_authorization(**{field: "0" * 64})


def test_recurring_request_approval_survives_service_day_rollover() -> None:
    moments = [NOW]
    approval = BoundRecurringApproval(
        authority=_verified(),
        service_day=NOW.date(),
        clock=lambda: moments[0],
    )
    approval.require_request()

    moments[0] += timedelta(days=1)
    approval.require_request()


def test_standing_authority_revocation_blocks_future_requests(tmp_path: Path) -> None:
    verified = replace(_verified(), raw_store_root=tmp_path)

    destination = publish_recurring_revocation(verified, NOW)

    assert destination.is_file()
    with pytest.raises(RecurringAuthorityError, match="revoked"):
        verified.require_request(NOW + timedelta(seconds=1))


def test_revoked_authority_is_rejected_during_fresh_verification(tmp_path: Path) -> None:
    private_key = Ed25519PrivateKey.generate()
    payload = canonical_json_bytes(_document(raw_store_root=str(tmp_path)))
    signature = private_key.sign(payload)
    authority = _authority(private_key)
    verified = verify_recurring_authority(payload, signature, authority, now=NOW)
    publish_recurring_revocation(verified, NOW)

    with pytest.raises(RecurringAuthorityError, match="revoked"):
        verify_recurring_authority(
            payload,
            signature,
            authority,
            now=NOW + timedelta(seconds=1),
        )


def test_fresh_verification_loads_original_key_then_enforces_expiry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    private_key = Ed25519PrivateKey.generate()
    payload = canonical_json_bytes(_document())
    signature = private_key.sign(payload)
    trust = _authority(private_key)
    observed: list[datetime] = []
    monkeypatch.setattr(
        fmp_daily_authority,
        "load_recurring_authority",
        lambda *_args: (payload, signature),
    )

    def load_trust(_path: Path, moment: datetime) -> OwnerApprovalAuthority:
        observed.append(moment)
        return trust

    monkeypatch.setattr(fmp_daily_authority, "load_owner_approval_authority", load_trust)
    with pytest.raises(RecurringAuthorityError, match="signing key is expired"):
        fmp_daily_authority.verified_daily_authority(
            argparse.Namespace(
                recurring_authority=tmp_path / "authority.json",
                recurring_authority_signature=tmp_path / "authority.sig",
            ),
            {"AAS_FMP_OWNER_APPROVAL_AUTHORITY_PATH": str(tmp_path / "owner.json")},
            NOW + timedelta(days=3650),
        )

    assert observed == [NOW - timedelta(minutes=1)]


def test_runtime_authorization_uses_original_signing_key_window(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    payload = canonical_json_bytes(_document())
    observed: list[datetime] = []

    class StopAfterTrustLoadError(Exception):
        pass

    monkeypatch.setattr(
        fmp_recurring_runtime,
        "load_recurring_authority",
        lambda *_args: (payload, b"synthetic-signature"),
    )

    def stop_after_trust_load(_path: Path, moment: datetime) -> OwnerApprovalAuthority:
        observed.append(moment)
        raise StopAfterTrustLoadError

    monkeypatch.setattr(
        fmp_recurring_runtime,
        "load_owner_approval_authority",
        stop_after_trust_load,
    )
    future = NOW + timedelta(days=3650)
    with pytest.raises(StopAfterTrustLoadError):
        authorize_recurring_operation(
            operation=RecurringOperation(
                command="build-universe",
                service_day=future.date(),
                output_path=tmp_path / "universe.json",
            ),
            recurring_authority_path=tmp_path / "authority.json",
            recurring_signature_path=tmp_path / "authority.sig",
            registry_path=tmp_path / "registry.json",
            notification_path=tmp_path / "notification.json",
            tier_path=tmp_path / "tier.json",
            environment={"AAS_FMP_OWNER_APPROVAL_AUTHORITY_PATH": str(tmp_path / "owner.json")},
            moment=future,
            approval_clock=lambda: future,
        )

    assert observed == [NOW - timedelta(minutes=1)]


def test_recurring_runtime_requires_scheduled_policy_and_has_no_run_cap(
    tmp_path: Path,
) -> None:
    verified = _verified()
    assert hasattr(verified, "output_root")
    policy = FmpPolicy(
        policy_id="fmp-operational-candidate-v1",
        license_classification="PROPRIETARY_SUBSCRIPTION",
        scheduled_collection_allowed=True,
        sha256="e" * 64,
    )
    notification = NotificationArtifact(
        notified_at_utc=NOW,
        channel="synthetic",
        storage_aliases=("external-data-root",),
        sha256="f" * 64,
    )
    usage = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=0,
        bytes_used_30d=0,
    )
    authorization = build_recurring_authorization(
        authority=verified,
        policy=policy,
        notification=notification,
        tier=TierArtifact(
            calls_per_minute=MAX_PROVIDER_CALLS_PER_MINUTE,
            calls_per_day=CALLS_PER_DAY,
            bandwidth_gb_30d=150,
        ),
        tier_sha256="2" * 64,
        usage=usage,
        operation=RecurringOperation(
            command="build-universe",
            service_day=NOW.date(),
            output_path=verified.output_root / "2026-08-21/universe-manifest.json",
        ),
        manifest=None,
        manifest_sha256=None,
        approval_clock=lambda: NOW,
    )

    assert authorization.max_calls is None
    assert authorization.operator_from == date(2026, 7, 29)
    refreshed = replace(
        authorization,
        run_id="fmp-run-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )
    provisional = replace(
        authorization,
        refresh_under_lock=lambda: refreshed,
    )
    with CollectorLock(tmp_path, run_identity=authorization.run_id) as lock:
        finalized = _refresh_authorization_under_lock(provisional, lock)
        lock_payload = json.loads(lock.owned_payload or b"{}")
    assert finalized.run_id == refreshed.run_id
    assert finalized.refresh_under_lock is provisional.refresh_under_lock
    assert lock_payload["run_identity"] == refreshed.run_id
    publish_recurring_audit(
        authority=verified,
        authorization=replace(authorization, raw_store_root=tmp_path),
        attempt_index=0,
        service_day=NOW.date(),
        now=NOW + timedelta(days=1),
    )
    assert (tmp_path / "fmp" / "runs" / authorization.run_id).is_dir()
    with pytest.raises(PreconditionError, match="tier digest"):
        build_recurring_authorization(
            authority=verified,
            policy=policy,
            notification=notification,
            tier=authorization.tier,
            tier_sha256="3" * 64,
            usage=usage,
            operation=RecurringOperation(
                command="build-universe",
                service_day=NOW.date(),
                output_path=authorization.output_path,
            ),
            manifest=None,
            manifest_sha256=None,
            approval_clock=lambda: NOW,
        )
    with pytest.raises(PreconditionError, match="daily plan"):
        build_recurring_authorization(
            authority=verified,
            policy=policy,
            notification=notification,
            tier=authorization.tier,
            tier_sha256="2" * 64,
            usage=usage,
            operation=RecurringOperation(
                command="build-universe",
                service_day=NOW.date(),
                output_path=verified.output_root / "other.json",
            ),
            manifest=None,
            manifest_sha256=None,
            approval_clock=lambda: NOW,
        )
    with pytest.raises(PreconditionError, match="scheduled"):
        build_recurring_authorization(
            authority=verified,
            policy=FmpPolicy(
                policy_id=policy.policy_id,
                license_classification=policy.license_classification,
                scheduled_collection_allowed=False,
                sha256=policy.sha256,
            ),
            notification=notification,
            tier=authorization.tier,
            tier_sha256="2" * 64,
            usage=usage,
            operation=RecurringOperation(
                command="build-universe",
                service_day=NOW.date(),
                output_path=authorization.output_path,
            ),
            manifest=None,
            manifest_sha256=None,
            approval_clock=lambda: NOW,
        )


def test_recurring_authorization_revalidates_service_day_with_live_clock() -> None:
    verified = _verified()
    policy = FmpPolicy(
        policy_id="fmp-operational-candidate-v1",
        license_classification="PROPRIETARY_SUBSCRIPTION",
        scheduled_collection_allowed=True,
        sha256="e" * 64,
    )
    notification = NotificationArtifact(
        notified_at_utc=NOW,
        channel="synthetic",
        storage_aliases=("external-data-root",),
        sha256="f" * 64,
    )
    tier = TierArtifact(
        calls_per_minute=MAX_PROVIDER_CALLS_PER_MINUTE,
        calls_per_day=CALLS_PER_DAY,
        bandwidth_gb_30d=150,
    )
    usage = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=0,
        bytes_used_30d=0,
    )
    operation = RecurringOperation(
        command="build-universe",
        service_day=NOW.date(),
        output_path=verified.output_root / "2026-08-21/universe-manifest.json",
    )
    with pytest.raises(PreconditionError, match="service day"):
        build_recurring_authorization(
            authority=verified,
            policy=policy,
            notification=notification,
            tier=tier,
            tier_sha256="2" * 64,
            usage=usage,
            operation=operation,
            manifest=None,
            manifest_sha256=None,
            approval_clock=lambda: NOW + timedelta(days=2),
        )
    recovery = build_recurring_authorization(
        authority=verified,
        policy=policy,
        notification=notification,
        tier=tier,
        tier_sha256="2" * 64,
        usage=usage,
        operation=operation,
        manifest=None,
        manifest_sha256=None,
        approval_clock=lambda: NOW + timedelta(days=2),
        allow_historical_service_day=True,
    )
    recovery.approval.require_request()


def test_synthetic_tier_artifact_keeps_null_daily_budget() -> None:
    # Given the synthetic tier artifact pinned by the test authority
    payload = SYNTHETIC_TIER_PATH.read_bytes()

    # When the artifact is hashed and parsed
    tier = parse_tier_artifact(json.loads(payload))

    # Then the null daily budget remains distinct from its signed overlay
    assert hashlib.sha256(payload).hexdigest() == SYNTHETIC_TIER_SHA256
    assert tier.calls_per_minute == SYNTHETIC_TIER_CALLS_PER_MINUTE
    assert tier.calls_per_day is None
    assert tier.bandwidth_gb_30d == SYNTHETIC_TIER_BANDWIDTH_GB_30D


def test_null_vendor_tier_admits_with_signed_daily_budget_overlay() -> None:
    # Given a signed positive daily budget over a synthetic null daily budget
    private_key = Ed25519PrivateKey.generate()
    payload = canonical_json_bytes(
        _document(
            calls_per_minute=SYNTHETIC_TIER_CALLS_PER_MINUTE, tier_sha256=SYNTHETIC_TIER_SHA256
        )
    )
    verified = verify_recurring_authority(
        payload,
        private_key.sign(payload),
        _authority(private_key),
        now=NOW,
    )
    tier = _synthetic_null_tier()

    # When the recurring authorization is built
    authorization = build_recurring_authorization(
        authority=verified,
        policy=FmpPolicy(
            policy_id="fmp-operational-candidate-v1",
            license_classification="PROPRIETARY_SUBSCRIPTION",
            scheduled_collection_allowed=True,
            sha256="e" * 64,
        ),
        notification=NotificationArtifact(
            notified_at_utc=NOW,
            channel="synthetic",
            storage_aliases=("external-data-root",),
            sha256="f" * 64,
        ),
        tier=tier,
        tier_sha256=SYNTHETIC_TIER_SHA256,
        usage=TrustedUsageSnapshot(
            source="signed-checkpoint",
            recorded_at_utc=NOW.isoformat(),
            integrity_sha256="1" * 64,
            authority_verified=True,
            calls_used_today=0,
            bytes_used_30d=0,
        ),
        operation=RecurringOperation(
            command="build-universe",
            service_day=NOW.date(),
            output_path=verified.output_root / "2026-08-21/universe-manifest.json",
        ),
        manifest=None,
        manifest_sha256=None,
        approval_clock=lambda: NOW,
    )

    # Then the signed budget overlays only the limiter view of the tier
    assert tier.calls_per_day is None
    assert authorization.tier.calls_per_day == verified.calls_per_day
    assert authorization.tier.calls_per_minute == SYNTHETIC_TIER_CALLS_PER_MINUTE
    assert authorization.tier.bandwidth_gb_30d == SYNTHETIC_TIER_BANDWIDTH_GB_30D
    assert authorization.max_calls is None
    assert authorization.artifact_hashes["tier"] == SYNTHETIC_TIER_SHA256


def test_null_vendor_tier_overlay_enforces_exact_baseline_exhaustion() -> None:
    # Given an overlaid authorization and a trusted baseline at the signed budget
    authorization = _null_tier_authorization()
    limiter = RateLimiter(
        tier=authorization.tier,
        max_calls=authorization.max_calls,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
        run_seed=1,
        usage_baseline=TrustedUsageSnapshot(
            source="signed-checkpoint",
            recorded_at_utc=NOW.isoformat(),
            integrity_sha256="1" * 64,
            authority_verified=True,
            calls_used_today=CALLS_PER_DAY,
            bytes_used_30d=0,
        ),
    )

    # When the next provider call is requested, Then it is refused before transport
    with pytest.raises(BudgetExhaustedError, match="daily cap"):
        limiter.before_request()
    assert limiter.calls_attempted == 0


def test_null_vendor_tier_overlay_blocks_after_consuming_the_final_signed_slot() -> None:
    # Given an overlaid authorization one slot below the signed daily budget
    authorization = _null_tier_authorization()
    limiter = RateLimiter(
        tier=authorization.tier,
        max_calls=authorization.max_calls,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
        run_seed=1,
        usage_baseline=TrustedUsageSnapshot(
            source="signed-checkpoint",
            recorded_at_utc=NOW.isoformat(),
            integrity_sha256="1" * 64,
            authority_verified=True,
            calls_used_today=CALLS_PER_DAY - 1,
            bytes_used_30d=0,
        ),
    )

    # When the final signed slot is consumed
    limiter.before_request()
    assert limiter.calls_attempted == 1

    # Then the following provider call is refused before transport
    with pytest.raises(BudgetExhaustedError, match="daily cap"):
        limiter.before_request()
    assert limiter.calls_attempted == 1


@pytest.mark.parametrize("tier_calls_per_day", [CALLS_PER_DAY - 1, CALLS_PER_DAY + 1])
def test_recurring_authorization_rejects_a_tier_daily_budget_mismatch(
    tier_calls_per_day: int,
) -> None:
    verified = _verified()
    policy = FmpPolicy(
        policy_id="fmp-operational-candidate-v1",
        license_classification="PROPRIETARY_SUBSCRIPTION",
        scheduled_collection_allowed=True,
        sha256="e" * 64,
    )
    notification = NotificationArtifact(
        notified_at_utc=NOW,
        channel="synthetic",
        storage_aliases=("external-data-root",),
        sha256="f" * 64,
    )
    usage = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=0,
        bytes_used_30d=0,
    )

    with pytest.raises(PreconditionError, match="daily budget"):
        build_recurring_authorization(
            authority=verified,
            policy=policy,
            notification=notification,
            tier=TierArtifact(
                calls_per_minute=MAX_PROVIDER_CALLS_PER_MINUTE,
                calls_per_day=tier_calls_per_day,
                bandwidth_gb_30d=150,
            ),
            tier_sha256="2" * 64,
            usage=usage,
            operation=RecurringOperation(
                command="build-universe",
                service_day=NOW.date(),
                output_path=verified.output_root / "2026-08-21/universe-manifest.json",
            ),
            manifest=None,
            manifest_sha256=None,
            approval_clock=lambda: NOW,
        )


def test_recurring_audit_clock_is_read_after_usage_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    verified = _verified()
    usage = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=0,
        bytes_used_30d=0,
    )
    current = [NOW]
    published_at: list[datetime] = []
    reconciliation_contexts: list[UsageExtensionContext] = []
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "load_owner_approval_authority",
        lambda *_arguments: object(),
    )
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "load_recurring_authority",
        lambda *_arguments: (canonical_json_bytes(_document()), b"signature"),
    )
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "verify_recurring_authority",
        lambda *_arguments, **_keywords: verified,
    )
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "load_fmp_policy",
        lambda *_arguments: FmpPolicy(
            policy_id="fmp-operational-candidate-v1",
            license_classification="PROPRIETARY_SUBSCRIPTION",
            scheduled_collection_allowed=True,
            sha256="e" * 64,
        ),
    )
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "load_notification_artifact",
        lambda *_arguments: NotificationArtifact(
            notified_at_utc=NOW,
            channel="synthetic",
            storage_aliases=("external-data-root",),
            sha256="f" * 64,
        ),
    )
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "_read_json",
        lambda *_arguments: (
            {
                "calls_per_minute": MAX_PROVIDER_CALLS_PER_MINUTE,
                "calls_per_day": CALLS_PER_DAY,
                "bandwidth_gb_30d": 150,
            },
            "2" * 64,
        ),
    )
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "load_trusted_fmp_usage_snapshot",
        lambda **_keywords: usage,
    )
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "select_recurring_run_from_database",
        lambda **_keywords: RecurringRunSelection(
            run_id=verified.run_identity("build-universe", NOW.date()),
            attempt_index=0,
        ),
    )

    def reconcile(
        _snapshot: TrustedUsageSnapshot,
        **keywords: object,
    ) -> TrustedUsageSnapshot:
        reconciliation_contexts.append(cast("UsageExtensionContext", keywords["context"]))
        current[0] = NOW + timedelta(hours=1)
        return usage

    monkeypatch.setattr(
        fmp_recurring_runtime,
        "extend_trusted_fmp_usage_snapshot",
        reconcile,
    )
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "publish_recurring_audit",
        lambda **keywords: published_at.append(cast("datetime", keywords["now"])),
    )
    operation = RecurringOperation(
        command="build-universe",
        service_day=NOW.date(),
        output_path=verified.output_root / "2026-08-21/universe-manifest.json",
    )
    authorization = authorize_recurring_operation(
        operation=operation,
        recurring_authority_path=Path("/synthetic/recurring.json"),
        recurring_signature_path=Path("/synthetic/recurring.sig"),
        registry_path=Path("/synthetic/registry.json"),
        notification_path=Path("/synthetic/notification.json"),
        tier_path=Path("/synthetic/tier.json"),
        environment={
            "AAS_FMP_OWNER_APPROVAL_AUTHORITY_PATH": "/synthetic/owner.json",
            "AAS_DATABASE_URL": "postgresql+psycopg://synthetic",
        },
        moment=NOW,
        approval_clock=lambda: current[0],
    )

    assert authorization.refresh_under_lock is not None
    authorization.refresh_under_lock()

    assert published_at == [NOW + timedelta(hours=1)]
    assert reconciliation_contexts[0].exclude_run_ids == ()


def test_recurring_attempt_usage_extends_the_signed_budget(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    authority = _verified()
    prior_run_id = authority.run_identity("build-universe", NOW.date(), attempt_index=0)
    current_run_id = authority.run_identity("build-universe", NOW.date(), attempt_index=1)
    registry = CollectionRegistry(clean_postgres)
    for attempt_index, run_id, quantity in (
        (0, prior_run_id, Decimal(1)),
        (1, current_run_id, Decimal(7)),
    ):
        plan = CollectionRunPlan(
            plan_id=f"plan-{run_id}",
            schema_version=1,
            provider="fmp",
            dataset="fmp_universe",
            mode=CollectionMode.INCREMENTAL,
            requested_window_start=None,
            requested_window_end=None,
            parameters={"attempt_index": attempt_index},
            created_at_utc=NOW,
        )
        registry.register_plan(plan)
        registry.start_run(CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=NOW))
        for usage_seq, metric, unit in (
            (1, "calls_attempted", "call"),
            (2, "bytes_received", "byte"),
        ):
            registry.record_usage(
                CollectionUsageRecord(
                    run_id=run_id,
                    usage_seq=usage_seq,
                    metric=metric,
                    quantity=quantity,
                    unit=unit,
                    recorded_at_utc=NOW + timedelta(seconds=1),
                    evidence={"source": "recurring attempt"},
                )
            )

    tier = TierArtifact(
        calls_per_minute=MAX_PROVIDER_CALLS_PER_MINUTE,
        calls_per_day=None,
        bandwidth_gb_30d=150,
    )
    cutoff = tier.bandwidth_cutoff_bytes
    assert cutoff is not None
    baseline_run_id = "fmp-run-signed-baseline"
    baseline_plan = CollectionRunPlan(
        plan_id=f"plan-{baseline_run_id}",
        schema_version=1,
        provider="fmp",
        dataset="fmp_universe",
        mode=CollectionMode.INCREMENTAL,
        requested_window_start=None,
        requested_window_end=None,
        parameters={"source": "signed baseline"},
        created_at_utc=NOW - timedelta(seconds=1),
    )
    registry.register_plan(baseline_plan)
    registry.start_run(
        CollectionRun(
            run_id=baseline_run_id,
            plan_id=baseline_plan.plan_id,
            created_at_utc=baseline_plan.created_at_utc,
        )
    )
    for usage_seq, metric, quantity, unit in (
        (1, "calls_attempted", Decimal(4), "call"),
        (2, "bytes_received", Decimal(cutoff - 1), "byte"),
    ):
        registry.record_usage(
            CollectionUsageRecord(
                run_id=baseline_run_id,
                usage_seq=usage_seq,
                metric=metric,
                quantity=quantity,
                unit=unit,
                recorded_at_utc=NOW - timedelta(microseconds=1),
                evidence={"source": "signed baseline"},
            )
        )
    baseline = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=4,
        bytes_used_30d=cutoff - 1,
    )
    extended = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            raw_store_root=tmp_path,
            exclude_run_ids=(current_run_id,),
        ),
        coverage_end_utc=NOW,
        now=NOW + timedelta(seconds=2),
    )

    assert extended.calls_used_today == baseline.calls_used_today + 1
    assert extended.bytes_used_30d == cutoff
    limiter = RateLimiter(
        tier=tier,
        max_calls=None,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
        run_seed=1,
        usage_baseline=extended,
    )
    with pytest.raises(BudgetExhaustedError, match="bandwidth"):
        limiter.before_request()


def test_signed_daily_budget_fails_closed_at_exhaustion(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    authority = _verified()
    checkpoint_calls = authority.calls_per_day - 1
    registry = CollectionRegistry(clean_postgres)
    for run_id, calls, recorded_at in (
        ("fmp-run-signed-daily-baseline", checkpoint_calls, NOW - timedelta(microseconds=1)),
        ("fmp-run-daily-budget-spend", 1, NOW + timedelta(seconds=1)),
    ):
        plan = CollectionRunPlan(
            plan_id=f"plan-{run_id}",
            schema_version=1,
            provider="fmp",
            dataset="fmp_universe",
            mode=CollectionMode.INCREMENTAL,
            requested_window_start=None,
            requested_window_end=None,
            parameters={"source": "signed daily budget", "run_id": run_id},
            created_at_utc=NOW - timedelta(seconds=1),
        )
        registry.register_plan(plan)
        registry.start_run(
            CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=plan.created_at_utc)
        )
        registry.record_usage(
            CollectionUsageRecord(
                run_id=run_id,
                usage_seq=1,
                metric="calls_attempted",
                quantity=Decimal(calls),
                unit="call",
                recorded_at_utc=recorded_at,
                evidence={"source": "signed daily budget"},
            )
        )
    baseline = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=checkpoint_calls,
        bytes_used_30d=0,
    )

    extended = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            raw_store_root=tmp_path,
        ),
        coverage_end_utc=NOW,
        now=NOW + timedelta(seconds=2),
    )

    assert extended.calls_used_today == authority.calls_per_day
    limiter = RateLimiter(
        tier=TierArtifact(
            calls_per_minute=MAX_PROVIDER_CALLS_PER_MINUTE,
            calls_per_day=authority.calls_per_day,
            bandwidth_gb_30d=150,
        ),
        max_calls=None,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
        run_seed=1,
        usage_baseline=extended,
    )
    assert limiter.calls_remaining is None
    with pytest.raises(BudgetExhaustedError, match="daily cap"):
        limiter.before_request()


def test_usage_extension_preserves_positive_signed_floor_without_later_rows(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    baseline = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=7,
        bytes_used_30d=70,
    )

    extended = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            raw_store_root=tmp_path,
        ),
        coverage_end_utc=NOW,
        now=NOW + timedelta(seconds=1),
    )

    assert extended.calls_used_today == baseline.calls_used_today
    assert extended.bytes_used_30d == baseline.bytes_used_30d


def test_usage_extension_preserves_signed_floor_and_adds_only_post_checkpoint_rows(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    registry = CollectionRegistry(clean_postgres)
    for suffix, recorded_at, calls, received_bytes in (
        ("before", NOW - timedelta(microseconds=1), 11, 110),
        ("at", NOW, 13, 130),
        ("after", NOW + timedelta(microseconds=1), 2, 20),
    ):
        run_id = f"fmp-run-checkpoint-{suffix}"
        plan = CollectionRunPlan(
            plan_id=f"plan-{run_id}",
            schema_version=1,
            provider="fmp",
            dataset="fmp_universe",
            mode=CollectionMode.INCREMENTAL,
            requested_window_start=None,
            requested_window_end=None,
            parameters={"checkpoint_boundary": suffix},
            created_at_utc=recorded_at,
        )
        registry.register_plan(plan)
        registry.start_run(
            CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=recorded_at)
        )
        for usage_seq, metric, quantity, unit in (
            (1, "calls_attempted", Decimal(calls), "call"),
            (2, "bytes_received", Decimal(received_bytes), "byte"),
        ):
            registry.record_usage(
                CollectionUsageRecord(
                    run_id=run_id,
                    usage_seq=usage_seq,
                    metric=metric,
                    quantity=quantity,
                    unit=unit,
                    recorded_at_utc=recorded_at,
                    evidence={"checkpoint_boundary": suffix},
                )
            )
    baseline = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=7,
        bytes_used_30d=70,
    )

    extended = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            raw_store_root=tmp_path,
        ),
        coverage_end_utc=NOW,
        now=NOW + timedelta(seconds=1),
    )

    assert extended.calls_used_today == baseline.calls_used_today + 13 + 2
    assert extended.bytes_used_30d == baseline.bytes_used_30d + 130 + 20


def test_usage_extension_drops_prior_day_calls_and_keeps_current_day_rows(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    current_day_usage = NOW + timedelta(days=1)
    current_day_calls = 2
    run_id = "fmp-run-current-day-after-stale-checkpoint"
    registry = CollectionRegistry(clean_postgres)
    plan = CollectionRunPlan(
        plan_id=f"plan-{run_id}",
        schema_version=1,
        provider="fmp",
        dataset="fmp_universe",
        mode=CollectionMode.INCREMENTAL,
        requested_window_start=None,
        requested_window_end=None,
        parameters={"checkpoint_boundary": "utc-rollover"},
        created_at_utc=current_day_usage,
    )
    registry.register_plan(plan)
    registry.start_run(
        CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=current_day_usage)
    )
    registry.record_usage(
        CollectionUsageRecord(
            run_id=run_id,
            usage_seq=1,
            metric="calls_attempted",
            quantity=Decimal(current_day_calls),
            unit="call",
            recorded_at_utc=current_day_usage,
            evidence={"checkpoint_boundary": "utc-rollover"},
        )
    )
    baseline = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=7,
        bytes_used_30d=70,
    )

    extended = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            raw_store_root=tmp_path,
        ),
        coverage_end_utc=NOW,
        now=current_day_usage + timedelta(seconds=1),
    )

    assert extended.calls_used_today == current_day_calls
    assert extended.bytes_used_30d == baseline.bytes_used_30d


@pytest.mark.parametrize(
    "coverage_end",
    [NOW - timedelta(microseconds=1), NOW + timedelta(microseconds=1)],
)
def test_usage_extension_requires_exact_signed_checkpoint_coverage(
    clean_postgres: Engine,
    tmp_path: Path,
    coverage_end: datetime,
) -> None:
    baseline = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=7,
        bytes_used_30d=70,
    )

    with pytest.raises(ValueError, match="signed checkpoint"):
        extend_trusted_fmp_usage_snapshot(
            baseline,
            context=UsageExtensionContext(
                database_url=clean_postgres.url.render_as_string(hide_password=False),
                raw_store_root=tmp_path,
            ),
            coverage_end_utc=coverage_end,
            now=NOW + timedelta(seconds=1),
        )


def test_stale_checkpoint_retains_the_signed_bandwidth_floor(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    run_id = "fmp-run-aged-out-usage"
    recorded_at = NOW - timedelta(days=29)
    registry = CollectionRegistry(clean_postgres)
    plan = CollectionRunPlan(
        plan_id=f"plan-{run_id}",
        schema_version=1,
        provider="fmp",
        dataset="fmp_universe",
        mode=CollectionMode.INCREMENTAL,
        requested_window_start=None,
        requested_window_end=None,
        parameters={"window": "aged-out"},
        created_at_utc=recorded_at - timedelta(days=1),
    )
    registry.register_plan(plan)
    registry.start_run(
        CollectionRun(
            run_id=run_id,
            plan_id=plan.plan_id,
            created_at_utc=plan.created_at_utc,
        )
    )
    stale_bytes = Decimal(10)
    registry.record_usage(
        CollectionUsageRecord(
            run_id=run_id,
            usage_seq=1,
            metric="bytes_received",
            quantity=stale_bytes,
            unit="byte",
            recorded_at_utc=recorded_at,
            evidence={"source": "stale checkpoint window"},
        )
    )
    baseline = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=0,
        bytes_used_30d=int(stale_bytes),
    )

    extended = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            raw_store_root=tmp_path,
        ),
        coverage_end_utc=NOW,
        now=NOW + timedelta(days=2),
    )

    assert extended.bytes_used_30d == int(stale_bytes)


def test_fully_expired_checkpoint_drops_the_signed_bandwidth_floor(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    coverage_end = NOW - timedelta(days=31)
    baseline = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=coverage_end.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=0,
        bytes_used_30d=70,
    )

    extended = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            raw_store_root=tmp_path,
        ),
        coverage_end_utc=coverage_end,
        now=NOW,
    )

    assert extended.bytes_used_30d == 0


def test_orphan_attempt_ledger_extends_usage_without_terminal_database_rows(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    run_id = "fmp-run-orphaned-attempt"
    orphan_body = b"12345"
    attempts_root = tmp_path / "fmp" / "runs" / run_id / "attempts"
    attempts_root.mkdir(parents=True)
    previous = "0" * 64
    marker_digest = ""
    for sequence in (1, 2):
        attempt = {
            "attempt_seq": sequence,
            "attempted_at_utc": NOW + timedelta(seconds=sequence),
            "content_sha256": hashlib.sha256(orphan_body).hexdigest(),
            "next_request_not_before_utc": NOW + timedelta(seconds=sequence + 1),
            "outcome": "response",
            "previous_attempt_sha256": previous,
            "raw_byte_length": len(orphan_body),
            "request_attempt_index": sequence,
            "request_fingerprint": f"sha256:{'a' * 64}",
            "run_identity": run_id,
            "status_code": 200,
        }
        payload = canonical_json_bytes(attempt)
        (attempts_root / f"{sequence:08d}.json").write_bytes(payload)
        previous = hashlib.sha256(payload).hexdigest()
        if sequence == 1:
            marker_digest = previous
    (attempts_root.parent / "latest-attempt.json").write_bytes(payload)
    (attempts_root.parent / "publication.json").write_bytes(
        canonical_json_bytes(
            {
                "attempt_ledger_sha256": marker_digest,
                "run_id": run_id,
                "usage": {
                    "calls_attempted": 1,
                    "bytes_received": len(orphan_body),
                },
            }
        )
    )
    baseline = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=0,
        bytes_used_30d=0,
    )

    extended = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            raw_store_root=tmp_path,
        ),
        coverage_end_utc=NOW,
        now=NOW + timedelta(seconds=4),
    )

    assert extended.calls_used_today == baseline.calls_used_today + 2
    assert extended.bytes_used_30d == baseline.bytes_used_30d + 2 * len(orphan_body)

    registry = CollectionRegistry(clean_postgres)
    plan = CollectionRunPlan(
        plan_id=f"plan-{run_id}",
        schema_version=1,
        provider="fmp",
        dataset="fmp_universe",
        mode=CollectionMode.INCREMENTAL,
        requested_window_start=None,
        requested_window_end=None,
        parameters={"orphan_reconciled": True},
        created_at_utc=NOW,
    )
    registry.register_plan(plan)
    registry.start_run(CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=NOW))
    for usage_seq, metric, quantity, unit in (
        (1, "calls_attempted", Decimal(1), "call"),
        (2, "bytes_received", Decimal(5), "byte"),
    ):
        registry.record_usage(
            CollectionUsageRecord(
                run_id=run_id,
                usage_seq=usage_seq,
                metric=metric,
                quantity=quantity,
                unit=unit,
                recorded_at_utc=NOW + timedelta(seconds=2),
                evidence={"source": "terminal reconciliation"},
            )
        )

    recorded = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            raw_store_root=tmp_path,
        ),
        coverage_end_utc=NOW,
        now=NOW + timedelta(seconds=4),
    )

    assert recorded.calls_used_today == extended.calls_used_today
    assert recorded.bytes_used_30d == extended.bytes_used_30d

    for usage_seq, metric, quantity, unit in (
        (3, "calls_attempted", Decimal(1), "call"),
        (4, "bytes_received", Decimal(5), "byte"),
    ):
        registry.record_usage(
            CollectionUsageRecord(
                run_id=run_id,
                usage_seq=usage_seq,
                metric=metric,
                quantity=quantity,
                unit=unit,
                recorded_at_utc=NOW + timedelta(seconds=3),
                evidence={"source": "complete terminal reconciliation"},
            )
        )

    fully_recorded = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            raw_store_root=tmp_path,
        ),
        coverage_end_utc=NOW,
        now=NOW + timedelta(seconds=4),
    )

    assert fully_recorded.calls_used_today == extended.calls_used_today
    assert fully_recorded.bytes_used_30d == extended.bytes_used_30d


def test_precheckpoint_orphan_attempt_extends_the_signed_floor(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    run_id = "fmp-run-precheckpoint-orphan"
    orphan_body = b"12345"
    attempts_root = tmp_path / "fmp" / "runs" / run_id / "attempts"
    attempts_root.mkdir(parents=True)
    attempt = {
        "attempt_seq": 1,
        "attempted_at_utc": NOW - timedelta(seconds=1),
        "content_sha256": hashlib.sha256(orphan_body).hexdigest(),
        "next_request_not_before_utc": NOW,
        "outcome": "response",
        "previous_attempt_sha256": "0" * 64,
        "raw_byte_length": len(orphan_body),
        "request_attempt_index": 1,
        "request_fingerprint": f"sha256:{'a' * 64}",
        "run_identity": run_id,
        "status_code": 200,
    }
    payload = canonical_json_bytes(attempt)
    (attempts_root / "00000001.json").write_bytes(payload)
    (attempts_root.parent / "latest-attempt.json").write_bytes(payload)
    (attempts_root.parent / "publication.json").write_bytes(
        canonical_json_bytes(
            {
                "run_id": run_id,
                "usage": {
                    "calls_attempted": 1,
                    "bytes_received": len(orphan_body),
                },
            }
        )
    )
    baseline = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=NOW.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=7,
        bytes_used_30d=70,
    )

    extended = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            raw_store_root=tmp_path,
        ),
        coverage_end_utc=NOW,
        now=NOW + timedelta(seconds=1),
    )

    assert extended.calls_used_today == baseline.calls_used_today + 1
    assert extended.bytes_used_30d == baseline.bytes_used_30d + len(orphan_body)

    registry = CollectionRegistry(clean_postgres)
    plan = CollectionRunPlan(
        plan_id=f"plan-{run_id}",
        schema_version=1,
        provider="fmp",
        dataset="fmp_universe",
        mode=CollectionMode.INCREMENTAL,
        requested_window_start=None,
        requested_window_end=None,
        parameters={"checkpoint_reconciliation": True},
        created_at_utc=NOW - timedelta(seconds=2),
    )
    registry.register_plan(plan)
    registry.start_run(
        CollectionRun(
            run_id=run_id,
            plan_id=plan.plan_id,
            created_at_utc=plan.created_at_utc,
        )
    )
    for usage_seq, metric, quantity, unit in (
        (1, "calls_attempted", Decimal(1), "call"),
        (2, "bytes_received", Decimal(len(orphan_body)), "byte"),
    ):
        registry.record_usage(
            CollectionUsageRecord(
                run_id=run_id,
                usage_seq=usage_seq,
                metric=metric,
                quantity=quantity,
                unit=unit,
                recorded_at_utc=NOW - timedelta(microseconds=1),
                evidence={"source": "signed checkpoint reconciliation"},
            )
        )

    represented = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=clean_postgres.url.render_as_string(hide_password=False),
            raw_store_root=tmp_path,
        ),
        coverage_end_utc=NOW,
        now=NOW + timedelta(seconds=1),
    )

    assert represented.calls_used_today == baseline.calls_used_today
    assert represented.bytes_used_30d == baseline.bytes_used_30d


def test_orphan_usage_skips_attempt_chains_outside_live_windows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "fmp-run-old-orphan"
    attempts_root = tmp_path / "fmp" / "runs" / run_id / "attempts"
    attempts_root.mkdir(parents=True)
    previous = "0" * 64
    paths = []
    for sequence, age_days in ((1, 32), (2, 31)):
        record = {
            "attempt_seq": sequence,
            "attempted_at_utc": NOW - timedelta(days=age_days),
            "previous_attempt_sha256": previous,
            "raw_byte_length": 1,
            "run_identity": run_id,
        }
        payload = canonical_json_bytes(record)
        path = attempts_root / f"{sequence:08d}.json"
        path.write_bytes(payload)
        paths.append(path)
        previous = hashlib.sha256(payload).hexdigest()
    (attempts_root.parent / "latest-attempt.json").write_bytes(payload)
    original_read_bytes = Path.read_bytes

    def guarded_read_bytes(path: Path) -> bytes:
        if path == paths[0]:
            raise AssertionError("irrelevant attempt chain must not be replayed")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)

    evidence = load_durable_attempt_usage(tmp_path, now=NOW)

    assert evidence.runs == ()
    assert evidence.attempt_count == 0


def test_recurring_resume_scope_accepts_recurring_contract_keys(tmp_path: Path) -> None:
    run_id = "fmp-run-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    scope_sha256 = "b" * 64
    plan = tmp_path / "fmp" / "plans" / f"plan-{run_id}.json"
    plan.parent.mkdir(parents=True)
    plan.write_bytes(
        canonical_json_bytes(
            {
                "parameters": {
                    "artifact_hashes": {
                        "recurring_scope_contract": "aegis-alpha/fmp-recurring-scope/v1",
                        "recurring_scope_sha256": scope_sha256,
                    }
                }
            }
        )
    )

    _resume_scope(
        tmp_path,
        run_id,
        scope_sha256,
        scope_sha256_key="recurring_scope_sha256",
        scope_contract_key="recurring_scope_contract",
        expected_scope_contract="aegis-alpha/fmp-recurring-scope/v1",
    )


@pytest.mark.parametrize(
    "mutation",
    [
        {"cadence": "hourly"},
        {"dataset_selection": "probe"},
        {"calls_per_minute": 3001},
        {"calls_per_day": 0},
        {"calls_per_day": -1},
        {"calls_per_day": 1.5},
        {"calls_per_day": "5"},
        {"calls_per_day": True},
        {"calls_per_day": None},
        {"backfill_from": "2026-07-28"},
        {"policy_id": "other-policy"},
        {"version": 2},
    ],
)
def test_recurring_authority_rejects_non_daily_or_over_limit_scope(
    mutation: dict[str, object],
) -> None:
    private_key = Ed25519PrivateKey.generate()
    payload = canonical_json_bytes(_document(**mutation))
    with pytest.raises(RecurringAuthorityError):
        verify_recurring_authority(
            payload,
            private_key.sign(payload),
            _authority(private_key),
            now=NOW,
        )


def test_recurring_authority_requires_a_signed_daily_budget() -> None:
    private_key = Ed25519PrivateKey.generate()
    document = _document()
    del document["calls_per_day"]
    payload = canonical_json_bytes(document)

    with pytest.raises(RecurringAuthorityError, match="fields"):
        verify_recurring_authority(
            payload,
            private_key.sign(payload),
            _authority(private_key),
            now=NOW,
        )


def test_recurring_authority_rejects_tampering_and_premature_use() -> None:
    private_key = Ed25519PrivateKey.generate()
    payload = canonical_json_bytes(_document())
    signature = private_key.sign(payload)
    authority = _authority(private_key)

    with pytest.raises(RecurringAuthorityError):
        verify_recurring_authority(
            canonical_json_bytes(_document(output_root="/data/other")),
            signature,
            authority,
            now=NOW,
        )

    verified = verify_recurring_authority(payload, signature, authority, now=NOW)
    with pytest.raises(RecurringAuthorityError, match="not active"):
        verified.require_request(NOW - timedelta(seconds=1))


def test_terminal_usage_is_attributed_to_actual_attempt_utc_days(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    run_id = "fmp-run-three-day-backfill"
    day_one = datetime(2026, 8, 19, 22, 0, tzinfo=UTC)
    day_two = datetime(2026, 8, 20, 10, 0, tzinfo=UTC)
    day_three = datetime(2026, 8, 21, 1, 0, tzinfo=UTC)
    terminal_at = datetime(2026, 8, 21, 2, 0, tzinfo=UTC)
    attempts_root = tmp_path / "fmp" / "runs" / run_id / "attempts"
    attempts_root.mkdir(parents=True)
    previous = "0" * 64
    body = b"12345"
    for sequence, attempted_at in enumerate((day_one, day_two, day_three), start=1):
        attempt = {
            "attempt_seq": sequence,
            "attempted_at_utc": attempted_at,
            "content_sha256": hashlib.sha256(body).hexdigest(),
            "next_request_not_before_utc": attempted_at + timedelta(seconds=1),
            "outcome": "response",
            "previous_attempt_sha256": previous,
            "raw_byte_length": len(body),
            "request_attempt_index": sequence,
            "request_fingerprint": f"sha256:{'a' * 64}",
            "run_identity": run_id,
            "status_code": 200,
        }
        payload = canonical_json_bytes(attempt)
        (attempts_root / f"{sequence:08d}.json").write_bytes(payload)
        previous = hashlib.sha256(payload).hexdigest()
    (attempts_root.parent / "latest-attempt.json").write_bytes(payload)
    registry = CollectionRegistry(clean_postgres)
    plan = CollectionRunPlan(
        plan_id=f"plan-{run_id}",
        schema_version=1,
        provider="fmp",
        dataset="fmp_price_eod_full",
        mode=CollectionMode.BACKFILL,
        requested_window_start=None,
        requested_window_end=None,
        parameters={"span": "three-utc-days"},
        created_at_utc=day_one,
    )
    registry.register_plan(plan)
    registry.start_run(CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=day_one))
    for usage_seq, metric, quantity, unit in (
        (1, "calls_attempted", Decimal(3), "call"),
        (2, "bytes_received", Decimal(3 * len(body)), "byte"),
    ):
        registry.record_usage(
            CollectionUsageRecord(
                run_id=run_id,
                usage_seq=usage_seq,
                metric=metric,
                quantity=quantity,
                unit=unit,
                recorded_at_utc=terminal_at,
                evidence={"source": "terminal recovery"},
            )
        )
    checkpoint_end = datetime(2026, 8, 19, 0, 0, tzinfo=UTC)
    baseline = TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=checkpoint_end.isoformat(),
        integrity_sha256="1" * 64,
        authority_verified=True,
        calls_used_today=0,
        bytes_used_30d=0,
    )
    context = UsageExtensionContext(
        database_url=clean_postgres.url.render_as_string(hide_password=False),
        raw_store_root=tmp_path,
    )
    day_one_total = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=context,
        coverage_end_utc=checkpoint_end,
        now=datetime(2026, 8, 19, 23, 59, tzinfo=UTC),
    )
    day_two_total = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=context,
        coverage_end_utc=checkpoint_end,
        now=datetime(2026, 8, 20, 23, 59, tzinfo=UTC),
    )
    day_three_total = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=context,
        coverage_end_utc=checkpoint_end,
        now=terminal_at + timedelta(seconds=1),
    )
    assert day_one_total.calls_used_today == 1
    assert day_two_total.calls_used_today == 1
    assert day_three_total.calls_used_today == 1
    assert day_three_total.bytes_used_30d == 3 * len(body)
    recovered = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=context,
        coverage_end_utc=checkpoint_end,
        now=terminal_at + timedelta(seconds=1),
    )
    assert recovered.calls_used_today == 1
    assert recovered.bytes_used_30d == day_three_total.bytes_used_30d


def test_zero_durable_usage_is_authoritative_for_its_day() -> None:
    """Calls landing before midnight must not charge the next day's cap."""

    terminal_row = UsageRecordLeaf(
        run_id="run-midnight",
        usage_seq=1,
        plan_id="plan-midnight",
        plan_sha256="a" * 64,
        provider="fmp",
        metric="calls_attempted",
        quantity=Decimal(5),
        unit="call",
        recorded_at_utc=datetime(2026, 8, 19, 0, 0, 1, tzinfo=UTC),
    )
    window_quantity, orphan_quantity = _window_and_durable_extension(
        post_checkpoint_leaves=(terminal_row,),
        rolling_leaves=(),
        durable_by_run={"run-midnight": 0},
        metric="calls_attempted",
        unit="call",
        already_start_utc=datetime.min.replace(tzinfo=UTC),
    )
    assert window_quantity == 0
    assert orphan_quantity == 0


def test_require_service_day_allows_the_previous_utc_day() -> None:
    now = datetime(2026, 8, 29, 0, 0, 1, tzinfo=UTC)
    require_service_day(date(2026, 8, 29), now)
    require_service_day(date(2026, 8, 28), now)
    with pytest.raises(PreconditionError, match="current UTC date"):
        require_service_day(date(2026, 8, 27), now)


def test_grace_day_authorization_binds_collector_as_of_to_the_requested_service_day() -> None:
    yesterday = date(2026, 8, 21)
    today = datetime(2026, 8, 22, 0, 0, 1, tzinfo=UTC)
    private_key = Ed25519PrivateKey.generate()
    payload = canonical_json_bytes(
        _document(
            calls_per_minute=SYNTHETIC_TIER_CALLS_PER_MINUTE, tier_sha256=SYNTHETIC_TIER_SHA256
        )
    )
    verified = verify_recurring_authority(
        payload,
        private_key.sign(payload),
        _authority(private_key),
        now=today,
    )
    authorization = build_recurring_authorization(
        authority=verified,
        policy=FmpPolicy(
            policy_id="fmp-operational-candidate-v1",
            license_classification="PROPRIETARY_SUBSCRIPTION",
            scheduled_collection_allowed=True,
            sha256="e" * 64,
        ),
        notification=NotificationArtifact(
            notified_at_utc=today,
            channel="synthetic",
            storage_aliases=("external-data-root",),
            sha256="f" * 64,
        ),
        tier=_synthetic_null_tier(),
        tier_sha256=SYNTHETIC_TIER_SHA256,
        usage=TrustedUsageSnapshot(
            source="signed-checkpoint",
            recorded_at_utc=today.isoformat(),
            integrity_sha256="1" * 64,
            authority_verified=True,
            calls_used_today=0,
            bytes_used_30d=0,
        ),
        operation=RecurringOperation(
            command="build-universe",
            service_day=yesterday,
            output_path=verified.output_root / "2026-08-21/universe-manifest.json",
        ),
        manifest=None,
        manifest_sha256=None,
        approval_clock=lambda: today,
    )
    assert authorization.service_day == yesterday
    as_of = authorization.service_day if authorization.service_day is not None else today.date()
    assert as_of == yesterday


def test_sharded_daily_budget_is_ceil_partitioned_against_own_calls() -> None:
    shard_total = 32
    calls_per_day = 1_000_000
    shard_budget = (calls_per_day + shard_total - 1) // shard_total
    assert shard_budget == (1_000_000 + 32 - 1) // 32
    clock = {"t": 0.0}

    def now() -> float:
        return clock["t"]

    def sleep(seconds: float) -> None:
        clock["t"] += seconds

    limiter = RateLimiter(
        tier=TierArtifact(
            calls_per_minute=3000,
            calls_per_day=shard_budget,
            bandwidth_gb_30d=None,
        ),
        max_calls=None,
        clock=now,
        sleep=sleep,
        run_seed=7,
        usage_baseline=TrustedUsageSnapshot(
            source="signed-checkpoint",
            recorded_at_utc=NOW.isoformat(),
            integrity_sha256="1" * 64,
            authority_verified=True,
            calls_used_today=0,
            bytes_used_30d=0,
        ),
    )
    for _ in range(shard_budget):
        limiter.before_request()
    with pytest.raises(BudgetExhaustedError, match="daily cap"):
        limiter.before_request()
    assert limiter.calls_attempted == shard_budget
