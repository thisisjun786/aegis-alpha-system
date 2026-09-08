from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from aegis_alpha.collection.records import CollectionMode
from aegis_alpha.data import fmp_live_authorization
from aegis_alpha.data.fmp_approval_scope import CommonScope, UniverseScope, universe_scope_sha256
from aegis_alpha.data.fmp_cli_artifacts import load_fmp_policy, load_notification_artifact
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_live_authorization import (
    AuthorizationContext,
    LiveAuthorization,
    authorize_live_command,
)
from aegis_alpha.data.fmp_owner_approval import (
    APPROVAL_CONTRACT,
    APPROVAL_VERSION,
    CONTRACT_REVISION,
)
from aegis_alpha.data.fmp_owner_authority import load_owner_approval_authority
from aegis_alpha.data.fmp_rate_limit import TrustedUsageSnapshot
from aegis_alpha.data.fmp_usage_trust import UsageExtensionContext
from aegis_alpha.data.serialization import canonical_json_bytes

if TYPE_CHECKING:
    import pytest


FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/provider_neutral/fmp_collector"
SYNTHETIC_NOW = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)
RUN_ID = "fmp-run-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


def _arguments(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        command="build-universe",
        registry=FIXTURES / "registry_cleared.json",
        owner_approval=tmp_path / "approval.json",
        owner_approval_signature=tmp_path / "approval.sig",
        storage_notification=FIXTURES / "storage_notification.json",
        tier=FIXTURES / "tier_artifact.json",
        max_calls=6,
        raw_store_root=tmp_path / "raw",
        dataset_root=tmp_path / "datasets",
        universe_manifest_out=tmp_path / "universe.json",
        mode="backfill",
        dataset_selection=DatasetSelection.DIVIDENDS.value,
        operator_from="2020-01-01",
    )


def _install_signed_approval(
    arguments: argparse.Namespace,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, str]:
    private_key = Ed25519PrivateKey.generate()
    authority_path = tmp_path / "authority" / "owner-key.json"
    authority_path.parent.mkdir(mode=0o700)
    authority_path.write_bytes(
        canonical_json_bytes(
            {
                "schema": "aegis-alpha/fmp-owner-approval-authority",
                "version": 1,
                "authority_id": "synthetic-owner-authority",
                "key_id": "ed25519:synthetic-2026-08",
                "public_key_encoding": "raw-ed25519-hex",
                "public_key": private_key.public_key().public_bytes_raw().hex(),
                "valid_from_utc": "2026-08-18T12:00:00.000000Z",
                "valid_until_utc": "2026-08-20T12:00:00.000000Z",
            }
        )
    )
    authority_path.chmod(0o600)
    policy = load_fmp_policy(arguments.registry)
    notification = load_notification_artifact(arguments.storage_notification)
    scope = universe_scope_sha256(
        UniverseScope(
            common=CommonScope(
                run_identity=RUN_ID,
                policy_sha256=policy.sha256,
                tier_sha256=hashlib.sha256(arguments.tier.read_bytes()).hexdigest(),
                notification_sha256=notification.sha256,
                raw_store_root=arguments.raw_store_root.resolve(),
                dataset_root=arguments.dataset_root.resolve(),
                max_calls=arguments.max_calls,
            ),
            destination=arguments.universe_manifest_out.resolve(),
        )
    )
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
            "run_identity": RUN_ID,
            "operation": "build-universe",
            "scope_sha256": scope,
            "max_calls": arguments.max_calls,
            "issued_at_utc": "2026-08-19T11:59:00.000000Z",
            "expires_at_utc": "2026-08-19T12:30:00.000000Z",
        }
    )
    arguments.owner_approval.write_bytes(payload)
    arguments.owner_approval_signature.write_bytes(private_key.sign(payload))
    monkeypatch.setattr(
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


def _authorize(
    arguments: argparse.Namespace,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> LiveAuthorization:
    environment = _install_signed_approval(arguments, tmp_path, monkeypatch)
    environment["AAS_DATABASE_URL"] = "synthetic-not-opened"
    monkeypatch.setattr(
        fmp_live_authorization,
        "load_trusted_fmp_usage_snapshot",
        lambda **_kwargs: TrustedUsageSnapshot(
            source="signed-synthetic",
            recorded_at_utc=SYNTHETIC_NOW.isoformat(),
            integrity_sha256="a" * 64,
            authority_verified=True,
            calls_used_today=0,
            bytes_used_30d=0,
        ),
    )
    return authorize_live_command(
        arguments,
        AuthorizationContext(
            environment=environment,
            moment=SYNTHETIC_NOW,
            approval_clock=lambda: SYNTHETIC_NOW,
        ),
    )


def test_signed_universe_scope_normalizes_unscoped_runtime_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authorization = _authorize(_arguments(tmp_path), tmp_path, monkeypatch)

    assert authorization.mode is CollectionMode.PROBE
    assert authorization.selection is DatasetSelection.PROBE
    assert authorization.operator_from is None


def test_one_shot_usage_refreshes_under_the_collector_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorization = _authorize(_arguments(tmp_path), tmp_path, monkeypatch)
    refreshed = TrustedUsageSnapshot(
        source="live-window",
        recorded_at_utc=SYNTHETIC_NOW.isoformat(),
        integrity_sha256="b" * 64,
        authority_verified=True,
        calls_used_today=1,
        bytes_used_30d=2,
    )
    observed: dict[str, object] = {}

    def extend(
        _snapshot: TrustedUsageSnapshot,
        **keywords: object,
    ) -> TrustedUsageSnapshot:
        observed.update(keywords)
        return refreshed

    monkeypatch.setattr(
        fmp_live_authorization,
        "extend_trusted_fmp_usage_snapshot",
        extend,
        raising=False,
    )

    assert authorization.refresh_under_lock is not None
    result = authorization.refresh_under_lock()

    assert result.usage_baseline is refreshed
    context = cast("UsageExtensionContext", observed["context"])
    assert context.raw_store_root == authorization.raw_store_root
    assert context.exclude_run_ids == ()
    assert observed["coverage_end_utc"] == authorization.usage_baseline.recorded_at
    assert observed["now"] == SYNTHETIC_NOW


def test_signed_universe_scope_ignores_injected_collect_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    arguments = _arguments(tmp_path)
    arguments.receipt_path = tmp_path / "rogue-receipt.json"

    authorization = _authorize(arguments, tmp_path, monkeypatch)

    assert authorization.output_path == arguments.universe_manifest_out.resolve()
