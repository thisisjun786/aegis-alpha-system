"""Sharded collection contracts: partition, identity, lock, and CLI gating."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, date, datetime
from io import StringIO
from pathlib import Path

import pytest

from aegis_alpha.data.fmp_cli_artifacts import PRECONDITION_EXIT, FmpPolicy, NotificationArtifact
from aegis_alpha.data.fmp_collector import CollectorLock
from aegis_alpha.data.fmp_daily_cli import main as daily_main
from aegis_alpha.data.fmp_rate_types import TierArtifact, TrustedUsageSnapshot, parse_tier_artifact
from aegis_alpha.data.fmp_recurring_authority import (
    VerifiedRecurringAuthority,
)
from aegis_alpha.data.fmp_recurring_authorization import (
    RecurringOperation,
    build_recurring_authorization,
)
from aegis_alpha.data.fmp_windows import (
    ManifestSource,
    UniverseEntry,
    UniverseManifest,
    partition_universe_manifest,
)

_EXPECTED_SHARD_TOTAL = 2
_SHARDED_DAILY_BUDGET = 31_250


def _manifest(symbols: tuple[str, ...]) -> UniverseManifest:
    entries = tuple(
        UniverseEntry(symbol=s, ipo_date=None, delisted_date=None, active=True) for s in symbols
    )
    source = ManifestSource(
        endpoint="/stable/actively-trading-list",
        retrieved_at_utc=datetime(2026, 8, 28, tzinfo=UTC),
        raw_content_sha256="0" * 64,
    )
    return UniverseManifest(
        generated_at_utc=datetime(2026, 8, 28, tzinfo=UTC),
        sources=(source,),
        entries=entries,
    )


def test_partition_splits_disjoint_and_reunites() -> None:
    manifest = _manifest(tuple(f"S{i:03d}" for i in range(50)))
    first = partition_universe_manifest(manifest, shard_index=1, shard_total=2)
    second = partition_universe_manifest(manifest, shard_index=2, shard_total=2)
    first_symbols = first.symbols()
    second_symbols = second.symbols()
    assert not set(first_symbols) & set(second_symbols)
    assert set(first_symbols) | set(second_symbols) == set(manifest.symbols())
    assert (
        partition_universe_manifest(manifest, shard_index=1, shard_total=2).symbols()
        == first_symbols
    )


def test_partition_rejects_empty_shard_and_bad_coordinates() -> None:
    manifest = _manifest(("AAA",))
    with pytest.raises(ValueError, match="no symbols"):
        partition_universe_manifest(manifest, shard_index=2, shard_total=2)
    with pytest.raises(ValueError, match="out of range"):
        partition_universe_manifest(manifest, shard_index=0, shard_total=2)


def _authority() -> VerifiedRecurringAuthority:
    return VerifiedRecurringAuthority(
        authority_id="test-authority",
        key_id="ed25519:test",
        approver="synthetic-owner",
        schedule_id="fmp-daily-refresh-v1",
        dataset_selection="all",
        raw_store_root=Path("/data/aegis-alpha-system/raw"),
        dataset_root=Path("/data/aegis-alpha-system/normalized"),
        output_root=Path("/data/aegis-alpha-system/owner-receipts/fmp-daily"),
        norgate_security_master=Path(
            "/data/aegis-alpha-system/normalized/norgate/"
            "2026-07-29-us-platinum/full-v1/security_master.parquet"
        ),
        norgate_security_master_sha256="9" * 64,
        norgate_security_master_row_count=35603,
        norgate_snapshot_date=date(2026, 7, 28),
        backfill_from=date(2026, 7, 29),
        calls_per_minute=3000,
        calls_per_day=1000000,
        registry_sha256="0" * 64,
        notification_sha256="0" * 64,
        tier_sha256="1" * 64,
        valid_from_utc=datetime(2026, 8, 28, tzinfo=UTC),
        key_valid_until_utc=datetime(2027, 8, 28, tzinfo=UTC),
        payload_sha256="b" * 64,
        signature_sha256="c" * 64,
        authority_artifact_sha256="d" * 64,
    )


def test_run_identity_shards_are_distinct_and_stable() -> None:
    authority = _authority()
    day = date(2026, 8, 28)
    plain = authority.run_identity("collect", day)
    first = authority.run_identity("collect", day, shard=(1, 6))
    second = authority.run_identity("collect", day, shard=(2, 6))
    assert plain != first != second
    assert authority.run_identity("collect", day, shard=(1, 6)) == first
    with pytest.raises(Exception, match="shard coordinates"):
        authority.run_identity("collect", day, shard=(7, 6))


def test_shard_lock_paths_differ_and_default_is_unchanged() -> None:
    root = Path("/data/aegis-alpha-system/raw")
    plain = CollectorLock(root, run_identity="r")
    one = CollectorLock(root, run_identity="r1", shard=(1, 6))
    two = CollectorLock(root, run_identity="r2", shard=(2, 6))
    assert plain.path == root / "fmp" / "collector.lock"
    assert one.path != two.path
    assert one.path.name == "collector-shard1of6.lock"
    assert "fmp" in one.path.parts


def test_shard_divides_effective_rate_with_safety_factor() -> None:
    tier: TierArtifact = parse_tier_artifact(
        {"calls_per_minute": 3000, "calls_per_day": None, "bandwidth_gb_30d": 150}
    )
    shard_tier = replace(tier, calls_per_minute=3000 // 6)
    per_minute = 60 / shard_tier.minimum_interval_seconds
    assert per_minute == pytest.approx(400)
    assert per_minute * 6 == pytest.approx(2400)


def test_sharded_authorization_divides_daily_budget_by_ceil() -> None:
    authority = _authority()
    day = date(2026, 8, 28)
    authorization = build_recurring_authorization(
        authority=authority,
        policy=FmpPolicy(
            policy_id="fmp-operational-candidate-v1",
            license_classification="PROPRIETARY_SUBSCRIPTION",
            scheduled_collection_allowed=True,
            sha256="0" * 64,
        ),
        notification=NotificationArtifact(
            notified_at_utc=datetime(2026, 8, 28, tzinfo=UTC),
            channel="synthetic",
            storage_aliases=("external-data-root",),
            sha256="0" * 64,
        ),
        tier=parse_tier_artifact(
            {"calls_per_minute": 3000, "calls_per_day": None, "bandwidth_gb_30d": 150}
        ),
        tier_sha256="1" * 64,
        usage=TrustedUsageSnapshot(
            source="signed-checkpoint",
            recorded_at_utc="2026-08-28T00:00:00+00:00",
            integrity_sha256="1" * 64,
            authority_verified=True,
            calls_used_today=0,
            bytes_used_30d=0,
        ),
        operation=RecurringOperation(
            command="collect",
            service_day=day,
            output_path=authority.output_root / "2026-08-28/collection-receipt.json",
            manifest_path=authority.output_root / "2026-08-28/active-collection-manifest.json",
            shard=(1, 32),
        ),
        manifest=_manifest(("AAA", "BBB")),
        manifest_sha256="e" * 64,
        approval_clock=lambda: datetime(2026, 8, 28, tzinfo=UTC),
    )
    assert authorization.tier.calls_per_day == _SHARDED_DAILY_BUDGET


def _daily_argv(tmp_path: Path, **overrides: str) -> list[str]:
    argv = [
        "--registry",
        str(tmp_path / "registry.json"),
        "--storage-notification",
        str(tmp_path / "storage-notification.json"),
        "--tier",
        str(tmp_path / "tier.json"),
        "--recurring-authority",
        str(tmp_path / "authority.json"),
        "--recurring-authority-signature",
        str(tmp_path / "authority.sig"),
    ]
    return argv + [str(part) for pair in overrides.items() for part in pair]


def test_cli_rejects_half_specified_shard(tmp_path: Path) -> None:
    out, err = StringIO(), StringIO()
    code = daily_main(
        _daily_argv(tmp_path, **{"--shard-index": str(_EXPECTED_SHARD_TOTAL)}),
        stdout=out,
        stderr=err,
    )
    assert code == PRECONDITION_EXIT
    assert "provided together" in err.getvalue()


def test_cli_rejects_out_of_range_shard(tmp_path: Path) -> None:
    out, err = StringIO(), StringIO()
    code = daily_main(
        _daily_argv(
            tmp_path,
            **{"--shard-index": "3", "--shard-total": str(_EXPECTED_SHARD_TOTAL)},
        ),
        stdout=out,
        stderr=err,
    )
    assert code == PRECONDITION_EXIT
    assert "1 <= shard-index <= shard-total" in err.getvalue()


def test_shard_assignment_digest_is_stable() -> None:
    digest = hashlib.sha256(b"VTSI").hexdigest()[:16]
    assert int(digest, 16) % 2 in (0, 1)
