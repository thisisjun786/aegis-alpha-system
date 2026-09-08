from __future__ import annotations

import argparse
import io
import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

from aegis_alpha.collection.records import RunEventType
from aegis_alpha.data import fmp_daily_cli
from aegis_alpha.data.fmp_collector import CollectorLockLostError, CollectorOutcome
from aegis_alpha.data.fmp_daily_refresh import NorgateUniverseRecord
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_windows import parse_universe_manifest
from aegis_alpha.data.serialization import canonical_json_bytes

if TYPE_CHECKING:
    import pytest

NOW = datetime(2026, 8, 21, 6, 30, tzinfo=UTC)
EXPECTED_PROVIDER_CALLS = 7
SHARDED_REUSE_PROVIDER_CALLS = 5
PRECONDITION_EXIT = 2


def _manifest() -> dict[str, object]:
    return {
        "generated_at_utc": NOW.isoformat(),
        "sources": [
            {
                "endpoint": "/stable/actively-trading-list",
                "retrieved_at_utc": NOW.isoformat(),
                "raw_content_sha256": "1" * 64,
            },
            {
                "endpoint": "/stable/delisted-companies",
                "retrieved_at_utc": NOW.isoformat(),
                "raw_content_sha256": "2" * 64,
            },
        ],
        "entries": [
            {"symbol": "known", "ipoDate": "2020-01-02", "delistedDate": None, "active": True},
            {"symbol": "new", "ipoDate": "2026-08-20", "delistedDate": None, "active": True},
            {
                "symbol": "gone",
                "ipoDate": "2000-01-02",
                "delistedDate": "2010-01-02",
                "active": False,
            },
        ],
    }


def test_daily_cli_builds_classifies_and_collects_active_symbols(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = tmp_path / "daily"
    authority = SimpleNamespace(
        schedule_id="fmp-daily-refresh-v1",
        output_root=output_root,
        payload_sha256="a" * 64,
        norgate_security_master=tmp_path / "security-master.parquet",
        norgate_security_master_sha256="b" * 64,
        norgate_security_master_row_count=1,
        norgate_snapshot_date=date(2026, 7, 28),
    )
    operations: list[str] = []

    def authorize(**values: object) -> SimpleNamespace:
        operation = cast("fmp_daily_cli.RecurringOperation", values["operation"])
        manifest = None
        if operation.manifest_path is not None:
            manifest = parse_universe_manifest(json.loads(operation.manifest_path.read_bytes()))
        return SimpleNamespace(
            output_path=operation.output_path,
            run_id=f"fmp-run-{operation.command}",
            manifest=manifest,
            selection=DatasetSelection.ALL
            if operation.command == "collect"
            else DatasetSelection.PROBE,
            max_calls=None,
        )

    def run(
        *,
        arguments: argparse.Namespace,
        authorization: SimpleNamespace,
        **_values: object,
    ) -> tuple[CollectorOutcome, int]:
        operations.append(arguments.command)
        if arguments.command == "build-universe":
            authorization.output_path.parent.mkdir(parents=True, exist_ok=True)
            authorization.output_path.write_bytes(canonical_json_bytes(_manifest()))
        else:
            assert authorization.max_calls is None
            assert authorization.selection is DatasetSelection.ALL
            assert authorization.manifest.symbols() == ("known", "new")
        return (
            CollectorOutcome(
                run_id=authorization.run_id,
                plan_id=f"plan-{arguments.command}",
                terminal_event=RunEventType.RUN_SUCCEEDED,
                published_paths=(),
                receipt_path=authorization.output_path,
                quality_results=(),
                watermarks_advanced=(),
            ),
            2 if arguments.command == "build-universe" else 5,
        )

    monkeypatch.setattr(fmp_daily_cli, "_verified_authority", lambda *_args: authority)
    monkeypatch.setattr(
        fmp_daily_cli,
        "finalize_pending_markers",
        lambda **_kwargs: ("fmp-run-recovered",),
    )
    monkeypatch.setattr(fmp_daily_cli, "authorize_recurring_operation", authorize)
    monkeypatch.setattr(fmp_daily_cli, "run_authorized_command", run)
    monkeypatch.setattr(
        fmp_daily_cli,
        "load_norgate_universe",
        lambda *_args, **_kwargs: (
            NorgateUniverseRecord(assetid=1, symbol="KNOWN", is_delisted=False),
        ),
    )
    stdout = io.StringIO()
    result = fmp_daily_cli.main(
        [
            "--registry",
            str(tmp_path / "registry.json"),
            "--storage-notification",
            str(tmp_path / "notification.json"),
            "--tier",
            str(tmp_path / "tier.json"),
            "--recurring-authority",
            str(tmp_path / "authority.json"),
            "--recurring-authority-signature",
            str(tmp_path / "authority.sig"),
        ],
        environ={
            "AAS_DATABASE_URL": "postgresql://synthetic",
            "FMP_API_KEY": "synthetic",
        },
        stdout=stdout,
        now=NOW,
    )

    assert result == 0
    assert operations == ["build-universe", "collect"]
    summary = json.loads(stdout.getvalue())
    assert summary["provider_calls"] == EXPECTED_PROVIDER_CALLS
    assert summary["recovered_run_ids"] == ["fmp-run-recovered"]
    assert summary["new_listing_candidates"] == 1
    classification = json.loads(
        (output_root / "2026-08-21" / "universe-classification.json").read_bytes()
    )
    assert classification["counts"]["norgate_overlap"] == 1
    assert classification["counts"]["new_listing_candidate"] == 1


def test_sharded_daily_cli_reuses_existing_day_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = tmp_path / "daily"
    day_root = output_root / "2026-08-21"
    day_root.mkdir(parents=True)
    universe_bytes = canonical_json_bytes(_manifest())
    (day_root / "universe-manifest.json").write_bytes(universe_bytes)
    classification = {
        "contract": "aegis-alpha/fmp-daily-universe-classification",
        "version": 1,
        "norgate_security_master_sha256": "b" * 64,
        "norgate_snapshot_date": "2026-07-28",
        "counts": {
            "norgate_overlap": 1,
            "new_listing_candidate": 1,
            "unresolved": 0,
            "provider_active_delisted_overlap": 0,
        },
        "ignored_delisted_symbols": ["gone"],
        "entries": [
            {"symbol": "known", "disposition": "norgate_overlap", "norgate_assetid": 1},
            {"symbol": "new", "disposition": "new_listing_candidate", "norgate_assetid": None},
        ],
    }
    (day_root / "universe-classification.json").write_bytes(canonical_json_bytes(classification))
    collection_manifest = {
        "generated_at_utc": NOW.isoformat(),
        "sources": _manifest()["sources"],
        "entries": [
            {"symbol": "known", "ipoDate": "2020-01-02", "delistedDate": None, "active": True},
            {"symbol": "new", "ipoDate": "2026-08-20", "delistedDate": None, "active": True},
        ],
    }
    (day_root / "active-collection-manifest.json").write_bytes(
        canonical_json_bytes(collection_manifest)
    )
    authority = SimpleNamespace(
        schedule_id="fmp-daily-refresh-v1",
        output_root=output_root,
        payload_sha256="a" * 64,
        norgate_security_master=tmp_path / "security-master.parquet",
        norgate_security_master_sha256="b" * 64,
        norgate_security_master_row_count=1,
        norgate_snapshot_date=date(2026, 7, 28),
    )
    operations: list[str] = []

    def authorize(**values: object) -> SimpleNamespace:
        operation = cast("fmp_daily_cli.RecurringOperation", values["operation"])
        manifest = None
        if operation.manifest_path is not None:
            manifest = parse_universe_manifest(json.loads(operation.manifest_path.read_bytes()))
        return SimpleNamespace(
            output_path=operation.output_path,
            run_id=f"fmp-run-{operation.command}",
            manifest=manifest,
            selection=DatasetSelection.ALL
            if operation.command == "collect"
            else DatasetSelection.PROBE,
            max_calls=None,
        )

    def run(
        *,
        arguments: argparse.Namespace,
        authorization: SimpleNamespace,
        **_values: object,
    ) -> tuple[CollectorOutcome, int]:
        operations.append(arguments.command)
        assert arguments.command == "collect"
        return (
            CollectorOutcome(
                run_id=authorization.run_id,
                plan_id="plan-collect",
                terminal_event=RunEventType.RUN_SUCCEEDED,
                published_paths=(),
                receipt_path=authorization.output_path,
                quality_results=(),
                watermarks_advanced=(),
            ),
            5,
        )

    monkeypatch.setattr(fmp_daily_cli, "_verified_authority", lambda *_args: authority)
    monkeypatch.setattr(fmp_daily_cli, "finalize_pending_markers", lambda **_kwargs: ())
    monkeypatch.setattr(fmp_daily_cli, "authorize_recurring_operation", authorize)
    monkeypatch.setattr(fmp_daily_cli, "run_authorized_command", run)
    stdout = io.StringIO()
    result = fmp_daily_cli.main(
        [
            "--registry",
            str(tmp_path / "registry.json"),
            "--storage-notification",
            str(tmp_path / "notification.json"),
            "--tier",
            str(tmp_path / "tier.json"),
            "--recurring-authority",
            str(tmp_path / "authority.json"),
            "--recurring-authority-signature",
            str(tmp_path / "authority.sig"),
            "--shard-index",
            "1",
            "--shard-total",
            "2",
        ],
        environ={
            "AAS_DATABASE_URL": "postgresql://synthetic",
            "FMP_API_KEY": "synthetic",
        },
        stdout=stdout,
        now=NOW,
    )
    assert result == 0
    assert operations == ["collect"]
    summary = json.loads(stdout.getvalue())
    assert summary["provider_calls"] == SHARDED_REUSE_PROVIDER_CALLS
    assert summary["universe_run_id"].startswith("fmp-run-")


def test_daily_cli_requires_credential_before_authority_or_provider_access(tmp_path: Path) -> None:
    stderr = io.StringIO()
    result = fmp_daily_cli.main(
        [
            "--registry",
            str(tmp_path / "registry.json"),
            "--storage-notification",
            str(tmp_path / "notification.json"),
            "--tier",
            str(tmp_path / "tier.json"),
            "--recurring-authority",
            str(tmp_path / "authority.json"),
            "--recurring-authority-signature",
            str(tmp_path / "authority.sig"),
        ],
        environ={},
        stderr=stderr,
        now=NOW,
    )

    assert result == PRECONDITION_EXIT
    assert "no provider calls were attempted" in stderr.getvalue()


def test_daily_cli_revokes_standing_authority_without_provider_credential(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = SimpleNamespace(
        authority_id="synthetic-standing-authority",
        payload_sha256="a" * 64,
        raw_store_root=tmp_path,
        require_request=lambda _now: None,
    )
    monkeypatch.setattr(fmp_daily_cli, "_verified_authority", lambda *_args: authority)
    monkeypatch.setattr(
        fmp_daily_cli,
        "run_authorized_command",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("provider path was constructed")),
    )
    stdout = io.StringIO()

    result = fmp_daily_cli.main(
        [
            "--registry",
            str(tmp_path / "registry.json"),
            "--storage-notification",
            str(tmp_path / "notification.json"),
            "--tier",
            str(tmp_path / "tier.json"),
            "--recurring-authority",
            str(tmp_path / "authority.json"),
            "--recurring-authority-signature",
            str(tmp_path / "authority.sig"),
            "--revoke-authority",
        ],
        environ={},
        stdout=stdout,
        now=NOW,
    )

    assert result == 0
    document = json.loads(stdout.getvalue())
    assert Path(document["revocation_path"]).is_file()


def test_daily_cli_requires_external_owner_authority_after_credential(tmp_path: Path) -> None:
    stderr = io.StringIO()
    result = fmp_daily_cli.main(
        [
            "--registry",
            str(tmp_path / "registry.json"),
            "--storage-notification",
            str(tmp_path / "notification.json"),
            "--tier",
            str(tmp_path / "tier.json"),
            "--recurring-authority",
            str(tmp_path / "authority.json"),
            "--recurring-authority-signature",
            str(tmp_path / "authority.sig"),
        ],
        environ={"FMP_API_KEY": "synthetic"},
        stderr=stderr,
        now=NOW,
    )

    assert result == PRECONDITION_EXIT
    assert "AAS_FMP_OWNER_APPROVAL_AUTHORITY_PATH is required" in stderr.getvalue()


def test_daily_cli_classifies_lock_loss_as_precondition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = SimpleNamespace(
        schedule_id="fmp-daily-refresh-v1",
        output_root=tmp_path / "daily",
        payload_sha256="a" * 64,
        norgate_security_master=tmp_path / "security-master.parquet",
        norgate_security_master_sha256="b" * 64,
        norgate_security_master_row_count=1,
        norgate_snapshot_date=date(2026, 7, 28),
    )
    monkeypatch.setattr(fmp_daily_cli, "_verified_authority", lambda *_args: authority)
    monkeypatch.setattr(
        fmp_daily_cli,
        "authorize_recurring_operation",
        lambda **_values: SimpleNamespace(),
    )
    monkeypatch.setattr(
        fmp_daily_cli,
        "run_authorized_command",
        lambda **_values: (_ for _ in ()).throw(CollectorLockLostError("lost lock")),
    )
    stderr = io.StringIO()

    result = fmp_daily_cli.main(
        [
            "--registry",
            str(tmp_path / "registry.json"),
            "--storage-notification",
            str(tmp_path / "notification.json"),
            "--tier",
            str(tmp_path / "tier.json"),
            "--recurring-authority",
            str(tmp_path / "authority.json"),
            "--recurring-authority-signature",
            str(tmp_path / "authority.sig"),
        ],
        environ={"FMP_API_KEY": "synthetic"},
        stderr=stderr,
        now=NOW,
    )

    assert result == PRECONDITION_EXIT
    assert "no unaccounted provider calls were attempted" in stderr.getvalue()


def test_daily_cli_reports_a_failed_universe_terminal_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = SimpleNamespace(
        schedule_id="fmp-daily-refresh-v1",
        output_root=tmp_path / "daily",
        payload_sha256="a" * 64,
        norgate_security_master=tmp_path / "security-master.parquet",
        norgate_security_master_sha256="b" * 64,
        norgate_security_master_row_count=1,
        norgate_snapshot_date=date(2026, 7, 28),
    )
    monkeypatch.setattr(fmp_daily_cli, "_verified_authority", lambda *_args: authority)
    monkeypatch.setattr(
        fmp_daily_cli,
        "authorize_recurring_operation",
        lambda **_values: SimpleNamespace(),
    )
    monkeypatch.setattr(
        fmp_daily_cli,
        "run_authorized_command",
        lambda **_values: (
            CollectorOutcome(
                run_id="fmp-run-universe-failed",
                plan_id="plan-build-universe",
                terminal_event=RunEventType.RUN_FAILED,
                published_paths=(),
                receipt_path=None,
                quality_results=(),
                watermarks_advanced=(),
                error_class="UniverseCompositionError",
            ),
            1,
        ),
    )
    stdout = io.StringIO()
    stderr = io.StringIO()

    result = fmp_daily_cli.main(
        [
            "--registry",
            str(tmp_path / "registry.json"),
            "--storage-notification",
            str(tmp_path / "notification.json"),
            "--tier",
            str(tmp_path / "tier.json"),
            "--recurring-authority",
            str(tmp_path / "authority.json"),
            "--recurring-authority-signature",
            str(tmp_path / "authority.sig"),
        ],
        environ={"FMP_API_KEY": "synthetic"},
        stdout=stdout,
        stderr=stderr,
        now=NOW,
    )

    assert result == 1
    assert stdout.getvalue() == ""
    report = json.loads(stderr.getvalue())
    assert report["run_id"] == "fmp-run-universe-failed"
    assert report["terminal_event"] == "run_failed"
    assert report["error_class"] == "UniverseCompositionError"


def test_daily_cli_reports_a_failed_collection_terminal_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = SimpleNamespace(
        schedule_id="fmp-daily-refresh-v1",
        output_root=tmp_path / "daily",
        payload_sha256="a" * 64,
        norgate_security_master=tmp_path / "security-master.parquet",
        norgate_security_master_sha256="b" * 64,
        norgate_security_master_row_count=1,
        norgate_snapshot_date=date(2026, 7, 28),
    )
    calls = {"n": 0}

    def run(**_values: object) -> tuple[CollectorOutcome, int]:
        calls["n"] += 1
        if calls["n"] == 1:
            universe_path = tmp_path / "daily" / "2026-08-21" / "universe-manifest.json"
            universe_path.parent.mkdir(parents=True, exist_ok=True)
            universe_path.write_bytes(canonical_json_bytes(_manifest()))
            return (
                CollectorOutcome(
                    run_id="fmp-run-universe-ok",
                    plan_id="plan-build-universe",
                    terminal_event=RunEventType.RUN_SUCCEEDED,
                    published_paths=(),
                    receipt_path=None,
                    quality_results=(),
                    watermarks_advanced=(),
                ),
                2,
            )
        return (
            CollectorOutcome(
                run_id="fmp-run-collect-failed",
                plan_id="plan-collect",
                terminal_event=RunEventType.RUN_FAILED,
                published_paths=(),
                receipt_path=None,
                quality_results=(),
                watermarks_advanced=(),
                error_class="CollectorContractError",
            ),
            12,
        )

    monkeypatch.setattr(fmp_daily_cli, "_verified_authority", lambda *_args: authority)
    monkeypatch.setattr(fmp_daily_cli, "finalize_pending_markers", lambda **_kwargs: ())
    monkeypatch.setattr(
        fmp_daily_cli,
        "authorize_recurring_operation",
        lambda **values: SimpleNamespace(
            output_path=values["operation"].output_path,
            run_id="fmp-run-op",
            manifest=None,
            selection=None,
            max_calls=None,
        ),
    )
    monkeypatch.setattr(fmp_daily_cli, "run_authorized_command", run)
    monkeypatch.setattr(
        fmp_daily_cli,
        "load_norgate_universe",
        lambda *_args, **_kwargs: (
            NorgateUniverseRecord(assetid=1, symbol="KNOWN", is_delisted=False),
        ),
    )
    stdout = io.StringIO()
    stderr = io.StringIO()
    result = fmp_daily_cli.main(
        [
            "--registry",
            str(tmp_path / "registry.json"),
            "--storage-notification",
            str(tmp_path / "notification.json"),
            "--tier",
            str(tmp_path / "tier.json"),
            "--recurring-authority",
            str(tmp_path / "authority.json"),
            "--recurring-authority-signature",
            str(tmp_path / "authority.sig"),
        ],
        environ={"FMP_API_KEY": "synthetic", "AAS_DATABASE_URL": "postgresql://synthetic"},
        stdout=stdout,
        stderr=stderr,
        now=NOW,
    )
    assert result == 1
    assert stdout.getvalue() == ""
    report = json.loads(stderr.getvalue())
    assert report["run_id"] == "fmp-run-collect-failed"
    assert report["terminal_event"] == "run_failed"
    assert report["error_class"] == "CollectorContractError"
