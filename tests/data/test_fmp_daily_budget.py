"""Invocation bounds through the real daily, command, limiter and PostgreSQL path."""

from __future__ import annotations

import hashlib
import io
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
import test_fmp_recurring_authority as authority_support
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import select

from aegis_alpha.application.provider_cli import run_collection_profile
from aegis_alpha.application.provider_config import CollectionConfig, ProviderProfile
from aegis_alpha.collection.records import RunEventType
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import collection_run_events, collection_usage_records
from aegis_alpha.data import fmp_daily_cli
from aegis_alpha.data.fmp_cli_artifacts import PRECONDITION_EXIT, FmpPolicy, NotificationArtifact
from aegis_alpha.data.fmp_collector import CollectorRequest, CollectorResponse
from aegis_alpha.data.fmp_daily_refresh import NorgateUniverseRecord
from aegis_alpha.data.fmp_live_authorization import LiveAuthorization
from aegis_alpha.data.fmp_rate_types import TierArtifact, TrustedUsageSnapshot
from aegis_alpha.data.fmp_recurring_authority import verify_recurring_authority
from aegis_alpha.data.fmp_recurring_authorization import (
    RecurringOperation,
    build_recurring_authorization,
)
from aegis_alpha.data.fmp_windows import parse_universe_manifest
from aegis_alpha.data.serialization import canonical_json_bytes

if TYPE_CHECKING:
    from sqlalchemy import Engine

NOW = authority_support.NOW
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/provider_neutral/fmp_collector"
SUCCESS_CALLS = 8


@dataclass
class DailyTransport:
    retry_first: bool = False
    calls: list[CollectorRequest] = field(default_factory=list)

    def __call__(self, request: CollectorRequest, credential: str) -> CollectorResponse:
        assert credential == "synthetic-budget-credential"
        self.calls.append(request)
        status = 200
        if self.retry_first and len(self.calls) == 1:
            status, payload = 429, b'{"Error Message":"temporary"}'
        elif request.endpoint == "/stable/actively-trading-list":
            payload = b'[{"symbol":"SYNTH.A","ipoDate":"2020-01-02"}]'
        elif request.endpoint == "/stable/delisted-companies":
            payload = b'[{"symbol":"SYNTH.OLD","delistedDate":"2021-01-01"}]'
        elif request.endpoint == "/stable/profile":
            payload = (FIXTURES / "profile.json").read_bytes()
        elif request.endpoint in {"/stable/splits", "/stable/dividends"}:
            payload = b"[]"
        else:
            fixture = {
                "/stable/historical-price-eod/full": "price_eod_full.json",
                "/stable/historical-price-eod/non-split-adjusted": (
                    "price_eod_non_split_adjusted.json"
                ),
                "/stable/historical-price-eod/dividend-adjusted": (
                    "price_eod_dividend_adjusted.json"
                ),
            }[request.endpoint]
            row = json.loads((FIXTURES / fixture).read_bytes())[0]
            row["date"] = NOW.date().isoformat()
            payload = canonical_json_bytes([row])
        return CollectorResponse(
            status_code=status,
            headers={"content-type": "application/json"},
            body=payload,
            requested_at_utc=NOW,
            retrieved_at_utc=NOW,
        )


@dataclass
class DailyHarness:
    root: Path
    engine: Engine
    transport: DailyTransport = field(default_factory=DailyTransport)

    def run(
        self, max_calls: int | None, *, universe_only: bool = False
    ) -> tuple[int, dict[str, object]]:
        argv = [
            "--registry",
            str(self.root / "registry.json"),
            "--storage-notification",
            str(self.root / "notification.json"),
            "--tier",
            str(self.root / "tier.json"),
            "--recurring-authority",
            str(self.root / "authority.json"),
            "--recurring-authority-signature",
            str(self.root / "authority.sig"),
        ]
        if max_calls is not None:
            argv.extend(["--max-calls", str(max_calls)])
        if universe_only:
            argv.append("--universe-only")
        stdout, stderr = io.StringIO(), io.StringIO()
        code = fmp_daily_cli.main(
            argv,
            environ={
                "AAS_DATABASE_URL": self.engine.url.render_as_string(hide_password=False),
                "FMP_API_KEY": "synthetic-budget-credential",
            },
            stdout=stdout,
            stderr=stderr,
            now=NOW,
            transport=self.transport,
            monotonic=lambda: 0.0,
            sleep=lambda _seconds: None,
            approval_clock=lambda: NOW,
            wall_clock=lambda: NOW,
        )
        report = json.loads(stdout.getvalue() or stderr.getvalue())
        if code:
            with self.engine.connect() as connection:
                report["persisted_errors"] = list(
                    connection.scalars(
                        select(collection_run_events.c.error_message).where(
                            collection_run_events.c.error_message.is_not(None)
                        )
                    )
                )
        return code, report


@pytest.fixture
def daily_harness(
    tmp_path: Path,
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> DailyHarness:
    # Reuse signed synthetic authority construction; only upstream admission inputs
    # are substituted. Daily dispatch, command, collector, limiter and registry run.
    private_key = Ed25519PrivateKey.generate()
    payload = canonical_json_bytes(
        authority_support._document(  # noqa: SLF001 -- established signed fixture
            raw_store_root=str(tmp_path / "raw"),
            dataset_root=str(tmp_path / "normalized"),
            output_root=str(tmp_path / "daily"),
            norgate_security_master=str(tmp_path / "master.parquet"),
            norgate_snapshot_date="2026-08-20",
            backfill_from="2026-08-21",
            calls_per_day=1000,
        )
    )
    authority = verify_recurring_authority(
        payload,
        private_key.sign(payload),
        authority_support._authority(private_key),  # noqa: SLF001 -- established signed fixture
        now=NOW,
    )

    def authorize(**values: object) -> LiveAuthorization:
        operation = cast("RecurringOperation", values["operation"])
        data = None if operation.manifest_path is None else operation.manifest_path.read_bytes()
        return build_recurring_authorization(
            authority=authority,
            policy=FmpPolicy(
                policy_id="fmp-operational-candidate-v1",
                license_classification="PROPRIETARY_SUBSCRIPTION",
                scheduled_collection_allowed=True,
                sha256="e" * 64,
            ),
            notification=NotificationArtifact(NOW, "synthetic", ("fixture-root",), "f" * 64),
            tier=TierArtifact(3000, 1000, None),
            tier_sha256="2" * 64,
            usage=TrustedUsageSnapshot(
                source="synthetic-signed",
                recorded_at_utc=NOW.isoformat(),
                integrity_sha256="a" * 64,
                authority_verified=True,
                calls_used_today=0,
                bytes_used_30d=0,
            ),
            operation=operation,
            manifest=None if data is None else parse_universe_manifest(json.loads(data)),
            manifest_sha256=None if data is None else hashlib.sha256(data).hexdigest(),
            approval_clock=lambda: NOW,
        )

    monkeypatch.setattr(fmp_daily_cli, "_verified_authority", lambda *_args: authority)
    monkeypatch.setattr(fmp_daily_cli, "authorize_recurring_operation", authorize)
    monkeypatch.setattr(fmp_daily_cli, "finalize_pending_markers", lambda **_kwargs: ())
    monkeypatch.setattr(
        fmp_daily_cli,
        "load_norgate_universe",
        lambda *_args, **_kwargs: (NorgateUniverseRecord(1, "SYNTH.A", is_delisted=False),),
    )
    return DailyHarness(tmp_path, clean_postgres)


@pytest.mark.parametrize(("limit", "retry"), [(1, False), (3, False), (4, True)])
def test_daily_budget_stops_before_next_call_and_persists_cancellation(
    daily_harness: DailyHarness,
    limit: int,
    *,
    retry: bool,
) -> None:
    daily_harness.transport.retry_first = retry
    code, report = daily_harness.run(limit)
    assert code == 1, report
    assert report["terminal_event"] == "run_cancelled", report.get("persisted_errors", report)
    assert report["error_class"] == "BudgetExhaustedError"
    assert report["invocation_calls_attempted"] == limit
    assert report["invocation_max_calls"] == limit
    assert len(daily_harness.transport.calls) == limit
    registry = CollectionRegistry(daily_harness.engine)
    state = registry.current_run_state(str(report["run_id"]))
    assert state is not None
    assert state.state is RunEventType.RUN_CANCELLED
    with daily_harness.engine.connect() as connection:
        usage = (
            connection.execute(
                select(collection_usage_records).where(
                    collection_usage_records.c.metric == "calls_attempted",
                )
            )
            .mappings()
            .all()
        )
        assert sum(row["quantity"] for row in usage) == limit
        recorded = [row["recorded_at_utc"] for row in usage if row["run_id"] == report["run_id"]]
        terminal_time = connection.scalar(
            select(collection_run_events.c.occurred_at_utc).where(
                collection_run_events.c.run_id == report["run_id"],
                collection_run_events.c.event_type == "run_cancelled",
            )
        )
        assert recorded
        assert all(moment <= terminal_time for moment in recorded)


@pytest.mark.parametrize("limit", [SUCCESS_CALLS, None])
def test_daily_budget_success_and_default_compatibility(
    daily_harness: DailyHarness,
    limit: int | None,
) -> None:
    code, report = daily_harness.run(limit)
    assert code == 0, report
    assert len(daily_harness.transport.calls) == SUCCESS_CALLS
    assert report["provider_calls"] == SUCCESS_CALLS
    if limit is None:
        assert "invocation_calls_attempted" not in report
    else:
        assert report["invocation_calls_attempted"] == SUCCESS_CALLS


@pytest.mark.parametrize(
    ("failure", "exit_code"), [(OSError, PRECONDITION_EXIT), (RuntimeError, 1)]
)
def test_completed_universe_resume_does_not_debit_new_invocation(
    daily_harness: DailyHarness,
    monkeypatch: pytest.MonkeyPatch,
    failure: type[Exception],
    exit_code: int,
) -> None:
    load = fmp_daily_cli.load_norgate_universe

    def interrupted(*_args: object, **_kwargs: object) -> None:
        raise failure("synthetic classification interruption")

    monkeypatch.setattr(fmp_daily_cli, "load_norgate_universe", interrupted)
    code, report = daily_harness.run(SUCCESS_CALLS)
    assert code == exit_code, report
    assert report["invocation_calls_attempted"] == 2  # noqa: PLR2004 -- active/delisted requests
    old_calls = len(daily_harness.transport.calls)
    monkeypatch.setattr(fmp_daily_cli, "load_norgate_universe", load)
    code, report = daily_harness.run(1)
    assert code == 1, report
    assert report["terminal_event"] == "run_cancelled"
    assert report["invocation_calls_attempted"] == 1
    assert len(daily_harness.transport.calls) == old_calls + 1
    assert daily_harness.transport.calls[-1].endpoint == "/stable/profile"


def test_application_dispatch_preserves_total_budget_and_stderr_result(
    daily_harness: DailyHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limit = 5
    root = daily_harness.root
    credential_file = root / "provider.env"
    credential_file.write_text(
        "FMP_API_KEY=synthetic-budget-credential\nAAS_DATABASE_URL=legacy-must-not-be-used\n"
    )
    credential_file.chmod(0o600)
    database_file = root / "database-url"
    database_file.write_text(daily_harness.engine.url.render_as_string(hide_password=False))
    database_file.chmod(0o600)
    data_profile = root / "data.json"
    data_profile.write_text(
        json.dumps(
            {
                "version": 1,
                "database_url_file": str(database_file),
                "dataset_roots": [],
            }
        )
    )
    options = {
        "registry": str(root / "registry.json"),
        "storage_notification": str(root / "notification.json"),
        "tier": str(root / "tier.json"),
        "recurring_authority": str(root / "authority.json"),
        "recurring_authority_signature": str(root / "authority.sig"),
    }
    # Profile existence checks are real; daily_harness supplies the signed authority inputs.
    for path in options.values():
        Path(path).write_text("{}")
    profile = ProviderProfile(
        provider="fmp",
        enabled=True,
        credential_file=credential_file,
        max_calls=limit,
        mode="daily",
        options=options,
    )
    config = CollectionConfig(data_profile, (profile,))
    original_main = fmp_daily_cli.main

    def controlled_main(
        argv: list[str],
        *,
        environ: dict[str, str],
        stdout: io.StringIO,
        stderr: io.StringIO,
    ) -> int:
        return original_main(
            argv,
            environ=environ,
            stdout=stdout,
            stderr=stderr,
            now=NOW,
            transport=daily_harness.transport,
            monotonic=lambda: 0.0,
            sleep=lambda _seconds: None,
            approval_clock=lambda: NOW,
            wall_clock=lambda: NOW,
        )

    monkeypatch.setattr(fmp_daily_cli, "main", controlled_main)
    report = run_collection_profile(config, profile)
    assert report["execution_started"] is True
    assert report["status"] == "failed", report
    assert report["exit_code"] == 1
    payload = report["result"]
    assert isinstance(payload, dict)
    result = cast("dict[str, object]", payload)
    assert result["invocation_calls_attempted"] == limit
    assert result["invocation_max_calls"] == limit
    assert result["terminal_event"] == "run_cancelled"
    assert result["error_class"] == "BudgetExhaustedError"
    diagnostic = report["diagnostic"]
    assert isinstance(diagnostic, str)
    assert result == json.loads(diagnostic)  # the owner emitted stderr, not stdout
    assert len(daily_harness.transport.calls) == limit
    assert [request.endpoint for request in daily_harness.transport.calls] == [
        "/stable/actively-trading-list",
        "/stable/delisted-companies",
        "/stable/profile",
        "/stable/historical-price-eod/full",
        "/stable/historical-price-eod/non-split-adjusted",
    ]
    state = CollectionRegistry(daily_harness.engine).current_run_state(str(result["run_id"]))
    assert state is not None
    assert state.state is RunEventType.RUN_CANCELLED
    with daily_harness.engine.connect() as connection:
        usage = list(
            connection.scalars(
                select(collection_usage_records.c.quantity).where(
                    collection_usage_records.c.metric == "calls_attempted",
                )
            )
        )
        assert sum(usage) == limit
    assert "synthetic-budget-credential" not in json.dumps(report)


def test_universe_probe_finishes_without_cancelling_collection_identity(
    daily_harness: DailyHarness,
) -> None:
    code, report = daily_harness.run(SUCCESS_CALLS, universe_only=True)
    assert code == 0, report
    assert report["collection_executed"] is False
    assert report["terminal_event"] == "run_succeeded"
    assert "collection_run_id" not in report
    assert {request.endpoint for request in daily_harness.transport.calls} == {
        "/stable/actively-trading-list",
        "/stable/delisted-companies",
    }
    old_calls = len(daily_harness.transport.calls)
    code, report = daily_harness.run(SUCCESS_CALLS)
    assert code == 0, report
    assert report["invocation_calls_attempted"] == SUCCESS_CALLS - old_calls
    assert len(daily_harness.transport.calls) == SUCCESS_CALLS
