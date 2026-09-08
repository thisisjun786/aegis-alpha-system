"""AAS-DATA-008C real-registry 005 lifecycle integration (AAS008C-R1-F5).

These tests use the actual CollectionRegistry and MetadataRegistry against a
real PostgreSQL database, so durable records and source references are proven
rather than asserted from in-memory projections. Provider calls remain zero.
"""

from __future__ import annotations

import io
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO
from uuid import uuid4

import pytest
from finimpulse_authority_support import (
    AUTHORITY_NOW,
    StandingAuthorityFixture,
    authority_argv,
    make_standing_authority,
    write_revocation,
)
from finimpulse_collector_support import (
    OBSERVED_AT,
    SYNTHETIC_CREDENTIAL,
    RecordingTransport,
    Transport,
    make_config,
    make_gate_evidence,
    make_identity_export,
    no_sleep,
    synthetic_pair,
    write_gate_evidence,
    write_identity_export,
)
from sqlalchemy import Connection, Table, func, select, text

from aegis_alpha.collection.records import CollectionRunEvent, RunEventType
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import (
    collection_run_events,
    collection_run_plans,
    collection_run_receipts,
    collection_runs,
    collection_usage_records,
)
from aegis_alpha.data import finimpulse_collector as collector
from aegis_alpha.data import finimpulse_collector_cli as cli_module
from aegis_alpha.data.finimpulse_collector import (
    CREDENTIAL_ENVIRONMENT_VARIABLE,
    RateLimiter,
    canonical_json_bytes,
    collect_snapshot,
    committed_snapshot_ids,
    register_collection_lifecycle,
    sha256_hex,
)
from aegis_alpha.data.finimpulse_collector_cli import PRECONDITION_EXIT
from aegis_alpha.data.finimpulse_collector_cli import main as _cli_main
from aegis_alpha.data.finimpulse_owner_authority import OWNER_AUTHORITY_ENV
from aegis_alpha.data.finimpulse_recurring_authority import verify_recurring_authority
from aegis_alpha.metadata.registry import MetadataRegistry
from aegis_alpha.metadata.schema import source_snapshots

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from aegis_alpha.data.finimpulse_collector import CollectionResult

# Disposable local-only credential for the restricted-role probe.
RESTRICTED_ROLE_SECRET = "probe_only"  # noqa: S105 - synthetic test role


def cli_main(  # noqa: PLR0913 - one parameter per injected production boundary
    argv: Sequence[str],
    *,
    environ: Mapping[str, str] | None = None,
    transport: Transport | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    now: datetime | None = None,
    request_clock: Callable[[], datetime] | None = None,
    sleeper: Callable[[float], None] | None = None,
    rate_limiter: RateLimiter | None = None,
) -> int:
    """Keep request-boundary pacing off the wall clock in this suite."""

    return _cli_main(
        argv,
        environ=environ,
        transport=transport,
        stdout=stdout,
        stderr=stderr,
        now=now,
        request_clock=request_clock,
        sleeper=sleeper if sleeper is not None else no_sleep,
        rate_limiter=rate_limiter,
    )


EXPECTED_BASELINE_TRANSMISSIONS = 2
EXPECTED_USAGE_RECORDS = 2
EXPECTED_COMPLETE_PROJECTION_ROWS = 11
HTTP_INTERNAL_SERVER_ERROR = 500
LIFECYCLE_TABLES = (
    collection_run_plans,
    collection_runs,
    collection_run_events,
    collection_run_receipts,
    collection_usage_records,
    source_snapshots,
)


def collect(tmp_path: Path, snapshot_id: str = "snap-life-001") -> CollectionResult:
    identity = make_identity_export({"AAPL": ["INST-AAPL"], "PLAB": []})
    offline, capability = synthetic_pair(RecordingTransport("baseline"))
    return collect_snapshot(
        snapshot_id=snapshot_id,
        config=make_config(
            identity_export_sha256=identity.export_sha256,
            identity_as_of=identity.as_of_utc,
        ),
        credential=SYNTHETIC_CREDENTIAL,
        transport=offline,
        raw_store_root=tmp_path / "raw",
        dataset_root=tmp_path / "dataset",
        identity_export=identity,
        gate_evidence=make_gate_evidence(tmp_path / "gates"),
        observed_at=OBSERVED_AT,
        sleeper=no_sleep,
        synthetic=capability,
    )


def register(engine: Engine, result: CollectionResult, snapshot_id: str) -> None:
    register_collection_lifecycle(
        result,
        make_config(
            identity_export_sha256=make_identity_export(
                {"AAPL": ["INST-AAPL"], "PLAB": []}
            ).export_sha256,
        ),
        collection_registry=CollectionRegistry(engine),
        metadata_registry=MetadataRegistry(engine),
        plan_id=f"{snapshot_id}.plan",
        run_id=f"{snapshot_id}.run",
        recorded_at_utc=OBSERVED_AT,
    )


def count(engine: Engine, table: Table) -> int:
    with engine.connect() as connection:
        return int(connection.scalar(select(func.count()).select_from(table)) or 0)


def lifecycle_counts(engine: Engine) -> dict[str, int]:
    return {table.name: count(engine, table) for table in LIFECYCLE_TABLES}


def assert_charged_failure_projections(
    engine: Engine,
    receipt: dict[str, Any],
    receipt_bytes: bytes,
) -> None:
    """Compare every resumed 005 projection with the pre-crash receipt."""

    expected_calls = {call["symbol"]: call for call in receipt["calls"]}
    failed = expected_calls["PLAB"]
    expected_raw_ids = {
        snapshot_id
        for call in receipt["calls"]
        for snapshot_id in call["transmission_snapshot_ids"]
    }
    transmission_hashes = receipt["pinned_replay_inputs"]["transmission_content_sha256"]
    expected_reserved = sum(
        (Decimal(call["reserved_usd"]) for call in receipt["calls"]), Decimal(0)
    ).quantize(Decimal("0.000001"))
    expected_actual = sum(
        (Decimal(call["actual_usd"]) for call in receipt["calls"]), Decimal(0)
    ).quantize(Decimal("0.000001"))
    with engine.connect() as connection:
        sources = {
            row.snapshot_id: row
            for row in connection.execute(
                select(
                    source_snapshots.c.snapshot_id,
                    source_snapshots.c.content_sha256,
                    source_snapshots.c.validation_status,
                )
            )
        }
        registered_receipts = {
            row.source_snapshot_id: row
            for row in connection.execute(select(collection_run_receipts))
        }
        usage = {
            row.metric: row.quantity
            for row in connection.execute(
                select(collection_usage_records.c.metric, collection_usage_records.c.quantity)
            )
        }
        failed_event = connection.execute(
            select(collection_run_events.c.details_json).where(
                collection_run_events.c.event_type == "attempt_failed"
            )
        ).scalar_one()
    assert set(sources) == expected_raw_ids
    assert sources[failed["raw_snapshot_id"]].validation_status == "BLOCKED"
    for raw_id in expected_raw_ids:
        assert sources[raw_id].content_sha256 == transmission_hashes[raw_id]
        assert registered_receipts[raw_id].byte_count > 0
        assert registered_receipts[raw_id].receipt_sha256 == sha256_hex(receipt_bytes)
    for call in expected_calls.values():
        if call["raw_snapshot_id"] is not None:
            assert registered_receipts[call["raw_snapshot_id"]].row_count == call["row_count"]
    assert usage["provider_cost_reserved_usd"] == expected_reserved
    assert usage["provider_cost_actual_usd"] == expected_actual
    assert failed_event["per_symbol"] == {
        symbol: {
            "attempts": call["attempts"],
            "error_class": call["error_class"],
            "row_count": call["row_count"],
            "status": call["status"],
        }
        for symbol, call in expected_calls.items()
    }


def test_executable_path_persists_the_whole_005_lifecycle(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    result = collect(tmp_path)

    register(clean_postgres, result, "snap-life-001")

    assert count(clean_postgres, collection_run_plans) == 1
    assert count(clean_postgres, collection_runs) == 1
    assert count(clean_postgres, source_snapshots) == EXPECTED_BASELINE_TRANSMISSIONS
    assert count(clean_postgres, collection_run_receipts) == EXPECTED_BASELINE_TRANSMISSIONS
    assert count(clean_postgres, collection_usage_records) == EXPECTED_USAGE_RECORDS
    assert count(clean_postgres, collection_run_events) > 0


def test_receipts_reference_the_registered_source_snapshots(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    result = collect(tmp_path)

    register(clean_postgres, result, "snap-life-001")

    with clean_postgres.connect() as connection:
        referenced = set(
            connection.execute(select(collection_run_receipts.c.source_snapshot_id)).scalars()
        )
        registered = set(connection.execute(select(source_snapshots.c.snapshot_id)).scalars())
    assert referenced
    assert referenced <= registered
    assert referenced == {
        snapshot_id
        for outcome in result.outcomes
        for snapshot_id in outcome.transmission_snapshot_ids
    }


def test_retry_transmissions_are_all_registered_with_receipts(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    """Every retry response remains queryable through both 003 and 005."""

    identity = make_identity_export({"AAPL": ["INST-AAPL"], "PLAB": []})
    offline, capability = synthetic_pair(
        RecordingTransport(
            "baseline",
            error_statuses={"PLAB": [HTTP_INTERNAL_SERVER_ERROR]},
        )
    )
    result = collect_snapshot(
        snapshot_id="snap-life-retry-001",
        config=make_config(
            identity_export_sha256=identity.export_sha256,
            identity_as_of=identity.as_of_utc,
        ),
        credential=SYNTHETIC_CREDENTIAL,
        transport=offline,
        raw_store_root=tmp_path / "raw",
        dataset_root=tmp_path / "dataset",
        identity_export=identity,
        gate_evidence=make_gate_evidence(tmp_path / "gates"),
        observed_at=OBSERVED_AT,
        sleeper=no_sleep,
        synthetic=capability,
    )
    expected = {
        snapshot_id
        for outcome in result.outcomes
        for snapshot_id in outcome.transmission_snapshot_ids
    }

    register(clean_postgres, result, "snap-life-retry-001")

    with clean_postgres.connect() as connection:
        sources = {
            row.snapshot_id: row.validation_status
            for row in connection.execute(
                select(
                    source_snapshots.c.snapshot_id,
                    source_snapshots.c.validation_status,
                )
            )
        }
        receipts = set(
            connection.execute(select(collection_run_receipts.c.source_snapshot_id)).scalars()
        )
    assert set(sources) == expected
    assert receipts == expected
    assert set(sources.values()) == {"BLOCKED", "PASS"}


def test_registering_the_same_snapshot_twice_is_idempotent(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    """A retried registration must not fork or duplicate durable evidence."""

    result = collect(tmp_path)
    register(clean_postgres, result, "snap-life-001")
    first_events = count(clean_postgres, collection_run_events)

    register(clean_postgres, result, "snap-life-001")

    assert count(clean_postgres, collection_run_plans) == 1
    assert count(clean_postgres, collection_runs) == 1
    assert count(clean_postgres, source_snapshots) == EXPECTED_BASELINE_TRANSMISSIONS
    assert count(clean_postgres, collection_run_receipts) == EXPECTED_BASELINE_TRANSMISSIONS
    assert count(clean_postgres, collection_usage_records) == EXPECTED_USAGE_RECORDS
    assert count(clean_postgres, collection_run_events) == first_events


def test_terminal_event_is_appended_only_after_all_evidence(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    result = collect(tmp_path)
    original = CollectionRegistry.append_event
    observed_terminal = False

    def assert_evidence_then_append(
        registry: CollectionRegistry,
        event: CollectionRunEvent,
        *,
        precommit_guard: Callable[[], None] | None = None,
        connection: Connection | None = None,
    ) -> object:
        nonlocal observed_terminal
        if event.event_type in {
            RunEventType.RUN_SUCCEEDED,
            RunEventType.RUN_FAILED,
        }:
            observed_terminal = True
            assert count(clean_postgres, source_snapshots) == EXPECTED_BASELINE_TRANSMISSIONS
            assert count(clean_postgres, collection_run_receipts) == (
                EXPECTED_BASELINE_TRANSMISSIONS
            )
            assert count(clean_postgres, collection_usage_records) == EXPECTED_USAGE_RECORDS
        return original(
            registry,
            event,
            precommit_guard=precommit_guard,
            connection=connection,
        )

    monkeypatch.setattr(CollectionRegistry, "append_event", assert_evidence_then_append)

    register(clean_postgres, result, "snap-life-terminal-last")

    assert observed_terminal is True


def test_stale_empty_receipt_reservation_is_reclaimed_without_another_owner_action(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot_id = "snap-stale-reservation-001"
    arguments, outside, identity_digest, gate_digest, authority_environ, _standing = cli_arguments(
        tmp_path, snapshot_id, monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    outside.mkdir(parents=True, exist_ok=True)
    (outside / "receipt.json").touch(mode=0o600)
    transport = RecordingTransport("baseline")

    code = cli_main(
        arguments,
        environ=_live_environ(
            authority_environ,
            AAS_DATABASE_URL=clean_postgres.url.render_as_string(hide_password=False),
        ),
        transport=transport,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
        now=AUTHORITY_NOW,
    )

    assert code == 0
    assert transport.calls == ["AAPL", "PLAB"]
    assert (outside / "receipt.json").stat().st_size > 0


def test_usage_records_reconcile_reserved_and_actual_cost(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    result = collect(tmp_path)

    register(clean_postgres, result, "snap-life-001")

    with clean_postgres.connect() as connection:
        rows = {
            row.metric: row.quantity
            for row in connection.execute(
                select(collection_usage_records.c.metric, collection_usage_records.c.quantity)
            )
        }
    assert set(rows) == {"provider_cost_reserved_usd", "provider_cost_actual_usd"}
    assert rows["provider_cost_actual_usd"] == Decimal("0.002500")
    assert rows["provider_cost_actual_usd"] <= rows["provider_cost_reserved_usd"]


def test_live_cli_path_registers_the_lifecycle_and_emits_a_receipt(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The executable path proves registration, receipt, and commit together."""

    arguments, outside, identity_digest, gate_digest, authority_environ, _standing = cli_arguments(
        tmp_path, "snap-cli-live-001", monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    stdout = io.StringIO()
    stderr = io.StringIO()

    code = cli_main(
        arguments,
        environ=_live_environ(
            authority_environ,
            AAS_DATABASE_URL=clean_postgres.url.render_as_string(hide_password=False),
        ),
        transport=RecordingTransport("baseline"),
        stdout=stdout,
        stderr=stderr,
        now=AUTHORITY_NOW,
    )

    assert code == 0, stderr.getvalue()
    summary = json.loads(stdout.getvalue())
    assert summary["collection_lifecycle"] == "REGISTERED"
    assert summary["published"] is True
    receipt = json.loads((outside / "receipt.json").read_text(encoding="utf-8"))
    assert receipt["task_id"] == "AAS-DATA-008C"
    assert receipt["eligibility"] == {
        "backtest": False,
        "canonical": False,
        "order": False,
        "paper": False,
    }
    assert committed_snapshot_ids(outside / "dataset") == ("snap-cli-live-001",)
    assert count(clean_postgres, collection_runs) == 1
    assert count(clean_postgres, source_snapshots) == EXPECTED_BASELINE_TRANSMISSIONS


def test_blank_schema_blocks_before_any_provider_call(
    monkeypatch: pytest.MonkeyPatch,
    postgres_engine: Engine,
    tmp_path: Path,
) -> None:
    """A reachable but unmigrated database must not reach the transport."""

    arguments, outside, identity_digest, gate_digest, authority_environ, _standing = cli_arguments(
        tmp_path, "snap-blank-001", monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    # Point the run at an empty schema: reachable, correctly credentialed, but
    # never migrated. This is the blank-database case the finding describes.
    with postgres_engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS blank_probe"))
    blank_url = postgres_engine.url.render_as_string(hide_password=False)
    blank_url = f"{blank_url}?options=-csearch_path%3Dblank_probe"
    transport = RecordingTransport("baseline")
    stderr = io.StringIO()

    code = cli_main(
        arguments,
        environ=_live_environ(authority_environ, AAS_DATABASE_URL=blank_url),
        transport=transport,
        stdout=io.StringIO(),
        stderr=stderr,
        now=AUTHORITY_NOW,
    )

    assert code == PRECONDITION_EXIT
    assert "schema is not migrated" in stderr.getvalue()
    assert transport.calls == []
    assert not outside.exists()


def test_registered_receipt_hash_always_has_its_exact_artifact(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The emitted artifact must hash to exactly what registration recorded."""

    arguments, outside, identity_digest, gate_digest, authority_environ, _standing = cli_arguments(
        tmp_path, "snap-receipt-001", monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    stdout = io.StringIO()

    code = cli_main(
        arguments,
        environ=_live_environ(
            authority_environ,
            AAS_DATABASE_URL=clean_postgres.url.render_as_string(hide_password=False),
        ),
        transport=RecordingTransport("baseline"),
        stdout=stdout,
        stderr=io.StringIO(),
        now=AUTHORITY_NOW,
    )

    assert code == 0
    artifact = (outside / "receipt.json").read_bytes()
    assert artifact, "the receipt artifact must not be empty"
    reported = json.loads(stdout.getvalue())["receipt_sha256"]
    assert sha256_hex(artifact) == reported
    with clean_postgres.connect() as connection:
        registered = set(
            connection.execute(select(collection_run_receipts.c.receipt_sha256)).scalars()
        )
    assert registered == {reported}


def cli_arguments(
    tmp_path: Path,
    snapshot_id: str,
    monkeypatch: pytest.MonkeyPatch | None = None,
) -> tuple[list[str], Path, str, str, dict[str, str], StandingAuthorityFixture]:
    """Build a full live-CLI argument vector plus its evidence digests."""

    identity_path, identity_digest = write_identity_export(
        tmp_path / "evidence", {"AAPL": ["INST-AAPL"], "PLAB": []}
    )
    gate_path, gate_digest = write_gate_evidence(tmp_path / "evidence")
    outside = tmp_path / "outside"
    universe = tmp_path / "universe.txt"
    universe.write_text("AAPL\nPLAB\n", encoding="utf-8")
    fixture = make_standing_authority(tmp_path / "authority", destinations=outside)
    if monkeypatch is not None:
        monkeypatch.setattr(
            cli_module,
            "load_owner_authority",
            lambda *_args, **_kwargs: fixture.owner_authority,
        )
    return (
        [
            "--live",
            "--snapshot-id",
            snapshot_id,
            "--universe-file",
            str(universe),
            "--raw-store-root",
            str(outside / "raw"),
            "--dataset-root",
            str(outside / "dataset"),
            "--receipt-path",
            str(outside / "receipt.json"),
            "--budget-usd",
            "0.10",
            "--identity-export",
            str(identity_path),
            "--gate-evidence",
            str(gate_path),
            *authority_argv(fixture),
        ],
        outside,
        identity_digest,
        gate_digest,
        {OWNER_AUTHORITY_ENV: str(fixture.owner_authority_path)},
        fixture,
    )


def _live_environ(authority_environ: Mapping[str, str], **extra: str) -> dict[str, str]:
    environ = {
        CREDENTIAL_ENVIRONMENT_VARIABLE: SYNTHETIC_CREDENTIAL,
        **authority_environ,
    }
    environ.update(extra)
    return environ


def argument_value(arguments: list[str], flag: str) -> str:
    return arguments[arguments.index(flag) + 1]


def test_denied_downstream_writes_block_before_any_provider_call(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A role that may write plans but not receipts must not reach transport."""

    arguments, outside, identity_digest, gate_digest, authority_environ, _standing = cli_arguments(
        tmp_path, "snap-denied-001", monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    role = f"restricted_{uuid4().hex[:12]}"
    with clean_postgres.begin() as connection:
        connection.execute(text(f"CREATE ROLE \"{role}\" LOGIN PASSWORD 'probe_only'"))
        connection.execute(text(f'GRANT USAGE ON SCHEMA public TO "{role}"'))
        connection.execute(text(f'GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA public TO "{role}"'))
        # Deny exactly one downstream table the lifecycle must write.
        connection.execute(text(f'REVOKE INSERT ON collection_run_receipts FROM "{role}"'))
        connection.execute(
            text(f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{role}"')
        )
    restricted_url = clean_postgres.url.set(username=role, password=RESTRICTED_ROLE_SECRET)
    transport = RecordingTransport("baseline")
    stderr = io.StringIO()

    try:
        code = cli_main(
            arguments,
            environ=_live_environ(
                authority_environ,
                AAS_DATABASE_URL=restricted_url.render_as_string(hide_password=False),
            ),
            transport=transport,
            stdout=io.StringIO(),
            stderr=stderr,
            now=AUTHORITY_NOW,
        )
    finally:
        with clean_postgres.begin() as connection:
            connection.execute(text(f'REVOKE ALL ON ALL TABLES IN SCHEMA public FROM "{role}"'))
            connection.execute(text(f'REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM "{role}"'))
            connection.execute(text(f'REVOKE ALL ON SCHEMA public FROM "{role}"'))
            connection.execute(text(f'DROP ROLE "{role}"'))

    assert code == PRECONDITION_EXIT
    assert "not writable" in stderr.getvalue()
    assert transport.calls == []
    assert not outside.exists()


def test_crash_between_receipt_and_registration_is_resumable(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Fault-inject the exact crash window, then prove a rerun completes it.

    The first invocation publishes a durable receipt and snapshot, then dies
    before lifecycle registration. The rerun must finish the database lifecycle
    from the verified existing artifacts, make zero additional provider calls,
    and append no duplicate rows.
    """

    arguments, outside, identity_digest, gate_digest, authority_environ, standing = cli_arguments(
        tmp_path, "snap-crash-001", monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    environ = _live_environ(
        authority_environ,
        AAS_DATABASE_URL=clean_postgres.url.render_as_string(hide_password=False),
    )

    class CrashAfterReceiptError(RuntimeError):
        pass

    def crash(*_args: object, **_kwargs: object) -> str:
        raise CrashAfterReceiptError("simulated crash before lifecycle registration")

    monkeypatch.setattr(cli_module, "_register_lifecycle", crash)
    first_transport = RecordingTransport("baseline")
    with pytest.raises(CrashAfterReceiptError):
        cli_main(
            arguments,
            environ=environ,
            transport=first_transport,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            now=AUTHORITY_NOW,
        )

    # The crash window: durable receipt and published snapshot, no lifecycle.
    assert (outside / "receipt.json").read_bytes()
    assert committed_snapshot_ids(outside / "dataset") == ("snap-crash-001",)
    assert first_transport.calls == ["AAPL", "PLAB"]
    assert count(clean_postgres, collection_runs) == 0

    monkeypatch.undo()
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    monkeypatch.setattr(
        cli_module,
        "load_owner_authority",
        lambda *_args, **_kwargs: standing.owner_authority,
    )
    resume_transport = RecordingTransport("baseline")
    stdout = io.StringIO()

    code = cli_main(
        arguments,
        environ=environ,
        transport=resume_transport,
        stdout=stdout,
        stderr=io.StringIO(),
        now=AUTHORITY_NOW,
    )

    assert code == 0
    assert json.loads(stdout.getvalue())["collection_lifecycle"] == "RESUMED"
    assert resume_transport.calls == [], "a resume must make zero provider calls"
    assert count(clean_postgres, collection_runs) == 1
    assert count(clean_postgres, collection_run_receipts) == EXPECTED_BASELINE_TRANSMISSIONS
    assert count(clean_postgres, source_snapshots) == EXPECTED_BASELINE_TRANSMISSIONS
    assert count(clean_postgres, collection_usage_records) == EXPECTED_USAGE_RECORDS


@pytest.mark.parametrize(
    "mismatch",
    ["universe", "budget", "identity", "collector_version"],
)
def test_recovery_rejects_changed_pinned_inputs_without_calls_or_rows(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    mismatch: str,
) -> None:
    """A new CLI invocation cannot rewrite the metadata of the crashed run."""

    snapshot_id = f"snap-mismatch-{mismatch}"
    arguments, outside, identity_digest, gate_digest, authority_environ, standing = cli_arguments(
        tmp_path, snapshot_id, monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    environ = _live_environ(
        authority_environ,
        AAS_DATABASE_URL=clean_postgres.url.render_as_string(hide_password=False),
    )

    def crash(*_args: object, **_kwargs: object) -> str:
        raise RuntimeError("simulated receipt-before-registration crash")

    monkeypatch.setattr(cli_module, "_register_lifecycle", crash)
    with pytest.raises(RuntimeError, match="receipt-before-registration"):
        cli_main(
            arguments,
            environ=environ,
            transport=RecordingTransport("baseline"),
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            now=AUTHORITY_NOW,
        )
    assert (outside / "receipt.json").is_file()
    assert count(clean_postgres, collection_runs) == 0

    monkeypatch.undo()
    if mismatch == "universe":
        Path(argument_value(arguments, "--universe-file")).write_text("AAPL\n", encoding="utf-8")
    elif mismatch == "budget":
        arguments[arguments.index("--budget-usd") + 1] = "0.09"
    elif mismatch == "identity":
        identity_path, identity_digest = write_identity_export(
            tmp_path / "changed-evidence",
            {"AAPL": ["INST-CHANGED"], "PLAB": []},
        )
        arguments[arguments.index("--identity-export") + 1] = str(identity_path)
    else:
        arguments.extend(("--collector-code-version", "changed-after-crash"))
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    monkeypatch.setattr(
        cli_module,
        "load_owner_authority",
        lambda *_args, **_kwargs: standing.owner_authority,
    )
    transport = RecordingTransport("baseline")
    stderr = io.StringIO()

    code = cli_main(
        arguments,
        environ=environ,
        transport=transport,
        stdout=io.StringIO(),
        stderr=stderr,
        now=AUTHORITY_NOW,
    )

    assert code == PRECONDITION_EXIT
    assert "recovery inputs do not match the receipt" in stderr.getvalue()
    assert transport.calls == []
    assert count(clean_postgres, collection_run_plans) == 0
    assert count(clean_postgres, collection_runs) == 0
    assert count(clean_postgres, collection_run_events) == 0
    assert count(clean_postgres, collection_run_receipts) == 0
    assert count(clean_postgres, collection_usage_records) == 0
    assert count(clean_postgres, source_snapshots) == 0


def test_recovery_rejects_a_canonical_but_semantically_rewritten_receipt(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The raw-store anchor, not rewritten receipt bytes, controls recovery."""

    arguments, outside, identity_digest, gate_digest, authority_environ, standing = cli_arguments(
        tmp_path, "snap-noncanonical-001", monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    environ = _live_environ(
        authority_environ,
        AAS_DATABASE_URL=clean_postgres.url.render_as_string(hide_password=False),
    )

    def crash(*_args: object, **_kwargs: object) -> str:
        raise RuntimeError("simulated receipt-before-registration crash")

    monkeypatch.setattr(cli_module, "_register_lifecycle", crash)
    with pytest.raises(RuntimeError):
        cli_main(
            arguments,
            environ=environ,
            transport=RecordingTransport("baseline"),
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            now=AUTHORITY_NOW,
        )
    receipt_path = outside / "receipt.json"
    rewritten = json.loads(receipt_path.read_bytes())
    rewritten["publication"]["published"] = False
    rewritten["cost"]["provider_reported_total_usd"] = "0.000000"
    receipt_path.write_bytes(canonical_json_bytes(rewritten))

    monkeypatch.undo()
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    monkeypatch.setattr(
        cli_module,
        "load_owner_authority",
        lambda *_args, **_kwargs: standing.owner_authority,
    )
    transport = RecordingTransport("baseline")
    stderr = io.StringIO()
    code = cli_main(
        arguments,
        environ=environ,
        transport=transport,
        stdout=io.StringIO(),
        stderr=stderr,
        now=AUTHORITY_NOW,
    )

    assert code == PRECONDITION_EXIT
    assert "does not match its immutable raw-store anchor" in stderr.getvalue()
    assert transport.calls == []
    assert count(clean_postgres, collection_runs) == 0
    assert count(clean_postgres, source_snapshots) == 0


def test_charged_failed_run_resumes_losslessly_without_another_call(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Recover a charged partial run from receipt and BLOCKED raw evidence."""

    snapshot_id = "snap-charged-failure-001"
    arguments, outside, identity_digest, gate_digest, authority_environ, standing = cli_arguments(
        tmp_path, snapshot_id, monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    environ = _live_environ(
        authority_environ,
        AAS_DATABASE_URL=clean_postgres.url.render_as_string(hide_password=False),
    )

    def crash(*_args: object, **_kwargs: object) -> str:
        raise RuntimeError("simulated receipt-before-registration crash")

    monkeypatch.setattr(cli_module, "_register_lifecycle", crash)
    first_transport = RecordingTransport(
        "baseline",
        error_statuses={"PLAB": [400]},
        error_cost=0.002,
    )
    with pytest.raises(RuntimeError, match="receipt-before-registration"):
        cli_main(
            arguments,
            environ=environ,
            transport=first_transport,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            now=AUTHORITY_NOW,
        )

    receipt_path = outside / "receipt.json"
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    assert receipt_bytes == canonical_json_bytes(receipt)
    assert receipt["publication"]["published"] is False
    failed = next(call for call in receipt["calls"] if call["symbol"] == "PLAB")
    assert failed["status"] == "FAILED"
    assert Decimal(failed["actual_usd"]) == Decimal("0.002")
    assert Decimal(failed["reserved_usd"]) > Decimal(failed["actual_usd"])
    assert failed["byte_count"] > 0
    assert failed["content_sha256"]
    assert failed["raw_snapshot_id"] in failed["transmission_snapshot_ids"]
    assert committed_snapshot_ids(outside / "dataset") == ()
    assert count(clean_postgres, collection_runs) == 0

    monkeypatch.undo()
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    monkeypatch.setattr(
        cli_module,
        "load_owner_authority",
        lambda *_args, **_kwargs: standing.owner_authority,
    )
    resume_transport = RecordingTransport("baseline")
    stdout = io.StringIO()
    code = cli_main(
        arguments,
        environ=environ,
        transport=resume_transport,
        stdout=stdout,
        stderr=io.StringIO(),
        now=AUTHORITY_NOW,
    )

    summary = json.loads(stdout.getvalue())
    assert code == 0
    assert summary["collection_lifecycle"] == "RESUMED"
    assert summary["published"] is False
    assert resume_transport.calls == []
    assert_charged_failure_projections(clean_postgres, receipt, receipt_bytes)

    counts_before = lifecycle_counts(clean_postgres)
    repeated_transport = RecordingTransport("baseline")
    repeated_code = cli_main(
        arguments,
        environ=environ,
        transport=repeated_transport,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
        now=AUTHORITY_NOW,
    )
    assert repeated_code == PRECONDITION_EXIT
    assert repeated_transport.calls == []
    assert lifecycle_counts(clean_postgres) == counts_before


@pytest.mark.parametrize(
    "stage",
    [
        ("collection", "register_plan", 1),
        ("collection", "start_run", 1),
        ("metadata", "register_source_snapshot", 1),
        ("collection", "append_event", 1),
        ("collection", "append_event", 2),
        ("collection", "record_receipt", 1),
        ("collection", "record_usage", 1),
    ],
)
def test_every_partial_lifecycle_commit_is_resumable(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    stage: tuple[str, str, int],
) -> None:
    """A crash after any independently committed stage converges on retry."""

    owner_name, method_name, occurrence = stage
    snapshot_id = f"snap-mid-{owner_name}-{method_name}-{occurrence}"
    arguments, _, identity_digest, gate_digest, authority_environ, standing = cli_arguments(
        tmp_path, snapshot_id, monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    environ = _live_environ(
        authority_environ,
        AAS_DATABASE_URL=clean_postgres.url.render_as_string(hide_password=False),
    )
    owner = CollectionRegistry if owner_name == "collection" else MetadataRegistry
    original = getattr(owner, method_name)
    calls = 0

    def commit_then_crash(self: object, *args: object, **kwargs: object) -> object:
        nonlocal calls
        result = original(self, *args, **kwargs)
        calls += 1
        if calls == occurrence:
            raise RuntimeError(f"simulated crash after {method_name} commit {occurrence}")
        return result

    monkeypatch.setattr(owner, method_name, commit_then_crash)
    first_transport = RecordingTransport("baseline")
    with pytest.raises(RuntimeError, match="simulated crash after"):
        cli_main(
            arguments,
            environ=environ,
            transport=first_transport,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            now=AUTHORITY_NOW,
        )
    assert first_transport.calls == ["AAPL", "PLAB"]
    partial_counts = lifecycle_counts(clean_postgres)
    assert 0 < sum(partial_counts.values()) < EXPECTED_COMPLETE_PROJECTION_ROWS

    monkeypatch.undo()
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    monkeypatch.setattr(
        cli_module,
        "load_owner_authority",
        lambda *_args, **_kwargs: standing.owner_authority,
    )
    resume_transport = RecordingTransport("baseline")
    stdout = io.StringIO()
    code = cli_main(
        arguments,
        environ=environ,
        transport=resume_transport,
        stdout=stdout,
        stderr=io.StringIO(),
        now=AUTHORITY_NOW,
    )

    assert code == 0
    assert json.loads(stdout.getvalue())["collection_lifecycle"] == "RESUMED"
    assert resume_transport.calls == []
    assert lifecycle_counts(clean_postgres) == {
        "collection_run_plans": 1,
        "collection_runs": 1,
        "collection_run_events": 3,
        "collection_run_receipts": EXPECTED_BASELINE_TRANSMISSIONS,
        "collection_usage_records": EXPECTED_USAGE_RECORDS,
        "source_snapshots": EXPECTED_BASELINE_TRANSMISSIONS,
    }
    state = CollectionRegistry(clean_postgres).current_run_state(f"{snapshot_id}.run")
    assert state is not None
    assert state.terminal

    counts_before = lifecycle_counts(clean_postgres)
    repeated_transport = RecordingTransport("baseline")
    repeated_code = cli_main(
        arguments,
        environ=environ,
        transport=repeated_transport,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
        now=AUTHORITY_NOW,
    )
    assert repeated_code == PRECONDITION_EXIT
    assert repeated_transport.calls == []
    assert lifecycle_counts(clean_postgres) == counts_before


def test_a_completed_snapshot_is_never_re_registered(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Rerunning a fully completed snapshot appends nothing and fails closed."""

    arguments, _, identity_digest, gate_digest, authority_environ, _standing = cli_arguments(
        tmp_path, "snap-done-001", monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    environ = _live_environ(
        authority_environ,
        AAS_DATABASE_URL=clean_postgres.url.render_as_string(hide_password=False),
    )
    assert (
        cli_main(
            arguments,
            environ=environ,
            transport=RecordingTransport("baseline"),
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            now=AUTHORITY_NOW,
        )
        == 0
    )
    before = count(clean_postgres, collection_run_events)
    transport = RecordingTransport("baseline")
    stderr = io.StringIO()

    code = cli_main(
        arguments,
        environ=environ,
        transport=transport,
        stdout=io.StringIO(),
        stderr=stderr,
        now=AUTHORITY_NOW,
    )

    assert code == PRECONDITION_EXIT
    assert "already registered" in stderr.getvalue()
    assert transport.calls == []
    assert count(clean_postgres, collection_run_events) == before


def test_expired_scope_zero_call_recovery_completes_while_fresh_admission_fails(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verified zero-transport recovery is not blocked by standing-scope expiry."""

    arguments, outside, identity_digest, gate_digest, authority_environ, standing = cli_arguments(
        tmp_path, "snap-zero-call-recovery-001", monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    environ = _live_environ(
        authority_environ,
        AAS_DATABASE_URL=clean_postgres.url.render_as_string(hide_password=False),
    )

    class CrashAfterReceiptError(RuntimeError):
        pass

    def crash(*_args: object, **_kwargs: object) -> str:
        raise CrashAfterReceiptError("simulated crash before lifecycle registration")

    monkeypatch.setattr(cli_module, "_register_lifecycle", crash)
    first_transport = RecordingTransport("baseline")
    with pytest.raises(CrashAfterReceiptError):
        cli_main(
            arguments,
            environ=environ,
            transport=first_transport,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            now=AUTHORITY_NOW,
        )
    assert first_transport.calls == ["AAPL", "PLAB"]
    assert (outside / "receipt.json").read_bytes()
    assert count(clean_postgres, collection_runs) == 0

    monkeypatch.undo()
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    monkeypatch.setattr(
        cli_module,
        "load_owner_authority",
        lambda *_args, **_kwargs: standing.owner_authority,
    )
    resume_transport = RecordingTransport("baseline")
    stdout = io.StringIO()
    expired = standing.owner_authority.valid_until_utc

    code = cli_main(
        arguments,
        environ=environ,
        transport=resume_transport,
        stdout=stdout,
        stderr=io.StringIO(),
        now=expired,
    )

    assert code == 0
    assert json.loads(stdout.getvalue())["collection_lifecycle"] == "RESUMED"
    assert resume_transport.calls == []
    assert count(clean_postgres, collection_runs) == 1

    fresh_snapshot = "snap-fresh-admission-001"
    fresh_arguments = list(arguments)
    fresh_arguments[fresh_arguments.index("--snapshot-id") + 1] = fresh_snapshot
    fresh_arguments[fresh_arguments.index("--receipt-path") + 1] = str(
        tmp_path / "fresh" / "receipt.json"
    )
    fresh_transport = RecordingTransport("baseline")
    stderr = io.StringIO()
    fresh_code = cli_main(
        fresh_arguments,
        environ=environ,
        transport=fresh_transport,
        stdout=io.StringIO(),
        stderr=stderr,
        now=expired,
    )

    assert fresh_code == PRECONDITION_EXIT
    assert (
        "signing key is expired" in stderr.getvalue() or "not currently valid" in stderr.getvalue()
    )
    assert fresh_transport.calls == []


def test_revoked_scope_zero_call_recovery_completes_while_fresh_admission_fails(
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A mid-crash revocation must not strand verified zero-call recovery."""

    arguments, _outside, identity_digest, gate_digest, authority_environ, standing = cli_arguments(
        tmp_path, "snap-revoked-recovery-001", monkeypatch
    )
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    environ = _live_environ(
        authority_environ,
        AAS_DATABASE_URL=clean_postgres.url.render_as_string(hide_password=False),
    )

    class CrashAfterReceiptError(RuntimeError):
        pass

    def crash(*_args: object, **_kwargs: object) -> str:
        raise CrashAfterReceiptError("simulated crash before lifecycle registration")

    monkeypatch.setattr(cli_module, "_register_lifecycle", crash)
    first_transport = RecordingTransport("baseline")
    with pytest.raises(CrashAfterReceiptError):
        cli_main(
            arguments,
            environ=environ,
            transport=first_transport,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            now=AUTHORITY_NOW,
        )
    assert first_transport.calls == ["AAPL", "PLAB"]

    verified = verify_recurring_authority(
        standing.payload, standing.signature, standing.owner_authority, now=AUTHORITY_NOW
    )
    write_revocation(verified, AUTHORITY_NOW)

    monkeypatch.undo()
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))
    monkeypatch.setattr(
        cli_module,
        "load_owner_authority",
        lambda *_args, **_kwargs: standing.owner_authority,
    )
    resume_transport = RecordingTransport("baseline")
    stdout = io.StringIO()

    code = cli_main(
        arguments,
        environ=environ,
        transport=resume_transport,
        stdout=stdout,
        stderr=io.StringIO(),
        now=AUTHORITY_NOW,
    )

    assert code == 0
    assert json.loads(stdout.getvalue())["collection_lifecycle"] == "RESUMED"
    assert resume_transport.calls == []
    assert count(clean_postgres, collection_runs) == 1

    fresh_snapshot = "snap-fresh-revoked-001"
    fresh_arguments = list(arguments)
    fresh_arguments[fresh_arguments.index("--snapshot-id") + 1] = fresh_snapshot
    fresh_arguments[fresh_arguments.index("--receipt-path") + 1] = str(
        tmp_path / "fresh-revoked" / "receipt.json"
    )
    fresh_transport = RecordingTransport("baseline")
    stderr = io.StringIO()
    fresh_code = cli_main(
        fresh_arguments,
        environ=environ,
        transport=fresh_transport,
        stdout=io.StringIO(),
        stderr=stderr,
        now=AUTHORITY_NOW,
    )

    assert fresh_code == PRECONDITION_EXIT
    assert "revoked" in stderr.getvalue()
    assert fresh_transport.calls == []
