from __future__ import annotations

import threading
import time
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import Connection, Engine, event, text

from aegis_alpha.collection.fmp_usage_checkpoint import (
    FmpUsageCheckpointService,
    VerifiedProviderUsageSnapshot,
)
from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunPlan,
    CollectionUsageRecord,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import (
    collection_run_plans,
    collection_runs,
    collection_usage_checkpoints,
    collection_usage_records,
)
from aegis_alpha.collection.usage_checkpoint_crypto import (
    Ed25519PublicKeyring,
    TrustedEd25519PublicKey,
)
from aegis_alpha.collection.usage_checkpoint_errors import (
    ProviderUsageIntegrityError,
    SignatureVerificationError,
    StaleVerificationKeyError,
    UnknownVerificationKeyError,
)
from aegis_alpha.collection.usage_checkpoint_schema import SignedUsageCheckpoint
from aegis_alpha.data.fmp_rate_types import TrustedUsageSnapshot
from aegis_alpha.data.fmp_usage_trust import (
    UsageExtensionContext,
    extend_trusted_fmp_usage_snapshot,
)

_START = datetime(2026, 8, 17, tzinfo=UTC)
_END = datetime(2026, 8, 18, tzinfo=UTC)
_GENERATED = _END + timedelta(minutes=1)
_AUTHORITY = "aas-usage-authority-v1"
_KEY_ID = "ed25519:2026-08-18"


@pytest.fixture
def collection_registry(clean_postgres: Engine) -> Iterator[CollectionRegistry]:
    registry = CollectionRegistry(clean_postgres)
    yield registry
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


def _seed_run(
    registry: CollectionRegistry,
    run_id: str,
    *,
    provider: str = "fmp",
) -> CollectionRunPlan:
    plan = CollectionRunPlan(
        plan_id=f"plan-{run_id}",
        schema_version=1,
        provider=provider,
        dataset="usage-test",
        mode=CollectionMode.PROBE,
        requested_window_start=None,
        requested_window_end=None,
        parameters={"run": run_id},
        created_at_utc=_START - timedelta(days=1),
    )
    registry.register_plan(plan)
    registry.start_run(
        CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=plan.created_at_utc)
    )
    return plan


def _record(  # noqa: PLR0913 - concise persisted-row test builder
    registry: CollectionRegistry,
    run_id: str,
    usage_seq: int,
    *,
    quantity: str = "1",
    metric: str = "provider_requests",
    unit: str = "count",
    recorded_at_utc: datetime = _START + timedelta(hours=1),
) -> None:
    registry.record_usage(
        CollectionUsageRecord(
            run_id=run_id,
            usage_seq=usage_seq,
            metric=metric,
            quantity=Decimal(quantity),
            unit=unit,
            recorded_at_utc=recorded_at_utc,
            evidence={"test": True},
        )
    )


def _prepare_sign_register(
    registry: CollectionRegistry,
    private_key: Ed25519PrivateKey,
    *,
    checkpoint_id: str = "fmp-usage-20260817",
) -> Ed25519PublicKeyring:
    candidate = registry.prepare_fmp_usage_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        coverage_start_utc=_START,
        coverage_end_utc=_END,
        authority_id=_AUTHORITY,
        key_id=_KEY_ID,
        generated_at_utc=_GENERATED,
    )
    assert candidate.payload_bytes == candidate.checkpoint.canonical_bytes()
    signed = SignedUsageCheckpoint(
        candidate.checkpoint,
        private_key.sign(candidate.payload_bytes),
    )
    keyring = Ed25519PublicKeyring(
        {(_AUTHORITY, _KEY_ID): private_key.public_key().public_bytes_raw()}
    )
    registry.register_usage_checkpoint(signed, candidate.leaves, keyring)
    return keyring


def _verify(
    registry: CollectionRegistry,
    keyring: Ed25519PublicKeyring,
    checkpoint_id: str = "fmp-usage-20260817",
) -> VerifiedProviderUsageSnapshot:
    return registry.verify_fmp_usage_checkpoint(
        checkpoint_id=checkpoint_id,
        coverage_start_utc=_START,
        coverage_end_utc=_END,
        keyring=keyring,
    )


@contextmanager
def _checkpoint_follower_pid(engine: Engine, worker: threading.Thread) -> Iterator[dict[str, int]]:
    # Retain multiple backends so separate pool checkouts cannot masquerade as
    # the same checkpoint connection simply because this test runs alone.
    with ExitStack() as stack:
        connections = [stack.enter_context(engine.connect()) for _ in range(4)]
        pids = {connection.scalar(text("SELECT pg_backend_pid()")) for connection in connections}
        assert len(pids) == len(connections)

    follower: dict[str, int] = {}

    def capture_checkpoint_pid(
        connection: Connection,
        _cursor: object,
        statement: str,
        *_args: object,
    ) -> None:
        if (
            threading.current_thread() is worker
            and not follower
            and statement.startswith("SELECT pg_advisory_xact_lock(")
        ):
            # Sample the transaction that is about to wait, not a separate
            # pool checkout that can belong to an unrelated backend.
            follower["pid"] = connection.execute(text("SELECT pg_backend_pid()")).scalar_one()

    event.listen(engine, "before_cursor_execute", capture_checkpoint_pid)
    try:
        yield follower
    finally:
        event.remove(engine, "before_cursor_execute", capture_checkpoint_pid)


def _wait_for_blocked_checkpoint_follower(
    engine: Engine,
    follower: Mapping[str, int],
) -> int:
    """Poll until the checkpoint follower is blocked by the insert holder."""

    deadline = time.monotonic() + 10
    blocking = 0
    while time.monotonic() < deadline:
        pid = follower.get("pid")
        if pid is not None:
            with engine.connect() as connection:
                blocking = int(
                    connection.execute(
                        text("SELECT cardinality(pg_blocking_pids(:pid))"),
                        {"pid": pid},
                    ).scalar_one()
                )
            if blocking:
                break
        time.sleep(0.02)
    return blocking


def _checkpoint_baseline(floor_quantity: Decimal) -> TrustedUsageSnapshot:
    """Build the trusted baseline that a post-checkpoint extension must top up."""

    return TrustedUsageSnapshot(
        source="signed-checkpoint",
        recorded_at_utc=_END.isoformat(),
        integrity_sha256="a" * 64,
        authority_verified=True,
        calls_used_today=int(floor_quantity),
        bytes_used_30d=0,
    )


def _post_checkpoint_extension_delta(
    engine: Engine,
    raw_store_root: Path,
    baseline: TrustedUsageSnapshot,
) -> int:
    """Measure the trusted-snapshot extension delta over the signed checkpoint."""

    extended = extend_trusted_fmp_usage_snapshot(
        baseline,
        context=UsageExtensionContext(
            database_url=engine.url.render_as_string(hide_password=False),
            raw_store_root=raw_store_root,
        ),
        coverage_end_utc=_END,
        now=_END + timedelta(seconds=1),
    )
    return extended.calls_used_today - baseline.calls_used_today


def test_latest_complete_checkpoint_pair_can_precede_runtime_moment(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    private_key = Ed25519PrivateKey.generate()
    keyring = Ed25519PublicKeyring(
        {(_AUTHORITY, _KEY_ID): private_key.public_key().public_bytes_raw()}
    )
    checkpoint_end = _END + timedelta(hours=12)
    day_start = checkpoint_end.replace(hour=0, minute=0, second=0, microsecond=0)
    for checkpoint_id, coverage_start in (
        ("fmp-usage-rolling", checkpoint_end - timedelta(days=30)),
        ("fmp-usage-daily", day_start),
    ):
        candidate = collection_registry.prepare_fmp_usage_checkpoint_candidate(
            checkpoint_id=checkpoint_id,
            coverage_start_utc=coverage_start,
            coverage_end_utc=checkpoint_end,
            authority_id=_AUTHORITY,
            key_id=_KEY_ID,
            generated_at_utc=checkpoint_end + timedelta(minutes=1),
        )
        collection_registry.register_usage_checkpoint(
            SignedUsageCheckpoint(
                candidate.checkpoint,
                private_key.sign(candidate.payload_bytes),
            ),
            candidate.leaves,
            keyring,
        )

    rolling, daily = FmpUsageCheckpointService(clean_postgres).verify_latest_available(
        at_or_before_utc=checkpoint_end + timedelta(minutes=5),
        keyring=keyring,
    )

    assert rolling.coverage_start_utc == checkpoint_end - timedelta(days=30)
    assert daily.coverage_start_utc == day_start
    assert rolling.coverage_end_utc == daily.coverage_end_utc == checkpoint_end


def test_prepare_sign_register_verify_aggregates_only_fmp_lineage(
    collection_registry: CollectionRegistry,
) -> None:
    fmp_plan = _seed_run(collection_registry, "fmp-a")
    _record(collection_registry, "fmp-a", 2, quantity="2.25", metric="egress", unit="byte")
    _record(collection_registry, "fmp-a", 1, quantity="3")
    _seed_run(collection_registry, "sec-a", provider="sec")
    _record(collection_registry, "sec-a", 1, quantity="999")
    private_key = Ed25519PrivateKey.generate()

    candidate = collection_registry.prepare_fmp_usage_checkpoint_candidate(
        checkpoint_id="fmp-usage-20260817",
        coverage_start_utc=_START,
        coverage_end_utc=_END,
        authority_id=_AUTHORITY,
        key_id=_KEY_ID,
        generated_at_utc=_GENERATED,
    )

    assert [(leaf.run_id, leaf.usage_seq) for leaf in candidate.leaves] == [
        ("fmp-a", 1),
        ("fmp-a", 2),
    ]
    assert all(leaf.provider == "fmp" for leaf in candidate.leaves)
    assert all(leaf.plan_id == fmp_plan.plan_id for leaf in candidate.leaves)
    assert all(leaf.plan_sha256 == fmp_plan.plan_sha256 for leaf in candidate.leaves)
    signed = SignedUsageCheckpoint(candidate.checkpoint, private_key.sign(candidate.payload_bytes))
    keyring = Ed25519PublicKeyring(
        {(_AUTHORITY, _KEY_ID): private_key.public_key().public_bytes_raw()}
    )
    collection_registry.register_usage_checkpoint(signed, candidate.leaves, keyring)

    snapshot = _verify(collection_registry, keyring)

    assert isinstance(snapshot, VerifiedProviderUsageSnapshot)
    assert snapshot.provider == "fmp"
    assert snapshot.usage_record_count == len(candidate.leaves)
    assert snapshot.totals == {
        ("egress", "byte"): Decimal("2.250000"),
        ("provider_requests", "count"): Decimal("3.000000"),
    }


def test_half_open_window_and_empty_checkpoint_are_valid(
    collection_registry: CollectionRegistry,
) -> None:
    _seed_run(collection_registry, "fmp-edges")
    _record(collection_registry, "fmp-edges", 1, recorded_at_utc=_START)
    _record(collection_registry, "fmp-edges", 2, recorded_at_utc=_END)
    first_key = Ed25519PrivateKey.generate()
    keyring = _prepare_sign_register(collection_registry, first_key)
    assert _verify(collection_registry, keyring).usage_record_count == 1

    empty_key = Ed25519PrivateKey.generate()
    empty_start = _END + timedelta(days=1)
    empty_end = empty_start + timedelta(days=1)
    candidate = collection_registry.prepare_fmp_usage_checkpoint_candidate(
        checkpoint_id="fmp-empty",
        coverage_start_utc=empty_start,
        coverage_end_utc=empty_end,
        authority_id=_AUTHORITY,
        key_id="empty-key",
        generated_at_utc=empty_end,
    )
    assert candidate.leaves == ()
    signed = SignedUsageCheckpoint(candidate.checkpoint, empty_key.sign(candidate.payload_bytes))
    empty_ring = Ed25519PublicKeyring(
        {(_AUTHORITY, "empty-key"): empty_key.public_key().public_bytes_raw()}
    )
    collection_registry.register_usage_checkpoint(signed, (), empty_ring)
    snapshot = collection_registry.verify_fmp_usage_checkpoint(
        checkpoint_id="fmp-empty",
        coverage_start_utc=empty_start,
        coverage_end_utc=empty_end,
        keyring=empty_ring,
    )
    assert snapshot.usage_record_count == 0
    assert snapshot.totals == {}


@pytest.mark.parametrize("tamper", ["quantity", "timestamp", "metric", "unit", "delete", "insert"])
def test_live_usage_row_tamper_is_typed_integrity_failure(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
    tamper: str,
) -> None:
    _seed_run(collection_registry, "fmp-tamper")
    _record(collection_registry, "fmp-tamper", 1)
    keyring = _prepare_sign_register(collection_registry, Ed25519PrivateKey.generate())
    where = (collection_usage_records.c.run_id == "fmp-tamper") & (
        collection_usage_records.c.usage_seq == 1
    )
    with clean_postgres.begin() as connection:
        if tamper == "delete":
            connection.execute(collection_usage_records.delete().where(where))
        elif tamper == "insert":
            connection.execute(
                collection_usage_records.insert().values(
                    run_id="fmp-tamper",
                    usage_seq=2,
                    metric="provider_requests",
                    quantity=Decimal(1),
                    unit="count",
                    evidence_json={},
                    recorded_at_utc=_START + timedelta(hours=2),
                )
            )
        else:
            values: dict[str, object] = {
                "quantity": Decimal(2),
                "timestamp": _START + timedelta(hours=2),
                "metric": "egress",
                "unit": "request",
            }
            column = "recorded_at_utc" if tamper == "timestamp" else tamper
            connection.execute(
                collection_usage_records.update().where(where).values(**{column: values[tamper]})
            )

    with pytest.raises(ProviderUsageIntegrityError):
        _verify(collection_registry, keyring)


@pytest.mark.parametrize("tamper", ["plan_sha256", "provider", "run_plan"])
def test_plan_lineage_tamper_breaks_verification(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
    tamper: str,
) -> None:
    plan = _seed_run(collection_registry, "fmp-plan-tamper")
    replacement = _seed_run(collection_registry, "fmp-replacement")
    _record(collection_registry, "fmp-plan-tamper", 1)
    keyring = _prepare_sign_register(collection_registry, Ed25519PrivateKey.generate())
    with clean_postgres.begin() as connection:
        if tamper == "run_plan":
            connection.execute(
                collection_runs.update()
                .where(collection_runs.c.run_id == "fmp-plan-tamper")
                .values(plan_id=replacement.plan_id)
            )
        else:
            value = "f" * 64 if tamper == "plan_sha256" else "sec"
            connection.execute(
                collection_run_plans.update()
                .where(collection_run_plans.c.plan_id == plan.plan_id)
                .values(**{tamper: value})
            )

    with pytest.raises(ProviderUsageIntegrityError):
        _verify(collection_registry, keyring)


def test_checkpoint_provider_and_coverage_must_be_exact(
    collection_registry: CollectionRegistry,
) -> None:
    _seed_run(collection_registry, "fmp-exact")
    _record(collection_registry, "fmp-exact", 1)
    keyring = _prepare_sign_register(collection_registry, Ed25519PrivateKey.generate())

    with pytest.raises(ProviderUsageIntegrityError):
        collection_registry.verify_fmp_usage_checkpoint(
            checkpoint_id="fmp-usage-20260817",
            coverage_start_utc=_START + timedelta(seconds=1),
            coverage_end_utc=_END,
            keyring=keyring,
        )


@pytest.mark.parametrize(
    ("keyring", "expected", "message"),
    [
        (
            Ed25519PublicKeyring({}),
            UnknownVerificationKeyError,
            "no trusted public key matches the checkpoint authority and key identifiers",
        ),
        (
            None,
            StaleVerificationKeyError,
            "the trusted public key is not valid at the checkpoint generation time",
        ),
    ],
)
def test_fmp_wrapper_preserves_typed_key_errors(
    collection_registry: CollectionRegistry,
    keyring: Ed25519PublicKeyring | None,
    expected: type[Exception],
    message: str,
) -> None:
    _seed_run(collection_registry, "fmp-auth-key")
    _record(collection_registry, "fmp-auth-key", 1)
    private_key = Ed25519PrivateKey.generate()
    _prepare_sign_register(collection_registry, private_key)
    if keyring is None:
        keyring = Ed25519PublicKeyring(
            {
                (_AUTHORITY, _KEY_ID): TrustedEd25519PublicKey(
                    private_key.public_key().public_bytes_raw(),
                    valid_until_utc=_GENERATED,
                )
            }
        )

    with pytest.raises(expected) as captured:
        _verify(collection_registry, keyring)
    assert str(captured.value) == message


def test_fmp_wrapper_preserves_typed_invalid_signature_error(
    collection_registry: CollectionRegistry,
) -> None:
    _seed_run(collection_registry, "fmp-auth-signature")
    _record(collection_registry, "fmp-auth-signature", 1)
    _prepare_sign_register(collection_registry, Ed25519PrivateKey.generate())
    wrong_keyring = Ed25519PublicKeyring(
        {(_AUTHORITY, _KEY_ID): Ed25519PrivateKey.generate().public_key().public_bytes_raw()}
    )

    with pytest.raises(SignatureVerificationError, match="usage checkpoint verification failed"):
        _verify(collection_registry, wrong_keyring)


@pytest.mark.parametrize("tamper", ["metadata", "signature"])
def test_fmp_wrapper_preserves_typed_persisted_checkpoint_authentication_error(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
    tamper: str,
) -> None:
    _seed_run(collection_registry, f"fmp-auth-{tamper}")
    _record(collection_registry, f"fmp-auth-{tamper}", 1)
    keyring = _prepare_sign_register(collection_registry, Ed25519PrivateKey.generate())
    with clean_postgres.begin() as connection:
        connection.exec_driver_sql(
            "ALTER TABLE collection_usage_checkpoints DISABLE TRIGGER "
            "collection_usage_checkpoints_append_only"
        )
        values = {"provider": "sec"} if tamper == "metadata" else {"signature": bytes(64)}
        connection.execute(collection_usage_checkpoints.update().values(**values))
        connection.exec_driver_sql(
            "ALTER TABLE collection_usage_checkpoints ENABLE TRIGGER "
            "collection_usage_checkpoints_append_only"
        )

    with pytest.raises(SignatureVerificationError, match="usage checkpoint verification failed"):
        _verify(collection_registry, keyring)


def test_fmp_transactions_are_repeatable_read_and_read_only(clean_postgres: Engine) -> None:
    service = FmpUsageCheckpointService(clean_postgres)

    with service._repeatable_read() as connection:  # noqa: SLF001 - transaction contract probe
        observed = connection.exec_driver_sql(
            "SELECT current_setting('transaction_isolation'), "
            "current_setting('transaction_read_only')"
        ).one()

    assert observed == ("repeatable read", "on")


def test_verification_uses_one_repeatable_read_snapshot_during_concurrent_update(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    _seed_run(collection_registry, "fmp-concurrent")
    _record(collection_registry, "fmp-concurrent", 1)
    keyring = _prepare_sign_register(collection_registry, Ed25519PrivateKey.generate())
    metadata_read = threading.Event()
    continue_query = threading.Event()

    def barrier(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: object,
    ) -> None:
        if "FROM collection_usage_checkpoints" in statement:
            metadata_read.set()
            assert continue_query.wait(timeout=10)

    event.listen(clean_postgres, "after_cursor_execute", barrier)
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_verify, collection_registry, keyring)
            assert metadata_read.wait(timeout=10)
            try:
                with clean_postgres.begin() as connection:
                    connection.execute(
                        collection_usage_records.update()
                        .where(collection_usage_records.c.run_id == "fmp-concurrent")
                        .values(quantity=Decimal(2))
                    )
            finally:
                continue_query.set()
            result = future.result(timeout=10)
    finally:
        event.remove(clean_postgres, "after_cursor_execute", barrier)
    assert result.totals == {("provider_requests", "count"): Decimal("1.000000")}
    with pytest.raises(ProviderUsageIntegrityError):
        _verify(collection_registry, keyring)


def test_usage_insert_and_checkpoint_snapshot_are_transactionally_ordered(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    run_id = "fmp-insert-checkpoint-race"
    _seed_run(collection_registry, run_id)
    private_key = Ed25519PrivateKey.generate()
    insert_started = threading.Event()
    allow_insert_commit = threading.Event()
    outcome: dict[str, object] = {}

    def insert_and_hold() -> None:
        with clean_postgres.connect() as connection:
            connection.execute(text("BEGIN"))
            connection.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": "aegis_alpha.collection.fmp_usage_checkpoint"},
            )
            connection.execute(
                collection_usage_records.insert().values(
                    run_id=run_id,
                    usage_seq=1,
                    metric="calls_attempted",
                    quantity=Decimal(1),
                    unit="call",
                    evidence_json={"race": True},
                    recorded_at_utc=_START + timedelta(hours=2),
                )
            )
            insert_started.set()
            # Outlast the observer's ten-second deadline; finally releases us.
            assert allow_insert_commit.wait(timeout=20)
            connection.execute(text("COMMIT"))
            outcome["insert"] = "committed"

    def prepare_checkpoint() -> None:
        try:
            assert insert_started.wait(timeout=10)
            candidate = collection_registry.prepare_fmp_usage_checkpoint_candidate(
                checkpoint_id="fmp-race-window",
                coverage_start_utc=_START,
                coverage_end_utc=_END,
                authority_id=_AUTHORITY,
                key_id=_KEY_ID,
                generated_at_utc=_GENERATED,
            )
            signed = SignedUsageCheckpoint(
                candidate.checkpoint,
                private_key.sign(candidate.payload_bytes),
            )
            keyring = Ed25519PublicKeyring(
                {(_AUTHORITY, _KEY_ID): private_key.public_key().public_bytes_raw()}
            )
            collection_registry.register_usage_checkpoint(signed, candidate.leaves, keyring)
            outcome["leaf_count"] = len(candidate.leaves)
            outcome["keyring"] = keyring
        except Exception as error:  # noqa: BLE001
            outcome["checkpoint_error"] = error

    holder = threading.Thread(target=insert_and_hold)
    waiter = threading.Thread(target=prepare_checkpoint)
    with _checkpoint_follower_pid(clean_postgres, waiter) as follower:
        try:
            holder.start()
            waiter.start()
            blocking = _wait_for_blocked_checkpoint_follower(clean_postgres, follower)
        finally:
            allow_insert_commit.set()
            for worker in (holder, waiter):
                if worker.ident is not None:
                    worker.join(timeout=10)

    assert blocking >= 1
    assert not holder.is_alive()
    assert not waiter.is_alive()
    assert outcome["insert"] == "committed"
    assert "checkpoint_error" not in outcome
    keyring = cast("Ed25519PublicKeyring", outcome["keyring"])
    snapshot = collection_registry.verify_fmp_usage_checkpoint(
        checkpoint_id="fmp-race-window",
        coverage_start_utc=_START,
        coverage_end_utc=_END,
        keyring=keyring,
    )
    floor_quantity = snapshot.totals.get(("calls_attempted", "call"), Decimal(0))

    baseline = _checkpoint_baseline(floor_quantity)
    extension_delta = _post_checkpoint_extension_delta(clean_postgres, tmp_path, baseline)
    assert int(bool(floor_quantity)) + int(bool(extension_delta)) == 1
    assert int(floor_quantity) + extension_delta == 1
