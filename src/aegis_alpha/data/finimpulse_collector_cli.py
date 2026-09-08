"""CLI entry for the AAS-DATA-008C FinImpulse collector.

Live collection remains blocked behind the separate G-B owner gates and a
detached-signed standing probe scope (ADR 0011 Tier 2). This entry point
fails closed without an explicit opt-in, a credential, destinations
outside any Git repository, and a verified standing spend bound, so a
G-A invocation makes zero provider calls. Per-probe owner-execution
approval flags are rejected fail-closed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TextIO
from uuid import uuid4

from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.data.finimpulse_collector import (
    CREDENTIAL_ENVIRONMENT_VARIABLE,
    DEFAULT_PAGE_LIMIT,
    REQUIRED_LIFECYCLE_COLUMNS,
    CollectionResult,
    CollectorConfig,
    CollectorError,
    GateEvidence,
    IdentityExport,
    RateLimiter,
    Transport,
    authorized_gate_digest,
    authorized_identity_digest,
    canonical_json_bytes,
    collect_snapshot,
    lifecycle_is_complete,
    load_gate_evidence,
    load_identity_export,
    make_https_transport,
    rebuild_result_from_receipt,
    register_collection_lifecycle,
    sha256_file,
    sha256_hex,
    validate_recovery_inputs,
)
from aegis_alpha.data.finimpulse_owner_authority import (
    OWNER_AUTHORITY_ENV,
    load_owner_authority,
)
from aegis_alpha.data.finimpulse_recurring_authority import (
    VerifiedRecurringAuthority,
    load_recurring_authority,
    recurring_authority_issued_at,
    usd_to_micros,
    verify_recurring_authority,
)
from aegis_alpha.data.finimpulse_recurring_errors import RecurringAuthorityError
from aegis_alpha.metadata.registry import MetadataRegistry

PRECONDITION_EXIT = 2


def _read_universe(path: Path) -> tuple[str, ...]:
    symbols = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    return tuple(symbol for symbol in symbols if symbol and not symbol.startswith("#"))


def _containing_git_repository(path: Path) -> Path | None:
    for candidate in (path, *path.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def validate_live_destinations(**destinations: Path) -> None:
    """Reject any destination inside a Git repository before the first call."""

    for label, destination in sorted(destinations.items()):
        resolved = destination.resolve(strict=False)
        if repository_root := _containing_git_repository(resolved):
            raise CollectorError(
                f"live {label.replace('_', ' ')} must be outside a Git repository "
                f"(resolved repository: {repository_root})"
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one bounded AAS-DATA-008C collection")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--universe-file", required=True, type=Path)
    parser.add_argument("--raw-store-root", required=True, type=Path)
    parser.add_argument("--dataset-root", required=True, type=Path)
    parser.add_argument("--receipt-path", required=True, type=Path)
    parser.add_argument("--budget-usd", required=True)
    parser.add_argument("--page-limit", default=DEFAULT_PAGE_LIMIT, type=int)
    parser.add_argument("--collector-code-version", default="NOT_OBSERVED")
    parser.add_argument("--identity-export", required=True, type=Path)
    parser.add_argument("--gate-evidence", required=True, type=Path)
    parser.add_argument("--recurring-authority", type=Path, default=None)
    parser.add_argument("--recurring-authority-signature", type=Path, default=None)
    parser.add_argument("--owner-approval", type=Path, default=None)
    parser.add_argument("--owner-approval-signature", type=Path, default=None)
    parser.add_argument("--database-url-env", default="AAS_DATABASE_URL")
    parser.add_argument("--predecessor-snapshot-id", default=None)
    parser.add_argument("--credential-env", default=CREDENTIAL_ENVIRONMENT_VARIABLE)
    return parser


def _parse_budget(raw_value: str) -> Decimal:
    try:
        budget = Decimal(raw_value)
    except InvalidOperation:
        raise CollectorError("--budget-usd must be a decimal amount") from None
    if not budget.is_finite() or budget <= 0:
        raise CollectorError("--budget-usd must be a positive finite amount")
    return budget


def reserve_receipt_target(path: Path) -> Path:
    """Claim the immutable receipt destination before any provider call.

    Creating the target exclusively proves it does not already exist and that
    the destination is writable, so paid work can never complete with no
    emitted receipt artifact. The reservation is replaced by the real receipt.
    """

    if path.exists():
        raise CollectorError(f"refusing to overwrite an existing receipt: {path}")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise CollectorError(f"refusing to overwrite an existing receipt: {path}") from None
    except OSError as error:
        raise CollectorError(f"receipt destination is not writable: {error}") from None
    os.close(descriptor)
    return path


def _receipt_anchor_directory(raw_store_root: Path, snapshot_id: str) -> Path:
    return raw_store_root / "receipt-anchors" / sha256_hex(snapshot_id.encode())


def _publish_immutable(path: Path, content: bytes) -> None:
    """Publish one append-only anchor with no overwrite window."""

    if path.exists():
        if path.read_bytes() != content:
            raise CollectorError("receipt anchor already exists with different content")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o444)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != content:
                raise CollectorError("receipt anchor raced with different content") from None
    finally:
        temporary.unlink(missing_ok=True)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _anchored_receipt(raw_store_root: Path, snapshot_id: str) -> bytes | None:
    directory = _receipt_anchor_directory(raw_store_root, snapshot_id)
    anchors = sorted(directory.glob("*.json")) if directory.is_dir() else []
    if not anchors:
        return None
    if len(anchors) != 1:
        raise CollectorError("multiple immutable receipt anchors exist for one snapshot")
    artifact = anchors[0].read_bytes()
    if anchors[0].stem != sha256_hex(artifact):
        raise CollectorError("receipt anchor filename does not match its content")
    return artifact


def _has_recovery_candidate(arguments: argparse.Namespace) -> bool:
    """True when a durable receipt or raw-store anchor may complete a crashed run."""

    path: Path = arguments.receipt_path
    if path.is_file():
        return True
    return _anchored_receipt(arguments.raw_store_root, arguments.snapshot_id) is not None


def _restore_receipt_from_anchor(path: Path, artifact: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_name(f".{path.name}.recovery")
    descriptor = os.open(pending, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(artifact)
        handle.flush()
        os.fsync(handle.fileno())
    pending.replace(path)
    path.with_name(f".{path.name}.pending").unlink(missing_ok=True)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _resume_interrupted_lifecycle(
    arguments: argparse.Namespace,
    engine: Engine,
    config: CollectorConfig,
) -> tuple[str, str, bool] | None:
    """Complete a lifecycle that a crash left unregistered, if one exists.

    A durable receipt with no matching 005 run means the process died between
    receipt publication and registration. The run is rebuilt from the verified
    receipt and immutable captures, so the resume makes zero provider calls and
    appends only the rows the interrupted run still owes.
    """

    path: Path = arguments.receipt_path
    anchored = _anchored_receipt(arguments.raw_store_root, arguments.snapshot_id)
    if not path.is_file() and anchored is None:
        return None
    artifact = b"" if not path.is_file() else path.read_bytes()
    if anchored is None:
        if not artifact:
            path.unlink(missing_ok=True)
            path.with_name(f".{path.name}.pending").unlink(missing_ok=True)
            return None
        raise CollectorError("existing receipt has no immutable raw-store anchor")
    if not artifact:
        _restore_receipt_from_anchor(path, anchored)
        artifact = anchored
    elif artifact != anchored:
        raise CollectorError("existing receipt does not match its immutable raw-store anchor")
    receipt_document = json.loads(artifact)
    if not isinstance(receipt_document, Mapping):
        raise CollectorError("existing receipt must be a JSON object")
    receipt = dict(receipt_document)
    canonical = canonical_json_bytes(receipt)
    if artifact != canonical:
        raise CollectorError("existing receipt is not its exact canonical artifact")
    snapshot_id = str(receipt["snapshot_id"])
    if snapshot_id != arguments.snapshot_id:
        raise CollectorError(
            f"existing receipt is for snapshot {snapshot_id}, not {arguments.snapshot_id}"
        )
    validate_recovery_inputs(receipt, config)
    result = rebuild_result_from_receipt(
        receipt,
        raw_store_root=arguments.raw_store_root,
        dataset_root=arguments.dataset_root,
    )
    if lifecycle_is_complete(engine, result, f"{snapshot_id}.run"):
        raise CollectorError(
            f"snapshot {snapshot_id} is already registered; refusing to repeat a completed run"
        )
    digest = sha256_hex(canonical)
    _register_lifecycle(engine, result, config)
    return "RESUMED", digest, result.published


def write_durable_receipt(
    path: Path,
    receipt: Mapping[str, object],
    *,
    raw_store_root: Path,
) -> str:
    """Write the receipt atomically and durably, then verify its hash.

    The bytes are written to a temporary file, fsynced, atomically renamed over
    the reservation, and the parent directory is fsynced. The artifact is then
    read back and hashed, so a registered receipt hash can never refer to a
    zero-byte or truncated file.
    """

    content = canonical_json_bytes(dict(receipt))
    expected = sha256_hex(content)
    snapshot_id = str(receipt["snapshot_id"])
    anchor = _receipt_anchor_directory(raw_store_root, snapshot_id) / f"{expected}.json"
    _publish_immutable(anchor, content)
    pending = path.with_name(f".{path.name}.pending")
    descriptor = os.open(pending, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        pending.unlink(missing_ok=True)
        raise
    pending.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    observed = sha256_hex(path.read_bytes())
    if observed != expected:
        raise CollectorError("receipt artifact does not match its own content hash")
    return observed


def _release_receipt_reservation(path: Path) -> None:
    """Drop an unused empty reservation so a failed run leaves no artifact."""

    if path.is_file() and path.stat().st_size == 0:
        path.unlink(missing_ok=True)


def _reject_per_probe_approval(arguments: argparse.Namespace) -> None:
    """Standing scope replaces per-probe owner-execution approval fail-closed."""

    if arguments.owner_approval is not None or arguments.owner_approval_signature is not None:
        raise CollectorError(
            "FinImpulse probe collection does not accept per-probe owner-approval inputs; "
            "no provider calls were attempted"
        )


def _require_standing_scope(
    arguments: argparse.Namespace,
    environment: Mapping[str, str],
    *,
    budget_usd: Decimal,
    now: datetime,
) -> VerifiedRecurringAuthority:
    """Authenticate the standing probe scope and bind the run's spend request."""

    if arguments.recurring_authority is None or arguments.recurring_authority_signature is None:
        raise CollectorError(
            "--recurring-authority and --recurring-authority-signature are required; "
            "no provider calls were attempted"
        )
    owner_authority_path = environment.get(OWNER_AUTHORITY_ENV)
    if not owner_authority_path:
        raise CollectorError(f"{OWNER_AUTHORITY_ENV} is required; no provider calls were attempted")
    payload, signature = load_recurring_authority(
        arguments.recurring_authority, arguments.recurring_authority_signature
    )
    trust = load_owner_authority(Path(owner_authority_path), recurring_authority_issued_at(payload))
    authority = verify_recurring_authority(payload, signature, trust, now=now)
    resolved_raw = arguments.raw_store_root.resolve(strict=False)
    resolved_dataset = arguments.dataset_root.resolve(strict=False)
    if resolved_raw != authority.raw_store_root or resolved_dataset != authority.dataset_root:
        raise CollectorError(
            "run roots do not match the signed standing authority roots; "
            "no provider calls were attempted"
        )
    requested_micros = usd_to_micros(budget_usd)
    if requested_micros > authority.max_spend_micros:
        raise CollectorError(
            "--budget-usd exceeds the signed standing authority max_spend_micros bound; "
            "no provider calls were attempted"
        )
    return authority


def _require_live_opt_in(arguments: argparse.Namespace) -> None:
    """Reject G-A invocations and leftover per-probe approval flags."""

    if not arguments.live:
        raise CollectorError(
            "--live is required; G-A makes zero provider calls and G-B is separately gated"
        )
    _reject_per_probe_approval(arguments)


def _require_live_call_inputs(
    *,
    credential: str | None,
    authority: VerifiedRecurringAuthority | None,
) -> tuple[str, VerifiedRecurringAuthority]:
    """Return the standing scope and credential or fail closed."""

    if credential is None or authority is None:
        raise CollectorError(
            "standing authority and credential are required before any provider call"
        )
    return credential, authority


def _require_transport_preconditions(
    arguments: argparse.Namespace,
    environment: Mapping[str, str],
) -> str:
    """Fail closed before any provider call and return the credential."""

    _require_live_opt_in(arguments)
    credential = environment.get(arguments.credential_env, "")
    if not credential:
        raise CollectorError(
            f"{arguments.credential_env} is not set; no provider calls were attempted"
        )
    validate_live_destinations(
        raw_store_root=arguments.raw_store_root,
        dataset_root=arguments.dataset_root,
        receipt_path=arguments.receipt_path,
    )
    return credential


def _load_evidence(
    arguments: argparse.Namespace,
) -> tuple[GateEvidence, IdentityExport]:
    """Load G-B gate and 007 identity evidence under the owner authority set.

    The caller supplies only file paths. Each artifact's digest must appear in
    the repository-committed owner authority reference, which no CLI flag or
    environment variable can supply or override, so a fabricated artifact
    cannot authorize itself.
    """

    gate_digest = authorized_gate_digest(sha256_file(arguments.gate_evidence))
    identity_digest = authorized_identity_digest(sha256_file(arguments.identity_export))
    gate_evidence = load_gate_evidence(
        arguments.gate_evidence,
        expected_sha256=gate_digest,
    )
    identity_export = load_identity_export(
        arguments.identity_export,
        expected_sha256=identity_digest,
    )
    return gate_evidence, identity_export


#: Tables one snapshot registration must be able to write.
REQUIRED_REGISTRY_TABLES = (
    "collection_run_plans",
    "collection_runs",
    "collection_run_events",
    "collection_run_receipts",
    "collection_usage_records",
    "source_snapshots",
)


def require_registry_target(
    arguments: argparse.Namespace,
    environment: Mapping[str, str],
) -> Engine:
    """Require a migrated, writable 005 registry before any provider call.

    Reachability alone is not enough: a blank database or one with read-only
    grants would accept the connection and then fail registration after paid
    work. This verifies the migrated schema and proves write readiness inside a
    transaction that is always rolled back, so nothing is mutated.
    """

    database_url = environment.get(arguments.database_url_env, "")
    if not database_url:
        raise CollectorError(
            f"{arguments.database_url_env} is not set; AAS-DATA-005 registration is required "
            "and no provider calls were attempted"
        )
    try:
        engine = create_engine(database_url)
        _verify_registry_schema(engine)
    except SQLAlchemyError as error:
        raise CollectorError(
            f"AAS-DATA-005 registry is unreachable ({type(error).__name__}); "
            "no provider calls were attempted"
        ) from None
    return engine


def _verify_registry_schema(engine: Engine) -> None:
    """Fail closed unless the migrated schema matches and is fully writable.

    Table presence alone is not enough: a schema can carry the right names with
    incompatible columns, and grants can permit one table while denying the
    rest. This checks the declared column contract for every table the 003/005
    lifecycle touches, then exercises the complete write surface inside a
    transaction that is always rolled back, so nothing is mutated.
    """

    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    inspector = inspect(engine)
    observed = set(inspector.get_table_names())
    missing = [name for name in REQUIRED_REGISTRY_TABLES if name not in observed]
    if missing:
        raise CollectorError(
            f"AAS-DATA-005 schema is not migrated (missing {sorted(missing)}); "
            "no provider calls were attempted"
        )
    for table, required_columns in REQUIRED_LIFECYCLE_COLUMNS.items():
        present = {column["name"] for column in inspector.get_columns(table)}
        absent = [name for name in required_columns if name not in present]
        if absent:
            raise CollectorError(
                f"AAS-DATA-005 schema is incompatible ({table} is missing {sorted(absent)}); "
                "no provider calls were attempted"
            )
    _probe_lifecycle_write_surface(engine)


_PROBE_SOURCE_SNAPSHOT = (
    "INSERT INTO source_snapshots (snapshot_id, schema_version, provider, dataset, "
    "source_uri, request_fingerprint, parameters_json, requested_at_utc, retrieved_at_utc, "
    "content_type, raw_byte_length, content_sha256, tree_sha256, parser_name, parser_version, "
    "validation_status, license_classification, retention_classification, manifest_json, "
    "registered_at_utc) VALUES (:id, 1, 'finimpulse', 'write-probe', "
    "'https://probe.invalid/x', :fingerprint, '{}'::jsonb, now(), now(), 'application/json', "
    "0, :digest, :digest, 'probe', '1', 'PASS', 'UNKNOWN', 'UNKNOWN', '{}'::jsonb, now())"
)
_PROBE_RUN_PLAN = (
    "INSERT INTO collection_run_plans (plan_id, schema_version, provider, dataset, mode, "
    "parameters_json, plan_sha256, created_at_utc) VALUES "
    "(:id, 1, 'finimpulse', 'write-probe', 'probe', '{}'::jsonb, :digest, now())"
)
_PROBE_RUN = (
    "INSERT INTO collection_runs (run_id, plan_id, created_at_utc) VALUES (:id, :id, now())"
)
_PROBE_EVENT = (
    "INSERT INTO collection_run_events (run_id, event_type, attempt_number, occurred_at_utc, "
    "event_seq, details_json) VALUES (:id, 'attempt_started', 1, now(), 1, '{}'::jsonb)"
)
_PROBE_RECEIPT = (
    "INSERT INTO collection_run_receipts (run_id, attempt_number, source_snapshot_id, "
    "receipt_sha256, registered_at_utc) VALUES (:id, 1, :id, :digest, now())"
)
_PROBE_USAGE = (
    "INSERT INTO collection_usage_records (run_id, usage_seq, metric, quantity, unit, "
    "evidence_json, recorded_at_utc) VALUES "
    "(:id, 1, 'write_probe', 0, 'USD', '{}'::jsonb, now())"
)


def _probe_lifecycle_write_surface(engine: Engine) -> None:
    """Write once into every lifecycle table, then always roll it back.

    The probe covers the whole 003/005 write surface in dependency order, so a
    denial or incompatible constraint on any downstream table surfaces before
    any provider call rather than after paid work. The transaction is always
    rolled back, so the probe proves capability and never leaves state.
    """

    probe = f"write-probe-{uuid4().hex}"
    parameters = {"id": probe, "digest": "0" * 64, "fingerprint": f"sha256:{'0' * 64}"}
    statements = (
        _PROBE_SOURCE_SNAPSHOT,
        _PROBE_RUN_PLAN,
        _PROBE_RUN,
        _PROBE_EVENT,
        _PROBE_RECEIPT,
        _PROBE_USAGE,
    )
    connection = engine.connect()
    transaction = connection.begin()
    try:
        for statement in statements:
            connection.execute(text(statement), parameters)
    except SQLAlchemyError as error:
        raise CollectorError(
            f"AAS-DATA-005 registry is not writable ({type(error).__name__}); "
            "no provider calls were attempted"
        ) from None
    finally:
        # Always roll back: the probe proves capability, never state.
        transaction.rollback()
        connection.close()


def _register_lifecycle(
    engine: Engine,
    result: CollectionResult,
    config: CollectorConfig,
) -> str:
    """Persist the 005 lifecycle through the already-validated registry."""

    register_collection_lifecycle(
        result,
        config,
        collection_registry=CollectionRegistry(engine),
        metadata_registry=MetadataRegistry(engine),
        plan_id=f"{result.snapshot_id}.plan",
        run_id=f"{result.snapshot_id}.run",
        recorded_at_utc=result.observed_at,
    )
    return "REGISTERED"


def main(  # noqa: PLR0913 - injectable production boundaries keep QA socketless
    argv: Sequence[str] | None = None,
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
    arguments = build_parser().parse_args(argv)
    environment = os.environ if environ is None else environ
    output = sys.stdout if stdout is None else stdout
    error_output = sys.stderr if stderr is None else stderr
    moment = datetime.now(UTC) if now is None else now
    if rate_limiter is not None:
        raise CollectorError(
            "a caller-supplied rate_limiter cannot replace the standing rate limiter; "
            "inject clock and sleeper instead"
        )
    engine: Engine | None = None
    reserved_receipt: Path | None = None
    try:
        # Determine whether this invocation can complete an interrupted run
        # before applying transport-only preconditions. Credential rotation
        # after a crash must not block zero-call recovery.
        _require_live_opt_in(arguments)
        budget_usd = _parse_budget(arguments.budget_usd)
        recovery_candidate = _has_recovery_candidate(arguments)
        credential: str | None = None
        authority: VerifiedRecurringAuthority | None = None
        if not recovery_candidate:
            credential = _require_transport_preconditions(arguments, environment)
            authority = _require_standing_scope(
                arguments, environment, budget_usd=budget_usd, now=moment
            )
        gate_evidence, identity_export = _load_evidence(arguments)
        config = CollectorConfig(
            universe=_read_universe(arguments.universe_file),
            budget_usd=budget_usd,
            page_limit=arguments.page_limit,
            collector_code_version=arguments.collector_code_version,
            identity_export_sha256=identity_export.export_sha256,
            identity_as_of=identity_export.as_of_utc,
            predecessor_snapshot_id=arguments.predecessor_snapshot_id,
        )
        engine = require_registry_target(arguments, environment)
        # Crash reconciliation: a durable receipt whose lifecycle never landed
        # is completed from verified artifacts with zero provider calls. This
        # path does not admit a new standing-scope transport grant.
        if recovery_candidate:
            resumed = _resume_interrupted_lifecycle(arguments, engine, config)
            if resumed is not None:
                lifecycle_state, receipt_digest, published = resumed
                output.write(
                    json.dumps(
                        {
                            "collection_lifecycle": lifecycle_state,
                            "published": published,
                            "receipt_path": str(arguments.receipt_path),
                            "receipt_sha256": receipt_digest,
                            "snapshot_id": arguments.snapshot_id,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                return 0
            credential = _require_transport_preconditions(arguments, environment)
            authority = _require_standing_scope(
                arguments, environment, budget_usd=budget_usd, now=moment
            )
        live_credential, live_authority = _require_live_call_inputs(
            credential=credential, authority=authority
        )
        reserved_receipt = reserve_receipt_target(arguments.receipt_path)
        result = collect_snapshot(
            snapshot_id=arguments.snapshot_id,
            config=config,
            credential=live_credential,
            transport=make_https_transport() if transport is None else transport,
            raw_store_root=arguments.raw_store_root,
            dataset_root=arguments.dataset_root,
            identity_export=identity_export,
            gate_evidence=gate_evidence,
            sleeper=time.sleep if sleeper is None else sleeper,
            standing_authority=live_authority,
            request_clock=(lambda: datetime.now(UTC)) if request_clock is None else request_clock,
        )
        # Write-ahead order: the receipt artifact is durable and hash-verified
        # before registration records its hash, so no registered hash can refer
        # to a missing, zero-byte, or truncated artifact.
        receipt_digest = write_durable_receipt(
            reserved_receipt,
            result.receipt,
            raw_store_root=arguments.raw_store_root,
        )
        lifecycle_state = _register_lifecycle(engine, result, config)
    except (
        CollectorError,
        RecurringAuthorityError,
        OSError,
        ValueError,
        SQLAlchemyError,
    ) as error:
        if reserved_receipt is not None:
            _release_receipt_reservation(reserved_receipt)
        error_output.write(f"error: {error}\n")
        return PRECONDITION_EXIT
    finally:
        if engine is not None:
            engine.dispose()

    summary = {
        "collection_lifecycle": lifecycle_state,
        "published": result.published,
        "receipt_path": str(arguments.receipt_path),
        "receipt_sha256": receipt_digest,
        "snapshot_id": result.snapshot_id,
    }
    output.write(json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n")
    return 0
