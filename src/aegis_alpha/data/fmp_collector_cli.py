from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

from aegis_alpha.collection.records import RunEventType
from aegis_alpha.data.fmp_cli_artifacts import (
    AUTHORIZED_MAX_CALLS,
    FMP_POLICY_ID,  # noqa: F401 - compatibility re-export
    PRECONDITION_EXIT,
    ApprovalArtifact,
    NotificationArtifact,
    PreconditionError,
    _read_json,
    license_classification,
    load_approval_artifact,
    load_notification_artifact,
)
from aegis_alpha.data.fmp_collector import (
    CollectorLockError,
    DestinationError,
    Transport,
    validate_destination,
)
from aegis_alpha.data.fmp_collector_command import RuntimeDependencies, run_live_command
from aegis_alpha.data.fmp_dataset_selection import add_dataset_selection_argument
from aegis_alpha.data.fmp_rate_limit import (
    TrustedUsageSnapshot,
    UsageEvidenceUnavailableError,
    parse_tier_artifact,
    require_trusted_usage_baseline,
)
from aegis_alpha.data.fmp_usage_trust import (
    load_trusted_fmp_usage_snapshot,
    require_current_usage_snapshot,
)


@dataclass(frozen=True, slots=True)
class PreflightResult:
    approval: ApprovalArtifact
    notification: NotificationArtifact
    tier_sha256: str
    artifact_hashes: Mapping[str, str]
    usage_baseline: TrustedUsageSnapshot


def run_preflight(  # noqa: PLR0913 - each argument is one explicit section 2 gate
    *,
    registry_path: Path,
    approval_path: Path | None,
    notification_path: Path,
    tier_path: Path,
    max_calls: int,
    raw_store_root: Path,
    dataset_root: Path,
    receipt_path: Path,
    now: datetime,
    environ: Mapping[str, str] | None = None,
) -> PreflightResult:
    """Run every hard gate before any network call could be attempted."""

    if max_calls < 1:
        raise PreconditionError("--max-calls is mandatory and must be a positive integer")
    if max_calls > AUTHORIZED_MAX_CALLS:
        raise PreconditionError("--max-calls exceeds the currently authorized ceiling of 25")

    # G1: destinations are canonicalized and never inside a Git repository.
    for label, destination in (
        ("raw store root", raw_store_root),
        ("dataset root", dataset_root),
        ("command output path", receipt_path),
    ):
        validate_destination(label, destination)

    license_classification(registry_path)
    if approval_path is None:
        raise PreconditionError(
            "--owner-approval is required for every live invocation; "
            "no provider calls were attempted"
        )
    approval = load_approval_artifact(approval_path)
    approval.require_unexpired(now)
    if approval.max_calls != max_calls:
        raise PreconditionError(
            "contract-review artifact 'max_calls' must equal the run's --max-calls"
        )

    notification = load_notification_artifact(notification_path)

    tier_document, tier_digest = _read_json("tier", tier_path)
    try:
        parse_tier_artifact(tier_document)
    except ValueError as error:
        raise PreconditionError(str(error)) from error

    usage_baseline = require_trusted_usage_baseline(
        require_current_usage_snapshot(
            load_trusted_fmp_usage_snapshot(now=now, environ=environ),
            now=now,
        )
    )

    hashes = {
        "tier": tier_digest,
        "notification": notification.sha256,
        "approval": approval.sha256,
    }
    return PreflightResult(
        approval=approval,
        notification=notification,
        tier_sha256=tier_digest,
        artifact_hashes=hashes,
        usage_baseline=usage_baseline,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bounded, fail-closed AAS-DATA-004C FMP collector",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("collect", "build-universe"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--registry", type=Path, required=True)
        subparser.add_argument("--owner-approval", type=Path)
        subparser.add_argument("--owner-approval-signature", type=Path)
        subparser.add_argument("--storage-notification", type=Path, required=True)
        subparser.add_argument("--tier", type=Path, required=True)
        subparser.add_argument("--max-calls", type=int)
        subparser.add_argument("--raw-store-root", type=Path, required=True)
        subparser.add_argument("--dataset-root", type=Path, required=True)
        subparser.add_argument("--credential-env", default="FMP_API_KEY")
        if command == "collect":
            subparser.add_argument("--recurring-authority", type=Path)
            subparser.add_argument("--recurring-authority-signature", type=Path)
            subparser.add_argument("--receipt-path", type=Path, required=True)
            subparser.add_argument("--universe-manifest", type=Path, required=True)
            subparser.add_argument(
                "--mode", choices=("probe", "incremental", "backfill"), required=True
            )
            add_dataset_selection_argument(subparser)
            subparser.add_argument("--from", dest="operator_from", default=None)
        else:
            subparser.add_argument("--universe-manifest-out", type=Path, required=True)
    return parser


def main(  # noqa: PLR0913 - injectable production boundaries keep QA socketless
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    now: datetime | None = None,
    transport: Transport | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    approval_clock: Callable[[], datetime] | None = None,
    wall_clock: Callable[[], datetime] | None = None,
) -> int:
    arguments = _build_parser().parse_args(argv)
    environment = os.environ if environ is None else environ
    output = sys.stdout if stdout is None else stdout
    error_output = sys.stderr if stderr is None else stderr
    moment = datetime.now(UTC) if now is None else now

    credential = environment.get(arguments.credential_env, "")
    if not credential:
        error_output.write(
            f"{arguments.credential_env} is not set; no provider calls were attempted\n"
        )
        return PRECONDITION_EXIT

    try:
        outcome, provider_calls = run_live_command(
            arguments=arguments,
            environment=environment,
            credential=credential,
            moment=moment,
            dependencies=RuntimeDependencies(
                transport=transport,
                monotonic=monotonic,
                sleep=sleep,
                approval_clock=(
                    (lambda: datetime.now(UTC)) if approval_clock is None else approval_clock
                ),
                wall_clock=(lambda: datetime.now(UTC)) if wall_clock is None else wall_clock,
            ),
        )
    except (
        CollectorLockError,
        DestinationError,
        PreconditionError,
        UsageEvidenceUnavailableError,
        ValueError,
        OSError,
    ) as error:
        error_output.write(f"{error}; no provider calls were attempted\n")
        return PRECONDITION_EXIT
    except Exception as error:  # noqa: BLE001 - never leak runtime/provider details
        error_output.write(f"FMP run failed with {type(error).__name__}\n")
        return 1
    output.write(
        json.dumps(
            {
                "artifact_path": (
                    None if outcome.artifact_path is None else str(outcome.artifact_path)
                ),
                "command": arguments.command,
                "plan_id": outcome.plan_id,
                "provider_calls": provider_calls,
                "receipt_path": None if outcome.receipt_path is None else str(outcome.receipt_path),
                "run_id": outcome.run_id,
                "terminal_event": outcome.terminal_event.value,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    return 0 if outcome.terminal_event is RunEventType.RUN_SUCCEEDED else 1
