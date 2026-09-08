"""Operator entry point for the signed standing daily FMP refresh."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TextIO

from aegis_alpha.collection.records import RunEventType
from aegis_alpha.data.fmp_catalog import FmpCatalogPendingError
from aegis_alpha.data.fmp_cli_artifacts import PRECONDITION_EXIT, PreconditionError
from aegis_alpha.data.fmp_collector import (
    CollectorLockError,
    CollectorLockLostError,
    CollectorOutcome,
    DestinationError,
    Transport,
    publish_bundle,
)
from aegis_alpha.data.fmp_collector_command import (
    RuntimeDependencies,
    run_authorized_command,
)
from aegis_alpha.data.fmp_daily_authority import verified_daily_authority as _verified_authority
from aegis_alpha.data.fmp_daily_refresh import (
    DailyClassification,
    DailyDisposition,
    DailyRefreshPlan,
    DailyUniverseClassification,
    build_daily_refresh_plan,
    classification_document,
    classify_daily_universe,
    collection_manifest_document,
    load_norgate_universe,
)
from aegis_alpha.data.fmp_rate_limit import BudgetExhaustedError
from aegis_alpha.data.fmp_recurring_errors import RecurringAuthorityError
from aegis_alpha.data.fmp_recurring_recovery import finalize_pending_markers
from aegis_alpha.data.fmp_recurring_revocation import publish_recurring_revocation
from aegis_alpha.data.fmp_recurring_runtime import (
    RecurringOperation,
    authorize_recurring_operation,
)
from aegis_alpha.data.fmp_usage_trust import UsageEvidenceUnavailableError
from aegis_alpha.data.fmp_windows import UniverseManifest, parse_universe_manifest
from aegis_alpha.data.serialization import canonical_json_bytes

MAX_SHARD_TOTAL = 40


@dataclass(slots=True)
class _InvocationBudget:
    """New attempts in one sequential invocation, excluding restored run usage."""

    max_calls: int
    calls_attempted: int = 0

    def admit(self) -> None:
        if self.calls_attempted >= self.max_calls:
            raise BudgetExhaustedError(
                f"invocation call budget of {self.max_calls} attempts is exhausted"
            )
        self.calls_attempted += 1

    def summary(self) -> dict[str, int]:
        return {
            "invocation_max_calls": self.max_calls,
            "invocation_calls_attempted": self.calls_attempted,
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Signed daily FMP universe refresh and active-symbol collection"
    )
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--storage-notification", type=Path, required=True)
    parser.add_argument("--tier", type=Path, required=True)
    parser.add_argument("--recurring-authority", type=Path, required=True)
    parser.add_argument("--recurring-authority-signature", type=Path, required=True)
    parser.add_argument("--credential-env", default="FMP_API_KEY")
    parser.add_argument(
        "--max-calls",
        type=int,
        default=None,
        help="total new call attempts across this invocation, including universe and retries",
    )
    parser.add_argument("--service-day", type=date.fromisoformat, default=None)
    parser.add_argument(
        "--universe-only",
        action="store_true",
        help="Finish the signed universe/classification step without starting price collection",
    )
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-total", type=int, default=None)
    parser.add_argument(
        "--revoke-authority",
        action="store_true",
        help="revoke the signed standing authority without contacting FMP",
    )
    return parser


def _existing_sharded_day_artifacts(
    plan: DailyRefreshPlan,
) -> tuple[UniverseManifest, DailyUniverseClassification] | None:
    """Return today's reusable universe artifacts, or None to fall through to build."""

    if not (
        plan.universe_path.is_file()
        and plan.classification_path.is_file()
        and plan.collection_manifest_path.is_file()
    ):
        return None
    try:
        parsed = (
            parse_universe_manifest(json.loads(plan.universe_path.read_bytes())),
            json.loads(plan.classification_path.read_bytes()),
            parse_universe_manifest(json.loads(plan.collection_manifest_path.read_bytes())),
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        return None
    manifest, classification_payload, collection_manifest = parsed
    classification = _classification_from_payload(classification_payload, collection_manifest)
    if classification is None:
        return None
    return manifest, classification


def _classification_from_payload(
    classification_payload: object,
    collection_manifest: UniverseManifest,
) -> DailyUniverseClassification | None:
    if not isinstance(classification_payload, dict):
        return None
    raw_entries = classification_payload.get("entries")
    ignored = classification_payload.get("ignored_delisted_symbols", [])
    if not isinstance(raw_entries, list) or not isinstance(ignored, list):
        return None
    parsed_entries: list[DailyClassification] = []
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, dict):
            return None
        symbol = raw_entry.get("symbol")
        disposition = raw_entry.get("disposition")
        assetid = raw_entry.get("norgate_assetid")
        if not isinstance(symbol, str) or not isinstance(disposition, str):
            return None
        try:
            parsed_entries.append(
                DailyClassification(
                    symbol=symbol,
                    disposition=DailyDisposition(disposition),
                    norgate_assetid=None if assetid is None else int(str(assetid)),
                )
            )
        except (TypeError, ValueError):
            return None
    return DailyUniverseClassification(
        entries=tuple(parsed_entries),
        ignored_delisted_symbols=tuple(str(symbol) for symbol in ignored),
        collection_manifest=collection_manifest,
    )


def _write_terminal_event(
    error_output: TextIO,
    payload: dict[str, object],
    budget: _InvocationBudget | None = None,
) -> int:
    if budget is not None:
        payload.update(budget.summary())
    error_output.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    return 1


def _write_error(error_output: TextIO, message: str, budget: _InvocationBudget | None) -> None:
    if budget is None:
        error_output.write(message + "\n")
    else:
        error_output.write(
            json.dumps({"error": message, **budget.summary()}, sort_keys=True) + "\n"
        )


def _incompatible_controls(arguments: argparse.Namespace, budget: _InvocationBudget | None) -> bool:
    return (budget is not None and budget.max_calls < 1) or (
        (budget is not None or arguments.universe_only)
        and (
            arguments.shard_index is not None
            or arguments.shard_total is not None
            or arguments.revoke_authority
        )
    )


def main(  # noqa: C901, PLR0911, PLR0912, PLR0913 - CLI fail-closed exits remain explicit
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
    arguments = _parser().parse_args(argv)
    environment = os.environ if environ is None else environ
    output = sys.stdout if stdout is None else stdout
    error_output = sys.stderr if stderr is None else stderr
    budget = None if arguments.max_calls is None else _InvocationBudget(arguments.max_calls)
    if _incompatible_controls(arguments, budget):
        _write_error(
            error_output,
            "--max-calls must be positive; bounded or universe-only calls cannot use "
            "shards or revocation; no provider calls were attempted",
            budget,
        )
        return PRECONDITION_EXIT
    moment = datetime.now(UTC) if now is None else now
    service_day = moment.date() if arguments.service_day is None else arguments.service_day
    shard = (
        (arguments.shard_index, arguments.shard_total)
        if arguments.shard_total is not None
        else None
    )
    if (arguments.shard_index is None) != (arguments.shard_total is None):
        _write_error(
            error_output,
            "--shard-index and --shard-total must be provided together;"
            " no provider calls were attempted",
            budget,
        )
        return PRECONDITION_EXIT
    if shard is not None and (
        arguments.shard_total < 1
        or arguments.shard_total > MAX_SHARD_TOTAL
        or not 1 <= arguments.shard_index <= arguments.shard_total
    ):
        _write_error(
            error_output,
            "shard coordinates must satisfy 1 <= shard-index <= shard-total <= 16;"
            " no provider calls were attempted",
            budget,
        )
        return PRECONDITION_EXIT
    credential = environment.get(arguments.credential_env, "")
    if not arguments.revoke_authority and not credential:
        _write_error(
            error_output,
            f"{arguments.credential_env} is not set; no provider calls were attempted",
            budget,
        )
        return PRECONDITION_EXIT
    dependencies = RuntimeDependencies(
        transport=transport,
        monotonic=monotonic,
        sleep=sleep,
        approval_clock=(lambda: datetime.now(UTC)) if approval_clock is None else approval_clock,
        wall_clock=(lambda: datetime.now(UTC)) if wall_clock is None else wall_clock,
        invocation_admission=None if budget is None else budget.admit,
    )
    try:
        authority = _verified_authority(arguments, environment, moment)
        if arguments.revoke_authority:
            destination = publish_recurring_revocation(authority, moment)
            output.write(json.dumps({"revocation_path": str(destination)}, sort_keys=True) + "\n")
            return 0
        recovered_run_ids = (
            finalize_pending_markers(
                authority=authority,
                recurring_authority_path=arguments.recurring_authority,
                recurring_signature_path=arguments.recurring_authority_signature,
                registry_path=arguments.registry,
                notification_path=arguments.storage_notification,
                tier_path=arguments.tier,
                environment=environment,
                moment=moment,
                credential=credential,
                dependencies=dependencies,
            )
            if "AAS_DATABASE_URL" in environment
            else ()
        )
        plan = build_daily_refresh_plan(
            schedule_id=authority.schedule_id,
            service_day=service_day,
            output_root=authority.output_root,
            authority_payload_sha256=authority.payload_sha256,
        )
        reused = _existing_sharded_day_artifacts(plan) if shard is not None else None
        if reused is not None:
            manifest, classification = reused
            universe_outcome = CollectorOutcome(
                run_id=plan.universe_run_id,
                plan_id="reused-daily-universe",
                terminal_event=RunEventType.RUN_SUCCEEDED,
                published_paths=(
                    plan.universe_path,
                    plan.classification_path,
                    plan.collection_manifest_path,
                ),
                receipt_path=None,
                quality_results=(),
                watermarks_advanced=(),
            )
            universe_calls = 0
        else:
            universe_authorization = authorize_recurring_operation(
                operation=RecurringOperation(
                    command="build-universe",
                    service_day=service_day,
                    output_path=plan.universe_path,
                ),
                recurring_authority_path=arguments.recurring_authority,
                recurring_signature_path=arguments.recurring_authority_signature,
                registry_path=arguments.registry,
                notification_path=arguments.storage_notification,
                tier_path=arguments.tier,
                environment=environment,
                moment=moment,
                approval_clock=dependencies.approval_clock,
            )
            universe_outcome, universe_calls = run_authorized_command(
                arguments=argparse.Namespace(command="build-universe"),
                authorization=universe_authorization,
                environment=environment,
                credential=credential,
                moment=moment,
                dependencies=dependencies,
            )
            if universe_outcome.terminal_event is not RunEventType.RUN_SUCCEEDED:
                return _write_terminal_event(
                    error_output,
                    {
                        "cancel_reason": universe_outcome.cancel_reason,
                        "error_class": universe_outcome.error_class,
                        "run_id": universe_outcome.run_id,
                        "terminal_event": universe_outcome.terminal_event.value,
                    },
                    budget,
                )
            manifest = parse_universe_manifest(json.loads(plan.universe_path.read_bytes()))
            norgate = load_norgate_universe(
                authority.norgate_security_master,
                expected_sha256=authority.norgate_security_master_sha256,
                expected_row_count=authority.norgate_security_master_row_count,
            )
            classification = classify_daily_universe(
                manifest,
                norgate,
                norgate_snapshot_date=authority.norgate_snapshot_date,
            )
            _ = publish_bundle(
                [
                    (
                        plan.classification_path,
                        canonical_json_bytes(
                            classification_document(
                                classification,
                                norgate_security_master_sha256=(
                                    authority.norgate_security_master_sha256
                                ),
                                norgate_snapshot_date=authority.norgate_snapshot_date,
                            )
                        ),
                    ),
                    (
                        plan.collection_manifest_path,
                        canonical_json_bytes(collection_manifest_document(classification)),
                    ),
                ]
            )
        if arguments.universe_only:
            output.write(
                json.dumps(
                    {
                        "terminal_event": universe_outcome.terminal_event.value,
                        "universe_run_id": universe_outcome.run_id,
                        "service_day": service_day.isoformat(),
                        "classification_path": str(plan.classification_path),
                        "provider_calls": universe_calls,
                        "collection_executed": False,
                        **({} if budget is None else budget.summary()),
                    },
                    sort_keys=True,
                )
                + "\n"
            )
            return 0
        collection_authorization = authorize_recurring_operation(
            operation=RecurringOperation(
                command="collect",
                service_day=service_day,
                output_path=plan.receipt_path,
                manifest_path=plan.collection_manifest_path,
                shard=shard,
            ),
            recurring_authority_path=arguments.recurring_authority,
            recurring_signature_path=arguments.recurring_authority_signature,
            registry_path=arguments.registry,
            notification_path=arguments.storage_notification,
            tier_path=arguments.tier,
            environment=environment,
            moment=moment,
            approval_clock=dependencies.approval_clock,
        )
        collection_outcome, collection_calls = run_authorized_command(
            arguments=argparse.Namespace(command="collect"),
            authorization=collection_authorization,
            environment=environment,
            credential=credential,
            moment=moment,
            dependencies=dependencies,
            shard=shard,
        )
    except FmpCatalogPendingError as error:
        return _write_terminal_event(error_output, error.report(), budget)
    except (
        CollectorLockError,
        CollectorLockLostError,
        DestinationError,
        PreconditionError,
        RecurringAuthorityError,
        UsageEvidenceUnavailableError,
        ValueError,
        OSError,
    ) as error:
        _write_error(error_output, f"{error}; no unaccounted provider calls were attempted", budget)
        return PRECONDITION_EXIT
    except Exception as error:  # noqa: BLE001
        _write_error(error_output, f"daily FMP refresh failed with {type(error).__name__}", budget)
        return 1
    if collection_outcome.terminal_event is not RunEventType.RUN_SUCCEEDED:
        return _write_terminal_event(
            error_output,
            {
                "cancel_reason": collection_outcome.cancel_reason,
                "error_class": collection_outcome.error_class,
                "run_id": collection_outcome.run_id,
                "terminal_event": collection_outcome.terminal_event.value,
                "universe_run_id": universe_outcome.run_id,
            },
            budget,
        )
    output.write(
        json.dumps(
            {
                "classification_path": str(plan.classification_path),
                "collection_run_id": collection_outcome.run_id,
                "new_listing_candidates": len(
                    classification.symbols(DailyDisposition.NEW_LISTING_CANDIDATE)
                ),
                "provider_calls": universe_calls + collection_calls,
                "recovered_run_ids": list(recovered_run_ids),
                "receipt_path": str(plan.receipt_path),
                "service_day": service_day.isoformat(),
                "terminal_event": collection_outcome.terminal_event.value,
                "universe_run_id": universe_outcome.run_id,
                **({} if budget is None else budget.summary()),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    return 0
