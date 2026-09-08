"""Signed-gated FRED CLI; planning stays read-only and live execution is durable."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final, TextIO

from aegis_alpha.collection.records import CollectionMode
from aegis_alpha.data.fred_alfred_collector import (
    CREDENTIAL_ENVIRONMENT_VARIABLE,
    CollectorConfig,
    DestinationError,
    Transport,
    new_run_id,
    validate_destination,
)
from aegis_alpha.data.fred_alfred_owner_authority import (
    OWNER_AUTHORITY_ENV,
    load_owner_authority,
)
from aegis_alpha.data.fred_alfred_policy import (
    FredAlfredPolicy,
    load_fred_alfred_policy,
    policy_blocks_live,
)
from aegis_alpha.data.fred_alfred_recurring_authority import (
    VerifiedRecurringAuthority,
    load_recurring_authority,
    recurring_authority_issued_at,
    verify_recurring_authority,
)
from aegis_alpha.data.fred_alfred_recurring_errors import DailyBudgetError
from aegis_alpha.data.fred_alfred_series import (
    DEFAULT_SERIES_IDS,
    MACRO_SERIES_IDS,
    series_universe_sha256,
    validate_series_universe,
)
from aegis_alpha.data.fred_alfred_usage_budget import (
    connect_control_plane,
    require_control_plane_url,
    require_remaining_budget,
)

PRECONDITION_EXIT: Final = 2


class PreconditionError(RuntimeError):
    """A hard gate failed, so the run performs zero provider calls."""


@dataclass(frozen=True, slots=True)
class PreflightResult:
    policy: FredAlfredPolicy
    authority: VerifiedRecurringAuthority | None
    series_ids: tuple[str, ...]
    artifact_hashes: Mapping[str, str]


def run_preflight(  # noqa: PLR0913, C901 - each argument is one explicit G-A gate
    *,
    registry_path: Path,
    recurring_authority_path: Path | None,
    recurring_signature_path: Path | None,
    environment: Mapping[str, str],
    max_calls: int,
    raw_store_root: Path,
    dataset_root: Path,
    receipt_path: Path,
    series_ids: Sequence[str],
    now: datetime,
    require_live_gates: bool,
) -> PreflightResult:
    if max_calls < 1:
        raise PreconditionError("--max-calls is mandatory and must be a positive integer")
    validated = validate_series_universe(series_ids)
    resolved_raw_store_root = validate_destination("raw store root", raw_store_root)
    resolved_dataset_root = validate_destination("dataset root", dataset_root)
    _ = validate_destination("receipt path", receipt_path)
    policy = load_fred_alfred_policy(registry_path)
    authority: VerifiedRecurringAuthority | None = None
    if require_live_gates:
        if policy_blocks_live(policy):
            raise PreconditionError(
                f"registry policy {policy.policy_id} is "
                f"{policy.license_classification} with scheduled_collection_allowed="
                f"{policy.scheduled_collection_allowed}; live collection requires a scheduled "
                "registry policy and no provider calls were attempted"
            )
        if recurring_authority_path is None or recurring_signature_path is None:
            raise PreconditionError(
                "--recurring-authority and --recurring-authority-signature are required and "
                "no provider calls were attempted"
            )
        owner_authority_path = environment.get(OWNER_AUTHORITY_ENV)
        if not owner_authority_path:
            raise PreconditionError(
                f"{OWNER_AUTHORITY_ENV} is required and no provider calls were attempted"
            )
        payload, signature = load_recurring_authority(
            recurring_authority_path, recurring_signature_path
        )
        trust = load_owner_authority(
            Path(owner_authority_path), recurring_authority_issued_at(payload)
        )
        authority = verify_recurring_authority(payload, signature, trust, now=now)
        if (
            resolved_raw_store_root != authority.raw_store_root
            or resolved_dataset_root != authority.dataset_root
        ):
            raise PreconditionError(
                "run roots do not match the signed standing authority roots; "
                "no provider calls were attempted"
            )
        if max_calls > authority.calls_per_day:
            raise PreconditionError(
                "--max-calls exceeds the signed standing authority calls_per_day budget; "
                "no provider calls were attempted"
            )
        database_url = require_control_plane_url(environment)
        engine = connect_control_plane(database_url)
        try:
            require_remaining_budget(
                engine=engine,
                authority=authority,
                requested_calls=max_calls,
                now=now,
            )
        except DailyBudgetError as error:
            raise PreconditionError(f"{error}; no provider calls were attempted") from error
        finally:
            engine.dispose()
    elif recurring_authority_path is not None or recurring_signature_path is not None:
        raise PreconditionError(
            "--recurring-authority is only accepted for live gated runs, not dry-run; "
            "no provider calls were attempted"
        )
    hashes = {"registry": hashlib.sha256(registry_path.read_bytes()).hexdigest()}
    if authority is not None:
        hashes["recurring_authority"] = authority.payload_sha256
        hashes["recurring_authority_signature"] = authority.signature_sha256
    return PreflightResult(
        policy=policy,
        authority=authority,
        series_ids=validated,
        artifact_hashes=hashes,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bounded, signed-gated durable FRED/ALFRED collector",
    )
    parser.add_argument(
        "--mode",
        choices=("probe", "incremental", "backfill"),
        required=True,
    )
    parser.add_argument("--max-calls", type=int, required=True)
    parser.add_argument("--run-id", help="stable live run identity for verified recovery")
    parser.add_argument(
        "--registry", type=Path, default=Path("config/data_authority_registry.json")
    )
    parser.add_argument("--recurring-authority", type=Path, default=None)
    parser.add_argument("--recurring-authority-signature", type=Path, default=None)
    parser.add_argument("--raw-store-root", type=Path, required=True)
    parser.add_argument("--dataset-root", type=Path, required=True)
    parser.add_argument("--receipt-path", type=Path, required=True)
    parser.add_argument("--credential-env", default=CREDENTIAL_ENVIRONMENT_VARIABLE)
    parser.add_argument("--observation-start", default=None)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--series",
        action="append",
        dest="series_ids",
        default=None,
        help=(
            "explicit subset of the frozen macro catalog; extra series are rejected; "
            "default is the legacy four-series universe"
        ),
    )
    selection.add_argument(
        "--macro-universe",
        action="store_true",
        help="select the full frozen macro catalog instead of the legacy four-series default",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the planned series universe and stop before any live path",
    )
    return parser


def _parse_observation_start(raw: str | None) -> date | None:
    if raw is None:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise PreconditionError("--observation-start must be an ISO calendar date") from None


def _select_series(arguments: argparse.Namespace) -> tuple[str, ...]:
    """Resolve the requested universe. argparse already forbids combining the flags."""

    if arguments.macro_universe:
        return MACRO_SERIES_IDS
    if arguments.series_ids:
        return tuple(arguments.series_ids)
    return DEFAULT_SERIES_IDS


def _selection_label(arguments: argparse.Namespace) -> str:
    if arguments.macro_universe:
        return "macro"
    if arguments.series_ids:
        return "explicit"
    return "legacy4"


def main(  # noqa: PLR0913 -- standard CLI plus injectable HTTP and clock ports
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    now: datetime | None = None,
    transport: Transport | None = None,
) -> int:
    arguments = _build_parser().parse_args(argv)
    environment = os.environ if environ is None else environ
    output = sys.stdout if stdout is None else stdout
    error_output = sys.stderr if stderr is None else stderr
    moment = datetime.now(UTC) if now is None else now
    series_ids = _select_series(arguments)

    if arguments.mode == CollectionMode.BACKFILL.value:
        start = _parse_observation_start(arguments.observation_start)
        if start is None:
            error_output.write(
                "backfill requires --observation-start; no provider calls were attempted\n"
            )
            return PRECONDITION_EXIT

    credential = environment.get(arguments.credential_env, "")
    if not arguments.dry_run and not credential:
        error_output.write(
            f"{arguments.credential_env} is not set; no provider calls were attempted\n"
        )
        return PRECONDITION_EXIT

    try:
        result = run_preflight(
            registry_path=arguments.registry,
            recurring_authority_path=arguments.recurring_authority,
            recurring_signature_path=arguments.recurring_authority_signature,
            environment=environment,
            max_calls=arguments.max_calls,
            raw_store_root=arguments.raw_store_root,
            dataset_root=arguments.dataset_root,
            receipt_path=arguments.receipt_path,
            series_ids=series_ids,
            now=moment,
            require_live_gates=not arguments.dry_run,
        )
    except (PreconditionError, DestinationError, ValueError, OSError) as error:
        error_output.write(f"{error}; no provider calls were attempted\n")
        return PRECONDITION_EXIT

    payload = {
        "command": "dry-run" if arguments.dry_run else "collect",
        "mode": arguments.mode,
        "provider": "fred_alfred",
        "policy_id": result.policy.policy_id,
        "license_classification": result.policy.license_classification,
        "scheduled_collection_allowed": result.policy.scheduled_collection_allowed,
        "series_ids": list(result.series_ids),
        "series_universe_sha256": series_universe_sha256(result.series_ids),
        "default_series_universe_sha256": series_universe_sha256(DEFAULT_SERIES_IDS),
        "macro_series_universe_sha256": series_universe_sha256(MACRO_SERIES_IDS),
        "series_selection": _selection_label(arguments),
        "max_calls": arguments.max_calls,
        "artifact_hashes": dict(sorted(result.artifact_hashes.items())),
        "provider_calls": 0,
    }
    if arguments.dry_run:
        output.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
        return 0

    run_id = arguments.run_id or new_run_id()
    receipt_path = arguments.receipt_path
    if arguments.run_id is None:
        receipt_path = receipt_path.with_stem(f"{receipt_path.stem}-{run_id}")
    if result.authority is None:
        raise AssertionError("live preflight must return verified standing authority")
    from aegis_alpha.data.fred_alfred_runtime import run_runtime  # noqa: PLC0415 -- lazy runtime

    engine = connect_control_plane(require_control_plane_url(environment))
    try:
        config = CollectorConfig(
            raw_store_root=arguments.raw_store_root,
            dataset_root=arguments.dataset_root,
            receipt_path=receipt_path,
            mode=CollectionMode(arguments.mode),
            max_calls=arguments.max_calls,
            run_identity=run_id,
            series_ids=result.series_ids,
            observation_start=_parse_observation_start(arguments.observation_start),
            artifact_hashes=result.artifact_hashes,
        )
        outcome = run_runtime(
            config=config,
            engine=engine,
            authority=result.authority,
            credential=credential,
            transport=transport,
        )
    except Exception as error:  # noqa: BLE001 -- provider/DB errors may carry credentials
        error_output.write(
            f"FRED runtime refused or interrupted ({type(error).__name__}); "
            "inspect durable run evidence\n"
        )
        return 1
    finally:
        engine.dispose()
    payload.update(
        run_id=outcome.run_id,
        provider_calls=0 if outcome.recovered else outcome.calls_attempted,
        run_calls_attempted=outcome.calls_attempted,
        recovered=outcome.recovered,
        partial=any(not item.succeeded for item in outcome.series_outcomes),
        failed_series=[item.series_id for item in outcome.series_outcomes if not item.succeeded],
        status=outcome.terminal_event.value,
        published_paths=[str(path) for path in outcome.published_paths],
    )
    output.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    return 0 if outcome.terminal_event.value == "run_succeeded" else 1


__all__ = [
    "PRECONDITION_EXIT",
    "FredAlfredPolicy",
    "PreconditionError",
    "load_fred_alfred_policy",
    "main",
    "policy_blocks_live",
    "run_preflight",
]
