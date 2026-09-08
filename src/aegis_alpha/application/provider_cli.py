from __future__ import annotations

# ruff: noqa: PLC0415 -- provider/DB imports are deferred until explicit execution.
import argparse
import io
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from aegis_alpha.application.data_config import load_data_config, read_secret
from aegis_alpha.application.provider_config import (
    CollectionConfig,
    ProviderProfile,
    load_collection_config,
    load_provider_environment,
)

_PRECONDITION_EXIT = 2
_MAX_CATALOG_RECOVERY_RUNS = 1000

_CAPABILITIES = {
    "qveris": ("bounded_raw_collection", ("explicit_pinned_jobs",)),
    "norgate": ("historical_local_publication", ("security_master", "historical_prices")),
    "fmp": (
        "signed_daily_collection",
        (
            "eod_raw",
            "eod_split_adjusted",
            "eod_total_return",
            "splits",
            "dividends",
            "profile",
            "active_delisted_universe",
        ),
    ),
    "fred_alfred": ("signed_vintage_collection", ("series", "vintage_dates", "observations")),
    "sec": ("official_verification", ("submissions", "companyfacts", "13f_index")),
    "finimpulse": ("gated_estimates_candidate", ("earnings_analysis",)),
}
_CREDENTIALS = {
    "fmp": "FMP_API_KEY",
    "fred_alfred": "FRED_API_KEY",
    "sec": "SEC_USER_AGENT",
    "finimpulse": "FINIMPULSE_API_TOKEN",
}
_INPUT_PATHS = frozenset(
    {
        "jobs",
        "registry",
        "storage_notification",
        "tier",
        "recurring_authority",
        "recurring_authority_signature",
        "identity_snapshot",
        "universe_file",
        "identity_export",
        "gate_evidence",
    }
)


def add_commands(commands: argparse._SubParsersAction) -> None:
    providers = commands.add_parser(
        "providers", help="List implemented provider coverage and configuration"
    )
    providers.add_argument("--config", type=Path)
    collect = commands.add_parser(
        "collect", help="Plan or execute one bounded configured collection"
    )
    actions = collect.add_subparsers(dest="collect_command", required=True)
    state_root = Path(
        os.environ.get(
            "AAS_COLLECTION_STATE", str(Path.home() / ".local/share/aegis-alpha/collection-state")
        )
    )
    config_path = Path(
        os.environ.get(
            "AAS_COLLECTION_CONFIG", str(Path.home() / ".local/share/aegis-alpha/collection.json")
        )
    )
    daily = actions.add_parser(
        "daily", help="Run each enabled provider at most once per service day"
    )
    daily.add_argument("--config", type=Path, default=config_path)
    daily.add_argument("--state-root", type=Path, default=state_root)
    status = actions.add_parser(
        "status", help="Read automatic collection receipts and last successes"
    )
    status.add_argument("--state-root", type=Path, default=state_root)
    recovery = actions.add_parser(
        "recover", help="Finish verified FMP catalog registrations without HTTP"
    )
    recovery.add_argument("--config", type=Path, default=config_path)
    recovery.add_argument("--provider", choices=("fmp",), required=True)
    recovery.add_argument("--limit", type=int, default=100)
    recovery.add_argument("--after-run-id", help="Continue after the previous recovery page cursor")
    for name in ("plan", "run"):
        parser = actions.add_parser(name)
        parser.add_argument("--config", type=Path, default=config_path)
        parser.add_argument("--provider", choices=tuple(_CAPABILITIES), required=True)
        if name == "run":
            parser.add_argument("--run-id", help="Explicit FRED/SEC identity for same-run recovery")


def provider_inventory() -> list[dict[str, object]]:
    return [
        {
            "provider": name,
            "capability": value[0],
            "endpoints": list(value[1]),
            "live_verified": False,
        }
        for name, value in _CAPABILITIES.items()
    ]


def _configuration_reasons(profile: ProviderProfile) -> list[str]:
    reasons = []
    if not profile.enabled:
        reasons.append("disabled_in_profile")
    reasons.extend("missing_option:" + name for name in profile.missing_options())
    if profile.provider == "finimpulse":
        reasons.append("finimpulse_requires_reviewed_gate_and_identity_artifacts_and_call_bound")
    for key in _INPUT_PATHS & profile.options.keys():
        value = profile.options[key]
        if not isinstance(value, str) or not Path(value).is_file():
            reasons.append("missing_input:" + key)
    if (
        profile.mode == "backfill"
        and profile.provider == "fred_alfred"
        and "observation_start" not in profile.options
    ):
        reasons.append("missing_option:observation_start")
    reasons.extend(_credential_reasons(profile))
    return reasons


def _credential_reasons(profile: ProviderProfile) -> list[str]:
    reasons = []
    if profile.provider == "qveris":
        if profile.credential_file is None or not profile.credential_file.is_file():
            reasons.append("missing_credential_file")
    elif profile.provider != "norgate":
        if profile.credential_file is None or not profile.credential_file.is_file():
            reasons.append("missing_credential_file")
        else:
            try:
                environment = load_provider_environment(profile)
                if not environment.get(_CREDENTIALS[profile.provider]):
                    reasons.append("missing_credential:" + _CREDENTIALS[profile.provider])
            except (ValueError, OSError, UnicodeError):
                reasons.append("invalid_private_credential_file")
    return reasons


def collection_plan(profile: ProviderProfile) -> dict[str, object]:
    reasons = _configuration_reasons(profile)
    return {
        "provider": profile.provider,
        "mode": profile.mode,
        "status": "blocked" if reasons else "configured_unverified",
        "reasons": reasons,
        "max_calls": profile.max_calls,
        "provider_calls": 0,
        "authority_verified": False,
        "live_verified": False,
        "data_eligibility_changed": False,
    }


def _arguments(profile: ProviderProfile, run_id: str | None) -> list[str]:
    arguments = ["--max-calls", str(profile.max_calls)]
    if profile.provider == "fmp" and profile.mode == "universe":
        arguments += ["--universe-only"]
    if profile.provider != "fmp":
        arguments += ["--mode", profile.mode]
    if profile.provider == "sec":
        arguments += ["--live"]
    if run_id is not None:
        flag = "--run-id" if profile.provider == "fred_alfred" else "--run-identity"
        arguments += [flag, run_id]
    for key, value in profile.options.items():
        if profile.provider == "fmp" and key in {"raw_store_root", "dataset_root"}:
            continue
        arguments.extend(_option_arguments(key, value, run_id))
    return arguments


def _option_arguments(key: str, value: str | tuple[str, ...], run_id: str | None) -> list[str]:
    flag = "--instrument-id" if key == "instrument_ids" else "--" + key.replace("_", "-")
    if isinstance(value, tuple):
        return [part for item in value for part in (flag, item)]
    if key == "receipt_path" and run_id is not None:
        base = Path(value)
        value = str(base.with_name(f"{base.stem}.{run_id}{base.suffix}"))
    return [flag, value]


def _dispatch(
    profile: ProviderProfile,
    environment: dict[str, str],
    stdout: io.StringIO,
    stderr: io.StringIO,
    run_id: str | None,
) -> int:
    arguments = _arguments(profile, run_id)
    match profile.provider:
        case "fmp":
            from aegis_alpha.data.fmp_daily_cli import main
        case "fred_alfred":
            from aegis_alpha.data.fred_alfred_collector_cli import main
        case "sec":
            from aegis_alpha.data.sec_collector_cli import main
        case _:
            raise ValueError("provider has no admitted network dispatch")
    return main(arguments, environ=environment, stdout=stdout, stderr=stderr)


def _redact(value: str, environment: dict[str, str]) -> str:
    variants = {
        encoded
        for secret in environment.values()
        if secret
        for encoded in (
            secret,
            json.dumps(secret)[1:-1],
            json.dumps(secret, ensure_ascii=False)[1:-1],
        )
    }
    for secret in sorted(variants, key=len, reverse=True):
        value = value.replace(secret, "[redacted]")
    return value


def _redact_document(value: object, environment: dict[str, str]) -> object:
    if isinstance(value, str):
        return _redact(value, environment)
    if isinstance(value, list):
        return [_redact_document(item, environment) for item in value]
    if isinstance(value, dict):
        return {
            _redact(str(key), environment): _redact_document(item, environment)
            for key, item in value.items()
        }
    return value


def _safe_output(value: str, environment: dict[str, str]) -> object:
    if not value.strip():
        return None
    try:
        return _redact_document(json.loads(value), environment)
    except ValueError:
        return {"text": _redact(value, environment)}


def run_collection_profile(
    config: CollectionConfig, profile: ProviderProfile, *, run_id: str | None = None
) -> dict[str, object]:
    if run_id is not None and (
        profile.provider not in {"fred_alfred", "sec"}
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", run_id) is None
    ):
        raise ValueError("explicit run identity requires FRED/SEC and a safe bounded identifier")
    if profile.provider in {"fred_alfred", "sec"} and run_id is None:
        run_id = f"aas-{profile.provider}-{uuid4().hex}"
    plan = collection_plan(profile)
    if plan["status"] == "blocked":
        return {**plan, "exit_code": 2, "execution_started": False}
    if profile.provider == "norgate":
        return _inspect_norgate(config, profile)
    if profile.provider == "qveris":
        from aegis_alpha.application.qveris_collection import run_qveris_profile

        return run_qveris_profile(profile)
    data = load_data_config(config.data_config)
    environment = load_provider_environment(profile)
    database_url = read_secret(data.database_url_file)
    environment["AAS_DATABASE_URL"] = database_url
    environment["AAS_FRED_CONTROL_PLANE_DATABASE_URL"] = database_url
    # Preserve only the explicitly selected credential assignments, never host env or old DB URLs.
    stdout, stderr = io.StringIO(), io.StringIO()
    started = datetime.now(UTC)
    try:
        code = _dispatch(profile, environment, stdout, stderr, run_id)
    except Exception as error:  # noqa: BLE001 -- public CLI boundary must never echo credential-bearing exceptions
        return {
            "provider": profile.provider,
            "status": "failed",
            "exit_code": 1,
            "execution_started": True,
            "run_id": run_id,
            "error_type": type(error).__name__,
            "message": "provider execution failed; inspect durable run records",
            "provider_calls": None,
        }
    payload = _safe_output(stdout.getvalue() or stderr.getvalue(), environment)
    diagnostic = _redact(stderr.getvalue(), environment)
    return {
        "provider": profile.provider,
        "mode": profile.mode,
        "exit_code": code,
        "status": "succeeded"
        if code == 0
        else "blocked"
        if code == _PRECONDITION_EXIT
        else "failed",
        "execution_started": True,
        "started_at_utc": started.isoformat(),
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "run_id": run_id,
        "result": payload,
        "diagnostic": diagnostic,
        "max_calls": profile.max_calls,
        "data_eligibility_changed": False,
    }


def _inspect_norgate(config: CollectionConfig, profile: ProviderProfile) -> dict[str, object]:
    from sqlalchemy.exc import SQLAlchemyError

    from aegis_alpha.data.catalog_access import load_dataset
    from aegis_alpha.metadata.runtime_install import runtime_engine

    data = load_data_config(config.data_config)
    engine = runtime_engine(read_secret(data.database_url_file), read_only=True)
    try:
        dataset, version = profile.options["dataset_id"], profile.options["dataset_version"]
        if not isinstance(dataset, str) or not isinstance(version, str):
            raise TypeError("Norgate requires exact catalog identities")
        view = load_dataset(engine, dataset, version)
        return {
            "provider": "norgate",
            "status": "inspected",
            "exit_code": 0,
            "provider_calls": 0,
            "network_collector": False,
            "dataset": view.to_dict(),
        }
    except SQLAlchemyError:
        raise ValueError(
            "database catalog inspection failed; check connection and schema"
        ) from None
    finally:
        engine.dispose()


def execute(args: argparse.Namespace) -> dict[str, object]:
    if args.command == "providers":
        result = {"providers": provider_inventory()}
        if args.config is not None:
            config = load_collection_config(args.config)
            result["configuration"] = [collection_plan(profile) for profile in config.providers]
        return result
    if args.collect_command == "status":
        from aegis_alpha.application.daily_collection import daily_status

        return daily_status(args.state_root)
    config = load_collection_config(args.config)
    if args.collect_command == "daily":
        from aegis_alpha.application.daily_collection import run_daily

        return run_daily(config, args.state_root)
    profile = config.select(args.provider)
    if args.collect_command == "plan":
        return collection_plan(profile)
    if args.collect_command == "recover":
        return recover_collection_catalog(
            config, profile, args.limit, after_run_id=args.after_run_id
        )
    return run_collection_profile(config, profile, run_id=args.run_id)


def recover_collection_catalog(
    config: CollectionConfig,
    profile: ProviderProfile,
    limit: int,
    *,
    after_run_id: str | None = None,
) -> dict[str, object]:
    if (
        profile.provider != "fmp"
        or type(limit) is not int
        or not 1 <= limit <= _MAX_CATALOG_RECOVERY_RUNS
    ):
        raise ValueError(
            f"catalog recovery requires FMP and a limit from 1 to {_MAX_CATALOG_RECOVERY_RUNS}"
        )
    roots = [profile.options.get(name) for name in ("raw_store_root", "dataset_root")]
    if any(not isinstance(root, str) for root in roots):
        raise ValueError("catalog recovery needs explicit raw_store_root and dataset_root")
    from sqlalchemy.exc import SQLAlchemyError

    from aegis_alpha.data.fmp_catalog import recover_completed_collections
    from aegis_alpha.metadata.runtime_install import runtime_engine

    data = load_data_config(config.data_config)
    engine = runtime_engine(read_secret(data.database_url_file))
    try:
        raw, datasets = roots
        if not isinstance(raw, str) or not isinstance(datasets, str):
            raise TypeError("catalog recovery roots must be strings")
        result = recover_completed_collections(
            engine, Path(raw), Path(datasets), limit=limit, after_run_id=after_run_id
        )
    except SQLAlchemyError:
        raise ValueError(
            "database catalog recovery failed; inspect retained run evidence"
        ) from None
    finally:
        engine.dispose()
    return {
        "provider": "fmp",
        "status": result["status"],
        "provider_calls": 0,
        "exit_code": 0 if result["status"] == "catalog_complete" else 1,
        "result": result,
    }
