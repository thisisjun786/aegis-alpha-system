from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from aegis_alpha.application.collection_journal import (
    CollectionBusyError,
    collection_lock,
    encoded,
    journal_status,
    publish,
    records,
    revalidate_root,
)
from aegis_alpha.application.provider_config import CollectionConfig, ProviderProfile

Runner = Callable[[CollectionConfig, ProviderProfile, str | None], dict[str, object]]
_SCHEDULE_MODES = {"fmp": "daily", "fred_alfred": "incremental", "sec": "incremental"}


def _profile_hash(config: CollectionConfig, profile: ProviderProfile) -> str:
    return hashlib.sha256(
        encoded(
            {
                "data_config": str(config.data_config),
                "provider": profile.provider,
                "mode": profile.mode,
                "max_calls": profile.max_calls,
                "options": {
                    key: list(value) if isinstance(value, tuple) else value
                    for key, value in profile.options.items()
                },
            }
        )
    ).hexdigest()


def _call_count(provider: str, report: Mapping[str, object], maximum: int) -> int | None:
    if report.get("execution_started") is False:
        return 0
    payload = report.get("result")
    if not isinstance(payload, dict):
        return None
    key = "invocation_calls_attempted" if provider == "fmp" else "provider_calls"
    value = payload.get(key)
    return value if type(value) is int and 0 <= value <= maximum else None


def _outcome(report: Mapping[str, object]) -> str:
    payload = report.get("result")
    if isinstance(payload, dict) and payload.get("partial") is True:
        return "partial"
    status = report.get("status")
    return (
        status
        if isinstance(status, str) and status in {"succeeded", "failed", "blocked"}
        else "unknown"
    )


def _execute(
    config: CollectionConfig, profile: ProviderProfile, run_id: str | None
) -> dict[str, object]:
    from aegis_alpha.application.provider_cli import (  # noqa: PLC0415 -- runtime-only CLI cycle
        run_collection_profile,
    )

    return run_collection_profile(config, profile, run_id=run_id)


def run_daily(
    config: CollectionConfig,
    root: Path,
    *,
    runner: Runner = _execute,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, object]:
    moment = clock()
    if not isinstance(moment, datetime) or moment.utcoffset() is None:
        raise ValueError("daily collection clock must be timezone-aware")
    day = moment.astimezone(UTC).date().isoformat()
    outcomes = []
    try:
        with collection_lock(root) as tree:
            history = records(tree)
            by_provider = {
                str(entry["start"]["provider"]): entry
                for entry in history
                if entry["start"]["service_day"] == day
            }
            for profile in config.providers:
                if not profile.enabled:
                    outcomes.append({"provider": profile.provider, "status": "disabled"})
                    continue
                prior = by_provider.get(profile.provider)
                if prior is not None:
                    finish = prior["finish"]
                    outcomes.append(
                        {
                            "provider": profile.provider,
                            "status": "unknown" if finish is None else finish["status"],
                            "already_admitted": True,
                        }
                    )
                    continue
                digest = _profile_hash(config, profile)
                invocation = f"daily-{profile.provider}-{day}-{digest[:16]}"
                start = {
                    "version": 1,
                    "service_day": day,
                    "provider": profile.provider,
                    "invocation_id": invocation,
                    "profile_sha256": digest,
                    "reserved_calls": profile.max_calls,
                    "started_at_utc": clock().isoformat(),
                }
                prefix = f"{day}.{profile.provider}."
                publish(tree, prefix + "start.json", start)
                revalidate_root(tree, root)
                try:
                    if _SCHEDULE_MODES.get(profile.provider) != profile.mode:
                        report = {
                            "provider": profile.provider,
                            "status": "blocked",
                            "exit_code": 2,
                            "execution_started": False,
                            "reasons": ["unsupported_scheduled_mode"],
                        }
                    else:
                        run_id = invocation if profile.provider in {"fred_alfred", "sec"} else None
                        report = runner(config, profile, run_id)
                except Exception as error:  # noqa: BLE001 -- preserve admission and avoid credential-bearing diagnostics
                    report = {
                        "status": "failed",
                        "exit_code": 1,
                        "error_type": type(error).__name__,
                        "execution_started": True,
                        "message": "provider failed; inspect durable evidence",
                    }
                calls = _call_count(profile.provider, report, profile.max_calls)
                status = _outcome(report)
                if status == "succeeded" and calls is None:
                    status = "unknown"
                finish = {
                    "version": 1,
                    "service_day": day,
                    "provider": profile.provider,
                    "start_sha256": hashlib.sha256(encoded(start)).hexdigest(),
                    "finished_at_utc": clock().isoformat(),
                    "status": status,
                    "calls_attempted": calls,
                    "result": report,
                }
                revalidate_root(tree, root)
                publish(tree, prefix + "result.json", finish)
                outcomes.append(
                    {
                        "provider": profile.provider,
                        "status": finish["status"],
                        "calls_attempted": finish["calls_attempted"],
                        "invocation_id": invocation,
                    }
                )
    except CollectionBusyError:
        return {
            "service_day": day,
            "status": "already_running",
            "exit_code": 0,
            "provider_calls": 0,
        }
    complete = all(item["status"] in {"succeeded", "disabled"} for item in outcomes)
    enabled = any(profile.enabled for profile in config.providers)
    return {
        "service_day": day,
        "status": "succeeded" if complete and enabled else "disabled" if not enabled else "partial",
        "providers": outcomes,
        "exit_code": 0 if complete else 1,
        "catch_up": "current_incremental_watermarks",
        "automatic_invocations_per_provider_day": 1,
    }


def daily_status(root: Path) -> dict[str, object]:
    observed = journal_status(root)
    latest: dict[str, dict[str, object]] = {}
    successes: dict[str, str] = {}
    for entry in cast("list[dict[str, dict[str, object] | None]]", observed["records"]):
        start, finish = entry["start"], entry["finish"]
        if start is None:
            raise ValueError("collection admission is missing")
        provider, day = str(start["provider"]), str(start["service_day"])
        latest[provider] = {
            "service_day": day,
            "status": "unknown" if finish is None else finish["status"],
            "invocation_id": start["invocation_id"],
            "reserved_calls": start["reserved_calls"],
            "calls_attempted": None if finish is None else finish["calls_attempted"],
        }
        if finish is not None and finish["status"] == "succeeded":
            successes[provider] = day
    return {"active": observed["active"], "providers": latest, "last_success": successes}
