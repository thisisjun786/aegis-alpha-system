"""``aas collect dart`` and ``aas collect kind``: KR collection into the installation."""

from __future__ import annotations

# ruff: noqa: PLC0415 -- lazy storage imports keep help independent of database drivers.
import argparse
from datetime import date
from pathlib import Path
from typing import cast

from aegis_alpha.application.storage_cli import home_option


def _day(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("dates are YYYY-MM-DD") from None


def add_commands(actions: argparse._SubParsersAction) -> None:
    dart = actions.add_parser(
        "dart", help="Plan or run the rolling OpenDART cohort (corp codes, list.json, statements)"
    )
    dart_actions = dart.add_subparsers(dest="dart_command", required=True)
    plan = dart_actions.add_parser(
        "plan", help="Report what a run would ask; no provider call and no write"
    )
    home_option(plan)
    plan.add_argument("--today", type=_day, help="Seoul date to plan for (default: now)")
    plan.add_argument(
        "--legacy-root",
        type=Path,
        help="Replay a legacy opendart directory (attempts.jsonl) instead of the installation",
    )
    plan.add_argument(
        "--kind-receipt",
        type=Path,
        action="append",
        default=[],
        help="With --legacy-root: a KIND response.json narrowing the corps to its listings",
    )
    run = dart_actions.add_parser("run", help="Run one bounded OpenDART collection")
    home_option(run)
    run.add_argument("--key-file", type=Path, required=True, help="Owner-only OpenDART key")
    run.add_argument("--max-calls", type=int, default=2_000)
    run.add_argument("--daily-quota", type=int, default=19_000)
    kind = actions.add_parser("kind", help="Collect KIND's KOSPI and KOSDAQ listed-company lists")
    kind_actions = kind.add_subparsers(dest="kind_command", required=True)
    kind_run = kind_actions.add_parser("run", help="Fetch both lists and commit them")
    home_option(kind_run)


def _legacy_plan(args: argparse.Namespace, today: date) -> dict[str, object]:
    from aegis_alpha.data.opendart_cohort import CohortPolicy
    from aegis_alpha.data.opendart_legacy import replay
    from aegis_alpha.storage import kr_identity
    from aegis_alpha.storage.kr_collection import plan_report

    replayed = replay(args.legacy_root)
    if args.kind_receipt:
        codes = set()
        for path in args.kind_receipt:
            unit = kr_identity.read_kind_receipt(path)
            column = unit.columns.index("short_code")
            codes.update(str(row[column]) for row in unit.rows)
        replayed.knowledge.kind_codes = frozenset(codes)
    return plan_report(replayed.knowledge, today=today, policy=CohortPolicy()) | {
        "legacy": replayed.report()
    }


def execute(args: argparse.Namespace) -> dict[str, object]:
    import sqlite3

    import duckdb

    from aegis_alpha.data.opendart import utc_now
    from aegis_alpha.data.opendart_cohort import CohortPolicy, seoul_day
    from aegis_alpha.storage.paths import resolve_home
    from aegis_alpha.storage.workspace import open_workspace

    home = resolve_home(getattr(args, "home", None))
    try:
        if args.collect_command == "kind":
            from aegis_alpha.data.opendart import urllib_transport
            from aegis_alpha.storage.kr_collection import collect_kind

            with open_workspace(
                home, writable=True, strategy_write=True, require_strategies=False
            ) as workspace:
                result = collect_kind(workspace, urllib_transport(), clock=utc_now)
            lists = cast("list[dict[str, object]]", result["lists"])
            failed = any(item.get("status") != "committed" for item in lists)
            return result | {"exit_code": int(failed)}
        if args.dart_command == "plan":
            today = args.today or seoul_day(utc_now())
            if args.legacy_root is not None:
                return _legacy_plan(args, today)
            if args.kind_receipt:
                raise ValueError("--kind-receipt reads only with --legacy-root")
            from aegis_alpha.storage.kr_collection import plan_dart

            with open_workspace(home, writable=False, require_strategies=False) as workspace:
                return plan_dart(workspace, today=today, policy=CohortPolicy())
        from aegis_alpha.application.data_config import read_secret
        from aegis_alpha.data.opendart import OpenDartClient, urllib_transport
        from aegis_alpha.storage.kr_collection import collect_dart

        client = OpenDartClient(read_secret(args.key_file.absolute()), urllib_transport())
        with open_workspace(
            home, writable=True, strategy_write=True, require_strategies=False
        ) as workspace:
            result = collect_dart(
                workspace,
                client,
                clock=utc_now,
                max_calls=args.max_calls,
                daily_quota=args.daily_quota,
            )
        # A run that spent its budget ends normally; a refused key or failing transport is
        # reported, with every answer it retained committed, and exits 1.
        return result | {"exit_code": int(result["stopped"] not in {None, "budget"})}
    except (sqlite3.Error, duckdb.Error):
        raise ValueError("local database operation failed; run aas db verify") from None
