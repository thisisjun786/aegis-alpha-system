"""``aas collect fred`` and ``aas collect sec``: US macro and filing collection."""

from __future__ import annotations

# ruff: noqa: PLC0415 -- lazy storage imports keep help independent of database drivers.
import argparse
from datetime import UTC, date, datetime, time
from pathlib import Path

from aegis_alpha.application.storage_cli import home_option


def _day(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("dates are YYYY-MM-DD") from None


def add_commands(actions: argparse._SubParsersAction) -> None:
    fred = actions.add_parser(
        "fred", help="Plan or run the native FRED/ALFRED and FRED CSV collection"
    )
    fred_actions = fred.add_subparsers(dest="fred_command", required=True)
    plan = fred_actions.add_parser(
        "plan", help="Report what a run would ask; no provider call and no write"
    )
    home_option(plan)
    plan.add_argument("--today", type=_day, help="FRED (St. Louis) date to plan for")
    run = fred_actions.add_parser("run", help="Run one bounded FRED collection")
    home_option(run)
    run.add_argument("--key-file", type=Path, required=True, help="Owner-only FRED API key")
    run.add_argument("--max-calls", type=int, default=500)
    sec = actions.add_parser("sec", help="Plan or run the native SEC EDGAR collection")
    sec_actions = sec.add_subparsers(dest="sec_command", required=True)
    for name, text in (
        ("plan", "Report what a run would ask; no provider call and no write"),
        ("run", "Run one bounded SEC collection"),
    ):
        parser = sec_actions.add_parser(name, help=text)
        home_option(parser)
        parser.add_argument("--since", type=_day, help="First index day to read (YYYY-MM-DD)")
        parser.add_argument(
            "--issuers",
            choices=("registered", "all"),
            default="registered",
            help="Collect documents of registered SEC issuers only (default) or of every filer",
        )
        if name == "plan":
            parser.add_argument("--today", type=_day, help="EDGAR (New York) date to plan for")
        else:
            parser.add_argument(
                "--user-agent-file",
                type=Path,
                required=True,
                help="Owner-only file holding SEC's fair-access User-Agent with a contact",
            )
            parser.add_argument("--max-calls", type=int, default=2_000)


def _plan_instant(today: date | None) -> datetime:
    from aegis_alpha.data.opendart import utc_now

    if today is None:
        return utc_now()
    return datetime.combine(today, time(12), tzinfo=UTC)


def _fred(args: argparse.Namespace, home: Path) -> dict[str, object]:
    from aegis_alpha.data import fred_collect as fred
    from aegis_alpha.data.opendart import urllib_transport, utc_now
    from aegis_alpha.storage.fred_collection import collect_fred, plan_fred
    from aegis_alpha.storage.workspace import open_workspace

    if args.fred_command == "plan":
        today = args.today or fred.fred_day(utc_now())
        with open_workspace(home, writable=False, require_strategies=False) as workspace:
            return plan_fred(workspace, today=today, policy=fred.FredPolicy())
    from aegis_alpha.application.data_config import read_secret

    transport = urllib_transport(max_bytes=fred.MAX_RESPONSE_BYTES)
    client = fred.FredClient(read_secret(args.key_file.absolute()), transport)
    with open_workspace(
        home, writable=True, strategy_write=True, require_strategies=False
    ) as workspace:
        result = collect_fred(workspace, client, clock=utc_now, max_calls=args.max_calls)
    return result | {"exit_code": int(result["stopped"] not in {None, "budget"})}


def _sec(args: argparse.Namespace, home: Path) -> dict[str, object]:
    from aegis_alpha.data import sec_collect as sec
    from aegis_alpha.data.opendart import urllib_transport, utc_now
    from aegis_alpha.storage.sec_collection import collect_sec, plan_sec
    from aegis_alpha.storage.workspace import open_workspace

    policy = sec.SecPolicy(issuers=args.issuers)
    if args.sec_command == "plan":
        with open_workspace(home, writable=False, require_strategies=False) as workspace:
            return plan_sec(
                workspace, now=_plan_instant(args.today), policy=policy, since=args.since
            )
    from aegis_alpha.application.data_config import read_secret

    transport = urllib_transport(max_bytes=sec.MAX_RESPONSE_BYTES)
    client = sec.SecClient(read_secret(args.user_agent_file.absolute()), transport)
    with open_workspace(
        home, writable=True, strategy_write=True, require_strategies=False
    ) as workspace:
        result = collect_sec(
            workspace,
            client,
            policy=policy,
            clock=utc_now,
            since=args.since,
            max_calls=args.max_calls,
        )
    return result | {"exit_code": int(result["stopped"] not in {None, "budget"})}


def execute(args: argparse.Namespace) -> dict[str, object]:
    import sqlite3

    import duckdb

    from aegis_alpha.storage.paths import resolve_home

    home = resolve_home(getattr(args, "home", None))
    try:
        if args.collect_command == "fred":
            return _fred(args, home)
        return _sec(args, home)
    except (sqlite3.Error, duckdb.Error):
        raise ValueError("local database operation failed; run aas db verify") from None
