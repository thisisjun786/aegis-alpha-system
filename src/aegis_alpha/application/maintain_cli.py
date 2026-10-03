"""``aas maintain plan|run|receipt|cutover-check``: the daily pass, its receipt and cutover."""

from __future__ import annotations

# ruff: noqa: PLC0415 -- lazy storage imports keep help independent of database drivers.
import argparse
from datetime import UTC, datetime
from pathlib import Path

from aegis_alpha.application.storage_cli import home_option


def _instant(value: str) -> datetime:
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("instants are ISO 8601 with an offset") from None
    if moment.utcoffset() is None:
        raise argparse.ArgumentTypeError("instants need an offset, such as Z or +09:00")
    return moment.astimezone(UTC)


def add_commands(commands: argparse._SubParsersAction) -> None:
    maintain = commands.add_parser(
        "maintain", help="Collect, import, register identities and promote in one bounded pass"
    )
    home_option(maintain)
    sub = maintain.add_subparsers(dest="maintain_command", required=True)
    plan = sub.add_parser("plan", help="Report what a run would do; no key, call or write")
    home_option(plan)
    plan.add_argument("--at", type=_instant, help="Plan for this instant (default: now)")
    plan.add_argument(
        "--promotions",
        action="store_true",
        help="Also plan every new source's promotion against the current heads",
    )
    run = sub.add_parser("run", help="Run one maintenance pass as the single market writer")
    home_option(run)
    receipt = sub.add_parser("receipt", help="Record the running installation's install receipt")
    home_option(receipt)
    receipt.add_argument("--lock", type=Path, help="The uv.lock the tool was installed from")
    check = sub.add_parser(
        "cutover-check",
        help="Check the operations cutover read-only; --record keeps a passing report",
    )
    home_option(check)
    check.add_argument(
        "--backup", type=Path, required=True, help="The deep backup taken after the cutover"
    )
    check.add_argument(
        "--expect-provider",
        action="append",
        default=[],
        choices=("kind", "dart", "sec", "fred", "qveris"),
        help="A provider section that must be enabled (repeatable)",
    )
    check.add_argument(
        "--legacy-manifest",
        action="append",
        default=[],
        type=Path,
        help="An imported legacy-import manifest whose entry paths were deleted (repeatable)",
    )
    check.add_argument(
        "--legacy-verify",
        action="append",
        default=[],
        type=Path,
        help="An `aas import legacy --verify` report of a named manifest (repeatable)",
    )
    check.add_argument(
        "--removed",
        action="append",
        default=[],
        type=Path,
        help="Another deleted legacy path (repeatable)",
    )
    check.add_argument(
        "--record", action="store_true", help="Keep a passing report in raw/ and runtime/"
    )


def execute(args: argparse.Namespace) -> dict[str, object]:
    import sqlite3

    import duckdb

    from aegis_alpha.application.compute_cli import price_compute
    from aegis_alpha.application.maintain import (
        config_ports,
        plan_maintenance,
        run_maintenance,
    )
    from aegis_alpha.application.maintain_config import load_config
    from aegis_alpha.data.opendart import utc_now
    from aegis_alpha.storage.locks import private_directory, storage_lock_targets
    from aegis_alpha.storage.paths import load_paths, resolve_home
    from aegis_alpha.storage.workspace import open_workspace

    home = resolve_home(getattr(args, "home", None))
    private_directory(home)
    paths = load_paths(home)
    if args.maintain_command == "receipt":
        from aegis_alpha.application.install_receipt import write_receipt

        return write_receipt(paths, lock=args.lock)
    if args.maintain_command == "cutover-check":
        return _cutover_check(home, args)
    config = load_config(paths)
    targets = storage_lock_targets(home, paths.stores())
    apply = args.maintain_command == "run"
    try:
        with (
            price_compute(excluded_locks=targets) as budget,
            open_workspace(home, writable=apply, strategy_write=apply) as workspace,
        ):
            if apply:
                return run_maintenance(
                    workspace, config, config_ports(config), clock=utc_now, budget=budget
                )
            return plan_maintenance(
                workspace,
                config,
                now=args.at or utc_now(),
                promotions=args.promotions,
                budget=budget,
            )
    except (sqlite3.Error, duckdb.Error):
        raise ValueError("local database operation failed; run aas db verify") from None


def _cutover_check(home: Path, args: argparse.Namespace) -> dict[str, object]:
    import sqlite3

    import duckdb

    from aegis_alpha.application.cutover import (
        CutoverRequest,
        check_cutover,
        read_backup,
        write_record,
    )
    from aegis_alpha.storage.workspace import open_workspace

    backup = read_backup(args.backup)
    request = CutoverRequest(
        expect_providers=tuple(args.expect_provider),
        legacy_manifests=tuple(args.legacy_manifest),
        legacy_verify=tuple(args.legacy_verify),
        removed=tuple(args.removed),
    )
    try:
        with open_workspace(home) as workspace:
            report = check_cutover(workspace, backup, request)
            paths = workspace.paths
    except (sqlite3.Error, duckdb.Error):
        raise ValueError("local database operation failed; run aas db verify") from None
    if args.record and report["passed"] is True:
        report = {**report, "record_sha256": write_record(paths, report, request.legacy_verify)}
    return {**report, "exit_code": 0 if report["passed"] is True else 1}
