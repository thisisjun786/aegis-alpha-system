"""CLI adapter for the integrated run API: argument parsing and delegation only.

Keeping every decision in `run_backtest` is what makes a CLI run and a Python run the
same execution, so nothing but parsing belongs in this module.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import cast

from aegis_alpha.application.run_backtest import (
    DEFAULT_REASON,
    RunBacktestRequest,
    list_backtest_runs,
    read_backtest_run,
    run_backtest,
)


def add_commands(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = commands.add_parser(
        "run", help="Run a registered strategy end to end and read stored runs"
    )
    sub = parser.add_subparsers(dest="run_command", required=True)
    execution = sub.add_parser(
        "execute", help="Prepare, calculate and record one run of a registered strategy"
    )
    execution.add_argument("--request", type=Path, required=True)
    execution.add_argument("--sha256", required=True, help="Expected exact request file SHA-256")
    execution.add_argument("--reason", default=DEFAULT_REASON, help="Recorded run reason")
    execution.add_argument(
        "--bundle-id", help="Input bundle name; default derives from the bindings and request"
    )
    execution.add_argument("--prior-run-id", help="Recomputation predecessor run ID")
    execution.add_argument("--run-id", help="Explicit run ID; default is generated")
    execution.add_argument(
        "--envelope-output",
        type=Path,
        help="Optional new envelope path; also creates PATH.preparation.json (no overwrites)",
    )
    stored = sub.add_parser("show", help="Re-verify and read one recorded run by its ID")
    stored.add_argument("--run-id", required=True)
    sub.add_parser("list", help="List recorded runs without verifying their results")


def execute(args: argparse.Namespace) -> dict[str, object]:
    home = cast("Path | None", getattr(args, "home", None))
    if args.run_command == "show":
        return read_backtest_run(args.run_id, home=home)
    if args.run_command == "list":
        return list_backtest_runs(home=home)
    return run_backtest(
        RunBacktestRequest(
            request=cast("Path", args.request),
            request_sha256=args.sha256,
            home=home,
            reason=args.reason,
            bundle_id=args.bundle_id,
            prior_run_id=args.prior_run_id,
            run_id=args.run_id,
            envelope_output=cast("Path | None", args.envelope_output),
        )
    )
