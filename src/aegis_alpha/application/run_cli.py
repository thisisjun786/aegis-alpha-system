"""CLI adapter for the integrated run API: argument parsing and delegation only.

Keeping every decision in `run_backtest` is what makes a CLI run and a Python run the
same execution, so nothing but parsing belongs in this module. `run_research` is the
same arrangement for the declared uncertified path, and both record into one store, so
one `show` and one `list` read either kind back rather than each contract growing a
reader of its own.
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
from aegis_alpha.application.run_research import DEFAULT_REASON as RESEARCH_REASON
from aegis_alpha.application.run_research import (
    RunResearchRequest,
    rerun_research_run,
    run_research,
)
from aegis_alpha.application.storage_cli import home_option


def add_commands(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = commands.add_parser(
        "run", help="Run a registered strategy end to end and read stored runs"
    )
    home_option(parser)
    sub = parser.add_subparsers(dest="run_command", required=True)
    execution = sub.add_parser(
        "execute", help="Prepare, calculate and record one run of a registered strategy"
    )
    home_option(execution)
    execution.add_argument(
        "--request", type=Path, required=True, help="Exact prepare request document"
    )
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
    _research_parsers(sub)
    stored = sub.add_parser("show", help="Re-verify and read one recorded run by its ID")
    home_option(stored)
    stored.add_argument("--run-id", required=True)
    home_option(sub.add_parser("list", help="List recorded runs without verifying their results"))


def _research_parsers(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """The declared uncertified path: one execution verb and one reproduction verb.

    `research` takes no --run-id. A declared run's identity comes from the declaration
    itself, so offering to name it would offer to file one calculation under another
    name, which is exactly what the stable identity exists to prevent.
    """
    research = sub.add_parser(
        "research",
        help="Record one declared UNCERTIFIED research run over pinned observations",
    )
    home_option(research)
    research.add_argument(
        "--declaration",
        type=Path,
        required=True,
        help="Exact aas-research-run-v2 or aas-research-composition-v1 document",
    )
    research.add_argument("--sha256", required=True, help="Expected exact declaration file SHA-256")
    research.add_argument("--reason", default=RESEARCH_REASON, help="Recorded run reason")
    research.add_argument(
        "--bundle-id", help="Input bundle name; default derives from the declaration"
    )
    research.add_argument("--prior-run-id", help="Recomputation predecessor run ID")
    research.add_argument(
        "--envelope-output",
        type=Path,
        help="Optional new envelope path; also creates PATH.preparation.json (no overwrites)",
    )
    rerun = sub.add_parser(
        "rerun", help="Reproduce one recorded run from its sealed evidence, writing nothing"
    )
    home_option(rerun)
    rerun.add_argument("--run-id", required=True)
    rerun.add_argument(
        "--declaration",
        type=Path,
        help="Also prepare this declaration again and compare it with what the run sealed",
    )
    rerun.add_argument("--sha256", help="Exact declaration file SHA-256; required with one")


def execute(args: argparse.Namespace) -> dict[str, object]:
    home = cast("Path | None", getattr(args, "home", None))
    if args.run_command == "show":
        return read_backtest_run(args.run_id, home=home)
    if args.run_command == "list":
        return list_backtest_runs(home=home)
    if args.run_command == "rerun":
        return rerun_research_run(
            args.run_id,
            home=home,
            declaration=cast("Path | None", args.declaration),
            declaration_sha256=args.sha256,
        )
    if args.run_command == "research":
        return run_research(
            RunResearchRequest(
                declaration=cast("Path", args.declaration),
                declaration_sha256=args.sha256,
                home=home,
                reason=args.reason,
                bundle_id=args.bundle_id,
                prior_run_id=args.prior_run_id,
                envelope_output=cast("Path | None", args.envelope_output),
            )
        )
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
