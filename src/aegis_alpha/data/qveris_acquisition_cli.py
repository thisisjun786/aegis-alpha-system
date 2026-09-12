"""Explicit Qveris jobs: offline plan, raw acquisition, or settlement-only recovery."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from aegis_alpha.data.qveris_acquisition import (
    acquire_jobs,
    acquisition_plan,
    quarantine_pending,
    reconcile_pending,
)
from aegis_alpha.data.qveris_client import QverisClient
from aegis_alpha.data.qveris_contracts import load_jobs
from aegis_alpha.data.sec_evidence import read_bytes


def main(argv: list[str] | None = None) -> int:  # noqa: C901 -- exclusive CLI modes
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=Path)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--key-file", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--reconcile", action="store_true")
    mode.add_argument("--quarantine-page")
    parser.add_argument("--reason")
    parser.add_argument("--max-calls", type=int)
    parser.add_argument("--max-credits")
    args = parser.parse_args(argv)
    try:
        if args.reason is not None and not (args.quarantine_page):
            raise ValueError("--reason requires --quarantine-page")  # noqa: TRY301
        if (args.reconcile or args.quarantine_page) and args.jobs is not None:
            raise ValueError("audit recovery does not accept --jobs")  # noqa: TRY301
        if args.quarantine_page:
            if args.key_file is None or args.reason is None:
                raise ValueError("quarantine requires --key-file and --reason")  # noqa: TRY301
            result = quarantine_pending(
                args.output_root, args.quarantine_page, args.reason, QverisClient(args.key_file)
            )
        elif args.reconcile:
            if args.key_file is None:
                raise ValueError("reconcile requires --key-file")  # noqa: TRY301 -- CLI validation
            result = reconcile_pending(args.output_root, QverisClient(args.key_file))
        else:
            if args.jobs is None:
                raise ValueError("--jobs is required for plan and execution")  # noqa: TRY301
            jobs = load_jobs(read_bytes(args.jobs.absolute()))
            result = acquisition_plan(jobs, args.output_root)
            if args.execute:
                if args.key_file is None:
                    raise ValueError("execution requires --key-file")  # noqa: TRY301 -- CLI validation
                from aegis_alpha.data.qveris import (  # noqa: PLC0415
                    InvocationBudget,
                    RequestBudgetPort,
                )

                budget = InvocationBudget(args.max_calls, args.max_credits)
                client = QverisClient(
                    args.key_file, max_response_bytes=max(job.max_response_bytes for job in jobs)
                )
                port = RequestBudgetPort(client, max(32, budget.max_calls * 16), 900)
                result = acquire_jobs(jobs, args.output_root, port, budget=budget)
                result.update(
                    paid_executions=port.paid_executions, http_requests=port.http_requests
                )
        print(json.dumps(result, sort_keys=True, indent=2))  # noqa: T201 -- CLI output
    except (OSError, ValueError, TypeError, RuntimeError) as error:
        print(  # noqa: T201 -- sanitized CLI diagnostic
            json.dumps(
                {"status": "STOPPED", "error_class": type(error).__name__, "error": str(error)}
            ),
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
