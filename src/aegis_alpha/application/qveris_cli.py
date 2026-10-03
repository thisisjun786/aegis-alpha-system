"""``aas collect qveris``: plan, run, daily jobs, quarantine and content-addressed import.

``plan`` and ``daily-jobs`` read job documents and the raw collection root only; neither
reads a key nor makes a request. ``run`` executes explicit job documents in order (one
worker) or in fair parallel groups (several workers) under per-run paid-call, credit,
HTTP-request and wall-time limits, pacing every request start on one shared interval.
``quarantine`` isolates one unresolved page or parallel group that cannot settle, keeping
its worst-case reservation and the operator's reason. ``import`` commits completed jobs
into the source library and never calls a provider.
"""

from __future__ import annotations

# ruff: noqa: PLC0415 -- provider, Arrow and database imports wait for the chosen action.
import argparse
import hashlib
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aegis_alpha.data.qveris_billing import QverisPort
    from aegis_alpha.data.qveris_contracts import QverisJob

WORKERS = (1, 2, 3, 4, 8, 16)
MAX_JOBS_BYTES = 64 * 1024 * 1024
DEFAULT_TIME_LIMIT_SECONDS = 3600.0
_AUDIT_REQUESTS_PER_EXECUTION = 16
_MIN_HTTP_REQUESTS = 32
_PROGRESS_EVERY = 25
_STOPPED_EXIT = 2


def add_commands(actions: argparse._SubParsersAction) -> None:
    from aegis_alpha.application.storage_cli import home_option

    qveris = actions.add_parser(
        "qveris", help="Plan, run, schedule and import explicit Qveris collection jobs"
    )
    sub = qveris.add_subparsers(dest="qveris_command", required=True)
    plan = sub.add_parser("plan", help="Validate job documents against the raw root; no HTTP")
    _jobs_options(plan)
    plan.add_argument("--workers", type=int, choices=WORKERS, default=1)
    run = sub.add_parser("run", help="Execute job documents under explicit per-run limits")
    _jobs_options(run)
    run.add_argument("--key-file", type=Path, required=True, help="Private Qveris key file")
    run.add_argument("--max-calls", type=int, required=True, help="Paid executions this run")
    run.add_argument("--max-credits", required=True, help="Credits this run may reserve")
    run.add_argument("--workers", type=int, choices=WORKERS, default=1)
    run.add_argument("--request-interval", type=float, default=None, help="Seconds between starts")
    run.add_argument("--max-http-requests", type=int, help="All HTTP attempts this run")
    run.add_argument("--time-limit-seconds", type=float, default=DEFAULT_TIME_LIMIT_SECONDS)
    run.add_argument("--timeout-seconds", type=float, default=45.0)
    daily = sub.add_parser("daily-jobs", help="Write the daily exchange-bulk jobs a window lacks")
    daily.add_argument("--raw-root", type=Path, required=True)
    daily.add_argument("--exchange", action="append", required=True, choices=("US", "KO", "KQ"))
    daily.add_argument(
        "--dataset", action="append", choices=("prices", "splits", "dividends"), default=None
    )
    window = daily.add_mutually_exclusive_group(required=True)
    window.add_argument("--lookback-days", type=int, help="Calendar days ending yesterday")
    window.add_argument("--from", dest="start", type=date.fromisoformat, help="First date")
    daily.add_argument("--through", type=date.fromisoformat, help="Last date (with --from)")
    daily.add_argument("--observation-date", type=date.fromisoformat, help="Default: UTC today")
    daily.add_argument("--declaration", type=Path, help="Calendar declaration overriding one")
    daily.add_argument("--declaration-sha256", help="SHA-256 of --declaration")
    daily.add_argument("--output", type=Path, required=True, help="New jobs document path")
    quarantine = sub.add_parser(
        "quarantine", help="Isolate an unresolved page or group, keeping its reservation"
    )
    quarantine.add_argument("--raw-root", type=Path, required=True, help="Raw collection root")
    quarantine.add_argument("--key-file", type=Path, required=True, help="Private Qveris key file")
    target = quarantine.add_mutually_exclusive_group(required=True)
    target.add_argument("--batch", help="Unsettled group, parallel-batches/<id>")
    target.add_argument("--page", help="Unresolved page, jobs/<fingerprint>/<NNNN>")
    quarantine.add_argument("--reason", required=True, help="Operator reason, recorded")
    importer = sub.add_parser("import", help="Commit completed jobs as content-addressed sources")
    home_option(importer)
    importer.add_argument("--raw-root", type=Path, required=True)
    importer.add_argument("--identity", type=Path, help="Symbol identity document")
    importer.add_argument("--market", action="append", default=[], help="Only these markets")
    importer.add_argument("--dataset", action="append", default=[], help="Only these datasets")
    importer.add_argument("--fingerprint", action="append", help="Only these jobs")
    importer.add_argument("--limit", type=int, help="Build at most this many new units")
    importer.add_argument("--plan", action="store_true", help="Normalize and report; write nothing")


def _jobs_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--jobs", type=Path, action="append", required=True, help="Jobs document")
    parser.add_argument("--raw-root", type=Path, required=True, help="Raw collection root")


def _cohorts(paths: list[Path]) -> tuple[list[tuple[QverisJob, ...]], list[dict[str, object]]]:
    from aegis_alpha.data.qveris_contracts import load_jobs
    from aegis_alpha.data.sec_evidence import read_bytes

    cohorts, documents = [], []
    for path in paths:
        payload = read_bytes(path, maximum=MAX_JOBS_BYTES)
        jobs = load_jobs(payload)
        cohorts.append(jobs)
        documents.append(
            {"path": str(path), "sha256": hashlib.sha256(payload).hexdigest(), "jobs": len(jobs)}
        )
    return cohorts, documents


def plan(args: argparse.Namespace) -> dict[str, object]:
    from aegis_alpha.application.qveris_daily import raw_attempts
    from aegis_alpha.data.qveris_acquisition import acquisition_plan
    from aegis_alpha.data.qveris_parallel_batch import interleave_cohorts

    cohorts, documents = _cohorts(args.jobs)
    for jobs in cohorts:
        acquisition_plan(jobs, args.raw_root)
    if args.workers > 1:
        # Parallel scheduling interleaves the documents, so a request may appear only once.
        interleave_cohorts(tuple(cohorts))
    attempts = raw_attempts(args.raw_root)
    attempted = {fingerprint for value in attempts.held.values() for fingerprint in value}
    for document, jobs in zip(documents, cohorts, strict=True):
        counts = dict.fromkeys(("completed", "held", "equivalent", "new"), 0)
        for job in jobs:
            request = (job.tool_id, job.upstream, job.market, job.dataset, job.parameters_json)
            if job.fingerprint in attempts.fingerprints:
                counts["completed"] += 1  # reused by run without HTTP
            elif job.fingerprint in attempted:
                counts["held"] += 1  # run settles this attempt; no new paid call
            elif request in attempts.completed or request in attempts.held:
                counts["equivalent"] += 1  # same request under another job; run pays again
            else:
                counts["new"] += 1
        document.update(counts)
    return {
        "provider": "qveris",
        "execute": False,
        "documents": documents,
        "workers": args.workers,
        "http_calls": 0,
        "provider_calls": 0,
        "exit_code": 0,
    }


def _progress(row: dict[str, object]) -> None:
    if row.get("workers") or int(str(row["processed"])) % _PROGRESS_EVERY == 0 or row["stopped"]:
        print(json.dumps(row, sort_keys=True), file=sys.stderr, flush=True)  # noqa: T201 -- service progress


def run(args: argparse.Namespace) -> dict[str, object]:
    from aegis_alpha.data.qveris import InvocationBudget
    from aegis_alpha.data.qveris_acquisition import acquisition_plan
    from aegis_alpha.data.qveris_batch import BUDGET_STOPS, collect_cohort
    from aegis_alpha.data.qveris_client import QverisClient
    from aegis_alpha.data.qveris_contracts import credit_value
    from aegis_alpha.data.qveris_pacing import (
        DEFAULT_REQUEST_INTERVAL,
        PacedQverisClient,
        RequestAdmission,
        RequestPacer,
    )
    from aegis_alpha.data.qveris_parallel_batch import collect_parallel_cohorts

    cohorts, documents = _cohorts(args.jobs)
    for jobs in cohorts:
        acquisition_plan(jobs, args.raw_root)
    budget = InvocationBudget(args.max_calls, credit_value(args.max_credits))
    limit = args.max_http_requests or max(
        _MIN_HTTP_REQUESTS, args.max_calls * _AUDIT_REQUESTS_PER_EXECUTION
    )
    admission = RequestAdmission(limit, args.time_limit_seconds)
    interval = DEFAULT_REQUEST_INTERVAL if args.request_interval is None else args.request_interval
    pacer = RequestPacer(interval)

    def factory() -> QverisPort:
        client = QverisClient(args.key_file, timeout_seconds=args.timeout_seconds)
        return PacedQverisClient(client, pacer, admission)

    if args.workers > 1:
        result = collect_parallel_cohorts(
            tuple(cohorts),
            args.raw_root,
            factory,
            workers=args.workers,
            budget=budget,
            progress=_progress,
        )
        cohort_results = [result]
    else:
        client = factory()
        cohort_results = []
        for jobs in cohorts:
            result = collect_cohort(jobs, args.raw_root, client, budget=budget, progress=_progress)
            cohort_results.append(result)
            if result["stopped"]:
                break
    statuses = {str(item["status"]) for item in cohort_results}
    stopped = [item["stopped"] for item in cohort_results if item["stopped"]]
    if stopped and not set(map(str, stopped)) <= BUDGET_STOPS:
        code, status = _STOPPED_EXIT, "stopped"
    elif "PARTIAL" in statuses:
        code, status = 1, "partial"
    else:
        code, status = 0, "budget_exhausted" if stopped else "succeeded"
    return {
        "provider": "qveris",
        "status": status,
        "exit_code": code,
        "documents": documents,
        "workers": args.workers,
        "cohorts": cohort_results,
        "paid_executions": admission.paid_executions,
        "http_requests": admission.http_requests,
        "max_http_requests": admission.max_requests,
        "reserved_calls": budget.reserved_calls,
        "reserved_credits": str(budget.reserved_credits),
        "max_calls": budget.max_calls,
        "max_credits": str(budget.max_credits),
        "request_interval_seconds": pacer.interval,
        "source_only": True,
        "native_import_completed": False,
    }


def daily_jobs(args: argparse.Namespace) -> dict[str, object]:
    from aegis_alpha.application.qveris_daily import default_window, plan_daily_jobs
    from aegis_alpha.data.sec_evidence import publish_bytes, read_bytes
    from aegis_alpha.storage.calendar_declaration import parse_declaration

    observation = args.observation_date or datetime.now(UTC).date()
    if args.lookback_days is not None:
        if args.through is not None:
            raise ValueError("--through goes with --from, not --lookback-days")
        start, through = default_window(observation, args.lookback_days)
    else:
        if args.through is None:
            raise ValueError("--from needs --through")
        start, through = args.start, args.through
    if (args.declaration is None) != (args.declaration_sha256 is None):
        raise ValueError("--declaration and --declaration-sha256 go together")
    declarations = []
    if args.declaration is not None:
        declarations.append(
            parse_declaration(read_bytes(args.declaration), args.declaration_sha256)
        )
    result = plan_daily_jobs(
        args.raw_root,
        exchanges=args.exchange,
        datasets=args.dataset or ["prices"],
        start=start,
        through=through,
        observation_date=observation,
        declarations=declarations,
    )
    document = result.pop("document")
    if isinstance(document, bytes):
        if args.output.exists():
            raise ValueError("--output already exists; jobs documents are never overwritten")
        publish_bytes(args.output, document)
        result["jobs"] = str(args.output)
        result["jobs_sha256"] = hashlib.sha256(document).hexdigest()
    else:
        result["jobs"] = None
    return {"provider": "qveris", **result, "exit_code": 0}


def quarantine(args: argparse.Namespace) -> dict[str, object]:
    """Record the operator's quarantine of one page or group; its reservation is kept."""
    from aegis_alpha.data.qveris_acquisition import quarantine_pending
    from aegis_alpha.data.qveris_client import QverisClient
    from aegis_alpha.data.qveris_parallel import quarantine_parallel_batch

    client = QverisClient(args.key_file)
    if args.batch is not None:
        result = quarantine_parallel_batch(args.raw_root, args.batch, args.reason, client)
    else:
        result = quarantine_pending(args.raw_root, args.page, args.reason, client)
    return {"provider": "qveris", **result, "automatic_retry": False, "exit_code": 0}


def import_jobs(args: argparse.Namespace) -> dict[str, object]:
    import sqlite3

    import duckdb

    from aegis_alpha.data.sec_evidence import read_bytes
    from aegis_alpha.storage import qveris_import
    from aegis_alpha.storage.paths import resolve_home
    from aegis_alpha.storage.source_library import committed_source_ids
    from aegis_alpha.storage.workspace import open_workspace

    if args.limit is not None and args.limit < 1:
        raise ValueError("--limit must be positive")
    identity = None
    if args.identity is not None:
        raw = read_bytes(args.identity, maximum=qveris_import.MAX_IDENTITY_BYTES)
        identity = qveris_import.parse_identity(raw)
    selected, missing = qveris_import.completions(
        args.raw_root,
        fingerprints=args.fingerprint,
        markets=frozenset(args.market),
        datasets=frozenset(args.dataset),
    )
    home = resolve_home(getattr(args, "home", None))
    try:
        with open_workspace(
            home, writable=not args.plan, strategy_write=not args.plan, require_strategies=False
        ) as workspace:
            committed = frozenset(committed_source_ids(workspace))
            result = qveris_import.import_completions(
                args.raw_root,
                selected,
                identity,
                workspace=None if args.plan else workspace,
                limit=args.limit,
                committed=committed,
            )
    except (sqlite3.Error, duckdb.Error):
        raise ValueError("local database operation failed; run aas db verify") from None
    failed = bool(result["failures"]) or bool(missing)
    return {
        "provider": "qveris",
        **result,
        "identity_sha256": None if identity is None else identity.sha256,
        "missing": missing,
        "exit_code": 1 if failed else 0,
    }


def execute(args: argparse.Namespace) -> dict[str, object]:
    match args.qveris_command:
        case "plan":
            return plan(args)
        case "run":
            return run(args)
        case "daily-jobs":
            return daily_jobs(args)
        case "quarantine":
            return quarantine(args)
        case "import":
            return import_jobs(args)
        case _:
            raise ValueError("unknown qveris command")
