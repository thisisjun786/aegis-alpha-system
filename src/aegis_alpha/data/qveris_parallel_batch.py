"""Fair, bounded scheduling of cohorts on one Qveris account."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from aegis_alpha.data.qveris_acquisition import acquisition_plan
from aegis_alpha.data.qveris_batch import BUDGET_STOPS, WARNED, budget_stop, collect_cohort
from aegis_alpha.data.qveris_contracts import (
    EOD_HISTORY_JSON_TOOL,
    EOD_TOOL,
    SEC_TOOL,
    QverisJob,
    object_value,
)
from aegis_alpha.data.qveris_parallel import DEFAULT_WORKERS, MAX_WORKERS, acquire_parallel_jobs
from aegis_alpha.data.sec_evidence import publish_bytes
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256

if TYPE_CHECKING:
    from aegis_alpha.data.qveris import InvocationBudget
    from aegis_alpha.data.qveris_billing import QverisPort

_PARALLEL_TOOLS = frozenset({EOD_TOOL, EOD_HISTORY_JSON_TOOL, SEC_TOOL})


def interleave_cohorts(cohorts: tuple[tuple[QverisJob, ...], ...]) -> tuple[QverisJob, ...]:
    """Round-robin the cohorts so a long first cohort never starves a later one."""
    queues = deque(deque(group) for group in cohorts if group)
    result: list[QverisJob] = []
    while queues:
        group = queues.popleft()
        result.append(group.popleft())
        if group:
            queues.append(group)
    if len({job.fingerprint for job in result}) != len(result):
        raise ValueError("cohorts contain duplicate acquisition requests")
    return tuple(result)


def _counts(value: object, group: list[QverisJob]) -> tuple[int, int, int, int]:
    if not isinstance(value, list) or len(value) != len(group):
        raise ValueError("parallel result does not cover the scheduled group")
    completed = failed = reused = warned = 0
    fingerprints = []
    for item in value:
        row = object_value(item)
        fingerprints.append(row.get("fingerprint"))
        if row.get("status") in {"RAW_ACQUIRED", WARNED}:
            completed += 1
            warned += row.get("status") == WARNED
        elif row.get("status") == "FAILED":
            failed += 1
        else:
            raise ValueError("parallel result is incomplete")
        reused += row.get("reused") is True
    if fingerprints != [job.fingerprint for job in group]:
        raise ValueError("parallel result identities differ from scheduled group")
    return completed, failed, reused, warned


def _group(remaining: deque[QverisJob], workers: int) -> list[QverisJob]:
    group = [remaining.popleft()]
    if group[0].tool_id in _PARALLEL_TOOLS:
        while remaining and len(group) < workers and remaining[0].tool_id in _PARALLEL_TOOLS:
            group.append(remaining.popleft())
    return group


def collect_parallel_cohorts(  # noqa: PLR0913, PLR0915 -- one scheduler loop and its report
    cohorts: tuple[tuple[QverisJob, ...], ...],
    root: Path,
    client_factory: Callable[[], QverisPort],
    *,
    workers: int = DEFAULT_WORKERS,
    budget: InvocationBudget | None = None,
    progress: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, object]:
    """Interleave the cohorts and acquire them in groups of at most ``workers`` jobs.

    Single-page tools run as one parallel group; any other tool runs serially. A group
    that settles with failures is counted and scheduling continues. An uncertain group,
    a group refused by the budget, or a changed result identity stops scheduling, and
    the unprocessed jobs are reported as pending. A budget refusal counts as budget
    exhaustion only when the store holds no unresolved page or group.
    """
    if type(workers) is not int or not 1 <= workers <= MAX_WORKERS:
        raise ValueError(f"workers must be an integer from 1 through {MAX_WORKERS}")
    jobs = interleave_cohorts(cohorts)
    acquisition_plan(jobs, root)
    run_id = uuid4().hex
    cohort_id = content_sha256([job.document() for job in jobs])
    report_root = root / "parallel-cohorts" / cohort_id
    publish_bytes(report_root / "jobs.json", canonical_json_bytes([j.document() for j in jobs]))
    remaining = deque(jobs)
    completed = failed = reused = warned = calls = processed = 0
    stopped: str | None = None
    batch_index = 0
    snapshot: dict[str, object] = {}
    while remaining:
        group = _group(remaining, workers)
        try:
            if group[0].tool_id not in _PARALLEL_TOOLS:
                serial = collect_cohort(tuple(group), root, client_factory(), budget=budget)
                completed += int(str(serial["completed"]))
                failed += int(str(serial["failed"]))
                reused += int(str(serial["reused"]))
                warned += int(str(serial["warned"]))
                calls += int(str(serial["successful_job_calls_this_run"]))
                processed += int(str(serial["processed"]))
                if serial["stopped"]:
                    stopped = str(serial["stopped"])
            else:
                result = acquire_parallel_jobs(
                    tuple(group), root, client_factory, workers=workers, budget=budget
                )
                calls += int(str(result["provider_calls_this_run"]))
                done, errors, cached, flagged = _counts(result["jobs"], group)
                completed += done
                failed += errors
                reused += cached
                warned += flagged
                processed += len(group)
        except (ValueError, TypeError, OSError, RuntimeError) as error:
            stop = None
            if isinstance(error, RuntimeError) and str(error).split(":", 1)[0] in BUDGET_STOPS:
                stop = budget_stop(error, root, client_factory().account_key)
            stopped = stop or type(error).__name__
        snapshot = {
            "cohort_id": cohort_id,
            "run_id": run_id,
            "workers": workers,
            "requested": len(jobs),
            "processed": processed,
            "completed": completed,
            "failed": failed,
            "pending": len(jobs) - processed,
            "reused": reused,
            "warned": warned,
            "provider_calls_this_run": calls,
            "stopped": stopped,
            "observed_at_utc": datetime.now(UTC).isoformat(),
            "billing_authority": "per-job receipts and batch settlement manifests",
        }
        publish_bytes(
            report_root / f"{run_id}-{batch_index:06d}.json", canonical_json_bytes(snapshot)
        )
        batch_index += 1
        if progress:
            progress(snapshot)
        if stopped:
            break
    if stopped in BUDGET_STOPS:
        status = "BUDGET_EXHAUSTED"
    else:
        status = "PARTIAL" if failed or stopped else "RAW_ACQUIRED"
    summary = {**snapshot, "status": status}
    publish_bytes(report_root / f"{run_id}-summary.json", canonical_json_bytes(summary))
    return summary
