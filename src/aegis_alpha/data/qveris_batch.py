"""Ordered serial cohorts with durable progress; failed rows are kept, paid calls never retried."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from aegis_alpha.data.qveris_acquisition import acquire_jobs, acquisition_plan
from aegis_alpha.data.qveris_store import QverisStore
from aegis_alpha.data.sec_evidence import publish_bytes
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256

if TYPE_CHECKING:
    from collections.abc import Callable

    from aegis_alpha.data.qveris import InvocationBudget
    from aegis_alpha.data.qveris_billing import QverisPort
    from aegis_alpha.data.qveris_contracts import QverisJob

# Refusals raised before an intent exists: the job was not attempted, nothing is uncertain.
BUDGET_STOPS = frozenset(
    {
        "INVOCATION_CALL_LIMIT",
        "INVOCATION_CREDIT_LIMIT",
        "INVOCATION_CREDIT_PRECISION",
        "INVOCATION_HTTP_LIMIT",
        "INSUFFICIENT_CREDITS",
    }
)
WARNED = "RAW_ACQUIRED_WITH_WARNINGS"


def budget_stop(error: BaseException) -> str | None:
    """The budget code of a refusal raised before any paid request, else ``None``."""
    code = str(error).split(":", 1)[0]
    return code if isinstance(error, RuntimeError) and code in BUDGET_STOPS else None


def collect_cohort(
    jobs: tuple[QverisJob, ...],
    root: Path,
    client: QverisPort,
    *,
    budget: InvocationBudget | None = None,
    progress: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, object]:
    """Acquire ``jobs`` in order, one at a time, recording progress after each job.

    A settled failure is recorded and the cohort continues; a provider warning is a
    completion (``warned``) and never stops it. An uncertain or failed preflight stops
    the cohort (``stopped``), as does a budget refusal, which attempted nothing.
    """
    acquisition_plan(jobs, root)
    cohort_id = content_sha256([job.document() for job in jobs])
    completed = failed = reused = warned = calls = 0
    failure_rows: list[dict[str, object]] = []
    run_id = uuid4().hex
    report_dir = root / "cohorts" / cohort_id
    publish_bytes(report_dir / "jobs.json", canonical_json_bytes([job.document() for job in jobs]))
    stopped: str | None = None
    snapshot: dict[str, object] = {}
    for index, job in enumerate(jobs):
        try:
            result = acquire_jobs((job,), root, client, budget=budget)
        except (ValueError, RuntimeError) as error:
            stop = budget_stop(error)
            if stop is not None:
                stopped = stop
            else:
                with QverisStore(root, client.account_key) as store:
                    unresolved = store.pending_pages()
                    prefix = f"jobs/{job.fingerprint}/"
                    terminal = store.exists(prefix + "0000.billing.json") and not unresolved
                failure_rows.append(
                    {
                        "index": index,
                        "job_id": job.job_id,
                        "fingerprint": job.fingerprint,
                        "error_class": type(error).__name__,
                        "reason": str(error),
                        "known_settled_failure": terminal,
                    }
                )
                if terminal:
                    failed += 1
                else:
                    stopped = "PENDING_OR_PREFLIGHT_FAILURE"
        else:
            completed += 1
            calls += int(str(result["provider_calls_this_run"]))
            item = result["jobs"]
            if isinstance(item, list) and item and isinstance(item[0], dict):
                reused += item[0].get("reused") is True
                warned += item[0].get("status") == WARNED
        snapshot = {
            "cohort_id": cohort_id,
            "run_id": run_id,
            "requested": len(jobs),
            "processed": completed + failed,
            "completed": completed,
            "failed": failed,
            "pending": len(jobs) - completed - failed,
            "reused": reused,
            "warned": warned,
            "successful_job_calls_this_run": calls,
            "stopped": stopped,
            "observed_at_utc": datetime.now(UTC).isoformat(),
        }
        publish_bytes(
            report_dir / f"{run_id}-{index:06d}.json",
            canonical_json_bytes({**snapshot, "failures": failure_rows}),
        )
        if progress:
            progress(snapshot)
        if stopped:
            break
    result = {
        **snapshot,
        "failures": failure_rows,
        "status": _status(failed=failed, stopped=stopped),
        "billing_authority": "per-execution billing receipts, including failed jobs",
    }
    publish_bytes(report_dir / f"{run_id}-summary.json", canonical_json_bytes(result))
    return result


def _status(*, failed: int, stopped: str | None) -> str:
    if stopped in BUDGET_STOPS:
        return "BUDGET_EXHAUSTED"
    return "PARTIAL" if failed or stopped else "RAW_ACQUIRED"
