"""Ordered serial cohorts with durable progress; failed rows are kept, paid calls never retried."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict, Unpack
from uuid import uuid4

from aegis_alpha.data.qveris_acquisition import acquire_jobs, acquisition_plan
from aegis_alpha.data.qveris_client import QverisTransportError
from aegis_alpha.data.qveris_payloads import QverisEmptyAnswerError
from aegis_alpha.data.qveris_store import QverisStore
from aegis_alpha.data.sec_evidence import publish_bytes
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256

if TYPE_CHECKING:
    from collections.abc import Callable

    from aegis_alpha.data.qveris import InvocationBudget
    from aegis_alpha.data.qveris_billing import QverisPort
    from aegis_alpha.data.qveris_contracts import QverisJob

# Refusals raised before an intent exists: the job was not attempted, nothing is uncertain.
# ``INVOCATION_HTTP_LIMIT`` is raised only at a page's first preflight request
# (``qveris_pacing.PAGE_START``); a stop counts as a budget stop only when the store also
# holds no unresolved page or group (``budget_stop``).
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


def budget_stop(error: BaseException, root: Path, account_key: str) -> str | None:
    """The budget code of a refusal that left nothing unresolved, else ``None``.

    A budget code with a pending page or an unsettled parallel group in the store is
    not a clean stop: that evidence blocks the account until it settles or an operator
    quarantines it, so the caller reports it as stopped.
    """
    code = str(error).split(":", 1)[0]
    if not isinstance(error, RuntimeError) or code not in BUDGET_STOPS:
        return None
    with QverisStore(root, account_key) as store:
        unresolved = store.pending_pages() or store.pending_batches()
    return None if unresolved else code


def _retryable_transport(error: BaseException, root: Path, account_key: str) -> bool:
    errors = [error]
    seen: set[int] = set()
    while errors:
        current = errors.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, QverisTransportError):
            # Execute publishes an intent first. Pending evidence forbids a retry;
            # billed pages reuse their intent and never execute a second time.
            with QverisStore(root, account_key) as store:
                return not store.pending_pages() and not store.pending_batches()
        errors.extend(
            linked for linked in (current.__cause__, current.__context__) if linked is not None
        )
    return False


def _acquire_job(
    job: QverisJob,
    root: Path,
    client: QverisPort,
    budget: InvocationBudget | None,
    pause: Callable[[], None],
) -> dict[str, object]:
    try:
        return acquire_jobs((job,), root, client, budget=budget)
    except (ValueError, TypeError, RuntimeError) as error:
        if not _retryable_transport(error, root, client.account_key):
            raise
    pause()
    return acquire_jobs((job,), root, client, budget=budget)


class TransportRetry(TypedDict, total=False):
    sleep: Callable[[float], None]
    retry_delay_seconds: float


def collect_cohort(
    jobs: tuple[QverisJob, ...],
    root: Path,
    client: QverisPort,
    *,
    budget: InvocationBudget | None = None,
    progress: Callable[[dict[str, object]], None] | None = None,
    **retry: Unpack[TransportRetry],
) -> dict[str, object]:
    """Acquire ``jobs`` in order, one at a time, recording progress after each job.

    A settled failure is recorded and the cohort continues; a provider warning is a
    completion (``warned``) and never stops it. An uncharged empty answer for an exchange
    day of actions (``QverisEmptyAnswerError``) is counted ``empty``: neither a completion
    nor a failure, so the request is asked again by rule and the cohort is not partial. An
    unsettled execution stops the cohort (``stopped``). A transport failure with no pending
    page or group retries the same job once after a pause; other preflight failures stop
    immediately. A second failure follows the usual failure handling, as does a budget refusal,
    which attempted nothing; a budget refusal with unresolved evidence in the store is
    reported as that uncertain stop.
    """
    acquisition_plan(jobs, root)
    sleep = retry.get("sleep", time.sleep)
    retry_delay_seconds = retry.get("retry_delay_seconds", 30.0)
    cohort_id = content_sha256([job.document() for job in jobs])
    completed = failed = empty = reused = warned = calls = 0
    failure_rows: list[dict[str, object]] = []
    empty_rows: list[dict[str, object]] = []
    run_id = uuid4().hex
    report_dir = root / "cohorts" / cohort_id
    publish_bytes(report_dir / "jobs.json", canonical_json_bytes([job.document() for job in jobs]))
    stopped: str | None = None
    snapshot: dict[str, object] = {}
    for index, job in enumerate(jobs):
        try:
            result = _acquire_job(job, root, client, budget, lambda: sleep(retry_delay_seconds))
        except (ValueError, TypeError, RuntimeError) as error:
            stop = budget_stop(error, root, client.account_key)
            if stop is not None:
                stopped = stop
            else:
                with QverisStore(root, client.account_key) as store:
                    unresolved = store.pending_pages()
                    prefix = f"jobs/{job.fingerprint}/"
                    terminal = store.exists(prefix + "0000.billing.json") and not unresolved
                if terminal and isinstance(error, QverisEmptyAnswerError):
                    # Settled, uncharged and empty: not a failure; the request stays open.
                    empty += 1
                    empty_rows.append(
                        {"index": index, "job_id": job.job_id, "fingerprint": job.fingerprint}
                    )
                else:
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
            "processed": completed + failed + empty,
            "completed": completed,
            "failed": failed,
            "empty": empty,
            "pending": len(jobs) - completed - failed - empty,
            "reused": reused,
            "warned": warned,
            "successful_job_calls_this_run": calls,
            "stopped": stopped,
            "observed_at_utc": datetime.now(UTC).isoformat(),
        }
        publish_bytes(
            report_dir / f"{run_id}-{index:06d}.json",
            canonical_json_bytes(
                {**snapshot, "failures": failure_rows, "empty_answers": empty_rows}
            ),
        )
        if progress:
            progress(snapshot)
        if stopped:
            break
    result = {
        **snapshot,
        "failures": failure_rows,
        "empty_answers": empty_rows,
        "status": _status(failed=failed, stopped=stopped),
        "billing_authority": "per-execution billing receipts, including failed jobs",
    }
    publish_bytes(report_dir / f"{run_id}-summary.json", canonical_json_bytes(result))
    return result


def _status(*, failed: int, stopped: str | None) -> str:
    if stopped in BUDGET_STOPS:
        return "BUDGET_EXHAUSTED"
    return "PARTIAL" if failed or stopped else "RAW_ACQUIRED"
