"""Bounded single-page acquisition, durable aggregate reservation and settlement.

A coordinator quotes every fresh job of one group, reserves the whole group against the
server balance (less other reservations) and the invocation budget, records one batch
manifest and one intent per job, executes the group on worker clients, drains every
in-flight paid future, and settles the group once against usage and the account ledger.
A group never executes again: an unsettled group blocks the account until it settles or
an operator quarantines it with its reservation retained.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from aegis_alpha.data.qveris_acquisition import (
    MAX_OPERATOR_REASON,
    _finish_job,
    _reuse_completion,
    acquisition_plan,
)
from aegis_alpha.data.qveris_billing import (
    QverisPort,
    account_credits,
    audit_items,
    ledger_items,
    reconcile_page,
    sanitized,
)
from aegis_alpha.data.qveris_client import QverisResponse
from aegis_alpha.data.qveris_contracts import (
    EOD_HISTORY_JSON_TOOL,
    EOD_TOOL,
    SEC_TOOL,
    QverisJob,
    credit_value,
    object_value,
)
from aegis_alpha.data.qveris_store import QverisStore
from aegis_alpha.data.serialization import content_sha256

if TYPE_CHECKING:
    from aegis_alpha.data.qveris import InvocationBudget

SUPPORTED_TOOLS = frozenset({EOD_TOOL, EOD_HISTORY_JSON_TOOL, SEC_TOOL})
MAX_WORKERS = 16
DEFAULT_WORKERS = 4
LOCK_WAIT_SECONDS = 60


def _stable_account(
    client: QverisPort, store: QverisStore, day: str
) -> tuple[Decimal, list[dict[str, object]]]:
    before = account_credits(client, store)
    ledger = ledger_items(client, store, day)
    after = account_credits(client, store)
    last = ledger_items(client, store, day)
    if before != after or ledger != last:
        raise RuntimeError("ACCOUNT_CHANGED: restart read-only preflight")
    return after, last


def _metadata(
    factory: Callable[[], QverisPort], job: QverisJob, account: str
) -> tuple[QverisResponse, QverisResponse]:
    client = factory()
    if client.account_key != account:
        raise ValueError("worker account differs")
    metadata = client.request("/tools/by-ids", body={"tool_ids": [job.tool_id]})
    probe = client.request(
        "/tools/probe",
        query={"tool_id": job.tool_id},
        body={"parameters": job.parameters, "checks": ["schema", "quote"], "live_budget": "none"},
    )
    return metadata, probe


def _quote(
    store: QverisStore, base: str, job: QverisJob, responses: tuple[QverisResponse, QverisResponse]
) -> dict[str, object]:
    metadata, probe = responses
    for label, response in zip(("metadata", "probe"), responses, strict=True):
        store.publish(f"{base}/{job.fingerprint}.{label}.raw", response.body)
        if response.status != HTTPStatus.OK:
            raise RuntimeError("metadata/probe HTTP failure")
    meta, check = metadata.document(), probe.document()
    tools = meta.get("results")
    if not isinstance(tools, list) or len(tools) != 1:
        raise ValueError("expected one tool")
    tool = object_value(tools[0])
    if tool.get("tool_id") != job.tool_id or tool.get("provider_id") != job.upstream:
        raise ValueError("tool/upstream changed")
    schema = tool.get("params")
    if not isinstance(schema, list) or not schema or not isinstance(meta.get("search_id"), str):
        raise ValueError("missing schema/search")
    quote = object_value(check.get("quote"))
    if (
        object_value(check.get("schema")).get("valid") is not True
        or quote.get("exact") is not True
        or quote.get("basis") != "per_call"
        or quote.get("currency") != "credits"
    ):
        raise ValueError("exact validated quote required")
    return {
        "search_id": meta["search_id"],
        "parameter_schema": schema,
        "parameter_schema_sha256": content_sha256(schema),
        "quoted_credits": str(credit_value(quote.get("estimate_credits"))),
    }


def _execute(
    factory: Callable[[], QverisPort], intent: dict[str, object], account: str
) -> QverisResponse:
    client = factory()
    if client.account_key != account:
        raise ValueError("worker account differs")
    return client.request(
        "/tools/execute",
        query={"tool_id": str(intent["tool_id"])},
        body={
            "parameters": intent["parameters"],
            "search_id": intent["search_id"],
            "session_id": intent["session_id"],
            "max_response_size": -1,
            "respond_with": "full",
        },
    )


def _record_response(store: QverisStore, page: str, response: QverisResponse) -> None:
    store.publish(f"{page}.raw", response.body)
    execution: object = None
    with suppress(ValueError):
        execution = response.document().get("execution_id")
    store.publish_document(
        f"{page}.response.json",
        {
            "execution_id": execution,
            "status": response.status,
            "headers": response.headers,
            "requested_at_utc": response.requested_at_utc,
            "retrieved_at_utc": response.retrieved_at_utc,
            "raw": store.pin(f"{page}.raw"),
        },
    )


def _usage(
    client: QverisPort, store: QverisStore, intent: dict[str, object], page: str
) -> dict[str, object]:
    execution = (
        store.document(f"{page}.response.json").get("execution_id")
        if store.exists(f"{page}.response.json")
        else None
    )
    query: dict[str, str | int] = {
        "start_date": str(intent["started_date"]),
        "end_date": datetime.now(UTC).date().isoformat(),
        "search_id": str(intent["search_id"]),
        "include_details": "false",
    }
    if isinstance(execution, str):
        query["execution_id"] = execution
    items = audit_items(client, store, "/auth/usage/history/v2", query)
    matching = [
        row
        for row in items
        if row.get("session_id") == intent["session_id"]
        and row.get("tool_id") == intent["tool_id"]
        and row.get("search_id") == intent["search_id"]
        and (execution is None or row.get("execution_id") == execution)
    ]
    if len(matching) != 1:
        raise RuntimeError("PENDING_BATCH: exactly one usage event required; no automatic retry")
    row = matching[0]
    outcome = row.get("charge_outcome")
    actual = credit_value(row.get("actual_amount_credits"))
    if outcome not in {
        "charged",
        "not_charged",
        "failed_not_charged",
        "included",
    } or actual != credit_value(row.get("settled_amount_credits")):
        raise RuntimeError("PENDING_BATCH: unsettled usage")
    if actual > credit_value(intent["quoted_credits"]):
        raise RuntimeError("QUOTE_EXCEEDED")
    if outcome in {"not_charged", "failed_not_charged", "included"} and actual != 0:
        raise RuntimeError("PENDING_BATCH: invalid uncharged usage")
    if outcome == "included" and credit_value(intent["quoted_credits"]) != 0:
        raise RuntimeError("PENDING_BATCH: included quote differs")
    failed = (
        outcome == "failed_not_charged"
        and row.get("success") is False
        and row.get("billable_success") is False
    )
    if row.get("billing_unit") != "call" or (not failed and credit_value(row.get("quantity")) != 1):
        raise RuntimeError("PENDING_BATCH: unexpected quantity/unit")
    return row


def _signed(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise TypeError("invalid signed ledger value")
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError("nonfinite ledger value")
    return number


def _settle(  # noqa: C901, PLR0912, PLR0915 -- ordered crash-safe settlement state machine
    client: QverisPort, store: QverisStore, base: str
) -> tuple[list[dict[str, object]], bool]:
    manifest = store.document(f"{base}/manifest.json")
    intents = [object_value(x) for x in cast("list[object]", manifest["intents"])]
    # Incomplete intent sets retain reservations; they never authorize a retry.
    for intent in intents:
        page = str(intent["page"])
        if (
            not store.exists(f"{page}.intent.json")
            or store.document(f"{page}.intent.json") != intent
        ):
            raise RuntimeError("PENDING_BATCH: incomplete durable intent set")
    usage = [_usage(client, store, intent, str(intent["page"])) for intent in intents]
    total = sum((credit_value(r["settled_amount_credits"]) for r in usage), Decimal(0))
    before = credit_value(manifest["credits_before"])
    for intent in intents:
        page = str(intent["page"])
        if store.exists(f"{page}.response.json") and store.exists(f"{page}.raw"):
            store.verify(store.document(f"{page}.response.json")["raw"])
    settlement_path = f"{base}/settlement.json"
    usage_identity = [
        {
            "id": row["id"],
            "execution_id": row["execution_id"],
            "settled_credits": str(credit_value(row["settled_amount_credits"])),
        }
        for row in usage
    ]
    if not store.exists(settlement_path):
        after, ledger = _stable_account(client, store, str(manifest["started_date"]))
        prior_ids = manifest["ledger_ids_before"]
        entries = [r for r in ledger if r["id"] not in cast("list[object]", prior_ids)]
        delta = sum((_signed(r.get("amount_credits")) for r in entries), Decimal(0))
        debits = sum(
            (-min(_signed(r.get("amount_credits")), Decimal(0)) for r in entries), Decimal(0)
        )
        if (
            after - before != delta
            or debits < total
            or total > credit_value(manifest["reserved_credits"])
        ):
            raise RuntimeError("PENDING_BATCH: aggregate settlement does not reconcile")
        store.publish_document(
            settlement_path,
            {
                "manifest": store.pin(f"{base}/manifest.json"),
                "usage": usage_identity,
                "credits_after": str(after),
                "ledger_delta": str(delta),
                "settled_credits": str(total),
                "ledger_entry_ids": [r["id"] for r in entries],
                "observed_at_utc": datetime.now(UTC).isoformat(),
            },
        )
    settlement = store.document(settlement_path)
    store.verify(settlement["manifest"])
    if (
        settlement["usage"] != usage_identity
        or credit_value(settlement["settled_credits"]) != total
    ):
        raise RuntimeError("PENDING_BATCH: settled usage changed")
    after = credit_value(settlement["credits_after"])
    delta = _signed(settlement["ledger_delta"])
    results: list[dict[str, object]] = []
    stopped = False
    for intent, row in zip(intents, usage, strict=True):
        page = str(intent["page"])
        settled = credit_value(row["settled_amount_credits"])
        billing = {
            "batch_id": base,
            "batch_settlement": store.pin(settlement_path),
            "settlement_scope": "batch",
            "batch_settled_credits": str(total),
            "execution_id": row.get("execution_id"),
            "usage_event_id": row["id"],
            "settled_credits": str(settled),
            "quoted_credits": intent["quoted_credits"],
            "charge_outcome": row["charge_outcome"],
            "billing_unit": "call",
            "quantity": row.get("quantity"),
            "usage": sanitized(row),
            "credits_before": str(before),
            "credits_after": str(after),
            "account_ledger_delta": str(delta),
            "other_account_delta": str(delta + total),
            "ledger_entry_ids": settlement["ledger_entry_ids"],
            "observed_at_utc": settlement["observed_at_utc"],
            "over_quote": False,
        }
        if not store.exists(f"{page}.billing.json"):
            store.publish_document(f"{page}.billing.json", billing)
        job = QverisJob.from_document(intent["job"])
        try:
            marker = f"jobs/{job.fingerprint}/complete.json"
            if store.exists(marker):
                result = _reuse_completion(client, store, job)
                if result is None:
                    raise ValueError("missing completion")  # noqa: TRY301 -- kept as failed evidence
            else:
                result = _finish_job(client, store, job, allow_execute=False)
                stored = {
                    k: v
                    for k, v in result.items()
                    if k not in {"reused", "provider_calls_this_run"}
                }
                store.publish_document(marker, stored)
        except (ValueError, RuntimeError, TypeError) as error:
            result = {
                "job_id": job.job_id,
                "fingerprint": job.fingerprint,
                "status": "FAILED",
                "reused": False,
                "provider_calls_this_run": 0,
                "reason": str(error),
            }
        with suppress(ValueError):
            if store.exists(f"{page}.response.json"):
                stopped = stopped or store.document(f"{page}.response.json")["status"] in {
                    401,
                    403,
                    429,
                }
            if store.exists(f"{page}.raw"):
                upstream = object_value(store.document(f"{page}.raw").get("result"))
                stopped = stopped or upstream.get("status_code") in {401, 403, 429}
        results.append(object_value(result))
    store.publish_document(
        f"{base}/complete.json",
        {
            "status": "SETTLED",
            "manifest": store.pin(f"{base}/manifest.json"),
            "settled_credits": str(total),
            "stopped": stopped,
            "jobs": results,
        },
    )
    return results, stopped


def _reserve_group(budget: InvocationBudget, amounts: list[Decimal]) -> None:
    """Reserve every quote of a group or none: a trial copy must admit them all first."""
    trial = type(budget)(budget.max_calls, budget.max_credits)
    trial.reserved_calls, trial.reserved_credits = budget.reserved_calls, budget.reserved_credits
    for amount in amounts:
        trial.reserve(amount)
    for amount in amounts:
        budget.reserve(amount)


def _acquire_locked(  # noqa: C901, PLR0912, PLR0913, PLR0915 -- ordered coordinator crash boundaries
    jobs: tuple[QverisJob, ...],
    store: QverisStore,
    client: QverisPort,
    factory: Callable[[], QverisPort],
    workers: int,
    *,
    budget: InvocationBudget | None,
) -> dict[str, object]:
    for base in store.pending_batches():
        _, stop = _settle(client, store, base)
        if stop:
            raise RuntimeError("PROVIDER_STOP: prior batch auth/quota failure")
    for page in store.pending_pages():
        if reconcile_page(client, store, page).get("over_quote") is not False:
            raise RuntimeError("QUOTE_EXCEEDED")
    old: dict[str, dict[str, object]] = {}
    fresh = []
    quarantined = store.quarantined_batch_fingerprints()
    for job in jobs:
        if job.fingerprint in quarantined:
            old[job.fingerprint] = {
                "fingerprint": job.fingerprint,
                "status": "FAILED",
                "reused": True,
                "provider_calls_this_run": 0,
                "reason": "QUARANTINED_BATCH",
            }
            continue
        cached = _reuse_completion(client, store, job)
        if cached is not None:
            old[job.fingerprint] = cached
        elif store.exists(f"jobs/{job.fingerprint}/0000.intent.json"):
            try:
                result = _finish_job(client, store, job, allow_execute=False)
                store.publish_document(
                    f"jobs/{job.fingerprint}/complete.json",
                    {
                        k: v
                        for k, v in result.items()
                        if k not in {"reused", "provider_calls_this_run"}
                    },
                )
                old[job.fingerprint] = {**result, "reused": True}
            except (ValueError, RuntimeError, TypeError):
                old[job.fingerprint] = {
                    "fingerprint": job.fingerprint,
                    "status": "FAILED",
                    "reused": True,
                    "provider_calls_this_run": 0,
                }
        else:
            fresh.append(job)
    if not fresh:
        return {
            "status": "REUSED",
            "jobs": [old[j.fingerprint] for j in jobs],
            "provider_calls_this_run": 0,
        }
    base = f"parallel-batches/{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_metadata, factory, job, client.account_key): job for job in fresh}
        quotes = {
            job.fingerprint: _quote(store, base, job, future.result())
            for future, job in futures.items()
        }
    day = datetime.now(UTC).date().isoformat()
    before, ledger = _stable_account(client, store, day)
    amounts = [credit_value(quotes[job.fingerprint]["quoted_credits"]) for job in fresh]
    reserved = sum(amounts, Decimal(0))
    if before - store.reserved_credits() < reserved:
        raise RuntimeError("INSUFFICIENT_CREDITS: no execute attempted")
    if budget is not None:
        _reserve_group(budget, amounts)
    intents = []
    for job in fresh:
        intents.append(  # noqa: PERF401 -- explicit per-job immutable intent construction
            {
                "job": job.document(),
                "fingerprint": job.fingerprint,
                "tool_id": job.tool_id,
                "upstream": job.upstream,
                "parameters": job.parameters,
                "page": f"jobs/{job.fingerprint}/0000",
                "batch_id": base,
                "session_id": f"aas-parallel-{uuid4().hex}",
                "cache_mode": job.cache_mode,
                "credits_before": str(before),
                "ledger_ids_before": [r["id"] for r in ledger],
                "started_date": day,
                "created_at_utc": datetime.now(UTC).isoformat(),
                **quotes[job.fingerprint],
            }
        )
    store.publish_document(
        f"{base}/manifest.json",
        {
            "intents": intents,
            "reserved_credits": str(reserved),
            "credits_before": str(before),
            "ledger_ids_before": [r["id"] for r in ledger],
            "started_date": day,
        },
    )
    for intent in intents:
        store.publish_document(f"{intent['page']}.intent.json", intent)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        executions = {
            pool.submit(_execute, factory, intent, client.account_key): intent for intent in intents
        }
        record_failed = False
        for future in as_completed(executions):
            page = str(executions[future]["page"])
            try:
                response = future.result()
            except Exception as error:  # noqa: BLE001 -- drain every in-flight paid future
                try:
                    store.publish_document(
                        f"{page}.transport-failure.json",
                        {
                            "error_class": type(error).__name__,
                            "outcome": "UNKNOWN",
                            "automatic_retry": False,
                        },
                    )
                except (OSError, RuntimeError, ValueError):
                    record_failed = True
            else:
                try:
                    _record_response(store, page, response)
                except (OSError, RuntimeError, ValueError):
                    record_failed = True
                    with suppress(OSError, RuntimeError, ValueError):
                        store.publish_document(
                            f"{page}.record-failure.json",
                            {"error_class": "RESPONSE_WRITE_FAILED", "automatic_retry": False},
                        )
    if record_failed:
        raise RuntimeError("RESPONSE_WRITE_FAILED: all futures drained; recover saved evidence")
    results, stop = _settle(client, store, base)
    for result in results:
        old[str(result["fingerprint"])] = result
    if stop:
        raise RuntimeError("PROVIDER_STOP: batch settled; auth/quota failure")
    return {
        "status": "SETTLED",
        "jobs": [old[j.fingerprint] for j in jobs],
        "provider_calls_this_run": len(fresh),
    }


def acquire_parallel_jobs(
    jobs: tuple[QverisJob, ...],
    root: Path,
    client_factory: Callable[[], QverisPort],
    *,
    workers: int = DEFAULT_WORKERS,
    budget: InvocationBudget | None = None,
) -> dict[str, object]:
    acquisition_plan(jobs, root)
    if type(workers) is not int or not 1 <= workers <= MAX_WORKERS or len(jobs) > workers:
        raise ValueError("batch/worker bound is invalid")
    if any(job.tool_id not in SUPPORTED_TOOLS for job in jobs):
        raise ValueError("parallel acquisition supports reviewed single-page tools only")
    client = client_factory()
    store = QverisStore(root, client.account_key)
    deadline = time.monotonic() + LOCK_WAIT_SECONDS
    while True:
        try:
            store.__enter__()
            break
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.05)
    try:
        return _acquire_locked(jobs, store, client, client_factory, workers, budget=budget)
    finally:
        store.__exit__(None, None, None)


def quarantine_parallel_batch(
    root: Path, batch_id: str, reason: str, client: QverisPort
) -> dict[str, object]:
    """Explicitly isolate an unresolved group, retaining its entire reservation."""
    if re.fullmatch(r"parallel-batches/[a-f0-9]{32}", batch_id) is None:
        raise ValueError("invalid batch identifier")
    if not reason.strip() or len(reason) > MAX_OPERATOR_REASON:
        raise ValueError("operator reason is required and bounded")
    with QverisStore(root, client.account_key) as store:
        if batch_id not in store.pending_batches(include_quarantined=True):
            raise ValueError("batch is not pending")
        manifest = store.document(f"{batch_id}/manifest.json")
        record = {
            "manifest": store.pin(f"{batch_id}/manifest.json"),
            "reason": reason,
            "reserved_credits": manifest["reserved_credits"],
            "automatic_retry": False,
        }
        store.publish_document(f"{batch_id}/quarantine.json", record)
        return {
            "status": "QUARANTINED",
            "batch_id": batch_id,
            "reserved_credits": record["reserved_credits"],
        }
