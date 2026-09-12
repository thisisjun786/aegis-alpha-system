"""One paid attempt per durable page intent, with independent settlement evidence."""

from __future__ import annotations

import re
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from aegis_alpha.data.qveris import RequestBudgetPort
from aegis_alpha.data.qveris_billing import (
    account_credits,
    audit_request,
    ledger_items,
    reconcile_page,
)
from aegis_alpha.data.qveris_contracts import (
    EOD_HISTORY_TOOL,
    MAX_PAGES,
    QverisJob,
    credit_value,
    load_json,
    object_value,
)
from aegis_alpha.data.qveris_payloads import validate_payload
from aegis_alpha.data.qveris_store import QverisStore, validate_destination
from aegis_alpha.data.serialization import content_sha256

MAX_OPERATOR_REASON = 500

if TYPE_CHECKING:
    from aegis_alpha.data.qveris import InvocationBudget
    from aegis_alpha.data.qveris_billing import QverisPort


def acquisition_plan(jobs: tuple[QverisJob, ...], root: Path) -> dict[str, object]:
    validate_destination("Qveris evidence root", root)
    if not jobs or len({job.fingerprint for job in jobs}) != len(jobs):
        raise ValueError("job set is empty or contains duplicate requests")
    return {
        "gateway": "qveris",
        "root": str(root),
        "execute": False,
        "jobs": [{**job.document(), "fingerprint": job.fingerprint} for job in jobs],
        "provider_calls": 0,
        "writes": 0,
        "daily_spending_cap": None,
        "spending_boundary": "server available credits; no automatic recharge",
        "raw_only": True,
        "coverage_verified": False,
    }


def _prepare_intent(  # noqa: PLR0913 -- explicit page and invocation admission
    client: QverisPort,
    store: QverisStore,
    job: QverisJob,
    parameters: dict[str, object],
    page: str,
    *,
    budget: InvocationBudget | None = None,
) -> dict[str, object]:
    if job.tool_id == EOD_HISTORY_TOOL:
        raise ValueError("CSV history route is disabled; use the reviewed JSON history tool")
    session = f"aas-qveris-{uuid4().hex}"
    metadata = audit_request(
        client,
        store,
        "/tools/by-ids",
        body={
            "tool_ids": [job.tool_id],
            "session_id": session,
        },
    )
    tools = metadata.get("results")
    search = metadata.get("search_id")
    if not isinstance(tools, list) or len(tools) != 1 or not isinstance(search, str) or not search:
        raise ValueError("Qveris inspect did not identify one exact tool")
    tool = object_value(tools[0])
    if tool.get("tool_id") != job.tool_id or tool.get("provider_id") != job.upstream:
        raise ValueError("Qveris tool upstream changed")
    schema = tool.get("params")
    if not isinstance(schema, list) or not schema:
        raise ValueError("Qveris parameter schema is unavailable")
    probe = audit_request(
        client,
        store,
        "/tools/probe",
        query={"tool_id": job.tool_id},
        body={
            "parameters": parameters,
            "checks": ["schema", "quote"],
            "live_budget": "none",
        },
    )
    quote = object_value(probe.get("quote"))
    if object_value(probe.get("schema")).get("valid") is not True:
        raise ValueError("Qveris parameter validation failed")
    if (
        quote.get("exact") is not True
        or quote.get("currency") != "credits"
        or quote.get("basis") != "per_call"
    ):
        raise ValueError("Qveris quote is not an exact per-call credit amount")
    amount = credit_value(quote.get("estimate_credits"))
    started_date = datetime.now(UTC).date().isoformat()
    balance = account_credits(client, store)
    ledger = ledger_items(client, store, started_date)
    balance_after = account_credits(client, store)
    ledger_after = ledger_items(client, store, started_date)
    if balance != balance_after or ledger != ledger_after:
        raise RuntimeError("ACCOUNT_CHANGED: retry preflight before creating an intent")
    reserved = store.reserved_credits()
    if balance - reserved < amount:
        raise RuntimeError("INSUFFICIENT_CREDITS: execute was not attempted")
    if budget is not None:
        budget.reserve(amount)
    intent: dict[str, object] = {
        "job": job.document(),
        "fingerprint": job.fingerprint,
        "tool_id": job.tool_id,
        "upstream": job.upstream,
        "parameters": parameters,
        "session_id": session,
        "search_id": search,
        "parameter_schema": schema,
        "parameter_schema_sha256": content_sha256(schema),
        "cache_mode": job.cache_mode,
        "quoted_credits": str(amount),
        "credits_before": str(balance),
        "other_reserved_credits": str(reserved),
        "ledger_ids_before": [entry["id"] for entry in ledger],
        "started_date": started_date,
        "created_at_utc": datetime.now(UTC).isoformat(),
    }
    store.publish_document(f"{page}.intent.json", intent)
    return intent


def _execute_once(
    client: QverisPort, store: QverisStore, page: str, intent: dict[str, object]
) -> None:
    store.assert_owned()
    try:
        response = client.request(
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
    except (OSError, RuntimeError, ValueError) as error:
        store.publish_document(
            f"{page}.transport-failure.json",
            {
                "error_class": type(error).__name__,
                "outcome": "UNKNOWN",
                "automatic_retry": False,
            },
        )
        return
    maximum = object_value(intent["job"])["max_response_bytes"]
    if type(maximum) is not int or len(response.body) > maximum:
        store.publish_document(
            f"{page}.transport-failure.json",
            {
                "error_class": "JobResponseTooLarge",
                "outcome": "UNKNOWN",
                "automatic_retry": False,
            },
        )
        return
    store.publish(f"{page}.raw", response.body)
    # Raw bytes survive malformed JSON; they are never silently discarded as an empty dataset.
    execution_id: object = None
    with suppress(ValueError, TypeError):
        execution_id = response.document().get("execution_id")
    store.publish_document(
        f"{page}.response.json",
        {
            "execution_id": execution_id,
            "status": response.status,
            "headers": response.headers,
            "requested_at_utc": response.requested_at_utc,
            "retrieved_at_utc": response.retrieved_at_utc,
            "raw": store.pin(f"{page}.raw"),
        },
    )


def _verified_page(store: QverisStore, page: str, job: QverisJob) -> dict[str, object]:
    if not store.exists(f"{page}.raw") or not store.exists(f"{page}.response.json"):
        raise RuntimeError("RAW_MISSING: charged or ambiguous intent cannot be executed again")
    response = store.document(f"{page}.response.json")
    store.verify(response["raw"])
    if response["status"] != HTTPStatus.OK:
        raise RuntimeError("UPSTREAM_FAILURE: raw response preserved without automatic retry")
    raw = store.read(f"{page}.raw", job.max_response_bytes)
    return object_value(load_json(raw))


def _reuse_completion(  # noqa: C901 -- validate and reconstruct an immutable marker
    client: QverisPort, store: QverisStore, job: QverisJob
) -> dict[str, object] | None:
    marker = f"jobs/{job.fingerprint}/complete.json"
    if not store.exists(marker):
        return None
    result = store.document(marker)
    if result.get("fingerprint") != job.fingerprint:
        raise ValueError("Qveris completion request differs")
    pins = result.get("files")
    if not isinstance(pins, list):
        raise TypeError("Qveris completion pins are missing")
    pages = result.get("pages")
    if type(pages) is not int or not 1 <= pages <= MAX_PAGES:
        raise ValueError("invalid Qveris completed page count")
    expected = [
        f"jobs/{job.fingerprint}/{number:04d}.{suffix}"
        for number in range(pages)
        for suffix in ("intent.json", "raw", "response.json", "billing.json")
    ]
    if [object_value(pin).get("path") for pin in pins] != expected:
        raise ValueError("Qveris completion does not pin every expected page artifact")
    for pin in pins:
        entry = object_value(pin)
        size = entry.get("size")
        if str(entry["path"]).endswith(".raw") and (
            type(size) is not int or size > job.max_response_bytes
        ):
            raise ValueError("cached response exceeds the requested byte limit")
        store.verify(pin)
    original = QverisJob.from_document(result.get("job"))
    if original.fingerprint != job.fingerprint:
        raise ValueError("Qveris completion job differs")
    completed_at = result.get("completed_at_utc")
    if not isinstance(completed_at, str):
        raise TypeError("Qveris completion timestamp is missing")
    recomputed = _finish_job(
        client, store, original, completed_at=completed_at, allow_execute=False
    )
    for key in ("reused", "provider_calls_this_run"):
        del recomputed[key]
    if recomputed != result:
        raise ValueError("Qveris completion differs from its pinned pages")
    return {**result, "reused": True, "provider_calls_this_run": 0}


def _finish_job(  # noqa: C901, PLR0912, PLR0913, PLR0915 -- sequential durable page state transitions
    client: QverisPort,
    store: QverisStore,
    job: QverisJob,
    *,
    completed_at: str | None = None,
    allow_execute: bool = True,
    budget: InvocationBudget | None = None,
) -> dict[str, object]:
    parameters = job.parameters
    files: list[dict[str, object]] = []
    keys: set[str] = set()
    total: int | None = None
    rows = calls = 0
    cost = Decimal(0)
    previous_key: str | None = None
    warnings: list[dict[str, object]] = []
    for number in range(MAX_PAGES):
        page = f"jobs/{job.fingerprint}/{number:04d}"
        if store.exists(f"{page}.quarantine.json") and not store.exists(f"{page}.billing.json"):
            raise RuntimeError("QUARANTINED: this intent will never execute again")
        if store.exists(f"{page}.intent.json"):
            intent = store.document(f"{page}.intent.json")
            if (
                intent.get("parameters") != parameters
                or intent.get("fingerprint") != job.fingerprint
            ):
                raise ValueError("Qveris persisted page differs from requested job")
        else:
            if not allow_execute:
                raise ValueError("completed Qveris page intent is missing")
            intent = _prepare_intent(client, store, job, parameters, page, budget=budget)
            _execute_once(client, store, page, intent)
            calls += 1
        if not allow_execute and not store.exists(f"{page}.billing.json"):
            raise ValueError("completed Qveris page billing is missing")
        billing = reconcile_page(client, store, page)
        if billing.get("over_quote") is not False:
            raise RuntimeError("QUOTE_EXCEEDED: charge preserved; new execution stopped")
        document = _verified_page(store, page, job)
        if billing.get("execution_id") != document.get("execution_id"):
            raise ValueError("Qveris response and billing execution IDs differ")
        retrieved_at = store.document(f"{page}.response.json")["retrieved_at_utc"]
        retrieved_on = datetime.fromisoformat(str(retrieved_at)).date()
        shape = validate_payload(job, parameters, document, retrieved_on=retrieved_on)
        if total is not None and shape.total != total:
            raise ValueError("provider count changed between pages")
        total = shape.total
        if keys.intersection(shape.keys):
            raise ValueError("provider repeated observations across pages")
        if shape.next_offset is not None or total is not None:
            if shape.keys and previous_key is not None and shape.keys[0] <= previous_key:
                raise ValueError("provider pages are not globally ordered")
            if shape.keys:
                previous_key = shape.keys[-1]
        keys.update(shape.keys)
        rows += shape.rows
        cost += credit_value(billing["settled_credits"])
        usage = object_value(billing["usage"])
        if usage.get("outcome") not in {None, "success"}:
            warnings.append(
                {
                    "page": number,
                    "execution_id": billing["execution_id"],
                    "outcome": usage.get("outcome"),
                    "reason_code": usage.get("reason_code"),
                }
            )
        files.extend(
            store.pin(f"{page}.{suffix}")
            for suffix in ("intent.json", "raw", "response.json", "billing.json")
        )
        if shape.next_offset is None:
            result: dict[str, object] = {
                "schema_version": 1,
                "gateway": "qveris",
                "upstream": job.upstream,
                "market": job.market,
                "dataset": job.dataset,
                "job": job.document(),
                "fingerprint": job.fingerprint,
                "pages": number + 1,
                "rows": rows,
                "settled_credits": str(cost),
                "files": files,
                "status": "RAW_ACQUIRED",
                "coverage_verified": False,
                "canonical_eligible": False,
                "backtest_eligible": False,
                "completed_at_utc": completed_at or datetime.now(UTC).isoformat(),
            }
            if warnings:
                result.update(
                    status="RAW_ACQUIRED_WITH_WARNINGS",
                    gateway_warnings=warnings,
                    provider_completeness_verified=False,
                )
            if allow_execute:
                store.publish_document(f"jobs/{job.fingerprint}/complete.json", result)
            return {**result, "reused": False, "provider_calls_this_run": calls}
        parameters = {**parameters, "offset": shape.next_offset}
    raise RuntimeError("INCOMPLETE: provider page bound reached")


def acquire_jobs(
    jobs: tuple[QverisJob, ...],
    root: Path,
    client: QverisPort,
    *,
    budget: InvocationBudget | None = None,
) -> dict[str, object]:
    acquisition_plan(jobs, root)
    outcomes: list[dict[str, object]] = []
    with QverisStore(root, client.account_key) as store:
        store.require_no_pending_batches()
        if any(job.fingerprint in store.quarantined_batch_fingerprints() for job in jobs):
            raise RuntimeError("QUARANTINED: batch members cannot be executed again")
        for job in jobs:
            reused = _reuse_completion(client, store, job)
            if reused is not None:
                outcomes.append(reused)
                continue
            # Uncertain charged responses retain reservations across restarts until audited.
            for pending in store.pending_pages():
                billing = reconcile_page(client, store, pending)
                if billing.get("over_quote") is not False:
                    raise RuntimeError("QUOTE_EXCEEDED: reconcile account before new execution")
            outcomes.append(_finish_job(client, store, job, budget=budget))
    return {
        "status": "RAW_ACQUIRED_WITH_WARNINGS"
        if any(item["status"] != "RAW_ACQUIRED" for item in outcomes)
        else "RAW_ACQUIRED",
        "jobs": outcomes,
        "provider_calls_this_run": sum(
            int(str(item["provider_calls_this_run"])) for item in outcomes
        ),
        "daily_spending_cap": None,
        "coverage_verified": False,
    }


def reconcile_pending(root: Path, client: QverisPort) -> dict[str, object]:
    client = RequestBudgetPort(client, 256, 900)
    with QverisStore(root, client.account_key) as store:
        store.require_no_pending_batches()
        pages = store.pending_pages(include_quarantined=True)
        outcomes = [
            {"page": page, "billing": reconcile_page(client, store, page)} for page in pages
        ]
    return {
        "provider_calls": 0,
        "paid_executions": 0,
        "http_requests": client.http_requests,
        "reconciled_pages": outcomes,
    }


def quarantine_pending(root: Path, page: str, reason: str, client: QverisPort) -> dict[str, object]:
    """Explicit operator action; retain an unknown charge as a worst-case reservation."""
    if re.fullmatch(r"jobs/[0-9a-f]{64}/[0-9]{4}", page) is None:
        raise ValueError("quarantine requires an exact existing page path")
    if not reason.strip() or len(reason) > MAX_OPERATOR_REASON:
        raise ValueError("quarantine requires a short operator reason")
    client = RequestBudgetPort(client, 256, 900)
    with QverisStore(root, client.account_key) as store:
        if page not in store.pending_pages(include_quarantined=True):
            raise ValueError("only an unresolved existing page can be quarantined")
        if store.exists(f"{page}.billing.json"):
            raise ValueError("quote overrun needs account investigation, not quarantine")
        # Append current audit evidence. Empty usage is not proof that no charge occurred.
        status = "settled"
        try:
            reconcile_page(client, store, page)
        except (RuntimeError, ValueError, TypeError) as error:
            status = type(error).__name__
        if store.exists(f"{page}.billing.json"):
            return {
                "page": page,
                "status": "SETTLED",
                "provider_calls": 0,
                "paid_executions": 0,
                "http_requests": client.http_requests,
            }
        document = {
            "page": page,
            "intent": store.pin(f"{page}.intent.json"),
            "status": "SETTLEMENT_UNKNOWN",
            "automatic_retry": False,
            "reserved_credits": str(
                credit_value(store.document(f"{page}.intent.json")["quoted_credits"])
            ),
            "audit_outcome": status,
            "reason": reason.strip(),
            "operator_action_at_utc": datetime.now(UTC).isoformat(),
        }
        destination = f"{page}.quarantine.json"
        if store.exists(destination):
            document = store.document(destination)
        else:
            store.publish_document(destination, document)
        return {
            **document,
            "provider_calls": 0,
            "paid_executions": 0,
            "http_requests": client.http_requests,
        }
