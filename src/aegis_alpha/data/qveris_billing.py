"""Read usage and account ledger independently of data parsing success."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from http import HTTPStatus
from typing import TYPE_CHECKING, Protocol
from uuid import uuid4

from aegis_alpha.data.qveris_contracts import MAX_PAGES, credit_value, object_value

if TYPE_CHECKING:
    from aegis_alpha.data.qveris_client import QverisResponse
    from aegis_alpha.data.qveris_store import QverisStore


class QverisPort(Protocol):
    @property
    def account_key(self) -> str: ...

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse: ...


def sanitized(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: sanitized(item)
            for key, item in value.items()
            if str(key).lower()
            not in {"api_key", "authorization", "access_token", "response_payload_summary"}
        }
    if isinstance(value, list):
        return [sanitized(item) for item in value]
    return value


def audit_request(
    client: QverisPort,
    store: QverisStore,
    path: str,
    *,
    body: dict[str, object] | None = None,
    query: dict[str, str | int] | None = None,
) -> dict[str, object]:
    store.assert_owned()
    response = client.request(path, body=body, query=query)
    document = response.document()
    store.publish_document(
        f"audits/{uuid4().hex}.json",
        {
            "path": path,
            "body": body,
            "query": query,
            "status": response.status,
            "headers": response.headers,
            "requested_at_utc": response.requested_at_utc,
            "retrieved_at_utc": response.retrieved_at_utc,
            "response": sanitized(document),
            "representation": "credential-attribution-redacted JSON projection",
        },
    )
    if response.status != HTTPStatus.OK:
        raise RuntimeError("Qveris audit endpoint returned an unsuccessful HTTP status")
    return document


def account_credits(client: QverisPort, store: QverisStore) -> Decimal:
    document = audit_request(client, store, "/auth/credits")
    if document.get("status") != "success":
        raise ValueError("Qveris balance response was not successful")
    return credit_value(object_value(document.get("data")).get("remaining_credits"))


def audit_items(
    client: QverisPort,
    store: QverisStore,
    path: str,
    query: dict[str, str | int],
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    total: int | None = None
    for page in range(1, MAX_PAGES + 1):
        document = audit_request(
            client, store, path, query={**query, "page": page, "page_size": 500}
        )
        if document.get("status") != "success":
            raise ValueError("Qveris audit page was not successful")
        data = object_value(document.get("data"))
        count = data.get("total")
        if type(count) is not int or count < 0 or (total is not None and count != total):
            raise ValueError("Qveris audit count is missing or changed during paging")
        total = count
        rows = data.get("items")
        if not isinstance(rows, list) or data.get("page") != page:
            raise ValueError("invalid Qveris audit page")
        if len(rows) != min(500, total - len(items)):
            raise ValueError("Qveris audit page is short or oversized")
        items.extend(object_value(row) for row in rows)
        if len(items) == total:
            identifiers = [row.get("id") for row in items]
            if any(not isinstance(item, str) for item in identifiers) or len(
                set(identifiers)
            ) != len(items):
                raise ValueError("Qveris audit contains duplicate or missing IDs")
            return items
    raise ValueError("Qveris audit page bound reached")


def ledger_items(client: QverisPort, store: QverisStore, start: str) -> list[dict[str, object]]:
    return audit_items(
        client,
        store,
        "/auth/credits/ledger",
        {
            "start_date": start,
            "end_date": datetime.now(UTC).date().isoformat(),
        },
    )


def _signed_credit(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, float, int)):
        raise TypeError("invalid ledger amount")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("non-finite ledger amount")
    return result


def reconcile_page(  # noqa: C901 -- independent identity, settlement, and ledger gates
    client: QverisPort, store: QverisStore, page: str
) -> dict[str, object]:
    """Never execute a tool, even when usage proves a charged response was lost."""
    if store.exists(f"{page}.billing.json"):
        return store.document(f"{page}.billing.json")
    intent = store.document(f"{page}.intent.json")
    response_file = f"{page}.response.json"
    execution_id: object = None
    if store.exists(response_file):
        execution_id = store.document(response_file).get("execution_id")
    query: dict[str, str | int] = {
        "start_date": str(intent["started_date"]),
        "end_date": datetime.now(UTC).date().isoformat(),
        "search_id": str(intent["search_id"]),
        "include_details": "false",
    }
    if isinstance(execution_id, str):
        query["execution_id"] = execution_id
    items = audit_items(client, store, "/auth/usage/history/v2", query)
    matches = [
        row
        for row in items
        if row.get("session_id") == intent["session_id"]
        and row.get("tool_id") == intent["tool_id"]
        and row.get("search_id") == intent["search_id"]
        and (execution_id is None or row.get("execution_id") == execution_id)
    ]
    if len(matches) != 1:
        raise RuntimeError("PENDING_SETTLEMENT: exactly one matching usage event is required")
    usage = matches[0]
    if usage.get("charge_outcome") not in {
        "charged",
        "not_charged",
        "included",
        "failed_not_charged",
    }:
        raise RuntimeError("PENDING_SETTLEMENT: usage charge outcome is unknown")
    settled = credit_value(usage.get("settled_amount_credits"))
    actual = credit_value(usage.get("actual_amount_credits"))
    if usage["charge_outcome"] == "included" and (
        settled != 0 or credit_value(intent["quoted_credits"]) != 0
    ):
        raise RuntimeError(
            "PENDING_SETTLEMENT: included charge requires a verified zero quote and settlement"
        )
    if settled != actual or (
        usage["charge_outcome"] in {"not_charged", "failed_not_charged"} and settled != 0
    ):
        raise RuntimeError("PENDING_SETTLEMENT: inconsistent usage amounts")
    failure_without_charge = (
        usage["charge_outcome"] == "failed_not_charged"
        and usage.get("success") is False
        and usage.get("billable_success") is False
    )
    quantity = usage.get("quantity")
    if usage.get("billing_unit") != "call" or (
        not failure_without_charge and credit_value(quantity) != 1
    ):
        raise RuntimeError("PENDING_SETTLEMENT: unexpected billing unit or quantity")
    ledger = ledger_items(client, store, str(intent["started_date"]))
    after = account_credits(client, store)
    before_ids = intent["ledger_ids_before"]
    if not isinstance(before_ids, list):
        raise TypeError("invalid intent ledger cursor")
    new_entries = [entry for entry in ledger if entry["id"] not in before_ids]
    ledger_delta = sum(
        (_signed_credit(entry.get("amount_credits")) for entry in new_entries), Decimal(0)
    )
    debits = sum(
        (-min(_signed_credit(entry.get("amount_credits")), Decimal(0)) for entry in new_entries),
        Decimal(0),
    )
    before = credit_value(intent["credits_before"])
    if after - before != ledger_delta or debits < settled:
        raise RuntimeError("PENDING_SETTLEMENT: account balance and ledger have not reconciled")
    result: dict[str, object] = {
        "execution_id": usage.get("execution_id"),
        "usage_event_id": usage["id"],
        "settled_credits": str(settled),
        "quoted_credits": intent["quoted_credits"],
        "charge_outcome": usage["charge_outcome"],
        "billing_unit": "call",
        "quantity": quantity,
        "usage": sanitized(usage),
        "credits_before": str(before),
        "credits_after": str(after),
        "account_ledger_delta": str(ledger_delta),
        "other_account_delta": str(ledger_delta + settled),
        "ledger_entry_ids": [entry["id"] for entry in new_entries],
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "over_quote": settled > credit_value(intent["quoted_credits"]),
    }
    store.publish_document(f"{page}.billing.json", result)
    return result
