from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import urllib.error
import urllib.request
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast

TASK_ID = "AAS-DATA-008A"
ENDPOINT = "https://api.finimpulse.com/v1/analysis/earnings"
CREDENTIAL_ENVIRONMENT_VARIABLE = "FINIMPULSE_API_TOKEN"
REQUESTED_TYPES = ("eps_trend", "eps_revisions")
DEFAULT_SYMBOLS = (
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOGL",
    "META",
    "JPM",
    "XOM",
    "JNJ",
    "WMT",
    "CAT",
    "LLY",
    "ROKU",
    "UAL",
    "DOCU",
    "ENPH",
    "CROX",
    "UPWK",
    "FIGS",
    "PLAB",
)
MAX_SYMBOLS = 20
MAIN_PAGE_LIMIT = 20
CALL_PRICE_USD = Decimal("0.0008")
ROW_PRICE_USD = Decimal("0.00015")
BUDGET_USD = Decimal("0.10")
HTTP_OK = 200
PROVIDER_STATUS_OK = 20000
PAGINATION_LIMIT = 1
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ALLOWED_RESPONSE_HEADERS = (
    "content-type",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
)
SENSITIVE_KEY_PARTS = (
    "apikey",
    "authorization",
    "cookie",
    "credential",
    "password",
    "secret",
    "token",
)
SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")
EXPECTED_TYPE_FIELDS = {
    "eps_trend": (
        "type",
        "date",
        "date_type",
        "current",
        "seven_days_ago",
        "thirty_days_ago",
        "sixty_days_ago",
        "ninety_days_ago",
    ),
    "eps_revisions": (
        "type",
        "date",
        "date_type",
        "up_last7days",
        "up_last30days",
        "down_last7days",
        "down_last30days",
    ),
}
EXPECTED_DATE_TYPES = frozenset({"quarter", "year"})
TOP_LEVEL_RESPONSE_FIELDS = frozenset(
    {"task_id", "status_code", "status_message", "live", "cost", "data", "result"}
)
REQUIRED_TOP_LEVEL_RESPONSE_FIELDS = frozenset(
    {"task_id", "status_code", "status_message", "cost", "data", "result"}
)
DATA_RESPONSE_FIELDS = frozenset({"symbol", "limit", "offset", "sort_by", "types"})
REQUIRED_DATA_RESPONSE_FIELDS = frozenset({"symbol", "limit", "offset", "types"})
RESULT_RESPONSE_FIELDS = frozenset(
    {
        "symbol",
        "target_price",
        "target_average_price",
        "target_low_price",
        "target_high_price",
        "total_count",
        "items_count",
        "items",
    }
)
REQUIRED_RESULT_RESPONSE_FIELDS = frozenset({"symbol", "total_count", "items_count", "items"})


class ProbeError(Exception):
    """Fail-closed probe error safe to report without credential material."""


class PartialProbeError(ProbeError):
    """Live probe failure carrying the credential-free partial call ledger."""

    def __init__(self, message: str, raw: dict[str, object]) -> None:
        super().__init__(message)
        self.raw = raw


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def format_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")


def safe_sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def validate_no_credentials(value: object, *, token: str | None = None) -> None:
    if token and token.encode() in canonical_json_bytes(value):
        raise ProbeError("provider response contained credential material")
    _validate_keys(value)


def _validate_keys(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized_key = re.sub(r"[^a-z0-9]", "", str(key).lower())
            if any(part in normalized_key for part in SENSITIVE_KEY_PARTS):
                raise ProbeError(f"credential-like field is not persistable: {key}")
            _validate_keys(item)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            _validate_keys(item)


def expected_maximum_cost(symbol_count: int) -> Decimal:
    main_cost = symbol_count * (CALL_PRICE_USD + MAIN_PAGE_LIMIT * ROW_PRICE_USD)
    pagination_cost = 2 * (CALL_PRICE_USD + PAGINATION_LIMIT * ROW_PRICE_USD)
    return main_cost + pagination_cost


def validate_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(symbol.upper() for symbol in symbols)
    if not normalized:
        raise ProbeError("at least one symbol is required")
    if len(normalized) > MAX_SYMBOLS:
        raise ProbeError("at most 20 symbols may be probed")
    if len(set(normalized)) != len(normalized):
        raise ProbeError("symbols must be unique")
    invalid = [symbol for symbol in normalized if SYMBOL_PATTERN.fullmatch(symbol) is None]
    if invalid:
        raise ProbeError("symbols must use the credential-free ticker syntax")
    return normalized


def request_bodies(symbols: Sequence[str]) -> tuple[dict[str, Any], ...]:
    sort = [{"selector": "date", "desc": True}]
    main = [
        {
            "symbol": symbol,
            "types": list(REQUESTED_TYPES),
            "limit": MAIN_PAGE_LIMIT,
            "offset": 0,
            "sort_by": sort,
        }
        for symbol in symbols
    ]
    pagination = [
        {
            "symbol": symbols[0],
            "types": ["eps_trend"],
            "limit": PAGINATION_LIMIT,
            "offset": offset,
            "sort_by": sort,
        }
        for offset in (0, 1)
    ]
    return tuple(main + pagination)


def execute_call(body: Mapping[str, object], token: str) -> dict[str, object]:
    encoded_body = canonical_json_bytes(body)
    requested_at = utc_now()
    request = urllib.request.Request(
        ENDPOINT,
        data=encoded_body,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            response_body = response.read()
            status = response.status
            headers = {
                name: response.headers[name]
                for name in ALLOWED_RESPONSE_HEADERS
                if response.headers[name] is not None
            }
    except urllib.error.HTTPError as error:
        error.read()
        raise ProbeError(f"FinImpulse returned HTTP {error.code}") from None
    except urllib.error.URLError as error:
        raise ProbeError(f"FinImpulse request failed: {type(error.reason).__name__}") from None
    retrieved_at = utc_now()
    if status != HTTP_OK:
        raise ProbeError(f"FinImpulse returned HTTP {status}")
    if token.encode() in response_body:
        raise ProbeError("provider response contained credential material")
    try:
        response_text = response_body.decode("utf-8")
        parsed_response = json.loads(response_text)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ProbeError("FinImpulse returned a non-JSON response") from None
    if not isinstance(parsed_response, dict):
        raise ProbeError("FinImpulse response must decode to an object")
    return {
        "request": {
            "body": dict(body),
            "request_body_sha256": safe_sha256(encoded_body),
            "requested_at_utc": requested_at,
        },
        "response": {
            "body_text": response_text,
            "body_sha256": safe_sha256(response_body),
            "headers": headers,
            "http_status": status,
            "retrieved_at_utc": retrieved_at,
        },
    }


def remaining_quota(call: Mapping[str, object]) -> int:
    response = call.get("response")
    headers = response.get("headers") if isinstance(response, Mapping) else None
    if not isinstance(headers, Mapping) or "x-ratelimit-remaining" not in headers:
        raise ProbeError("remaining quota header is required before the next call")
    typed_headers = cast("Mapping[str, object]", headers)
    try:
        remaining = int(str(typed_headers["x-ratelimit-remaining"]))
    except ValueError:
        raise ProbeError("remaining quota header must be a non-negative integer") from None
    if remaining < 0:
        raise ProbeError("remaining quota header must be a non-negative integer")
    return remaining


def _raise_partial(raw: dict[str, object], message: str) -> None:
    raw["probe_status"] = "PARTIAL_FAILED"
    raw["failure"] = {"message": message, "recorded_at_utc": utc_now()}
    raise PartialProbeError(message, raw)


def validate_live_progress(
    response: Mapping[str, object],
    call: Mapping[str, object],
    reported_cost: Decimal,
    *,
    has_next_call: bool,
) -> Decimal:
    updated_cost = reported_cost + provider_cost(response)
    if updated_cost > BUDGET_USD:
        raise ProbeError("provider-reported cost exceeded the $0.10 budget")
    remaining = remaining_quota(call)
    if has_next_call and remaining <= 0:
        raise ProbeError("remaining quota is exhausted before the next call")
    return updated_cost


def run_live_probe(
    symbols: Sequence[str],
    token: str,
    *,
    call_executor: Callable[[Mapping[str, object], str], dict[str, object]] = execute_call,
) -> dict[str, object]:
    planned_cost = expected_maximum_cost(len(symbols))
    if planned_cost > BUDGET_USD:
        raise ProbeError("planned maximum provider cost exceeds the $0.10 budget")
    bodies = request_bodies(symbols)
    calls: list[dict[str, object]] = []
    raw: dict[str, object] = {
        "fixture_version": 1,
        "fixture_kind": "live_provider_probe",
        "task_id": TASK_ID,
        "provider": "finimpulse",
        "endpoint": ENDPOINT,
        "generated_at_utc": utc_now(),
        "probe_status": "IN_PROGRESS",
        "probe_plan": {
            "symbols": list(symbols),
            "types": list(REQUESTED_TYPES),
            "call_count": len(bodies),
            "maximum_cost_usd": format_decimal(planned_cost),
        },
        "completed_call_count": 0,
        "calls": calls,
    }
    reported_cost = Decimal(0)
    for index, body in enumerate(bodies):
        try:
            call = call_executor(body, token)
        except ProbeError as error:
            _raise_partial(raw, str(error))
        calls.append(call)
        raw["completed_call_count"] = len(calls)
        try:
            response = validate_call_semantics(call)
            reported_cost = validate_live_progress(
                response,
                call,
                reported_cost,
                has_next_call=index < len(bodies) - 1,
            )
        except ProbeError as error:
            _raise_partial(raw, str(error))
    raw["probe_status"] = "COMPLETED"
    raw["completed_at_utc"] = utc_now()
    return raw


def parse_response_body(call: Mapping[str, object]) -> dict[str, Any]:
    response = call.get("response")
    if not isinstance(response, Mapping):
        raise ProbeError("raw fixture response must be an object")
    body_text = response.get("body_text")
    expected_sha256 = response.get("body_sha256")
    if not isinstance(body_text, str) or not isinstance(expected_sha256, str):
        raise ProbeError("raw fixture response body and SHA-256 are required")
    body_bytes = body_text.encode()
    if safe_sha256(body_bytes) != expected_sha256:
        raise ProbeError("raw fixture response SHA-256 mismatch")
    try:
        parsed = json.loads(body_text)
    except json.JSONDecodeError:
        raise ProbeError("raw fixture contains a non-JSON response") from None
    if not isinstance(parsed, dict):
        raise ProbeError("raw fixture response must decode to an object")
    return parsed


def _validate_fields(
    value: Mapping[str, object],
    *,
    allowed: frozenset[str],
    required: frozenset[str],
    label: str,
) -> None:
    fields = frozenset(value)
    unexpected = fields - allowed
    if unexpected:
        raise ProbeError(f"unexpected {label} fields: {sorted(unexpected)}")
    missing = required - fields
    if missing:
        raise ProbeError(f"missing {label} fields: {sorted(missing)}")


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_optional_number(value: object, *, label: str) -> None:
    if value is None:
        return
    if not _is_number(value) or not Decimal(str(value)).is_finite():
        raise ProbeError(f"{label} must be a finite number or null")


def _validate_item(item: Mapping[str, object], requested_types: Sequence[str]) -> None:
    record_type = item.get("type")
    if (
        not isinstance(record_type, str)
        or record_type not in requested_types
        or record_type not in EXPECTED_TYPE_FIELDS
    ):
        raise ProbeError("record type is not in the requested response types")
    expected_fields = frozenset(EXPECTED_TYPE_FIELDS[record_type])
    _validate_fields(
        item,
        allowed=expected_fields,
        required=expected_fields,
        label=f"{record_type} record",
    )
    record_date = item["date"]
    if not isinstance(record_date, str):
        raise ProbeError("record date must be an ISO date string")
    try:
        date.fromisoformat(record_date)
    except ValueError:
        raise ProbeError("record date must be an ISO date string") from None
    if item["date_type"] not in EXPECTED_DATE_TYPES:
        raise ProbeError("record date_type must be quarter or year")
    for field in expected_fields - {"type", "date", "date_type"}:
        value = item[field]
        if record_type == "eps_revisions":
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                raise ProbeError(f"{field} must be a non-negative integer or null")
        else:
            _validate_optional_number(value, label=field)


def provider_cost(response: Mapping[str, object]) -> Decimal:
    try:
        cost = Decimal(str(response.get("cost")))
    except (InvalidOperation, ValueError):
        raise ProbeError("provider response cost must be a finite non-negative cost") from None
    if not cost.is_finite() or cost < 0:
        raise ProbeError("provider response cost must be a finite non-negative cost")
    return cost


def response_items(response: Mapping[str, Any]) -> list[dict[str, Any]]:
    result = response.get("result")
    if not isinstance(result, Mapping):
        raise ProbeError("provider result must be an object")
    items = result.get("items")
    if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
        raise ProbeError("provider result.items must be an array of objects")
    return items


def _request_body(call: Mapping[str, object]) -> Mapping[str, object]:
    request = call.get("request")
    if not isinstance(request, Mapping):
        raise ProbeError("raw fixture request body must be an object")
    typed_request = cast("Mapping[str, object]", request)
    request_body = typed_request.get("body")
    if not isinstance(request_body, Mapping):
        raise ProbeError("raw fixture request body must be an object")
    return cast("Mapping[str, object]", request_body)


def _validate_http_envelope(call: Mapping[str, object]) -> None:
    response_envelope = call.get("response")
    if not isinstance(response_envelope, Mapping):
        raise ProbeError("raw fixture response must be an object")
    if response_envelope.get("http_status") != HTTP_OK:
        raise ProbeError("raw fixture HTTP status must be 200")


def _validate_response_metadata(response: Mapping[str, object]) -> None:
    _validate_fields(
        response,
        allowed=TOP_LEVEL_RESPONSE_FIELDS,
        required=REQUIRED_TOP_LEVEL_RESPONSE_FIELDS,
        label="response",
    )
    validate_no_credentials(response)
    if response["status_code"] != PROVIDER_STATUS_OK:
        raise ProbeError("provider status_code must be 20000")
    if response["status_message"] != "OK":
        raise ProbeError("provider status_message must be OK")
    if not isinstance(response["task_id"], str) or not response["task_id"]:
        raise ProbeError("provider task_id must be a non-empty string")
    if "live" in response and not isinstance(response["live"], bool):
        raise ProbeError("provider live flag must be boolean")
    provider_cost(response)


def _validate_data_response(
    response: Mapping[str, object],
    request_body: Mapping[str, object],
) -> None:
    data = response["data"]
    if not isinstance(data, Mapping):
        raise ProbeError("provider data must be an object")
    typed_data = cast("Mapping[str, object]", data)
    _validate_fields(
        typed_data,
        allowed=DATA_RESPONSE_FIELDS,
        required=REQUIRED_DATA_RESPONSE_FIELDS,
        label="data response",
    )
    if typed_data["symbol"] != request_body.get("symbol"):
        raise ProbeError("response symbol does not match request symbol")
    if typed_data["types"] != request_body.get("types"):
        raise ProbeError("response types do not match request types")
    if typed_data["limit"] != request_body.get("limit"):
        raise ProbeError("response limit does not match request limit")
    if typed_data["offset"] != request_body.get("offset"):
        raise ProbeError("response offset does not match request offset")
    if "sort_by" in typed_data and typed_data["sort_by"] != request_body.get("sort_by"):
        raise ProbeError("response sort_by does not match request sort_by")


def _validate_counts(
    result: Mapping[str, object],
    request_body: Mapping[str, object],
    items: Sequence[Mapping[str, object]],
) -> None:
    items_count = result["items_count"]
    total_count = result["total_count"]
    limit = request_body.get("limit")
    offset = request_body.get("offset")
    if not isinstance(items_count, int) or isinstance(items_count, bool):
        raise ProbeError("items_count must be an integer")
    if items_count != len(items):
        raise ProbeError("items_count does not match result.items length")
    if not isinstance(limit, int) or items_count > limit:
        raise ProbeError("items_count exceeds request limit")
    if not isinstance(total_count, int) or isinstance(total_count, bool):
        raise ProbeError("total_count must be an integer")
    if not isinstance(offset, int) or total_count < offset + items_count:
        raise ProbeError("total_count is inconsistent with offset and items_count")


def _validate_records(
    items: Sequence[Mapping[str, object]],
    requested_types: Sequence[str],
) -> None:
    identities: set[tuple[object, object, object]] = set()
    for item in items:
        _validate_item(item, requested_types)
        identity = (item["type"], item["date"], item["date_type"])
        if identity in identities:
            raise ProbeError("duplicate record identity in provider response")
        identities.add(identity)


def _validate_result_response(
    response: Mapping[str, Any],
    request_body: Mapping[str, object],
) -> None:
    result = response["result"]
    if not isinstance(result, Mapping):
        raise ProbeError("provider result must be an object")
    typed_result = cast("Mapping[str, object]", result)
    _validate_fields(
        typed_result,
        allowed=RESULT_RESPONSE_FIELDS,
        required=REQUIRED_RESULT_RESPONSE_FIELDS,
        label="result response",
    )
    if typed_result["symbol"] != request_body.get("symbol"):
        raise ProbeError("response result symbol does not match request symbol")
    for field in RESULT_RESPONSE_FIELDS - REQUIRED_RESULT_RESPONSE_FIELDS:
        if field in typed_result:
            _validate_optional_number(typed_result[field], label=field)
    items = response_items(response)
    requested_types = request_body.get("types")
    if not isinstance(requested_types, list) or not all(
        isinstance(record_type, str) for record_type in requested_types
    ):
        raise ProbeError("request types must be an array of strings")
    typed_requested_types = cast("list[str]", requested_types)
    _validate_counts(typed_result, request_body, items)
    _validate_records(items, typed_requested_types)


def validate_call_semantics(call: Mapping[str, object]) -> dict[str, Any]:
    request_body = _request_body(call)
    _validate_http_envelope(call)
    response = parse_response_body(call)
    _validate_response_metadata(response)
    _validate_data_response(response, request_body)
    _validate_result_response(response, request_body)
    return response


def validate_call_set(raw: Mapping[str, Any]) -> tuple[str, ...]:
    calls = raw.get("calls")
    if not isinstance(calls, list) or not calls:
        raise ProbeError("raw fixture must contain calls")
    main_symbols: list[str] = []
    actual_bodies: list[dict[str, Any]] = []
    for call in calls:
        if not isinstance(call, Mapping):
            raise ProbeError("raw fixture call must be an object")
        request = call.get("request")
        body = request.get("body") if isinstance(request, Mapping) else None
        if not isinstance(body, dict):
            raise ProbeError("raw fixture request body must be an object")
        actual_bodies.append(body)
        if (
            body.get("types") == list(REQUESTED_TYPES)
            and body.get("limit") == MAIN_PAGE_LIMIT
            and body.get("offset") == 0
        ):
            symbol = body.get("symbol")
            if not isinstance(symbol, str):
                raise ProbeError("main request symbol must be a string")
            main_symbols.append(symbol)
    symbols = validate_symbols(main_symbols)
    plan = raw.get("probe_plan")
    if isinstance(plan, Mapping) and plan.get("symbols") != list(symbols):
        raise ProbeError("raw fixture call set does not match probe_plan symbols")
    if actual_bodies != list(request_bodies(symbols)):
        raise ProbeError("raw fixture call set does not match the bounded probe plan")
    return symbols


def json_type(value: object) -> str:
    if value is None:
        json_value_type = "null"
    elif isinstance(value, bool):
        json_value_type = "boolean"
    elif isinstance(value, int):
        json_value_type = "integer"
    elif isinstance(value, float):
        json_value_type = "number"
    elif isinstance(value, str):
        json_value_type = "string"
    elif isinstance(value, list):
        json_value_type = "array"
    elif isinstance(value, dict):
        json_value_type = "object"
    else:
        json_value_type = type(value).__name__
    return json_value_type


def main_calls(raw: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    calls = raw.get("calls")
    if not isinstance(calls, list):
        raise ProbeError("raw fixture calls must be an array")
    selected: list[Mapping[str, Any]] = []
    for call in calls:
        if not isinstance(call, Mapping):
            raise ProbeError("raw fixture call must be an object")
        request = call.get("request")
        body = request.get("body") if isinstance(request, Mapping) else None
        if not isinstance(body, Mapping):
            raise ProbeError("raw fixture request body must be an object")
        if body.get("types") == list(REQUESTED_TYPES) and body.get("offset") == 0:
            selected.append(call)
    return selected


def build_schema_and_missingness(
    calls: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    observed_types: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    missing_fields: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    row_counts: dict[str, int] = defaultdict(int)
    for call in calls:
        for item in response_items(parse_response_body(call)):
            record_type = item.get("type")
            if record_type not in EXPECTED_TYPE_FIELDS:
                continue
            row_counts[record_type] += 1
            for field in EXPECTED_TYPE_FIELDS[record_type]:
                value = item.get(field)
                observed_types[record_type][field].add(json_type(value))
                if field not in item or value is None:
                    missing_fields[record_type][field] += 1
    schema = {
        record_type: {
            "documented_fields": list(fields),
            "observed_value_types": {
                field: sorted(observed_types[record_type][field]) for field in fields
            },
            "row_count": row_counts[record_type],
        }
        for record_type, fields in EXPECTED_TYPE_FIELDS.items()
    }
    missingness = {
        record_type: {
            "field_missing_or_null_counts": {
                field: missing_fields[record_type][field] for field in fields
            },
            "row_count": row_counts[record_type],
        }
        for record_type, fields in EXPECTED_TYPE_FIELDS.items()
    }
    return schema, missingness


def build_coverage(calls: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_symbol: dict[str, dict[str, int]] = {}
    for call in calls:
        request = call["request"]
        body = request["body"]
        symbol = str(body["symbol"])
        counts = dict.fromkeys(REQUESTED_TYPES, 0)
        for item in response_items(parse_response_body(call)):
            record_type = item.get("type")
            if record_type in counts:
                counts[record_type] += 1
        by_symbol[symbol] = counts
    requested_cells = len(by_symbol) * len(REQUESTED_TYPES)
    missing_cells = sum(count == 0 for counts in by_symbol.values() for count in counts.values())
    rate = Decimal(missing_cells) / Decimal(requested_cells) if requested_cells else Decimal(1)
    return {
        "observed_symbol_count": len(by_symbol),
        "requested_type_symbol_cells": requested_cells,
        "missing_type_symbol_cells": missing_cells,
        "missing_type_symbol_rate": format_decimal(rate),
        "records_by_symbol": by_symbol,
    }


def build_pagination(raw: Mapping[str, Any]) -> dict[str, Any]:
    pages: dict[int, tuple[int | None, list[dict[str, Any]]]] = {}
    selected_main_calls = main_calls(raw)
    pagination_symbol = selected_main_calls[0]["request"]["body"]["symbol"]
    for call in raw["calls"]:
        body = call["request"]["body"]
        if (
            body.get("symbol") == pagination_symbol
            and body.get("types") == ["eps_trend"]
            and body.get("limit") == PAGINATION_LIMIT
        ):
            response = parse_response_body(call)
            result = response.get("result")
            total = result.get("total_count") if isinstance(result, Mapping) else None
            pages[int(body["offset"])] = (total, response_items(response))
    if set(pages) != {0, 1}:
        return {"status": "NOT_OBSERVED", "reason": "two pagination pages were not captured"}
    page_zero = pages[0]
    page_one = pages[1]
    distinct = bool(page_zero[1] and page_one[1] and page_zero[1][0] != page_one[1][0])
    totals_match = page_zero[0] == page_one[0]
    return {
        "status": "PASS" if distinct and totals_match else "WARN",
        "limit": PAGINATION_LIMIT,
        "offsets": [0, 1],
        "page_items_distinct": distinct,
        "total_count_consistent": totals_match,
        "reported_total_count": page_zero[0],
    }


def build_rate_limit(raw: Mapping[str, Any]) -> dict[str, Any]:
    limits: list[int] = []
    remaining: list[int] = []
    for call in raw["calls"]:
        headers = call["response"].get("headers", {})
        if "x-ratelimit-limit" in headers:
            limits.append(int(headers["x-ratelimit-limit"]))
        if "x-ratelimit-remaining" in headers:
            remaining.append(int(headers["x-ratelimit-remaining"]))
    return {
        "status": "OBSERVED" if limits and remaining else "NOT_OBSERVED",
        "observed_limit": min(limits) if limits else None,
        "minimum_remaining": min(remaining) if remaining else None,
        "window": {"status": "NOT_OBSERVED", "seconds": None},
        "exhaustion_tested": False,
    }


def build_provider_mode(calls: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    live_values = [parse_response_body(call).get("live") for call in calls]
    observed_live_values = sorted({value for value in live_values if isinstance(value, bool)})
    all_responses_live = len(live_values) == len(calls) and observed_live_values == [True]
    sandbox_detected = False in observed_live_values
    if all_responses_live:
        status = "LIVE"
    elif sandbox_detected:
        status = "SANDBOX"
    else:
        status = "NOT_PROVEN"
    return {
        "status": status,
        "all_responses_live": all_responses_live,
        "sandbox_detected": sandbox_detected,
        "observed_live_values": observed_live_values,
    }


def validate_live_completion(raw: Mapping[str, Any]) -> None:
    if raw.get("fixture_kind") != "live_provider_probe":
        return
    if raw.get("probe_status") != "COMPLETED":
        raise ProbeError("live probe_status must be COMPLETED before receipt generation")
    if "failure" in raw:
        raise ProbeError("completed live probe must not contain a failure field")
    calls = raw.get("calls")
    plan = raw.get("probe_plan")
    completed_call_count = raw.get("completed_call_count")
    if not isinstance(calls, list) or not isinstance(plan, Mapping):
        raise ProbeError("live completed_call_count, probe_plan.call_count, and calls must match")
    plan_call_count = plan.get("call_count")
    if completed_call_count != plan_call_count or completed_call_count != len(calls):
        raise ProbeError("live completed_call_count, probe_plan.call_count, and calls must match")


def build_receipt(raw: Mapping[str, Any], raw_bytes: bytes) -> dict[str, Any]:
    validate_no_credentials(raw)
    if raw.get("task_id") != TASK_ID or raw.get("provider") != "finimpulse":
        raise ProbeError("raw fixture identity does not match AAS-DATA-008A")
    validate_live_completion(raw)
    calls = raw.get("calls")
    if not isinstance(calls, list) or not calls:
        raise ProbeError("raw fixture must contain calls")
    validate_call_set(raw)
    for call in calls:
        validate_call_semantics(call)
    selected_main_calls = main_calls(raw)
    coverage = build_coverage(selected_main_calls)
    schema, missingness = build_schema_and_missingness(selected_main_calls)
    costs = [provider_cost(parse_response_body(call)) for call in calls]
    total_cost = sum(costs, Decimal(0))
    if total_cost > BUDGET_USD:
        raise ProbeError("completed live probe cost exceeds the $0.10 budget")
    retrieved_times = [str(call["response"]["retrieved_at_utc"]) for call in calls]
    raw_sha256 = safe_sha256(raw_bytes)
    return {
        "receipt_version": 1,
        "task_id": TASK_ID,
        "provider": "finimpulse",
        "dataset": "analysis/earnings:eps_trend+eps_revisions",
        "probe_status": "PROBE_ONLY_NOT_PROMOTED",
        "generated_at_utc": max(retrieved_times),
        "endpoint": ENDPOINT,
        "scope": {
            "max_symbols": MAX_SYMBOLS,
            "symbols": sorted(coverage["records_by_symbol"]),
            "types": list(REQUESTED_TYPES),
            "budget_usd": format_decimal(BUDGET_USD),
            "main_page_limit": MAIN_PAGE_LIMIT,
            "call_count": len(calls),
        },
        "credential_handling": {
            "source_name": f"environment:{CREDENTIAL_ENVIRONMENT_VARIABLE}",
            "artifact_schema_validation": "PASS",
            "external_persistence": "NOT_OBSERVED",
            "console_disclosure": "NOT_OBSERVED",
        },
        "raw_fixture": {
            "logical_id": f"sha256:{raw_sha256}",
            "byte_length": len(raw_bytes),
            "kind": raw.get("fixture_kind"),
        },
        "schema": schema,
        "provider_mode": build_provider_mode(calls),
        "coverage": coverage,
        "missingness": missingness,
        "pagination": build_pagination(raw),
        "rate_limit": build_rate_limit(raw),
        "cost": {
            "pricing": {
                "call_usd": format_decimal(CALL_PRICE_USD),
                "row_usd": format_decimal(ROW_PRICE_USD),
                "source": "https://finimpulse.com/api/pricing/",
            },
            "provider_reported_total_usd": format_decimal(total_cost),
            "budget_usd": format_decimal(BUDGET_USD),
            "within_budget": True,
        },
        "pit_assessment": {
            "status": "NOT_PROVEN",
            "observed_item_temporal_fields": ["date", "date_type"],
            "missing_vendor_temporal_fields": [
                "observed_at",
                "published_at",
                "available_at",
                "vintage_start",
                "vintage_end",
            ],
            "reason": (
                "The response identifies forecast periods but has no vendor observation, "
                "publication, availability, or vintage timestamp."
            ),
            "backtest_eligible": False,
        },
        "promotion_gate": {
            "status": "BLOCKED",
            "symbol_coverage_400_to_500": False,
            "stable_identifier_mapping": False,
            "missingness_measured": True,
            "refresh_cadence_proven": False,
            "agreement_diagnostic_completed": False,
            "observation_timestamp_proven": False,
            "cost_measured": True,
            "license_retention_derived_use_approved": False,
            "immutable_snapshot_captured": True,
        },
        "license_retention_evidence": {
            "status": "NOT_VERIFIED",
            "public_documentation_as_of": "2026-07-29",
            "raw_git_retention_allowed": False,
            "history_purge_required": True,
            "reason": (
                "Public API documentation and pricing pages did not establish raw retention, "
                "redistribution, or derived-use rights."
            ),
            "sources": [
                "https://developers.finimpulse.com/",
                "https://finimpulse.com/api/pricing/",
                "https://finimpulse.com/faqs/",
            ],
        },
        "eligibility": {
            "canonical": False,
            "backtest": False,
            "paper": False,
            "order": False,
        },
    }


def read_raw_fixture(path: Path) -> tuple[dict[str, Any], bytes]:
    raw_bytes = path.read_bytes()
    try:
        raw = json.loads(raw_bytes)
    except json.JSONDecodeError:
        raise ProbeError("raw fixture is not valid JSON") from None
    if not isinstance(raw, dict):
        raise ProbeError("raw fixture must be a JSON object")
    return raw, raw_bytes


def write_exclusive_json(path: Path, value: object) -> bytes:
    content = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ).encode()
    content += b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise ProbeError(f"refusing to overwrite existing output: {path}")
    descriptor, temporary_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as temporary_file:
            temporary_file.write(content)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError:
            raise ProbeError(f"refusing to overwrite existing output: {path}") from None
    finally:
        temporary_path.unlink(missing_ok=True)
    return content


def validate_live_raw_output(path: Path) -> None:
    if path.resolve().is_relative_to(REPOSITORY_ROOT):
        raise ProbeError("live raw output must be outside the Git repository")


def parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the bounded AAS-DATA-008A probe")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--symbols", nargs="+", default=list(DEFAULT_SYMBOLS))
    parser.add_argument("--raw-output", type=Path)
    parser.add_argument("--offline-raw", type=Path)
    parser.add_argument("--receipt-output", required=True, type=Path)
    return parser.parse_args(arguments)


def execute_probe(
    args: argparse.Namespace,
    *,
    call_executor: Callable[[Mapping[str, object], str], dict[str, object]] = execute_call,
) -> dict[str, Any]:
    if args.offline_raw is not None:
        if args.live:
            raise ProbeError("--live cannot be combined with --offline-raw")
        if args.raw_output is not None:
            raise ProbeError("--raw-output cannot be combined with --offline-raw")
        raw, raw_bytes = read_raw_fixture(args.offline_raw)
        receipt = build_receipt(raw, raw_bytes)
    else:
        if not args.live:
            raise ProbeError("--live is required for a live provider probe")
        symbols = validate_symbols(args.symbols)
        if args.raw_output is None:
            raise ProbeError("--raw-output is required for a live probe")
        validate_live_raw_output(args.raw_output)
        token = os.environ.get(CREDENTIAL_ENVIRONMENT_VARIABLE)
        if not token:
            raise ProbeError(f"{CREDENTIAL_ENVIRONMENT_VARIABLE} is required for a live probe")
        try:
            raw = run_live_probe(symbols, token, call_executor=call_executor)
        except PartialProbeError as error:
            validate_no_credentials(error.raw, token=token)
            write_exclusive_json(args.raw_output, error.raw)
            raise ProbeError(f"{error}; partial call ledger written") from None
        validate_no_credentials(raw, token=token)
        raw_bytes = write_exclusive_json(args.raw_output, raw)
        receipt = build_receipt(raw, raw_bytes)
    write_exclusive_json(args.receipt_output, receipt)
    return receipt


def main(arguments: Sequence[str] | None = None) -> int:
    args = parse_args(arguments)
    try:
        receipt = execute_probe(args)
    except (OSError, ProbeError, ValueError) as error:
        sys.stderr.write(f"error: {error}\n")
        return 2
    safe_summary = {
        "probe_status": receipt["probe_status"],
        "receipt": str(args.receipt_output),
        "provider_reported_total_usd": receipt["cost"]["provider_reported_total_usd"],
    }
    sys.stdout.write(f"{json.dumps(safe_summary, sort_keys=True)}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
