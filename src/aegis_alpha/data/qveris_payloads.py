"""Endpoint completeness checks, separate from market coverage and admission."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from http import HTTPStatus

from aegis_alpha.data.qveris_contracts import (
    EOD_HISTORY_JSON_TOOL,
    EOD_HISTORY_TOOL,
    EOD_TOOL,
    EOD_UNIVERSE_TOOL,
    FRED_TOOL,
    QverisJob,
    object_value,
)


@dataclass(frozen=True, slots=True)
class PageShape:
    rows: int
    keys: tuple[str, ...]
    next_offset: int | None = None
    total: int | None = None


def _eod(job: QverisJob, data: object) -> PageShape:
    if not isinstance(data, list):
        raise TypeError("EODHD bulk response must be an array")
    required = {
        "prices": {
            "code",
            "exchange_short_name",
            "date",
            "open",
            "high",
            "low",
            "close",
            "adjusted_close",
            "volume",
        },
        "splits": {"code", "exchange", "date", "split"},
        "dividends": {"code", "exchange", "date", "dividend", "currency"},
    }[job.dataset]
    target_date = job.parameters["date"]
    target_exchange = job.parameters["exchange"]
    keys = []
    for value in data:
        row = object_value(value)
        if not required <= row.keys():
            raise ValueError("EODHD bulk row is missing required fields")
        if row["date"] != target_date:
            raise ValueError("EODHD bulk returned a different date")
        code = row["code"]
        exchange = row.get("exchange_short_name", row.get("exchange"))
        if not isinstance(code, str) or not code or exchange != target_exchange:
            raise ValueError("EODHD row has an invalid symbol/exchange")
        keys.append(code)
    if len(set(keys)) != len(keys):
        raise ValueError("EODHD bulk contains duplicate symbols")
    if job.dataset == "prices" and not keys:
        raise ValueError("empty prices do not prove a trading-day snapshot")
    return PageShape(len(data), tuple(keys))


def _eod_history(job: QverisJob, data: object, retrieved_on: date | None) -> PageShape:
    if not isinstance(data, list) or not data:
        raise ValueError("EODHD history requires a nonempty array")
    params = job.parameters
    if "to" in params:
        upper = str(params["to"])
    elif retrieved_on is None:
        raise ValueError("open-ended history requires its actual retrieval date")
    else:
        upper = str(min(job.observation_date, retrieved_on))
    required = {"date", "open", "high", "low", "close", "adjusted_close", "volume"}
    keys = []
    for value in data:
        row = object_value(value)
        if not required <= row.keys():
            raise ValueError("EODHD history row is missing fields")
        observed = row["date"]
        if not isinstance(observed, str) or date.fromisoformat(observed).isoformat() != observed:
            raise ValueError("invalid EODHD history date")
        if not str(params["from"]) <= observed <= upper:
            raise ValueError("EODHD history returned dates outside request")
        if "symbol" in row and row["symbol"] != params["symbol"]:
            raise ValueError("EODHD history returned a different symbol")
        keys.append(observed)
    if keys != sorted(keys) or len(set(keys)) != len(keys):
        raise ValueError("EODHD history has unordered or duplicate dates")
    return PageShape(len(data), tuple(keys))


def _eod_universe(job: QverisJob, data: object) -> PageShape:
    if not isinstance(data, list):
        raise TypeError("EODHD universe must be an array")
    params = job.parameters
    if not data and params["delisted"] == "0":
        raise ValueError("active universe cannot be empty")
    keys = []
    for value in data:
        row = object_value(value)
        if not {"Code", "Name", "Exchange", "Currency", "Type"} <= row.keys():
            raise ValueError("EODHD universe row is missing fields")
        code = row["Code"]
        if not isinstance(code, str) or not code:
            raise ValueError("EODHD universe requires a symbol")
        if job.market in {"KR", "INDEX"} and row["Exchange"] != params["EXCHANGE_CODE"]:
            raise ValueError("EODHD universe returned a different requested exchange")
        keys.append(f"{row['Exchange']}/{code}")
    if len(set(keys)) != len(keys):
        raise ValueError("EODHD universe has duplicate symbols")
    return PageShape(len(data), tuple(keys))


def _fred(  # noqa: C901 -- pagination and row vintage acceptance guards
    parameters: dict[str, object], data: object
) -> PageShape:
    document = object_value(data)
    count, offset, limit = (document.get(key) for key in ("count", "offset", "limit"))
    if type(count) is not int or type(offset) is not int or type(limit) is not int:
        raise ValueError("FRED page must declare integer count/offset/limit")
    if count < 0 or offset != parameters["offset"] or limit != parameters["limit"]:
        raise ValueError("FRED page differs from requested pagination")
    if (
        document.get("realtime_start") != parameters["realtime_start"]
        or document.get("realtime_end") != parameters["realtime_end"]
    ):
        raise ValueError("FRED returned a different vintage window")
    rows = document.get("observations")
    if not isinstance(rows, list) or len(rows) != min(limit, count - offset):
        raise ValueError("FRED page is truncated, short, or oversized")
    keys = []
    for value in rows:
        row = object_value(value)
        if not {"date", "value", "realtime_start", "realtime_end"} <= row.keys():
            raise ValueError("FRED observation is incomplete")
        observed = row["date"]
        if not isinstance(observed, str):
            raise TypeError("FRED observation date must be text")
        date.fromisoformat(observed)
        first, last = row["realtime_start"], row["realtime_end"]
        if not isinstance(first, str) or not isinstance(last, str):
            raise TypeError("FRED vintage dates must be text")
        date.fromisoformat(first)
        date.fromisoformat(last)
        if (
            first > last
            or first > str(parameters["realtime_end"])
            or last < str(parameters["realtime_start"])
        ):
            raise ValueError("FRED observation is outside the requested vintage")
        if (
            not str(parameters["observation_start"])
            <= observed
            <= str(parameters["observation_end"])
        ):
            raise ValueError("FRED observation is outside the requested window")
        keys.append(f"{observed}/{row['realtime_start']}/{row['realtime_end']}")
    if len(set(keys)) != len(keys) or keys != sorted(keys):
        raise ValueError("FRED page has duplicate or unordered observations")
    next_offset = offset + len(rows)
    return PageShape(len(rows), tuple(keys), next_offset if next_offset < count else None, count)


def validate_payload(  # noqa: C901 -- reviewed endpoint variants
    job: QverisJob,
    parameters: dict[str, object],
    response: dict[str, object],
    *,
    retrieved_on: date | None = None,
) -> PageShape:
    if response.get("success") is not True or not isinstance(response.get("execution_id"), str):
        raise ValueError("Qveris execute did not succeed with an execution ID")
    result = object_value(response.get("result"))
    if result.get("status_code") != HTTPStatus.OK:
        raise ValueError("upstream returned an unsuccessful status")
    if any(
        result.get(key)
        for key in (
            "truncated_content",
            "full_content_file_url",
            "truncated",
            "next_page",
            "next_cursor",
        )
    ):
        raise ValueError("Qveris result is truncated or needs an unsupported continuation")
    data = result.get("data")
    if job.tool_id == EOD_TOOL:
        return _eod(job, data)
    if job.tool_id == FRED_TOOL:
        return _fred(parameters, data)
    if job.tool_id in {EOD_HISTORY_TOOL, EOD_HISTORY_JSON_TOOL}:
        return _eod_history(job, data, retrieved_on)
    if job.tool_id == EOD_UNIVERSE_TOOL:
        return _eod_universe(job, data)
    facts = object_value(data)
    if str(facts.get("cik")).zfill(10) != parameters["cik"]:
        raise ValueError("SEC companyfacts returned a different CIK")
    namespaces = object_value(facts.get("facts"))
    count = 0
    for namespace in namespaces.values():
        for concept in object_value(namespace).values():
            units = object_value(object_value(concept).get("units"))
            for values in units.values():
                if not isinstance(values, list):
                    raise TypeError("SEC fact units must contain arrays")
                count += len(values)
    return PageShape(count, ())
