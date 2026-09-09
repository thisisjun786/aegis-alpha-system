"""Explicit, immutable jobs for the reviewed Qveris acquisition surface."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import cast

from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256

EOD_TOOL = "eodhd.eod_bulk_last_day.retrieve.v1.5c37e49e"
EOD_HISTORY_TOOL = "eodhd.eod.retrieve.v1.7b3edfe5"
EOD_HISTORY_JSON_TOOL = "eodhd.eod.eod.retrieve.v1.f22ec958"
EOD_UNIVERSE_TOOL = "eodhd.exchange_symbols.list.v1.f4fcf2d1"
SEC_TOOL = "sec.company.facts.v1"
FRED_TOOL = "stlouisfed_fred.fred_series_observations.get.v1"
MAX_RESPONSE_BYTES = 128 * 1024 * 1024
MAX_PAGES = 100
MAX_FRED_PAGE = 100_000
MIN_RESPONSE_BYTES = 1024
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,149}")


def object_value(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(k, str) for k in value):
        raise ValueError("expected a JSON object")
    return cast("dict[str, object]", value)


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _constant(_value: str) -> None:
    raise ValueError("non-finite JSON value")


def load_json(payload: str | bytes) -> object:
    return json.loads(payload, object_pairs_hook=_pairs, parse_constant=_constant)


def credit_value(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise TypeError("credit amount is missing or invalid")
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise ValueError("credit amount is invalid") from None
    if not result.is_finite() or result < 0:
        raise ValueError("credit amount must be finite and nonnegative")
    return result


def _date(value: object) -> date:
    if not isinstance(value, str):
        raise TypeError("date must be an ISO date string")
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("date must use YYYY-MM-DD")
    return parsed


def _exact_keys(parameters: dict[str, object], required: set[str], optional: set[str]) -> None:
    if not required <= parameters.keys() or parameters.keys() - required - optional:
        raise ValueError("request has missing or unreviewed parameters")


def _validate_eod(job: QverisJob, params: dict[str, object]) -> None:
    _exact_keys(params, {"exchange", "date", "fmt"}, {"type"})
    _validate_exchange(job.market, params["exchange"])
    if params["fmt"] != "json":
        raise ValueError("EODHD bulk requires JSON")
    expected = {None: "prices", "splits": "splits", "dividends": "dividends"}
    kind = params.get("type")
    if kind is not None and not isinstance(kind, str):
        raise ValueError("invalid bulk type")
    if kind not in expected or job.dataset != expected[kind]:
        raise ValueError("bulk dataset differs from request")
    if _date(params["date"]) > job.observation_date:
        raise ValueError("cannot collect a future price date")


def _validate_exchange(market: str, exchange: object) -> None:
    allowed = {"US": {"US"}, "KR": {"KO", "KQ"}}
    if market not in allowed or not isinstance(exchange, str) or exchange not in allowed[market]:
        raise ValueError("EODHD exchange does not match the reviewed market")


def _validate_history(job: QverisJob, params: dict[str, object]) -> None:
    if job.tool_id == EOD_HISTORY_JSON_TOOL:
        _exact_keys(params, {"symbol", "order", "from", "fmt"}, set())
        if params["fmt"] != "json":
            raise ValueError("the JSON history route requires fmt=json")
    else:
        _exact_keys(params, {"symbol", "period", "order", "from", "to"}, set())
    symbol = params["symbol"]
    if not isinstance(symbol, str) or _NAME.fullmatch(symbol) is None:
        raise ValueError("invalid EODHD history symbol")
    code, separator, exchange = symbol.rpartition(".")
    if job.market in {"INDEX", "CRYPTO"}:
        expected_exchange = "INDX" if job.market == "INDEX" else "CC"
        if job.tool_id != EOD_HISTORY_JSON_TOOL or exchange != expected_exchange:
            raise ValueError("research history requires its reviewed JSON market route")
        expected_dataset = "research_price_history"
    else:
        _validate_exchange(job.market, exchange)
        expected_dataset = "price_history"
    if (
        not separator
        or not code
        or (job.market == "KR" and re.fullmatch(r"[0-9A-Z]{6}", code) is None)
    ):
        raise ValueError("invalid market symbol")
    if (
        job.dataset != expected_dataset
        or params.get("period", "d") != "d"
        or params["order"] != "a"
    ):
        raise ValueError("EODHD history requires daily ascending prices")
    if (
        not _date(params["from"])
        <= _date(params.get("to", job.observation_date.isoformat()))
        <= job.observation_date
    ):
        raise ValueError("invalid EODHD history window")


def _validate_universe(job: QverisJob, params: dict[str, object]) -> None:
    _exact_keys(params, {"EXCHANGE_CODE", "delisted", "fmt"}, set())
    if job.market == "INDEX" and job.dataset == "research_universe":
        if params != {"EXCHANGE_CODE": "INDX", "delisted": "0", "fmt": "json"}:
            raise ValueError("research index universe requires active INDX JSON exchange")
        return
    _validate_exchange(job.market, params["EXCHANGE_CODE"])
    if job.dataset != "universe" or params["delisted"] not in {"0", "1"} or params["fmt"] != "json":
        raise ValueError("invalid EODHD universe request")


def _validate_sec(job: QverisJob, params: dict[str, object]) -> None:
    _exact_keys(params, {"cik"}, set())
    if job.market != "US" or job.dataset != "companyfacts":
        raise ValueError("unsupported SEC job domain")
    if not isinstance(params["cik"], str) or re.fullmatch(r"[0-9]{10}", params["cik"]) is None:
        raise ValueError("SEC facts require a ten-digit CIK")


def _validate_fred(job: QverisJob, params: dict[str, object]) -> None:
    required = {
        "series_id",
        "file_type",
        "observation_start",
        "observation_end",
        "realtime_start",
        "realtime_end",
        "limit",
        "offset",
    }
    _exact_keys(params, required, {"sort_order", "units"})
    if job.market not in {"US", "KR"} or job.dataset != "macro":
        raise ValueError("unsupported FRED job domain")
    series = params["series_id"]
    if not isinstance(series, str) or _NAME.fullmatch(series) is None:
        raise ValueError("invalid FRED series ID")
    if params["file_type"] != "json" or params.get("sort_order", "asc") != "asc":
        raise ValueError("FRED acquisition requires ascending JSON")
    if params.get("units", "lin") != "lin":
        raise ValueError("only untransformed FRED observations are supported")
    if type(params["offset"]) is not int or params["offset"] != 0:
        raise ValueError("initial FRED offset must be zero")
    limit = params["limit"]
    if type(limit) is not int or not 1 <= limit <= MAX_FRED_PAGE:
        raise ValueError("invalid FRED page limit")
    if (
        not _date(params["observation_start"])
        <= _date(params["observation_end"])
        <= job.observation_date
    ):
        raise ValueError("invalid observation window")
    if not _date(params["realtime_start"]) <= _date(params["realtime_end"]) <= job.observation_date:
        raise ValueError("invalid vintage window")


@dataclass(frozen=True, slots=True)
class QverisJob:
    job_id: str
    tool_id: str
    upstream: str
    market: str
    dataset: str
    parameters_json: str
    observation_date: date
    max_response_bytes: int = 128 * 1024 * 1024
    cache_mode: str = "default"

    def __post_init__(self) -> None:
        if not isinstance(self.job_id, str) or _NAME.fullmatch(self.job_id) is None:
            raise ValueError("invalid job ID")
        if type(self.observation_date) is not date:
            raise ValueError("observation_date must be a date")
        if (
            type(self.max_response_bytes) is not int
            or not MIN_RESPONSE_BYTES <= self.max_response_bytes <= MAX_RESPONSE_BYTES
        ):
            raise ValueError("invalid response byte limit")
        if self.cache_mode != "default":
            raise ValueError("unreviewed Qveris cache mode")
        params = self.parameters
        if canonical_json_bytes(params).decode() != self.parameters_json:
            raise ValueError("parameters_json must use canonical serialization")
        upstreams = {
            EOD_TOOL: "eodhd",
            EOD_HISTORY_TOOL: "eodhd",
            EOD_HISTORY_JSON_TOOL: "eodhd",
            EOD_UNIVERSE_TOOL: "eodhd",
            SEC_TOOL: "sec-gov",
            FRED_TOOL: "stlouisfed_fred",
        }
        if self.tool_id not in upstreams or self.upstream != upstreams[self.tool_id]:
            raise ValueError("unreviewed tool/upstream pair")
        validators = {
            EOD_TOOL: _validate_eod,
            EOD_HISTORY_TOOL: _validate_history,
            EOD_HISTORY_JSON_TOOL: _validate_history,
            EOD_UNIVERSE_TOOL: _validate_universe,
            SEC_TOOL: _validate_sec,
            FRED_TOOL: _validate_fred,
        }
        validators[self.tool_id](self, params)

    @property
    def parameters(self) -> dict[str, object]:
        return object_value(load_json(self.parameters_json))

    @property
    def fingerprint(self) -> str:
        value = self.document()
        del value["job_id"]
        del value["max_response_bytes"]
        return content_sha256(value)

    def document(self) -> dict[str, object]:
        return object_value(load_json(canonical_json_bytes(self)))

    @classmethod
    def from_document(cls, value: object) -> QverisJob:
        document = object_value(value)
        fields = {
            "job_id",
            "tool_id",
            "upstream",
            "market",
            "dataset",
            "parameters_json",
            "observation_date",
            "max_response_bytes",
            "cache_mode",
        }
        if set(document) != fields:
            raise ValueError("invalid Qveris job fields")
        strings = (
            "job_id",
            "tool_id",
            "upstream",
            "market",
            "dataset",
            "parameters_json",
            "cache_mode",
        )
        if any(not isinstance(document[k], str) for k in strings):
            raise ValueError("invalid Qveris job field type")
        return cls(
            job_id=cast("str", document["job_id"]),
            tool_id=cast("str", document["tool_id"]),
            upstream=cast("str", document["upstream"]),
            market=cast("str", document["market"]),
            dataset=cast("str", document["dataset"]),
            parameters_json=cast("str", document["parameters_json"]),
            observation_date=_date(document["observation_date"]),
            max_response_bytes=cast("int", document["max_response_bytes"]),
            cache_mode=cast("str", document["cache_mode"]),
        )


def load_jobs(payload: bytes) -> tuple[QverisJob, ...]:
    document = object_value(load_json(payload))
    if (
        set(document) != {"schema_version", "jobs"}
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
    ):
        raise ValueError("unsupported jobs document")
    values = document["jobs"]
    if not isinstance(values, list) or not values:
        raise ValueError("jobs must be a nonempty array")
    jobs = tuple(QverisJob.from_document(value) for value in values)
    if len({job.job_id for job in jobs}) != len(jobs) or len(
        {job.fingerprint for job in jobs}
    ) != len(jobs):
        raise ValueError("duplicate job identity")
    return jobs
