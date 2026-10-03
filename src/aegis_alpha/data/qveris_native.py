"""Verified Qveris source rows; no credentials, HTTP, or eligibility promotion."""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from http import HTTPStatus
from pathlib import Path
from types import MappingProxyType

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.qveris_contracts import (
    ACTION_DATASETS,
    EOD_HISTORY_JSON_TOOL,
    EOD_TOOL,
    FX_DATASET,
    FX_MARKET,
    MAX_RESPONSE_BYTES,
    QverisJob,
    credit_value,
    load_json,
    object_value,
)
from aegis_alpha.data.qveris_payloads import validate_payload

_SHA = re.compile(r"[0-9a-f]{64}")
_RATIO = re.compile(r"(?P<new>[0-9]+(?:\.[0-9]+)?)/(?P<old>[0-9]+(?:\.[0-9]+)?)")
_PAGE_FILES = ("intent.json", "raw", "response.json", "billing.json")
SUPPORTED_COMPLETIONS = frozenset(
    {
        ("KR", "price_history", EOD_HISTORY_JSON_TOOL),
        ("US", "price_history", EOD_HISTORY_JSON_TOOL),
        ("INDEX", "research_price_history", EOD_HISTORY_JSON_TOOL),
        ("CRYPTO", "research_price_history", EOD_HISTORY_JSON_TOOL),
        (FX_MARKET, FX_DATASET, EOD_HISTORY_JSON_TOOL),
        ("KR", "prices", EOD_TOOL),
        ("US", "prices", EOD_TOOL),
        *((market, dataset, EOD_TOOL) for market in ("KR", "US") for dataset in ACTION_DATASETS),
    }
)


@dataclass(frozen=True, slots=True)
class CompletedHistory:
    job: QverisJob
    completion_sha256: str
    raw_sha256: str
    retrieved_at: datetime
    rows: tuple[Mapping[str, object], ...]
    provider_warning: bool = False
    # ``complete.json`` followed by the four page files it pins, as read and verified.
    evidence: tuple[bytes, ...] = ()


@dataclass(frozen=True, slots=True)
class NormalizedHistory:
    rows: tuple[Mapping[str, object], ...]
    quarantine: tuple[Mapping[str, object], ...]
    source_only: bool = True
    point_in_time_certified: bool = False
    next_open_eligible: bool = False


def _read(tree: DescriptorTree, path: str, maximum: int) -> bytes:
    before = tree.stat(path)
    body = tree.read_bytes(path, max_bytes=maximum)
    after = tree.stat(path)
    if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ):
        raise ValueError("Qveris source changed while reading")
    return body


def read_completed_job(  # noqa: C901, PLR0912, PLR0915 -- one complete evidence chain
    root: Path, fingerprint: str, expected_completion_sha256: str
) -> CompletedHistory:
    if _SHA.fullmatch(fingerprint) is None or _SHA.fullmatch(expected_completion_sha256) is None:
        raise ValueError("Qveris source pins must be SHA-256")
    prefix = f"jobs/{fingerprint}/"
    with DescriptorTree.open_path(root) as tree:
        body = _read(tree, prefix + "complete.json", 1024 * 1024)
        if hashlib.sha256(body).hexdigest() != expected_completion_sha256:
            raise ValueError("Qveris completion hash differs")
        marker = object_value(load_json(body))
        job = QverisJob.from_document(marker.get("job"))
        if (
            job.fingerprint != fingerprint
            or marker.get("fingerprint") != fingerprint
            or (job.market, job.dataset, job.tool_id) not in SUPPORTED_COMPLETIONS
            or marker.get("status") not in {"RAW_ACQUIRED", "RAW_ACQUIRED_WITH_WARNINGS"}
            or type(marker.get("schema_version")) is not int
            or marker.get("schema_version") != 1
            or type(marker.get("pages")) is not int
            or marker.get("pages") != 1
            or marker.get("gateway") != "qveris"
            or marker.get("upstream") != "eodhd"
            or marker.get("market") != job.market
            or marker.get("dataset") != job.dataset
        ):
            raise ValueError("unsupported or inconsistent completed price history")
        pins = marker.get("files")
        suffixes = _PAGE_FILES
        if not isinstance(pins, list) or len(pins) != len(suffixes):
            raise ValueError("completion must pin all four page artifacts")
        payloads = {}
        for suffix, value in zip(suffixes, pins, strict=True):
            pin = object_value(value)
            path = prefix + "0000." + suffix
            size = pin.get("size")
            # Generated metadata follows the writer's bound, not the paid payload cap.
            maximum = job.max_response_bytes if suffix == "raw" else MAX_RESPONSE_BYTES
            if pin.get("path") != path or type(size) is not int or not 0 <= size <= maximum:
                raise ValueError("invalid Qveris artifact pin")
            content = _read(tree, path, maximum)
            if len(content) != size or hashlib.sha256(content).hexdigest() != pin.get("sha256"):
                raise ValueError("Qveris artifact hash differs")
            payloads[suffix] = content
        intent = object_value(load_json(payloads["intent.json"]))
        response = object_value(load_json(payloads["response.json"]))
        billing = object_value(load_json(payloads["billing.json"]))
        raw = object_value(load_json(payloads["raw"]))
        raw_pin = pins[1]
        if response.get("status") != HTTPStatus.OK or response.get("raw") != raw_pin:
            raise ValueError("Qveris response raw pin or status differs")
        if (
            intent.get("fingerprint") != fingerprint
            or intent.get("parameters") != job.parameters
            or intent.get("tool_id") != job.tool_id
            or intent.get("upstream") != job.upstream
            or QverisJob.from_document(intent.get("job")).fingerprint != fingerprint
        ):
            raise ValueError("Qveris intent identity differs")
        execution = raw.get("execution_id")
        if (
            not isinstance(execution, str)
            or not execution
            or any(item.get("execution_id") != execution for item in (response, billing))
        ):
            raise ValueError("Qveris execution identity differs")
        usage = object_value(billing.get("usage"))
        if any(usage.get(key) != intent.get(key) for key in ("tool_id", "session_id", "search_id")):
            raise ValueError("Qveris usage identity differs")
        warning = usage.get("outcome") not in {None, "success"}
        expected_status = "RAW_ACQUIRED_WITH_WARNINGS" if warning else "RAW_ACQUIRED"
        if marker.get("status") != expected_status:
            raise ValueError("Qveris completion warning state differs from usage")
        settled = credit_value(billing.get("settled_credits"))
        if (
            billing.get("over_quote") is not False
            or settled > credit_value(intent.get("quoted_credits"))
            or settled != credit_value(marker.get("settled_credits"))
            or settled != credit_value(usage.get("actual_amount_credits"))
            or settled != credit_value(usage.get("settled_amount_credits"))
            or object_value(billing.get("usage")).get("execution_id") != execution
        ):
            raise ValueError("Qveris settlement differs")
        retrieved = datetime.fromisoformat(str(response.get("retrieved_at_utc")))
        if retrieved.utcoffset() is None:
            raise ValueError("Qveris retrieval timestamp must be timezone-aware")
        shape = validate_payload(job, job.parameters, raw, retrieved_on=retrieved.date())
        if type(marker.get("rows")) is not int or shape.rows != marker.get("rows"):
            raise ValueError("Qveris completion row count differs")
        values = object_value(raw.get("result")).get("data")
        if not isinstance(values, list):
            raise TypeError("history rows must be an array")
        return CompletedHistory(
            job,
            expected_completion_sha256,
            hashlib.sha256(payloads["raw"]).hexdigest(),
            retrieved,
            tuple(MappingProxyType(dict(object_value(row))) for row in values),
            provider_warning=warning,
            evidence=(body, *(payloads[suffix] for suffix in suffixes)),
        )


def _number(value: object, *, positive: bool = True) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("missing_or_non_numeric")
    try:
        number = float(value)
    except OverflowError:
        raise ValueError("non_finite") from None
    if not math.isfinite(number) or (number <= 0 if positive else number < 0):
        raise ValueError("invalid_price_or_volume")
    return number


def _identity(symbol: str, identities: Mapping[str, Mapping[str, str]]) -> Mapping[str, str]:
    identity = identities.get(symbol)
    if identity is None or set(identity) != {
        "instrument_id",
        "venue",
        "instrument_type",
        "currency",
    }:
        raise ValueError("explicit instrument identity is required")
    exchange = symbol.rsplit(".", 1)[-1]
    instrument_id = identity["instrument_id"]
    if (
        identity["venue"] != exchange
        or identity["currency"] != ("USD" if exchange == "US" else "KRW")
        or identity["instrument_type"] not in {"ETF", "Common Stock"}
        or type(instrument_id) is not str
        or not instrument_id
        or instrument_id != instrument_id.strip()
        or not instrument_id.isprintable()
    ):
        raise ValueError("provider identity or currency differs")
    return identity


def _bar(  # noqa: PLR0913 -- one provider row and its resolved context
    history: CompletedHistory,
    raw: Mapping[str, object],
    ordinal: int,
    symbol: str,
    identity: Mapping[str, str],
    *,
    fingerprint: str,
) -> Mapping[str, object]:
    observed = date.fromisoformat(str(raw["date"]))
    prices = {
        name: _number(raw.get(name)) for name in ("open", "high", "low", "close", "adjusted_close")
    }
    volume = _number(raw.get("volume"), positive=False)
    if (
        prices["low"] > min(prices["open"], prices["close"])
        or prices["high"] < max(prices["open"], prices["close"])
        or prices["low"] > prices["high"]
    ):
        raise ValueError("inconsistent_ohlc")
    return MappingProxyType(
        {
            **identity,
            "provider_symbol": symbol,
            "date": observed,
            **prices,
            "volume": volume,
            "source_fingerprint": fingerprint,
            "raw_sha256": history.raw_sha256,
            "retrieved_at": history.retrieved_at,
            "source_row": ordinal,
            "calendar_verified": False,
            "independent_identity_verified": False,
        }
    )


def _normalize(
    history: CompletedHistory, identities: Mapping[str, Mapping[str, str]], fixed_symbol: str | None
) -> NormalizedHistory:
    accepted, rejected = [], []
    fingerprint = history.job.fingerprint
    for ordinal, raw in enumerate(history.rows):
        if history.provider_warning:
            rejected.append(
                MappingProxyType(
                    {
                        "ordinal": ordinal,
                        "reason": "provider_reported_partial",
                        "source_row": dict(raw),
                    }
                )
            )
            continue
        symbol = fixed_symbol or str(raw.get("code")) + "." + str(raw.get("exchange_short_name"))
        try:
            identity = _identity(symbol, identities)
            accepted.append(_bar(history, raw, ordinal, symbol, identity, fingerprint=fingerprint))
        except (ValueError, TypeError, KeyError) as error:
            rejected.append(
                MappingProxyType(
                    {"ordinal": ordinal, "reason": str(error), "source_row": dict(raw)}
                )
            )
    return NormalizedHistory(tuple(accepted), tuple(rejected))


def normalize_korean_price_history(
    history: CompletedHistory, identities: Mapping[str, Mapping[str, str]]
) -> NormalizedHistory:
    if history.job.market != "KR" or history.job.dataset != "price_history":
        raise ValueError("expected Korean single-instrument history")
    return normalize_price_history(history, identities)


def normalize_price_history(
    history: CompletedHistory, identities: Mapping[str, Mapping[str, str]]
) -> NormalizedHistory:
    """One instrument's KR or US daily history; the job's symbol must have an identity."""
    if history.job.market not in {"KR", "US"} or history.job.dataset != "price_history":
        raise ValueError("expected KR or US single-instrument history")
    symbol = str(history.job.parameters["symbol"])
    _identity(symbol, identities)
    return _normalize(history, identities, symbol)


def _held(ordinal: int, reason: str, raw: Mapping[str, object]) -> Mapping[str, object]:
    return MappingProxyType({"ordinal": ordinal, "reason": reason, "source_row": dict(raw)})


def _text(raw: Mapping[str, object], name: str, *, required: bool = False) -> str | None:
    """A provider value as text: strings as sent, numbers as their shortest decimal."""
    value = raw.get(name)
    if value is None or value == "":
        if required:
            raise ValueError("missing_" + name)
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise TypeError("non_text_" + name)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("non_finite")
    return value if isinstance(value, str) else repr(value)


def _positive_decimal(value: str | None, reason: str) -> None:
    try:
        number = Decimal(value or "")
    except InvalidOperation:
        raise ValueError(reason) from None
    if not number.is_finite() or number <= 0:
        raise ValueError(reason)


def _action(
    history: CompletedHistory,
    raw: Mapping[str, object],
    ordinal: int,
    identities: Mapping[str, Mapping[str, str]],
    fingerprint: str,
) -> Mapping[str, object]:
    symbol = str(raw.get("code")) + "." + str(raw.get("exchange"))
    identity = _identity(symbol, identities)
    observed = date.fromisoformat(str(raw["date"]))
    if history.job.dataset == "splits":
        ratio = _text(raw, "split", required=True)
        match = _RATIO.fullmatch(ratio or "")
        if match is None:
            raise ValueError("invalid_split_ratio")
        _positive_decimal(match["new"], "invalid_split_ratio")
        _positive_decimal(match["old"], "invalid_split_ratio")
        values: dict[str, object] = {"split": ratio}
    else:
        dividend = _text(raw, "dividend", required=True)
        _positive_decimal(dividend, "invalid_dividend")
        unadjusted = _text(raw, "unadjustedValue")
        if unadjusted is not None:
            _positive_decimal(unadjusted, "invalid_dividend")
        values = {
            "dividend": dividend,
            "dividend_currency": _text(raw, "currency", required=True),
            "unadjusted_value": unadjusted,
            "declaration_date": _text(raw, "declarationDate"),
            "record_date": _text(raw, "recordDate"),
            "payment_date": _text(raw, "paymentDate"),
            "period": _text(raw, "period"),
        }
    return MappingProxyType(
        {
            **identity,
            "provider_symbol": symbol,
            "date": observed,
            **values,
            "source_fingerprint": fingerprint,
            "raw_sha256": history.raw_sha256,
            "retrieved_at": history.retrieved_at,
            "source_row": ordinal,
            "independent_identity_verified": False,
        }
    )


def normalize_bulk_actions(
    history: CompletedHistory, identities: Mapping[str, Mapping[str, str]]
) -> NormalizedHistory:
    """One exchange-day of splits or dividends, values kept as the provider's text.

    Rows of a download the gateway reported as partial are all held with the reason
    ``provider_reported_partial``; rows without an explicit identity or with a value
    that is not a positive decimal are held with that reason. Nothing is repaired.
    """
    if history.job.tool_id != EOD_TOOL or history.job.dataset not in ACTION_DATASETS:
        raise ValueError("expected exchange bulk splits or dividends")
    accepted, rejected = [], []
    fingerprint = history.job.fingerprint
    for ordinal, raw in enumerate(history.rows):
        if history.provider_warning:
            rejected.append(_held(ordinal, "provider_reported_partial", raw))
            continue
        try:
            accepted.append(_action(history, raw, ordinal, identities, fingerprint))
        except (ValueError, TypeError, KeyError) as error:
            rejected.append(_held(ordinal, str(error), raw))
    return NormalizedHistory(tuple(accepted), tuple(rejected))


def normalize_fx_history(history: CompletedHistory) -> NormalizedHistory:
    """One currency pair's daily history; a pair needs no instrument identity."""
    if history.job.market != FX_MARKET or history.job.dataset != FX_DATASET:
        raise ValueError("expected forex pair history")
    symbol = str(history.job.parameters["symbol"])
    pair = symbol.rpartition(".")[0]
    currencies = {
        "pair": pair,
        "base_currency": pair[:3],
        "quote_currency": pair[3:],
    }
    accepted, rejected = [], []
    fingerprint = history.job.fingerprint
    for ordinal, raw in enumerate(history.rows):
        if history.provider_warning:
            rejected.append(_held(ordinal, "provider_reported_partial", raw))
            continue
        try:
            bar = dict(_bar(history, raw, ordinal, symbol, currencies, fingerprint=fingerprint))
        except (ValueError, TypeError, KeyError) as error:
            rejected.append(_held(ordinal, str(error), raw))
            continue
        del bar["calendar_verified"], bar["independent_identity_verified"]
        accepted.append(MappingProxyType(bar))
    return NormalizedHistory(tuple(accepted), tuple(rejected))


def normalize_bulk_prices(
    history: CompletedHistory, identities: Mapping[str, Mapping[str, str]]
) -> NormalizedHistory:
    if history.job.tool_id != EOD_TOOL or history.job.dataset != "prices":
        raise ValueError("expected exchange bulk prices")
    return _normalize(history, identities, None)
