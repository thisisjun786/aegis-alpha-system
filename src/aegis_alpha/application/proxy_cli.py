"""Explicit non-executable proxy returns, separate from ETF trade replay."""

from __future__ import annotations

import argparse
import hashlib
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Literal, cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.engine.numbers import require_finite
from aegis_alpha.engine.proxy import ProxyRecipe, ReturnSeries, build_proxy_returns
from aegis_alpha.storage.publication import json_value

_MAX_INPUT_BYTES = 64 * 1024 * 1024
_ENVELOPE = frozenset({"schema_version", "module", "target_type", "donor", "target", "recipe"})
_SERIES = frozenset(
    {
        "instrument_id",
        "currency",
        "return_kind",
        "close_convention",
        "net_of_fees",
        "anchor_date",
        "dates",
        "returns",
        "source_sha256",
    }
)
_RECIPE = frozenset({"target_id", "donor_id", "switch_date", "annual_fee", "fee_model", "reason"})


def add_commands(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = commands.add_parser("proxy", help="Build a non-executable research return index")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--sha256", required=True, help="Expected exact input file SHA-256")


def _object(value: object, fields: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, dict) or value.keys() != fields:
        raise ValueError("proxy object has missing or unknown fields")
    return cast("dict[str, object]", value)


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("proxy text fields must be strings")
    return value


def _day(value: object) -> date:
    text = _text(value)
    parsed = date.fromisoformat(text)
    if parsed.isoformat() != text:
        raise ValueError("proxy dates must use YYYY-MM-DD")
    return parsed


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise TypeError("proxy dates and returns must be arrays")
    return value


def _series(value: object) -> ReturnSeries:
    body = _object(value, _SERIES)
    kind = body["return_kind"]
    if kind not in ("price_return", "total_return"):
        raise ValueError("unknown proxy return_kind")
    net = body["net_of_fees"]
    if type(net) is not bool:
        raise TypeError("net_of_fees must be a boolean")
    return ReturnSeries(
        instrument_id=_text(body["instrument_id"]),
        currency=_text(body["currency"]),
        return_kind=cast("Literal['price_return', 'total_return']", kind),
        close_convention=_text(body["close_convention"]),
        net_of_fees=net,
        anchor_date=_day(body["anchor_date"]),
        dates=tuple(_day(day) for day in _array(body["dates"])),
        returns=tuple(require_finite(number, field="return") for number in _array(body["returns"])),
        source_sha256=_text(body["source_sha256"]),
    )


def _recipe(value: object) -> ProxyRecipe:
    body = _object(value, _RECIPE)
    model = body["fee_model"]
    if model not in ("already_net", "annual_expense", "zero_expense_sensitivity"):
        raise ValueError("unknown proxy fee_model")
    return ProxyRecipe(
        target_id=_text(body["target_id"]),
        donor_id=_text(body["donor_id"]),
        switch_date=_day(body["switch_date"]),
        annual_fee=require_finite(body["annual_fee"], field="annual_fee"),
        fee_model=cast(
            "Literal['already_net', 'annual_expense', 'zero_expense_sensitivity']", model
        ),
        reason=_text(body["reason"]),
    )


def run_document(raw: bytes, expected_sha256: str) -> dict[str, object]:
    if len(raw) > _MAX_INPUT_BYTES:
        raise ValueError("proxy input exceeds the byte limit")
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256:
        raise ValueError("proxy input SHA-256 mismatch")
    body = _object(decode_json(raw), _ENVELOPE)
    if body["schema_version"] != "aas-proxy-returns-v1" or body["module"] != "aegis":
        raise ValueError("unsupported proxy schema or module")
    if body["target_type"] != "ETF":
        raise ValueError("proxy target requires explicit ETF type")
    result = build_proxy_returns(
        _series(body["donor"]), _series(body["target"]), _recipe(body["recipe"])
    )
    return {
        "schema_version": "aas-proxy-returns-result-v1",
        "module": "aegis",
        "input_sha256": digest,
        "instrument_type_verified": False,
        "live_orders": False,
        "result": json_value(asdict(result)),
    }


def execute(args: argparse.Namespace) -> dict[str, object]:
    path = cast("Path", args.input).absolute()
    with DescriptorTree.open_path(path.parent) as tree:
        raw = tree.read_bytes(path.name, max_bytes=_MAX_INPUT_BYTES)
    return run_document(raw, args.sha256)
