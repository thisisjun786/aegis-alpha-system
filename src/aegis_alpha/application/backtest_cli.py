"""Explicit ETF research execution; supplied pins are not certified provenance."""

from __future__ import annotations

import argparse
import hashlib
import math
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.publication import json_value
from aegis_alpha.storage.source_reader import SourcePin

_MAX_INPUT_BYTES = 64 * 1024 * 1024
_FIELDS = frozenset(
    {
        "schema_version",
        "module",
        "instrument_types",
        "dates",
        "opens",
        "closes",
        "targets",
        "initial_cash",
        "cost",
        "source_pins",
        "research_mode",
    }
)


def add_commands(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = commands.add_parser(
        "backtest", help="Replay supplied ETF targets at next-session opens"
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--sha256", required=True, help="Expected exact input file SHA-256")


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("expected an object with text keys")
    return cast("dict[str, object]", value)


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("expected a finite number, not a boolean")
    try:
        number = float(value)
    except OverflowError as error:
        raise ValueError("number is outside finite float range") from error
    if not math.isfinite(number):
        raise ValueError("expected a finite number")
    return number


def _day(value: object) -> date:
    if not isinstance(value, str):
        raise TypeError("dates must be ISO date strings")
    result = date.fromisoformat(value)
    if result.isoformat() != value:
        raise ValueError("dates must use YYYY-MM-DD")
    return result


def _numbers(value: object) -> dict[str, float]:
    mapping = _mapping(value)
    if any(not key.strip() or "\0" in key for key in mapping):
        raise ValueError("instrument IDs must be nonempty text")
    return {key: _number(number) for key, number in mapping.items()}


def _prices(value: object) -> tuple[dict[str, float], ...]:
    if not isinstance(value, list):
        raise TypeError("prices must be arrays of instrument-price objects")
    return tuple(_numbers(row) for row in value)


def _pins(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise TypeError("source_pins must be an array")
    fields = {"source_id", "source_sha256", "table", "table_digest"}
    result = []
    for item in value:
        body = _mapping(item)
        if body.keys() != fields or any(not isinstance(value, str) for value in body.values()):
            raise ValueError("source pin fields must match the source-reader contract")
        pin = SourcePin(**cast("dict[str, str]", body))
        result.append(asdict(pin))
    return result


def run_document(raw: bytes, expected_sha256: str) -> dict[str, object]:  # noqa: C901 -- single explicit envelope and accounting boundary
    """Run explicit prices/targets, without claiming to resolve their source pins."""
    from aegis_alpha.engine.execution import replay_next_open  # noqa: PLC0415 -- execution owner

    if len(raw) > _MAX_INPUT_BYTES:
        raise ValueError("backtest input exceeds the byte limit")
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256:
        raise ValueError("backtest input SHA-256 mismatch")
    body = _mapping(decode_json(raw))
    if body.keys() != _FIELDS or body["schema_version"] != "aas-etf-backtest-v1":
        raise ValueError("unsupported backtest schema or missing/unknown fields")
    if body["module"] != "aegis":
        raise ValueError("this backtest contract belongs to aegis")
    if body["research_mode"] not in ("observed_etf_research", "synthetic"):
        raise ValueError("unsupported research mode")
    types = _mapping(body["instrument_types"])
    if any(value not in ("ETF", "INDEX", "SPOT") for value in types.values()):
        raise ValueError("unsupported instrument type")
    if not isinstance(body["dates"], list):
        raise TypeError("dates must be an array")
    dates = tuple(_day(day) for day in body["dates"])
    targets = {_day(day): _numbers(weights) for day, weights in _mapping(body["targets"]).items()}
    for weights in targets.values():
        if any(weight > 0 and types.get(symbol) != "ETF" for symbol, weight in weights.items()):
            raise ValueError("positive aegis target weights require explicit ETF instrument type")
    pins = _pins(body["source_pins"])
    try:
        result = replay_next_open(
            dates,
            _prices(body["opens"]),
            _prices(body["closes"]),
            targets,
            _number(body["initial_cash"]),
            _number(body["cost"]),
        )
    except ArithmeticError as error:
        raise ValueError(f"backtest accounting failed: {error}") from error
    return {
        "module": "aegis",
        "input_sha256": digest,
        "research_mode": body["research_mode"],
        "execution_convention": "decision close to next supplied session open",
        "source_pins": pins,
        "source_pins_verified": False,
        "observed_prices_verified": False,
        "point_in_time_verified": False,
        "live_orders": False,
        "result": json_value(asdict(result)),
    }


def execute(args: argparse.Namespace) -> dict[str, object]:
    path = cast("Path", args.input)
    with DescriptorTree.open_path(path.parent) as tree:
        raw = tree.read_bytes(path.name, max_bytes=_MAX_INPUT_BYTES)
    return run_document(raw, args.sha256)
