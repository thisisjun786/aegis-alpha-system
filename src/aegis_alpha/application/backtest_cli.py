"""Explicit ETF research execution; supplied pins are not certified provenance."""

from __future__ import annotations

import argparse
import hashlib
import math
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.publication import json_value
from aegis_alpha.storage.source_reader import SourcePin

if TYPE_CHECKING:
    from aegis_alpha.engine.execution import CashFlow

_MAX_INPUT_BYTES = 64 * 1024 * 1024
# The one mode whose inputs are reference observations by the store's own
# classification. Named separately so the two established modes keep the guard that
# stops a target resting on a series nobody can trade.
DECLARED_RESEARCH_MODE = "declared_uncertified_research"
_RESEARCH_MODES = ("observed_etf_research", "synthetic", DECLARED_RESEARCH_MODE)
_INSTRUMENT_TYPES = ("ETF", "INDEX", "SPOT")
_OBSERVATION_TYPE = "OBSERVATION"
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


def _cashflows(value: object, session_count: int) -> tuple[CashFlow, ...]:
    from aegis_alpha.engine.execution import CashFlow  # noqa: PLC0415 -- execution owner

    if not isinstance(value, list):
        raise TypeError("cashflows must be an array")
    if len(value) > max(0, session_count - 1):
        raise ValueError("cashflows exceed the supplied session count")
    flows = []
    for item in value:
        body = _mapping(item)
        if body.keys() != {"date", "amount"}:
            raise ValueError("cashflow has missing or unknown fields")
        flows.append(CashFlow(_day(body["date"]), _number(body["amount"])))
    return tuple(flows)


def run_document(raw: bytes, expected_sha256: str) -> dict[str, object]:  # noqa: C901 -- single explicit envelope and accounting boundary
    """Run explicit prices/targets, without claiming to resolve their source pins."""
    from aegis_alpha.engine.execution import (  # noqa: PLC0415 -- execution owner
        replay_next_open,
        replay_next_open_cashflows,
    )

    if len(raw) > _MAX_INPUT_BYTES:
        raise ValueError("backtest input exceeds the byte limit")
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256:
        raise ValueError("backtest input SHA-256 mismatch")
    body = _mapping(decode_json(raw))
    version = body.get("schema_version")
    fields = _FIELDS | {"cashflows"} if version == "aas-etf-backtest-v2" else _FIELDS
    if body.keys() != fields or version not in ("aas-etf-backtest-v1", "aas-etf-backtest-v2"):
        raise ValueError("unsupported backtest schema or missing/unknown fields")
    if body["module"] != "aegis":
        raise ValueError("this backtest contract belongs to aegis")
    mode = body["research_mode"]
    if mode not in _RESEARCH_MODES:
        raise ValueError("unsupported research mode")
    # A declared uncertified run is explicitly not executable: its own envelope says so
    # and the response below repeats it. That is the only reason a target may rest on an
    # observation here, and the reason it still may not anywhere else.
    declared = mode == DECLARED_RESEARCH_MODE
    types = _mapping(body["instrument_types"])
    admitted = (*_INSTRUMENT_TYPES, _OBSERVATION_TYPE) if declared else _INSTRUMENT_TYPES
    if any(value not in admitted for value in types.values()):
        raise ValueError("unsupported instrument type")
    if not isinstance(body["dates"], list):
        raise TypeError("dates must be an array")
    dates = tuple(_day(day) for day in body["dates"])
    targets = {_day(day): _numbers(weights) for day, weights in _mapping(body["targets"]).items()}
    weighted = ("ETF", _OBSERVATION_TYPE) if declared else ("ETF",)
    for weights in targets.values():
        if any(
            weight > 0 and types.get(symbol) not in weighted for symbol, weight in weights.items()
        ):
            raise ValueError(
                "positive aegis target weights require an explicit "
                + " or ".join(weighted)
                + " instrument type"
            )
    pins = _pins(body["source_pins"])
    opens = _prices(body["opens"])
    closes = _prices(body["closes"])
    initial_cash = _number(body["initial_cash"])
    cost = _number(body["cost"])
    try:
        result = (
            replay_next_open_cashflows(
                dates,
                opens,
                closes,
                targets,
                initial_cash,
                cost,
                _cashflows(body["cashflows"], len(dates)),
            )
            if version == "aas-etf-backtest-v2"
            else replay_next_open(dates, opens, closes, targets, initial_cash, cost)
        )
    except ArithmeticError as error:
        raise ValueError(f"backtest accounting failed: {error}") from error
    response: dict[str, object] = {
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
    if version == "aas-etf-backtest-v2":
        response["cashflow_convention"] = (
            "explicit cashflows at supplied session open before rebalancing; "
            "withdrawals use existing cash only; unit NAV removes external flow effects"
        )
    if declared:
        # This mode computes fills and NAV over reference observations, so the response
        # says what it is rather than leaving a reader to infer it from a mode name.
        response["certified"] = False
        response["non_executable"] = True
        response["executable_prices"] = False
    return response


def execute(args: argparse.Namespace) -> dict[str, object]:
    path = cast("Path", args.input).absolute()
    with DescriptorTree.open_path(path.parent) as tree:
        raw = tree.read_bytes(path.name, max_bytes=_MAX_INPUT_BYTES)
    return run_document(raw, args.sha256)
