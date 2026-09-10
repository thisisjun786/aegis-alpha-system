"""Compare explicitly supplied ETF evidence; no discovery, provider, or DB access."""

from __future__ import annotations

import argparse
import hashlib
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.engine.etf_candidates import (
    ComparisonPolicy,
    ETFProfile,
    TrackingMeasure,
    compare_etfs,
)
from aegis_alpha.storage.publication import json_value

_MAX_INPUT_BYTES = 64 * 1024 * 1024
_MAX_CANDIDATES = 256
_ENVELOPE = frozenset({"schema_version", "module", "action", "current", "candidates", "policy"})
_PROFILE = frozenset(
    {
        "instrument_id",
        "exposure_id",
        "currency",
        "hedged",
        "leverage",
        "reset",
        "fee_bps",
        "inception",
        "as_of",
        "source_hash",
        "tracking",
        "liquidity",
    }
)
_TRACKING = frozenset({"start", "end", "value", "source_hash", "basis"})
_POLICY = frozenset(
    {
        "as_of",
        "max_profile_age_days",
        "min_liquidity",
        "min_fee_saving_bps",
        "max_tracking_error",
        "tracking_start",
        "tracking_end",
        "tracking_basis",
    }
)


def add_commands(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = commands.add_parser(
        "etfs", help="Compare explicitly supplied ETF evidence without automatic replacement"
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--sha256", required=True, help="Expected exact input file SHA-256")


def _object(value: object, fields: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, dict) or value.keys() != fields:
        raise ValueError("etf comparison object has missing or unknown fields")
    return cast("dict[str, object]", value)


def _array(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"etf comparison {field} must be an array")
    return value


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("etf comparison text fields must be strings")
    return value


def _optional_text(value: object) -> str | None:
    return None if value is None else _text(value)


def _day(value: object) -> date:
    text = _text(value)
    parsed = date.fromisoformat(text)
    if parsed.isoformat() != text:
        raise ValueError("etf comparison dates must use YYYY-MM-DD")
    return parsed


def _optional_day(value: object) -> date | None:
    return None if value is None else _day(value)


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("etf comparison numeric fields must be numbers, not booleans")
    try:
        return float(value)
    except OverflowError as error:
        raise ValueError("etf comparison numeric fields must fit in a float") from error


def _optional_number(value: object) -> float | None:
    return None if value is None else _number(value)


def _boolean(value: object) -> bool:
    if type(value) is not bool:
        raise TypeError("etf comparison hedged must be a boolean")
    return value


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("etf comparison max_profile_age_days must be an integer")
    return value


def _tracking(value: object) -> TrackingMeasure:
    body = _object(value, _TRACKING)
    return TrackingMeasure(
        _day(body["start"]),
        _day(body["end"]),
        _number(body["value"]),
        _text(body["source_hash"]),
        _text(body["basis"]),
    )


def _optional_tracking(value: object) -> TrackingMeasure | None:
    return None if value is None else _tracking(value)


def _profile(value: object) -> ETFProfile:
    body = _object(value, _PROFILE)
    return ETFProfile(
        instrument_id=_text(body["instrument_id"]),
        exposure_id=_text(body["exposure_id"]),
        currency=_text(body["currency"]),
        hedged=_boolean(body["hedged"]),
        leverage=_number(body["leverage"]),
        reset=_text(body["reset"]),
        fee_bps=_optional_number(body["fee_bps"]),
        inception=_optional_day(body["inception"]),
        as_of=_optional_day(body["as_of"]),
        source_hash=_optional_text(body["source_hash"]),
        tracking=_optional_tracking(body["tracking"]),
        liquidity=_optional_number(body["liquidity"]),
    )


def _policy(value: object) -> ComparisonPolicy:
    body = _object(value, _POLICY)
    return ComparisonPolicy(
        _day(body["as_of"]),
        _integer(body["max_profile_age_days"]),
        _number(body["min_liquidity"]),
        _number(body["min_fee_saving_bps"]),
        _number(body["max_tracking_error"]),
        _day(body["tracking_start"]),
        _day(body["tracking_end"]),
        _text(body["tracking_basis"]),
    )


def run_document(raw: bytes, expected_sha256: str) -> dict[str, object]:
    """Hash, decode, and compare untrusted ETF evidence; never resolve or replace assets."""
    if len(raw) > _MAX_INPUT_BYTES:
        raise ValueError("etf comparison input exceeds the byte limit")
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256:
        raise ValueError("etf comparison input SHA-256 mismatch")
    body = _object(decode_json(raw), _ENVELOPE)
    if (
        body["schema_version"] != "aas-etf-comparison-v1"
        or body["module"] != "aegis"
        or body["action"] != "compare"
    ):
        raise ValueError("unsupported etf comparison schema, module, or action")
    candidates_raw = _array(body["candidates"], "candidates")
    if len(candidates_raw) > _MAX_CANDIDATES:
        raise ValueError(f"etf comparison candidates exceed the cap of {_MAX_CANDIDATES}")
    current = _profile(body["current"])
    candidates = [_profile(item) for item in candidates_raw]
    policy = _policy(body["policy"])
    report = compare_etfs(current, candidates, policy)
    return {
        "schema_version": "aas-etf-comparison-result-v1",
        "module": "aegis",
        "action": "compare",
        "input_sha256": digest,
        "research_only": True,
        "automatic_replacement": False,
        "source_pins_verified": False,
        "result": json_value(asdict(report)),
    }


def execute(args: argparse.Namespace) -> dict[str, object]:
    path = cast("Path", args.input)
    with DescriptorTree.open_path(path.parent) as tree:
        raw = tree.read_bytes(path.name, max_bytes=_MAX_INPUT_BYTES)
    return run_document(raw, args.sha256)
