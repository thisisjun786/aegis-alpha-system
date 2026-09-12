"""Generate bounded, caller-supplied ETF research candidates without evaluation."""

from __future__ import annotations

import argparse
import hashlib
from datetime import date
from pathlib import Path
from typing import cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.engine.research import ParameterGrid, generate_trials

_MAX_INPUT_BYTES = 64 * 1024 * 1024
_MAX_TRIALS = 256
_ENVELOPE = frozenset(
    {
        "schema_version",
        "module",
        "action",
        "parent_hash",
        "seed",
        "grid",
        "train_end",
        "validation_end",
        "test_end",
        "cost_ref",
        "max_trials",
    }
)
_GRID = frozenset(
    {
        "etf_universes",
        "instrument_types",
        "momentum_horizons",
        "absolute_filters",
        "trend_filters",
        "weightings",
        "volatility_caps",
    }
)


def add_commands(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = commands.add_parser("research", help="Generate bounded ETF research candidates")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--sha256", required=True, help="Expected exact input file SHA-256")


def _object(value: object, fields: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, dict) or value.keys() != fields:
        raise ValueError("research object has missing or unknown fields")
    return cast("dict[str, object]", value)


def _array(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"research {field} must be an array")
    return value


def _day(value: object, field: str) -> date:
    if not isinstance(value, str):
        raise TypeError(f"research {field} must be a YYYY-MM-DD string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"research {field} must use YYYY-MM-DD") from error
    if parsed.isoformat() != value:
        raise ValueError(f"research {field} must use YYYY-MM-DD")
    return parsed


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"research {field} must be an integer")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"research {field} must be a string")
    return value


def run_document(raw: bytes, expected_sha256: str) -> dict[str, object]:
    """Hash, decode, and generate public trial specifications from untrusted bytes."""
    if len(raw) > _MAX_INPUT_BYTES:
        raise ValueError("research input exceeds the byte limit")
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256:
        raise ValueError("research input SHA-256 mismatch")
    body = _object(decode_json(raw), _ENVELOPE)
    if (
        body["schema_version"] != "aas-etf-research-v1"
        or body["module"] != "aegis"
        or body["action"] != "generate"
    ):
        raise ValueError("unsupported research schema, module, or action")
    grid_body = _object(body["grid"], _GRID)
    grid = ParameterGrid(
        etf_universes=cast(
            "tuple[tuple[str, ...], ...]",
            tuple(_array(grid_body["etf_universes"], "etf_universes")),
        ),
        instrument_types=cast(
            "tuple[tuple[str, str], ...]",
            tuple(_array(grid_body["instrument_types"], "instrument_types")),
        ),
        momentum_horizons=cast(
            "tuple[int, ...]", tuple(_array(grid_body["momentum_horizons"], "momentum_horizons"))
        ),
        absolute_filters=cast(
            "tuple[bool, ...]", tuple(_array(grid_body["absolute_filters"], "absolute_filters"))
        ),
        trend_filters=cast(
            "tuple[bool, ...]", tuple(_array(grid_body["trend_filters"], "trend_filters"))
        ),
        weightings=cast("tuple[str, ...]", tuple(_array(grid_body["weightings"], "weightings"))),
        volatility_caps=cast(
            "tuple[float, ...]", tuple(_array(grid_body["volatility_caps"], "volatility_caps"))
        ),
    )
    max_trials = _integer(body["max_trials"], "max_trials")
    if max_trials <= 0 or max_trials > _MAX_TRIALS:
        raise ValueError(f"research max_trials must be between 1 and {_MAX_TRIALS}")
    if grid.size > max_trials:
        raise ValueError(
            f"research grid would produce {grid.size} candidates, exceeding max_trials"
        )
    trials = generate_trials(
        grid,
        parent_hash=_text(body["parent_hash"], "parent_hash"),
        seed=_integer(body["seed"], "seed"),
        train_end=_day(body["train_end"], "train_end"),
        validation_end=_day(body["validation_end"], "validation_end"),
        test_end=_day(body["test_end"], "test_end"),
        cost_ref=_text(body["cost_ref"], "cost_ref"),
        max_trials=max_trials,
    )
    return {
        "schema_version": "aas-etf-research-result-v1",
        "input_sha256": digest,
        "module": "aegis",
        "action": "generate",
        "trial_count": len(trials),
        "trials": [trial.to_document() for trial in trials],
        "research_only": True,
        "instrument_types_verified": False,
        "source_pins_verified": False,
        "observed_prices_verified": False,
        "point_in_time_verified": False,
        "proxy_admitted": False,
        "source_parity_certified": False,
        "evaluation_performed": False,
        "live_orders": False,
    }


def execute(args: argparse.Namespace) -> dict[str, object]:
    path = cast("Path", args.input).absolute()
    with DescriptorTree.open_path(path.parent) as tree:
        raw = tree.read_bytes(path.name, max_bytes=_MAX_INPUT_BYTES)
    return run_document(raw, args.sha256)
