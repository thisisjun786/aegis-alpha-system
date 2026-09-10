from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.application.cli import main
from aegis_alpha.application.research_cli import run_document
from aegis_alpha.engine.research import TrialSpec


def document() -> dict[str, object]:
    return {
        "schema_version": "aas-etf-research-v1",
        "module": "aegis",
        "action": "generate",
        "parent_hash": "a" * 64,
        "seed": 7,
        "grid": {
            "etf_universes": [["ETF-A", "ETF-B"]],
            "instrument_types": [["ETF-A", "ETF"], ["ETF-B", "ETF"]],
            "momentum_horizons": [3],
            "absolute_filters": [True],
            "trend_filters": [False],
            "weightings": ["equal"],
            "volatility_caps": [0.15],
        },
        "train_end": "2020-12-31",
        "validation_end": "2021-12-31",
        "test_end": "2022-12-31",
        "cost_ref": "synthetic-cost-v1",
        "max_trials": 1,
    }


def run(body: dict[str, object]) -> dict[str, object]:
    raw = json.dumps(body).encode()
    return run_document(raw, hashlib.sha256(raw).hexdigest())


def test_cli_generates_round_trippable_research_only_trials(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    raw = json.dumps(document()).encode()
    path = tmp_path / "research.json"
    path.write_bytes(raw)
    assert (
        main(["research", "--input", str(path), "--sha256", hashlib.sha256(raw).hexdigest()]) == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["schema_version"] == "aas-etf-research-result-v1"
    assert result["trial_count"] == 1
    assert TrialSpec.from_document(result["trials"][0]).parameter_grid.etf_universe == (
        "ETF-A",
        "ETF-B",
    )
    assert result["research_only"] is True
    assert result["instrument_types_verified"] is False
    assert result["source_pins_verified"] is False
    assert result["evaluation_performed"] is False
    assert result["live_orders"] is False
    assert result["point_in_time_verified"] is False


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("unknown",), True, "missing or unknown"),
        (("grid", "unknown"), True, "missing or unknown"),
        (("grid", "momentum_horizons"), [3], "missing or unknown"),
    ],
)
def test_unknown_or_missing_fields_are_rejected(
    path: tuple[str, ...], value: object, message: str
) -> None:
    body = document()
    target: dict[str, object] = body
    for field in path[:-1]:
        target = cast("dict[str, object]", target[field])
    if path == ("grid", "momentum_horizons"):
        del target[path[-1]]
    else:
        target[path[-1]] = value
    with pytest.raises(ValueError, match=message):
        run(body)


def test_hash_precedes_decode_and_duplicate_keys_are_rejected() -> None:
    with pytest.raises(ValueError, match="SHA-256"):
        run_document(b"not JSON", "0" * 64)
    raw = b'{"schema_version":"aas-etf-research-v1","schema_version":"aas-etf-research-v1"}'
    with pytest.raises(ValueError, match="duplicate"):
        run_document(raw, hashlib.sha256(raw).hexdigest())


@pytest.mark.parametrize(
    ("path", "value", "error", "message"),
    [
        (("grid", "instrument_types"), {"ETF-A": "ETF", "ETF-B": "ETF"}, TypeError, "array"),
        (
            ("grid", "instrument_types"),
            [["ETF-A", "ETF"], ["ETF-A", "ETF"]],
            ValueError,
            "duplicate",
        ),
        (("grid", "instrument_types"), [["ETF-A", "ETF"], ["ETF-B", "STOCK"]], ValueError, "ETF"),
        (("train_end",), "2020-12-32", ValueError, "YYYY-MM-DD"),
        (("seed",), True, TypeError, "integer"),
        (("max_trials",), True, TypeError, "integer"),
        (("max_trials",), 257, ValueError, "256"),
    ],
)
def test_invalid_input_boundaries_are_rejected(
    path: tuple[str, ...], value: object, error: type[Exception], message: str
) -> None:
    body = deepcopy(document())
    target: dict[str, object] = body
    for field in path[:-1]:
        target = cast("dict[str, object]", target[field])
    target[path[-1]] = value
    with pytest.raises(error, match=message):
        run(body)


def test_grid_limit_and_input_size_are_rejected_before_generation() -> None:
    body = document()
    body["max_trials"] = 1
    grid = body["grid"]
    assert isinstance(grid, dict)
    grid["trend_filters"] = [False, True]
    with pytest.raises(ValueError, match="exceeding"):
        run(body)
    raw = b"x" * (64 * 1024 * 1024 + 1)
    with pytest.raises(ValueError, match="byte limit"):
        run_document(raw, hashlib.sha256(raw).hexdigest())
