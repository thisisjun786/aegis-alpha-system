"""Offline CLI coverage for aggregate research candidate output limits."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING

from aegis_alpha.application.cli import main
from aegis_alpha.engine import research
from aegis_alpha.engine.research import TrialSpec
from tests.engine.test_research_output_bounds import forbid_materialization

if TYPE_CHECKING:
    import pytest


def request_bytes(universe: tuple[str, ...], horizons: tuple[int, ...]) -> bytes:
    return json.dumps(
        {
            "schema_version": "aas-etf-research-v1",
            "module": "aegis",
            "action": "generate",
            "parent_hash": "a" * 64,
            "seed": 7,
            "grid": {
                "etf_universes": [universe],
                "instrument_types": [(item, "ETF") for item in universe],
                "momentum_horizons": horizons,
                "absolute_filters": [True],
                "trend_filters": [False],
                "weightings": ["equal"],
                "volatility_caps": [0.15],
            },
            "train_end": "2020-12-31",
            "validation_end": "2021-12-31",
            "test_end": "2022-12-31",
            "cost_ref": "synthetic-cost-v1",
            "max_trials": 256,
        }
    ).encode()


def test_cli_rejects_before_product_when_projected_output_is_oversized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Given: a hash-pinned sub-MiB input, still within the 256-trial ceiling.
    raw = request_bytes(("X" * 300_000,), tuple(range(1, 257)))
    path = tmp_path / "oversized.json"
    path.write_bytes(raw)
    monkeypatch.setattr(research, "product", forbid_materialization)

    # When: use the real file-read, parse, generation and CLI error surface.
    code = main(["research", "--input", str(path), "--sha256", hashlib.sha256(raw).hexdigest()])

    # Then: no candidate result is emitted, only the structured rejection.
    output = capsys.readouterr()
    assert code == 1
    assert output.out == ""
    assert "error" in json.loads(output.err)


def test_cli_preserves_candidate_when_grid_is_small(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Given: the same synthetic inputs as the pre-fix candidate fixture.
    raw = request_bytes(("ETF-A", "ETF-B"), (3,))
    path = tmp_path / "valid.json"
    path.write_bytes(raw)

    # When: generate through the real CLI surface and its pretty JSON encoder.
    code = main(["research", "--input", str(path), "--sha256", hashlib.sha256(raw).hexdigest()])

    # Then: the immutable candidate retains its input values and dispositions.
    output = capsys.readouterr()
    result = json.loads(output.out)
    assert code == 0
    assert output.err == ""
    assert result["trial_count"] == 1
    spec = TrialSpec.from_document(result["trials"][0])
    assert spec.parameter_grid.etf_universe == ("ETF-A", "ETF-B")
    assert (
        spec.canonical_sha256 == "82b2cf4f08036c09ca2851c1c3676284cad687ef1d2220cb17c06572ea64dbdd"
    )
    assert result["research_only"] is True
    assert result["evaluation_performed"] is False
    assert result["live_orders"] is False


def test_cli_output_fits_when_escaped_and_unicode_candidates_are_accepted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Given: a scaled ceiling includes the whole CLI envelope and newline.
    byte_limit = 8192
    universe = ("I" + "\x01" * 32, "\U00010000" * 32)
    raw = request_bytes(universe, (3, 6, 9))
    path = tmp_path / "encoded.json"
    path.write_bytes(raw)
    monkeypatch.setattr(research, "_MAX_GENERATED_BYTES", byte_limit)

    # When: the real encoder writes nested specs using ensure_ascii=False.
    code = main(["research", "--input", str(path), "--sha256", hashlib.sha256(raw).hexdigest()])

    # Then: accepted output fits in bytes, with all requested values preserved.
    output = capsys.readouterr()
    assert code == 0
    assert output.err == ""
    assert len(output.out.encode("utf-8")) <= byte_limit
    result = json.loads(output.out)
    assert result["trial_count"] == len((3, 6, 9))
    assert all(
        TrialSpec.from_document(trial).parameter_grid.etf_universe == universe
        for trial in result["trials"]
    )
