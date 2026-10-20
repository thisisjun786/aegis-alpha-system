"""Synthetic run artifacts straddling the former one-MiB read ceiling."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from typing import Any, cast

import pytest

from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.application.cli import main
from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage.runs import RunResult, commit_run, open_run
from aegis_alpha.storage.workspace import open_workspace
from tests.storage.test_runs import intent, prepared


def envelope(count: int) -> bytes:
    prices = {f"SYNTHETIC_{index:05}": 100.0 for index in range(count)}
    return canonical_json_bytes(
        {
            "schema_version": "aas-etf-backtest-v1",
            "module": "aegis",
            "instrument_types": dict.fromkeys(prices, "ETF"),
            "dates": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "opens": [prices] * 3,
            "closes": [prices] * 3,
            "targets": {"2024-01-02": {"SYNTHETIC_00000": 1.0}},
            "initial_cash": 1000.0,
            "cost": 0.0,
            "source_pins": [],
            "research_mode": "synthetic",
        }
    )


@pytest.mark.parametrize("case", [(5000, False), (10000, True)])
@pytest.mark.parametrize("writer_default", [False, True])
def test_sealed_envelopes_show_and_rerun_across_one_mib(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    case: tuple[int, bool],
    *,
    writer_default: bool,
) -> None:
    count, above_boundary = case
    fx = prepared(tmp_path)
    raw = envelope(count)
    assert (len(raw) > 1024 * 1024) == above_boundary
    digest = hashlib.sha256(raw).hexdigest()
    original = intent(fx, "budget-boundary")
    preparation = json.loads(original.preparation_bytes)
    preparation["envelope_sha256"] = digest
    candidate = replace(
        original, envelope_bytes=raw, preparation_bytes=canonical_json_bytes(preparation)
    )
    result = RunResult(canonical_json_bytes(run_document(raw, digest)))
    budget = None if writer_default else ComputeBudget(Fraction(1), 2 * 1024**3)
    with open_workspace(fx.home, writable=True) as workspace:
        handle = open_run(workspace, candidate, budget=budget)
        receipt = commit_run(workspace, handle, result, budget=budget)
    assert receipt["status"] == "SUCCESS"
    for name in (
        "AAS_HOST_CPU_LIMIT",
        "AAS_HOST_MEMORY_LIMIT_BYTES",
        "AAS_CPU_LIMIT",
        "AAS_MEMORY_LIMIT_BYTES",
        "AAS_COMPUTE_LOCK_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    assert main(["--home", str(fx.home), "run", "show", "--run-id", handle.run_id]) == 0
    shown = cast("dict[str, Any]", json.loads(capsys.readouterr().out))["run"]
    assert shown["result_hash"] == receipt["result_hash"]
    assert shown["artifact_sizes"]["envelope.json"] == len(raw)

    # The explicit resolver allocates 80%: this yields the same 512 MiB as show.
    monkeypatch.setenv("AAS_HOST_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_HOST_MEMORY_LIMIT_BYTES", str(640 * 1024**2))
    monkeypatch.setenv("AAS_COMPUTE_LOCK_FILE", str(tmp_path / "compute.lock"))
    assert main(["--home", str(fx.home), "run", "rerun", "--run-id", handle.run_id]) == 0
    rerun = json.loads(capsys.readouterr().out)
    assert rerun["reproduced"] is True
    assert (
        rerun["checks"]["result"]["recorded_sha256"]
        == hashlib.sha256(result.backtest_bytes).hexdigest()
    )
