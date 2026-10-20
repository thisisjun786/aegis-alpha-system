"""Capacity refusals and explicit-budget readback warnings use synthetic stores."""

from __future__ import annotations

import json
from dataclasses import replace
from fractions import Fraction
from pathlib import Path

import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage import runs
from aegis_alpha.storage.market_inputs import _raw_payload
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.workspace import open_workspace, write_json
from tests.storage.test_runs import BUDGET, RESULT, intent, prepared


def test_large_pinned_source_document_uses_remaining_bytes_once(tmp_path: Path) -> None:
    fx = prepared(tmp_path)
    # A source document of the reported size, dominated by retained textual evidence.
    raw = canonical_json_bytes({"rows": [{"source": "synthetic observation " * 12}] * 92000})
    reported_size = 24_329_248
    assert len(raw) > reported_size
    allocation = ComputeBudget(Fraction(1), 8 * 1024**3 * 4 // 5).component(2)
    budget = replace(allocation, reserved_bytes=128 * 1024**2)
    with open_workspace(fx.home, writable=True) as workspace:
        _relative, digest, _size = put_raw(workspace.paths.raw, raw)
        assert _raw_payload(workspace, digest, budget) == raw
        with pytest.raises(ComputeResourceError, match="materialization budget"):
            _raw_payload(workspace, digest, BUDGET)


def test_commit_warns_when_explicit_budget_exceeds_default_read_capacity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx = prepared(tmp_path)
    candidate = intent(fx, "readback-warning")
    # DuckDB configuration does not override the Python reader's allowance.
    runtime = fx.home / "runtime.json"
    configuration = json.loads(runtime.read_bytes())
    configuration["resources"]["memory_limit"] = "8GB"
    write_json(runtime, configuration)
    # Make even this small fixture exceed the fallback while allowing a real commit.
    monkeypatch.setattr(runs, "_DEFAULT_MEMORY_BYTES", 2 * 1024**2)
    extra = canonical_json_bytes({"padding": "synthetic" * 100000})
    candidate = replace(
        candidate, preparation_bytes=candidate.preparation_bytes + b" " * len(extra)
    )
    with open_workspace(fx.home, writable=True) as workspace:
        handle = runs.open_run(workspace, candidate, budget=BUDGET)
        receipt = runs.commit_run(workspace, handle, runs.RunResult(RESULT), budget=BUDGET)
        assert receipt["status"] == "SUCCESS"
        assert "explicit compute budget" in str(receipt["warnings"])
        with pytest.raises(ComputeResourceError):
            runs.read_run(workspace, handle.run_id)
        assert runs.read_run(workspace, handle.run_id, budget=BUDGET)["status"] == "SUCCESS"
