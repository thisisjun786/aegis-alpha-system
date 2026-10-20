"""JSON estimates cover measured decoder/accounting allocations and chunk boundaries."""

from __future__ import annotations

import hashlib
import json
import tracemalloc

import pytest

from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.json_memory import json_bytes_memory, json_memory
from tests.application.test_run_budgets import envelope


@pytest.mark.parametrize("width", [1, 2, 7, 65536])
def test_streaming_estimate_is_independent_of_chunk_boundaries(width: int) -> None:
    raw = canonical_json_bytes({"text": ['\\"{},:[]한😀', "\\", '"'] * 100})
    assert json_memory(raw[i : i + width] for i in range(0, len(raw), width)) == json_bytes_memory(
        raw
    )


@pytest.mark.parametrize(
    "document",
    [
        [0.1] * 10000,
        [{}] * 10000,
        {str(i): [i, None, "한😀"] for i in range(1000)},
        {"long": "synthetic" * 200000},
    ],
)
def test_estimate_covers_measured_decoder_peak(document: object) -> None:
    raw = canonical_json_bytes(document)
    tracemalloc.start()
    try:
        decoded = json.loads(raw)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert decoded == document
    assert json_bytes_memory(raw) > peak


def test_run_estimate_covers_measured_accounting_peak() -> None:
    raw = envelope(8000)
    tracemalloc.start()
    try:
        result = run_document(raw, hashlib.sha256(raw).hexdigest())
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert result["module"] == "aegis"
    assert json_bytes_memory(raw, copies=4) > peak
    assert json_bytes_memory(raw, copies=4) < 512 * 1024**2 // 4
