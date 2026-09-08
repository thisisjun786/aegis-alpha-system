from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from test_qveris_acquisition import FakeQveris

from aegis_alpha.data.qveris import InvocationBudget
from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_client import QverisResponse
from aegis_alpha.data.qveris_contracts import EOD_HISTORY_JSON_TOOL, QverisJob
from aegis_alpha.data.qveris_native import normalize_korean_price_history, read_completed_job
from aegis_alpha.data.serialization import canonical_json_bytes

IDENTITY = {
    "123456.KO": {
        "instrument_id": "synthetic-etf",
        "venue": "KO",
        "instrument_type": "ETF",
        "currency": "KRW",
    }
}


class HistoryClient(FakeQveris):
    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        response = super().request(path, body=body, query=query)
        if path == "/tools/by-ids":
            doc = response.document()
            results = doc["results"]
            assert isinstance(results, list)
            results[0]["provider_id"] = "eodhd"
            return replace(response, body=canonical_json_bytes(doc))
        return response


def complete(tmp_path: Path) -> tuple[Path, str, str]:
    job = QverisJob(
        "synthetic-history",
        EOD_HISTORY_JSON_TOOL,
        "eodhd",
        "KR",
        "price_history",
        canonical_json_bytes(
            {"symbol": "123456.KO", "fmt": "json", "from": "2026-08-01", "order": "a"}
        ).decode(),
        date(2026, 9, 6),
    )
    acquire_jobs((job,), tmp_path, HistoryClient(), budget=InvocationBudget(1, Decimal(3)))
    marker = tmp_path / "jobs" / job.fingerprint / "complete.json"
    return tmp_path, job.fingerprint, hashlib.sha256(marker.read_bytes()).hexdigest()


def test_verified_rows_keep_raw_and_adjusted_fields_separate(tmp_path: Path) -> None:
    history = read_completed_job(*complete(tmp_path))
    row = {**history.rows[0], "adjusted_close": 22}
    normalized = normalize_korean_price_history(replace(history, rows=(row,)), IDENTITY)
    assert normalized.rows[0]["open"] == 10  # noqa: PLR2004 -- independent raw opening price
    assert normalized.rows[0]["adjusted_close"] == 22  # noqa: PLR2004 -- independent adjustment input
    assert "adjusted_open" not in normalized.rows[0]
    assert normalized.source_only
    assert not normalized.next_open_eligible
    assert not normalized.point_in_time_certified


@pytest.mark.parametrize(
    ("field", "value"),
    [("open", None), ("high", 1), ("volume", -1), ("low", 100), ("close", float("inf"))],
)
def test_malformed_rows_quarantined_without_clamping(
    tmp_path: Path, field: str, value: object
) -> None:
    history = read_completed_job(*complete(tmp_path))
    result = normalize_korean_price_history(
        replace(history, rows=({**history.rows[0], field: value},)), IDENTITY
    )
    assert not result.rows
    assert len(result.quarantine) == 1


def test_changed_raw_rejected(tmp_path: Path) -> None:
    args = complete(tmp_path)
    raw = tmp_path / "jobs" / args[1] / "0000.raw"
    raw.write_bytes(raw.read_bytes() + b" ")
    with pytest.raises(ValueError, match="artifact hash"):
        read_completed_job(*args)


def test_wrong_completion_pin_rejected(tmp_path: Path) -> None:
    root, fingerprint, _pin = complete(tmp_path)
    with pytest.raises(ValueError, match="completion hash"):
        read_completed_job(root, fingerprint, "0" * 64)


def test_unknown_identity_and_wrong_venue_rejected(tmp_path: Path) -> None:
    history = read_completed_job(*complete(tmp_path))
    with pytest.raises(ValueError, match="identity"):
        normalize_korean_price_history(history, {})
    with pytest.raises(ValueError, match="identity"):
        normalize_korean_price_history(
            history, {"123456.KO": {**IDENTITY["123456.KO"], "venue": "KQ"}}
        )
