from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from test_qveris_native import HistoryClient

from aegis_alpha.data.qveris import InvocationBudget
from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_contracts import (
    EOD_HISTORY_JSON_TOOL,
    EOD_TOOL,
    EOD_UNIVERSE_TOOL,
    QverisJob,
)
from aegis_alpha.data.qveris_native import normalize_korean_price_history, read_completed_job
from aegis_alpha.data.serialization import canonical_json_bytes


def history(market: str, symbol: str, dataset: str) -> QverisJob:
    return QverisJob(
        "synthetic-research",
        EOD_HISTORY_JSON_TOOL,
        "eodhd",
        market,
        dataset,
        canonical_json_bytes(
            {"symbol": symbol, "fmt": "json", "from": "2026-08-01", "order": "a"}
        ).decode(),
        date(2026, 9, 6),
    )


@pytest.mark.parametrize(
    ("market", "symbol", "dataset"),
    [
        ("INDEX", "SYNTHETIC.INDX", "research_price_history"),
        ("CRYPTO", "SYNTHETIC-USD.CC", "research_price_history"),
        ("US", "SYNTHETIC.US", "price_history"),
    ],
)
def test_research_raw_verifies_without_etf_admission(
    tmp_path: Path, market: str, symbol: str, dataset: str
) -> None:
    job = history(market, symbol, dataset)
    client = HistoryClient()
    acquire_jobs((job,), tmp_path, client, budget=InvocationBudget(1, Decimal(3)))
    marker = tmp_path / "jobs" / job.fingerprint / "complete.json"
    result = read_completed_job(
        tmp_path, job.fingerprint, hashlib.sha256(marker.read_bytes()).hexdigest()
    )
    assert result.rows[0]["close"] == 11  # noqa: PLR2004 -- synthetic provider observation
    with pytest.raises(ValueError, match="Korean"):
        normalize_korean_price_history(result, {})


@pytest.mark.parametrize(
    ("market", "symbol", "dataset"),
    [
        ("INDEX", "SYNTHETIC.INDX", "price_history"),
        ("CRYPTO", "SYNTHETIC-USD.CC", "price_history"),
        ("US", "SYNTHETIC.US", "research_price_history"),
        ("KR", "123456.KO", "research_price_history"),
        ("US", "SYNTHETIC.INDX", "price_history"),
        ("INDEX", "SYNTHETIC.US", "research_price_history"),
        ("CRYPTO", "SYNTHETIC.INDX", "research_price_history"),
    ],
)
def test_cross_market_dataset_or_suffix_rejected(market: str, symbol: str, dataset: str) -> None:
    with pytest.raises(ValueError, match=r"history|exchange"):
        history(market, symbol, dataset)


@pytest.mark.parametrize(
    ("tool", "market", "dataset", "params"),
    [
        (
            EOD_UNIVERSE_TOOL,
            "INDEX",
            "universe",
            {"EXCHANGE_CODE": "INDX", "delisted": "0", "fmt": "json"},
        ),
        (EOD_TOOL, "CRYPTO", "prices", {"exchange": "CC", "date": "2026-08-31", "fmt": "json"}),
    ],
)
def test_research_markets_do_not_widen_bulk_or_universe(
    tool: str, market: str, dataset: str, params: dict[str, object]
) -> None:
    with pytest.raises(ValueError, match="exchange"):
        QverisJob(
            "synthetic",
            tool,
            "eodhd",
            market,
            dataset,
            canonical_json_bytes(params).decode(),
            date(2026, 9, 6),
        )
