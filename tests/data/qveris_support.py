"""Synthetic completed Qveris jobs: a scripted gateway answers each request with given rows."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_client import QverisResponse
from aegis_alpha.data.qveris_contracts import (
    EOD_HISTORY_JSON_TOOL,
    EOD_TOOL,
    QverisJob,
    object_value,
)
from aegis_alpha.data.serialization import canonical_json_bytes
from tests.data.test_qveris_acquisition import FakeQveris

OBSERVED = date(2026, 9, 6)
KR_IDENTITY = {
    "123456.KO": {
        "instrument_id": "synthetic-kr-etf",
        "venue": "KO",
        "instrument_type": "ETF",
        "currency": "KRW",
        "name": "Synthetic ETF",
    },
    "AAA.US": {
        "instrument_id": "synthetic-us-stock",
        "venue": "US",
        "instrument_type": "Common Stock",
        "currency": "USD",
    },
}


def identity_bytes(identities: Mapping[str, object] | None = None) -> bytes:
    return canonical_json_bytes({"schema_version": 1, "identities": identities or KR_IDENTITY})


class ScriptedQveris(FakeQveris):
    """Answer every execute with the rows scripted for its exact parameters."""

    def __init__(self, rows: Mapping[str, object], account_key: str | None = None) -> None:
        super().__init__()
        self.rows = rows
        if account_key is not None:
            # The account lease is host-wide; a key per raw root keeps concurrent runs apart.
            self.account_key = account_key

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        response = super().request(path, body=body, query=query)
        if path == "/tools/by-ids":
            document = response.document()
            results = document["results"]
            assert isinstance(results, list)
            if str(results[0]["tool_id"]).startswith("eodhd."):
                results[0]["provider_id"] = "eodhd"
            return replace(response, body=canonical_json_bytes(document))
        return response

    def _execute(self, body: dict[str, object], query: dict[str, str | int]) -> dict[str, object]:
        document = super()._execute(body, query)
        key = canonical_json_bytes(object_value(body["parameters"])).decode()
        if key in self.rows and document["success"] is True:
            object_value(document["result"])["data"] = self.rows[key]
        return document


def history_job(symbol: str = "123456.KO", market: str = "KR") -> QverisJob:
    return QverisJob(
        f"history-{symbol}",
        EOD_HISTORY_JSON_TOOL,
        "eodhd",
        market,
        "price_history",
        canonical_json_bytes(
            {"symbol": symbol, "fmt": "json", "from": "2026-08-01", "order": "a"}
        ).decode(),
        OBSERVED,
    )


def fx_job(pair: str = "USDKRW") -> QverisJob:
    return QverisJob(
        f"fx-{pair}",
        EOD_HISTORY_JSON_TOOL,
        "eodhd",
        "FX",
        "fx_history",
        canonical_json_bytes(
            {"symbol": f"{pair}.FOREX", "fmt": "json", "from": "2026-08-01", "order": "a"}
        ).decode(),
        OBSERVED,
    )


def bulk_job(
    exchange: str = "US",
    dataset: str = "prices",
    day: str = "2026-08-31",
    observed: date = OBSERVED,
) -> QverisJob:
    parameters: dict[str, object] = {"exchange": exchange, "date": day, "fmt": "json"}
    if dataset != "prices":
        parameters["type"] = dataset
    return QverisJob(
        f"bulk-{exchange}-{dataset}-{day}",
        EOD_TOOL,
        "eodhd",
        "US" if exchange == "US" else "KR",
        dataset,
        canonical_json_bytes(parameters).decode(),
        observed,
    )


def bar(day: str, **values: object) -> dict[str, object]:
    return {
        "date": day,
        "open": 10,
        "high": 12,
        "low": 9,
        "close": 11,
        "adjusted_close": 10.5,
        "volume": 100,
        **values,
    }


def account_key(root: Path) -> str:
    return "synthetic-" + hashlib.sha256(str(root.absolute()).encode()).hexdigest()


def complete(root: Path, job: QverisJob, rows: object, *, warning: bool = False) -> str:
    """Acquire ``job`` through the scripted gateway; return its fingerprint."""
    client = ScriptedQveris({job.parameters_json: rows}, account_key(root))
    if warning:
        client.price = Decimal(0)
        client.failure = "included"
    acquire_jobs((job,), root, client)
    return job.fingerprint
