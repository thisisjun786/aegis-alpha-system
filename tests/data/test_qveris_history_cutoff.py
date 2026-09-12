from __future__ import annotations

from datetime import date

import pytest

from aegis_alpha.data.qveris_contracts import EOD_HISTORY_JSON_TOOL, EOD_HISTORY_TOOL, QverisJob
from aegis_alpha.data.qveris_payloads import validate_payload
from aegis_alpha.data.serialization import canonical_json_bytes


def json_history_job(observation_date: date = date(2026, 1, 2)) -> QverisJob:
    return QverisJob(
        "synthetic-cutoff",
        EOD_HISTORY_JSON_TOOL,
        "eodhd",
        "US",
        "price_history",
        canonical_json_bytes(
            {"symbol": "AAPL.US", "fmt": "json", "from": "2026-01-01", "order": "a"}
        ).decode(),
        observation_date,
    )


def history_row(day: str) -> dict[str, object]:
    return {
        "date": day,
        "open": 1,
        "high": 2,
        "low": 0,
        "close": 1,
        "adjusted_close": 1,
        "volume": 100,
    }


def success_response(rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "success": True,
        "execution_id": "synthetic",
        "result": {"status_code": 200, "data": rows},
    }


def test_openended_accepts_row_exactly_on_observation_date() -> None:
    """Given an open-ended JSON history job pinned to 2026-01-02, when retrieved
    on 2026-01-06, then a row exactly on the observation date is accepted."""
    job = json_history_job()
    rows = [history_row("2026-01-02")]
    shape = validate_payload(
        job, job.parameters, success_response(rows), retrieved_on=date(2026, 1, 6)
    )
    assert shape.rows == 1


def test_openended_rejects_row_after_pinned_cutoff() -> None:
    """Given an open-ended JSON history job pinned to 2026-01-02, when retrieved
    on 2026-01-06, then a row after the pinned observation cutoff is rejected."""
    job = json_history_job()
    rows = [history_row("2026-01-05")]
    with pytest.raises(ValueError, match="outside request"):
        validate_payload(job, job.parameters, success_response(rows), retrieved_on=date(2026, 1, 6))


def test_openended_retrieval_date_earlier_than_cutoff_rejects_future_observations() -> None:
    """Given an open-ended JSON history job pinned to 2026-01-06, when retrieved
    on 2026-01-02, then a row between retrieval and cutoff is rejected."""
    job = json_history_job(date(2026, 1, 6))
    rows = [history_row("2026-01-05")]
    with pytest.raises(ValueError, match="outside request"):
        validate_payload(job, job.parameters, success_response(rows), retrieved_on=date(2026, 1, 2))


def test_openended_requires_retrieval_date() -> None:
    """Given an open-ended JSON history job, when no retrieval date is supplied,
    then validation fails before inspecting rows."""
    job = json_history_job()
    with pytest.raises(ValueError, match="open-ended history requires"):
        validate_payload(job, job.parameters, success_response([history_row("2026-01-02")]))


def test_explicit_to_uses_to_bound_not_retrieval_date() -> None:
    """Given a history job with an explicit 'to' parameter, when the retrieval
    date is earlier than 'to', then the explicit 'to' bound governs."""
    job = QverisJob(
        "synthetic-explicit-to",
        EOD_HISTORY_TOOL,
        "eodhd",
        "US",
        "price_history",
        canonical_json_bytes(
            {
                "symbol": "AAPL.US",
                "period": "d",
                "order": "a",
                "from": "2026-01-01",
                "to": "2026-01-05",
            }
        ).decode(),
        date(2026, 1, 5),
    )
    rows = [history_row("2026-01-05")]
    shape = validate_payload(
        job, job.parameters, success_response(rows), retrieved_on=date(2026, 1, 2)
    )
    assert shape.rows == 1
