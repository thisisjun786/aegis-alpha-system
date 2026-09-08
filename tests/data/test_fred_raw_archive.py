from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.data.fred_alfred_collector import CollectorRequest, CollectorResponse
from aegis_alpha.data.fred_alfred_rate_limit import RateLimiter
from aegis_alpha.data.fred_raw_archive import acquire_raw
from aegis_alpha.data.fred_raw_archive_cli import main

if TYPE_CHECKING:
    from collections.abc import Callable

KEY = "synthetic-private-key"
NOW = datetime(2026, 9, 6, tzinfo=UTC)


def scripted(
    case: str = "valid",
) -> tuple[Callable[[CollectorRequest, str], CollectorResponse], list[str]]:
    calls = []

    def fetch(request: CollectorRequest, _credential: str) -> CollectorResponse:
        calls.append(request.endpoint)
        if request.endpoint == "/fred/series":
            data = {
                "seriess": [
                    {
                        "id": "CPIAUCSL",
                        "title": "Synthetic CPI",
                        "frequency": "Monthly",
                        "units": "Index",
                        "seasonal_adjustment": "Seasonally Adjusted",
                        "last_updated": "2026-09-05 12:00:00-05",
                        "notes": "synthetic",
                    }
                ]
            }
        elif request.endpoint == "/fred/series/vintagedates":
            # UTC day can be tomorrow at FRED; documented max-date sentinel is accepted.
            assert request.parameters["realtime_end"] == "9999-12-31"
            data = {"count": 1, "offset": 0, "vintage_dates": ["2025-02-01"]}
        else:
            data = {
                "realtime_start": "1776-07-04",
                "realtime_end": "9999-12-31",
                "count": 1,
                "offset": 0,
                "output_type": 1,
                "observations": [
                    {
                        "date": "2025-01-01",
                        "value": ".",
                        "realtime_start": "2025-02-01",
                        "realtime_end": "9999-12-31",
                    }
                ],
            }
            if case == "echo":
                data["realtime_end"] = "2026-09-06"
            if case == "truncated":
                data["observations"] = []
            if case == "invalid":
                data["observations"] = [{"date": "2025-01-01"}]
        body = json.dumps(data).encode() if case != "leak" else KEY.encode()
        return CollectorResponse(429 if case == "rate" else 200, {}, body, NOW, NOW)

    return fetch, calls


def limiter(max_calls: int = 10) -> RateLimiter:
    return RateLimiter(max_calls=max_calls, clock=lambda: 100.0, sleep=lambda _seconds: None)


def test_full_revision_raw_capture_and_verified_reuse(tmp_path: Path) -> None:
    transport, calls = scripted()
    root = tmp_path / "fred-manual"
    result = acquire_raw(
        root,
        credential=KEY,
        max_calls=10,
        series=["CPIAUCSL"],
        as_of=NOW.date(),
        transport=transport,
        limiter=limiter(),
    )
    assert result["series_completed"] == 1
    assert result["window_row_sum"] == 1
    assert result["unique_rows"] is None
    assert calls == ["/fred/series", "/fred/series/vintagedates", "/fred/series/observations"]
    receipt = json.loads((root / "CPIAUCSL/receipt.json").read_text())
    assert all(KEY.encode() not in x.read_bytes() for x in root.rglob("*") if x.is_file())
    raw = [
        (root / a["path"]).read_bytes() for a in receipt["artifacts"] if a["path"].endswith(".raw")
    ]
    assert any(b'"value": "."' in b for b in raw)
    again = acquire_raw(
        root,
        credential=KEY,
        max_calls=10,
        series=["CPIAUCSL"],
        as_of=NOW.date(),
        transport=transport,
        limiter=limiter(),
    )
    assert again["calls_attempted"] == 0
    assert len(calls) == 3  # noqa: PLR2004 -- two requests from first invocation only
    (root / receipt["artifacts"][0]["path"]).write_bytes(b"changed")
    with pytest.raises(ValueError, match="differs"):
        acquire_raw(
            root,
            credential=KEY,
            max_calls=10,
            series=["CPIAUCSL"],
            as_of=NOW.date(),
            transport=transport,
            limiter=limiter(),
        )


@pytest.mark.parametrize("case", ["echo", "truncated", "invalid", "rate", "leak"])
def test_failed_capture_never_publishes_series_receipt(tmp_path: Path, case: str) -> None:
    transport, calls = scripted(case)
    root = tmp_path / "fred-manual"
    with pytest.raises((ValueError, RuntimeError)):
        acquire_raw(
            root,
            credential=KEY,
            max_calls=10,
            series=["CPIAUCSL"],
            as_of=NOW.date(),
            transport=transport,
            limiter=limiter(),
        )
    assert not (root / "CPIAUCSL/receipt.json").exists()
    assert len(calls) <= 3  # noqa: PLR2004 -- one metadata and one observations call, no retries


def test_budget_exhaustion_prevents_next_call(tmp_path: Path) -> None:
    transport, calls = scripted()
    with pytest.raises(ValueError, match="budget insufficient"):
        acquire_raw(
            tmp_path / "fred-manual",
            credential=KEY,
            max_calls=1,
            series=["CPIAUCSL"],
            as_of=NOW.date(),
            transport=transport,
            limiter=limiter(1),
        )
    assert calls == []


def test_plan_has_no_data_writes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = tmp_path / "fred-manual"
    assert main(["--output-root", str(root), "--max-calls", "100"]) == 0
    assert not root.exists()
    assert json.loads(capsys.readouterr().out)["raw_only"] is True


def test_vintage_limit_splits_real_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from aegis_alpha.data import fred_raw_archive as module  # noqa: PLC0415 -- monkeypatch owner

    monkeypatch.setattr(module, "VINTAGES_PER_WINDOW", 2)
    base, _calls = scripted()
    bounds = []
    values = ["2025-01-01", "2025-01-02", "2025-01-03"]

    def fetch(request: CollectorRequest, key: str) -> CollectorResponse:
        if request.endpoint == "/fred/series":
            return base(request, key)
        if request.endpoint == "/fred/series/vintagedates":
            offset = int(request.parameters["offset"])
            document = {
                "count": len(values),
                "offset": offset,
                "vintage_dates": values[offset : offset + 2],
            }
        else:
            start = request.parameters["realtime_start"]
            end = request.parameters["realtime_end"]
            bounds.append((start, end))
            document = {
                "count": 0,
                "offset": 0,
                "observations": [],
                "output_type": 1,
                "realtime_start": start,
                "realtime_end": end,
            }
        return CollectorResponse(200, {}, json.dumps(document).encode(), NOW, NOW)

    result = acquire_raw(
        tmp_path / "fred-manual",
        credential=KEY,
        max_calls=10,
        series=["CPIAUCSL"],
        as_of=NOW.date(),
        transport=fetch,
        limiter=limiter(),
    )
    assert bounds == [("1776-07-04", "2025-01-02"), ("2025-01-03", "9999-12-31")]
    assert result["calls_attempted"] == 5  # noqa: PLR2004 -- metadata + two vintage + two observation


def test_missing_vintage_history_is_incomplete(tmp_path: Path) -> None:
    base, _calls = scripted()

    def fetch(request: CollectorRequest, key: str) -> CollectorResponse:
        if request.endpoint.endswith("vintagedates"):
            return CollectorResponse(
                200, {}, b'{"count":0,"offset":0,"vintage_dates":[]}', NOW, NOW
            )
        return base(request, key)

    with pytest.raises(ValueError, match="no vintage history"):
        acquire_raw(
            tmp_path / "fred-manual",
            credential=KEY,
            max_calls=10,
            series=["CPIAUCSL"],
            as_of=NOW.date(),
            transport=fetch,
            limiter=limiter(),
        )
    assert not (tmp_path / "fred-manual/CPIAUCSL/receipt.json").exists()


def test_cli_pins_observation_day_for_resume(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (
        main(
            [
                "--output-root",
                str(tmp_path / "fred-manual"),
                "--max-calls",
                "100",
                "--as-of",
                "2026-09-05",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["as_of"] == "2026-09-05"
