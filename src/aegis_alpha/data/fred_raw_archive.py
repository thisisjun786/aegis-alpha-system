"""Manual, key-authenticated raw FRED acquisition; no standing authority.

Never writes the signed collector's roots, DB, catalog, watermarks, eligibility
or daily ledger. No scheduling or automatic retries. Explicit invocation budget
and raw-only receipts cannot authorize the signed recurring pipeline.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import secrets
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from http import HTTPStatus
from pathlib import Path
from typing import cast

from aegis_alpha.data.fred_alfred_collector import (
    CollectorRequest,
    Transport,
    assert_credential_absent,
    make_https_transport,
    parse_series_record,
)
from aegis_alpha.data.fred_alfred_rate_limit import RateLimiter
from aegis_alpha.data.fred_alfred_series import (
    MACRO_SERIES_IDS,
    series_universe_sha256,
    validate_series_universe,
)
from aegis_alpha.data.sec_evidence import open_directory, publish_bytes, read_bytes
from aegis_alpha.data.serialization import canonical_json_bytes

PAGE_LIMIT = 100000
MAX_PAGES = 1000
VINTAGES_PER_WINDOW = 1000
REALTIME_START = "1776-07-04"
REALTIME_END = "9999-12-31"


def plan_archive(root: Path, series: Sequence[str], max_calls: int) -> dict[str, object]:
    selected = validate_series_universe(series)
    if not root.is_absolute() or ".." in root.parts or "fred-manual" not in root.parts:
        raise ValueError("manual FRED root must be absolute under a distinct fred-manual directory")
    if any((parent / ".git").exists() for parent in (root, *root.parents)):
        raise ValueError("manual FRED archive must be outside Git")
    if type(max_calls) is not int or max_calls < 1:
        raise ValueError("manual FRED max_calls must be positive")
    return {
        "contract": "aas-fred-raw-plan/v1",
        "series_ids": list(selected),
        "series_universe_sha256": series_universe_sha256(selected),
        "catalog_sha256": series_universe_sha256(MACRO_SERIES_IDS),
        "max_calls": max_calls,
        "minimum_calls": 3 * len(selected),
        "endpoints": ["/fred/series", "/fred/series/vintagedates", "/fred/series/observations"],
        "raw_only": True,
        "scheduled": False,
    }


class RawSession:
    def __init__(
        self, root: Path, credential: str, limiter: RateLimiter, transport: Transport
    ) -> None:
        self.root = root
        self.credential = credential
        self.limiter = limiter
        self.transport = transport
        self.artifacts: list[dict[str, object]] = []
        self.request_index = 0

    def save(self, relative: str, payload: bytes) -> None:
        assert_credential_absent(self.credential, payload)
        publish_bytes(self.root / relative, payload)
        self.artifacts.append(
            {"path": relative, "sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}
        )

    def fetch(self, request: CollectorRequest) -> Mapping[str, object]:
        self.limiter.before_request()
        index = self.request_index
        self.request_index += 1
        context = {
            "source_uri": request.source_uri,
            "request_fingerprint": request.request_fingerprint,
            "requested_at_utc": datetime.now(UTC).isoformat(),
            "index": index,
        }
        self.save(f"{index:05d}.request.json", canonical_json_bytes(context))
        try:
            response = self.transport(request, self.credential)
        except Exception:
            self.limiter.after_response(byte_count=0)
            raise
        self.limiter.after_response(byte_count=len(response.body))
        assert_credential_absent(
            self.credential, response.body, canonical_json_bytes(response.headers)
        )
        self.save(f"blobs/{response.content_sha256}.raw", response.body)
        metadata = {
            **context,
            "status": response.status_code,
            "headers": dict(response.headers),
            "content_sha256": response.content_sha256,
            "bytes": len(response.body),
            "response_requested_at_utc": response.requested_at_utc.isoformat(),
            "retrieved_at_utc": response.retrieved_at_utc.isoformat(),
        }
        self.save(f"{index:05d}.response.json", canonical_json_bytes(metadata))
        if response.status_code != HTTPStatus.OK:
            raise ValueError(
                f"FRED HTTP {response.status_code}; manual archive stopped without retry"
            )
        length = response.headers.get("content-length")
        if length is not None and int(length) != len(response.body):
            raise ValueError("FRED response length mismatch")
        document = json.loads(response.body)
        if not isinstance(document, dict):
            raise TypeError("FRED response must be an object")
        return document


def _reuse(root: Path, series_id: str, as_of: date) -> dict[str, object]:
    receipt = json.loads(read_bytes(root / series_id / "receipt.json"))
    if (
        receipt.get("contract") not in {"aas-fred-raw-series/v1", "aas-fred-raw-series/v2"}
        or receipt.get("series_id") != series_id
        or receipt.get("observation_end") != as_of.isoformat()
        or receipt.get("catalog_sha256") != series_universe_sha256(MACRO_SERIES_IDS)
    ):
        raise ValueError("FRED raw receipt scope differs")
    with open_directory(root) as tree:
        for artifact in receipt["artifacts"]:
            payload = tree.read_bytes(artifact["path"], max_bytes=32 * 1024 * 1024)
            if (
                len(payload) != artifact["bytes"]
                or hashlib.sha256(payload).hexdigest() != artifact["sha256"]
            ):
                raise ValueError("FRED raw artifact differs from receipt")
    return {**receipt, "reused": True}


def _observation_window(
    session: RawSession, series_id: str, as_of: date, bounds: tuple[str, str]
) -> tuple[int, int]:
    offset = 0
    total = None
    for page in range(MAX_PAGES):
        request = CollectorRequest(
            "/fred/series/observations",
            {
                "file_type": "json",
                "series_id": series_id,
                "output_type": "1",
                "realtime_start": bounds[0],
                "realtime_end": bounds[1],
                "observation_end": as_of.isoformat(),
                "limit": str(PAGE_LIMIT),
                "offset": str(offset),
                "sort_order": "asc",
                "units": "lin",
            },
            series_id,
        )
        document = session.fetch(request)
        rows = document.get("observations")
        count = document.get("count")
        if (
            type(count) is not int
            or count < 0
            or document.get("offset") != offset
            or document.get("realtime_start") != bounds[0]
            or document.get("realtime_end") != bounds[1]
            or document.get("output_type") != 1
            or not isinstance(rows, list)
        ):
            raise ValueError("FRED observation scope or pagination echo differs")
        if total is not None and total != count:
            raise ValueError("FRED observation count changed during pagination")
        total = count
        if len(rows) != min(PAGE_LIMIT, total - offset):
            raise ValueError("FRED observation page is truncated or oversized")
        for row in rows:
            if (
                not isinstance(row, dict)
                or not {"date", "value", "realtime_start", "realtime_end"} <= row.keys()
            ):
                raise ValueError("FRED observation is missing date/value/vintage fields")
            record = cast("Mapping[str, object]", row)
            start, end = record["realtime_start"], record["realtime_end"]
            if not isinstance(start, str) or not isinstance(end, str):
                raise TypeError("FRED observation vintage bounds must be date text")
            if date.fromisoformat(start) > date.fromisoformat(bounds[1]) or date.fromisoformat(
                end
            ) < date.fromisoformat(bounds[0]):
                raise ValueError("FRED observation does not intersect its requested window")
        offset += len(rows)
        if offset == total:
            return total, page + 1
    raise ValueError("FRED raw page count exceeds limit")


def vintage_windows(values: Sequence[date]) -> tuple[tuple[str, str], ...]:
    if list(values) != sorted(set(values)):
        raise ValueError("FRED vintage dates must be unique and ordered")
    windows = []
    for index in range(0, len(values), VINTAGES_PER_WINDOW):
        start = REALTIME_START if index == 0 else values[index].isoformat()
        following = index + VINTAGES_PER_WINDOW
        end = (
            (values[following] - timedelta(days=1)).isoformat()
            if following < len(values)
            else REALTIME_END
        )
        windows.append((start, end))
    return tuple(windows)


def _observations(session: RawSession, series_id: str, as_of: date) -> list[dict[str, object]]:
    dates: list[date] = []
    expected = None
    for _page in range(MAX_PAGES):
        result = session.fetch(
            CollectorRequest(
                "/fred/series/vintagedates",
                {
                    "series_id": series_id,
                    "file_type": "json",
                    "limit": str(VINTAGES_PER_WINDOW),
                    "offset": str(len(dates)),
                    "sort_order": "asc",
                    "realtime_start": REALTIME_START,
                    "realtime_end": REALTIME_END,
                },
                series_id,
            )
        )
        count = result.get("count")
        rows = result.get("vintage_dates")
        if (
            type(count) is not int
            or count < len(dates)
            or not isinstance(rows, list)
            or result.get("offset") != len(dates)
            or (expected is not None and expected != count)
            or len(rows) != min(VINTAGES_PER_WINDOW, count - len(dates))
        ):
            raise ValueError("FRED vintage pagination changed or was truncated")
        expected = count
        for item in rows:
            if not isinstance(item, str):
                raise TypeError("FRED vintage date must be text")
            value = date.fromisoformat(item)
            if value.isoformat() != item or value > as_of:
                raise ValueError("FRED vintage date is outside the frozen capture day")
            dates.append(value)
        if len(dates) == count:
            break
    else:
        raise ValueError("FRED vintage page count exceeds limit")
    if not dates:
        raise ValueError("FRED returned no vintage history; series remains incomplete")
    windows: list[dict[str, object]] = []
    for bounds in vintage_windows(dates):
        count, pages = _observation_window(session, series_id, as_of, bounds)
        windows.append(
            {
                "realtime_start": bounds[0],
                "realtime_end": bounds[1],
                "returned_rows": count,
                "pages": pages,
            }
        )
    return windows


def acquire_raw(  # noqa: PLR0913 -- explicit manual invocation boundary
    root: Path,
    *,
    credential: str,
    max_calls: int,
    as_of: date,
    series: Sequence[str] = MACRO_SERIES_IDS,
    transport: Transport | None = None,
    limiter: RateLimiter | None = None,
) -> dict[str, object]:
    plan = plan_archive(root, series, max_calls)
    if not credential:
        raise ValueError("FRED_API_KEY is required")
    pacing = limiter or RateLimiter(max_calls=max_calls, clock=time.monotonic, sleep=time.sleep)
    fetch = make_https_transport(timeout_seconds=120.0) if transport is None else transport
    outcomes = []
    with open_directory(root, create=True) as tree:
        try:
            fcntl.flock(tree.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("manual FRED output is already in use") from None
        for series_id in validate_series_universe(series):
            if tree.exists(f"{series_id}/receipt.json"):
                outcomes.append(_reuse(root, series_id, as_of))
                continue
            if pacing.calls_remaining < 3:  # noqa: PLR2004 -- metadata, vintages, observations
                raise ValueError(f"FRED budget insufficient; {len(outcomes)} series completed")
            relative = f"partial/{series_id}-{secrets.token_hex(8)}"
            session = RawSession(root / relative, credential, pacing, fetch)
            meta = session.fetch(
                CollectorRequest(
                    "/fred/series", {"file_type": "json", "series_id": series_id}, series_id
                )
            )
            parse_series_record(meta, series_id=series_id)
            windows = _observations(session, series_id, as_of)
            receipt = {
                "contract": "aas-fred-raw-series/v2",
                "series_id": series_id,
                "observation_end": as_of.isoformat(),
                "window_row_sum": sum(cast("int", window["returned_rows"]) for window in windows),
                "unique_rows": None,
                "windows": windows,
                "catalog_sha256": plan["catalog_sha256"],
                "raw_only": True,
                "catalog_registered": False,
                "completed_at_utc": datetime.now(UTC).isoformat(),
                "artifacts": [{**a, "path": f"{relative}/{a['path']}"} for a in session.artifacts],
            }
            encoded = canonical_json_bytes(receipt)
            assert_credential_absent(credential, encoded)
            publish_bytes(root / series_id / "receipt.json", encoded)
            outcomes.append(receipt)
    return {
        "raw_only": True,
        "catalog_registered": False,
        "series_completed": len(outcomes),
        "window_row_sum": sum(
            cast("int", r.get("window_row_sum", r.get("row_count"))) for r in outcomes
        ),
        "unique_rows": None,
        "series_reused": sum(bool(r.get("reused")) for r in outcomes),
        "calls_attempted": pacing.calls_attempted,
        "series_ids": plan["series_ids"],
    }
