"""Provider-list composition and immutable universe marker publication."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Final
from urllib.parse import urlparse

from aegis_alpha.data.fmp_collector import FmpCollector, publish_bundle
from aegis_alpha.data.fmp_collector_state import marker_path
from aegis_alpha.data.fmp_universe import (
    ListWalkSource,
    RawListPage,
    build_universe_manifest,
    universe_manifest_document,
)
from aegis_alpha.data.fmp_windows import parse_universe_manifest
from aegis_alpha.data.serialization import canonical_json_bytes

ACTIVE_LIST_ENDPOINT: Final = "/stable/actively-trading-list"
DELISTED_LIST_ENDPOINT: Final = "/stable/delisted-companies"
_HTTP_SUCCESS_MIN: Final = 200
_HTTP_REDIRECT_MIN: Final = 300


def _source_uri_path(source_uri: object) -> str:
    if not isinstance(source_uri, str) or not source_uri:
        return ""
    return urlparse(source_uri).path


def _is_successful_status(status: object) -> bool:
    return isinstance(status, int) and _HTTP_SUCCESS_MIN <= status < _HTTP_REDIRECT_MIN


def terminal_successful_list_pages(
    provenance_records: Iterable[bytes],
    bodies: Mapping[str, bytes],
    endpoint: str,
) -> tuple[RawListPage, ...]:
    """Select the terminal 2xx body for each exact logical list request."""

    selected: dict[str, Mapping[str, object]] = {}
    order: list[str] = []
    for raw in provenance_records:
        record = json.loads(raw)
        if _source_uri_path(record.get("source_uri")) != endpoint or not _is_successful_status(
            record.get("status_code")
        ):
            continue
        fingerprint = record.get("request_fingerprint")
        if not isinstance(fingerprint, str) or not fingerprint:
            continue
        if fingerprint not in selected:
            order.append(fingerprint)
        selected[fingerprint] = record
    return tuple(
        RawListPage(
            body=bodies[str(selected[fingerprint]["content_sha256"])],
            retrieved_at_utc=datetime.fromisoformat(str(selected[fingerprint]["retrieved_at_utc"])),
        )
        for fingerprint in order
    )


def _walk_source(collector: FmpCollector, endpoint: str) -> ListWalkSource:
    walk = collector.walk_list_endpoint(endpoint=endpoint, identity_key="symbol")
    bodies = {hashlib.sha256(body).hexdigest(): body for body in collector.captured_bodies}
    return ListWalkSource(
        endpoint=endpoint,
        walk=walk,
        pages=terminal_successful_list_pages(collector.iter_provenance_records(), bodies, endpoint),
    )


def publish_universe_manifest(
    collector: FmpCollector,
    run_id: str,
    plan_id: str,
    destination: Path,
    generated_at: datetime,
) -> dict[str, object]:
    manifest = build_universe_manifest(
        generated_at_utc=generated_at,
        active=_walk_source(collector, ACTIVE_LIST_ENDPOINT),
        delisted=_walk_source(collector, DELISTED_LIST_ENDPOINT),
    )
    payload = canonical_json_bytes(universe_manifest_document(manifest))
    parse_universe_manifest(json.loads(payload))
    ledger = collector._limiter.ledger()  # noqa: SLF001
    marker: dict[str, object] = {
        "attempt_ledger_sha256": collector.attempt_ledger_sha256,
        "plan_id": plan_id,
        "run_id": run_id,
        "artifacts": [{"path": str(destination), "sha256": hashlib.sha256(payload).hexdigest()}],
        "advances": [],
        "usage": {
            "calls_attempted": ledger.calls_attempted,
            "bytes_received": ledger.bytes_received,
            "retry_after_waits": ledger.retry_after_waits,
            "rate_limited_attempts": ledger.rate_limited_attempts,
        },
    }
    collector.require_bound_approval()
    publish_bundle(
        [(destination, payload), (marker_path(collector, run_id), canonical_json_bytes(marker))]
    )
    return marker
