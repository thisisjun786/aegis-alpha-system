"""Synthetic provider-neutral helpers for the AAS-DATA-008C collector tests.

Every transport here is built from a committed synthetic fixture. No test in
this module family opens a socket, reads a credential, or incurs cost.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from aegis_alpha.data.finimpulse_collector import (
    REQUIRED_GB_GATES,
    CollectorConfig,
    GateEvidence,
    IdentityExport,
    RawCall,
    SyntheticExecution,
    SyntheticTransport,
    Transport,
    TransportFailureError,
    canonical_json_bytes,
    instant_literal,
    load_gate_evidence,
    load_identity_export,
    sha256_hex,
    universe_sha256,
)

FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "provider_neutral"
    / "finimpulse_collector_snapshots.json"
)
SYNTHETIC_CREDENTIAL = "synthetic-not-a-real-token"
OBSERVED_AT = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)
LATER_OBSERVED_AT = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)
GATE_EXPIRY = datetime(2026, 12, 31, 12, 0, tzinfo=UTC)


def load_fixture() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def snapshot_items(name: str, symbol: str) -> list[dict[str, Any]]:
    entry = load_fixture()["snapshots"][name][symbol]
    return list(entry["items"])


def build_response(
    symbol: str,
    items: Sequence[Mapping[str, Any]],
    *,
    cost: float,
    total_count: int | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    return {
        "task_id": f"synthetic-{symbol.lower()}",
        "status_code": 20000,
        "status_message": "OK",
        "cost": cost,
        "data": {
            "symbol": symbol,
            "limit": limit,
            "offset": 0,
            "types": ["eps_trend", "eps_revisions"],
        },
        "result": {
            "symbol": symbol,
            "total_count": len(items) if total_count is None else total_count,
            "items_count": len(items),
            "items": [dict(item) for item in items],
        },
    }


def build_raw_call(
    symbol: str,
    response: Mapping[str, Any],
    *,
    headers: Mapping[str, str] | None = None,
    limit: int = 20,
    requested_at: datetime = OBSERVED_AT,
) -> RawCall:
    body = {
        "symbol": symbol,
        "types": ["eps_trend", "eps_revisions"],
        "limit": limit,
        "offset": 0,
        "sort_by": [{"selector": "date", "desc": True}],
    }
    return RawCall(
        symbol=symbol,
        request_body=body,
        requested_at_utc=requested_at,
        retrieved_at_utc=requested_at,
        http_status=200,
        headers=dict(headers or {}),
        body_bytes=canonical_json_bytes(dict(response)),
    )


class RecordingTransport:
    """Serve committed synthetic responses and record every attempted symbol."""

    def __init__(  # noqa: PLR0913 - each option varies one synthetic transport behavior
        self,
        snapshot: str,
        *,
        limit: int = 20,
        overrides: Mapping[str, Mapping[str, Any]] | None = None,
        failures: Mapping[str, int] | None = None,
        error_statuses: Mapping[str, Sequence[int]] | None = None,
        error_cost: float | None = None,
        transport_errors: Mapping[str, int] | None = None,
        requested_at: datetime = OBSERVED_AT,
    ) -> None:
        self._entries = dict(load_fixture()["snapshots"][snapshot])
        self._limit = limit
        self._overrides = dict(overrides or {})
        self._failures = dict(failures or {})
        self._error_statuses = {key: list(value) for key, value in (error_statuses or {}).items()}
        self._error_cost = error_cost
        self._transport_errors = dict(transport_errors or {})
        self._requested_at = requested_at
        self.calls: list[str] = []

    def __call__(self, body: Mapping[str, object], credential: str) -> RawCall:
        symbol = str(body["symbol"])
        self.calls.append(symbol)
        if not credential:
            raise AssertionError("synthetic transport requires a non-empty credential")
        pending_transport = self._transport_errors.get(symbol, 0)
        if pending_transport > 0:
            self._transport_errors[symbol] = pending_transport - 1
            raise TransportFailureError(
                "synthetic transport failure", "transport_error", charged=False
            )
        pending_statuses = self._error_statuses.get(symbol)
        if pending_statuses:
            status = pending_statuses.pop(0)
            return build_error_call(
                symbol,
                status,
                limit=self._limit,
                requested_at=self._requested_at,
                cost=self._error_cost,
            )
        remaining = self._failures.get(symbol, 0)
        if remaining > 0:
            self._failures[symbol] = remaining - 1
            return build_error_call(
                symbol,
                429,
                limit=self._limit,
                requested_at=self._requested_at,
            )
        override = self._overrides.get(symbol)
        if override is not None:
            return build_raw_call(
                symbol,
                override,
                headers={"x-ratelimit-limit": "2000", "x-ratelimit-remaining": "1900"},
                limit=self._limit,
                requested_at=self._requested_at,
            )
        entry = self._entries[symbol]
        response = build_response(
            symbol,
            entry["items"],
            cost=float(entry["cost"]),
            limit=self._limit,
        )
        return build_raw_call(
            symbol,
            response,
            headers=entry.get("headers"),
            limit=self._limit,
            requested_at=self._requested_at,
        )


def make_config(  # noqa: PLR0913 - each argument pins one frozen config input
    symbols: Sequence[str] = ("AAPL", "PLAB"),
    *,
    budget_usd: str = "0.10",
    page_limit: int = 20,
    predecessor_snapshot_id: str | None = None,
    identity_as_of: datetime | None = OBSERVED_AT,
    identity_export_sha256: str | None = None,
    max_retries: int = 3,
) -> CollectorConfig:
    return CollectorConfig(
        universe=tuple(symbols),
        budget_usd=Decimal(budget_usd),
        page_limit=page_limit,
        max_retries=max_retries,
        collector_code_version="synthetic-test",
        identity_export_sha256=(
            identity_export_sha256
            if identity_export_sha256 is not None
            else identity_export_digest(symbols)
        ),
        identity_as_of=identity_as_of,
        predecessor_snapshot_id=predecessor_snapshot_id,
    )


def synthetic_pair(
    transport: Transport,
) -> tuple[SyntheticTransport, SyntheticExecution]:
    """Wrap a fixture transport as an offline transport plus its capability.

    The capability is issued by the offline transport itself, so tests exercise
    the same non-forgeable boundary production callers face.
    """

    offline = SyntheticTransport(transport)
    return offline, offline.capability("AAS-DATA-008C G-A synthetic fixture run")


def no_sleep(_seconds: float) -> None:
    """Keep retry tests instant without touching real time."""


def receipt_view(receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Round-trip a receipt through JSON so assertions read typed evidence.

    The round trip also proves the receipt is canonically serializable.
    """

    return json.loads(canonical_json_bytes(dict(receipt)))


def build_error_call(
    symbol: str,
    status: int,
    *,
    limit: int = 20,
    requested_at: datetime = OBSERVED_AT,
    cost: float | None = None,
) -> RawCall:
    """Build a non-200 provider response that still carries a real body."""

    body: dict[str, Any] = {
        "status_code": status * 100,
        "status_message": "ERROR",
        "task_id": f"synthetic-error-{symbol.lower()}",
    }
    if cost is not None:
        body["cost"] = cost
    call = build_raw_call(symbol, body, limit=limit, requested_at=requested_at)
    return RawCall(
        symbol=call.symbol,
        request_body=call.request_body,
        requested_at_utc=call.requested_at_utc,
        retrieved_at_utc=call.retrieved_at_utc,
        http_status=status,
        headers={"x-ratelimit-limit": "2000", "x-ratelimit-remaining": "1900"},
        body_bytes=call.body_bytes,
    )


def identity_export_document(
    mapping: Mapping[str, Sequence[str]],
    *,
    as_of: datetime = OBSERVED_AT,
) -> dict[str, Any]:
    return {
        "as_of_utc": instant_literal(as_of),
        "mappings": {symbol: list(values) for symbol, values in mapping.items()},
        "namespace": "ticker",
        "provider": "finimpulse",
        "schema_version": 1,
    }


def write_identity_export(
    tmp_path: Path,
    mapping: Mapping[str, Sequence[str]],
    *,
    as_of: datetime = OBSERVED_AT,
    name: str = "identity-export.json",
) -> tuple[Path, str]:
    """Write a synthetic 007 export and return its path and exact digest."""

    path = tmp_path / name
    payload = canonical_json_bytes(identity_export_document(mapping, as_of=as_of))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path, sha256_hex(payload)


def make_identity_export(
    mapping: Mapping[str, Sequence[str]] | None = None,
    *,
    symbols: Sequence[str] = ("AAPL", "PLAB"),
    as_of: datetime = OBSERVED_AT,
) -> IdentityExport:
    """Build the in-memory export the collector resolves against."""

    resolved = (
        {symbol: [] for symbol in symbols}
        if mapping is None
        else {symbol: list(values) for symbol, values in mapping.items()}
    )
    document = identity_export_document(resolved, as_of=as_of)
    payload = canonical_json_bytes(document)
    return IdentityExport(
        export_sha256=sha256_hex(payload),
        as_of_utc=as_of,
        mappings={symbol: tuple(values) for symbol, values in resolved.items()},
    )


def identity_export_digest(symbols: Sequence[str] = ("AAPL", "PLAB")) -> str:
    return make_identity_export(symbols=symbols).export_sha256


def gate_document(
    *,
    symbols: Sequence[str] = ("AAPL", "PLAB"),
    budget_usd: str = "0.10",
    expires_at: datetime = GATE_EXPIRY,
    decisions: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    resolved = dict.fromkeys(REQUIRED_GB_GATES, "PERMITTED")
    if decisions is not None:
        resolved.update(decisions)
    return {
        "budget_usd": budget_usd,
        "decisions": [
            {
                "decided_at_utc": instant_literal(OBSERVED_AT),
                "decision": decision,
                "evidence_ref": f"owner-record/{gate}",
                "gate": gate,
            }
            for gate, decision in resolved.items()
        ],
        "expires_at_utc": instant_literal(expires_at),
        "universe_sha256": universe_sha256(list(symbols)),
    }


def write_gate_evidence(  # noqa: PLR0913 - each argument varies one gate input
    tmp_path: Path,
    *,
    name: str = "gb-gates.json",
    symbols: Sequence[str] = ("AAPL", "PLAB"),
    budget_usd: str = "0.10",
    expires_at: datetime = GATE_EXPIRY,
    decisions: Mapping[str, str] | None = None,
) -> tuple[Path, str]:
    """Write a synthetic G-B gate artifact and return its path and digest."""

    path = tmp_path / name
    payload = canonical_json_bytes(
        gate_document(
            symbols=symbols,
            budget_usd=budget_usd,
            expires_at=expires_at,
            decisions=decisions,
        )
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path, sha256_hex(payload)


def make_gate_evidence(
    tmp_path: Path,
    *,
    symbols: Sequence[str] = ("AAPL", "PLAB"),
    budget_usd: str = "0.10",
    expires_at: datetime = GATE_EXPIRY,
    decisions: Mapping[str, str] | None = None,
) -> GateEvidence:
    path, digest = write_gate_evidence(
        tmp_path,
        symbols=symbols,
        budget_usd=budget_usd,
        expires_at=expires_at,
        decisions=decisions,
    )
    return load_gate_evidence(path, expected_sha256=digest)


def load_export(path: Path, digest: str) -> IdentityExport:
    return load_identity_export(path, expected_sha256=digest)
