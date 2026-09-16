"""Canonical request CONTENT store. Financial execution admission belongs to engine/application."""

from __future__ import annotations

import hashlib
import re
from typing import TYPE_CHECKING

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage.input_pins import (
    HASH_FORMAT,
    InputBundleRef,
    binding_document,
    decode_pin_document,
    parse_bindings,
    read_input_bundle,
)
from aegis_alpha.storage.run_schema import require_run_schema
from aegis_alpha.storage.state import atomic

if TYPE_CHECKING:
    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace

_ROOT = frozenset(
    {
        "schema",
        "hash_format",
        "strategy",
        "bindings",
        "refs",
        "price_inputs",
        "macro_inputs",
        "derived_inputs",
        "proxy_rules",
        "period",
        "history",
        "cutoff",
        "decision_latency_us",
        "explicit_decision_dates",
        "account",
        "comparison",
        "envelope",
        "conventions",
        "engine",
        "environment",
    }
)


def _validate(
    workspace: Workspace, bundle: InputBundleRef, raw: bytes, digest: str, budget: ComputeBudget
) -> None:
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("request hash must be lowercase SHA-256")
    body = decode_pin_document(raw)
    if (
        body.keys() != _ROOT
        or body["schema"] != "aas-backtest-request-v1"
        or body["hash_format"] != HASH_FORMAT
    ):
        raise ValueError("invalid backtest request root/schema")
    if canonical_json_bytes(body) != raw:
        raise ValueError("request bytes must be exactly canonical")
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("request content hash mismatch")
    bindings = [binding_document(item) for item in parse_bindings(body["bindings"])]
    stored = decode_pin_document(read_input_bundle(workspace, bundle, budget=budget))
    if body["bindings"] != bindings or bindings != stored["bindings"]:
        raise ValueError("request binding order/complete bundle content mismatch")


def register_backtest_request(
    workspace: Workspace,
    bundle: InputBundleRef,
    canonical_request: bytes,
    *,
    expected_request_hash: str,
    budget: ComputeBudget,
) -> str:
    require_run_schema(workspace)
    with atomic(workspace.state):
        _validate(workspace, bundle, canonical_request, expected_request_hash, budget)
        previous = workspace.state.execute(
            "SELECT 1 FROM backtest_requests WHERE bundle_id=?", (bundle.bundle_id,)
        ).fetchone()
        if previous is None:
            workspace.state.execute(
                "INSERT INTO backtest_requests VALUES (?,?,?)",
                (bundle.bundle_id, canonical_request, expected_request_hash),
            )
        if (
            read_backtest_request(
                workspace, bundle, expected_request_hash=expected_request_hash, budget=budget
            )
            != canonical_request
        ):
            raise ValueError("bundle already identifies different request bytes")
    return expected_request_hash


def read_backtest_request(
    workspace: Workspace,
    bundle: InputBundleRef,
    *,
    expected_request_hash: str,
    budget: ComputeBudget,
) -> bytes:
    require_run_schema(workspace)
    size = workspace.state.execute(
        "SELECT length(request_bytes) FROM backtest_requests WHERE bundle_id=?", (bundle.bundle_id,)
    ).fetchone()
    if size is None or size[0] > 1024 * 1024:
        raise ValueError("request missing or exceeds byte bound")
    row = workspace.state.execute(
        "SELECT request_bytes,request_hash FROM backtest_requests WHERE bundle_id=?",
        (bundle.bundle_id,),
    ).fetchone()
    if row[1] != expected_request_hash:
        raise ValueError("stored request hash mismatch")
    _validate(workspace, bundle, row[0], expected_request_hash, budget)
    return row[0]
