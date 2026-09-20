"""Canonical request CONTENT store. Financial execution admission belongs to engine/application.

Two root shapes are stored here, and the store never converts one into the other. An
aas-backtest-request-v1 describes an executable run and keeps every refusal the engine
puts on it. An aas-research-run-v1 describes a declared uncertified research run over
reference observations, which no executable request can describe. Both are held to the
same content identity: exactly canonical bytes, a hash over those bytes, and bindings
that agree with the bundle the request is registered against.
"""

from __future__ import annotations

import hashlib
import re
from typing import TYPE_CHECKING, cast

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage.input_pins import (
    HASH_FORMAT,
    InputBinding,
    InputBundleRef,
    binding_document,
    decode_pin_document,
    parse_bindings,
    read_input_bundle,
)
from aegis_alpha.storage.run_schema import (
    BACKTEST_REQUEST_SCHEMA,
    RESEARCH_REQUEST_SCHEMA,
    require_request_schema,
    require_run_schema,
)
from aegis_alpha.storage.state import atomic

if TYPE_CHECKING:
    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace

# A declared run states its mode in its own bytes. A stored declaration that says
# anything else would be a research record claiming a status nobody granted.
RESEARCH_EXECUTION_MODE = "research-uncertified"
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
# The declared research root, mirrored from aegis_alpha.application.research_run so this
# store stays free of an application import. A change to that contract's root belongs in
# the same change as this set; an unlisted field would otherwise be stored unread.
_RESEARCH_ROOT = frozenset(
    {
        "schema_version",
        "execution_mode",
        "strategy",
        "observations",
        "calendar",
        "membership",
        "period",
        "history",
        "execution",
        "instrument_map",
        "conventions",
        "uncertainty",
        "semantics",
        "unsettled",
    }
)


def request_schema(body: dict[str, object]) -> str:
    """Name the one request contract this document is, from its own root shape.

    The two roots are disjoint, so a document is one contract or neither. Nothing here
    guesses: a root that is not exactly one of them is refused rather than defaulted.
    """
    if body.keys() == _ROOT:
        if body["schema"] != BACKTEST_REQUEST_SCHEMA or body["hash_format"] != HASH_FORMAT:
            raise ValueError("invalid backtest request root/schema")
        return BACKTEST_REQUEST_SCHEMA
    if body.keys() == _RESEARCH_ROOT:
        if body["schema_version"] != RESEARCH_REQUEST_SCHEMA:
            raise ValueError("invalid research run root/schema")
        if body["execution_mode"] != RESEARCH_EXECUTION_MODE:
            raise ValueError("a stored research run must declare " + RESEARCH_EXECUTION_MODE)
        return RESEARCH_REQUEST_SCHEMA
    raise ValueError("invalid backtest request root/schema")


# The exact pin shape the declaration spells. Checked here rather than assumed, so a
# binding is never built from a document that is missing or renaming a field.
_MEMBERSHIP = {"kind", "id", "version", "hash"}


def _pin(value: object, field: str, keys: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or value.keys() != keys:
        raise ValueError("research request " + field + " has missing or unknown fields")
    return cast("dict[str, object]", value)


def research_bindings(body: dict[str, object]) -> list[dict[str, object]]:
    """The exact bundle a declared research run must be registered against.

    A declaration carries no bindings array, so the tie to its bundle is derived from
    the pins it does name, and the membership is the only one the binding vocabulary can
    express. The observation panels cannot be bound, because there is no role for
    reference observations and inventing one would put adjusted reference data in the
    namespace the executable price roles use. The calendar cannot be bound either: it is
    a declared name over the panel's own dates rather than a published generation, so
    there is no pin to authenticate. Both stay covered by the declaration's own content
    hash. The bundle is held to exactly this, so a declaration cannot be filed under
    inputs it never named.
    """
    membership = _pin(body["membership"], "membership", _MEMBERSHIP)
    if membership.get("kind") != "membership":
        raise ValueError("research membership pin must name the membership kind")
    return [
        binding_document(
            InputBinding(
                "membership",
                0,
                "membership",
                membership["id"],  # ty: ignore[invalid-argument-type]
                membership["version"],  # ty: ignore[invalid-argument-type]
                membership["hash"],  # ty: ignore[invalid-argument-type]
            )
        ),
    ]


def _validate(
    workspace: Workspace, bundle: InputBundleRef, raw: bytes, digest: str, budget: ComputeBudget
) -> str:
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("request hash must be lowercase SHA-256")
    body = decode_pin_document(raw)
    schema = request_schema(body)
    # The installed add-on has to be able to record a run under this contract before its
    # request becomes a stored fact; otherwise the refusal only arrives at open_run, on
    # an installation that already accepted the document.
    require_request_schema(workspace, schema)
    if canonical_json_bytes(body) != raw:
        raise ValueError("request bytes must be exactly canonical")
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("request content hash mismatch")
    executable = schema == BACKTEST_REQUEST_SCHEMA
    bindings = (
        [binding_document(item) for item in parse_bindings(body["bindings"])]
        if executable
        else research_bindings(body)
    )
    stored = decode_pin_document(read_input_bundle(workspace, bundle, budget=budget))
    if (executable and body["bindings"] != bindings) or bindings != stored["bindings"]:
        raise ValueError("request binding order/complete bundle content mismatch")
    return schema


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
