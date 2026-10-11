"""Maintenance of the KIND chain: the mapper a chain continues with and the shapes it reads.

Sources are synthetic KIND lists (``tests.storage.kr_identity_support``) committed the way
``aas identity kr-import`` commits them, and a synthetic ``kind-listings`` table in the
shape of the ``korea.public_response@1`` legacy import; every code and name is made up.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.storage import maintain_identity
from aegis_alpha.storage.identity import decode_registry, register_identities, snapshot_identities
from aegis_alpha.storage.kr_identity import build_from_workspace, eodhd_unit, import_unit, kind_unit
from aegis_alpha.storage.maintain_promotion import ROUTES, promote_datasets, unmapped
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.kr_identity_support import (
    commit_legacy_kind_listing,
    eodhd_job,
    isin,
    kind_listing,
    symbol,
)

ROUTE = next(route for route in ROUTES if route.dataset_id == "classifications.kr.kind")
OBSERVED = {"rule": "source_column@1", "basis": "revision", "input": "observed_at", "args": {}}
CODE = "100010"


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _kind(ws: Workspace, day: str, list_id: str = "kind-kospi") -> dict[str, str]:
    """Commit a KIND list of the synthetic company collected on ``day``; its spec pin."""
    rows = [("합성전자", CODE, "1975-06-11")]
    unit = kind_unit(*kind_listing(rows, list_id=list_id, retrieved=f"{day}T06:05:48Z"))
    committed = import_unit(ws, unit)
    return {
        "source_id": unit.content.source_id,
        "source_sha256": unit.content.sha256,
        "table": "listings",
        "digest": str(committed["digest"]),
    }


def _identity(ws: Workspace, kind: dict[str, str]) -> dict[str, str]:
    """Register the synthetic company from an EODHD list and ``kind``; snapshot it."""
    job = eodhd_unit(*eodhd_job([symbol(CODE, isin("710000100"))]))
    import_unit(ws, job)
    registry = build_from_workspace(ws, eodhd=[job.content.source_id], kind=[kind["source_id"]])
    document = decode_registry(registry.raw(), expected_file_sha256=registry.sha256())
    register_identities(ws.state, document, apply=True)
    snapshot = snapshot_identities(ws.state, "kr", created_at_us=5, apply=True)
    return {"snapshot_id": str(snapshot["snapshot_id"]),
            "content_hash": str(snapshot["content_hash"])}  # fmt: skip


def _publish(ws: Workspace, name: str, pin: dict[str, str], identity: dict[str, str],
             parent: str | None) -> str:  # fmt: skip
    """The operator's generation of ``classifications.kr.kind`` under mapper ``name``."""
    document = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": "classifications", "dataset_id": ROUTE.dataset_id,
                   "parent": parent},
        "sources": [pin],
        "mapper": {"name": name, "args": {}},
        "partition": None,
        "time_rules": {"available_at_us": OBSERVED, "revision_known_at_us": OBSERVED},
        "decimal_rule": {},
        "quality_rules": [],
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": identity,
    }  # fmt: skip
    raw = json.dumps(document, sort_keys=True).encode()
    result = promote(ws, raw, hashlib.sha256(raw).hexdigest(), apply=True)
    assert result["published"] is True, result.get("refusals")
    return str(result["generation_id"])


def _run(report: dict[str, object]) -> dict[str, object]:
    (run,) = cast("list[dict[str, object]]", report["datasets"])
    return run


def _head_mapper(ws: Workspace) -> str:
    row = ws.state.execute(
        "SELECT normalizer_version FROM dataset_versions WHERE dataset_id=? "
        "AND status='committed' ORDER BY sequence DESC LIMIT 1",
        (ROUTE.dataset_id,),
    ).fetchone()
    return str(row[0])


def _sources(run: dict[str, object]) -> list[tuple[object, object]]:
    items = cast("list[dict[str, object]]", run["sources"])
    return [(item["source_id"], item["status"]) for item in items]


def test_a_kind_chain_continues_with_the_mapper_of_its_latest_generation(ws: Workspace) -> None:
    # No chain yet: the route reports its own mapper and starts nothing.
    first = _kind(ws, "2026-09-06")
    assert _run(promote_datasets(ws, apply=False, now_us=1, routes=[ROUTE]))["mapper"] == (
        "kind.industry@2"
    )
    identity = _identity(ws, first)
    head = _publish(ws, "kind.industry@1", first, identity, None)

    # A chain an install published under @1 is continued under @1, not left as no_template.
    second = _kind(ws, "2026-09-07")
    run = _run(promote_datasets(ws, apply=True, now_us=2, routes=[ROUTE]))
    assert (run["mapper"], run["status"]) == ("kind.industry@1", "advanced")
    assert "no_template" not in cast("dict[str, int]", run["skipped"])
    assert _sources(run) == [(first["source_id"], "unchanged"), (second["source_id"], "promoted")]
    assert _head_mapper(ws) == "kind.industry@1"
    children = ws.state.execute(
        "SELECT count(*) FROM dataset_versions WHERE parent_generation_id=?", (head,)
    ).fetchone()
    assert children[0] == 1

    # Once the operator publishes a @2 generation, maintenance continues the chain with @2.
    third = _kind(ws, "2026-09-08")
    parent = ws.state.execute(
        "SELECT generation_id FROM dataset_versions WHERE dataset_id=? AND status='committed' "
        "ORDER BY sequence DESC LIMIT 1",
        (ROUTE.dataset_id,),
    ).fetchone()[0]
    _publish(ws, "kind.industry@2", third, identity, str(parent))
    fourth = _kind(ws, "2026-09-09")
    run = _run(promote_datasets(ws, apply=True, now_us=3, routes=[ROUTE]))
    assert (run["mapper"], run["status"]) == ("kind.industry@2", "advanced")
    assert _sources(run) == [(third["source_id"], "unchanged"), (fourth["source_id"], "promoted")]
    assert _head_mapper(ws) == "kind.industry@2"


def test_legacy_shaped_kind_listings_are_skipped_and_reported(ws: Workspace) -> None:
    legacy = commit_legacy_kind_listing(ws, CODE)
    assert legacy.startswith("kind-listings-")
    first = _kind(ws, "2026-09-06")
    # The identity build reads the kr-import list and lists the legacy one as skipped.
    planned = maintain_identity.advance(ws, apply=False, now_us=1)
    assert planned["skipped_sources"] == {"kind": [legacy]}
    assert planned["new_sources"] == {"eodhd": 0, "kind": 1, "dart": 0}
    identity = _identity(ws, first)
    applied = maintain_identity.advance(ws, apply=True, now_us=2)
    assert applied["skipped_sources"] == {"kind": [legacy]}
    registry = cast("dict[str, object]", applied["registry"])
    assert registry["inputs"] == {"eodhd": 1, "kind": 1, "dart": 0}
    _publish(ws, "kind.industry@2", first, identity, None)

    # The route plans the new kr-import list and counts the legacy table as another shape.
    second = _kind(ws, "2026-09-07")
    run = _run(promote_datasets(ws, apply=False, now_us=3, routes=[ROUTE]))
    assert run["status"] == "planned"
    assert cast("dict[str, int]", run["skipped"])["shape"] == 1
    assert _sources(run) == [(first["source_id"], "planned"), (second["source_id"], "planned")]
    assert unmapped(ws) == {"status.kr.kind": 2}
