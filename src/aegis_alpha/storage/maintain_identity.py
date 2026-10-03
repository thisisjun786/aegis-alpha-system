"""Maintenance identity: the KR registry advanced by new listings, and the snapshot it pins.

A run builds the KR registry document (``kr_identity.build_from_workspace``) when a
committed KR identity source is newer (by its ``sl:`` link) than the newest maintenance
identity snapshot, or when no such snapshot exists. The build reads every source the
registered KR assertions cite, so registered claims rebuild with the same assertion IDs,
plus every newer source: EODHD KR symbol lists (``qveris-eodhd-exchange-symbols-*``),
KIND lists (``kind-listings-*``) and OpenDART receipts tables that hold a completed
``corp_codes`` answer. The document is registered like any other (``aas identity
register``): identical rows are reused, a conflict refuses the whole document.

The maintenance identity snapshot projects every registered assertion. Its ID is
``maintain-`` and the first 32 hex digits of the SHA-256 of the sorted assertion IDs, so
an unchanged registry names the same snapshot and a run registers a new one only when the
assertion set changed. A run that registers nothing and finds no maintenance snapshot
leaves each promotion template's own snapshot in place. US identity sources (Norgate
master, FMP profiles, the SEC archive) are frozen inputs of the cutover registry and have
no maintenance increment.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Final

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage import identity, kr_identity
from aegis_alpha.storage.provider_collection import committed_tables
from aegis_alpha.storage.source_identity import LINK_PREFIX

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

SNAPSHOT_PREFIX: Final = "maintain-"
EODHD_PREFIX: Final = f"{kr_identity.EODHD_PROVIDER}-{kr_identity.EODHD_SHAPE}-"
KIND_PREFIX: Final = f"{kr_identity.KIND_PROVIDER}-{kr_identity.KIND_SHAPE}-"
DART_PREFIX: Final = "opendart-receipts-"
_CITED: Final = {"eodhd": "eodhd", "kind": "kind", "dart": "dart"}


def _linked(workspace: Workspace) -> dict[str, int]:
    return {
        str(snapshot)[len(LINK_PREFIX) :]: int(at)
        for snapshot, at in workspace.state.execute(
            "SELECT snapshot_id, retrieved_at_us FROM source_snapshots "
            "WHERE snapshot_id LIKE 'sl:%'"
        )
    }


def _sources(workspace: Workspace) -> dict[str, list[str]]:
    """Committed KR identity sources by build slot (``eodhd``, ``kind``, ``dart``)."""
    found: dict[str, list[str]] = {"eodhd": [], "kind": [], "dart": []}
    for slot, prefix, name in (
        ("eodhd", EODHD_PREFIX, kr_identity.EODHD_TABLE),
        ("kind", KIND_PREFIX, kr_identity.KIND_TABLE),
    ):
        found[slot] = sorted(
            {
                table.source_id
                for table in committed_tables(workspace, prefix)
                if table.store == "market" and table.entry["name"] == name
            }
        )
    for table in committed_tables(workspace, DART_PREFIX):
        if table.store != "market" or table.entry["name"] != kr_identity.DART_TABLE:
            continue
        target = '"' + str(table.entry["target"]).replace('"', '""') + '"'
        row = workspace.market.execute(
            f"SELECT count(*) FROM {target} WHERE endpoint='corp_codes' AND outcome='COMPLETED'"  # noqa: S608 -- quoted manifest identifier
        ).fetchone()
        if row is not None and int(row[0]):
            found["dart"].append(table.source_id)
    found["dart"].sort()
    return found


def _cited(workspace: Workspace) -> dict[str, set[str]]:
    cited: dict[str, set[str]] = {"eodhd": set(), "kind": set(), "dart": set()}
    for provider, source in workspace.state.execute(
        "SELECT DISTINCT a.provider, a.source_snapshot_id FROM identity_assertions a "
        "WHERE a.provider='kind' OR (a.provider='dart' AND a.namespace='issuer') "
        "OR (a.provider='eodhd' AND (a.namespace='krx_short_code' OR (a.namespace="
        "'eodhd_symbol' AND (a.token LIKE '%.KO' OR a.token LIKE '%.KQ'))))"
    ):
        slot = _CITED.get(str(provider))
        if slot is not None and str(source).startswith(LINK_PREFIX):
            cited[slot].add(str(source)[len(LINK_PREFIX) :])
    return cited


def snapshot_id(workspace: Workspace) -> str:
    """The maintenance snapshot ID of the registered assertion set."""
    ids = [str(row[0]) for row in workspace.state.execute(
        "SELECT assertion_id FROM identity_assertions ORDER BY assertion_id")]  # fmt: skip
    return SNAPSHOT_PREFIX + hashlib.sha256(canonical_json_bytes(ids)).hexdigest()[:32]


def _newest_snapshot(workspace: Workspace) -> tuple[str, int] | None:
    row = workspace.state.execute(
        "SELECT snapshot_id, created_at_us FROM identity_snapshots WHERE snapshot_id LIKE ? "
        "AND snapshot_id NOT LIKE '%#%' ORDER BY created_at_us DESC, snapshot_id LIMIT 1",
        (SNAPSHOT_PREFIX + "%",),
    ).fetchone()
    return None if row is None else (str(row[0]), int(row[1]))


def _pin(workspace: Workspace, snapshot: str) -> dict[str, str] | None:
    row = workspace.state.execute(
        "SELECT content_hash FROM identity_snapshots WHERE snapshot_id=?", (snapshot,)
    ).fetchone()
    return None if row is None else {"snapshot_id": snapshot, "content_hash": str(row[0])}


def advance(workspace: Workspace, *, apply: bool, now_us: int) -> dict[str, object]:
    """Register new KR identity claims, then pin the maintenance snapshot of the registry."""
    report: dict[str, object] = {"mode": "apply" if apply else "plan"}
    newest = _newest_snapshot(workspace)
    linked = _linked(workspace)
    sources = _sources(workspace)
    since = None if newest is None else newest[1]
    fresh = {
        slot: [item for item in items if since is None or linked.get(item, 0) > since]
        for slot, items in sources.items()
    }
    report["new_sources"] = {slot: len(items) for slot, items in fresh.items()}
    registered = False
    if not any(fresh.values()):
        report["registry"] = "no_new_sources"
    elif not sources["eodhd"]:
        report["registry"] = "no_eodhd_symbol_lists"
    else:
        cited = _cited(workspace)
        inputs = {
            slot: sorted(cited[slot] | set(fresh[slot]) | ({*sources[slot]} if slot == "eodhd"
                                                            else set()))
            for slot in sources
        }  # fmt: skip
        built = kr_identity.build_from_workspace(
            workspace, eodhd=inputs["eodhd"], kind=inputs["kind"], dart=inputs["dart"]
        )
        raw = built.raw()
        document = identity.decode_registry(raw, expected_file_sha256=built.sha256())
        result = identity.register_identities(workspace.state, document, apply=apply)
        report["registry"] = {
            "inputs": {slot: len(items) for slot, items in inputs.items()},
            "document_sha256": built.sha256(),
            **{key: built.report()[key] for key in ("issuers", "instruments", "assertions",
                                                    "unresolved_count", "withdrawn_count")},
            "registration": result,
        }  # fmt: skip
        added = result.get("new")
        registered = apply and isinstance(added, dict) and any(added.values())
    current = snapshot_id(workspace)
    pin = _pin(workspace, current)
    if pin is None and apply and (registered or newest is not None):
        identity.snapshot_identities(workspace.state, current, created_at_us=now_us, apply=True)
        pin = _pin(workspace, current)
        report["snapshot_registered"] = True
    report["snapshot"] = pin
    report["planned_snapshot_id"] = current
    return report
