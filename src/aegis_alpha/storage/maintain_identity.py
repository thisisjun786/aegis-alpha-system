"""Maintenance identity: KR listings and dated US symbol lists, and the snapshot they pin.

A run builds the KR registry document (``kr_identity.build_from_workspace``) when a
committed KR identity source is newer (by its ``sl:`` link) than the newest maintenance
identity snapshot, or when no such snapshot exists. The build reads every source the
registered KR assertions cite, so registered claims rebuild with the same assertion IDs,
plus every newer source: EODHD KR symbol lists (``qveris-eodhd-exchange-symbols-*``),
KIND lists (``kind-listings-*`` tables with the ``kind.listings@1`` columns; a legacy
``korea.public_response@1`` table of the same name is skipped and listed in the report's
``skipped_sources``) and OpenDART receipts tables that hold a completed
``corp_codes`` answer. The document is registered like any other (``aas identity
register``): identical rows are reused, a conflict refuses the whole document.

The maintenance identity snapshot projects every registered assertion. Its ID is
``maintain-`` and the first 32 hex digits of the SHA-256 of the sorted assertion IDs, so
an unchanged registry names the same snapshot and a run registers a new one only when the
assertion set changed. A run that registers nothing and finds no maintenance snapshot
leaves each promotion template's own snapshot in place.

The US registry's Norgate master, FMP profiles and SEC archive are frozen inputs of the
cutover registry. Its increment is the dated EODHD US symbol list (an
``qveris-eodhd-exchange-symbols-*`` source whose rows were asked for ``US``, which the KR
build never reads): when one is linked after the newest maintenance snapshot, the run
builds ``eodhd.us_symbol_list@1`` claims (``us_symbols``) from the new lists and the lists
linked up to ``US_CONTEXT_DAYS`` before the earliest of them against the registered
claims, and registers that document. The context lists hold the earlier observations a
claim may continue from when no registered claim shows them: a renamed or newly registered
ticker's first day, which no claim covers, starts its continuation the next day. Its report
is ``us_symbols``; the KR report keys are unchanged.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Final

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage import identity, kr_identity, us_symbols
from aegis_alpha.storage.provider_collection import committed_tables
from aegis_alpha.storage.source_identity import LINK_PREFIX

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

SNAPSHOT_PREFIX: Final = "maintain-"
EODHD_PREFIX: Final = f"{kr_identity.EODHD_PROVIDER}-{kr_identity.EODHD_SHAPE}-"
KIND_PREFIX: Final = f"{kr_identity.KIND_PROVIDER}-{kr_identity.KIND_SHAPE}-"
DART_PREFIX: Final = "opendart-receipts-"
_CITED: Final = {"eodhd": "eodhd", "kind": "kind", "dart": "dart"}
US_CONTEXT_DAYS: Final = 7
_DAY_US: Final = 86_400_000_000


def _linked(workspace: Workspace) -> dict[str, int]:
    return {
        str(snapshot)[len(LINK_PREFIX) :]: int(at)
        for snapshot, at in workspace.state.execute(
            "SELECT snapshot_id, retrieved_at_us FROM source_snapshots "
            "WHERE snapshot_id LIKE 'sl:%'"
        )
    }


def _sources(workspace: Workspace) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Committed KR identity sources by build slot (``eodhd``, ``kind``, ``dart``).

    The second mapping lists the ``kind-listings`` sources skipped because their table
    lacks a column ``kind.listings@1`` reads (``kr_identity.KIND_REQUIRED``): the
    ``korea.public_response@1`` legacy import shares the source ID prefix and table name.
    """
    found: dict[str, list[str]] = {"eodhd": [], "kind": [], "dart": []}
    skipped: dict[str, list[str]] = {"kind": []}
    us = us_symbols.list_sources(workspace)
    found["eodhd"] = sorted(
        {
            table.source_id
            for table in committed_tables(workspace, EODHD_PREFIX)
            if table.store == "market"
            and table.entry["name"] == kr_identity.EODHD_TABLE
            and table.source_id not in us
        }
    )
    kind: set[str] = set()
    for table in committed_tables(workspace, KIND_PREFIX):
        if table.store != "market" or table.entry["name"] != kr_identity.KIND_TABLE:
            continue
        if set(kr_identity.KIND_REQUIRED) <= set(table.columns):
            kind.add(table.source_id)
        else:
            skipped["kind"].append(table.source_id)
    found["kind"] = sorted(kind)
    skipped["kind"].sort()
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
    return found, skipped


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


def _advance_us(
    workspace: Workspace, *, apply: bool, since: int | None, linked: dict[str, int]
) -> tuple[dict[str, object], bool]:
    """Register the per-day US symbol claims of lists linked after ``since``."""
    lists = us_symbols.list_sources(workspace)
    fresh = sorted(item for item, at in lists.items() if since is None or at > since)
    if not fresh:
        return {"new_sources": 0, "registry": "no_new_sources"}, False
    earliest = min(linked[item] for item in fresh) - US_CONTEXT_DAYS * _DAY_US
    inputs = sorted(item for item, at in lists.items() if at >= earliest)
    built = us_symbols.build_from_workspace(workspace, inputs)
    report: dict[str, object] = {"new_sources": len(fresh), "inputs": len(inputs),
                                 **built.report()}  # fmt: skip
    if not built.assertions:
        report["registry"] = "no_new_claims"
        return report, False
    raw = canonical_json_bytes(built.document())
    document = identity.decode_registry(raw, expected_file_sha256=hashlib.sha256(raw).hexdigest())
    result = identity.register_identities(workspace.state, document, apply=apply)
    report["registry"] = {"document_sha256": hashlib.sha256(raw).hexdigest(),
                          "registration": result}  # fmt: skip
    added = result.get("new")
    return report, apply and isinstance(added, dict) and any(added.values())


def advance(workspace: Workspace, *, apply: bool, now_us: int) -> dict[str, object]:
    """Register new KR and US identity claims, then pin the maintenance snapshot."""
    report: dict[str, object] = {"mode": "apply" if apply else "plan"}
    newest = _newest_snapshot(workspace)
    linked = _linked(workspace)
    sources, skipped = _sources(workspace)
    report["skipped_sources"] = skipped
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
    us, us_registered = _advance_us(workspace, apply=apply, since=since, linked=linked)
    report["us_symbols"] = us
    registered = registered or us_registered
    current = snapshot_id(workspace)
    pin = _pin(workspace, current)
    if pin is None and apply and (registered or newest is not None):
        identity.snapshot_identities(workspace.state, current, created_at_us=now_us, apply=True)
        pin = _pin(workspace, current)
        report["snapshot_registered"] = True
    report["snapshot"] = pin
    report["planned_snapshot_id"] = current
    return report
