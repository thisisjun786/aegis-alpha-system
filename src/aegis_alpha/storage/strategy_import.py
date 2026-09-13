"""Complete strategy registration across its private store and the state journal."""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.engine.bundle import load_bundle
from aegis_alpha.storage.strategies import (
    LineageSpec,
    read_strategy_lineage,
    strategy_request_hash,
    verify_strategy_content,
)
from aegis_alpha.storage.workspace import Workspace


def register_strategy(  # noqa: PLR0913 -- preserve explicit bundle pins plus optional lineage
    workspace: Workspace,
    file: Path,
    sha256: str,
    strategy_id: str,
    version: str,
    *,
    lineage: LineageSpec | None = None,
) -> dict[str, object]:
    from aegis_alpha.storage.state import complete_operation, prepare_operation  # noqa: PLC0415
    from aegis_alpha.storage.strategies import import_strategy  # noqa: PLC0415

    if workspace.strategies is None:
        raise ValueError("private strategy database is unavailable")
    with DescriptorTree.open_path(file.parent) as tree:
        raw = tree.read_bytes(file.name, max_bytes=16 * 1024 * 1024)
    bundle = load_bundle(raw, sha256, strategy_id, version)
    operation_id = (
        "strategy-"
        + hashlib.sha256(
            (strategy_id + "\x00" + version + "\x00" + bundle.source_sha256).encode()
        ).hexdigest()
    )
    # Keep the key independent of lineage so changed retries conflict with the same intent.
    request_hash = strategy_request_hash(bundle, lineage)
    prepare_operation(
        workspace.state,
        operation_id=operation_id,
        kind="strategy_import",
        request_hash=request_hash,
        target_id=strategy_id + ":" + version,
        expected_parent=None,
        payload_hash=bundle.source_sha256,
    )
    result = import_strategy(
        workspace.strategies,
        raw,
        sha256,
        strategy_id,
        version,
        operation_id,
        lineage=lineage,
    )
    complete_operation(workspace.state, operation_id, request_hash)
    return result


def verify_strategy_import(workspace: Workspace, operation: sqlite3.Row) -> bool:
    """SELECT-only receipt/intent/content check; an absent private commit stays pending."""
    if workspace.strategies is None:
        return False
    op_id = operation["operation_id"]
    marker = workspace.strategies.execute(
        "SELECT request_hash,strategy_id,version FROM strategy_imports WHERE operation_id=?",
        (op_id,),
    ).fetchone()
    if marker is None:
        return False
    if (
        marker["request_hash"] != operation["request_hash"]
        or marker["strategy_id"] + ":" + marker["version"] != operation["target_id"]
        or operation["expected_parent"] is not None
    ):
        raise ValueError("strategy receipt does not match prepared operation")
    # Integrity and journal completion check content, not execution eligibility.
    bundle = verify_strategy_content(
        workspace.strategies, marker["strategy_id"], marker["version"], operation["payload_hash"]
    )
    lineage = read_strategy_lineage(workspace.strategies, marker["strategy_id"], marker["version"])
    if strategy_request_hash(bundle, lineage) != operation["request_hash"]:
        raise ValueError("strategy stored lineage does not match prepared operation")
    return True


def verify_strategy_imports(workspace: Workspace) -> None:
    """Check both directions of the private evidence/journal graph without recovery."""
    if workspace.strategies is None:
        raise ValueError("private strategy database is unavailable")
    operations = {
        row["operation_id"]: row
        for row in workspace.state.execute(
            "SELECT * FROM storage_operations WHERE kind='strategy_import'"
        )
    }
    for marker in workspace.strategies.execute("SELECT operation_id FROM strategy_imports"):
        operation = operations.get(marker["operation_id"])
        if operation is None or operation["phase"] not in {"PREPARED", "COMPLETED"}:
            raise ValueError("strategy receipt has no matching active import intent")
    for operation in operations.values():
        committed = verify_strategy_import(workspace, operation)
        if not committed and operation["phase"] == "COMPLETED":
            raise ValueError("completed strategy intent has no private receipt")
    if workspace.strategies.execute(
        "SELECT 1 FROM strategy_versions v WHERE NOT EXISTS "
        "(SELECT 1 FROM strategy_imports i "
        "WHERE i.strategy_id=v.strategy_id AND i.version=v.version)"
    ).fetchone():
        raise ValueError("strategy version has no private import receipt")


def recover_strategy_import(workspace: Workspace, operation: sqlite3.Row) -> bool:
    """Complete only a receipt whose stored bytes and lineage match its durable intent."""
    from aegis_alpha.storage.state import complete_operation  # noqa: PLC0415

    if not verify_strategy_import(workspace, operation):
        return False
    complete_operation(workspace.state, operation["operation_id"], operation["request_hash"])
    return True
