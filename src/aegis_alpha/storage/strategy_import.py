"""Complete strategy registration across its private store and the state journal."""

from __future__ import annotations

import hashlib
from pathlib import Path

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.engine.bundle import load_bundle
from aegis_alpha.storage.strategies import LineageSpec
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
    prepare_operation(
        workspace.state,
        operation_id=operation_id,
        kind="strategy_import",
        request_hash=bundle.source_sha256,
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
    complete_operation(workspace.state, operation_id, bundle.source_sha256)
    return result
