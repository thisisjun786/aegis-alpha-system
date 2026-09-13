"""Verify logical references as well as each database's own integrity."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.storage.input_pins import ConventionPin, read_convention
from aegis_alpha.storage.market import verify_generation
from aegis_alpha.storage.raw import verify_raw
from aegis_alpha.storage.strategies import verify_strategy_content
from aegis_alpha.storage.strategy_import import verify_strategy_imports

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace


def verify_workspace(workspace: Workspace) -> dict[str, object]:  # noqa: C901, PLR0912 -- full cross-store verification boundary
    if workspace.strategies is None:
        raise ValueError("strategy store is required for complete verification")
    for connection in (workspace.state, workspace.strategies):
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("SQLite integrity check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise ValueError("SQLite foreign key check failed")
    versions = workspace.state.execute(
        "SELECT dataset_id,version,generation_id,chain_hash,row_count,manifest_hash "
        "FROM dataset_versions WHERE status='committed'"
    ).fetchall()
    for version in versions:
        marker = verify_generation(workspace.market, version["generation_id"])
        for field in ("dataset_id", "version", "chain_hash", "row_count"):
            if marker[field] != version[field]:
                raise ValueError("market generation/catalog mismatch")
        operation = workspace.state.execute(
            "SELECT phase,request_hash,target_id,expected_parent FROM storage_operations "
            "WHERE operation_id=?",
            (marker["operation_id"],),
        ).fetchone()
        if operation is None or tuple(operation) != (
            "COMPLETED",
            marker["request_hash"],
            marker["generation_id"],
            marker["parent_id"],
        ):
            raise ValueError("committed generation has no matching completed intent")
        sources = workspace.state.execute(
            "SELECT source_snapshot_id FROM dataset_sources WHERE dataset_id=? AND version=?",
            (version["dataset_id"], version["version"]),
        ).fetchall()
        if not sources:
            raise ValueError("committed dataset has no source lineage")
    for source in workspace.state.execute(
        "SELECT relative_path,byte_hash,size_bytes FROM source_files"
    ):
        verify_raw(
            workspace.paths.raw, source["relative_path"], source["byte_hash"], source["size_bytes"]
        )
    strategies = workspace.strategies.execute(
        "SELECT strategy_id,version,raw_sha256 FROM strategy_versions"
    ).fetchall()
    for strategy in strategies:
        verify_strategy_content(workspace.strategies, *strategy)
    verify_strategy_imports(workspace)
    for convention in workspace.state.execute(
        "SELECT kind,convention_id,version,content_hash FROM conventions"
    ):
        read_convention(workspace.state, ConventionPin(*convention))
    for row in workspace.state.execute(
        "SELECT run_id,relative_path,size_bytes,content_hash FROM artifacts"
    ):
        relative = row["run_id"] + "/" + row["relative_path"]
        with (
            DescriptorTree.open_path(workspace.paths.runs) as tree,
            tree.binary_reader(relative, require_single_link=True) as source,
        ):
            hasher = hashlib.sha256()
            size = 0
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                hasher.update(chunk)
                size += len(chunk)
            if hasher.hexdigest() != row["content_hash"] or size != row["size_bytes"]:
                raise ValueError("run artifact hash/size mismatch")
    pending = workspace.state.execute(
        "SELECT count(*) FROM storage_operations WHERE phase='PREPARED'"
    ).fetchone()[0]
    untracked = workspace.market.execute("SELECT generation_id FROM market_generations").fetchall()
    visible = {row["generation_id"] for row in versions}
    report: dict[str, object] = {
        "verified": True,
        "dataset_versions": len(versions),
        "strategy_versions": len(strategies),
        "pending_operations": pending,
        "orphan_generations": [row[0] for row in untracked if row[0] not in visible],
    }
    from aegis_alpha.storage.source_library import verify_sources  # noqa: PLC0415

    sources = verify_sources(workspace)
    if sources is not None:
        report["source_library"] = sources
    return report
