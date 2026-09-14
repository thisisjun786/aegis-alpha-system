"""Verify logical references as well as each database's own integrity."""

from __future__ import annotations

import hashlib
from fractions import Fraction
from typing import TYPE_CHECKING

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.storage.input_pins import (
    ConventionPin,
    DefinitionPin,
    InputBundleRef,
    read_convention,
    read_definition,
    read_input_bundle,
)
from aegis_alpha.storage.market import generation_chain
from aegis_alpha.storage.market_inputs import verify_sealed_publication
from aegis_alpha.storage.membership_pins import (
    IdentityPin,
    UniversePin,
    read_membership_pins,
)
from aegis_alpha.storage.raw import verify_raw
from aegis_alpha.storage.run_schema import inspect_run_schema
from aegis_alpha.storage.strategies import verify_strategy_content
from aegis_alpha.storage.strategy_import import verify_strategy_imports

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace


def verify_workspace(  # noqa: C901, PLR0912 -- full cross-store verification boundary
    workspace: Workspace, *, budget: ComputeBudget | None = None
) -> dict[str, object]:
    """Verify under a caller-owned allocation, retaining the serial default when omitted."""
    budget = budget or ComputeBudget(Fraction(1), 512 * 1024 * 1024)
    if workspace.strategies is None:
        raise ValueError("strategy store is required for complete verification")
    for connection in (workspace.state, workspace.strategies):
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("SQLite integrity check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise ValueError("SQLite foreign key check failed")
    # Bound variable-width header enumeration before handing each exact pin to
    # the shared aggregate/content verifier. A root alone cannot exceed 1 MiB.
    for sql in (
        "SELECT EXISTS(SELECT 1 FROM identity_snapshots WHERE length(CAST(snapshot_id AS BLOB))>?)",
        (
            "SELECT EXISTS(SELECT 1 FROM universe_versions "
            "WHERE length(CAST(universe_id AS BLOB))+length(CAST(version AS BLOB))>?)"
        ),
    ):
        if workspace.state.execute(sql, (1024 * 1024,)).fetchone()[0]:
            raise ValueError("membership root exceeds document byte limit")
    for header in workspace.state.execute(
        "SELECT snapshot_id,content_hash FROM identity_snapshots"
    ):
        read_membership_pins(
            workspace.state,
            IdentityPin(*header),
            None,
            max_materialization_bytes=64 * 1024 * 1024,
        )
    for header in workspace.state.execute(
        "SELECT universe_id,version,content_hash FROM universe_versions"
    ):
        read_membership_pins(
            workspace.state,
            None,
            UniversePin(*header),
            max_materialization_bytes=64 * 1024 * 1024,
        )
    size = workspace.state.execute(
        "SELECT count(*)*2048 + coalesce(sum(32*(length(dataset_id)+length(version)+"
        "length(generation_id)+coalesce(length(parent_generation_id),0))),0) "
        "FROM dataset_versions WHERE status='committed'"
    ).fetchone()[0]
    if size > budget.memory_limit_bytes // 8:
        raise ComputeResourceError("publication catalog exceeds materialization budget")
    versions = workspace.state.execute(
        "SELECT dataset_id,version,generation_id,chain_hash,manifest_hash,parent_generation_id "
        "FROM dataset_versions WHERE status='committed'"
    ).fetchall()
    committed = {version["generation_id"] for version in versions}
    parents = {version["parent_generation_id"] for version in versions}
    covered: set[str] = set()
    for generation in sorted(committed - parents):
        verify_sealed_publication(workspace, generation, budget=budget)
        covered.update(
            str(marker["generation_id"])
            for marker in generation_chain(workspace.market, generation)
        )
    if covered != committed:
        raise ValueError("committed publications are not covered by verified leaf chains")
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
    _verify_input_documents(workspace, budget)
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


def _verify_input_documents(workspace: Workspace, budget: ComputeBudget) -> None:
    from aegis_alpha.storage.backtest_requests import (  # noqa: PLC0415 -- optional content owner
        read_backtest_request,
    )
    from aegis_alpha.storage.market_inputs import verify_proxy_publications  # noqa: PLC0415

    status = inspect_run_schema(workspace)
    schemas = {"aas-derived-definition-v1": "derived", "aas-ensemble-membership-v1": "membership"}
    for row in workspace.state.execute(
        "SELECT name,version,content_hash,record_schema FROM feature_contracts"
    ):
        if row[3] in schemas:
            read_definition(workspace, DefinitionPin(schemas[row[3]], *row[:3]), budget=budget)
        elif row[3] != "aas-market-rowset-v1":
            raise ValueError("unsupported feature definition schema")
    verify_proxy_publications(workspace, budget=budget)
    for row in workspace.state.execute("SELECT bundle_id,content_hash FROM input_bundles"):
        pin = InputBundleRef(*row)
        read_input_bundle(workspace, pin, budget=budget)
    if status.state == "complete":
        for row in workspace.state.execute(
            "SELECT r.bundle_id,b.content_hash,r.request_hash FROM backtest_requests r "
            "JOIN input_bundles b ON b.bundle_id=r.bundle_id"
        ):
            read_backtest_request(
                workspace,
                InputBundleRef(row[0], row[1]),
                expected_request_hash=row[2],
                budget=budget,
            )
