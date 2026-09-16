"""Verify logical references as well as each database's own integrity."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from fractions import Fraction
from typing import TYPE_CHECKING, cast

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


def _verify_membership(workspace: Workspace, allowance: int) -> None:
    """Reconstruct every stored membership document under the caller's allowance."""
    for header in workspace.state.execute(
        "SELECT snapshot_id,content_hash FROM identity_snapshots"
    ):
        read_membership_pins(
            workspace.state, IdentityPin(*header), None, max_materialization_bytes=allowance
        )
    for header in workspace.state.execute(
        "SELECT universe_id,version,content_hash FROM universe_versions"
    ):
        read_membership_pins(
            workspace.state, None, UniversePin(*header), max_materialization_bytes=allowance
        )


def _verify_artifacts(workspace: Workspace) -> None:
    """Re-hash every recorded run artifact in bounded chunks."""
    for row in workspace.state.execute(
        "SELECT run_id,relative_path,size_bytes,content_hash FROM artifacts"
    ):
        relative = row["run_id"] + "/" + row["relative_path"]
        with (
            DescriptorTree.open_path(workspace.paths.runs) as tree,
            tree.binary_reader(relative, require_single_link=True) as source,
        ):
            hasher = hashlib.sha256()
            observed = 0
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                hasher.update(chunk)
                observed += len(chunk)
            if hasher.hexdigest() != row["content_hash"] or observed != row["size_bytes"]:
                raise ValueError("run artifact hash/size mismatch")


def _admit_document(row: object, allowance: int, message: str) -> None:
    """Charge a document that is materialized, checked, then released.

    One of these is live at a time, so it is compared with the whole remaining
    allowance. Reusing the collection rule here would add an unrelated eightfold
    restriction and refuse documents that comfortably fit.
    """
    if cast("tuple[int]", row)[0] > allowance:
        raise ComputeResourceError(message)


def _admit_retained(row: object, allowance: int, message: str) -> int:
    """Charge a collection that stays live across later steps, before fetching it.

    Returns the charge so the caller can keep reserving it while it is held.
    """
    charge = cast("tuple[int]", row)[0]
    if charge > allowance // 8:
        raise ComputeResourceError(message)
    return charge


def _verify_runs(workspace: Workspace, budget: ComputeBudget) -> list[str]:
    """Check both directions between successful runs and their result markers.

    A successful run is re-derived from its own evidence, so an altered stored row or a
    replaced artifact cannot keep certifying itself through a hash string that still
    matches. A run that is still open, or one that ended without a result, is reported
    rather than raised: those are the states recovery exists to resolve, and failing
    here would block backup on exactly the workspace that needs one.
    """
    from aegis_alpha.storage.runs import verify_run  # noqa: PLC0415

    if inspect_run_schema(workspace).state != "complete":
        if workspace.market.execute("SELECT 1 FROM result_commits").fetchone():
            raise ValueError("result markers exist without the run add-on")
        return []
    runs_held = _admit_retained(
        workspace.state.execute(
            "SELECT coalesce(count(*)*2048 + sum(128*length(CAST(run_id AS BLOB))),0) FROM runs"
        ).fetchone(),
        budget.available_bytes,
        "run catalog exceeds materialization budget",
    )
    markers_held = _admit_retained(
        workspace.market.execute(
            "SELECT count(*)*2048 + "
            "coalesce(sum(128*coalesce(octet_length(encode(run_id)),0)),0) FROM result_commits"
        ).fetchone(),
        budget.available_bytes,
        "result marker catalog exceeds materialization budget",
    )
    committed = {
        row[0] for row in workspace.market.execute("SELECT run_id FROM result_commits").fetchall()
    }
    recorded = {
        row[0]: (row[1], row[2])
        for row in workspace.state.execute(
            "SELECT r.run_id,r.status,d.request_hash FROM runs r "
            "LEFT JOIN run_details d ON d.run_id=r.run_id"
        )
    }
    # Both catalogs stay live while every successful run is re-derived, so each run is
    # admitted against what is left rather than against the whole allowance.
    held = replace(budget, reserved_bytes=budget.reserved_bytes + runs_held + markers_held)
    for run_id, (status, request_hash) in sorted(recorded.items()):
        if status != "SUCCESS":
            continue
        if request_hash is None:
            raise ValueError("successful run has no recorded request")
        verify_run(workspace, run_id, request_hash, budget=held)
    if any(run_id not in recorded for run_id in committed):
        raise ValueError("result marker has no run record")
    return [
        run_id for run_id in sorted(committed) if recorded.get(run_id, (None, None))[0] != "SUCCESS"
    ]


def verify_workspace(  # noqa: C901, PLR0912 -- full cross-store verification boundary
    workspace: Workspace, *, budget: ComputeBudget | None = None
) -> dict[str, object]:
    """Verify under a caller-owned allocation, retaining the serial default when omitted."""
    budget = budget or ComputeBudget(Fraction(1), 512 * 1024 * 1024)
    # Every step below charges against the same non-DuckDB allowance. DuckDB's own
    # share is bounded separately by the connection limit derived from this budget.
    allowance = budget.available_bytes
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
    _verify_membership(workspace, allowance)
    size = workspace.state.execute(
        "SELECT count(*)*2048 + coalesce(sum(32*(length(dataset_id)+length(version)+"
        "length(generation_id)+coalesce(length(parent_generation_id),0))),0) "
        "FROM dataset_versions WHERE status='committed'"
    ).fetchone()[0]
    if size > allowance // 8:
        raise ComputeResourceError("publication catalog exceeds materialization budget")
    # The catalog rows stay live through every later step, so every later step is
    # admitted against what is left rather than the whole allowance. Accumulate onto
    # whatever the caller already reserved instead of replacing it.
    held = replace(budget, reserved_bytes=budget.reserved_bytes + size)
    versions = workspace.state.execute(
        "SELECT dataset_id,version,generation_id,chain_hash,manifest_hash,parent_generation_id "
        "FROM dataset_versions WHERE status='committed'"
    ).fetchall()
    committed = {version["generation_id"] for version in versions}
    parents = {version["parent_generation_id"] for version in versions}
    covered: set[str] = set()
    for generation in sorted(committed - parents):
        verify_sealed_publication(workspace, generation, budget=held)
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
    # This list stays live while every strategy is verified, so charge it first.
    # Held only while the strategies below are verified, then released.
    _admit_retained(
        workspace.strategies.execute(
            "SELECT count(*)*2048 + coalesce(sum(32*(length(CAST(strategy_id AS BLOB))+"
            "length(CAST(version AS BLOB))+length(CAST(raw_sha256 AS BLOB)))),0) "
            "FROM strategy_versions"
        ).fetchone(),
        held.available_bytes,
        "strategy catalog exceeds materialization budget",
    )
    # Content verification fetches one stored bundle and its contract at a time, so
    # charge the largest of those before any of them is read.
    _admit_document(
        workspace.strategies.execute(
            "SELECT coalesce(max(2048 + 32*(length(CAST(raw_bundle AS BLOB))+"
            "length(CAST(contract_json AS BLOB)))),0) FROM strategy_versions"
        ).fetchone(),
        held.available_bytes,
        "strategy payload exceeds materialization budget",
    )
    strategies = workspace.strategies.execute(
        "SELECT strategy_id,version,raw_sha256 FROM strategy_versions"
    ).fetchall()
    strategy_versions = len(strategies)
    for strategy in strategies:
        verify_strategy_content(workspace.strategies, *strategy)
    # Only the count is needed from here on, so this charge is released.
    del strategies
    verify_strategy_imports(workspace)
    # read_convention decodes and re-canonicalizes one whole document at a time.
    _admit_document(
        workspace.state.execute(
            "SELECT coalesce(max(2048 + 128*length(CAST(payload AS BLOB))),0) FROM conventions"
        ).fetchone(),
        held.available_bytes,
        "convention document exceeds materialization budget",
    )
    for convention in workspace.state.execute(
        "SELECT kind,convention_id,version,content_hash FROM conventions"
    ):
        read_convention(workspace.state, ConventionPin(*convention))
    _verify_input_documents(workspace, held)
    _verify_artifacts(workspace)
    unfinished_runs = _verify_runs(workspace, held)
    pending = workspace.state.execute(
        "SELECT count(*) FROM storage_operations WHERE phase='PREPARED'"
    ).fetchone()[0]
    # market_generations is DuckDB, so the SQLite length(CAST(... AS BLOB)) form
    # does not apply; encode() gives the byte width of each identifier.
    generations = _admit_retained(
        workspace.market.execute(
            "SELECT count(*)*2048 + "
            "coalesce(sum(32*coalesce(octet_length(encode(generation_id)),0)),0) "
            "FROM market_generations"
        ).fetchone(),
        held.available_bytes,
        "market generation catalog exceeds materialization budget",
    )
    untracked = workspace.market.execute("SELECT generation_id FROM market_generations").fetchall()
    # This list is still live while sources are verified below, so reserve it too.
    held = replace(held, reserved_bytes=held.reserved_bytes + generations)
    visible = {row["generation_id"] for row in versions}
    report: dict[str, object] = {
        "verified": True,
        "dataset_versions": len(versions),
        "strategy_versions": strategy_versions,
        "pending_operations": pending,
        "orphan_generations": [row[0] for row in untracked if row[0] not in visible],
    }
    if unfinished_runs:
        report["unfinished_runs"] = unfinished_runs
    from aegis_alpha.storage.source_library import verify_sources  # noqa: PLC0415

    # The catalog and generation lists are still held, so source verification is
    # admitted against what is left rather than the whole allowance.
    sources = verify_sources(workspace, budget=held)
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
        # Each stored request is decoded and canonicalized whole before comparison.
        _admit_document(
            workspace.state.execute(
                "SELECT coalesce(max(2048 + 128*length(CAST(request_bytes AS BLOB))),0) "
                "FROM backtest_requests"
            ).fetchone(),
            budget.available_bytes,
            "backtest request exceeds materialization budget",
        )
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
