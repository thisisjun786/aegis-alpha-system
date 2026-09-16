"""Publish exact market generations only after raw bytes and target commits verify."""

from __future__ import annotations

import argparse
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, cast

from aegis_alpha.storage.import_document import ImportDocument, parse_import, read_import
from aegis_alpha.storage.raw import put_raw, verify_raw

if TYPE_CHECKING:
    import sqlite3

    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace


def json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    return value


def _field(document: ImportDocument, key: str) -> str:
    value = document.body[key]
    if not isinstance(value, str):
        raise TypeError("expected text import field")
    return value


def publish_document(workspace: Workspace, document: ImportDocument) -> dict[str, object]:
    from aegis_alpha.storage.market import plan_generation, publish_generation  # noqa: PLC0415
    from aegis_alpha.storage.state import get_operation, prepare_operation  # noqa: PLC0415

    body = document.body
    recovery = recover_operations(workspace)
    pending = set(cast("list[str]", recovery["pending"])) - {_field(document, "operation_id")}
    if pending:
        raise ValueError("recover or quarantine pending operations before a new import")
    previous = get_operation(workspace.state, _field(document, "operation_id"))
    ingested = cast("int", previous["created_at_us"]) if previous else time.time_ns() // 1000
    rows = [{**row, "ingested_at_us": ingested} for row in document.rows]
    plan_generation(
        workspace.market,
        dataset_id=_field(document, "dataset_id"),
        version=_field(document, "version"),
        generation_id=_field(document, "generation_id"),
        operation_id=_field(document, "operation_id"),
        request_hash=document.sha256,
        parent_id=cast("str | None", body["parent_id"]),
        domain=_field(document, "domain"),
        rows=rows,
    )
    _check_parent(workspace, document)
    _check_instruments(workspace, document)
    prepare_operation(
        workspace.state,
        operation_id=_field(document, "operation_id"),
        kind="market_publish",
        request_hash=document.sha256,
        target_id=_field(document, "generation_id"),
        expected_parent=cast("str | None", body["parent_id"]),
        payload_hash=document.sha256,
        created_at_us=ingested,
    )
    relative, digest, size = put_raw(workspace.paths.raw, document.payload)
    _register_source(workspace, document, relative, digest, size)
    marker = publish_generation(
        workspace.market,
        dataset_id=_field(document, "dataset_id"),
        version=_field(document, "version"),
        generation_id=_field(document, "generation_id"),
        operation_id=_field(document, "operation_id"),
        request_hash=document.sha256,
        parent_id=cast("str | None", body["parent_id"]),
        domain=_field(document, "domain"),
        rows=rows,
    )
    _complete_publication(workspace, document, marker)
    return {
        "published": True,
        "dataset_id": body["dataset_id"],
        "version": body["version"],
        "generation_id": body["generation_id"],
        "source_sha256": digest,
        "backtest_eligible": False,
        "marker": marker,
    }


def _check_parent(workspace: Workspace, document: ImportDocument) -> None:
    rows = workspace.state.execute(
        "SELECT generation_id FROM dataset_versions WHERE dataset_id=? AND status='committed' "
        "ORDER BY sequence DESC LIMIT 1",
        (_field(document, "dataset_id"),),
    ).fetchall()
    existing = workspace.state.execute(
        "SELECT generation_id FROM dataset_versions WHERE dataset_id=? AND version=?",
        (_field(document, "dataset_id"), _field(document, "version")),
    ).fetchone()
    if existing:
        if existing[0] != document.body["generation_id"]:
            raise ValueError("dataset/version already has a different generation")
        return
    expected = rows[0][0] if rows else None
    if expected != document.body["parent_id"]:
        raise ValueError("dataset parent changed; publish a new request with its exact parent")


def _check_instruments(workspace: Workspace, document: ImportDocument) -> None:
    supplied = document.body.get("instruments", [])
    if not isinstance(supplied, list):
        raise TypeError("instruments must be an array")
    with workspace.state:
        for entry in supplied:
            if not isinstance(entry, dict):
                raise TypeError("instrument identity must be an object")
            item = cast("dict[str, object]", entry)
            if set(item) != {"instrument_id", "asset_type", "venue"}:
                raise ValueError("instrument identity requires ID, asset type and venue")
            if any(not isinstance(value, str) or not value.strip() for value in item.values()):
                raise ValueError("instrument identity fields must be nonempty text")
            previous = workspace.state.execute(
                "SELECT asset_type, venue FROM instruments WHERE instrument_id=?",
                (item["instrument_id"],),
            ).fetchone()
            if previous is not None:
                if tuple(previous) != (item["asset_type"], item["venue"]):
                    raise ValueError("instrument identity conflicts with registered definition")
            else:
                workspace.state.execute(
                    "INSERT INTO instruments(instrument_id, issuer_id, asset_type, venue) "
                    "VALUES (?, NULL, ?, ?)",
                    (item["instrument_id"], item["asset_type"], item["venue"]),
                )
        for row in document.rows:
            instrument = row.get("instrument_id")
            if (
                instrument is not None
                and workspace.state.execute(
                    "SELECT 1 FROM instruments WHERE instrument_id=?",
                    (instrument,),
                ).fetchone()
                is None
            ):
                raise ValueError("market row references an unknown instrument")


def _register_source(
    workspace: Workspace, document: ImportDocument, relative: str, digest: str, size: int
) -> None:
    now = time.time_ns() // 1000
    with workspace.state:
        if (
            workspace.state.execute(
                "SELECT 1 FROM source_snapshots WHERE snapshot_id=?", (document.source_id,)
            ).fetchone()
            is None
        ):
            workspace.state.execute(
                "INSERT INTO source_snapshots(snapshot_id, provider, requested_at_us, "
                "retrieved_at_us, publication_at_us, status) "
                "VALUES (?, ?, ?, ?, ?, 'raw_verified')",
                (
                    document.source_id,
                    _field(document, "provider"),
                    now,
                    now,
                    document.body["publication_at_us"],
                ),
            )
            workspace.state.execute(
                "INSERT INTO source_files(snapshot_id, relative_path, byte_hash, "
                "size_bytes) VALUES (?, ?, ?, ?)",
                (document.source_id, relative, digest, size),
            )
    verify_raw(workspace.paths.raw, relative, digest, size)


def _complete_publication(
    workspace: Workspace, document: ImportDocument, marker: dict[str, object]
) -> None:
    from aegis_alpha.storage.market import verify_generation  # noqa: PLC0415
    from aegis_alpha.storage.state import complete_operation  # noqa: PLC0415

    checked = verify_generation(workspace.market, _field(document, "generation_id"))
    if (
        any(
            checked[key] != document.body[key]
            for key in (
                "generation_id",
                "dataset_id",
                "version",
                "operation_id",
                "parent_id",
                "domain",
            )
        )
        or checked["request_hash"] != document.sha256
    ):
        raise ValueError("generation marker does not match the publication intent")
    if checked != marker:
        raise ValueError("generation marker changed during publication")
    digest = document.sha256
    verify_raw(workspace.paths.raw, digest[:2] + "/" + digest, digest, len(document.payload))
    _check_parent(workspace, document)
    with workspace.state:
        workspace.state.execute(
            "INSERT INTO datasets(dataset_id, domain, record_schema, owner) VALUES (?, ?, "
            "?, ?) ON CONFLICT(dataset_id) DO NOTHING",
            (
                _field(document, "dataset_id"),
                _field(document, "domain"),
                "aas-market-rowset-v1",
                "local-import",
            ),
        )
        # Catalog visibility is the final write after all three independent stores verify.
        if (
            workspace.state.execute(
                "SELECT 1 FROM dataset_versions WHERE generation_id=?",
                (_field(document, "generation_id"),),
            ).fetchone()
            is None
        ):
            workspace.state.execute(
                "INSERT INTO dataset_versions(dataset_id, version, generation_id, "
                "parent_generation_id, sequence, chain_hash, manifest_hash, "
                "record_schema, normalizer_version, transform_hash, "
                "identity_snapshot_hash, authority_policy_hash, row_count, coverage, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, 'committed')",
                (
                    _field(document, "dataset_id"),
                    _field(document, "version"),
                    _field(document, "generation_id"),
                    document.body["parent_id"],
                    marker["sequence"],
                    marker["chain_hash"],
                    document.sha256,
                    "aas-market-rowset-v1",
                    _field(document, "normalizer_version"),
                    _field(document, "transform_sha256"),
                    marker["row_count"],
                    "unverified",
                ),
            )
            workspace.state.execute(
                "INSERT INTO dataset_sources(dataset_id, version, source_snapshot_id) "
                "VALUES (?, ?, ?)",
                (_field(document, "dataset_id"), _field(document, "version"), document.source_id),
            )
        complete_operation(workspace.state, _field(document, "operation_id"), document.sha256)


def read_dataset(workspace: Workspace, dataset_id: str, version: str) -> dict[str, object]:
    row = workspace.state.execute(
        "SELECT dataset_id, version, generation_id, chain_hash, manifest_hash, row_count, status "
        "FROM "
        "dataset_versions "
        "WHERE dataset_id=? AND version=? AND status='committed'",
        (dataset_id, version),
    ).fetchone()
    if row is None:
        raise ValueError("dataset/version is not published")
    from aegis_alpha.storage.market import verify_generation  # noqa: PLC0415

    marker = verify_generation(workspace.market, row["generation_id"])
    if (
        marker["chain_hash"] != row["chain_hash"]
        or marker["row_count"] != row["row_count"]
        or marker["request_hash"] != row["manifest_hash"]
    ):
        raise ValueError("catalog and market generation disagree")
    return dict(row)


def recover_operations(
    workspace: Workspace, *, budget: ComputeBudget | None = None
) -> dict[str, object]:
    from aegis_alpha.storage.runs import RUN_OPERATION_KIND  # noqa: PLC0415

    recovered: list[str] = []
    pending: list[str] = []
    rows = workspace.state.execute(
        "SELECT operation_id, kind, request_hash, target_id, expected_parent, payload_hash "
        "FROM storage_operations WHERE phase='PREPARED'"
    ).fetchall()
    for operation in rows:
        op_id = operation["operation_id"]
        if operation["kind"] == "strategy_import":
            from aegis_alpha.storage.strategy_import import recover_strategy_import  # noqa: PLC0415

            if not recover_strategy_import(workspace, operation):
                pending.append(op_id)
                continue
        elif operation["kind"] == "source_import":
            from aegis_alpha.storage.source_library import recover_source  # noqa: PLC0415

            if not recover_source(workspace, op_id):
                pending.append(op_id)
                continue
        elif operation["kind"] == "market_publish":
            if not _recover_publication(workspace, operation):
                pending.append(op_id)
                continue
        elif operation["kind"] == RUN_OPERATION_KIND:
            from aegis_alpha.storage.runs import recover_run  # noqa: PLC0415

            if not recover_run(workspace, operation, budget=budget):
                pending.append(op_id)
                continue
        else:
            pending.append(op_id)
            continue
        recovered.append(op_id)
    return {"recovered": recovered, "pending": pending, "provider_calls": 0}


def _recover_publication(workspace: Workspace, operation: sqlite3.Row) -> bool:
    """Finish a publication whose verifiable generation marker already exists."""
    from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415
    from aegis_alpha.storage.market import verify_generation  # noqa: PLC0415

    op_id = operation["operation_id"]
    marker_row = workspace.market.execute(
        "SELECT generation_id FROM market_generations WHERE operation_id=?", [op_id]
    ).fetchone()
    if marker_row is None:
        return False
    marker = verify_generation(workspace.market, marker_row[0])
    digest = operation["payload_hash"]
    relative = digest[:2] + "/" + digest
    with DescriptorTree.open_path(workspace.paths.raw) as tree:
        raw = tree.read_bytes(relative, max_bytes=64 * 1024 * 1024)
    document = parse_import(raw)
    if document.sha256 != operation["request_hash"] or document.body["operation_id"] != op_id:
        raise ValueError("stored source does not match prepared publication")
    _complete_publication(workspace, document, marker)
    return True


def execute_data(workspace: Workspace, args: argparse.Namespace) -> dict[str, object]:
    if args.data_command == "import":
        return publish_document(workspace, read_import(Path(args.file).absolute(), args.sha256))
    if args.data_command == "datasets":
        rows = workspace.state.execute(
            "SELECT dataset_id, version, generation_id, chain_hash, row_count FROM "
            "dataset_versions WHERE status='committed' ORDER BY dataset_id, sequence"
        ).fetchall()
        return {"datasets": [dict(row) for row in rows]}
    dataset = read_dataset(workspace, args.dataset, args.version)
    if args.data_command == "inspect":
        return dataset
    from aegis_alpha.storage.market import read_generation  # noqa: PLC0415

    rows = read_generation(
        workspace.market,
        str(dataset["generation_id"]),
        cutoff_us=args.cutoff_us,
        ingestion_cutoff_us=args.ingestion_cutoff_us,
        limit=args.limit,
    )
    return {"dataset": dataset, "rows": json_value(rows), "backtest_eligible": False}


def quarantine(workspace: Workspace, operation_id: str, reason: str) -> dict[str, object]:
    from aegis_alpha.storage.runs import RUN_OPERATION_KIND  # noqa: PLC0415
    from aegis_alpha.storage.state import get_operation, quarantine_operation  # noqa: PLC0415

    intent = get_operation(workspace.state, operation_id)
    if intent is not None and intent["kind"] == RUN_OPERATION_KIND:
        # Ending the intent alone would leave its run RUNNING and invisible to the
        # PREPARED-only recovery scan.
        raise ValueError("a run intent is ended by recovery, which also ends its run")
    if (
        workspace.market.execute(
            "SELECT 1 FROM market_generations WHERE operation_id=?", [operation_id]
        ).fetchone()
        or workspace.market.execute(
            "SELECT 1 FROM result_commits WHERE operation_id=?", [operation_id]
        ).fetchone()
    ):
        raise ValueError("committed target must be recovered, not quarantined as an empty intent")
    if (
        workspace.strategies is not None
        and workspace.strategies.execute(
            "SELECT 1 FROM strategy_imports WHERE operation_id=?", (operation_id,)
        ).fetchone()
    ):
        raise ValueError("committed strategy must be recovered")
    quarantine_operation(workspace.state, operation_id, reason)
    return {"quarantined": operation_id, "reason": reason, "deleted": False}
