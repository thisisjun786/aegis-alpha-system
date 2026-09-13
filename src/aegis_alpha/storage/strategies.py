"""Pinned bundles in the private strategy database; never execute source code."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass

from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine.bundle import EngineBundle, load_bundle
from aegis_alpha.engine.requirements import derive_execution_definition, legacy_requirement_rows
from aegis_alpha.storage.sqlite import initialize
from aegis_alpha.storage.state import atomic
from aegis_alpha.storage.strategy_schema import STRATEGY_DDL, STRATEGY_KIND


def initialize_strategies(connection: sqlite3.Connection, installation_id: str) -> None:
    initialize(connection, installation_id, STRATEGY_KIND, STRATEGY_DDL)


@dataclass(frozen=True, slots=True)
class LineageSpec:
    """Exact optional parent evidence supplied when a child version is first registered."""

    parent_id: str
    parent_version: str
    change_kind: str
    reason: str

    def __post_init__(self) -> None:
        for value in (self.parent_id, self.parent_version, self.change_kind):
            if not isinstance(value, str) or not value.strip() or value != value.strip():
                raise ValueError("lineage identity fields must be nonempty trimmed strings")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("lineage reason must be a nonempty string")


def strategy_request_hash(bundle: EngineBundle, lineage: LineageSpec | None) -> str:
    """Pin caller evidence separately from raw bytes; retain v1 no-lineage receipts."""
    if lineage is None:
        return bundle.source_sha256
    return content_sha256(
        {
            "schema_version": "aas-strategy-import-request-v1",
            "strategy_id": bundle.bundle_id,
            "version": bundle.bundle_version,
            "raw_sha256": bundle.source_sha256,
            "lineage": lineage,
        }
    )


def read_strategy_lineage(
    connection: sqlite3.Connection, strategy_id: str, version: str
) -> LineageSpec | None:
    """Reconstruct exact caller evidence, not the derived parent eligibility status."""
    rows = connection.execute(
        "SELECT parent_strategy_id,parent_version,change_kind,reason,reason_hash "
        "FROM strategy_lineage WHERE strategy_id=? AND version=?",
        (strategy_id, version),
    ).fetchall()
    if not rows:
        return None
    if len(rows) != 1 or rows[0]["reason_hash"] != content_sha256(rows[0]["reason"]):
        raise ValueError("strategy stored lineage evidence mismatch")
    return LineageSpec(*tuple(rows[0])[:4])


def import_strategy(  # noqa: PLR0913, PLR0917 -- explicit external bundle pins
    connection: sqlite3.Connection,
    raw: bytes,
    expected_sha256: str,
    expected_id: str,
    expected_version: str,
    operation_id: str,
    *,
    lineage: LineageSpec | None = None,
) -> dict[str, object]:
    bundle = load_bundle(raw, expected_sha256, expected_id, expected_version)
    contract = canonical_json_bytes(bundle.contract).decode()
    request_hash = strategy_request_hash(bundle, lineage)
    with atomic(connection):
        previous = connection.execute(
            "SELECT raw_sha256,contract_sha256,raw_bundle,contract_json FROM strategy_versions "
            "WHERE strategy_id=? "
            "AND version=?",
            (expected_id, expected_version),
        ).fetchone()
        if previous is not None and tuple(previous)[:2] != (
            expected_sha256,
            bundle.contract_sha256,
        ):
            raise ValueError("strategy ID/version already contains different content")
        if previous is not None:
            load_bundle(previous["raw_bundle"], expected_sha256, expected_id, expected_version)
            if previous["contract_json"] != contract:
                raise ValueError("strategy parsed contract hash mismatch")
            if read_strategy_lineage(connection, expected_id, expected_version) != lineage:
                raise ValueError("strategy ID/version already contains different lineage")
        receipt = connection.execute(
            "SELECT strategy_id,version,request_hash FROM strategy_imports WHERE operation_id=?",
            (operation_id,),
        ).fetchone()
        if receipt is not None and tuple(receipt) != (
            expected_id,
            expected_version,
            request_hash,
        ):
            raise ValueError("strategy operation ID already identifies a different import")
        if previous is None:
            connection.execute(
                "INSERT INTO strategies VALUES (?,?,'active') ON CONFLICT(strategy_id) DO NOTHING",
                (expected_id, expected_id),
            )
            connection.execute(
                "INSERT INTO strategy_versions VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    expected_id,
                    expected_version,
                    raw,
                    expected_sha256,
                    contract,
                    bundle.contract_sha256,
                    bundle.schema_version,
                    bundle.contract.contract_version,
                    time.time_ns() // 1000,
                ),
            )
            connection.execute(
                "INSERT INTO strategy_sources VALUES (?,?,?,?,?,?,?)",
                (
                    "source-" + expected_sha256,
                    expected_id,
                    expected_version,
                    "sha256:" + expected_sha256,
                    expected_sha256,
                    "unknown",
                    "user supplied; origin not independently verified",
                ),
            )
            _requirements(connection, bundle)
            if lineage is not None:
                _register_lineage(connection, expected_id, expected_version, lineage)
        if receipt is None:
            connection.execute(
                "INSERT INTO strategy_imports VALUES (?,?,?,?,?)",
                (
                    operation_id,
                    request_hash,
                    expected_id,
                    expected_version,
                    time.time_ns() // 1000,
                ),
            )
    return {
        "imported": True,
        "strategy_id": expected_id,
        "version": expected_version,
        "raw_sha256": expected_sha256,
        "contract_sha256": bundle.contract_sha256,
    }


def _requirements(connection: sqlite3.Connection, bundle: EngineBundle) -> None:
    connection.executemany(
        "INSERT INTO strategy_requirements VALUES (?,?,?,?,?,?,?,?,?,?)",
        legacy_requirement_rows(derive_execution_definition(bundle)),
    )


def _register_lineage(
    connection: sqlite3.Connection, strategy_id: str, version: str, lineage: LineageSpec
) -> None:
    # Include unresolved edges: registering a missing parent must not close a cycle.
    cycle = connection.execute(
        "WITH RECURSIVE walk(strategy_id,version) AS ("
        "SELECT ?,? UNION SELECT l.parent_strategy_id,l.parent_version "
        "FROM strategy_lineage l JOIN walk w "
        "ON l.strategy_id=w.strategy_id AND l.version=w.version) "
        "SELECT 1 FROM walk WHERE strategy_id=? AND version=?",
        (lineage.parent_id, lineage.parent_version, strategy_id, version),
    ).fetchone()
    if cycle is not None:
        raise ValueError("strategy lineage cycle")
    parent = connection.execute(
        "SELECT 1 FROM strategy_versions WHERE strategy_id=? AND version=?",
        (lineage.parent_id, lineage.parent_version),
    ).fetchone()
    connection.execute(
        "INSERT INTO strategy_lineage VALUES (?,?,?,?,?,?,?,?)",
        (
            strategy_id,
            version,
            lineage.parent_id,
            lineage.parent_version,
            lineage.change_kind,
            lineage.reason,
            content_sha256(lineage.reason),
            "resolved" if parent is not None else "unresolved",
        ),
    )


def list_strategies(connection: sqlite3.Connection) -> list[dict[str, object]]:
    return [
        dict(row)
        for row in connection.execute(
            "SELECT "
            "s.strategy_id,s.name,s.lifecycle,v.version,v.raw_sha256,"
            "v.contract_sha256,v.imported_at_us "
            "FROM strategies s JOIN strategy_versions v USING(strategy_id) ORDER BY "
            "s.strategy_id,v.version"
        ).fetchall()
    ]


def load_strategy(
    connection: sqlite3.Connection, strategy_id: str, version: str, expected_sha256: str
) -> EngineBundle:
    bundle = verify_strategy_content(connection, strategy_id, version, expected_sha256)
    if connection.execute(
        "SELECT 1 FROM strategy_lineage WHERE strategy_id=? AND version=? AND "
        "parent_status='unresolved'",
        (strategy_id, version),
    ).fetchone():
        raise ValueError("strategy has unresolved parent lineage")
    return bundle


def verify_strategy_content(
    connection: sqlite3.Connection, strategy_id: str, version: str, expected_sha256: str
) -> EngineBundle:
    """Verify persisted identity, raw bytes and contract, not execution eligibility."""
    row = connection.execute(
        "SELECT raw_bundle,raw_sha256,contract_json,contract_sha256 FROM strategy_versions "
        "WHERE strategy_id=? AND version=?",
        (strategy_id, version),
    ).fetchone()
    if row is None:
        raise ValueError("strategy ID/version is not registered")
    if row["raw_sha256"] != expected_sha256:
        raise ValueError("strategy version hash does not match execution pin")
    bundle = load_bundle(row["raw_bundle"], expected_sha256, strategy_id, version)
    if (
        bundle.contract_sha256 != row["contract_sha256"]
        or canonical_json_bytes(bundle.contract).decode() != row["contract_json"]
    ):
        raise ValueError("strategy parsed contract hash mismatch")
    return bundle
