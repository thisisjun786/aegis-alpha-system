"""Pinned bundles in the private strategy database; never execute source code."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from operator import itemgetter

from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine.bundle import EngineBundle, load_bundle
from aegis_alpha.engine.requirements import (
    derive_execution_definition,
    legacy_requirement_rows,
    project_legacy_requirement_rows,
)
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


def strategy_request_hash(
    bundle: EngineBundle, lineage: LineageSpec | None, *, parent_status: str | None = None
) -> str:
    """Seal the accepted status and exact caller bytes; no-lineage v1 stays raw SHA."""
    return _request_hash(
        (bundle.bundle_id, bundle.bundle_version, bundle.source_sha256), lineage, parent_status
    )


def _request_hash(
    identity: tuple[str, str, str], lineage: LineageSpec | None, parent_status: str | None
) -> str:
    if lineage is None:
        if parent_status is not None:
            raise ValueError("no-lineage request cannot have parent status")
        return identity[2]
    if parent_status not in {"resolved", "unresolved"}:
        raise ValueError("unsupported strategy lineage parent status")
    return content_sha256(
        {
            "schema_version": "aas-strategy-import-request-v2",
            "strategy_id": identity[0],
            "version": identity[1],
            "raw_sha256": identity[2],
            "lineage": lineage,
            "parent_status": parent_status,
        }
    )


def _committed_status(
    bundle: EngineBundle, lineage: LineageSpec | None, request_hash: str
) -> str | None:
    # Decode the two-element commitment domain, never today's parent existence.
    candidates = (None,) if lineage is None else ("resolved", "unresolved")
    matches = [
        status
        for status in candidates
        if strategy_request_hash(bundle, lineage, parent_status=status) == request_hash
    ]
    if len(matches) != 1:
        raise ValueError("strategy operation identifies a different request or unsealed lineage")
    return matches[0]


def accept_strategy_request(
    connection: sqlite3.Connection,
    bundle: EngineBundle,
    lineage: LineageSpec | None,
    *,
    request_hash: str | None = None,
) -> str:
    """Select fresh acceptance or replay a commitment, authenticating existing rows first."""
    previous = connection.execute(
        "SELECT 1 FROM strategy_versions WHERE strategy_id=? AND version=?",
        (bundle.bundle_id, bundle.bundle_version),
    ).fetchone()
    status = None
    if previous is not None:
        stored, status = _read_lineage(connection, bundle.bundle_id, bundle.bundle_version)
        if stored != lineage:
            raise ValueError("strategy ID/version already contains different lineage")
    if request_hash is not None:
        accepted = _committed_status(bundle, lineage, request_hash)
        if previous is not None and status != accepted:
            raise ValueError("strategy stored lineage does not match accepted request")
        status = accepted
    elif previous is None and lineage is not None:
        status = "resolved" if _parent_exists(connection, lineage) else "unresolved"
    if lineage is not None:
        _validate_lineage_cycle(connection, bundle.bundle_id, bundle.bundle_version, lineage)
        if status == "resolved" and not _parent_exists(connection, lineage):
            raise ValueError("strategy resolved parent missing")
    return strategy_request_hash(bundle, lineage, parent_status=status)


def _parent_exists(connection: sqlite3.Connection, lineage: LineageSpec) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM strategy_versions WHERE strategy_id=? AND version=?",
            (lineage.parent_id, lineage.parent_version),
        ).fetchone()
        is not None
    )


def read_strategy_lineage(
    connection: sqlite3.Connection, strategy_id: str, version: str
) -> LineageSpec | None:
    """Authenticate actual stored status against every private receipt, without state access."""
    return _read_lineage(connection, strategy_id, version)[0]


def _read_lineage(
    connection: sqlite3.Connection, strategy_id: str, version: str
) -> tuple[LineageSpec | None, str | None]:
    rows = connection.execute(
        "SELECT parent_strategy_id,parent_version,change_kind,reason,reason_hash,parent_status "
        "FROM strategy_lineage WHERE strategy_id=? AND version=?",
        (strategy_id, version),
    ).fetchall()
    if len(rows) > 1 or (rows and rows[0]["reason_hash"] != content_sha256(rows[0]["reason"])):
        raise ValueError("strategy stored lineage evidence mismatch")
    lineage = LineageSpec(*tuple(rows[0])[:4]) if rows else None
    status = rows[0]["parent_status"] if rows else None
    raw = connection.execute(
        "SELECT raw_sha256 FROM strategy_versions WHERE strategy_id=? AND version=?",
        (strategy_id, version),
    ).fetchone()
    if raw is None:
        raise ValueError("strategy ID/version is not registered")
    expected = _request_hash((strategy_id, version, raw[0]), lineage, status)
    receipts = connection.execute(
        "SELECT request_hash FROM strategy_imports WHERE strategy_id=? AND version=?",
        (strategy_id, version),
    ).fetchall()
    if not receipts or any(row[0] != expected for row in receipts):
        raise ValueError(
            "strategy stored lineage receipt mismatch or unsupported unsealed evidence"
        )
    # Only the direct parent's existence matters, never its execution eligibility.
    if lineage is not None and status == "resolved" and not _parent_exists(connection, lineage):
        raise ValueError("strategy resolved parent missing")
    return lineage, status


def validate_strategy_import(
    connection: sqlite3.Connection,
    bundle: EngineBundle,
    operation_id: str,
    *,
    lineage: LineageSpec | None = None,
    request_hash: str | None = None,
) -> tuple[bool, bool]:
    """SELECT-only admission checks; return whether the version and receipt exist.

    The private write repeats these checks inside its atomic transaction. A preflight
    result is never authorization to skip revalidation after durable admission.
    """
    strategy_id, version = bundle.bundle_id, bundle.bundle_version
    previous = connection.execute(
        "SELECT raw_sha256,contract_sha256 FROM strategy_versions "
        "WHERE strategy_id=? AND version=?",
        (strategy_id, version),
    ).fetchone()
    if previous is not None and tuple(previous) != (
        bundle.source_sha256,
        bundle.contract_sha256,
    ):
        raise ValueError("strategy ID/version already contains different content")
    if previous is not None:
        verify_strategy_content(connection, strategy_id, version, bundle.source_sha256)
    accepted = accept_strategy_request(connection, bundle, lineage, request_hash=request_hash)
    receipt = connection.execute(
        "SELECT strategy_id,version,request_hash FROM strategy_imports WHERE operation_id=?",
        (operation_id,),
    ).fetchone()
    if receipt is not None and tuple(receipt) != (
        strategy_id,
        version,
        accepted,
    ):
        raise ValueError("strategy operation ID already identifies a different import")
    return previous is not None, receipt is not None


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
    return _write_import(connection, raw, bundle, operation_id, lineage=lineage)


def import_prepared_strategy(  # noqa: PLR0913 -- raw pins plus journal commitment
    connection: sqlite3.Connection,
    raw: bytes,
    bundle: EngineBundle,
    operation_id: str,
    *,
    request_hash: str,
    lineage: LineageSpec | None = None,
) -> dict[str, object]:
    """Internal journal handoff: consume the accepted commitment, not a status override."""
    bundle = load_bundle(raw, bundle.source_sha256, bundle.bundle_id, bundle.bundle_version)
    return _write_import(
        connection, raw, bundle, operation_id, lineage=lineage, request_hash=request_hash
    )


def _write_import(  # noqa: PLR0913 -- one private writer for both admission paths
    connection: sqlite3.Connection,
    raw: bytes,
    bundle: EngineBundle,
    operation_id: str,
    *,
    lineage: LineageSpec | None,
    request_hash: str | None = None,
) -> dict[str, object]:
    expected_id, expected_version = bundle.bundle_id, bundle.bundle_version
    expected_sha256 = bundle.source_sha256
    contract = canonical_json_bytes(bundle.contract).decode()
    with atomic(connection):
        version_exists, receipt_exists = validate_strategy_import(
            connection, bundle, operation_id, lineage=lineage, request_hash=request_hash
        )
        request_hash = accept_strategy_request(
            connection, bundle, lineage, request_hash=request_hash
        )
        parent_status = _committed_status(bundle, lineage, request_hash)
        if not version_exists:
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
                _register_lineage(connection, expected_id, expected_version, lineage, parent_status)
        if not receipt_exists:
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


def _validate_lineage_cycle(
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


def _register_lineage(
    connection: sqlite3.Connection,
    strategy_id: str,
    version: str,
    lineage: LineageSpec,
    parent_status: str | None,
) -> None:
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
            parent_status,
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
    _lineage, status = _read_lineage(connection, strategy_id, version)
    if status == "unresolved":
        raise ValueError("strategy has unresolved parent lineage")
    return bundle


def verify_strategy_content(
    connection: sqlite3.Connection, strategy_id: str, version: str, expected_sha256: str
) -> EngineBundle:
    """Verify persisted identity, bytes, contract and v1 rows, not execution eligibility."""
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
    rows = connection.execute(
        "SELECT strategy_id,version,role,ordinal,required_schema,required_field,domain,"
        "warmup,basis,cadence FROM strategy_requirements WHERE strategy_id=? AND version=? "
        "ORDER BY role,ordinal",
        (strategy_id, version),
    ).fetchall()
    if [tuple(row) for row in rows] != sorted(
        project_legacy_requirement_rows(
            bundle.bundle_id,
            bundle.bundle_version,
            bundle.contract.calendar,
            bundle.contract.macro_signals,
        ),
        key=itemgetter(2, 3),
    ):
        raise ValueError("stored strategy requirements do not match the execution definition")
    return bundle
