"""Pinned bundles in the private strategy database; never execute source code."""

from __future__ import annotations

import sqlite3
import time

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.bundle import EngineBundle, load_bundle
from aegis_alpha.storage.sqlite import initialize
from aegis_alpha.storage.state import atomic
from aegis_alpha.storage.strategy_schema import STRATEGY_DDL, STRATEGY_KIND


def initialize_strategies(connection: sqlite3.Connection, installation_id: str) -> None:
    initialize(connection, installation_id, STRATEGY_KIND, STRATEGY_DDL)


def import_strategy(  # noqa: PLR0913, PLR0917 -- explicit external bundle pins
    connection: sqlite3.Connection,
    raw: bytes,
    expected_sha256: str,
    expected_id: str,
    expected_version: str,
    operation_id: str,
) -> dict[str, object]:
    bundle = load_bundle(raw, expected_sha256, expected_id, expected_version)
    contract = canonical_json_bytes(bundle.contract).decode()
    with atomic(connection):
        previous = connection.execute(
            "SELECT raw_sha256,contract_sha256,raw_bundle FROM strategy_versions "
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
        receipt = connection.execute(
            "SELECT strategy_id,version,request_hash FROM strategy_imports WHERE operation_id=?",
            (operation_id,),
        ).fetchone()
        if receipt is not None and tuple(receipt) != (
            expected_id,
            expected_version,
            expected_sha256,
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
        if receipt is None:
            connection.execute(
                "INSERT INTO strategy_imports VALUES (?,?,?,?,?)",
                (
                    operation_id,
                    expected_sha256,
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
    contract = bundle.contract
    connection.execute(
        "INSERT INTO strategy_requirements VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            bundle.bundle_id,
            bundle.bundle_version,
            "prices",
            1,
            "engine-price-v1",
            "close",
            "prices",
            contract.calendar.history_observations,
            "explicit-input",
            "calendar_month_end",
        ),
    )
    for ordinal, signal in enumerate(contract.macro_signals, 1):
        connection.execute(
            "INSERT INTO strategy_requirements VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                bundle.bundle_id,
                bundle.bundle_version,
                "macro",
                ordinal,
                "engine-macro-v1",
                signal.series_id,
                "macro_observations",
                max(signal.lag_months, default=0),
                "not_applicable",
                "calendar_month_end",
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
    row = connection.execute(
        "SELECT raw_bundle,raw_sha256,contract_json,contract_sha256 FROM strategy_versions "
        "WHERE strategy_id=? AND version=?",
        (strategy_id, version),
    ).fetchone()
    if row is None:
        raise ValueError("strategy ID/version is not registered")
    if row["raw_sha256"] != expected_sha256:
        raise ValueError("strategy version hash does not match execution pin")
    if connection.execute(
        "SELECT 1 FROM strategy_lineage WHERE strategy_id=? AND version=? AND "
        "parent_status='unresolved'",
        (strategy_id, version),
    ).fetchone():
        raise ValueError("strategy has unresolved parent lineage")
    bundle = load_bundle(row["raw_bundle"], expected_sha256, strategy_id, version)
    if (
        bundle.contract_sha256 != row["contract_sha256"]
        or canonical_json_bytes(bundle.contract).decode() != row["contract_json"]
    ):
        raise ValueError("strategy parsed contract hash mismatch")
    return bundle
