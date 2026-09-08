"""Immutable projections covered by a one-shot FMP owner signature."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final

from aegis_alpha.data.fmp_cli_artifacts import FMP_POLICY_ID
from aegis_alpha.data.fmp_owner_approval import SCOPE_CONTRACT
from aegis_alpha.data.fmp_universe_run import ACTIVE_LIST_ENDPOINT, DELISTED_LIST_ENDPOINT
from aegis_alpha.data.fmp_windows import (
    BACKFILL_CHUNK_DAYS,
    LIST_PAGE_LIMIT,
    REVALIDATION_TRADING_DAYS,
)
from aegis_alpha.data.serialization import canonical_json_bytes

SCOPE_VERSION: Final = 1
UNIVERSE_OUTPUT_CONTRACT: Final = "aegis-alpha/fmp-universe-manifest/v1"
UNIVERSE_OUTPUT_SEMANTICS: Final = "active-plus-delisted-no-clobber"


@dataclass(frozen=True, slots=True)
class CommonScope:
    run_identity: str
    policy_sha256: str
    tier_sha256: str
    notification_sha256: str
    raw_store_root: Path
    dataset_root: Path
    max_calls: int


@dataclass(frozen=True, slots=True)
class CollectScope:
    common: CommonScope
    manifest_sha256: str
    dataset_selection: str
    datasets: tuple[str, ...]
    mode: str
    as_of: date
    operator_from: date | None
    receipt_path: Path


@dataclass(frozen=True, slots=True)
class UniverseScope:
    common: CommonScope
    destination: Path


def _common(scope: CommonScope, operation: str) -> dict[str, object]:
    return {
        "contract": SCOPE_CONTRACT,
        "version": SCOPE_VERSION,
        "operation": operation,
        "run_identity": scope.run_identity,
        "policy_id": FMP_POLICY_ID,
        "policy_sha256": scope.policy_sha256,
        "scheduled_collection_allowed": False,
        "tier_sha256": scope.tier_sha256,
        "storage_notification_sha256": scope.notification_sha256,
        "raw_store_root": str(scope.raw_store_root),
        "dataset_root": str(scope.dataset_root),
        "max_calls": scope.max_calls,
    }


def collect_scope_sha256(scope: CollectScope) -> str:
    """Hash exact manifest, selection, windows, destinations, and common policy inputs."""

    document = {
        **_common(scope.common, "collect"),
        "manifest_sha256": scope.manifest_sha256,
        "dataset_selection": scope.dataset_selection,
        "datasets": list(scope.datasets),
        "mode": scope.mode,
        "window": {
            "as_of": scope.as_of.isoformat(),
            "operator_from": (
                None if scope.operator_from is None else scope.operator_from.isoformat()
            ),
        },
        "window_policy": {
            "version": 1,
            "backfill_chunk_days": BACKFILL_CHUNK_DAYS,
            "revalidation_trading_days": REVALIDATION_TRADING_DAYS,
        },
        "receipt_path": str(scope.receipt_path),
    }
    return hashlib.sha256(canonical_json_bytes(document)).hexdigest()


def universe_scope_sha256(scope: UniverseScope) -> str:
    """Hash fixed universe walking semantics and its exact output destination."""

    document = {
        **_common(scope.common, "build-universe"),
        "mode": "probe",
        "dataset": "fmp_universe",
        "active_endpoint": ACTIVE_LIST_ENDPOINT,
        "delisted_endpoint": DELISTED_LIST_ENDPOINT,
        "list_page_limit": LIST_PAGE_LIMIT,
        "output_contract": UNIVERSE_OUTPUT_CONTRACT,
        "output_semantics": UNIVERSE_OUTPUT_SEMANTICS,
        "universe_manifest_out": str(scope.destination),
    }
    return hashlib.sha256(canonical_json_bytes(document)).hexdigest()
