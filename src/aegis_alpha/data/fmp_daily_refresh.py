"""Deterministic planning and universe classification for daily FMP refreshes."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.fmp_universe import universe_manifest_document
from aegis_alpha.data.fmp_windows import UniverseManifest
from aegis_alpha.data.serialization import canonical_json_bytes

_SHA256 = re.compile(r"[0-9a-f]{64}")
_MAX_SECURITY_MASTER_BYTES = 64_000_000


class DailyDisposition(StrEnum):
    """How one FMP symbol relates to the frozen Norgate universe."""

    NORGATE_OVERLAP = "norgate_overlap"
    NEW_LISTING_CANDIDATE = "new_listing_candidate"
    UNRESOLVED = "unresolved"
    PROVIDER_ACTIVE_DELISTED_OVERLAP = "provider_active_delisted_overlap"


@dataclass(frozen=True, slots=True)
class NorgateUniverseRecord:
    assetid: int
    symbol: str
    is_delisted: bool

    def __post_init__(self) -> None:
        if self.assetid < 1:
            raise ValueError("Norgate assetid must be positive")
        if not self.symbol.strip():
            raise ValueError("Norgate symbol cannot be empty")


@dataclass(frozen=True, slots=True)
class DailyClassification:
    symbol: str
    disposition: DailyDisposition
    norgate_assetid: int | None


@dataclass(frozen=True, slots=True)
class DailyUniverseClassification:
    entries: tuple[DailyClassification, ...]
    ignored_delisted_symbols: tuple[str, ...]
    collection_manifest: UniverseManifest

    def symbols(self, disposition: DailyDisposition) -> tuple[str, ...]:
        return tuple(entry.symbol for entry in self.entries if entry.disposition is disposition)


@dataclass(frozen=True, slots=True)
class DailyRefreshPlan:
    schedule_id: str
    service_day: date
    universe_run_id: str
    collection_run_id: str
    universe_path: Path
    collection_manifest_path: Path
    classification_path: Path
    receipt_path: Path


def _normalized_symbol(value: str) -> str:
    normalized = value.strip().casefold()
    if not normalized:
        raise ValueError("symbol cannot be empty")
    return normalized


def _require_canonical_symbol(value: str) -> str:
    normalized = _normalized_symbol(value)
    if value != normalized:
        raise ValueError("backfill manifest is not bound to signed Norgate universe facts")
    return normalized


def _canonical_manifest_bindings(manifest: UniverseManifest) -> dict[str, bool]:
    supplied: dict[str, bool] = {}
    for entry in manifest.entries:
        symbol = _require_canonical_symbol(entry.symbol)
        if symbol in supplied:
            raise ValueError("backfill manifest is not bound to signed Norgate universe facts")
        supplied[symbol] = entry.active
    return supplied


def _expected_norgate_bindings(
    norgate_records: tuple[NorgateUniverseRecord, ...],
) -> dict[str, bool]:
    by_symbol: dict[str, list[NorgateUniverseRecord]] = {}
    for record in norgate_records:
        by_symbol.setdefault(_normalized_symbol(record.symbol), []).append(record)
    expected: dict[str, bool] = {}
    for symbol, candidates in by_symbol.items():
        if len(candidates) != 1:
            raise ValueError("backfill manifest is not bound to signed Norgate universe facts")
        expected[symbol] = not candidates[0].is_delisted
    return expected


def classify_daily_universe(
    manifest: UniverseManifest,
    norgate_records: tuple[NorgateUniverseRecord, ...],
    *,
    norgate_snapshot_date: date,
) -> DailyUniverseClassification:
    """Classify active FMP symbols without inventing cross-provider identity."""

    by_symbol: dict[str, list[NorgateUniverseRecord]] = {}
    for record in norgate_records:
        by_symbol.setdefault(_normalized_symbol(record.symbol), []).append(record)

    classified: list[DailyClassification] = []
    collection_entries = []
    ignored = []
    active_folded = {entry.symbol.casefold() for entry in manifest.entries if entry.active}
    overlapping_folded = {
        entry.symbol.casefold()
        for entry in manifest.entries
        if not entry.active and entry.symbol.casefold() in active_folded
    }
    for entry in manifest.entries:
        if not entry.active:
            ignored.append(entry.symbol)
            continue
        if entry.symbol.casefold() in overlapping_folded:
            classified.append(
                DailyClassification(
                    symbol=entry.symbol,
                    disposition=DailyDisposition.PROVIDER_ACTIVE_DELISTED_OVERLAP,
                    norgate_assetid=None,
                )
            )
            continue
        candidates = by_symbol.get(_normalized_symbol(entry.symbol), [])
        active_candidates = [record for record in candidates if not record.is_delisted]
        if len(candidates) == 1 and len(active_candidates) == 1:
            disposition = DailyDisposition.NORGATE_OVERLAP
            assetid = active_candidates[0].assetid
        elif (
            not candidates and entry.ipo_date is not None and entry.ipo_date > norgate_snapshot_date
        ):
            disposition = DailyDisposition.NEW_LISTING_CANDIDATE
            assetid = None
        else:
            disposition = DailyDisposition.UNRESOLVED
            assetid = None
        classified.append(
            DailyClassification(
                symbol=entry.symbol,
                disposition=disposition,
                norgate_assetid=assetid,
            )
        )
        if disposition is not DailyDisposition.UNRESOLVED:
            collection_entries.append(entry)

    return DailyUniverseClassification(
        entries=tuple(classified),
        ignored_delisted_symbols=tuple(ignored),
        collection_manifest=UniverseManifest(
            generated_at_utc=manifest.generated_at_utc,
            sources=manifest.sources,
            entries=tuple(collection_entries),
        ),
    )


def _run_identity(
    schedule_id: str,
    service_day: date,
    operation: str,
    authority_payload_sha256: str,
) -> str:
    digest = hashlib.sha256(
        canonical_json_bytes(
            {
                "operation": operation,
                "payload_sha256": authority_payload_sha256,
                "schedule_id": schedule_id,
                "service_day": service_day.isoformat(),
            }
        )
    ).hexdigest()
    return f"fmp-run-{digest[:32]}"


def build_daily_refresh_plan(
    *,
    schedule_id: str,
    service_day: date,
    output_root: Path,
    authority_payload_sha256: str,
) -> DailyRefreshPlan:
    """Build stable per-day paths and independent operation identities."""

    if not schedule_id.strip():
        raise ValueError("schedule_id cannot be empty")
    if _SHA256.fullmatch(authority_payload_sha256) is None:
        raise ValueError("authority payload digest must contain 64 hexadecimal characters")
    day_root = output_root / service_day.isoformat()
    return DailyRefreshPlan(
        schedule_id=schedule_id,
        service_day=service_day,
        universe_run_id=_run_identity(
            schedule_id, service_day, "build-universe", authority_payload_sha256
        ),
        collection_run_id=_run_identity(
            schedule_id, service_day, "collect", authority_payload_sha256
        ),
        universe_path=day_root / "universe-manifest.json",
        collection_manifest_path=day_root / "active-collection-manifest.json",
        classification_path=day_root / "universe-classification.json",
        receipt_path=day_root / "collection-receipt.json",
    )


def load_norgate_universe(
    path: Path,
    *,
    expected_sha256: str,
    expected_row_count: int,
) -> tuple[NorgateUniverseRecord, ...]:
    """Read the exact signed Norgate security-master bytes through one descriptor."""

    with DescriptorTree.open_path(path.parent) as tree:
        payload = tree.read_bytes(path.name, max_bytes=_MAX_SECURITY_MASTER_BYTES)
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise ValueError("Norgate security-master digest differs from standing authority")
    table = pq.read_table(pa.BufferReader(payload), columns=["assetid", "symbol", "is_delisted"])
    if table.num_rows != expected_row_count:
        raise ValueError("Norgate security-master row count differs from standing authority")
    records: list[NorgateUniverseRecord] = []
    for row in table.to_pylist():
        assetid = row["assetid"]
        symbol = row["symbol"]
        is_delisted = row["is_delisted"]
        if type(assetid) is not int or not isinstance(symbol, str) or type(is_delisted) is not bool:
            raise ValueError("Norgate security-master contains invalid universe fields")
        records.append(
            NorgateUniverseRecord(
                assetid=assetid,
                symbol=symbol,
                is_delisted=is_delisted,
            )
        )
    return tuple(records)


def require_backfill_manifest_binding(
    manifest: UniverseManifest,
    *,
    norgate_security_master: Path,
    norgate_security_master_sha256: str,
    norgate_security_master_row_count: int,
    norgate_snapshot_date: date,
) -> None:
    """Require a backfill manifest to be exactly derivable from signed Norgate facts."""

    del norgate_snapshot_date  # Listing dates are rejected; the authority signs backfill_from.
    if any(
        entry.ipo_date is not None or entry.delisted_date is not None for entry in manifest.entries
    ):
        raise ValueError(
            "backfill manifest listing dates are not authenticated by signed Norgate facts"
        )
    supplied = _canonical_manifest_bindings(manifest)
    norgate_records = load_norgate_universe(
        norgate_security_master,
        expected_sha256=norgate_security_master_sha256,
        expected_row_count=norgate_security_master_row_count,
    )
    if supplied != _expected_norgate_bindings(norgate_records):
        raise ValueError("backfill manifest is not bound to signed Norgate universe facts")


def classification_document(
    classification: DailyUniverseClassification,
    *,
    norgate_security_master_sha256: str,
    norgate_snapshot_date: date,
) -> dict[str, object]:
    """Render a machine-consumed quarantine report without granting identity."""

    return {
        "contract": "aegis-alpha/fmp-daily-universe-classification",
        "version": 1,
        "norgate_security_master_sha256": norgate_security_master_sha256,
        "norgate_snapshot_date": norgate_snapshot_date.isoformat(),
        "counts": {
            disposition.value: len(classification.symbols(disposition))
            for disposition in DailyDisposition
        },
        "ignored_delisted_symbols": list(classification.ignored_delisted_symbols),
        "entries": [
            {
                "symbol": entry.symbol,
                "disposition": entry.disposition.value,
                "norgate_assetid": entry.norgate_assetid,
            }
            for entry in classification.entries
        ],
    }


def collection_manifest_document(
    classification: DailyUniverseClassification,
) -> dict[str, object]:
    return universe_manifest_document(classification.collection_manifest)
