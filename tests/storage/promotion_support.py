"""Synthetic installations, source tables, identities and specs for promotion tests.

The source shape is the one ``eodhd.bars@1`` reads: provider symbol, session date,
unadjusted binary64 OHLCV, currency and a UTC collection instant. Every value is
synthetic; instrument anchors are minted from made-up Norgate asset IDs.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Final
from unittest.mock import patch

import pyarrow as pa

from aegis_alpha.storage import source_library
from aegis_alpha.storage.identity import parse_registry, register_identities, snapshot_identities
from aegis_alpha.storage.market import publish_generation
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import SourceContent, SourceFile
from aegis_alpha.storage.state import atomic

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from aegis_alpha.storage.workspace import Workspace

ZONE: Final = "Asia/Seoul"
UNBOUNDED: Final = -(2**63)
SCHEMA: Final = pa.schema(
    [
        ("provider_symbol", pa.string()),
        ("date", pa.date32()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("adjusted_close", pa.float64()),
        ("volume", pa.float64()),
        ("currency", pa.string()),
        ("retrieved_at", pa.timestamp("us", tz="UTC")),
    ]
)
SYMBOLS: Final = {"AAA.KO": "100001", "BBB.KQ": "100002", "CCC.KO": "100003"}
DAY_RULE: Final = {
    "rule": "local_day_end@1",
    "basis": "record",
    "input": "session_date",
    "args": {"timezone": ZONE},
}
DECIMALS: Final = {
    "open": "krw_tick@1",
    "high": "krw_tick@1",
    "low": "krw_tick@1",
    "close": "krw_tick@1",
    "volume": "exact@1",
}


def at(text: str) -> datetime:
    """A UTC instant from an ISO text without offset."""
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def us(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // (moment.resolution)


def bar(  # noqa: PLR0913 -- one synthetic bar spells every source column
    symbol: str,
    day: date,
    close: float | None,
    *,
    retrieved: datetime,
    volume: float | None = 1000.0,
    currency: str = "KRW",
) -> tuple[object, ...]:
    """A bar whose open, high and low equal its close; None makes an empty bar."""
    return (symbol, day, close, close, close, close, close, volume, currency, retrieved)


def add_source(
    workspace: Workspace,
    rows: list[tuple[object, ...]],
    *,
    tag: str,
    linked: datetime | None = None,
) -> dict[str, str]:
    """Commit ``rows`` as one content-addressed, linked source table; return its spec pin.

    ``linked`` fixes the clock the commit and its ``sl:`` link record, so a test can place
    the link's retrieval time among its synthetic collection times.
    """
    if linked is not None:
        stamp = us(linked) * 1000
        with patch.object(time, "time_ns", lambda: stamp):
            return add_source(workspace, rows, tag=tag)
    _, digest, size = put_raw(workspace.paths.raw, f"synthetic-export-{tag}".encode())
    content = SourceContent("synthetic", "kr-bars", 1, (SourceFile(digest, size),))
    columns = list(zip(*rows, strict=True)) if rows else [[] for _ in SCHEMA.names]
    table = pa.table(
        {name: list(column) for name, column in zip(SCHEMA.names, columns, strict=True)},
        schema=SCHEMA,
    )
    result = source_library.import_content_arrow(workspace, content, "bars", table.to_reader())
    tables = result["tables"]
    assert isinstance(tables, list)
    return {
        "source_id": content.source_id,
        "source_sha256": content.sha256,
        "table": "bars",
        "digest": str(tables[0]["digest"]),
    }


def register_symbols(
    workspace: Workspace, link: str, symbols: dict[str, str] | None = None, *, name: str = "kr"
) -> dict[str, str]:
    """Register an eodhd_symbol assertion per symbol and snapshot them; return the pin."""
    chosen = SYMBOLS if symbols is None else symbols
    document = {
        "schema": "aas-identity-registry-v1",
        "issuers": [],
        "instruments": [
            {
                "anchor_namespace": "norgate_assetid",
                "anchor_token": token,
                "issuer": None,
                "asset_type": "equity",
                "venue": "XKRX",
            }
            for token in sorted(set(chosen.values()))
        ],
        "assertions": [
            {
                "instrument": {"anchor_namespace": "norgate_assetid", "anchor_token": token},
                "provider": "eodhd",
                "namespace": "eodhd_symbol",
                "token": symbol,
                "valid_from_us": UNBOUNDED,
                "valid_to_us": None,
                "known_from_us": 1,
                "supersedes_assertion_id": None,
                "source_snapshot_id": "sl:" + link,
                "source_hash": hashlib.sha256(symbol.encode()).hexdigest(),
            }
            for symbol, token in sorted(chosen.items())
        ],
    }
    register_identities(workspace.state, parse_registry(document), apply=True)
    report = snapshot_identities(workspace.state, name, created_at_us=5, apply=True)
    return {"snapshot_id": str(report["snapshot_id"]), "content_hash": str(report["content_hash"])}


def spec(  # noqa: PLR0913 -- every spec field a test varies
    sources: list[dict[str, str]],
    identity: dict[str, str],
    *,
    dataset: str = "prices.kr.eodhd",
    parent: str | None = None,
    rules: Mapping[str, object] | None = None,
    tombstone: Mapping[str, object] | None = None,
    partition: dict[str, str] | None = None,
    quality: Sequence[Mapping[str, object]] | None = None,
    decimals: dict[str, str] | None = None,
) -> tuple[bytes, str]:
    """Exact spec bytes and their SHA-256."""
    document = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": "prices", "dataset_id": dataset, "parent": parent},
        "sources": sources,
        "mapper": {"name": "eodhd.bars@1", "args": {"timezone": ZONE}},
        "partition": partition,
        "time_rules": rules or {"available_at_us": DAY_RULE, "revision_known_at_us": DAY_RULE},
        "decimal_rule": decimals or DECIMALS,
        "quality_rules": quality or [],
        "tombstone_policy": tombstone or {"mode": "never"},
        "identity_snapshot": identity,
    }
    raw = json.dumps(document, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def full_snapshot(
    pin: dict[str, str], start: str, end: str, instruments: list[str] | None = None
) -> dict[str, object]:
    return {
        "mode": "absent_in_full_snapshot",
        "source": {"source_id": pin["source_id"], "table": pin["table"]},
        "scope": {"instruments": instruments, "from": start, "to": end},
    }


def publish_calendar(
    workspace: Workspace,
    sessions: Mapping[date, tuple[int | None, int | None]],
    *,
    dataset: str = "sessions.xkrx",
    sequence: int = 1,
    parent: str | None = None,
) -> dict[str, str]:
    """Publish and catalog a declared XKRX session generation; return its generation pin.

    ``sequence`` and ``parent`` extend an earlier generation of ``dataset`` with new sessions.
    """
    base = "cal" if dataset == "sessions.xkrx" else f"{dataset}-cal"
    generation_id = f"{base}-{sequence}"
    rows: list[dict[str, object]] = []
    for index, (day, (opened, closed)) in enumerate(sorted(sessions.items())):
        rows.append(
            {
                "revision_id": f"{generation_id}-{index}",
                "supersedes_revision_id": None,
                "op": "ASSERT",
                "available_at_us": 0,
                "revision_known_at_us": 0,
                "ingested_at_us": 1,
                "source_snapshot_id": "declared-calendar",
                "source_row_hash": hashlib.sha256(day.isoformat().encode()).hexdigest(),
                "calendar_id": "XKRX",
                "venue": "XKRX",
                "session_date": day,
                "open_at_us": opened,
                "close_at_us": closed,
                "status": "open" if closed is not None else "closed",
                "timezone_version": "synthetic",
            }
        )
    seed = b"declared-calendar" if generation_id == "cal-1" else generation_id.encode()
    request = hashlib.sha256(seed).hexdigest()
    version = str(sequence)
    marker = publish_generation(
        workspace.market,
        dataset_id=dataset,
        version=version,
        generation_id=generation_id,
        operation_id=f"op-{generation_id}" if generation_id != "cal-1" else "cal-op-1",
        request_hash=request,
        parent_id=parent,
        domain="calendar_sessions",
        rows=rows,
    )
    with atomic(workspace.state):
        if parent is None:
            workspace.state.execute(
                "INSERT INTO datasets VALUES (?,'calendar_sessions',?,'test')",
                (dataset, "aas-market-rowset-v1"),
            )
        workspace.state.execute(
            "INSERT INTO dataset_versions VALUES (?,?,?,?,?,?,?,?,"
            "'synthetic',?,NULL,NULL,?,'all','committed')",
            (
                dataset,
                version,
                generation_id,
                parent,
                sequence,
                marker["chain_hash"],
                request,
                "aas-market-rowset-v1",
                request,
                marker["row_count"],
            ),
        )
    return {
        "dataset_id": dataset,
        "version": version,
        "generation_id": generation_id,
        "chain_hash": str(marker["chain_hash"]),
        "manifest_hash": request,
    }


def prices(workspace: Workspace, generation_id: str) -> list[dict[str, object]]:
    """The stored rows of one generation, keyed for assertions."""
    cursor = workspace.market.execute(
        "SELECT instrument_id, session_date, op, close, available_at_us, revision_known_at_us, "
        "ingested_at_us, revision_id, supersedes_revision_id, record_id, source_row_hash, "
        "value_state FROM prices WHERE generation_id=? ORDER BY instrument_id, session_date",
        [generation_id],
    )
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
