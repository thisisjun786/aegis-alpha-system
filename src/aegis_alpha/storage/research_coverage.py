"""Observed price ranges; identities are source-backed, never inferred from tickers."""

from __future__ import annotations

# ruff: noqa: S608 -- fixed SQL templates; manifest-owned identifiers are quoted.
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, cast

import duckdb

from aegis_alpha.storage import source_library_schema as schema
from aegis_alpha.storage.research_inspection import (
    KR_VENUES,
    PRICE_SHAPES,
    RECORD_LIMIT,
    US_VENUES,
    _shape,
    _sources,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from aegis_alpha.storage.workspace import Workspace

READ_SECONDS = 30
_ID_FIELDS = (
    "assetid",
    "symbol",
    "security_name",
    "database_or_watchlist",
    "exchange",
    "base_type",
    "subtype1",
    "is_etf",
    "source",
)


@contextmanager
def read_deadline(workspace: Workspace) -> Iterator[None]:
    """Interrupt only this admitted read connection; always disarm before release."""
    deadline = time.monotonic() + READ_SECONDS
    connections = schema.connections(workspace)
    for conn in connections.values():
        if isinstance(conn, sqlite3.Connection):
            conn.set_progress_handler(lambda: int(time.monotonic() >= deadline), 10000)
    timer = threading.Timer(READ_SECONDS, workspace.market.interrupt)
    timer.daemon = True
    timer.start()
    try:
        yield
    except (sqlite3.OperationalError, duckdb.InterruptException) as error:
        if time.monotonic() >= deadline:
            raise RuntimeError("research read deadline exceeded") from error
        raise
    finally:
        timer.cancel()
        timer.join()
        for conn in connections.values():
            if isinstance(conn, sqlite3.Connection):
                conn.set_progress_handler(None, 0)


def _category(venue: object, kind: object) -> str:
    venue_text, kind_text = str(venue).upper(), str(kind).upper()
    market = "us" if venue_text in US_VENUES else "kr" if venue_text in KR_VENUES else ""
    if not market or kind in (None, ""):
        return "unknown"
    if kind_text == "ETF":
        return market + "_etf"
    if kind_text in {"EQUITY", "COMMON STOCK"}:
        return market + "_equity"
    return "other"


def _metadata_category(item: dict[str, object]) -> str:
    flag = str(item.get("is_etf")).casefold()
    subtype = str(item.get("subtype1")).casefold()
    kind = "ETF" if flag in {"true", "1"} else "Equity" if subtype == "equity" else None
    return _category(item.get("exchange"), kind)


def _identities(
    workspace: Workspace,
    sources: list[dict[str, object]],
) -> tuple[dict[str, dict[str, object]], bool]:
    result: dict[str, dict[str, object]] = {}
    seen: set[str] = set()
    truncated = False
    for source in sources:
        connection = schema.connections(workspace)[str(source["store"])]
        for table in cast("list[dict[str, object]]", source["tables"]):
            if not set(_ID_FIELDS) <= set(cast("list[str]", table.get("columns", []))):
                continue
            digest = str(table["digest"])
            if digest in seen:
                continue
            seen.add(digest)
            selected = ",".join(
                f"substr(CAST({schema.quoted(field)} AS VARCHAR),1,240)" for field in _ID_FIELDS
            )
            cursor = connection.execute(
                f"SELECT {selected} FROM {schema.quoted(str(table['target']))} LIMIT ?",
                [RECORD_LIMIT + 1],
            )
            count = 0
            while row := cursor.fetchone():
                count += 1
                if count > RECORD_LIMIT or len(result) >= RECORD_LIMIT:
                    truncated = True
                    break
                item = dict(zip(_ID_FIELDS, row, strict=True))
                if str(item["source"]).casefold() != "norgate":
                    continue
                key = str(item["assetid"])
                prior = result.get(key)
                if prior is None:
                    result[key] = item
                elif not prior or any(prior[field] != item[field] for field in _ID_FIELDS):
                    result[key] = {}  # Conflicting metadata stays unknown on later snapshots.
    return result, truncated


def _aggregate_query(
    table: dict[str, object], variant: str, *, sqlite: bool
) -> tuple[str, list[str]]:
    identity = (
        "assetid"
        if variant == "raw"
        else "instrument_id"
        if variant in {"bars", "canonical"}
        else "symbol"
    )
    fields = [identity]
    if variant == "bars":
        fields += ["venue", "instrument_type", "provider_symbol"]
    elif variant in {"raw", "canonical"}:
        fields += ["source" if variant == "raw" else "source_provider"]
    dc = schema.quoted("observation_date" if variant == "canonical" else "date")
    pc = schema.quoted("market_price_raw" if variant == "issuer" else "close")
    if sqlite:
        day = f"date({dc}, '+0 days')"
        valid_date = f"typeof({dc})='text' AND length({dc})=10 AND {day}={dc}"
        valid_value = f"typeof({pc}) IN ('real','integer') AND abs({pc})<=1.7976931348623157e308"
    else:
        day = f"try_cast({dc} AS DATE)"
        valid_date = (
            f"{day} IS NOT NULL AND (typeof({dc}) IN "
            "('DATE','TIMESTAMP','TIMESTAMP_NS','TIMESTAMP_MS','TIMESTAMP_S') "
            f"OR CAST({dc} AS VARCHAR)=CAST({day} AS VARCHAR))"
        )
        valid_value = f"isfinite(try_cast({pc} AS DOUBLE))"
    valid = f"COALESCE(({valid_date}) AND ({valid_value}), false)"
    identity_sql = schema.quoted(identity)
    # Reject oversize identifiers instead of truncating them into false identity matches.
    group_sql = ",".join(schema.quoted(f) for f in fields)
    query = (
        f"SELECT {group_sql},MIN(CASE WHEN {valid} THEN {day} END),"
        f"MAX(CASE WHEN {valid} THEN {day} END),"
        f"SUM(CASE WHEN {valid} THEN 1 ELSE 0 END),"
        f"SUM(CASE WHEN COALESCE({valid_date},false) THEN 0 ELSE 1 END),"
        f"SUM(CASE WHEN COALESCE({valid_value},false) THEN 0 ELSE 1 END) "
        f"FROM {schema.quoted(str(table['target']))} "
        f"WHERE {identity_sql} IS NOT NULL AND length(CAST({identity_sql} AS VARCHAR))<=240 "
        f"GROUP BY {group_sql} LIMIT ?"
    )
    return query, [*fields, "first", "last", "rows", "invalid_dates", "invalid_values"]


def _record_identity(
    item: dict[str, object],
    variant: str,
    source: str,
    identities: dict[str, dict[str, object]],
) -> tuple[str, str, object, str]:
    provider = str(item.get("source_provider", item.get("source", ""))).casefold()
    raw_id = str(item.get("assetid", item.get("instrument_id", item.get("symbol"))))
    if variant in {"raw", "canonical"} and provider == "norgate":
        assetid = raw_id.removeprefix("norgate-instrument-")
        metadata = identities.get(assetid, {})
        return (
            "norgate-instrument-" + assetid,
            str(metadata.get("symbol", raw_id)),
            metadata.get("security_name"),
            _metadata_category(metadata),
        )
    if variant == "bars":
        return (
            raw_id,
            str(item["provider_symbol"]),
            None,
            _category(item["venue"], item["instrument_type"]),
        )
    if variant == "canonical":
        return f"{provider}:{raw_id}", raw_id, None, "unknown"
    return f"{source}:{raw_id}", raw_id, None, "unknown"


def coverage(workspace: Workspace) -> dict[str, object]:  # noqa: C901 -- bounded source adapters and partial-state handling.
    sources, truncated = _sources(workspace)
    identities, identity_limited = _identities(workspace, sources)
    truncated = truncated or identity_limited
    records: dict[str, dict[str, object]] = {}
    seen: set[tuple[str, tuple[str, ...]]] = set()
    unsupported = invalid_dates = invalid_values = 0
    for source in sources:
        connection = schema.connections(workspace)[str(source["store"])]
        for table in cast("list[dict[str, object]]", source["tables"]):
            columns = cast("list[str]", table.get("columns", []))
            variant = _shape(columns, PRICE_SHAPES)
            if variant is None:
                if source["store"] == "market":
                    unsupported += 1
                continue
            fingerprint = (str(table["digest"]), tuple(columns))
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            query, fields = _aggregate_query(
                table, variant, sqlite=isinstance(connection, sqlite3.Connection)
            )
            cursor = connection.execute(query, [RECORD_LIMIT + 1])
            count = 0
            while row := cursor.fetchone():
                count += 1
                if count > RECORD_LIMIT:
                    truncated = True
                    break
                item = dict(zip(fields, row, strict=True))
                invalid_dates += int(cast("int", item["invalid_dates"]))
                invalid_values += int(cast("int", item["invalid_values"]))
                if not item["rows"]:
                    continue
                identifier, symbol, name, category = _record_identity(
                    item, variant, str(source["id"]), identities
                )
                if identifier not in records and len(records) >= RECORD_LIMIT:
                    truncated = True
                    continue
                first, last = str(item["first"]), str(item["last"])
                record = records.setdefault(
                    identifier,
                    {
                        "id": identifier,
                        "symbol": symbol,
                        "name": name,
                        "category": category,
                        "first_date": first,
                        "last_date": last,
                        "rows": 0,
                        "_sources": set(),
                    },
                )
                if record["category"] != category:
                    record["category"] = "unknown"
                record["first_date"] = min(str(record["first_date"]), first)
                record["last_date"] = max(str(record["last_date"]), last)
                record["rows"] = int(cast("int", record["rows"])) + int(cast("int", item["rows"]))
                cast("set[str]", record["_sources"]).add(str(source["id"]))
    output = list(records.values())
    for record in output:
        record["source_count"] = len(cast("set[str]", record.pop("_sources")))
    output.sort(key=lambda item: (str(item["symbol"]), str(item["id"])))
    return {
        "items": output,
        "truncated": truncated,
        "unclassified_tables": unsupported,
        "invalid_dates": invalid_dates,
        "invalid_values": invalid_values,
        "native_status": "Imported source history only; native revision visibility is separate.",
    }
