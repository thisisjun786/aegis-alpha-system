"""Bounded, source-only research inventory reads for the local console."""

from __future__ import annotations

# ruff: noqa: S608 -- source identifiers originate in admitted manifests and are quoted.
import json
from typing import TYPE_CHECKING, cast

from aegis_alpha.storage import source_library_schema as schema
from aegis_alpha.storage.inspection import CATALOG_BYTES, MANIFEST_BYTES, SOURCE_LIMIT
from aegis_alpha.storage.state import get_operation

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

JSON_BYTES = 32768
RECORD_LIMIT = 100000
STRATEGY_SHAPES = (
    (
        "original",
        {
            "id",
            "title",
            "country",
            "request_json",
            "start_date",
            "finish_date",
            "source_data_basis",
            "quality_status",
        },
    ),
    ("derived", {"id", "title", "country", "status", "payload_json"}),
)
PRICE_SHAPES = (
    ("bars", {"instrument_id", "venue", "instrument_type", "provider_symbol", "date", "close"}),
    ("canonical", {"instrument_id", "observation_date", "close", "source_provider"}),
    ("raw", {"assetid", "symbol", "date", "close", "source"}),
    ("closes", {"symbol", "date", "close"}),
    ("issuer", {"symbol", "date", "market_price_raw", "nav_raw"}),
)
US_VENUES = {"US", "NASDAQ", "NYSE", "NYSE ARCA", "NYSE AMERICAN", "CBOE BZX", "OTC", "OTCID"}
KR_VENUES = {"KO", "KQ", "KR"}


def _visible(workspace: Workspace, operation_id: object, request_hash: object) -> bool:
    operation = get_operation(workspace.state, str(operation_id))
    return bool(
        operation
        and operation["phase"] == "COMPLETED"
        and operation["request_hash"] == request_hash
    )


def _sources(workspace: Workspace) -> tuple[list[dict[str, object]], bool]:
    """Read only manifest-admitted tables, with SQL text caps before JSON parsing."""
    if not schema.ensure(workspace):
        return [], False
    sources: list[dict[str, object]] = []
    scanned = metadata_bytes = 0
    truncated = False
    for store, connection in schema.connections(workspace).items():
        cursor = connection.execute(
            "SELECT source_id,operation_id,request_hash,"
            "CASE WHEN length(manifest_json)<=? THEN manifest_json ELSE NULL END "
            "FROM source_library_commits ORDER BY source_id LIMIT ?",
            [MANIFEST_BYTES, max(0, SOURCE_LIMIT - scanned)],
        )
        while row := cursor.fetchone():
            source_id, operation_id, request_hash, encoded = row
            scanned += 1
            metadata_bytes += len(str(encoded).encode())
            if metadata_bytes > CATALOG_BYTES:
                return sources, True
            if encoded is None:
                truncated = True
                continue
            if not _visible(workspace, operation_id, request_hash):
                continue
            manifest = json.loads(str(encoded))
            if not isinstance(manifest, dict) or manifest.get("store") != store:
                raise ValueError("invalid source manifest")
            tables = manifest.get("tables")
            if not isinstance(tables, list):
                raise TypeError("invalid source manifest tables")
            sources.append({"id": str(source_id), "store": store, "tables": tables})
    return sources, truncated or scanned >= SOURCE_LIMIT


def _shape(columns: object, shapes: tuple[tuple[str, set[str]], ...]) -> str | None:
    if not isinstance(columns, list) or not all(isinstance(value, str) for value in columns):
        return None
    names = set(cast("list[str]", columns))
    return next((name for name, required in shapes if required <= names), None)


def _assets(value: object) -> tuple[list[str] | None, list[str] | None, str]:
    if not isinstance(value, dict):
        return None, None, "unparsed"

    def listed(key: str) -> list[str] | None:
        values = value.get(key)
        return (
            [str(item) for item in values if isinstance(item, (str, int, float))]
            if isinstance(values, list)
            else None
        )

    offensive = listed("offensive") or listed("offensive_assets") or listed("assets")
    defensive_rule = value.get("defensive_rule")
    defensive = listed("defensive_assets") or (
        listed_from(defensive_rule, "defensive") if isinstance(defensive_rule, dict) else None
    )
    return offensive, defensive, "parsed"


def listed_from(value: dict[str, object], key: str) -> list[str] | None:
    items = value.get(key)
    return (
        [str(item) for item in items if isinstance(item, (str, int, float))]
        if isinstance(items, list)
        else None
    )


def strategies(workspace: Workspace) -> dict[str, object]:  # noqa: C901, PLR0912 -- bounded source adapters and partial-state handling.
    sources, truncated = _sources(workspace)
    collections: list[dict[str, object]] = []
    seen: set[tuple[str, tuple[str, ...]]] = set()
    byte_count = 0
    for source in sources:
        connection = schema.connections(workspace)[str(source["store"])]
        for table in cast("list[dict[str, object]]", source["tables"]):
            variant = _shape(table.get("columns"), STRATEGY_SHAPES)
            if variant is None:
                continue
            fingerprint = (str(table["digest"]), tuple(cast("list[str]", table["columns"])))
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            columns = set(cast("list[str]", table["columns"]))
            json_column = "request_json" if variant == "original" else "payload_json"
            selected = ["id", "title", "country"]
            selected.extend(
                column
                for column in (
                    "start_date",
                    "finish_date",
                    "source_data_basis",
                    "quality_status",
                    "status",
                )
                if column in columns
            )
            select = ",".join(
                f"substr(CAST({schema.quoted(column)} AS VARCHAR),1,2000)" for column in selected
            )
            select += (
                ",CASE WHEN length("
                + schema.quoted(json_column)
                + ")<=? THEN "
                + schema.quoted(json_column)
                + " ELSE NULL END"
            )
            records: list[dict[str, object]] = []
            cursor = connection.execute(
                "SELECT "
                + select
                + " FROM "
                + schema.quoted(str(table["target"]))
                + " ORDER BY "
                + schema.quoted("id")
                + " LIMIT ?",
                [JSON_BYTES, RECORD_LIMIT],
            )
            while row := cursor.fetchone():
                byte_count += sum(len(str(cell).encode()) for cell in row)
                if byte_count > CATALOG_BYTES:
                    truncated = True
                    break
                value = dict(zip([*selected, json_column], row, strict=True))
                parsed: object = None
                if value[json_column] is not None:
                    try:
                        parsed = json.loads(str(value[json_column]))
                    except json.JSONDecodeError:
                        parsed = None
                settings = (
                    parsed.get("economic_config", parsed) if isinstance(parsed, dict) else parsed
                )
                offensive, defensive, settings_status = _assets(settings)
                records.append(
                    {
                        "id": str(value["id"]),
                        "name": value["title"],
                        "country": value["country"],
                        "description": None,
                        "assets": offensive,
                        "defensive_assets": defensive,
                        "start_date": value.get("start_date"),
                        "end_date": value.get("finish_date"),
                        "data_basis": value.get("source_data_basis"),
                        "status": value.get("quality_status") or value.get("status"),
                        "settings_status": settings_status,
                        "source_id": source["id"],
                        "table": table["name"],
                    }
                )
            if records:
                collections.append(
                    {
                        "id": f"{source['id']}:{table['name']}",
                        "label": ("원본 전략" if variant == "original" else "가공 전략")
                        + f" · {source['id']!s}",
                        "kind": variant,
                        "records": records,
                    }
                )
            truncated = truncated or len(records) >= RECORD_LIMIT
    if workspace.strategies is not None:
        native: list[dict[str, object]] = []
        cursor = workspace.strategies.execute(
            "SELECT substr(strategy_id,1,240) AS strategy_id,substr(name,1,2000) AS name,"
            "substr(lifecycle,1,80) AS lifecycle FROM strategies "
            "WHERE EXISTS (SELECT 1 FROM strategy_versions v "
            "WHERE v.strategy_id=strategies.strategy_id) ORDER BY strategy_id LIMIT ?",
            (RECORD_LIMIT + 1,),
        )
        for row in cursor:
            byte_count += sum(len(str(cell).encode()) for cell in row)
            if len(native) >= RECORD_LIMIT or byte_count > CATALOG_BYTES:
                truncated = True
                break
            native.append(dict(row))
        if native:
            collections.append(
                {
                    "id": "native",
                    "label": "AAS 등록 전략",
                    "kind": "native",
                    "records": [
                        {
                            "id": r["strategy_id"],
                            "name": r["name"],
                            "country": None,
                            "description": None,
                            "assets": None,
                            "defensive_assets": None,
                            "start_date": None,
                            "end_date": None,
                            "data_basis": None,
                            "status": r["lifecycle"],
                            "settings_status": "unparsed",
                            "source_id": None,
                            "table": None,
                        }
                        for r in native[:RECORD_LIMIT]
                    ],
                }
            )
            truncated = truncated or len(native) > RECORD_LIMIT
    collections.sort(key=lambda item: (item["kind"] != "original", str(item["label"])))
    return {"collections": collections, "truncated": truncated}
