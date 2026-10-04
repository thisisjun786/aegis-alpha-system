"""Register retained source strategy records as immutable versioned definitions.

A source strategy record is the request a strategy's author wrote down, preserved
losslessly in the private source library. Registration copies each record into the
private strategy store as an `aas-strategy-definition-v1` document whose version is
its own content hash, and derives the inputs it names onto dataset ids through a
versioned requirement map. A definition is not an engine bundle: it grants no
execution, research or backtest eligibility, and `strategy_versions` stays reserved
for validated bundles.

The registry is an add-on schema inside `strategies.sqlite3` with its own version
row, like the source library. Registration is one state intent, one private
transaction ending in a registration marker, then completion; `db recover` finishes
a committed marker by re-deriving what it covers.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from fractions import Fraction
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.storage.state import atomic, complete_operation, get_operation, prepare_operation

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

__all__ = [
    "DEFINITION_SCHEMA",
    "OPERATION_KIND",
    "REGISTRY_REQUEST_SCHEMA",
    "REQUIREMENT_MAP",
    "SOURCE_FORMAT",
    "Definition",
    "Requirement",
    "admit_registry",
    "definition_document",
    "definition_requirements",
    "list_definitions",
    "plan_registration",
    "recover_registration",
    "register_strategies",
    "registry_source_references",
    "verify_registry",
]

DEFINITION_SCHEMA: Final = "aas-strategy-definition-v1"
REGISTRY_REQUEST_SCHEMA: Final = "aas-strategy-registry-request-v1"
SOURCE_FORMAT: Final = "snowball-request@1"
REQUIREMENT_MAP: Final = "aas-strategy-requirement-map-v1"
OPERATION_KIND: Final = "strategy_registry"
_VERSION_PREFIX: Final = "def-"
_VERSION_HEX: Final = 16
_MAX_DOCUMENT_BYTES: Final = 1024 * 1024
_MAX_STRATEGIES: Final = 100_000
# The repository's serial default when no compute budget is configured. Table admission
# charges a whole retained table, including the derived column registration never reads.
_DEFAULT_BUDGET: Final = ComputeBudget(Fraction(1), 512 * 1024 * 1024)

# The three retained tables a source of this format carries, each with exact columns.
_STRATEGY_COLUMNS: Final = (
    "id",
    "title",
    "source_type",
    "country",
    "is_personal",
    "report_path",
    "request_json",
    "normalized_json",
    "exact_hash",
    "rule_hash",
    "family_hash",
    "start_date",
    "finish_date",
    "source_data_basis",
    "quality_status",
)
_SOURCE_TABLES: Final = {
    "strategy": _STRATEGY_COLUMNS,
    "asset_dependency": ("strategy_id", "role", "ordinal", "raw_token_json", "token_kind"),
    "macro_dependency": ("strategy_id", "ordinal", "raw_json"),
}
# normalized_json is the source's own derived structure; its hashes are kept instead.
_READ_COLUMNS: Final = tuple(name for name in _STRATEGY_COLUMNS if name != "normalized_json")

# Every request path that names an asset, in derivation order. A list path yields one
# requirement per element; a scalar path yields one when present. The benchmark is a
# priced input the source's own dependency table does not list.
_ASSET_PATHS: Final = (
    ("/offensive", ("offensive",)),
    ("/defensive_rule/defensive", ("defensive_rule", "defensive")),
    ("/defensive_rule/unallocated", ("defensive_rule", "unallocated")),
    ("/canary/etf_list", ("canary", "etf_list")),
    ("/defensive_rule/abs_compare", ("defensive_rule", "abs_compare")),
    ("/asset_selection_rule/abs_compare", ("asset_selection_rule", "abs_compare")),
)
_BENCHMARK_PATH: Final = "/benchmark"
_MACRO_PATH: Final = "/crash_protection/crash_protector"

# aas-strategy-requirement-map-v1. Changing any of this is a new map id, and so a new
# version of every definition it touches; a stored definition never changes meaning.
_CASH: Final = "CASH"
_KR_CODE: Final = re.compile(r"\d{6}")
_US_TICKER: Final = re.compile(r"[A-Z][A-Z0-9]{0,5}(?:[.-][A-Z0-9]{1,2})?")
# Benchmark names of the source format that are composites or indices, not listings.
_COMPOSITE_BENCHMARKS: Final = frozenset({"6040", "SP500", "NASDAQ", "KOSPI"})
_KR_PRICES: Final = "prices.kr.eodhd"
_US_PRICES: Final = "prices.us.norgate"
# Macro functions that name an ALFRED series one to one. Every other function is a
# derived series with no registered definition yet.
_ALFRED_SERIES: Final = frozenset({"T10Y2Y", "T10Y3M"})
_US_MACRO: Final = "macro.us.alfred"
# A request states the currency it is measured in. A mapped price of a market quoted in
# another currency is converted through the USD/KRW fixing, an input of its own.
_FX_PATH: Final = "/exchange"
_MARKET_CURRENCY: Final = {"us": "USD", "kr": "KRW"}
_USDKRW: Final = "USD/KRW"
_USDKRW_FX: Final = "fx.usdkrw.norgate"

_DDL: Final = """
CREATE TABLE strategy_registry_schema (
    version INTEGER PRIMARY KEY, checksum TEXT NOT NULL
) STRICT;
CREATE TABLE strategy_registrations (
    operation_id TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    definitions INTEGER NOT NULL,
    registered_at_us INTEGER NOT NULL,
    CHECK(length(request_hash) = 64 AND request_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK(length(source_sha256) = 64 AND source_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK(definitions >= 0),
    CHECK(registered_at_us >= 0)
) STRICT;
CREATE TABLE strategy_definitions (
    strategy_id TEXT NOT NULL REFERENCES strategies(strategy_id),
    version TEXT NOT NULL,
    definition_schema TEXT NOT NULL,
    document BLOB NOT NULL,
    document_sha256 TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    country TEXT NOT NULL,
    registered_at_us INTEGER NOT NULL,
    PRIMARY KEY (strategy_id, version),
    CHECK(length(document_sha256) = 64 AND document_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(version = 'def-' || substr(document_sha256, 1, 16)),
    CHECK(registered_at_us >= 0)
) STRICT;
CREATE TABLE strategy_definition_requirements (
    strategy_id TEXT NOT NULL,
    version TEXT NOT NULL,
    role TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    domain TEXT NOT NULL,
    token TEXT NOT NULL,
    market TEXT,
    dataset_id TEXT,
    series_id TEXT,
    mapping TEXT NOT NULL,
    reason TEXT,
    PRIMARY KEY (strategy_id, version, role, ordinal),
    FOREIGN KEY (strategy_id, version) REFERENCES strategy_definitions(strategy_id, version),
    CHECK(ordinal >= 0),
    CHECK(domain IN ('prices', 'macro', 'cash', 'fx')),
    CHECK(mapping IN ('mapped', 'unmapped', 'not_applicable')),
    CHECK((mapping = 'mapped') = (dataset_id IS NOT NULL)),
    CHECK((mapping = 'mapped') = (reason IS NULL))
) STRICT;
CREATE TABLE strategy_definition_sources (
    strategy_id TEXT NOT NULL,
    version TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES strategy_registrations(operation_id),
    source_table TEXT NOT NULL,
    table_digest TEXT NOT NULL,
    source_row INTEGER NOT NULL,
    PRIMARY KEY (strategy_id, version, operation_id),
    FOREIGN KEY (strategy_id, version) REFERENCES strategy_definitions(strategy_id, version),
    CHECK(length(table_digest) = 64 AND table_digest NOT GLOB '*[^0-9a-f]*'),
    CHECK(source_row >= 0)
) STRICT;
CREATE INDEX strategy_definition_requirements_dataset
    ON strategy_definition_requirements(dataset_id);
CREATE INDEX strategy_definition_sources_operation
    ON strategy_definition_sources(operation_id);
""" + "\n".join(
    f"CREATE TRIGGER immutable_{table}_{action.lower()} BEFORE {action} ON {table} "
    "BEGIN SELECT RAISE(ABORT,'immutable strategy registry evidence'); END;"
    for table in (
        "strategy_registry_schema",
        "strategy_registrations",
        "strategy_definitions",
        "strategy_definition_requirements",
        "strategy_definition_sources",
    )
    for action in ("UPDATE", "DELETE")
)
_SCHEMA_VERSION: Final = 1
_CHECKSUM: Final = hashlib.sha256(_DDL.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Requirement:
    """One input a definition names, and where requirement map v1 says it is read."""

    role: str
    ordinal: int
    domain: str
    token: str
    market: str | None
    dataset_id: str | None
    series_id: str | None
    mapping: str
    reason: str | None


@dataclass(frozen=True, slots=True)
class Definition:
    """One canonical definition document, its content version and its source row."""

    strategy_id: str
    version: str
    document: bytes
    document_sha256: str
    title: str
    country: str
    source_row: int
    requirements: tuple[Requirement, ...]


def admit_registry(connection: sqlite3.Connection, *, create: bool) -> bool:
    """Return whether the add-on is installed, installing it when asked."""
    present = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='strategy_registry_schema'"
    ).fetchone()
    if present is None:
        if not create:
            return False
        try:
            # The script leaves its transaction open, so the receipt row commits with the DDL.
            connection.executescript("BEGIN IMMEDIATE;" + _DDL)
            connection.execute(
                "INSERT INTO strategy_registry_schema VALUES (?,?)", (_SCHEMA_VERSION, _CHECKSUM)
            )
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
    rows = connection.execute("SELECT version,checksum FROM strategy_registry_schema LIMIT 2")
    if [tuple(row) for row in rows] != [(_SCHEMA_VERSION, _CHECKSUM)]:
        raise ValueError("unsupported strategy registry schema/checksum")
    return True


def _text(value: object, field: str, *, trimmed: bool = True) -> str:
    if not isinstance(value, str) or not value.strip() or (trimmed and value != value.strip()):
        raise ValueError(f"source strategy {field} must be nonempty text")
    return value


def _optional_text(value: object, field: str) -> str | None:
    return None if value is None else _text(value, field, trimmed=False)


def definition_document(row: Mapping[str, object]) -> dict[str, object]:
    """Build the definition document of one retained strategy row, losslessly.

    Every source column except the source's own derived `normalized_json` is kept with
    its value; the request is decoded so its canonical bytes are the document's.
    """
    request_text = _text(row["request_json"], "request_json")
    request = json.loads(request_text)
    if not isinstance(request, dict):
        raise ValueError("source strategy request must be an object")  # noqa: TRY004
    personal = row["is_personal"]
    if type(personal) is not int or personal not in {0, 1}:
        raise ValueError("source strategy is_personal must be 0 or 1")
    hashes = {
        name: _text(row[f"{name}_hash"], f"{name}_hash") for name in ("exact", "rule", "family")
    }
    return {
        "schema_version": DEFINITION_SCHEMA,
        "strategy_id": _text(row["id"], "id"),
        "source_format": SOURCE_FORMAT,
        "requirement_map": REQUIREMENT_MAP,
        "title": _text(row["title"], "title", trimmed=False),
        "country": _text(row["country"], "country"),
        "source_type": _text(row["source_type"], "source_type"),
        "is_personal": personal,
        "report_path": _optional_text(row["report_path"], "report_path"),
        "start_date": _optional_text(row["start_date"], "start_date"),
        "finish_date": _optional_text(row["finish_date"], "finish_date"),
        "data_basis": _optional_text(row["source_data_basis"], "source_data_basis"),
        "quality_status": _optional_text(row["quality_status"], "quality_status"),
        "source_hashes": hashes,
        "request": request,
    }


def _at(request: Mapping[str, object], keys: Sequence[str]) -> object:
    value: object = request
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = cast("Mapping[str, object]", value).get(key)
    return value


def _price(role: str, ordinal: int, token: str) -> Requirement:
    if token == _CASH:
        return Requirement(role, ordinal, "cash", token, None, None, None, "not_applicable", "cash")
    if role == _BENCHMARK_PATH and token in _COMPOSITE_BENCHMARKS:
        reason = "composite_benchmark"
    elif _KR_CODE.fullmatch(token):
        return Requirement(role, ordinal, "prices", token, "kr", _KR_PRICES, None, "mapped", None)
    elif _US_TICKER.fullmatch(token):
        return Requirement(role, ordinal, "prices", token, "us", _US_PRICES, None, "mapped", None)
    else:
        reason = "unrecognized_token"
    return Requirement(role, ordinal, "prices", token, None, None, None, "unmapped", reason)


def _macro(ordinal: int, function: str) -> Requirement:
    if function in _ALFRED_SERIES:
        return Requirement(
            _MACRO_PATH, ordinal, "macro", function, "us", _US_MACRO, function, "mapped", None
        )
    return Requirement(
        _MACRO_PATH, ordinal, "macro", function, None, None, None, "unmapped", "derived_series"
    )


def _fx(request: Mapping[str, object], rows: Sequence[Requirement]) -> Requirement | None:
    """The conversion a request's currency needs for the mapped prices it reads, if any."""
    markets = {r.market for r in rows if r.domain == "prices" and r.mapping == "mapped"}
    if not markets:
        return None
    currency = request.get("exchange")
    if currency is None:
        return Requirement(_FX_PATH, 0, "fx", "", None, None, None, "unmapped", "missing_currency")
    currency = _text(currency, _FX_PATH)
    if currency not in _MARKET_CURRENCY.values():
        reason = "unrecognized_currency"
        return Requirement(_FX_PATH, 0, "fx", currency, None, None, None, "unmapped", reason)
    if {_MARKET_CURRENCY[cast("str", market)] for market in markets} <= {currency}:
        return None
    return Requirement(_FX_PATH, 0, "fx", currency, None, _USDKRW_FX, _USDKRW, "mapped", None)


def _asset_tokens(request: Mapping[str, object]) -> Iterator[tuple[str, int, str]]:
    for role, keys in _ASSET_PATHS:
        value = _at(request, keys)
        if value is None:
            continue
        values = value if isinstance(value, list) else [value]
        for ordinal, token in enumerate(values):
            yield role, ordinal, _text(token, role)


def _macro_signals(request: Mapping[str, object]) -> list[Mapping[str, object]]:
    signals = _at(request, ("crash_protection", "crash_protector"))
    if signals is None:
        return []
    if not isinstance(signals, list) or not all(isinstance(s, Mapping) for s in signals):
        raise ValueError("source crash_protector must be an array of objects")
    return cast("list[Mapping[str, object]]", signals)


def definition_requirements(document: Mapping[str, object]) -> tuple[Requirement, ...]:
    """Derive every named input of a definition through requirement map v1.

    Pure and deterministic: the stored rows are re-derived from the stored document.
    A constant weight keyed by an asset the request lists nowhere else is refused,
    so no priced input can go unlisted, and a mapped price quoted in another currency
    than the request's `exchange` adds the USD/KRW conversion as an `fx` row.
    """
    if document.get("requirement_map") != REQUIREMENT_MAP:
        raise ValueError("unsupported strategy requirement map")
    request = cast("Mapping[str, object]", document["request"])
    rows = [_price(role, ordinal, token) for role, ordinal, token in _asset_tokens(request)]
    benchmark = request.get("benchmark")
    if benchmark is not None:
        rows.append(_price(_BENCHMARK_PATH, 0, _text(benchmark, _BENCHMARK_PATH)))
    constants = _at(request, ("weight_calculation_rule", "constant"))
    if constants is not None:
        if not isinstance(constants, Mapping):
            raise ValueError("source constant weights must be an object")
        listed = {row.token for row in rows}
        if not set(cast("Mapping[str, object]", constants)) <= listed:
            raise ValueError("source constant weight names an unlisted asset")
    for ordinal, signal in enumerate(_macro_signals(request)):
        rows.append(_macro(ordinal, _text(signal.get("func"), _MACRO_PATH)))
    fx = _fx(request, rows)
    return tuple(rows) if fx is None else (*rows, fx)


def _definition(row: Mapping[str, object], source_row: int) -> Definition:
    document = definition_document(row)
    raw = canonical_json_bytes(document)
    if len(raw) > _MAX_DOCUMENT_BYTES:
        raise ValueError("strategy definition document exceeds its byte limit")
    digest = content_sha256(document)
    return Definition(
        strategy_id=cast("str", document["strategy_id"]),
        version=_VERSION_PREFIX + digest[:_VERSION_HEX],
        document=raw,
        document_sha256=digest,
        title=cast("str", document["title"]),
        country=cast("str", document["country"]),
        source_row=source_row,
        requirements=definition_requirements(document),
    )


@dataclass(frozen=True, slots=True)
class _Source:
    source_id: str
    source_sha256: str
    digests: Mapping[str, str]
    definitions: tuple[Definition, ...]
    mismatches: tuple[str, ...]

    @property
    def request_hash(self) -> str:
        return content_sha256(
            {
                "schema_version": REGISTRY_REQUEST_SCHEMA,
                "source_id": self.source_id,
                "source_sha256": self.source_sha256,
                "tables": dict(self.digests),
                "source_format": SOURCE_FORMAT,
                "definition_schema": DEFINITION_SCHEMA,
                "requirement_map": REQUIREMENT_MAP,
            }
        )

    @property
    def payload_hash(self) -> str:
        return _payload_hash(
            (d.strategy_id, d.version, d.document_sha256) for d in self.definitions
        )

    @property
    def operation_id(self) -> str:
        return "strategy-registry:" + self.request_hash


def _payload_hash(entries: Iterable[tuple[str, str, str]]) -> str:
    return content_sha256(sorted(entries))


def _rows(
    workspace: Workspace, source_id: str, source_sha256: str, budget: ComputeBudget
) -> tuple[dict[str, str], dict[str, list[tuple[int, Mapping[str, object]]]]]:
    from aegis_alpha.storage.source_library import (  # noqa: PLC0415
        admit_source_table,
        list_tables,
        source_entry,
    )
    from aegis_alpha.storage.source_reader import SourcePin, iter_source_rows  # noqa: PLC0415

    source = source_entry(workspace, source_id)
    if source is None or source["sha256"] != source_sha256:
        raise ValueError("source is missing or its SHA-256 differs from the request")
    if source["store"] != "strategies":
        raise ValueError("source strategy records live in the private strategy store")
    tables = {str(table["name"]): table for table in list_tables(workspace, source_id)}
    digests: dict[str, str] = {}
    rows: dict[str, list[tuple[int, Mapping[str, object]]]] = {}
    # All three tables are held at once, so each is admitted against what the earlier left.
    remaining = budget.available_bytes
    for name, columns in _SOURCE_TABLES.items():
        table = tables.get(name)
        if table is None or tuple(cast("list[str]", table["columns"])) != columns:
            raise ValueError(f"source is not {SOURCE_FORMAT}: table {name} is missing or differs")
        if cast("int", table["rows"]) > _MAX_STRATEGIES * 64:
            raise ValueError(f"source table {name} exceeds the registry row limit")
        remaining -= admit_source_table(
            workspace, source_id, name, max_materialization_bytes=remaining
        )
        digests[name] = str(table["digest"])
        pin = SourcePin(source_id, source_sha256, name, digests[name])
        selected = _READ_COLUMNS if name == "strategy" else columns
        rows[name] = [
            (index, row)
            for index, row in enumerate(
                row for batch in iter_source_rows(workspace, pin, columns=selected) for row in batch
            )
        ]
    return digests, rows


def _dependency_mismatches(
    definitions: Sequence[Definition], rows: Mapping[str, list[tuple[int, Mapping[str, object]]]]
) -> list[str]:
    """Strategies whose request disagrees with the source's own dependency tables."""
    assets: dict[str, set[tuple[str, int, str]]] = defaultdict(set)
    for _, row in rows["asset_dependency"]:
        token = json.loads(_text(row["raw_token_json"], "raw_token_json"))
        if row["token_kind"] != "string" or not isinstance(token, str):  # noqa: S105
            raise ValueError("source asset dependency must be a string token")
        assets[_text(row["strategy_id"], "strategy_id")].add(
            (str(row["role"]), cast("int", row["ordinal"]), token)
        )
    macros: dict[str, set[tuple[int, str]]] = defaultdict(set)
    for _, row in rows["macro_dependency"]:
        raw = json.loads(_text(row["raw_json"], "raw_json"))
        macros[_text(row["strategy_id"], "strategy_id")].add(
            (cast("int", row["ordinal"]), canonical_json_bytes(raw).decode())
        )
    known = {d.strategy_id for d in definitions}
    mismatched = {key for key in (*assets, *macros) if key not in known}
    for definition in definitions:
        request = json.loads(definition.document)["request"]
        derived = set(_asset_tokens(request))
        signals = {
            (ordinal, canonical_json_bytes(signal).decode())
            for ordinal, signal in enumerate(_macro_signals(request))
        }
        if derived != assets.get(definition.strategy_id, set()) or signals != macros.get(
            definition.strategy_id, set()
        ):
            mismatched.add(definition.strategy_id)
    return sorted(mismatched)


def _read_source(
    workspace: Workspace, source_id: str, source_sha256: str, budget: ComputeBudget | None
) -> _Source:
    digests, rows = _rows(workspace, source_id, source_sha256, budget or _DEFAULT_BUDGET)
    if len(rows["strategy"]) > _MAX_STRATEGIES:
        raise ValueError("source holds more strategies than one registration admits")
    definitions = tuple(_definition(row, index) for index, row in rows["strategy"])
    duplicates = [key for key, n in Counter(d.strategy_id for d in definitions).items() if n > 1]
    if duplicates:
        raise ValueError("source names a strategy id more than once: " + duplicates[0])
    return _Source(
        source_id,
        source_sha256,
        digests,
        definitions,
        tuple(_dependency_mismatches(definitions, rows)),
    )


def _stored_versions(connection: sqlite3.Connection) -> dict[tuple[str, str], str]:
    if not admit_registry(connection, create=False):
        return {}
    return {
        (row[0], row[1]): row[2]
        for row in connection.execute(
            "SELECT strategy_id,version,document_sha256 FROM strategy_definitions"
        )
    }


def _catalog(workspace: Workspace) -> dict[str, int]:
    """Committed version count of every dataset id the state catalog names."""
    counts = {str(row[0]): 0 for row in workspace.state.execute("SELECT dataset_id FROM datasets")}
    for row in workspace.state.execute(
        "SELECT dataset_id,count(*) FROM dataset_versions WHERE status='committed' "
        "GROUP BY dataset_id"
    ):
        counts[str(row[0])] = int(row[1])
    return counts


def _report(workspace: Workspace, source: _Source) -> dict[str, object]:
    strategies = cast("sqlite3.Connection", workspace.strategies)
    stored = _stored_versions(strategies)
    known = {
        str(row[0]) for row in strategies.execute("SELECT strategy_id FROM strategies").fetchall()
    }
    stored_ids = {key[0] for key in stored}
    for definition in source.definitions:
        sha = stored.get((definition.strategy_id, definition.version))
        if sha is not None and sha != definition.document_sha256:
            raise ValueError("strategy definition version already identifies other content")
    requirements = [r for d in source.definitions for r in d.requirements]
    catalog = _catalog(workspace)
    datasets: dict[str, dict[str, object]] = {}
    for definition in source.definitions:
        for requirement in definition.requirements:
            if requirement.dataset_id is None:
                continue
            entry = datasets.setdefault(
                requirement.dataset_id,
                {
                    "dataset_id": requirement.dataset_id,
                    "requirements": 0,
                    "strategies": set(),
                    "in_catalog": requirement.dataset_id in catalog,
                    "committed_versions": catalog.get(requirement.dataset_id, 0),
                },
            )
            entry["requirements"] = cast("int", entry["requirements"]) + 1
            cast("set[str]", entry["strategies"]).add(definition.strategy_id)
    unmapped = Counter(
        (r.domain, r.reason, r.token) for r in requirements if r.mapping == "unmapped"
    )
    return {
        "source_id": source.source_id,
        "source_sha256": source.source_sha256,
        "tables": dict(source.digests),
        "operation_id": source.operation_id,
        "strategies": len(source.definitions),
        "new_strategies": sum(d.strategy_id not in known for d in source.definitions),
        "new_versions": sum((d.strategy_id, d.version) not in stored for d in source.definitions),
        "reused_versions": sum((d.strategy_id, d.version) in stored for d in source.definitions),
        "strategies_with_other_versions": sum(
            d.strategy_id in stored_ids and (d.strategy_id, d.version) not in stored
            for d in source.definitions
        ),
        "countries": dict(sorted(Counter(d.country for d in source.definitions).items())),
        "requirements": {
            "total": len(requirements),
            "by_mapping": dict(sorted(Counter(r.mapping for r in requirements).items())),
            "by_domain": dict(sorted(Counter(r.domain for r in requirements).items())),
        },
        "datasets": [
            {**entry, "strategies": len(cast("set[str]", entry["strategies"]))}
            for _, entry in sorted(datasets.items())
        ],
        "unmapped": [
            {"domain": domain, "reason": reason, "token": token, "requirements": count}
            for (domain, reason, token), count in sorted(unmapped.items(), key=str)
        ],
        "dependency_mismatches": list(source.mismatches),
        "execution_eligible": False,
    }


def plan_registration(
    workspace: Workspace,
    source_id: str,
    source_sha256: str,
    *,
    budget: ComputeBudget | None = None,
) -> dict[str, object]:
    """Report what registering one source would add, reconciled with the catalog; no writes."""
    if workspace.strategies is None:
        raise ValueError("strategy registration requires the private strategy store")
    source = _read_source(workspace, source_id, source_sha256, budget)
    return {"plan": True, **_report(workspace, source), "writes": 0}


def register_strategies(
    workspace: Workspace,
    source_id: str,
    source_sha256: str,
    *,
    budget: ComputeBudget | None = None,
) -> dict[str, object]:
    """Register every strategy record of one source; a repeated request is reused."""
    strategies = workspace.strategies
    if strategies is None:
        raise ValueError("strategy registration requires the private strategy store")
    source = _read_source(workspace, source_id, source_sha256, budget)
    if source.mismatches:
        raise ValueError(
            "source dependency tables disagree with their requests: "
            + ", ".join(source.mismatches[:5])
        )
    report = _report(workspace, source)
    prepare_operation(
        workspace.state,
        operation_id=source.operation_id,
        kind=OPERATION_KIND,
        request_hash=source.request_hash,
        target_id=source_id,
        expected_parent=None,
        payload_hash=source.payload_hash,
    )
    admit_registry(strategies, create=True)
    reused = _marker(strategies, source.operation_id) is not None
    if not reused:
        _write(strategies, source)
    operation = cast("dict[str, object]", get_operation(workspace.state, source.operation_id))
    if not _verify_registration(strategies, operation):
        raise ValueError("strategy registration has no matching private marker")
    complete_operation(workspace.state, source.operation_id, source.request_hash)
    return {"plan": False, **report, "reused": reused}


def _write(connection: sqlite3.Connection, source: _Source) -> None:
    now = time.time_ns() // 1000
    with atomic(connection):
        connection.execute(
            "INSERT INTO strategy_registrations VALUES (?,?,?,?,?,?,?)",
            (
                source.operation_id,
                source.request_hash,
                source.source_id,
                source.source_sha256,
                source.payload_hash,
                len(source.definitions),
                now,
            ),
        )
        for definition in source.definitions:
            connection.execute(
                "INSERT INTO strategies VALUES (?,?,'active') ON CONFLICT(strategy_id) DO NOTHING",
                (definition.strategy_id, definition.title.strip()),
            )
            stored = connection.execute(
                "SELECT document_sha256 FROM strategy_definitions "
                "WHERE strategy_id=? AND version=?",
                (definition.strategy_id, definition.version),
            ).fetchone()
            if stored is not None and stored[0] != definition.document_sha256:
                raise ValueError("strategy definition version already identifies other content")
            if stored is None:
                connection.execute(
                    "INSERT INTO strategy_definitions VALUES (?,?,?,?,?,?,?,?)",
                    (
                        definition.strategy_id,
                        definition.version,
                        DEFINITION_SCHEMA,
                        definition.document,
                        definition.document_sha256,
                        definition.title,
                        definition.country,
                        now,
                    ),
                )
                connection.executemany(
                    "INSERT INTO strategy_definition_requirements VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    [
                        (definition.strategy_id, definition.version, *_fields(requirement))
                        for requirement in definition.requirements
                    ],
                )
            connection.execute(
                "INSERT INTO strategy_definition_sources VALUES (?,?,?,?,?,?)",
                (
                    definition.strategy_id,
                    definition.version,
                    source.operation_id,
                    "strategy",
                    source.digests["strategy"],
                    definition.source_row,
                ),
            )


def _fields(requirement: Requirement) -> tuple[object, ...]:
    return (
        requirement.role,
        requirement.ordinal,
        requirement.domain,
        requirement.token,
        requirement.market,
        requirement.dataset_id,
        requirement.series_id,
        requirement.mapping,
        requirement.reason,
    )


def _marker(connection: sqlite3.Connection, operation_id: str) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT operation_id,request_hash,source_id,source_sha256,payload_hash,definitions "
        "FROM strategy_registrations WHERE operation_id=?",
        (operation_id,),
    ).fetchone()


def _verify_definition(connection: sqlite3.Connection, strategy_id: str, version: str) -> None:
    row = connection.execute(
        "SELECT definition_schema,document,document_sha256,title,country "
        "FROM strategy_definitions WHERE strategy_id=? AND version=?",
        (strategy_id, version),
    ).fetchone()
    if row is None:
        raise ValueError("strategy definition is not registered")
    document = json.loads(row["document"])
    if (
        row["definition_schema"] != DEFINITION_SCHEMA
        or document.get("schema_version") != DEFINITION_SCHEMA
        or canonical_json_bytes(document) != row["document"]
        or content_sha256(document) != row["document_sha256"]
        or _VERSION_PREFIX + row["document_sha256"][:_VERSION_HEX] != version
        or (document["strategy_id"], document["title"], document["country"])
        != (strategy_id, row["title"], row["country"])
    ):
        raise ValueError("stored strategy definition does not match its content hash")
    stored = [
        tuple(r)
        for r in connection.execute(
            "SELECT role,ordinal,domain,token,market,dataset_id,series_id,mapping,reason "
            "FROM strategy_definition_requirements WHERE strategy_id=? AND version=? "
            "ORDER BY role,ordinal",
            (strategy_id, version),
        )
    ]
    derived = sorted(
        (_fields(r) for r in definition_requirements(document)),
        key=lambda value: (cast("str", value[0]), cast("int", value[1])),
    )
    if stored != derived:
        raise ValueError("stored strategy requirements do not match the definition")


def _verify_registration(connection: sqlite3.Connection, operation: Mapping[str, object]) -> bool:
    """Check one intent's marker and everything it covers; an absent marker stays pending."""
    if not admit_registry(connection, create=False):
        return False
    marker = _marker(connection, str(operation["operation_id"]))
    if marker is None:
        return False
    if (
        operation["kind"] != OPERATION_KIND
        or marker["request_hash"] != operation["request_hash"]
        or marker["source_id"] != operation["target_id"]
        or marker["payload_hash"] != operation["payload_hash"]
        or operation["expected_parent"] is not None
    ):
        raise ValueError("strategy registration marker does not match its intent")
    covered = connection.execute(
        "SELECT s.strategy_id,s.version,d.document_sha256 FROM strategy_definition_sources s "
        "JOIN strategy_definitions d USING(strategy_id,version) WHERE s.operation_id=?",
        (marker["operation_id"],),
    ).fetchall()
    if (
        len(covered) != marker["definitions"]
        or _payload_hash(tuple(row) for row in covered) != marker["payload_hash"]
    ):
        raise ValueError("strategy registration marker does not cover its definitions")
    for row in covered:
        _verify_definition(connection, row[0], row[1])
    return True


def recover_registration(workspace: Workspace, operation: Mapping[str, object]) -> bool:
    """Complete an intent whose private marker re-derives exactly; never re-reads a source."""
    if workspace.strategies is None or not _verify_registration(workspace.strategies, operation):
        return False
    complete_operation(
        workspace.state, str(operation["operation_id"]), str(operation["request_hash"])
    )
    return True


def verify_registry(workspace: Workspace) -> dict[str, object] | None:
    """Verify every definition and both directions of the marker/intent graph."""
    strategies = workspace.strategies
    if strategies is None or not admit_registry(strategies, create=False):
        return None
    operations = {
        str(row["operation_id"]): dict(row)
        for row in workspace.state.execute(
            "SELECT * FROM storage_operations WHERE kind=?", (OPERATION_KIND,)
        )
    }
    for (operation_id,) in strategies.execute("SELECT operation_id FROM strategy_registrations"):
        operation = operations.get(operation_id)
        if operation is None or operation["phase"] not in {"PREPARED", "COMPLETED"}:
            raise ValueError("strategy registration marker has no matching active intent")
    registrations = 0
    for operation in operations.values():
        committed = _verify_registration(strategies, operation)
        if not committed and operation["phase"] == "COMPLETED":
            raise ValueError("completed strategy registration intent has no private marker")
        registrations += committed
    if strategies.execute(
        "SELECT 1 FROM strategy_definitions d WHERE NOT EXISTS (SELECT 1 FROM "
        "strategy_definition_sources s WHERE s.strategy_id=d.strategy_id AND s.version=d.version)"
    ).fetchone():
        raise ValueError("strategy definition has no registration source")
    definitions = strategies.execute(
        "SELECT strategy_id,version FROM strategy_definitions"
    ).fetchall()
    for row in definitions:
        _verify_definition(strategies, row[0], row[1])
    return {
        "registrations": registrations,
        "strategies": strategies.execute(
            "SELECT count(DISTINCT strategy_id) FROM strategy_definitions"
        ).fetchone()[0],
        "definitions": len(definitions),
    }


def registry_source_references(connection: sqlite3.Connection) -> set[str]:
    """Every source id a registration names; retirement counts these as references."""
    if not admit_registry(connection, create=False):
        return set()
    return {
        str(row[0]) for row in connection.execute("SELECT source_id FROM strategy_registrations")
    }


def list_definitions(
    connection: sqlite3.Connection, strategy_id: str | None = None
) -> list[dict[str, object]]:
    """Registered definitions with their requirement counts, oldest version first."""
    if not admit_registry(connection, create=False):
        return []
    query = (
        "SELECT d.strategy_id,s.name,d.version,d.document_sha256,d.title,d.country,"
        "d.registered_at_us,"
        "(SELECT count(*) FROM strategy_definition_requirements r WHERE "
        "r.strategy_id=d.strategy_id AND r.version=d.version) AS requirements,"
        "(SELECT count(*) FROM strategy_definition_requirements r WHERE "
        "r.strategy_id=d.strategy_id AND r.version=d.version AND r.mapping='unmapped') "
        "AS unmapped_requirements "
        "FROM strategy_definitions d JOIN strategies s USING(strategy_id)"
    )
    parameters: tuple[str, ...] = ()
    if strategy_id is not None:
        query += " WHERE d.strategy_id=?"
        parameters = (strategy_id,)
    query += " ORDER BY d.strategy_id,d.registered_at_us,d.version"
    return [
        {**dict(row), "execution_eligible": False}
        for row in connection.execute(query, parameters).fetchall()
    ]
