"""The ``aas-promotion-v1`` document: one hashed request to promote pinned sources.

The exact bytes are strict UTF-8 JSON of at most 64 MiB, supplied with their SHA-256 and
kept in ``raw/`` when applied; their hash is the generation's ``transform_hash``. Unknown
or missing keys, duplicate keys, non-finite numbers and moving references such as
``latest`` are refused. The limit applies to the document only: promoted rows stay in
DuckDB.

```json
{
  "schema_version": "aas-promotion-v1",
  "target": {"domain": "prices", "dataset_id": "prices.kr.eodhd", "parent": null},
  "sources": [{"source_id": "...", "source_sha256": "...", "table": "bars", "digest": "..."}],
  "mapper": {"name": "eodhd.bars@1", "args": {"timezone": "Asia/Seoul"}},
  "partition": {"from": "2025-01-01", "to": "2026-01-01"},
  "time_rules": {"available_at_us": {...}, "revision_known_at_us": {...}},
  "decimal_rule": {"open": "krw_tick@1", "volume": "exact@1", "...": "..."},
  "quality_rules": [],
  "tombstone_policy": {"mode": "never"},
  "identity_snapshot": {"snapshot_id": "...", "content_hash": "..."}
}
```

``partition`` (or null) keeps the source rows whose mapper partition date falls in
``[from, to)``; a backfill promotes one partition per generation. A tombstone policy
``absent_in_full_snapshot`` names the one pinned source table that holds every record
of its ``scope`` (instrument IDs or null for all, and a date interval inside the
partition); records of the scope that no pinned source row carries become TOMBSTONEs.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Final, Literal

from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.market_inputs import GenerationPin
from aegis_alpha.storage.market_schema import COMMON, DOMAINS
from aegis_alpha.storage.membership_pins import IdentityPin
from aegis_alpha.storage.promotion import decimal_rules
from aegis_alpha.storage.promotion.mappers import Mapper, mapper
from aegis_alpha.storage.promotion.time_rules import TIME_COLUMNS, TimeRule, parse_rule
from aegis_alpha.storage.source_reader import SourcePin

if TYPE_CHECKING:
    from collections.abc import Mapping

SPEC_SCHEMA: Final = "aas-promotion-v1"
MAX_SPEC_BYTES: Final = 64 * 1024 * 1024
_ROOT: Final = frozenset(
    {
        "schema_version",
        "target",
        "sources",
        "mapper",
        "partition",
        "time_rules",
        "decimal_rule",
        "quality_rules",
        "tombstone_policy",
        "identity_snapshot",
    }
)
_DATASET: Final = re.compile(r"[a-z][a-z0-9]*(?:\.[a-z0-9]+){1,6}")
_SHA256: Final = re.compile(r"[0-9a-f]{64}")
_MAX_DATASET: Final = 200
CROSS_PROVIDER: Final = "cross_provider_mismatch@1"
_PIN_KEYS: Final = ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")


@dataclass(frozen=True, slots=True)
class Partition:
    start: date
    end: date


@dataclass(frozen=True, slots=True)
class TombstonePolicy:
    mode: Literal["never", "absent_in_full_snapshot"]
    source: SourcePin | None = None
    instruments: tuple[str, ...] | None = None
    start: date | None = None
    end: date | None = None


@dataclass(frozen=True, slots=True)
class QualityRule:
    """``cross_provider_mismatch@1``: flag a value that differs beyond ``tolerance``.

    ``reference`` pins another provider's published prices; ``column`` is compared on the
    same instrument, session date, interval, bar end, basis and currency (every price key
    but the role), and a relative difference ``|value - reference| > tolerance * |reference|``
    flags the promoted revision once.
    """

    rule: str
    reference: GenerationPin
    column: str
    tolerance: Decimal

    @property
    def rule_id(self) -> str:
        return self.rule.split("@", 1)[0]

    @property
    def version(self) -> str:
        return self.rule.split("@", 1)[1]


@dataclass(frozen=True, slots=True)
class PromotionSpec:
    raw: bytes
    sha256: str
    domain: str
    dataset_id: str
    parent: str | None
    sources: tuple[SourcePin, ...]
    mapper_name: str
    mapper: Mapper
    mapper_args: Mapping[str, object]
    partition: Partition | None
    time_rules: Mapping[str, TimeRule]
    decimal_rules: Mapping[str, decimal_rules.DecimalRule]
    quality_rules: tuple[QualityRule, ...]
    tombstone: TombstonePolicy
    identity_snapshot: IdentityPin | None

    def time_rule_identities(self) -> dict[str, object]:
        return {column: self.time_rules[column].identity() for column in TIME_COLUMNS}


def _object(value: object, keys: frozenset[str] | set[str], name: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError(f"promotion spec {name} needs exactly {sorted(keys)}")
    return value


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
        raise ValueError(f"promotion spec {name} must be exact nonempty text")
    return value


def _day(value: object, name: str) -> date:
    text = _text(value, name)
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        raise ValueError(f"promotion spec {name} must be an ISO date") from None
    if parsed.isoformat() != text:
        raise ValueError(f"promotion spec {name} must be an ISO date")
    return parsed


def _no_moving_references(value: object) -> None:
    if isinstance(value, str) and value == "latest":
        raise ValueError("promotion spec refuses moving references such as latest")
    if isinstance(value, dict):
        for item in value.values():
            _no_moving_references(item)
    if isinstance(value, list):
        for item in value:
            _no_moving_references(item)


def _decode(raw: bytes, sha256: str) -> dict[str, object]:
    if len(raw) > MAX_SPEC_BYTES:
        raise ValueError("promotion spec exceeds 64 MiB")
    if hashlib.sha256(raw).hexdigest() != sha256:
        raise ValueError("promotion spec bytes do not match the expected SHA-256")
    if raw.startswith(b"\xef\xbb\xbf") or b"\x00" in raw:
        raise ValueError("promotion spec must be UTF-8 JSON without BOM or NUL")
    try:
        raw.decode("utf-8", errors="strict")
        document = decode_json(raw)
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise ValueError(f"promotion spec is not strict JSON: {error}") from None
    _no_moving_references(document)
    body = _object(document, _ROOT, "document")
    if body["schema_version"] != SPEC_SCHEMA:
        raise ValueError("unsupported promotion spec schema")
    return body


def _sources(value: object) -> tuple[SourcePin, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError("promotion spec needs at least one source pin")
    pins = []
    for entry in value:
        item = _object(entry, {"source_id", "source_sha256", "table", "digest"}, "source")
        pins.append(
            SourcePin(
                _text(item["source_id"], "source_id"),
                _text(item["source_sha256"], "source_sha256"),
                _text(item["table"], "table"),
                _text(item["digest"], "digest"),
            )
        )
    if len({(pin.source_id, pin.table) for pin in pins}) != len(pins):
        raise ValueError("promotion spec pins one source table at most once")
    return tuple(pins)


def _generation_pin(value: object, name: str) -> GenerationPin:
    item = _object(value, set(_PIN_KEYS), name)
    return GenerationPin(**{key: _text(item[key], key) for key in _PIN_KEYS})


def _quality(value: object, domain: str, numeric: Mapping[str, str]) -> tuple[QualityRule, ...]:
    if not isinstance(value, list):
        raise TypeError("promotion spec quality_rules must be a list")
    rules = []
    for entry in value:
        item = _object(entry, {"rule", "args"}, "quality rule")
        if item["rule"] != CROSS_PROVIDER:
            raise ValueError(f"unknown quality rule {item['rule']}")
        if domain != "prices":
            raise ValueError(f"{CROSS_PROVIDER} compares prices only")
        args = _object(item["args"], {"reference", "column", "tolerance"}, "quality rule args")
        column = _text(args["column"], "quality rule column")
        if column not in numeric:
            raise ValueError("cross-provider comparison needs a numeric price column")
        try:
            tolerance = Decimal(_text(args["tolerance"], "tolerance"))
        except InvalidOperation:
            raise ValueError("tolerance must be a decimal string") from None
        if not tolerance.is_finite() or tolerance < 0:
            raise ValueError("tolerance must be a nonnegative decimal")
        rules.append(
            QualityRule(
                CROSS_PROVIDER, _generation_pin(args["reference"], "reference"), column, tolerance
            )
        )
    if len({rule.rule for rule in rules}) != len(rules):
        raise ValueError("each quality rule is declared at most once")
    return tuple(rules)


def _tombstone(
    value: object, sources: tuple[SourcePin, ...], partition: Partition | None
) -> TombstonePolicy:
    if not isinstance(value, dict) or value.get("mode") not in {"never", "absent_in_full_snapshot"}:
        raise ValueError("tombstone_policy mode is never or absent_in_full_snapshot")
    if value["mode"] == "never":
        _object(value, {"mode"}, "tombstone_policy")
        return TombstonePolicy("never")
    item = _object(value, {"mode", "source", "scope"}, "tombstone_policy")
    named = _object(item["source"], {"source_id", "table"}, "tombstone source")
    pin = next(
        (
            pin
            for pin in sources
            if (pin.source_id, pin.table) == (named["source_id"], named["table"])
        ),
        None,
    )
    if pin is None:
        raise ValueError("tombstone source must be one of the pinned source tables")
    scope = _object(item["scope"], {"instruments", "from", "to"}, "tombstone scope")
    instruments: tuple[str, ...] | None = None
    if scope["instruments"] is not None:
        listed = scope["instruments"]
        if not isinstance(listed, list) or not listed:
            raise ValueError("tombstone scope instruments must be a nonempty list or null")
        instruments = tuple(sorted({_text(entry, "instrument") for entry in listed}))
        if len(instruments) != len(listed):
            raise ValueError("tombstone scope repeats an instrument")
    start, end = _day(scope["from"], "scope from"), _day(scope["to"], "scope to")
    if start >= end:
        raise ValueError("tombstone scope dates must be increasing")
    if partition is not None and (start < partition.start or end > partition.end):
        raise ValueError("tombstone scope must lie inside the partition")
    return TombstonePolicy("absent_in_full_snapshot", pin, instruments, start, end)


def _mapper(value: object, domain: str) -> tuple[str, Mapper, dict[str, object]]:
    declared = _object(value, {"name", "args"}, "mapper")
    name = _text(declared["name"], "mapper name")
    found = mapper(name)
    if found.domain != domain:
        raise ValueError(f"mapper {name} writes {found.domain}, not {domain}")
    args = declared["args"]
    if not isinstance(args, dict):
        raise TypeError("mapper args must be an object")
    found.check_args(args)
    return name, found, dict(args)


def _identity(
    value: object, found: Mapper, args: Mapping[str, object], *, instruments: bool
) -> IdentityPin | None:
    if instruments != (value is not None) or instruments != (found.identity(args) is not None):
        raise ValueError("an instrument domain pins an identity snapshot; others pin none")
    if value is None:
        return None
    item = _object(value, {"snapshot_id", "content_hash"}, "identity_snapshot")
    return IdentityPin(
        _text(item["snapshot_id"], "snapshot_id"), _text(item["content_hash"], "content_hash")
    )


def parse_spec(raw: bytes, sha256: str) -> PromotionSpec:
    """Validate an exact ``aas-promotion-v1`` document; nothing is read from any store."""
    if not isinstance(sha256, str) or _SHA256.fullmatch(sha256) is None:
        raise ValueError("promotion spec hash must be lowercase SHA-256 hex")
    body = _decode(raw, sha256)
    target = _object(body["target"], {"domain", "dataset_id", "parent"}, "target")
    domain = _text(target["domain"], "domain")
    if domain not in DOMAINS:
        raise ValueError(f"unknown market domain {domain}")
    dataset_id = _text(target["dataset_id"], "dataset_id")
    if len(dataset_id) > _MAX_DATASET or _DATASET.fullmatch(dataset_id) is None:
        raise ValueError("dataset_id must be dotted lowercase words <domain>.<market>.<provider>")
    parent = None if target["parent"] is None else _text(target["parent"], "parent")
    sources = _sources(body["sources"])
    mapper_name, found, args = _mapper(body["mapper"], domain)
    prefixes = found.source_prefixes
    for source in sources:
        if prefixes and not source.source_id.startswith(prefixes):
            raise ValueError(
                f"mapper {mapper_name} reads only sources {list(prefixes)}, not {source.source_id}"
            )
    partition = None
    if body["partition"] is not None:
        bounds = _object(body["partition"], {"from", "to"}, "partition")
        partition = Partition(
            _day(bounds["from"], "partition from"), _day(bounds["to"], "partition to")
        )
        if partition.start >= partition.end:
            raise ValueError("partition dates must be increasing")
    rules = _object(body["time_rules"], set(TIME_COLUMNS), "time_rules")
    time_rules = {
        column: parse_rule(column, rules[column], found.time_inputs) for column in TIME_COLUMNS
    }
    numeric = found.numeric_columns(args)
    declared_rules = _object(body["decimal_rule"], set(numeric), "decimal_rule")
    kinds = dict(COMMON + DOMAINS[domain])
    conversions = {}
    for column, name in declared_rules.items():
        if kinds.get(column, "").rstrip("?") != "DECIMAL(38,12)":
            raise ValueError(f"{column} is not an exact decimal column")
        conversions[column] = decimal_rules.check_column(
            _text(name, "decimal rule"), column, numeric[column], domain=domain
        )
    pin = _identity(body["identity_snapshot"], found, args, instruments="instrument_id" in kinds)
    return PromotionSpec(
        raw=raw,
        sha256=sha256,
        domain=domain,
        dataset_id=dataset_id,
        parent=parent,
        sources=sources,
        mapper_name=mapper_name,
        mapper=found,
        mapper_args=dict(args),
        partition=partition,
        time_rules=time_rules,
        decimal_rules=conversions,
        quality_rules=_quality(body["quality_rules"], domain, numeric),
        tombstone=_tombstone(body["tombstone_policy"], sources, partition),
        identity_snapshot=pin,
    )
