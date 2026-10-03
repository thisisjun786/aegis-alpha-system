"""The ``aas-promotion-v1`` document admits exactly one shape and no moving reference."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any

import pytest

from aegis_alpha.storage.promotion.spec import MAX_SPEC_BYTES, parse_spec
from tests.storage.dart_receipt_support import spec as dart_spec
from tests.storage.promotion_support import DAY_RULE, DECIMALS, ZONE

_PIN = {
    "source_id": "synthetic-kr-bars-" + "a" * 64,
    "source_sha256": "a" * 64,
    "table": "bars",
    "digest": "b" * 64,
}
_IDENTITY = {"snapshot_id": "kr", "content_hash": "c" * 64}


def _document() -> dict[str, Any]:
    return {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": "prices", "dataset_id": "prices.kr.eodhd", "parent": None},
        "sources": [dict(_PIN)],
        "mapper": {"name": "eodhd.bars@1", "args": {"timezone": ZONE}},
        "partition": {"from": "2025-01-01", "to": "2026-01-01"},
        "time_rules": {"available_at_us": DAY_RULE, "revision_known_at_us": DAY_RULE},
        "decimal_rule": dict(DECIMALS),
        "quality_rules": [],
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": dict(_IDENTITY),
    }


def _parse(document: dict[str, Any]) -> None:
    raw = json.dumps(document).encode()
    parse_spec(raw, hashlib.sha256(raw).hexdigest())


def _change(edit: Callable[[dict[str, Any]], object]) -> dict[str, Any]:
    document = _document()
    edit(document)
    return document


def test_spec_rejects_unknown_fields_and_moving_refs() -> None:
    raw = json.dumps(_document()).encode()
    spec = parse_spec(raw, hashlib.sha256(raw).hexdigest())
    assert (spec.dataset_id, spec.mapper_name, spec.partition is not None) == (
        "prices.kr.eodhd",
        "eodhd.bars@1",
        True,
    )
    with pytest.raises(ValueError, match="SHA-256"):
        parse_spec(raw, "0" * 64)
    refused: list[tuple[dict[str, Any], str]] = [
        (_change(lambda d: d.update(extra=1)), "exactly"),
        (_change(lambda d: d.pop("partition")), "exactly"),
        (_change(lambda d: d["target"].update(parent="latest")), "latest"),
        (_change(lambda d: d["sources"][0].update(table="latest")), "latest"),
        (_change(lambda d: d.update(schema_version="aas-promotion-v0")), "schema"),
        (_change(lambda d: d["mapper"].update(name="eodhd.bars@9")), "unknown mapper"),
        (_change(lambda d: d["decimal_rule"].update(volume="krw_tick@1")), "KRW|convert"),
        (_change(lambda d: d["decimal_rule"].pop("volume")), "exactly"),
        (_change(lambda d: d.update(identity_snapshot=None)), "identity"),
        (_change(lambda d: d["target"].update(dataset_id="Prices KR")), "dataset_id"),
        (
            _change(lambda d: d.update(partition={"from": "2025-01-01", "to": "2025-01-01"})),
            "increasing",
        ),
        (_change(lambda d: d["sources"].append(dict(_PIN))), "once"),
        (
            _change(
                lambda d: d["time_rules"].update(
                    available_at_us={**DAY_RULE, "rule": "session_close_plus_lag@1"}
                )
            ),
            "args",
        ),
        (
            _change(
                lambda d: d.update(
                    tombstone_policy={
                        "mode": "absent_in_full_snapshot",
                        "source": {"source_id": "other", "table": "bars"},
                        "scope": {"instruments": None, "from": "2025-01-01", "to": "2025-02-01"},
                    }
                )
            ),
            "pinned source",
        ),
    ]
    for document, message in refused:
        with pytest.raises(ValueError, match=message):
            _parse(document)
    duplicate = b'{"schema_version":"aas-promotion-v1","schema_version":"aas-promotion-v1"}'
    for payload in (duplicate, b"\xef\xbb\xbf" + raw, raw.replace(b"2025", b"NaN", 1)):
        with pytest.raises(ValueError, match=r"JSON|BOM|exactly|ISO"):
            parse_spec(payload, hashlib.sha256(payload).hexdigest())
    assert b'"quality_rules": []' in raw
    for literal in (b"NaN", b"Infinity", b"-Infinity"):
        payload = raw.replace(b'"quality_rules": []', b'"quality_rules": [' + literal + b"]")
        with pytest.raises(ValueError, match="non-finite"):
            parse_spec(payload, hashlib.sha256(payload).hexdigest())
    oversized = raw + b" " * (MAX_SPEC_BYTES + 1 - len(raw))
    with pytest.raises(ValueError, match="64 MiB"):
        parse_spec(oversized, hashlib.sha256(oversized).hexdigest())


def test_an_optional_instrument_needs_no_identity_snapshot() -> None:
    raw, sha = dart_spec([dict(_PIN, table="receipts")])
    spec = parse_spec(raw, sha)
    assert (spec.domain, spec.identity_snapshot, spec.mapper.identity({})) == (
        "fundamentals",
        None,
        None,
    )
    pinned = json.loads(raw)
    pinned["identity_snapshot"] = dict(_IDENTITY)
    with pytest.raises(ValueError, match="resolves no subject pins none"):
        _parse(pinned)
    # A domain whose instrument is required keeps needing a mapper that resolves it.
    unresolved = _change(lambda d: d.update(identity_snapshot=None))
    with pytest.raises(ValueError, match="an instrument domain pins"):
        _parse(unresolved)
