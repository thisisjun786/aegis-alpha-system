from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256


def test_canonical_serialization_is_order_independent() -> None:
    first = {
        "value": Decimal("1.2300"),
        "retrieved_at": datetime(2026, 7, 29, 1, 2, 3, tzinfo=UTC),
        "labels": {"b": 2, "a": 1},
    }
    reordered = {
        "labels": {"a": 1, "b": 2},
        "retrieved_at": datetime(2026, 7, 29, 1, 2, 3, tzinfo=UTC),
        "value": Decimal("1.2300"),
    }

    expected = b'{"labels":{"a":1,"b":2},"retrieved_at":"2026-07-29T01:02:03Z","value":"1.2300"}'
    assert canonical_json_bytes(first) == expected
    assert canonical_json_bytes(reordered) == expected
    assert content_sha256(first) == content_sha256(reordered)
