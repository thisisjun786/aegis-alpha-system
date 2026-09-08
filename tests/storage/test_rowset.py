# ruff: noqa: PLR2004
"""Frozen aas-rowset-v1 hashes and typed encoding refusals."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from math import inf, nan
from typing import Final

import pytest

from aegis_alpha.storage.rowset import HASH_ALGORITHM, ROWSET_FORMAT, rowset_hash

_SCHEMA: Final = (
    ("record_id", "text"),
    ("revision_id", "int"),
    ("available_at_us", "utc_us"),
    ("session_date", "date"),
    ("close", "decimal"),
    ("volume", "float"),
    ("listed", "bool"),
    ("note", "text"),
)
_MIXED_HASH: Final = "329b125f89b03914717bb1f6b69e4c1d2401ccd92502ab21e70a40d76df0dc1f"
_EMPTY_HASH: Final = "712ab936d0ab08a3c1d672bb7c114050bfb7ef0df59a004b2431402d595cd31d"
_ZERO_VOLUME_HASH: Final = "4a7a8cb4e821b81ff354115d9d30e27ea0d7f6c5eb19fd4e47c16a421a098016"


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "record_id": "instr-1",
        "revision_id": 1,
        "available_at_us": 1_577_836_800_000_000,
        "session_date": date(2020, 1, 2),
        "close": Decimal("11.50"),
        "volume": 100.0,
        "listed": True,
        "note": None,
    }
    row.update(overrides)
    return row


def _mixed_rows() -> list[dict[str, object]]:
    return [
        _row(),
        _row(
            record_id="instr-2",
            revision_id=2,
            close=Decimal("11.500"),
            listed=False,
            volume=-0.0,
            note="",
        ),
    ]


def test_format_identity_and_empty_rowset_are_frozen() -> None:
    assert (ROWSET_FORMAT, HASH_ALGORITHM) == ("aas-rowset-v1", "sha256")
    assert rowset_hash(_SCHEMA, []) == _EMPTY_HASH


def test_mixed_typed_rows_hash_is_order_independent() -> None:
    rows = _mixed_rows()
    assert rowset_hash(_SCHEMA, rows) == _MIXED_HASH
    assert rowset_hash(_SCHEMA, list(reversed(rows))) == _MIXED_HASH


def test_duplicate_rows_remain_in_the_digest() -> None:
    one = rowset_hash((("v", "int"),), [{"v": 1}])
    two = rowset_hash((("v", "int"),), [{"v": 1}, {"v": 1}])
    assert one == "ec476164a7f8a678c058a136964fc33c3a87238f66c1f62fba2525527282b7a0"
    assert two == "ec7d705e4ca7a13c5b55cc2c27fc22bcc73d91842d8c87eb38834511318d2d6b"
    assert one != two


def test_decimal_scale_and_signed_zero_are_canonical() -> None:
    assert rowset_hash(_SCHEMA, [_row(volume=0.0)]) == _ZERO_VOLUME_HASH
    assert rowset_hash(_SCHEMA, [_row(volume=-0.0)]) == _ZERO_VOLUME_HASH
    zero = rowset_hash((("v", "decimal"),), [{"v": Decimal(0)}])
    assert zero == rowset_hash((("v", "decimal"),), [{"v": Decimal("-0")}])
    assert zero == "d199305784d2065ec546e19b28c9307ad6fe3f204db203b0305d2be73fe45af7"
    scaled = rowset_hash((("v", "decimal"),), [{"v": Decimal("0.0")}])
    assert scaled == "72818bd0e0065288063f0ad695a075f0f3546ac1f8e2bb88bac2f42c6203cfa9"
    assert scaled != zero
    assert rowset_hash(_SCHEMA, [_row()]) != rowset_hash(_SCHEMA, [_row(close=Decimal("11.500"))])


def test_tagged_values_and_concatenations_do_not_collide() -> None:
    hashes = {
        rowset_hash((("v", "int"),), [{"v": 1}]),
        rowset_hash((("v", "utc_us"),), [{"v": 1}]),
        rowset_hash((("v", "text"),), [{"v": "1"}]),
        rowset_hash((("v", "decimal"),), [{"v": Decimal(1)}]),
        rowset_hash((("v", "float"),), [{"v": 1.0}]),
        rowset_hash((("v", "text"),), [{"v": None}]),
        rowset_hash((("v", "int"),), [{"v": None}]),
        rowset_hash((("a", "text"), ("b", "text")), [{"a": "ab", "b": "c"}]),
        rowset_hash((("a", "text"), ("b", "text")), [{"a": "a", "b": "bc"}]),
        rowset_hash((("a", "text"), ("b", "text")), [{"a": "x", "b": "y"}]),
        rowset_hash((("b", "text"), ("a", "text")), [{"a": "x", "b": "y"}]),
    }
    assert len(hashes) == 11
    assert "ec476164a7f8a678c058a136964fc33c3a87238f66c1f62fba2525527282b7a0" in hashes
    assert "cd598de0960f843c5d1fb6f9a690b57be7053793d2ee591f9ab823aaec6c10a0" in hashes
    assert "32b36ff95705c3a6cf46765edad54096b7d8d83f02acdea8681f58ac2765bbdc" in hashes
    assert "8c550c0c445848426e2dfe359af03dabeb2bb24619cd9c96b8b8a2db6fc91d23" in hashes


@pytest.mark.parametrize(
    ("schema", "rows", "match"),
    [
        ((), [], "nonempty"),
        ((("v", "int"), ("v", "text")), [{"v": 1}], "unique"),
        ((("v", "json"),), [{"v": "{}"}], "unsupported"),
        ((("v", "int"),), [{"v": 1, "extra": 2}], "exactly"),
        ((("v", "int"),), [{}], "exactly"),
        ((("v", "int"),), [{"v": True}], "integer"),
        ((("v", "text"),), [{"v": b"raw"}], "str or null"),
        ((("v", "decimal"),), [{"v": 1.0}], "Decimal"),
        ((("v", "date"),), [{"v": "2020-01-02"}], "date"),
        ((("v", "float"),), [{"v": nan}], "finite"),
        ((("v", "float"),), [{"v": inf}], "finite"),
        ((("v", "decimal"),), [{"v": Decimal("NaN")}], "finite"),
        ((("v", "int"),), [{"v": 1 << 63}], "64-bit"),
        ((("v", "bool"),), [{"v": 1}], "bool"),
        (("v", "int"), [{"v": 1}], "pair"),
    ],
)
def test_invalid_schema_and_values_are_rejected(
    schema: object, rows: list[dict[str, object]], match: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=match):
        rowset_hash(schema, rows)  # ty: ignore[invalid-argument-type]
