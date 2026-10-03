# ruff: noqa: PLR2004, S311, S608 -- seeded synthetic values and test-owned SQL
"""Every hash format a promotion writes is frozen, and its SQL form equals its Python form."""

from __future__ import annotations

import hashlib
import math
import random
import struct
from datetime import UTC, date, datetime, timedelta

import duckdb
import pytest

from aegis_alpha.storage.promotion import formats

_DATASET = "prices.kr.eodhd"
_RECORD = "a" * 64
_PRIOR = "b" * 64
_ROW = "c" * 64
_ROW_COLUMNS = (
    ("symbol", "AAA.KO"),
    ("close", 1.5),
    ("raw", b"\x00\xff"),
    ("missing", None),
    ("day", date(2025, 1, 2)),
    ("at", datetime(2025, 1, 2, 6, 30, tzinfo=UTC)),
    ("local", datetime(2025, 1, 2, 15, 30)),  # noqa: DTZ001 -- a naive source timestamp
    ("flag", True),
    ("count", 7),
    ("note", 'é"\\'),
)
FROZEN_REVISION = "a1f18f590bb2a6807c9f8b06df83cc13cfbf5604787008ad6cd24aaaf05be8f5"
FROZEN_ASSERT = "43520c90c3ca8c5e46aaed91572c401ca0c9987dc3d83e3b8bcef599d2c05d4b"
FROZEN_SOURCE_ROW = "f3d981a091f266ef8040904ef2b5c15baf474ba0ac5a877dfda411fa74d90386"
FROZEN_TOMBSTONE = "7b82da7638815760052fadcf3212d26b25d7afa43c552aaff9c6a1fc15bb0a81"
FROZEN_REQUEST = "ca0795271fbc4c43145f6269494d916de66e19002c2f9e373db4cc82300d45fd"
_FROZEN_ROW_DOCUMENT = (
    b'["aas-source-row-v1",[["symbol","AAA.KO"],["close",{"float_hex":"0x1.8000000000000p+0"}],'
    b'["raw",{"base64":"AP8="}],["missing",null],["day",{"date":"2025-01-02"}],'
    b'["at",{"utc_us":1735799400000000}],["local",{"local_us":1735831800000000}],'
    b'["flag",true],["count",7],["note","\\u00e9\\"\\\\"]]]'
)


def test_revision_id_format_is_frozen() -> None:
    document = formats.canonical(
        [formats.REVISION_FORMAT, _DATASET, _RECORD, "SUPERSEDE", _PRIOR, _ROW]
    )
    assert document == (
        b'["aas-revision-v1","prices.kr.eodhd","'
        + _RECORD.encode()
        + b'","SUPERSEDE","'
        + _PRIOR.encode()
        + b'","'
        + _ROW.encode()
        + b'"]'
    )
    assert hashlib.sha256(document).hexdigest() == formats.revision_id(
        _DATASET, _RECORD, "SUPERSEDE", _PRIOR, _ROW
    )
    assert formats.revision_id(_DATASET, _RECORD, "SUPERSEDE", _PRIOR, _ROW) == FROZEN_REVISION
    assert formats.revision_id(_DATASET, _RECORD, "ASSERT", None, _ROW) == FROZEN_ASSERT
    connection = duckdb.connect()
    for op, prior in (("SUPERSEDE", _PRIOR), ("ASSERT", None)):
        sql = formats.revision_id_sql(_DATASET, "r", "o", "s", "h")
        row = connection.execute(
            f"SELECT {sql} FROM (SELECT ? AS r, ? AS o, ?::VARCHAR AS s, ? AS h)",
            [_RECORD, op, prior, _ROW],
        ).fetchone()
        assert row == (formats.revision_id(_DATASET, _RECORD, op, prior, _ROW),)


def test_source_row_hash_format_is_frozen() -> None:
    document = formats.canonical(
        [
            formats.SOURCE_ROW_FORMAT,
            [[name, formats.source_value(value)] for name, value in _ROW_COLUMNS],
        ]
    )
    assert document == _FROZEN_ROW_DOCUMENT
    assert formats.source_row_hash(_ROW_COLUMNS) == hashlib.sha256(_FROZEN_ROW_DOCUMENT).hexdigest()
    assert formats.source_row_hash(_ROW_COLUMNS) == FROZEN_SOURCE_ROW
    with pytest.raises(ValueError, match="ordinal"):
        formats.source_row_hash((("_aas_ordinal", 1), ("symbol", "AAA.KO")))


def test_tombstone_hash_format_is_frozen() -> None:
    document = formats.canonical(
        [formats.TOMBSTONE_FORMAT, "synthetic-kr-bars-" + _RECORD, "bars", _ROW]
    )
    assert hashlib.sha256(document).hexdigest() == formats.tombstone_hash(
        "synthetic-kr-bars-" + _RECORD, "bars", _ROW
    )
    assert formats.tombstone_hash("synthetic-kr-bars-" + _RECORD, "bars", _ROW) == FROZEN_TOMBSTONE


def test_request_hash_format_is_frozen() -> None:
    document = formats.request_document(_RECORD, [_PRIOR, _ROW], None)
    assert document == (
        b'["aas-promotion-request-v1","'
        + _RECORD.encode()
        + b'",["'
        + _PRIOR.encode()
        + b'","'
        + _ROW.encode()
        + b'"],null]'
    )
    assert (
        formats.request_hash(_RECORD, [_PRIOR, _ROW], None) == hashlib.sha256(document).hexdigest()
    )
    assert formats.request_hash(_RECORD, [_PRIOR, _ROW], None) == FROZEN_REQUEST
    assert formats.request_hash(_RECORD, [_PRIOR, _ROW], "prm-parent") != FROZEN_REQUEST


_TYPES = (
    ("b", "BOOLEAN"),
    ("i", "BIGINT"),
    ("h", "HUGEINT"),
    ("d", "DOUBLE"),
    ("f", "FLOAT"),
    ("s", "VARCHAR"),
    ("x", "BLOB"),
    ("day", "DATE"),
    ("tz", "TIMESTAMP WITH TIME ZONE"),
    ("ts", "TIMESTAMP"),
)
_TEXT = "aZ09-_.:/é한字😀\t \x7f\u0085\"\\'"


def _double(rng: random.Random) -> float:
    shape = rng.randrange(4)
    if shape == 0:
        return rng.choice(
            [0.0, -0.0, math.inf, -math.inf, math.nan, 5e-324, 1.7976931348623157e308]
        )
    while True:
        value = struct.unpack(">d", rng.getrandbits(64).to_bytes(8, "big"))[0]
        if not math.isnan(value):
            return value


def _row(rng: random.Random) -> dict[str, object]:
    def maybe(value: object) -> object:
        return None if rng.random() < 0.15 else value

    single = struct.unpack(">f", struct.pack(">f", rng.uniform(-1e6, 1e6)))[0]
    text = "".join(rng.choice(_TEXT) for _ in range(rng.randrange(8)))
    if rng.random() < 0.5:
        text = "".join(rng.choice("abcXYZ019 .-") for _ in range(rng.randrange(8)))
    return {
        "b": maybe(rng.random() < 0.5),
        "i": maybe(rng.randrange(-(2**63), 2**63)),
        "h": maybe(rng.randrange(-(2**100), 2**100)),
        "d": maybe(_double(rng)),
        "f": maybe(single),
        "s": maybe(text),
        "x": maybe(bytes(rng.randrange(256) for _ in range(rng.randrange(6)))),
        "day": maybe(date(1, 1, 1) + timedelta(days=rng.randrange(3_652_058))),
        "tz": maybe(rng.randrange(-(6 * 10**16), 25 * 10**16)),
        "ts": maybe(rng.randrange(-(6 * 10**16), 25 * 10**16)),
    }


def test_source_row_hash_sql_matches_python() -> None:
    rng = random.Random(20261003)
    rows = [_row(rng) for _ in range(1500)]
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    columns = ", ".join(f'"{name}" {kind}' for name, kind in _TYPES)
    connection.execute(f"CREATE TABLE t (n BIGINT, {columns})")
    connection.executemany(
        "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "
        "make_timestamp(?)::TIMESTAMPTZ, make_timestamp(?))",
        [[index, *(row[name] for name, _ in _TYPES)] for index, row in enumerate(rows)],
    )
    expression, plain, invalid = formats.source_row_hash_sql(_TYPES)
    fragments = ", ".join(formats.source_row_fragments_sql(_TYPES))
    checked = {"sql": 0, "python": 0}
    for stored in connection.execute(
        f"SELECT n, ({plain}) AND NOT ({invalid}), CASE WHEN ({plain}) THEN {expression} END, "
        f"{fragments} FROM t ORDER BY n"
    ).fetchall():
        row = rows[stored[0]]
        values = []
        for name, kind in _TYPES:
            value = row[name]
            if value is not None and kind == "TIMESTAMP WITH TIME ZONE":
                value = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=value)
            elif value is not None and kind == "TIMESTAMP":
                value = datetime(1970, 1, 1) + timedelta(microseconds=value)  # noqa: DTZ001
            elif value is not None and kind == "FLOAT":
                value = float(value)
            values.append((name, value))
        expected = formats.source_row_hash(values)
        assert formats.source_row_hash_from_fragments(_TYPES, stored[3:]) == expected
        if stored[1]:
            assert stored[2] == expected
            checked["sql"] += 1
        else:
            checked["python"] += 1
    # Both routes are exercised: plain rows in SQL and escaped text through the fallback.
    assert checked["sql"] > 300
    assert checked["python"] > 300
