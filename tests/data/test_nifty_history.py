from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime

import pytest

from aegis_alpha.data.nifty_history import (
    MAX_HISTORY_BYTES,
    NiftyPricePoint,
    parse_nifty_price_history,
)

START = date(2000, 1, 3)
END = date(2000, 1, 4)
NAME = "Synthetic Index"


def row(day: str = "03 Jan 2000", close: object = "10.5") -> dict[str, object]:
    return {"INDEX_NAME": NAME, "HistoricalDate": day, "CLOSE": close, "extra": [1, "x"]}


def encoded(value: object) -> bytes:
    return json.dumps(value).encode()


def parse(raw: bytes) -> tuple[NiftyPricePoint, ...]:
    return parse_nifty_price_history(
        raw,
        expected_sha256=hashlib.sha256(raw).hexdigest(),
        expected_index_name=NAME,
        start=START,
        end=END,
    )


@pytest.mark.parametrize("wrapper", ["direct", "array", "string"])
def test_bom_and_wrappers_preserve_bounds_order_and_raw(wrapper: str) -> None:
    rows = [row("04 Jan 2000", 12), row()]
    value: object = rows
    if wrapper == "array":
        value = {"d": rows}
    elif wrapper == "string":
        value = {"d": json.dumps(rows)}
    result = parse(b"\xef\xbb\xbf" + encoded(value))
    assert [(p.date, p.close) for p in result] == [(START, 10.5), (END, 12.0)]
    assert json.loads(result[0].raw_row_json) == row()
    with pytest.raises(FrozenInstanceError):
        setattr(result[0], "close", 99)  # noqa: B010 -- exercise frozen output


@pytest.mark.parametrize("value", [[], {"d": []}, {"d": "[]"}])
def test_empty_is_no_data(value: object) -> None:
    assert parse(encoded(value)) == ()


@pytest.mark.parametrize(
    "close",
    [
        True,
        None,
        0,
        -1,
        float("nan"),
        float("inf"),
        "0",
        "-1",
        "+1",
        "1e3",
        "1_0",
        " 1",
        "1\n",
        "NaN",
        "Infinity",
        "9" * 400,
        10**400,
    ],
)
def test_invalid_close_rejected(close: object) -> None:
    with pytest.raises(ValueError, match=r"CLOSE|non-finite|too large"):
        parse(encoded([row(close=close)]))


@pytest.mark.parametrize(
    "day",
    [
        "02 Jan 2000",
        "05 Jan 2000",
        "31 Feb 2000",
        "03 jan 2000",
        "03 Janv 2000",
        "3 Jan 2000",
        "2000-01-03",
        "03 Jan 2000 00:00:00",
    ],
)
def test_invalid_or_out_of_range_day_rejected(day: str) -> None:
    with pytest.raises(ValueError, match=r"HistoricalDate|day"):
        parse(encoded([row(day)]))


@pytest.mark.parametrize("missing", ["INDEX_NAME", "HistoricalDate", "CLOSE"])
def test_missing_required_field_rejected(missing: str) -> None:
    value = row()
    del value[missing]
    with pytest.raises(ValueError, match=r"required field is missing"):
        parse(encoded([value]))


def test_wrong_identity_and_duplicate_dates_rejected() -> None:
    with pytest.raises(ValueError, match=r"INDEX_NAME"):
        parse(encoded([{**row(), "INDEX_NAME": "Other Index"}]))
    with pytest.raises(ValueError, match=r"duplicate observation"):
        parse(encoded([row(), row()]))


@pytest.mark.parametrize(
    "raw", [b'{"d":[],"d":[]}', b'[{"x":1,"x":2}]', b'{"d":"[{\\"x\\":1,\\"x\\":2}]"}']
)
def test_duplicate_json_keys_rejected(raw: bytes) -> None:
    with pytest.raises(ValueError, match=r"duplicate JSON"):
        parse(raw)


@pytest.mark.parametrize(
    "value", [None, 12, "[]", [None], [1], ["row"], {"d": [], "other": 1}, {"d": {}}, {"d": "{}"}]
)
def test_invalid_response_shape(value: object) -> None:
    with pytest.raises(ValueError, match=r"historical|wrapper"):
        parse(encoded(value))


@pytest.mark.parametrize("raw", [b"not-json", b"\xff", b"[", b'{"d":"not-json"}'])
def test_invalid_encoding_or_json(raw: bytes) -> None:
    with pytest.raises(ValueError, match=r"Expecting|invalid historical response"):
        parse(raw)


def test_hash_precedes_decode() -> None:
    with pytest.raises(ValueError, match=r"SHA256 mismatch"):
        parse_nifty_price_history(
            b"not-json", expected_sha256="0" * 64, expected_index_name=NAME, start=START, end=END
        )


@pytest.mark.parametrize("sha", ["A" * 64, "a" * 63, "z" * 64, "", None])
def test_invalid_digest(sha: str | None) -> None:
    with pytest.raises(ValueError, match=r"lowercase"):
        parse_nifty_price_history(
            b"[]",
            expected_sha256=sha,  # ty: ignore[invalid-argument-type]
            expected_index_name=NAME,
            start=START,
            end=END,
        )


@pytest.mark.parametrize("name", ["", " ", " bad", "bad\n", None, 1])
def test_invalid_expected_name(name: object) -> None:
    with pytest.raises(ValueError, match=r"index name"):
        parse_nifty_price_history(
            b"[]",
            expected_sha256=hashlib.sha256(b"[]").hexdigest(),
            expected_index_name=name,  # ty: ignore[invalid-argument-type]
            start=START,
            end=END,
        )


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (END, START),
        (datetime(2000, 1, 3, tzinfo=UTC), END),
        (START, datetime(2000, 1, 4, tzinfo=UTC)),
        ("2000-01-03", END),
        (START, None),
    ],
)
def test_invalid_request_bounds(start: object, end: object) -> None:
    with pytest.raises(ValueError, match=r"ordered date"):
        parse_nifty_price_history(
            b"[]",
            expected_sha256=hashlib.sha256(b"[]").hexdigest(),
            expected_index_name=NAME,
            start=start,  # ty: ignore[invalid-argument-type] -- invalid boundary input
            end=end,  # ty: ignore[invalid-argument-type] -- invalid boundary input
        )


@pytest.mark.parametrize("raw", ["[]", bytearray(b"[]"), None])
def test_raw_must_be_bytes(raw: object) -> None:
    with pytest.raises(ValueError, match=r"must be bytes"):
        parse_nifty_price_history(
            raw,  # ty: ignore[invalid-argument-type] -- invalid boundary input
            expected_sha256="0" * 64,
            expected_index_name=NAME,
            start=START,
            end=END,
        )


def test_actual_size_bound_precedes_hash() -> None:
    with pytest.raises(ValueError, match=r"64 MiB"):
        parse_nifty_price_history(
            b" " * (MAX_HISTORY_BYTES + 1),
            expected_sha256="0" * 64,
            expected_index_name=NAME,
            start=START,
            end=END,
        )
