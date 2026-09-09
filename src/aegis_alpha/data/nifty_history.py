"""Parse pinned Nifty historical price responses without admitting ETF prices.

The caller owns retrieval, index identity evidence, currency and timing. An index
close is not an adjusted ETF close or a total-return observation.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date

from aegis_alpha.data.canonical_json import as_float, as_mapping, as_sequence, as_str, field
from aegis_alpha.data.canonical_records import CanonicalBuildError
from aegis_alpha.data.serialization import canonical_json_bytes

MAX_HISTORY_BYTES = 64 * 1024 * 1024
_SHA = re.compile(r"[0-9a-f]{64}")
_DECIMAL = re.compile(r"[0-9]+(?:\.[0-9]+)?")
_MONTHS = {
    name: i
    for i, name in enumerate(
        ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), 1
    )
}
_DAY = re.compile(r"([0-9]{2}) ([A-Za-z]{3}) ([0-9]{4})")


@dataclass(frozen=True, slots=True)
class NiftyPricePoint:
    date: date
    close: float
    raw_row_json: str


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _constant(_value: str) -> None:
    raise ValueError("non-finite JSON value")


def _decode(text: str) -> object:
    return json.loads(text, object_pairs_hook=_pairs, parse_constant=_constant)


def _date(value: object) -> date:
    text = as_str("HistoricalDate", value)
    match = _DAY.fullmatch(text)
    if match is None or match[2] not in _MONTHS:
        raise ValueError("HistoricalDate must use DD Mon YYYY with English months")
    return date(int(match[3]), _MONTHS[match[2]], int(match[1]))


def _close(value: object) -> float:
    if isinstance(value, str):
        if _DECIMAL.fullmatch(value) is None:
            raise ValueError("CLOSE must be a plain unsigned decimal string")
        value = float(value)
    result = as_float("CLOSE", value)
    if result <= 0:
        raise ValueError("CLOSE must be positive")
    return result


def _points(
    document: object, expected_name: str, start: date, end: date
) -> tuple[NiftyPricePoint, ...]:
    if isinstance(document, dict):
        if set(document) != {"d"}:
            raise ValueError("historical response wrapper must contain only d")
        document = document["d"]
        if isinstance(document, str):
            document = _decode(document)
    result: list[NiftyPricePoint] = []
    seen: set[date] = set()
    for record in as_sequence("historical response", document):
        row = as_mapping("historical row", record)
        if as_str("INDEX_NAME", field(row, "INDEX_NAME")) != expected_name:
            raise ValueError("INDEX_NAME differs from expected index")
        day = _date(field(row, "HistoricalDate"))
        if not start <= day <= end:
            raise ValueError("HistoricalDate outside requested range")
        if day in seen:
            raise ValueError("duplicate observation date")
        seen.add(day)
        result.append(
            NiftyPricePoint(
                day, _close(field(row, "CLOSE")), canonical_json_bytes(row).decode("utf-8")
            )
        )
    return tuple(sorted(result, key=lambda point: point.date))


def parse_nifty_price_history(
    raw: bytes, *, expected_sha256: str, expected_index_name: str, start: date, end: date
) -> tuple[NiftyPricePoint, ...]:
    """Verify exact bytes, identity and request bounds; return immutable price rows.

    Empty responses return an empty tuple, not an assertion of source coverage.
    Malformed inputs raise ValueError; no network or filesystem access occurs.
    """
    if not isinstance(raw, bytes):
        raise ValueError("raw history must be bytes")  # noqa: TRY004 -- one ingress error type
    if len(raw) > MAX_HISTORY_BYTES:
        raise ValueError("history exceeds 64 MiB")
    if not isinstance(expected_sha256, str) or _SHA.fullmatch(expected_sha256) is None:
        raise ValueError("expected SHA256 must be lowercase hexadecimal")
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("history SHA256 mismatch")
    if (
        not isinstance(expected_index_name, str)
        or not expected_index_name
        or not expected_index_name.isprintable()
        or expected_index_name != expected_index_name.strip()
    ):
        raise ValueError("expected index name must be nonempty printable text")
    if type(start) is not date or type(end) is not date or start > end:
        raise ValueError("start and end must be ordered date values")
    try:
        return _points(_decode(raw.decode("utf-8-sig")), expected_index_name, start, end)
    except (CanonicalBuildError, UnicodeError, RecursionError, OverflowError) as exc:
        raise ValueError(f"invalid historical response: {exc}") from exc
