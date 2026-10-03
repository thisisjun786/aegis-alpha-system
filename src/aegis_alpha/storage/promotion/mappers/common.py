"""SQL pieces the text-valued macro and FX mappers share.

- A value is ``present`` only when its source text is a plain decimal that ``decimal_text@1``
  converts; empty text, no text, and FRED's ``.`` are ``missing``; any other text is
  ``invalid`` and keeps no value. Nothing is trimmed or repaired.
- A time input never precedes the Unix epoch. Market times are nonnegative, so a source day
  before 1970-01-01 gives that day instead: a later day is still an upper bound on when the
  row was public, which is all a time rule claims.
- A currency is three uppercase letters (ISO 4217 form); an FX mapper's base and quote must
  differ.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aegis_alpha.storage.promotion.formats import sql_literal

DECIMAL_TEXT_SQL: Final = r"[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?"
_ZONE: Final = re.compile(r"[A-Za-z][A-Za-z0-9_+-]*(?:/[A-Za-z0-9_+-]+)*")
_CURRENCY: Final = re.compile(r"[A-Z]{3}")
_SERIES: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._$^-]{0,63}")
EPOCH_DAY: Final = "DATE '1970-01-01'"


def decimal_state(text: str) -> str:
    """``present``, ``missing`` or ``invalid`` for a source text column."""
    return (
        f"CASE WHEN {text} IS NULL OR {text} IN ('', '.') THEN 'missing' "
        f"WHEN regexp_full_match({text}, {sql_literal(DECIMAL_TEXT_SQL)}) THEN 'present' "
        "ELSE 'invalid' END"
    )


def iso_day(text: str) -> str:
    """The DATE of a ``YYYY-MM-DD`` text, NULL for any other spelling or an impossible day."""
    return (
        f"CASE WHEN regexp_full_match({text}, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}') "
        f"THEN CAST(try_strptime({text}, '%Y-%m-%d') AS DATE) END"
    )


def epoch_floor(day: str) -> str:
    """A time-input day no earlier than 1970-01-01."""
    return f"CASE WHEN {day} < {EPOCH_DAY} THEN {EPOCH_DAY} ELSE {day} END"


def day_end_us(zone: str, day: str) -> str:
    """The last microsecond of ``day`` in the IANA ``zone``, as UTC microseconds."""
    literal = sql_literal(zone)
    return f"(epoch_us(timezone({literal}, CAST({day} AS TIMESTAMP) + INTERVAL 1 DAY)) - 1)"


def zone_arg(name: str, args: Mapping[str, object]) -> str:
    zone = args.get("timezone")
    if not isinstance(zone, str) or _ZONE.fullmatch(zone) is None:
        raise ValueError(f"{name} timezone must be an IANA zone name")
    try:
        ZoneInfo(zone)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError(f"{name} timezone must be an IANA zone name") from None
    return zone


def pair_args(name: str, args: Mapping[str, object]) -> None:
    """Check an FX mapper's ``series``, ``base``, ``quote`` and ``timezone`` arguments."""
    if set(args) != {"series", "base", "quote", "timezone"}:
        raise ValueError(f"{name} takes exactly series, base, quote and timezone arguments")
    series = args["series"]
    if not isinstance(series, str) or _SERIES.fullmatch(series) is None:
        raise ValueError(f"{name} series must be a source series name")
    base, quote = args["base"], args["quote"]
    for value in (base, quote):
        if not isinstance(value, str) or _CURRENCY.fullmatch(value) is None:
            raise ValueError(f"{name} base and quote must be three-letter currency codes")
    if base == quote:
        raise ValueError(f"{name} base and quote must differ")
    zone_arg(name, args)
