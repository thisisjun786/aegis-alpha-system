"""SQL pieces the daily price mappers share.

- A session date spelled as text is a DATE only when it is exactly ``YYYY-MM-DD`` and a real
  day; any other text is NULL, which a partition refuses and a required column refuses.
- A text value is ``present`` when it is an unsigned decimal (``decimal_text@1`` converts
  it; no price or volume is negative), ``missing`` when it is empty or absent, and
  ``invalid`` otherwise. Nothing is trimmed or repaired.
- A bar ends at the last microsecond of its session date in an IANA zone: an upper bound on
  when a daily bar can end that needs no calendar.
- A time input never precedes the Unix epoch. Market times are nonnegative, so a session
  before 1970-01-01 gives that day instead: a later day is still an upper bound on when the
  row was public, which is all a time rule claims.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aegis_alpha.storage.promotion.formats import sql_literal

UNSIGNED_TEXT: Final = r"\+?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?"
_ZONE: Final = re.compile(r"[A-Za-z][A-Za-z0-9_+-]*(?:/[A-Za-z0-9_+-]+)*")
EPOCH_DAY: Final = "DATE '1970-01-01'"


def iso_day(text: str) -> str:
    """The DATE of a ``YYYY-MM-DD`` text, NULL for any other spelling or an impossible day."""
    return (
        f"CASE WHEN regexp_full_match({text}, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}') "
        f"THEN CAST(try_strptime({text}, '%Y-%m-%d') AS DATE) END"
    )


def text_state(text: str) -> str:
    """``present``, ``missing`` or ``invalid`` for one source text value."""
    return (
        f"CASE WHEN {text} IS NULL OR {text} = '' THEN 'missing' "
        f"WHEN regexp_full_match({text}, {sql_literal(UNSIGNED_TEXT)}) THEN 'present' "
        "ELSE 'invalid' END"
    )


def bar_state(states: list[str]) -> str:
    """A bar is ``present`` when every value is, ``missing`` when none is, else ``invalid``."""
    present = " AND ".join(f"({state}) = 'present'" for state in states)
    missing = " AND ".join(f"({state}) = 'missing'" for state in states)
    return f"CASE WHEN {present} THEN 'present' WHEN {missing} THEN 'missing' ELSE 'invalid' END"


def zone_start_us(zone: str, day: str) -> str:
    """UTC microseconds of the start of ``day`` in the IANA ``zone``."""
    return f"epoch_us(timezone({sql_literal(zone)}, CAST({day} AS TIMESTAMP)))"


def day_end_us(zone: str, day: str) -> str:
    """The last microsecond of ``day`` in the IANA ``zone``, as UTC microseconds."""
    literal = sql_literal(zone)
    return f"(epoch_us(timezone({literal}, CAST({day} AS TIMESTAMP) + INTERVAL 1 DAY)) - 1)"


def epoch_floor(day: str) -> str:
    """A time-input day no earlier than 1970-01-01."""
    return f"CASE WHEN {day} < {EPOCH_DAY} THEN {EPOCH_DAY} ELSE {day} END"


def zone_arg(name: str, args: Mapping[str, object]) -> str:
    """The checked IANA ``timezone`` argument of mapper ``name``."""
    zone = args.get("timezone")
    if not isinstance(zone, str) or _ZONE.fullmatch(zone) is None:
        raise ValueError(f"{name} timezone must be an IANA zone name")
    try:
        ZoneInfo(zone)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError(f"{name} timezone must be an IANA zone name") from None
    return zone


def exact_args(name: str, args: Mapping[str, object], keys: set[str]) -> None:
    if set(args) != keys:
        raise ValueError(f"{name} takes exactly the arguments {sorted(keys)}")
