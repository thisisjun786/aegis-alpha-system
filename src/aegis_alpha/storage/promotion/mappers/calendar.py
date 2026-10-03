"""``calendar.declared@1``: a declared venue calendar as ``calendar_sessions``.

Input is the source table ``aas calendar refresh`` commits from one
``aas-calendar-declaration-v1`` document (``storage/calendar_declaration.py``): one row per
calendar date with ``calendar_id``, ``venue``, the IANA ``timezone``, ``session_date``,
``status`` (``open`` or ``closed``), the local wall-clock ``open_local`` and
``close_local`` of an open session, and the declaration's ``declared_at`` instant.

- ``open_at_us`` and ``close_at_us`` are the local wall times resolved in ``timezone``
  through DuckDB's ICU zone data; ``timezone_version`` is the spec argument naming that
  data. A closed date has neither.
- An open row needs both local times with the open before the close, and a closed row
  needs neither; any other row has no ``status`` and is refused as missing a required
  column. Nothing is repaired.
- The one time input, ``public_by``, is the latest instant at which the declared row can
  first have been public: the declaration instant, or the session's own end when that is
  earlier (the close of an open session, the last local microsecond of a closed date).
  Whether a venue opened on a date, and its hours, are facts by the end of that session,
  so a declaration made today states nothing later than that about a past date, while
  for a future date it states only what the declaration itself made public. The bound
  is computed from the record's date, so ``aas calendar refresh`` gives it to
  ``declared_session_end@1`` with basis ``record``: a strict reader uses it only under a
  grant, and a changed date takes the time AAS received the correcting declaration.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.promotion.formats import sql_literal
from aegis_alpha.storage.promotion.time_rules import InputKind

_VERSION: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,99}")


class CalendarDeclared:
    name: Final = "calendar.declared"
    major: Final = 1
    provider: Final = "calendar"
    domain: Final = "calendar_sessions"
    partition_sql: Final = '"session_date"'
    date_column: Final = "session_date"
    time_inputs: Final[Mapping[str, InputKind]] = {"public_by": "utc_us"}

    def check_args(self, args: Mapping[str, object]) -> None:
        if set(args) != {"timezone_version"}:
            raise ValueError("calendar.declared@1 takes exactly a timezone_version argument")
        version = args["timezone_version"]
        if not isinstance(version, str) or _VERSION.fullmatch(version) is None:
            raise ValueError("calendar.declared@1 timezone_version must be a short version label")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "calendar_id": frozenset({"VARCHAR"}),
            "venue": frozenset({"VARCHAR"}),
            "timezone": frozenset({"VARCHAR"}),
            "session_date": frozenset({"DATE"}),
            "status": frozenset({"VARCHAR"}),
            "open_local": frozenset({"TIMESTAMP"}),
            "close_local": frozenset({"TIMESTAMP"}),
            "declared_at": frozenset({"TIMESTAMP WITH TIME ZONE"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {}

    def identity(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        version = sql_literal(str(args["timezone_version"]))
        opened = "epoch_us(timezone(timezone, open_local))"
        closed = "epoch_us(timezone(timezone, close_local))"
        is_open = (
            "status = 'open' AND open_local IS NOT NULL AND close_local IS NOT NULL "
            "AND CAST(open_local AS DATE) = session_date "
            "AND CAST(close_local AS DATE) = session_date AND open_local < close_local"
        )
        is_closed = "status = 'closed' AND open_local IS NULL AND close_local IS NULL"
        day_end = (
            "epoch_us(timezone(timezone, CAST(session_date AS TIMESTAMP) + INTERVAL 1 DAY)) - 1"
        )
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "CAST(NULL AS BIGINT) AS _aas_ingested_at_us, "
            "calendar_id, venue, session_date, "
            f"CASE WHEN {is_open} THEN {opened} END AS open_at_us, "
            f"CASE WHEN {is_open} THEN {closed} END AS close_at_us, "
            f"CASE WHEN {is_open} THEN 'open' WHEN {is_closed} THEN 'closed' END AS status, "
            f"{version} AS timezone_version, "
            f"CASE WHEN {is_open} THEN least(epoch_us(declared_at), {closed}) "
            f"WHEN {is_closed} THEN least(epoch_us(declared_at), {day_end}) "
            f"END AS _aas_t_public_by FROM {source}"
        )
