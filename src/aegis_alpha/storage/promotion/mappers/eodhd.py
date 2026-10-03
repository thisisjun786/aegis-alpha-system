"""``eodhd.bars@1``: provider daily bars as canonical unadjusted prices.

Input is the source library's EODHD daily bar table: ``provider_symbol``, ``date``,
unadjusted ``open``/``high``/``low``/``close``, ``volume`` (binary64), ``currency`` and
the collection instant ``retrieved_at``. Other source columns (the provider's adjusted
close among them) are not read here; they stay in the source row and its hash.

- The instrument is the pinned identity snapshot's resolution of the assertion key
  (``eodhd``, ``eodhd_symbol``, ``provider_symbol``) at the local start of the session
  date. A ticker never mints an instrument.
- ``bar_end_us`` is the last microsecond of the session date in the spec's ``timezone``,
  an upper bound on when a daily bar can end that needs no calendar.
- A bar with all five values finite and nonnegative is ``present``; a bar with none is
  ``missing``; any other bar is ``invalid`` and keeps no values. Values are never filled
  from neighbours.
- ``session_date`` is the one time input, for record-basis rules.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Final
from zoneinfo import ZoneInfo

from aegis_alpha.storage.promotion.formats import sql_literal
from aegis_alpha.storage.promotion.mappers import IdentityKey
from aegis_alpha.storage.promotion.time_rules import InputKind

_VALUES: Final = ("open", "high", "low", "close", "volume")
_ZONE: Final = re.compile(r"[A-Za-z][A-Za-z0-9_+-]*(?:/[A-Za-z0-9_+-]+)*")


class EodhdBars:
    name: Final = "eodhd.bars"
    major: Final = 1
    provider: Final = "eodhd"
    domain: Final = "prices"
    partition_column: Final = "date"
    date_column: Final = "session_date"
    time_inputs: Final[Mapping[str, InputKind]] = {"session_date": "date"}

    def check_args(self, args: Mapping[str, object]) -> None:
        if set(args) != {"timezone"}:
            raise ValueError("eodhd.bars@1 takes exactly a timezone argument")
        zone = args["timezone"]
        if not isinstance(zone, str) or _ZONE.fullmatch(zone) is None:
            raise ValueError("eodhd.bars@1 timezone must be an IANA zone name")
        ZoneInfo(zone)

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "provider_symbol": frozenset({"VARCHAR"}),
            "date": frozenset({"DATE"}),
            **{name: frozenset({"DOUBLE"}) for name in _VALUES},
            "currency": frozenset({"VARCHAR"}),
            "retrieved_at": frozenset({"TIMESTAMP WITH TIME ZONE"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return dict.fromkeys(_VALUES, "DOUBLE")

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return IdentityKey("eodhd", "eodhd_symbol")

    def select(self, source: str, args: Mapping[str, object]) -> str:
        zone = sql_literal(str(args["timezone"]))
        start = f"timezone({zone}, CAST(date AS TIMESTAMP))"
        present = " AND ".join(f"(isfinite({name}) AND {name} >= 0)" for name in _VALUES)
        missing = " AND ".join(f"{name} IS NULL" for name in _VALUES)
        state = (
            f"CASE WHEN {present} THEN 'present' WHEN {missing} THEN 'missing' ELSE 'invalid' END"
        )
        values = ", ".join(f"CASE WHEN {present} THEN {name} END AS {name}" for name in _VALUES)
        end = f"epoch_us(timezone({zone}, CAST(date AS TIMESTAMP) + INTERVAL 1 DAY)) - 1"
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "epoch_us(retrieved_at) AS _aas_ingested_at_us, "
            f"provider_symbol AS _aas_id_token, epoch_us({start}) AS _aas_id_at_us, "
            "date AS session_date, '1d' AS interval, "
            f"{end} AS bar_end_us, "
            "'unadjusted' AS basis, currency, 'canonical' AS price_role, "
            f"{state} AS value_state, {values}, "
            f"date AS _aas_t_session_date FROM {source}"
        )
