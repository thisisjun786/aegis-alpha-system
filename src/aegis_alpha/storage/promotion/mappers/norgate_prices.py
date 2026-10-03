"""Norgate daily prices: canonical unadjusted bars, provider-adjusted and reference closes.

Every mapper resolves the instrument through the assertion key (``norgate``,
``norgate_assetid``) with the row's asset ID as the token, at the start of its session date
in the spec's ``timezone``; the asset ID is Norgate's permanent anchor, so the claim holds
for the whole series. ``bar_end_us`` is the last microsecond of that session date in the
same zone. Norgate rows carry no collection time, so ingestion is the source's ``sl:``
retrieval. Nothing is filled from a neighbour; a bar with some values and not others is
``invalid`` and keeps none.

- ``norgate.prices_none@1`` reads a history CSV export (``norgate.history_export@1``: the
  ``bars`` table of ``norgate-history-csv`` sources, every value the CSV text) of the US
  equity databases (``US Equities``, ``US Equities Delisted``) as canonical ``unadjusted``
  USD bars. Open, high, low, close and volume are the CSV texts for ``decimal_text@1``, so a
  volume Norgate wrote as ``1.6357e+06`` keeps exactly that value and its precision flag. A
  bar is ``present`` when all five texts are unsigned decimals. A row of another database
  or with a date that is not ``YYYY-MM-DD`` has no session date and is refused.
- ``norgate.prices_adjusted@1`` reads Norgate's adjusted price parts (``assetid``, a
  nanosecond ``date`` at midnight, binary32 OHLCV, ``adjustment_type``) as ``reference``
  USD bars: ``CAPITAL`` rows are ``split_adjusted`` and ``TOTALRETURN`` rows
  ``total_return``. The binary32 values go to ``float_shortest@1``. A bar is ``present``
  when all five values are finite and nonnegative. Another adjustment type or a date with a
  time of day has no basis or session date and is refused.
- ``norgate.reference_closes@1`` reads a reference close table (``symbol``, ``assetid``,
  ``date``, the binary64 ``close`` and the export row ``raw_row_json``) as close-only
  (``fields='close'``) ``reference`` prices. The close is the export's ``Close`` text for
  ``decimal_text@1``, the value Norgate wrote, and it is ``present`` only when that text is
  an unsigned decimal whose double is ``close`` and the row's ``Date`` text is its date.
- ``norgate.reference_history@1`` reads a history CSV export of any other Norgate database
  (indices, economic series, spot FX and commodities) as close-only ``reference`` prices
  from its ``Close`` text. An equity row has no session date and is refused, so an equity
  export never becomes a reference series by mistake.

A negative level (a spread or a rate below zero) is ``invalid``: the price domain holds no
negative value. A reference series level is not an amount of money, so both close-only
mappers name the currency ``XXX`` (ISO 4217 "no currency") and the basis ``unadjusted``:
the series as Norgate publishes it. Their one time input, ``session_date``, is never before
1970-01-01 (some indices start in the 1890s); the canonical and adjusted equity bars start
in 1990.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.promotion.formats import sql_literal
from aegis_alpha.storage.promotion.mappers import IdentityKey
from aegis_alpha.storage.promotion.mappers.daily import (
    bar_state,
    day_end_us,
    epoch_floor,
    exact_args,
    iso_day,
    text_state,
    zone_arg,
    zone_start_us,
)
from aegis_alpha.storage.promotion.time_rules import InputKind

EQUITY_DATABASES: Final = ("US Equities", "US Equities Delisted")
NO_CURRENCY: Final = "XXX"
_VALUES: Final = ("open", "high", "low", "close", "volume")
_IDENTITY: Final = IdentityKey("norgate", "norgate_assetid")
_ADJUSTMENTS: Final = {"CAPITAL": "split_adjusted", "TOTALRETURN": "total_return"}
_CLOSE_ONLY: Final = ("open", "high", "low", "volume")


def _equity(column: str) -> str:
    listed = ", ".join(sql_literal(name) for name in EQUITY_DATABASES)
    return f"{column} IN ({listed})"


def _head(zone: str, day: str) -> str:
    """The pass-through, ingestion and identity columns every Norgate mapper starts with."""
    return (
        "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "
        "CAST(NULL AS BIGINT) AS _aas_ingested_at_us, "
        "CAST(assetid AS VARCHAR) AS _aas_id_token, "
        f"{zone_start_us(zone, day)} AS _aas_id_at_us, "
    )


def _bar(zone: str, day: str, basis: str, currency: str, role: str) -> str:
    return (
        f"{day} AS session_date, '1d' AS interval, {day_end_us(zone, day)} AS bar_end_us, "
        f"{basis} AS basis, {sql_literal(currency)} AS currency, "
        f"{sql_literal(role)} AS price_role, "
    )


class _Norgate:
    provider: Final = "norgate"
    domain: Final = "prices"
    date_column: Final = "session_date"
    time_inputs: Final[Mapping[str, InputKind]] = {"session_date": "date"}
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    name: str
    major: int

    def check_args(self, args: Mapping[str, object]) -> None:
        exact_args(f"{self.name}@{self.major}", args, {"timezone"})
        zone_arg(f"{self.name}@{self.major}", args)

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return _IDENTITY


class NorgatePricesNone(_Norgate):
    name: Final = "norgate.prices_none"
    major: Final = 1
    partition_sql: Final = iso_day('"date"')

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "assetid": frozenset({"BIGINT"}),
            "database": frozenset({"VARCHAR"}),
            "date": frozenset({"VARCHAR"}),
            **{name: frozenset({"VARCHAR"}) for name in _VALUES},
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return dict.fromkeys(_VALUES, "VARCHAR")

    def select(self, source: str, args: Mapping[str, object]) -> str:
        zone = str(args["timezone"])
        day = f"CASE WHEN {_equity('database')} THEN {iso_day('date')} END"
        state = bar_state([text_state(f'"{name}"') for name in _VALUES])
        values = ", ".join(
            f'CASE WHEN {state} = \'present\' THEN "{name}" END AS "{name}"' for name in _VALUES
        )
        return (
            _head(zone, day)
            + _bar(zone, day, "'unadjusted'", "USD", "canonical")
            + f"{state} AS value_state, {values}, {day} AS _aas_t_session_date "
            f"FROM {source}"
        )


class NorgatePricesAdjusted(_Norgate):
    name: Final = "norgate.prices_adjusted"
    major: Final = 1
    partition_sql: Final = 'CAST("date" AS DATE)'

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "assetid": frozenset({"BIGINT"}),
            "date": frozenset({"TIMESTAMP_NS"}),
            **{name: frozenset({"FLOAT"}) for name in _VALUES},
            "adjustment_type": frozenset({"VARCHAR"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return dict.fromkeys(_VALUES, "FLOAT")

    def select(self, source: str, args: Mapping[str, object]) -> str:
        zone = str(args["timezone"])
        day = 'CASE WHEN "date" = date_trunc(\'day\', "date") THEN CAST("date" AS DATE) END'
        basis = (
            "CASE adjustment_type "
            + " ".join(
                f"WHEN {sql_literal(kind)} THEN {sql_literal(name)}"
                for kind, name in _ADJUSTMENTS.items()
            )
            + " END"
        )
        present = " AND ".join(f'(isfinite("{name}") AND "{name}" >= 0)' for name in _VALUES)
        missing = " AND ".join(f'"{name}" IS NULL' for name in _VALUES)
        state = (
            f"CASE WHEN {present} THEN 'present' WHEN {missing} THEN 'missing' ELSE 'invalid' END"
        )
        values = ", ".join(f'CASE WHEN {present} THEN "{name}" END AS "{name}"' for name in _VALUES)
        return (
            _head(zone, day)
            + _bar(zone, day, basis, "USD", "reference")
            + f"{state} AS value_state, {values}, {day} AS _aas_t_session_date "
            f"FROM {source}"
        )


def _close_only(zone: str, day: str, close: str, state: str) -> str:
    """The domain columns of a close-only reference row from its close text and state."""
    nulls = ", ".join(f'CAST(NULL AS DECIMAL(38,12)) AS "{name}"' for name in _CLOSE_ONLY)
    return (
        _bar(zone, day, "'unadjusted'", NO_CURRENCY, "reference")
        + f"{state} AS value_state, {nulls}, "
        f"CASE WHEN {state} = 'present' THEN {close} END AS close, 'close' AS \"fields\", "
        f"{epoch_floor(day)} AS _aas_t_session_date "
    )


class NorgateReferenceCloses(_Norgate):
    name: Final = "norgate.reference_closes"
    major: Final = 1
    partition_sql: Final = '"date"'

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "assetid": frozenset({"BIGINT"}),
            "date": frozenset({"DATE"}),
            "close": frozenset({"DOUBLE"}),
            "raw_row_json": frozenset({"VARCHAR"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"close": "VARCHAR"}

    def select(self, source: str, args: Mapping[str, object]) -> str:
        zone = str(args["timezone"])
        text = "json_extract_string(raw_row_json, '$.Close')"
        stated = "json_extract_string(raw_row_json, '$.Date')"
        agrees = f"TRY_CAST({text} AS DOUBLE) = close AND {stated} = strftime(date, '%Y-%m-%d')"
        found = text_state(text)
        state = (
            f"CASE WHEN close IS NULL AND {found} = 'missing' THEN 'missing' "
            f"WHEN {found} = 'present' AND coalesce({agrees}, false) THEN 'present' "
            "ELSE 'invalid' END"
        )
        return _head(zone, '"date"') + _close_only(zone, '"date"', text, state) + f"FROM {source}"


class NorgateReferenceHistory(_Norgate):
    name: Final = "norgate.reference_history"
    major: Final = 1
    partition_sql: Final = iso_day('"date"')

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "assetid": frozenset({"BIGINT"}),
            "database": frozenset({"VARCHAR"}),
            "date": frozenset({"VARCHAR"}),
            "close": frozenset({"VARCHAR"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"close": "VARCHAR"}

    def select(self, source: str, args: Mapping[str, object]) -> str:
        zone = str(args["timezone"])
        reference = f"database IS NOT NULL AND NOT {_equity('database')}"
        day = f"CASE WHEN {reference} THEN {iso_day('date')} END"
        state = text_state("close")
        return _head(zone, day) + _close_only(zone, day, "close", state) + f"FROM {source}"
