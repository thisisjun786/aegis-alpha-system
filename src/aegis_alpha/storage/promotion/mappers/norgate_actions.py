"""Norgate corporate actions and listing status.

Both action mappers read Norgate's adjusted price parts (``assetid``, a nanosecond ``date``
at midnight, binary32 ``close``, ``unadjusted_close`` and ``dividend``, ``adjustment_type``)
and only their ``CAPITAL`` rows, in each asset's date order over every pinned part (an
asset's series can continue in the next part, so a spec pins all of them). Norgate states
no event table of its own: its ``CAPITAL`` close is the unadjusted close divided by the
product of every later capital event's share ratio, and its ``dividend`` is the cash paid,
in that same capital basis, on the last session before the ex-date. The mappers read those
facts back as ``corporate_actions`` and resolve the instrument through (``norgate``,
``norgate_assetid``) at the start of the ex-date in the spec's ``timezone``. Both rows
carry no collection time, so ingestion is the source's ``sl:`` retrieval, and their one time
input is ``ex_date`` (``exdate_open@1``).

- ``norgate.dividends@1`` selects every row with a positive ``dividend``. The ex-date is the
  next session of the asset's series, the first session Norgate's total return series
  prices without the dividend; a dividend on a series' last row has no ex-date and is not
  selected. The amount is the cash per share as paid: the capital-basis dividend times the
  row's ``unadjusted_close / close``, rounded to binary32 (the precision of all three
  inputs) for ``float_shortest@1``. ``action_id`` is ``dividend:<ex-date>``, the currency
  ``USD``. Any nonzero dividend is selected: a row whose dividend, close or unadjusted close
  is not finite and positive is ``invalid`` and keeps no amount. A next row whose ``date`` is
  not at midnight gives no ex-date, which refuses the row.
- ``norgate.capital_adjustments@1`` selects the sessions where the series' capital factor
  ``f = unadjusted_close / close`` steps. The ratio is ``f`` of the asset's previous row
  over ``f`` of this row: the new shares per old share of every capital event Norgate folds
  into ``CAPITAL`` on that ex-date (splits, consolidations, stock dividends and other capital
  distributions), rounded to binary32 for ``float_shortest@1``. A step counts when the ratio
  differs from 1 by more than one part in a million: binary32 storage moves ``f`` between
  consecutive rows by less than that (the live parts' largest such move is below 2e-7 and
  the smallest real event above 1e-5). Rows with a close or unadjusted close that is not
  finite and positive carry no factor. A row is compared with the last row that has one;
  when a factorless row lies between them, the step's session is unknown, so the action is
  ``invalid`` with no ratio on the row that shows it. A step on a row whose ``date`` is not
  at midnight has no ex-date, which refuses the row. ``action_id`` is
  ``capital_adjustment:<ex-date>``.

Neither mapper can be partitioned: each reads the asset's neighbouring row, which a partition
would cut off, so its partition date is always NULL and a partitioned spec is refused.

``norgate.status@1`` reads the security master (``assetid``, ``is_delisted``, ``first_date``
and ``last_date`` texts) as ``instrument_status``. Its argument ``event`` picks one event per
master row: ``listed`` (every row) starts at the local start of ``first_date``, the first
session of Norgate's series, which can be later than the listing itself (``reason``
``norgate_first_date``); ``delisted`` (rows with ``is_delisted``) starts at the local start of
the day after ``last_date``, the series' last session (``reason`` ``norgate_last_date``).
Neither has an end. The time input ``status_date`` is the date the event is read from, so a
delisting is never known before the series' last session, and the instrument is resolved at
the local start of that date. A date that is not ``YYYY-MM-DD``, a ``last_date`` before
``first_date`` or a delisting on the last representable date leaves the event without a
start, which refuses the row.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.promotion.formats import sql_literal
from aegis_alpha.storage.promotion.mappers import IdentityKey
from aegis_alpha.storage.promotion.mappers.common import (
    exact_args,
    iso_day,
    zone_arg,
    zone_start_us,
)
from aegis_alpha.storage.promotion.time_rules import InputKind

_IDENTITY: Final = IdentityKey("norgate", "norgate_assetid")
# Relative capital-factor step below which consecutive binary32 rows differ only by storage.
STEP_TOLERANCE: Final = "0.000001"
_DAY: Final = 'CASE WHEN "date" = date_trunc(\'day\', "date") THEN CAST("date" AS DATE) END'
_EVENTS: Final = {"listed": "first_date", "delisted": "last_date"}


def _positive(column: str) -> str:
    return f'(isfinite("{column}") AND "{column}" > 0)'


class _NorgateActions:
    provider: Final = "norgate"
    domain: Final = "corporate_actions"
    source_prefixes: Final[tuple[str, ...]] = ()
    # The mapper reads neighbouring rows, so no partition may cut a series.
    partition_sql: Final = "CAST(NULL AS DATE)"
    date_column: Final = "effective_date"
    time_inputs: Final[Mapping[str, InputKind]] = {"ex_date": "date"}
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    expands: Final = False
    name: str
    major: int

    def check_args(self, args: Mapping[str, object]) -> None:
        exact_args(f"{self.name}@{self.major}", args, {"timezone"})
        zone_arg(f"{self.name}@{self.major}", args)

    def outcome(self, args: Mapping[str, object]) -> None:
        del args

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "assetid": frozenset({"BIGINT"}),
            "date": frozenset({"TIMESTAMP_NS"}),
            "close": frozenset({"FLOAT"}),
            "unadjusted_close": frozenset({"FLOAT"}),
            "dividend": frozenset({"FLOAT"}),
            "adjustment_type": frozenset({"VARCHAR"}),
        }

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return _IDENTITY

    @staticmethod
    def _action(zone: str, kind: str, ex_date: str) -> str:
        """The pass-through, identity and key columns of one action on ``ex_date``."""
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "
            "CAST(NULL AS BIGINT) AS _aas_ingested_at_us, "
            "CAST(assetid AS VARCHAR) AS _aas_id_token, "
            f"{zone_start_us(zone, ex_date)} AS _aas_id_at_us, "
            f"{sql_literal(kind + ':')} || strftime({ex_date}, '%Y-%m-%d') AS action_id, "
            f"{sql_literal(kind)} AS action_type, {ex_date} AS ex_date, "
            "CAST(NULL AS DATE) AS record_date, CAST(NULL AS DATE) AS pay_date, "
            f"{ex_date} AS effective_date, "
        )


class NorgateDividends(_NorgateActions):
    name: Final = "norgate.dividends"
    major: Final = 1

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"amount": "FLOAT"}

    def select(self, source: str, args: Mapping[str, object]) -> str:
        zone = str(args["timezone"])
        present = " AND ".join(
            _positive(name) for name in ("dividend", "close", "unadjusted_close")
        )
        paid = 'CAST(CAST("dividend" AS DOUBLE) * "unadjusted_close" / "close" AS FLOAT)'
        return (
            self._action(zone, "dividend", "_aas_ex")
            + f"CASE WHEN {present} THEN {paid} END AS amount, "  # noqa: S608 -- engine-named relation
            "CAST(NULL AS DECIMAL(38,12)) AS ratio, 'USD' AS currency, "
            f"CASE WHEN {present} THEN 'present' ELSE 'invalid' END AS value_state, "
            "_aas_ex AS _aas_t_ex_date "
            f'FROM (SELECT *, lead({_DAY}) OVER (PARTITION BY assetid ORDER BY "date") AS _aas_ex, '
            'lead("date") OVER (PARTITION BY assetid ORDER BY "date") AS _aas_next '
            f"FROM {source} WHERE adjustment_type = 'CAPITAL') "
            'WHERE ("dividend" <> 0 OR isnan("dividend")) AND _aas_next IS NOT NULL'
        )


class NorgateCapitalAdjustments(_NorgateActions):
    name: Final = "norgate.capital_adjustments"
    major: Final = 1

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"ratio": "FLOAT"}

    def select(self, source: str, args: Mapping[str, object]) -> str:
        zone = str(args["timezone"])
        factor = 'CAST("unadjusted_close" AS DOUBLE) / "close"'
        usable = f"{_positive('close')} AND {_positive('unadjusted_close')}"
        series = 'OVER (PARTITION BY assetid ORDER BY "date"'
        factors = (
            f"SELECT *, {_DAY} AS _aas_day, CASE WHEN {usable} THEN {factor} END AS _aas_f "  # noqa: S608 -- engine-named relation
            f"FROM {source} WHERE adjustment_type = 'CAPITAL'"
        )
        # The last factor before this row, and whether the row just before carries it.
        steps = (
            f"SELECT *, last_value(_aas_f IGNORE NULLS) {series} ROWS BETWEEN UNBOUNDED "  # noqa: S608 -- code-owned fragments
            f"PRECEDING AND 1 PRECEDING) / _aas_f AS _aas_step, "
            f"lag(_aas_f) {series}) IS NOT NULL AS _aas_adjacent FROM ({factors})"
        )
        return (
            self._action(zone, "capital_adjustment", "_aas_day")
            + "CAST(NULL AS DECIMAL(38,12)) AS amount, "
            "CASE WHEN _aas_adjacent THEN CAST(_aas_step AS FLOAT) END AS ratio, "
            "CAST(NULL AS VARCHAR) AS currency, "
            "CASE WHEN _aas_adjacent THEN 'present' ELSE 'invalid' END AS value_state, "
            "_aas_day AS _aas_t_ex_date "
            f"FROM ({steps}) WHERE abs(_aas_step - 1) > {STEP_TOLERANCE}"
        )


class NorgateStatus:
    name: Final = "norgate.status"
    major: Final = 1
    provider: Final = "norgate"
    domain: Final = "instrument_status"
    source_prefixes: Final[tuple[str, ...]] = ()
    date_column: Final = "effective_from_us"
    time_inputs: Final[Mapping[str, InputKind]] = {"status_date": "date"}
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    expands: Final = False
    # The master is one snapshot; it has no session date to partition on.
    partition_sql: Final = "CAST(NULL AS DATE)"

    def check_args(self, args: Mapping[str, object]) -> None:
        label = f"{self.name}@{self.major}"
        exact_args(label, args, {"timezone", "event"})
        zone_arg(label, args)
        if args["event"] not in _EVENTS:
            raise ValueError(f"{label} event is one of {sorted(_EVENTS)}")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "assetid": frozenset({"BIGINT"}),
            "is_delisted": frozenset({"BOOLEAN"}),
            "first_date": frozenset({"VARCHAR"}),
            "last_date": frozenset({"VARCHAR"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {}

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return _IDENTITY

    def outcome(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        zone = str(args["timezone"])
        event = str(args["event"])
        day = iso_day(f'"{_EVENTS[event]}"')
        first, last = iso_day('"first_date"'), iso_day('"last_date"')
        # A series that ends before it starts has no consistent listing or delisting.
        ordered = f"coalesce({last} >= {first}, true)"
        if event == "listed":
            start = f"CASE WHEN {ordered} THEN {day} END"
        else:
            start = f"CASE WHEN {ordered} AND {day} < DATE '9999-12-31' THEN {day} + 1 END"
        where = "" if event == "listed" else " WHERE is_delisted"
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "CAST(NULL AS BIGINT) AS _aas_ingested_at_us, "
            "CAST(assetid AS VARCHAR) AS _aas_id_token, "
            f"{zone_start_us(zone, day)} AS _aas_id_at_us, "
            f"{sql_literal('norgate:' + event)} AS status_event_id, "
            f"{zone_start_us(zone, start)} AS effective_from_us, "
            "CAST(NULL AS BIGINT) AS effective_to_us, "
            f"{sql_literal(event)} AS status, "
            f"{sql_literal('norgate_' + _EVENTS[event])} AS reason, "
            f"{day} AS _aas_t_status_date FROM {source}{where}"
        )
