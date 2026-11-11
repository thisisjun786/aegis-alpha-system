"""FX series as ``fx_rates``: text ``fred.fx_series@1`` and ``norgate.fx_history@1``, and
``eodhd.fx_history@1``.

Both read a table the legacy import keeps as text and promote the rows of the spec's
``series`` as one currency pair; rows of other series are not selected.

- ``fred.fx_series@1`` reads ``fred.series_csv@1`` sources (``fred-series-csv-*``): the
  ``observation_date,<SERIES>`` download as ``series_id``, ``observation_date``, ``value``.
- ``norgate.fx_history@1`` reads ``norgate.history_export@1`` sources
  (``norgate-history-csv-*``): the export's ``bars`` as ``symbol``, ``date``, ``close``.

``rate`` is the source text of the quote currency per one unit of the base, converted by
``decimal_text@1``: ``present`` only for a positive decimal, ``missing`` for no text,
empty text or FRED's ``.``, ``invalid`` otherwise. ``fixing_at_us`` is the last
microsecond of the row's date in the spec's ``timezone``; the time input ``fixing_date`` is
that date. A date that is not ``YYYY-MM-DD`` leaves the required fixing time empty, so the
promotion refuses it. The sources carry no collection time, so ingestion is the source's
``sl:`` retrieval.

The day end bounds the fixing only in a zone whose day ends after the provider's fixing or
close of that date: the fixing's own zone (New York for FRED's noon buying rates), or for a
close the provider does not place, ``Etc/GMT+12``, whose day end is the latest of any zone. A
quote currency's market zone is no bound by itself: onshore USDKRW trades until 02:00 the
next Seoul day. The fixing day says nothing about publication, so a spec gives
``local_day_end@1`` on ``fixing_date`` only when the provider publishes a fixing by that day
end; FRED's H.10 release publishes a week's fixings days later, so ``fred.fx_series@1`` takes
``unknown_null@1``.

``eodhd.fx_history@1`` reads the table ``aas collect qveris import`` commits for one EODHD
``<PAIR>.FOREX`` daily history (``qveris-fx-history-bars-*`` ``bars``): ``base_currency``,
``quote_currency``, the bar's ``date``, its binary64 ``close`` and the collection instant
``retrieved_at``. The collector held every bar that is not finite, positive and consistent,
and every bar of a warned download, in ``quarantine``; this mapper reads the rows table only.
Rows of the spec's ``base``/``quote`` pair are selected. ``rate`` is ``close`` in binary64 for
``float_shortest@1``, ``present`` when finite and positive, ``missing`` when absent and
``invalid`` otherwise. ``fixing_at_us`` is the last microsecond of ``date`` in the spec's
``timezone`` and the time input ``fixing_date`` is that date. Ingestion is ``retrieved_at``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.promotion.formats import sql_literal
from aegis_alpha.storage.promotion.mappers.common import (
    day_end_us,
    decimal_state,
    epoch_floor,
    exact_args,
    iso_day,
    pair_args,
    zone_arg,
)
from aegis_alpha.storage.promotion.time_rules import InputKind

_CURRENCY: Final = re.compile(r"[A-Z]{3}")


class TextFxSeries:
    major: Final = 1
    domain: Final = "fx_rates"
    date_column: Final = "fixing_at_us"
    time_inputs: Final[Mapping[str, InputKind]] = {"fixing_date": "date"}
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    expands: Final = False

    def __init__(  # noqa: PLR0913 -- one source shape: provider, prefix and three columns
        self, name: str, provider: str, prefix: str, *, series: str, day: str, value: str
    ) -> None:
        self.name = name
        self.provider = provider
        self.source_prefixes = (prefix,)
        self.partition_sql = iso_day(day)
        self._series, self._day, self._value = series, day, value

    def check_args(self, args: Mapping[str, object]) -> None:
        pair_args(f"{self.name}@1", args)

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {column: frozenset({"VARCHAR"}) for column in (self._series, self._day, self._value)}

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"rate": "VARCHAR"}

    def identity(self, args: Mapping[str, object]) -> None:
        del args

    def outcome(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        day = iso_day(self._day)
        value = self._value
        state = decimal_state(value, "positive")
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "CAST(NULL AS BIGINT) AS _aas_ingested_at_us, "
            f"{sql_literal(str(args['base']))} AS base_currency, "
            f"{sql_literal(str(args['quote']))} AS quote_currency, "
            f"{day_end_us(str(args['timezone']), day)} AS fixing_at_us, "
            f"CASE WHEN {state} = 'present' THEN {value} END AS rate, "
            f"{state} AS value_state, "
            f"{epoch_floor(day)} AS _aas_t_fixing_date "
            f"FROM {source} WHERE {self._series} = {sql_literal(str(args['series']))}"
        )


def fred_fx_series() -> TextFxSeries:
    return TextFxSeries(
        "fred.fx_series",
        "fred",
        "fred-series-csv-",
        series="series_id",
        day="observation_date",
        value="value",
    )


def norgate_fx_history() -> TextFxSeries:
    return TextFxSeries(
        "norgate.fx_history",
        "norgate",
        "norgate-history-csv-",
        series="symbol",
        day="date",
        value="close",
    )


class EodhdFxHistory:
    """``eodhd.fx_history@1``: one EODHD FOREX pair's daily closes."""

    name: Final = "eodhd.fx_history"
    major: Final = 1
    provider: Final = "eodhd"
    domain: Final = "fx_rates"
    source_prefixes: Final[tuple[str, ...]] = ("qveris-fx-history-bars-",)
    partition_sql: Final = '"date"'
    date_column: Final = "fixing_at_us"
    time_inputs: Final[Mapping[str, InputKind]] = {"fixing_date": "date"}
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    expands: Final = False

    def check_args(self, args: Mapping[str, object]) -> None:
        label = f"{self.name}@{self.major}"
        exact_args(label, args, {"base", "quote", "timezone"})
        base, quote = args["base"], args["quote"]
        for value in (base, quote):
            if not isinstance(value, str) or _CURRENCY.fullmatch(value) is None:
                raise ValueError(f"{label} base and quote must be three-letter currency codes")
        if base == quote:
            raise ValueError(f"{label} base and quote must differ")
        zone_arg(label, args)

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "base_currency": frozenset({"VARCHAR"}),
            "quote_currency": frozenset({"VARCHAR"}),
            "date": frozenset({"DATE"}),
            "close": frozenset({"DOUBLE"}),
            "retrieved_at": frozenset({"TIMESTAMP WITH TIME ZONE"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"rate": "DOUBLE"}

    def identity(self, args: Mapping[str, object]) -> None:
        del args

    def outcome(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        day = '"date"'
        present = "(isfinite(close) AND close > 0)"
        state = (
            f"CASE WHEN {present} THEN 'present' WHEN close IS NULL THEN 'missing' "
            "ELSE 'invalid' END"
        )
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "epoch_us(retrieved_at) AS _aas_ingested_at_us, "
            f"{sql_literal(str(args['base']))} AS base_currency, "
            f"{sql_literal(str(args['quote']))} AS quote_currency, "
            f"{day_end_us(str(args['timezone']), day)} AS fixing_at_us, "
            f"CASE WHEN {present} THEN close END AS rate, {state} AS value_state, "
            f"{epoch_floor(day)} AS _aas_t_fixing_date FROM {source} "
            f"WHERE base_currency = {sql_literal(str(args['base']))} "
            f"AND quote_currency = {sql_literal(str(args['quote']))}"
        )
