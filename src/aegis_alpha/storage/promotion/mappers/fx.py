"""Text FX series as ``fx_rates``: ``fred.fx_series@1`` and ``norgate.fx_history@1``.

Both read a table the legacy import keeps as text and promote the rows of the spec's
``series`` as one currency pair; rows of other series are not selected.

- ``fred.fx_series@1`` reads ``fred.series_csv@1`` sources (``fred-series-csv-*``): the
  ``observation_date,<SERIES>`` download as ``series_id``, ``observation_date``, ``value``.
- ``norgate.fx_history@1`` reads ``norgate.history_export@1`` sources
  (``norgate-history-csv-*``): the export's ``bars`` as ``symbol``, ``date``, ``close``.

``rate`` is the source text of the quote currency per one unit of the base, converted by
``decimal_text@1``: ``present`` only for a positive plain decimal, ``missing`` for no text,
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
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.promotion.formats import sql_literal
from aegis_alpha.storage.promotion.mappers.common import (
    day_end_us,
    decimal_state,
    epoch_floor,
    iso_day,
    pair_args,
)
from aegis_alpha.storage.promotion.time_rules import InputKind


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
        state = (
            f"CASE WHEN {decimal_state(value)} = 'present' AND "
            f"TRY_CAST({value} AS DOUBLE) <= 0 THEN 'invalid' "
            f"ELSE {decimal_state(value)} END"
        )
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
