"""``norgate.fx_closes@1``: one Norgate currency series' daily closes as ``fx_rates``.

Input is a Norgate reference close table: ``symbol``, ``date``, the binary64 ``close`` and
``raw_row_json``, the export's own row (``Date``, ``Open``, ``High``, ``Low``, ``Close`` as
text). The table holds many reference series; the rows of the spec's ``series`` are this
pair and the others are not selected.

- ``rate`` is the export's ``Close`` text, the quote currency per one unit of the base,
  converted by ``decimal_text@1``: the value Norgate wrote, not the binary double read from
  it. A row whose ``Date`` is not the row's ``date`` is ``invalid``. Otherwise a row is
  ``present`` only when the text is a positive decimal whose double equals ``close``, and
  ``missing`` when it has neither close; any other row is ``invalid`` and keeps no rate.
- ``fixing_at_us`` is the last microsecond of ``date`` in the spec's ``timezone``, an upper
  bound on the close that needs no fixing schedule; the time input ``fixing_date`` is
  ``date``.
- The table has no collection time, so ingestion is the source's ``sl:`` retrieval.

The legacy import of a Norgate export keeps its rows as text; ``norgate.fx_history@1`` in
``mappers.fx`` reads that shape.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.promotion.formats import sql_literal
from aegis_alpha.storage.promotion.mappers.common import (
    day_end_us,
    decimal_state,
    epoch_floor,
    pair_args,
)
from aegis_alpha.storage.promotion.time_rules import InputKind

_CLOSE: Final = "json_extract_string(raw_row_json, '$.Close')"
_DATE: Final = "json_extract_string(raw_row_json, '$.Date')"


class NorgateFxCloses:
    name: Final = "norgate.fx_closes"
    major: Final = 1
    provider: Final = "norgate"
    source_prefixes: Final = ()
    domain: Final = "fx_rates"
    partition_column: Final = "date"
    date_column: Final = None
    time_inputs: Final[Mapping[str, InputKind]] = {"fixing_date": "date"}

    def check_args(self, args: Mapping[str, object]) -> None:
        pair_args("norgate.fx_closes@1", args)

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "symbol": frozenset({"VARCHAR"}),
            "date": frozenset({"DATE"}),
            "close": frozenset({"DOUBLE"}),
            "raw_row_json": frozenset({"VARCHAR"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"rate": "VARCHAR"}

    def identity(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        text = decimal_state(_CLOSE)
        dated = f"coalesce({_DATE} = strftime(date, '%Y-%m-%d'), false)"
        agrees = f"coalesce(TRY_CAST({_CLOSE} AS DOUBLE) = close AND close > 0, false)"
        state = (
            f"CASE WHEN NOT {dated} THEN 'invalid' "
            f"WHEN close IS NULL AND {text} = 'missing' THEN 'missing' "
            f"WHEN {text} = 'present' AND {agrees} THEN 'present' "
            "ELSE 'invalid' END"
        )
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "CAST(NULL AS BIGINT) AS _aas_ingested_at_us, "
            f"{sql_literal(str(args['base']))} AS base_currency, "
            f"{sql_literal(str(args['quote']))} AS quote_currency, "
            f"{day_end_us(str(args['timezone']), 'date')} AS fixing_at_us, "
            f"CASE WHEN {state} = 'present' THEN {_CLOSE} END AS rate, "
            f"{state} AS value_state, "
            f"{epoch_floor('date')} AS _aas_t_fixing_date "
            f"FROM {source} WHERE symbol = {sql_literal(str(args['series']))}"
        )
