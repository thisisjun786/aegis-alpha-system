"""``bok.observations@1`` and ``oecd.observations@1``: KR public series as ``macro_observations``.

Input is the observation table the legacy ``korea.public_response@1`` import keeps for one
provider (BOK ``bok-observations``, OECD ``oecd-observations``): ``series_id``, ``period``,
``value``, ``units``, ``unit_multiplier``, ``base_period`` and ``regime`` as text, beside
columns that stay only in the source row (``value_raw``, ``status``).

- ``observation_period`` is the first day of ``period``: ``YYYY-MM-DD`` itself, ``YYYY-MM``
  its month, ``YYYY-Qn`` its quarter, ``YYYY`` its year. Any other spelling leaves the
  required column empty and the promotion refuses it.
- ``unit`` is ``units``, followed by ``;base=<base_period>``, ``;multiplier=<unit_multiplier>``
  and ``;regime=<regime>`` when the source states them, so a rebased or rescaled series, or a
  rate set under another definition (BOK's call-rate target before its base rate), is a
  different unit rather than a revision of the same number. A row without ``units`` is
  refused.
- The responses carry no vintage and no publication time, so these mappers declare no time
  input: a spec gives both time columns ``unknown_null@1`` and strict readers never select
  these rows. ``source_vintage_start`` and ``source_vintage_end`` are NULL; ingestion is the
  source's ``sl:`` retrieval.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.promotion.mappers.common import decimal_state
from aegis_alpha.storage.promotion.time_rules import InputKind

_TEXT: Final = (
    "series_id",
    "period",
    "value",
    "units",
    "unit_multiplier",
    "base_period",
    "regime",
)
_PERIOD: Final = (
    "CASE "
    "WHEN regexp_full_match(period, '[0-9]{4}-[0-9]{2}-[0-9]{2}') "
    "THEN CAST(try_strptime(period, '%Y-%m-%d') AS DATE) "
    "WHEN regexp_full_match(period, '[0-9]{4}-[0-9]{2}') "
    "THEN CAST(try_strptime(period || '-01', '%Y-%m-%d') AS DATE) "
    "WHEN regexp_full_match(period, '[0-9]{4}-Q[1-4]') "
    "THEN CAST(make_date(CAST(substr(period, 1, 4) AS INTEGER), "
    "3 * CAST(substr(period, 7, 1) AS INTEGER) - 2, 1) AS DATE) "
    "WHEN regexp_full_match(period, '[0-9]{4}') "
    "THEN CAST(try_strptime(period || '-01-01', '%Y-%m-%d') AS DATE) END"
)
_UNIT: Final = (
    "CASE WHEN units IS NULL OR units = '' THEN NULL ELSE units "
    "|| CASE WHEN base_period IS NULL OR base_period = '' THEN '' "
    "ELSE ';base=' || base_period END "
    "|| CASE WHEN unit_multiplier IS NULL OR unit_multiplier = '' THEN '' "
    "ELSE ';multiplier=' || unit_multiplier END "
    "|| CASE WHEN regime IS NULL OR regime = '' THEN '' "
    "ELSE ';regime=' || regime END END"
)


class KoreaObservations:
    major: Final = 1
    domain: Final = "macro_observations"
    partition_sql: Final = _PERIOD
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    date_column: Final = "observation_period"
    time_inputs: Final[Mapping[str, InputKind]] = {}

    def __init__(self, provider: str) -> None:
        self.provider = provider
        self.name = f"{provider}.observations"
        self.source_prefixes = (f"{provider}-observations-",)

    def check_args(self, args: Mapping[str, object]) -> None:
        if args:
            raise ValueError(f"{self.name}@1 takes no arguments")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {name: frozenset({"VARCHAR"}) for name in _TEXT}

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"value": "VARCHAR"}

    def identity(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        del args
        state = decimal_state("value")
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "CAST(NULL AS BIGINT) AS _aas_ingested_at_us, "
            f"series_id, {_PERIOD} AS observation_period, {_UNIT} AS unit, "
            "CAST(NULL AS DATE) AS source_vintage_start, "
            "CAST(NULL AS DATE) AS source_vintage_end, "
            f"CASE WHEN {state} = 'present' THEN value END AS value, "
            f"{state} AS value_state FROM {source}"
        )
