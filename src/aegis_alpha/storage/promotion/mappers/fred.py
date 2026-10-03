"""``fred.alfred@1``: ALFRED vintages as ``macro_observations``.

Input is an ALFRED observation table: ``series_id``, ``observation_date``,
the vintage's ``realtime_start``, the provider's ``value`` text and the collection instant
``retrieved_at_utc``. Other source columns (the vintage's ``realtime_end`` among them) are
not read; they stay in the source row and its hash.

- One record is one observation of one series: ``series_id``, ``observation_period`` (the
  observation date) and ``unit``. FRED states a series' units in its series metadata, which
  observation rows do not carry, so ``unit`` is ``as_published``: the units FRED published
  for the series in that vintage. A rebased or rescaled vintage is a new revision of the
  same record, never a second record.
- ``source_vintage_start`` is ``realtime_start``. ``source_vintage_end`` is NULL: a vintage
  ends where the next one starts, which the chain records as that next revision. A closed
  ``realtime_end`` in a source collected later states a fact that was not known while the
  vintage was current, so it is not copied onto it.
- One partition must hold at most one vintage of an observation, because a generation holds
  one revision per record. The partition column is ``realtime_start``; a backfill promotes
  the partitions ``vintage_partitions`` computes, in order, so each later vintage is a
  SUPERSEDE of the one before it. Daily maintenance promotes one vintage day at a time.
- The time input ``vintage_start`` is ``realtime_start`` (never before 1970-01-01); a spec
  gives it to ``local_day_end@1`` with basis ``revision``, so both a first vintage and a
  revision are known from the end of their own vintage day.

``fred.fx_series@1`` is a text FX series in ``mappers.fx``.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, timedelta
from itertools import pairwise
from typing import TYPE_CHECKING, Final

from aegis_alpha.storage.promotion.formats import sql_literal
from aegis_alpha.storage.promotion.mappers.common import decimal_state, epoch_floor
from aegis_alpha.storage.promotion.time_rules import InputKind

if TYPE_CHECKING:
    import duckdb

UNIT_AS_PUBLISHED: Final = "as_published"
_BATCH: Final = 65536


class FredAlfred:
    name: Final = "fred.alfred"
    major: Final = 1
    provider: Final = "fred"
    source_prefixes: Final = ()
    domain: Final = "macro_observations"
    partition_column: Final = "realtime_start"
    date_column: Final = "observation_period"
    time_inputs: Final[Mapping[str, InputKind]] = {"vintage_start": "date"}

    def check_args(self, args: Mapping[str, object]) -> None:
        if args:
            raise ValueError("fred.alfred@1 takes no arguments")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "series_id": frozenset({"VARCHAR"}),
            "observation_date": frozenset({"DATE"}),
            "realtime_start": frozenset({"DATE"}),
            "value": frozenset({"VARCHAR"}),
            "retrieved_at_utc": frozenset({"TIMESTAMP WITH TIME ZONE"}),
        }

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
            "epoch_us(retrieved_at_utc) AS _aas_ingested_at_us, "
            "series_id, observation_date AS observation_period, "
            f"{sql_literal(UNIT_AS_PUBLISHED)} AS unit, "
            "realtime_start AS source_vintage_start, "
            "CAST(NULL AS DATE) AS source_vintage_end, "
            f"CASE WHEN {state} = 'present' THEN value END AS value, "
            f"{state} AS value_state, "
            f"{epoch_floor('realtime_start')} AS _aas_t_vintage_start FROM {source}"
        )


def vintage_partitions(
    connection: duckdb.DuckDBPyConnection, relation: str
) -> list[tuple[date, date]]:
    """The fewest ordered ``[from, to)`` vintage partitions of an ALFRED table.

    Each partition holds at most one vintage of each observation, and together they cover
    every ``realtime_start`` in ``relation`` (an engine-quoted table or view name); a row
    without one is refused, because no partition would hold it. Two
    consecutive vintages ``a < b`` of one observation need a boundary in ``(a, b]``; taking
    the pairs in order of ``b`` and cutting at ``b`` only when no cut lies in ``(a, b]`` yet
    is the classic optimal interval stabbing.
    """
    bounds = connection.execute(
        "SELECT min(realtime_start), max(realtime_start), "  # noqa: S608
        f"count(*) FILTER (WHERE realtime_start IS NULL) FROM {relation}"
    ).fetchone()
    if bounds is None:
        return []
    first, last, undated = bounds
    if undated:
        raise ValueError(f"{undated} ALFRED rows have no vintage start, so no partition holds them")
    if first is None:
        return []
    cursor = connection.execute(
        "SELECT DISTINCT previous, realtime_start FROM (SELECT realtime_start, "  # noqa: S608
        "lag(realtime_start) OVER (PARTITION BY series_id, observation_date "
        f"ORDER BY realtime_start) AS previous FROM {relation}) "
        "WHERE previous IS NOT NULL ORDER BY realtime_start, previous"
    )
    cuts: list[date] = []
    while batch := cursor.fetchmany(_BATCH):
        for previous, start in batch:
            if previous == start:
                raise ValueError("an ALFRED observation repeats one vintage start")
            if not cuts or cuts[-1] <= previous:
                cuts.append(start)
    edges = [first, *cuts, last + timedelta(days=1)]
    return list(pairwise(edges))
