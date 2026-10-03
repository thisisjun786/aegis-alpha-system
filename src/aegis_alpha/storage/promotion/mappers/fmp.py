"""``fmp.eod_non_split@1``: FMP's frozen end-of-day responses as reference prices.

FMP stopped being collected; its retained responses are reference evidence and never a
canonical price or a strict execution input. The non-split-adjusted endpoint holds the
prices as traded, so its ``adjOpen``, ``adjHigh``, ``adjLow``, ``adjClose`` (binary64)
and ``volume`` (integer) become ``unadjusted`` USD bars with role ``reference``.

Every row names ``symbol``, ``date`` and the instant ``retrieved_at_utc`` it was collected.
The instrument is the pinned identity snapshot's resolution of (``fmp``, ``fmp_symbol``,
``symbol``) at the start of the session date in the spec's ``timezone``, and ``bar_end_us`` is
the last microsecond of that date there. A bar is ``present`` when all five values are finite
and nonnegative, ``missing`` when none is given, and ``invalid`` otherwise, keeping none.

FMP was collected again and again, and a later response can correct a bar. The tables keep
every response, so one session's bar can appear many times. Ordered by collection instant,
a bar's responses form runs of identical values; each run is one revision. ``revision`` (from
1) selects, for every bar, the first response of its run with that number: revision 1 is what
FMP first said, revision 2 its first correction, and so on. Promoting revisions 1, 2, ... as
consecutive generations gives each correction as a SUPERSEDE known from the instant it was
collected, while a bar that never changed is not repeated. Two responses collected at the
same instant that disagree are both selected in revision 1, so the promotion refuses the
repeated key instead of choosing one, and no later revision selects anything of that bar.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.promotion.mappers import IdentityKey
from aegis_alpha.storage.promotion.mappers.common import (
    day_end_us,
    exact_args,
    zone_arg,
    zone_start_us,
)
from aegis_alpha.storage.promotion.time_rules import InputKind

_TARGET: Final = ("open", "high", "low", "close", "volume")
_ADJUSTED: Final = ("adjOpen", "adjHigh", "adjLow", "adjClose")
_KEY: Final = "symbol, date"
_ORDER: Final = "retrieved_at_utc, _aas_pin, _aas_ordinal"


class FmpEodNonSplit:
    name: Final = "fmp.eod_non_split"
    major: Final = 1
    provider: Final = "fmp"
    source_prefixes: Final = ()
    domain: Final = "prices"
    partition_sql: Final = '"date"'
    date_column: Final = "session_date"
    time_inputs: Final[Mapping[str, InputKind]] = {"session_date": "date"}
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    prices: Final = _ADJUSTED

    def check_args(self, args: Mapping[str, object]) -> None:
        label = f"{self.name}@{self.major}"
        exact_args(label, args, {"timezone", "revision"})
        zone_arg(label, args)
        revision = args["revision"]
        if type(revision) is not int or revision < 1:
            raise ValueError(f"{label} revision is a positive integer")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "symbol": frozenset({"VARCHAR"}),
            "date": frozenset({"DATE"}),
            **{name: frozenset({"DOUBLE"}) for name in self.prices},
            "volume": frozenset({"BIGINT"}),
            "retrieved_at_utc": frozenset({"TIMESTAMP WITH TIME ZONE"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {**dict.fromkeys(_TARGET[:4], "DOUBLE"), "volume": "BIGINT"}

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return IdentityKey("fmp", "fmp_symbol")

    def runs_sql(self, source: str) -> str:
        """Every source row with its revision number, whether it starts it, and its bar's ties."""
        value = "(" + ", ".join(f'"{name}"' for name in (*self.prices, "volume")) + ")"
        return (
            "SELECT *, sum(CASE WHEN _aas_new THEN 1 ELSE 0 END) OVER (PARTITION BY "  # noqa: S608
            f"{_KEY} ORDER BY {_ORDER} ROWS UNBOUNDED PRECEDING) AS _aas_revision, "
            f"bool_or(_aas_tie) OVER (PARTITION BY {_KEY}) AS _aas_tied "
            f"FROM (SELECT *, {value} IS DISTINCT FROM lag({value}) OVER (PARTITION BY {_KEY} "
            f"ORDER BY {_ORDER}) AS _aas_new, count(DISTINCT {value}) OVER (PARTITION BY {_KEY}, "
            f"retrieved_at_utc) > 1 AS _aas_tie FROM {source})"
        )

    def select(self, source: str, args: Mapping[str, object]) -> str:
        zone = str(args["timezone"])
        revision = int(str(args["revision"]))
        sources = (*self.prices, "volume")
        present = " AND ".join(
            f'("{name}" >= 0 AND isfinite("{name}"))' if name != "volume" else '"volume" >= 0'
            for name in sources
        )
        missing = " AND ".join(f'"{name}" IS NULL' for name in sources)
        state = (
            f"CASE WHEN {present} THEN 'present' WHEN {missing} THEN 'missing' ELSE 'invalid' END"
        )
        values = ", ".join(
            f'CASE WHEN {present} THEN "{name}" END AS "{target}"'
            for name, target in zip(sources, _TARGET, strict=True)
        )
        # A bar with a tie belongs to revision 1 alone, where its repeated key is refused.
        chosen = f"(_aas_new AND _aas_revision = {revision})"
        chosen += " OR _aas_tie" if revision == 1 else " AND NOT _aas_tied"
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "epoch_us(retrieved_at_utc) AS _aas_ingested_at_us, "
            f"symbol AS _aas_id_token, {zone_start_us(zone, 'date')} AS _aas_id_at_us, "
            f"date AS session_date, '1d' AS interval, {day_end_us(zone, 'date')} AS bar_end_us, "
            "'unadjusted' AS basis, 'USD' AS currency, 'reference' AS price_role, "
            f"{state} AS value_state, {values}, date AS _aas_t_session_date "
            f"FROM ({self.runs_sql(source)}) WHERE {chosen}"
        )
