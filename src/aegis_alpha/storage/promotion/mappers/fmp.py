"""FMP's frozen responses: end-of-day prices, dividends and splits as reference evidence.

``fmp.dividends@1`` and ``fmp.splits@1`` (``_FmpActions``) read the dividend and split
responses into ``corporate_actions`` with the same revision runs as the prices below.

``fmp.eod_non_split@1`` reads the end-of-day responses as reference prices.

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


def revision_runs(source: str, key: str, values: tuple[str, ...]) -> str:
    """Every source row with its revision number, whether it starts one, and its key's ties.

    Ordered by collection instant within each ``key``, ``_aas_new`` marks a row whose
    ``values`` differ from the previous response's, ``_aas_revision`` counts those starts,
    ``_aas_tie`` marks a response that gave two different values for the key at one instant
    and ``_aas_tied`` marks every row of a key that ever had such a response.
    """
    value = "(" + ", ".join(f'"{name}"' for name in values) + ")"
    return (
        "SELECT *, sum(CASE WHEN _aas_new THEN 1 ELSE 0 END) OVER (PARTITION BY "  # noqa: S608
        f"{key} ORDER BY {_ORDER} ROWS UNBOUNDED PRECEDING) AS _aas_revision, "
        f"bool_or(_aas_tie) OVER (PARTITION BY {key}) AS _aas_tied "
        f"FROM (SELECT *, {value} IS DISTINCT FROM lag({value}) OVER (PARTITION BY {key} "
        f"ORDER BY {_ORDER}) AS _aas_new, count(DISTINCT {value}) OVER (PARTITION BY {key}, "
        f"retrieved_at_utc) > 1 AS _aas_tie FROM {source})"
    )


def _revision_arg(label: str, args: Mapping[str, object]) -> None:
    exact_args(label, args, {"timezone", "revision"})
    zone_arg(label, args)
    revision = args["revision"]
    if type(revision) is not int or revision < 1:
        raise ValueError(f"{label} revision is a positive integer")


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
    expands: Final = False
    prices: Final = _ADJUSTED

    def check_args(self, args: Mapping[str, object]) -> None:
        _revision_arg(f"{self.name}@{self.major}", args)

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

    def outcome(self, args: Mapping[str, object]) -> None:
        del args

    def runs_sql(self, source: str) -> str:
        """Every source row with its revision number, whether it starts it, and its bar's ties."""
        return revision_runs(source, _KEY, (*self.prices, "volume"))

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


# FMP split types and the action type each becomes. Another type keeps its name with ``-``
# spelled ``_`` and an empty one is ``unspecified_split``.
SPLIT_TYPES: Final = {
    "stock-split": "split",
    "stock-dividend": "stock_dividend",
    "spin-off": "spin_off",
    "adr-change": "adr_change",
}


class _FmpActions:
    """FMP's frozen dividend and split responses as reference ``corporate_actions``.

    A response row names ``symbol``, its ex-date ``date`` and the instant
    ``retrieved_at_utc`` it was collected. The instrument resolves through (``fmp``,
    ``fmp_symbol``, ``symbol``) at the start of the ex-date in the spec's ``timezone``; the
    action's one time input is that ex-date (``exdate_open@1``). Responses repeat like the
    price responses: ``revision`` picks, for every (symbol, ex-date), the first response of
    the Nth run of equal values (``revision_runs``). FMP lists several different dividends
    on one ex-date in one response when a company pays more than one, so a key that ever
    had two different values at one instant is selected by no revision rather than refused:
    the promotion cannot tell which entry a later response corrects.
    """

    provider: Final = "fmp"
    domain: Final = "corporate_actions"
    source_prefixes: Final[tuple[str, ...]] = ()
    partition_sql: Final = '"date"'
    date_column: Final = "effective_date"
    time_inputs: Final[Mapping[str, InputKind]] = {"ex_date": "date"}
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    expands: Final = False
    name: str
    major: int
    values: tuple[str, ...]

    def check_args(self, args: Mapping[str, object]) -> None:
        _revision_arg(f"{self.name}@{self.major}", args)

    def outcome(self, args: Mapping[str, object]) -> None:
        del args

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return IdentityKey("fmp", "fmp_symbol")

    def _select(self, source: str, args: Mapping[str, object], kind: str, columns: str) -> str:
        zone = str(args["timezone"])
        revision = int(str(args["revision"]))
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "epoch_us(retrieved_at_utc) AS _aas_ingested_at_us, "
            f"symbol AS _aas_id_token, {zone_start_us(zone, 'date')} AS _aas_id_at_us, "
            f"{kind} || ':' || strftime(date, '%Y-%m-%d') AS action_id, {kind} AS action_type, "
            f"date AS ex_date, {columns}, date AS _aas_t_ex_date "
            f"FROM ({revision_runs(source, _KEY, self.values)}) "
            f"WHERE _aas_new AND _aas_revision = {revision} AND NOT _aas_tied"
        )


class FmpDividends(_FmpActions):
    """``fmp.dividends@1``: the cash ``dividend`` per share, as paid, in USD.

    ``recordDate`` and ``paymentDate`` are the record and pay dates. A dividend that is not
    finite and positive is ``invalid`` and keeps no amount.
    """

    name: Final = "fmp.dividends"
    major: Final = 1
    values: Final = ("dividend", "recordDate", "paymentDate")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "symbol": frozenset({"VARCHAR"}),
            "date": frozenset({"DATE"}),
            "dividend": frozenset({"DOUBLE"}),
            "recordDate": frozenset({"DATE"}),
            "paymentDate": frozenset({"DATE"}),
            "retrieved_at_utc": frozenset({"TIMESTAMP WITH TIME ZONE"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"amount": "DOUBLE"}

    def select(self, source: str, args: Mapping[str, object]) -> str:
        present = '(isfinite("dividend") AND "dividend" > 0)'
        return self._select(
            source,
            args,
            "'dividend'",
            '"recordDate" AS record_date, "paymentDate" AS pay_date, date AS effective_date, '
            f'CASE WHEN {present} THEN "dividend" END AS amount, '
            "CAST(NULL AS DECIMAL(38,12)) AS ratio, 'USD' AS currency, "
            f"CASE WHEN {present} THEN 'present' ELSE 'invalid' END AS value_state",
        )


class FmpSplits(_FmpActions):
    """``fmp.splits@1``: ``numerator / denominator`` new shares per old share.

    ``splitType`` names the action (``SPLIT_TYPES``). A ratio whose two terms are not
    finite and positive is ``invalid`` and keeps no ratio.
    """

    name: Final = "fmp.splits"
    major: Final = 1
    values: Final = ("numerator", "denominator", "splitType")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "symbol": frozenset({"VARCHAR"}),
            "date": frozenset({"DATE"}),
            "numerator": frozenset({"DOUBLE"}),
            "denominator": frozenset({"DOUBLE"}),
            "splitType": frozenset({"VARCHAR"}),
            "retrieved_at_utc": frozenset({"TIMESTAMP WITH TIME ZONE"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"ratio": "DOUBLE"}

    def select(self, source: str, args: Mapping[str, object]) -> str:
        present = " AND ".join(f'(isfinite("{n}") AND "{n}" > 0)' for n in self.values[:2])
        kind = (
            'CASE "splitType" '
            + " ".join(f"WHEN '{name}' THEN '{action}'" for name, action in SPLIT_TYPES.items())
            + " ELSE coalesce(replace(\"splitType\", '-', '_'), 'unspecified_split') END"
        )
        return self._select(
            source,
            args,
            kind,
            "CAST(NULL AS DATE) AS record_date, CAST(NULL AS DATE) AS pay_date, "
            "date AS effective_date, CAST(NULL AS DECIMAL(38,12)) AS amount, "
            f'CASE WHEN {present} THEN "numerator" / "denominator" END AS ratio, '
            "CAST(NULL AS VARCHAR) AS currency, "
            f"CASE WHEN {present} THEN 'present' ELSE 'invalid' END AS value_state",
        )
