"""EODHD daily bars as prices: three source shapes.

``eodhd.bars@1`` reads the source library's EODHD daily bar table: ``provider_symbol``,
``date``, unadjusted ``open``/``high``/``low``/``close``, ``volume`` (binary64),
``currency`` and the collection instant ``retrieved_at``. Other source columns stay in
the source row and its hash.

``eodhd.bulk_quarantine@1`` reads the rows an EODHD exchange-wide daily download held
back: one table per download with ``reason`` and the provider row as JSON text
(``code``, ``exchange_short_name``, ``date``, OHLCV, ``adjusted_close``). Only the reason
``provider_reported_partial`` is mapped: the provider answered with a warning that the
exchange's response was partial. Every such row carries the row flag
``provider_reported_partial``; any other reason has no session date and is refused as
missing a required column, so an unknown hold is never promoted silently. The row has no
collection instant, so ingestion is the source's ``sl:`` link time. The JSON names no
currency: the ``currencies`` argument maps each exchange code to its ISO currency, and an
exchange it does not list has none (refused). JSON numbers are read as binary64; an
integer beyond 2^53, which binary64 cannot hold exactly, or a value of any other JSON type
is invalid.

``eodhd.bars_adjusted@1`` and ``eodhd.bulk_quarantine_adjusted@1`` read the same shapes
and emit the provider's ``adjusted_close`` as a close-only (``fields='close'``) reference
price with basis ``total_return``.

``eodhd.bars_quarantine@1`` reads the rows a daily-bar history download held back as
invalid (``EodhdBarsQuarantine``); they become ``invalid`` bars that keep no values.

For all five:

- The instrument is the pinned identity snapshot's resolution of the assertion key
  (``eodhd``, ``eodhd_symbol``, ``<code>.<exchange>``) at the local start of the session
  date. A ticker never mints an instrument.
- ``bar_end_us`` is the last microsecond of the session date in the ``timezone``
  argument, an upper bound on when a daily bar can end that needs no calendar.
- A bar whose values are all finite and nonnegative is ``present``; one with none is
  ``missing``; any other bar, and every held history row, is ``invalid`` and keeps no
  values. Values are never filled from neighbours.
- ``session_date`` is the one time input, for record-basis rules.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Final
from zoneinfo import ZoneInfo

from aegis_alpha.storage.promotion.formats import quote_identifier, sql_literal
from aegis_alpha.storage.promotion.mappers import MANIFEST_ITEMS, IdentityKey
from aegis_alpha.storage.promotion.time_rules import InputKind

PARTIAL_FLAG: Final = "provider_reported_partial"
HELD_INVALID: Final = ("invalid_price_or_volume", "inconsistent_ohlc")
_VALUES: Final = ("open", "high", "low", "close", "volume")
_ZONE: Final = re.compile(r"[A-Za-z][A-Za-z0-9_+-]*(?:/[A-Za-z0-9_+-]+)*")
_EXCHANGE: Final = re.compile(r"[A-Z0-9]{1,10}")
_CURRENCY: Final = re.compile(r"[A-Z]{3}")
_DAY: Final = "[0-9]{4}-[0-9]{2}-[0-9]{2}"
_EXACT_INTEGER: Final = 2**53
_JSON: Final = "_aas_json"
_DECIMAL: Final = "DECIMAL(38,12)"


def _check_zone(name: str, args: Mapping[str, object]) -> None:
    zone = args["timezone"]
    if not isinstance(zone, str) or _ZONE.fullmatch(zone) is None:
        raise ValueError(f"{name} timezone must be an IANA zone name")
    ZoneInfo(zone)


def _check_currencies(name: str, args: Mapping[str, object]) -> None:
    currencies = args["currencies"]
    if not isinstance(currencies, dict) or not currencies:
        raise ValueError(f"{name} currencies must map exchange codes to ISO currencies")
    for exchange, currency in currencies.items():
        if _EXCHANGE.fullmatch(exchange) is None or not (
            isinstance(currency, str) and _CURRENCY.fullmatch(currency)
        ):
            raise ValueError(f"{name} currencies must map exchange codes to ISO currencies")


def _bounds(zone_arg: object) -> tuple[str, str]:
    """(local start of the session date as an instant, last microsecond of it)."""
    zone = sql_literal(str(zone_arg))
    start = f"epoch_us(timezone({zone}, CAST(session_date AS TIMESTAMP)))"
    end = f"epoch_us(timezone({zone}, CAST(session_date AS TIMESTAMP) + INTERVAL 1 DAY)) - 1"
    return start, end


def _state(values: tuple[str, ...], absent: Mapping[str, str]) -> tuple[str, str]:
    """(value_state SQL, the condition under which values are kept) over raw value columns."""
    present = " AND ".join(f"(isfinite({name}) AND {name} >= 0)" for name in values)
    missing = " AND ".join(absent[name] for name in values)
    state = f"CASE WHEN {present} THEN 'present' WHEN {missing} THEN 'missing' ELSE 'invalid' END"
    return state, present


def _prices(  # noqa: PLR0913 -- the price columns differ by series and shape
    *,
    zone: object,
    values: tuple[str, ...],
    absent: Mapping[str, str],
    adjusted: bool,
    extra: str,
    inner: str,
) -> str:
    """The domain SELECT over ``inner``, whose columns are the parsed shape's fields."""
    start, end = _bounds(zone)
    state, present = _state(values, absent)
    if adjusted:
        prices = (
            f"CAST(NULL AS {_DECIMAL}) AS open, CAST(NULL AS {_DECIMAL}) AS high, "
            f"CAST(NULL AS {_DECIMAL}) AS low, "
            f"CASE WHEN {present} THEN adjusted_close END AS close, "
            f"CAST(NULL AS {_DECIMAL}) AS volume, 'close' AS fields, "
            "'total_return' AS basis, 'reference' AS price_role"
        )
    else:
        prices = (
            ", ".join(f"CASE WHEN {present} THEN {name} END AS {name}" for name in values)
            + ", 'unadjusted' AS basis, 'canonical' AS price_role"
        )
    return (
        "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, _aas_ingested_at_us, "  # noqa: S608 -- engine-named relation
        f"_aas_token AS _aas_id_token, {start} AS _aas_id_at_us, "
        f"session_date, '1d' AS interval, {end} AS bar_end_us, currency, "
        f"{state} AS value_state, {prices}{extra}, "
        f"session_date AS _aas_t_session_date FROM ({inner})"
    )


class _Bars:
    """The source library's EODHD daily bar table."""

    major: Final = 1
    provider: Final = "eodhd"
    domain: Final = "prices"
    partition_sql: Final = '"date"'
    date_column: Final = "session_date"
    time_inputs: Final[Mapping[str, InputKind]] = {"session_date": "date"}
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    name: str
    _adjusted: bool

    def check_args(self, args: Mapping[str, object]) -> None:
        if set(args) != {"timezone"}:
            raise ValueError(f"{self.name}@1 takes exactly a timezone argument")
        _check_zone(f"{self.name}@1", args)

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        read = ("adjusted_close",) if self._adjusted else _VALUES
        return {
            "provider_symbol": frozenset({"VARCHAR"}),
            "date": frozenset({"DATE"}),
            **{name: frozenset({"DOUBLE"}) for name in read},
            "currency": frozenset({"VARCHAR"}),
            "retrieved_at": frozenset({"TIMESTAMP WITH TIME ZONE"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"close": "DOUBLE"} if self._adjusted else dict.fromkeys(_VALUES, "DOUBLE")

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return IdentityKey("eodhd", "eodhd_symbol")

    def select(self, source: str, args: Mapping[str, object]) -> str:
        values = ("adjusted_close",) if self._adjusted else _VALUES
        inner = (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "epoch_us(retrieved_at) AS _aas_ingested_at_us, provider_symbol AS _aas_token, "
            f"date AS session_date, currency, {', '.join(values)} FROM {source}"
        )
        return _prices(
            zone=args["timezone"],
            values=values,
            absent={name: f"{name} IS NULL" for name in values},
            adjusted=self._adjusted,
            extra="",
            inner=inner,
        )


class EodhdBars(_Bars):
    name: Final = "eodhd.bars"
    _adjusted = False


class EodhdBarsAdjusted(_Bars):
    name: Final = "eodhd.bars_adjusted"
    _adjusted = True


def _json_path(field: str) -> str:
    return sql_literal("$." + field)


def _json_number(field: str) -> str:
    """A JSON number as binary64, or NULL when it is another type or a too-wide integer."""
    path = _json_path(field)
    kind = f"json_type({_JSON}, {path})"
    text = f"json_extract_string({_JSON}, {path})"
    return (
        f"CASE WHEN {kind} IN ('UBIGINT', 'BIGINT') AND "
        f"abs(TRY_CAST({text} AS HUGEINT)) <= {_EXACT_INTEGER} THEN CAST({text} AS DOUBLE) "
        f"WHEN {kind} = 'DOUBLE' THEN CAST({text} AS DOUBLE) END"
    )


def _json_absent(field: str) -> str:
    return f"coalesce(json_type({_JSON}, {_json_path(field)}), 'NULL') = 'NULL'"


def _json_text(field: str) -> str:
    path = _json_path(field)
    return (
        f"CASE WHEN json_type({_JSON}, {path}) = 'VARCHAR' "
        f"THEN json_extract_string({_JSON}, {path}) END"
    )


def _bulk_day(json: str) -> str:
    """The session date of a mapped bulk row: a partial-response row with an ISO date."""
    text = f"json_extract_string({json}, '$.date')"
    return (
        f"CASE WHEN reason = {sql_literal(PARTIAL_FLAG)} AND json_valid(source_row_json) AND "
        f"json_type({json}, '$.date') = 'VARCHAR' AND regexp_full_match({text}, '{_DAY}') "
        f"THEN TRY_CAST({text} AS DATE) END"
    )


class _BulkQuarantine:
    """The held rows of one EODHD exchange-wide daily download."""

    major: Final = 1
    provider: Final = "eodhd"
    domain: Final = "prices"
    partition_sql: Final = _bulk_day(
        "CASE WHEN json_valid(source_row_json) THEN source_row_json END"
    )
    date_column: Final = "session_date"
    time_inputs: Final[Mapping[str, InputKind]] = {"session_date": "date"}
    row_flags: Final[Mapping[str, str]] = {PARTIAL_FLAG: "_aas_f_" + PARTIAL_FLAG}
    manifest_items: Final = None
    name: str
    _adjusted: bool

    def check_args(self, args: Mapping[str, object]) -> None:
        if set(args) != {"timezone", "currencies"}:
            raise ValueError(f"{self.name}@1 takes exactly timezone and currencies arguments")
        _check_zone(f"{self.name}@1", args)
        _check_currencies(f"{self.name}@1", args)

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {"reason": frozenset({"VARCHAR"}), "source_row_json": frozenset({"VARCHAR"})}

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"close": "DOUBLE"} if self._adjusted else dict.fromkeys(_VALUES, "DOUBLE")

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return IdentityKey("eodhd", "eodhd_symbol")

    def select(self, source: str, args: Mapping[str, object]) -> str:
        values = ("adjusted_close",) if self._adjusted else _VALUES
        currencies = args["currencies"]
        assert isinstance(currencies, dict)  # noqa: S101 -- check_args admitted the shape
        exchange = _json_text("exchange_short_name")
        currency = " ".join(
            f"WHEN {sql_literal(code)} THEN {sql_literal(str(name))}"
            for code, name in sorted(currencies.items())
        )
        columns = [
            "_aas_pin",
            "_aas_ordinal",
            "_aas_row_hash",
            "CAST(NULL AS BIGINT) AS _aas_ingested_at_us",
            f"{_json_text('code')} || '.' || {exchange} AS _aas_token",
            f"{_bulk_day(_JSON)} AS session_date",
            f"CASE {exchange} {currency} END AS currency",
            *(f"{_json_number(name)} AS {name}" for name in values),
            *(f"{_json_absent(name)} AS {quote_identifier('_aas_a_' + name)}" for name in values),
            f"reason = {sql_literal(PARTIAL_FLAG)} AS _aas_partial",
        ]
        parsed = (
            f"SELECT {', '.join(columns)} FROM (SELECT *, "  # noqa: S608 -- engine-named relation
            f"CASE WHEN json_valid(source_row_json) THEN source_row_json END AS {_JSON} "
            f"FROM {source})"
        )
        return _prices(
            zone=args["timezone"],
            values=values,
            absent={name: quote_identifier("_aas_a_" + name) for name in values},
            adjusted=self._adjusted,
            extra=f", _aas_partial AS {self.row_flags[PARTIAL_FLAG]}",
            inner=parsed,
        )


class EodhdBulkQuarantine(_BulkQuarantine):
    name: Final = "eodhd.bulk_quarantine"
    _adjusted = False


class EodhdBulkQuarantineAdjusted(_BulkQuarantine):
    name: Final = "eodhd.bulk_quarantine_adjusted"
    _adjusted = True


def _held_day(json: str) -> str:
    """The session date of a held history row whose reason says its values are invalid."""
    text = f"json_extract_string({json}, '$.date')"
    reasons = ", ".join(sql_literal(reason) for reason in HELD_INVALID)
    return (
        f"CASE WHEN reason IN ({reasons}) AND json_valid(source_row_json) AND "
        f"json_type({json}, '$.date') = 'VARCHAR' AND regexp_full_match({text}, '{_DAY}') "
        f"THEN TRY_CAST({text} AS DATE) END"
    )


_OFFSET_INSTANT: Final = (
    "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})"
)


class EodhdBarsQuarantine:
    """The rows of an EODHD daily-bar history download the collector held back as invalid.

    One table per download with ``source_fingerprint`` (the collection job), ``reason``
    and the provider row as JSON text (``date``, OHLCV, ``adjusted_close``; no symbol).
    The symbol and the job's completion instant come from the ``jobs`` list of the
    pinned source's commit manifest (``fingerprint``, ``symbol``, ``completed_at_utc``):
    a fingerprint that list names once with one symbol resolves, any other does not.
    Only the reasons ``invalid_price_or_volume`` and ``inconsistent_ohlc`` are mapped,
    as bars whose ``value_state`` is ``invalid`` and that keep no values; any other
    reason has no session date and is refused as missing a required column. Ingestion
    is the job's completion instant when it is an ISO instant with an offset, else the
    source's ``sl:`` link time. The currency is the ``currencies`` entry of the symbol's
    exchange suffix.
    """

    major: Final = 1
    provider: Final = "eodhd"
    domain: Final = "prices"
    name: Final = "eodhd.bars_quarantine"
    partition_sql: Final = _held_day(
        "CASE WHEN json_valid(source_row_json) THEN source_row_json END"
    )
    date_column: Final = "session_date"
    time_inputs: Final[Mapping[str, InputKind]] = {"session_date": "date"}
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = "jobs"

    def check_args(self, args: Mapping[str, object]) -> None:
        if set(args) != {"timezone", "currencies"}:
            raise ValueError(f"{self.name}@1 takes exactly timezone and currencies arguments")
        _check_zone(f"{self.name}@1", args)
        _check_currencies(f"{self.name}@1", args)

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "source_fingerprint": frozenset({"VARCHAR"}),
            "reason": frozenset({"VARCHAR"}),
            "source_row_json": frozenset({"VARCHAR"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return dict.fromkeys(_VALUES, "DOUBLE")

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return IdentityKey("eodhd", "eodhd_symbol")

    def select(self, source: str, args: Mapping[str, object]) -> str:
        currencies = args["currencies"]
        assert isinstance(currencies, dict)  # noqa: S101 -- check_args admitted the shape
        currency = " ".join(
            f"WHEN {sql_literal(code)} THEN {sql_literal(str(name))}"
            for code, name in sorted(currencies.items())
        )
        start, end = _bounds(args["timezone"])
        completed = "json_extract_string(item, '$.completed_at_utc')"
        jobs = (
            "SELECT _aas_pin, json_extract_string(item, '$.fingerprint') AS fingerprint, "  # noqa: S608 -- engine-named relation
            "CASE WHEN count(*) = 1 THEN min(json_extract_string(item, '$.symbol')) END "
            "AS symbol, CASE WHEN count(*) = 1 THEN min(CASE WHEN "
            f"regexp_full_match({completed}, '{_OFFSET_INSTANT}') "
            f"THEN epoch_us(TRY_CAST({completed} AS TIMESTAMPTZ)) END) END AS completed_us "
            f"FROM {MANIFEST_ITEMS} WHERE json_valid(item) "
            "AND json_type(item, '$.fingerprint') = 'VARCHAR' "
            "AND json_type(item, '$.symbol') = 'VARCHAR' GROUP BY ALL"
        )
        nulls = ", ".join(f"CAST(NULL AS DOUBLE) AS {name}" for name in _VALUES)
        parsed = (
            "SELECT s._aas_pin, s._aas_ordinal, s._aas_row_hash, "  # noqa: S608 -- engine-named relations
            "j.completed_us AS _aas_ingested_at_us, j.symbol AS _aas_token, "
            f"{_held_day(_JSON)} AS session_date, "
            f"CASE regexp_extract(j.symbol, '\\.([A-Z0-9]{{1,10}})$', 1) {currency} END "
            "AS currency "
            f"FROM (SELECT *, CASE WHEN json_valid(source_row_json) THEN source_row_json END "
            f"AS {_JSON} FROM {source}) s "
            f"LEFT JOIN ({jobs}) j ON j._aas_pin = s._aas_pin "
            "AND j.fingerprint = s.source_fingerprint"
        )
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, _aas_ingested_at_us, "  # noqa: S608 -- engine-named relation
            f"_aas_token AS _aas_id_token, {start} AS _aas_id_at_us, "
            f"session_date, '1d' AS interval, {end} AS bar_end_us, currency, "
            f"'invalid' AS value_state, {nulls}, "
            "'unadjusted' AS basis, 'canonical' AS price_role, "
            f"session_date AS _aas_t_session_date FROM ({parsed})"
        )
