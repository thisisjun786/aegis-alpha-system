"""EODHD exchange-day splits and dividends as ``corporate_actions``.

Both mappers read the tables ``aas collect qveris import`` commits for one exchange-day
download of EODHD's bulk ``splits`` or ``dividends`` (``qveris-splits-*`` ``splits``,
``qveris-dividends-*`` ``dividends``): ``provider_symbol`` (``<code>.<exchange>``), the
ex-date ``date``, the provider's values as text, and the collection instant
``retrieved_at``. The collector already held every row of a warned download, every row
without an explicit identity and every value that is not a positive decimal in the
``quarantine`` table, and it refuses a download that names one symbol twice, so a rows
table holds at most one action per symbol and type. These two mappers read the rows tables.

``eodhd.splits_quarantine@1`` and ``eodhd.dividends_quarantine@1`` read the held table of
one such download (``qveris-splits-quarantine-*``, ``qveris-dividends-quarantine-*``
``quarantine``): ``reason`` and the provider row as JSON text (``code``, ``exchange``,
``date``, ``split`` or ``dividend``, ``currency``, ``unadjustedValue``, ``recordDate``,
``paymentDate``). Only the reason ``provider_reported_partial`` is mapped: the provider
answered with a warning and the row passed the clean path's value checks. Its values are the
JSON texts the collector would have kept (a JSON number as its token text, an empty text as
none) and map as above; every such row carries the row flag ``provider_reported_partial``.
Any other reason has no ex-date and is refused as missing a required column. The row has no
collection instant, so ingestion is the source's ``sl:`` link time.

Their values have the shape of the Norgate action mappers (``norgate_actions``):

- The instrument is the pinned identity snapshot's resolution of (``eodhd``,
  ``eodhd_symbol``, ``provider_symbol``) at the start of the ex-date in the spec's
  ``timezone``, the key the EODHD daily bars resolve. A ticker never mints an instrument.
- ``ex_date`` and ``effective_date`` are the row's ``date``, the action's one time input
  (``exdate_open@1``); ingestion is ``retrieved_at``. ``action_id`` is
  ``<action_type>:<ex-date>``.
- ``eodhd.splits@1``: ``split`` is ``<new>/<old>`` shares; ``action_type`` is ``split`` and
  ``ratio`` the new shares per old share, ``new / old`` in binary64 for ``float_shortest@1``.
  A ratio text of another shape, or a term that is not positive, is ``invalid`` and keeps no
  ratio.
- ``eodhd.dividends@1``: ``action_type`` is ``dividend`` and ``amount`` the cash per share as
  paid: the provider's ``unadjusted_value``, as text for ``decimal_text@1``. ``dividend`` alone
  is not used: EODHD adjusts it for later splits, and a historical ex-day download may follow
  one, so without ``unadjusted_value`` the amount is unavailable (``invalid``). ``currency`` is
  ``dividend_currency`` when it
  is three uppercase letters; ``record_date`` and ``pay_date`` are ``record_date`` and
  ``payment_date`` when they are ``YYYY-MM-DD`` dates, else NULL. An amount that is not a
  positive decimal or a currency that is no ISO code is ``invalid`` and keeps no amount.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.promotion.formats import sql_literal
from aegis_alpha.storage.promotion.mappers import IdentityKey
from aegis_alpha.storage.promotion.mappers.common import (
    decimal_state,
    exact_args,
    iso_day,
    zone_arg,
    zone_start_us,
)
from aegis_alpha.storage.promotion.time_rules import InputKind

_TEXT: Final = frozenset({"VARCHAR"})
_TERM: Final = r"[0-9]+(\.[0-9]+)?"
_CURRENCY: Final = "regexp_full_match(coalesce(dividend_currency, ''), '[A-Z]{3}')"
_DAY: Final = '"date"'
_NO_VALUE: Final = "CAST(NULL AS DECIMAL(38,12))"
PARTIAL_FLAG: Final = "provider_reported_partial"
_FLAG_COLUMN: Final = "_aas_f_" + PARTIAL_FLAG
_JSON: Final = "_aas_json"
_RAW_JSON: Final = "CASE WHEN json_valid(source_row_json) THEN source_row_json END"


def _held_text(field: str) -> str:
    """A held row's provider value as the collector's text: a JSON text or number, else NULL."""
    path = sql_literal("$." + field)
    kind = f"json_type({_JSON}, {path})"
    return (
        f"CASE WHEN {kind} IN ('VARCHAR', 'UBIGINT', 'BIGINT', 'DOUBLE') "
        f"THEN nullif(json_extract_string({_JSON}, {path}), '') END"
    )


def _held_day(json: str) -> str:
    """The ex-date of a mapped held row: a partial-response row with an ISO date."""
    text = f"json_extract_string({json}, '$.date')"
    return (
        f"CASE WHEN reason = {sql_literal(PARTIAL_FLAG)} AND json_valid(source_row_json) AND "
        f"json_type({json}, '$.date') = 'VARCHAR' AND "
        f"regexp_full_match({text}, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}') "
        f"THEN TRY_CAST({text} AS DATE) END"
    )


class _EodhdActions:
    provider: Final = "eodhd"
    domain: Final = "corporate_actions"
    major: Final = 1
    date_column: Final = "effective_date"
    time_inputs: Final[Mapping[str, InputKind]] = {"ex_date": "date"}
    manifest_items: Final = None
    expands: Final = False
    name: str
    source_prefixes: tuple[str, ...]
    partition_sql: str
    row_flags: Mapping[str, str]
    _values: tuple[str, ...]
    _flag: str = ""
    """The row flag column a held-row mapper adds to its select."""

    def check_args(self, args: Mapping[str, object]) -> None:
        label = f"{self.name}@{self.major}"
        exact_args(label, args, {"timezone"})
        zone_arg(label, args)

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "provider_symbol": _TEXT,
            "date": frozenset({"DATE"}),
            **dict.fromkeys(self._values, _TEXT),
            "retrieved_at": frozenset({"TIMESTAMP WITH TIME ZONE"}),
        }

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return IdentityKey("eodhd", "eodhd_symbol")

    def outcome(self, args: Mapping[str, object]) -> None:
        del args

    def _relation(self, source: str) -> str:
        """The rows-table columns the select reads, from ``source``."""
        return source

    def _select(self, source: str, args: Mapping[str, object], kind: str, columns: str) -> str:
        zone = str(args["timezone"])
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "epoch_us(retrieved_at) AS _aas_ingested_at_us, "
            f"provider_symbol AS _aas_id_token, {zone_start_us(zone, _DAY)} AS _aas_id_at_us, "
            f"{sql_literal(kind + ':')} || strftime(\"date\", '%Y-%m-%d') AS action_id, "
            f'{sql_literal(kind)} AS action_type, "date" AS ex_date, {columns}, '
            f'"date" AS effective_date, "date" AS _aas_t_ex_date{self._flag} '
            f"FROM {self._relation(source)}"
        )


class _Held:
    """The held rows of one exchange-day download, read as the rows table's columns."""

    source_prefixes: Final[tuple[str, ...]] = ()
    partition_sql: Final = _held_day(_RAW_JSON)
    row_flags: Final[Mapping[str, str]] = {PARTIAL_FLAG: _FLAG_COLUMN}
    _flag: Final = f", _aas_partial AS {_FLAG_COLUMN}"
    _fields: Mapping[str, str]
    """Each rows-table column the select reads and the provider field it comes from."""

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {"reason": _TEXT, "source_row_json": _TEXT}

    def _relation(self, source: str) -> str:
        values = ", ".join(f"{_held_text(field)} AS {column}"
                           for column, field in self._fields.items())  # fmt: skip
        return (
            "(SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS retrieved_at, "
            f"{_held_text('code')} || '.' || {_held_text('exchange')} AS provider_symbol, "
            f'{_held_day(_JSON)} AS "date", {values}, '
            f"reason = {sql_literal(PARTIAL_FLAG)} AS _aas_partial "
            f"FROM (SELECT *, {_RAW_JSON} AS {_JSON} FROM {source}))"
        )


class _Splits(_EodhdActions):
    _values: Final = ("split",)

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"ratio": "DOUBLE"}

    def select(self, source: str, args: Mapping[str, object]) -> str:
        new = "TRY_CAST(split_part(split, '/', 1) AS DOUBLE)"
        old = "TRY_CAST(split_part(split, '/', 2) AS DOUBLE)"
        present = (
            f"coalesce(regexp_full_match(split, '{_TERM}/{_TERM}'), false) "
            f"AND {new} > 0 AND {old} > 0"
        )
        return self._select(
            source,
            args,
            "split",
            "CAST(NULL AS DATE) AS record_date, CAST(NULL AS DATE) AS pay_date, "
            f"{_NO_VALUE} AS amount, CASE WHEN {present} THEN {new} / {old} END "
            "AS ratio, CAST(NULL AS VARCHAR) AS currency, "
            f"CASE WHEN {present} THEN 'present' ELSE 'invalid' END AS value_state",
        )


class _Dividends(_EodhdActions):
    _values: Final = (
        "dividend",
        "dividend_currency",
        "unadjusted_value",
        "record_date",
        "payment_date",
    )

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"amount": "VARCHAR"}

    def select(self, source: str, args: Mapping[str, object]) -> str:
        paid = "unadjusted_value"
        present = f"({decimal_state(paid, 'positive')}) = 'present' AND {_CURRENCY}"
        return self._select(
            source,
            args,
            "dividend",
            f"{iso_day('record_date')} AS record_date, {iso_day('payment_date')} AS pay_date, "
            f"CASE WHEN {present} THEN {paid} END AS amount, "
            f"{_NO_VALUE} AS ratio, "
            f"CASE WHEN {_CURRENCY} THEN dividend_currency END AS currency, "
            f"CASE WHEN {present} THEN 'present' ELSE 'invalid' END AS value_state",
        )


class EodhdSplits(_Splits):
    name: Final = "eodhd.splits"
    source_prefixes: Final[tuple[str, ...]] = ("qveris-splits-",)
    partition_sql: Final = _DAY
    row_flags: Final[Mapping[str, str]] = {}


class EodhdDividends(_Dividends):
    name: Final = "eodhd.dividends"
    source_prefixes: Final[tuple[str, ...]] = ("qveris-dividends-",)
    partition_sql: Final = _DAY
    row_flags: Final[Mapping[str, str]] = {}


class EodhdSplitsQuarantine(_Held, _Splits):
    name: Final = "eodhd.splits_quarantine"
    _fields: Final[Mapping[str, str]] = {"split": "split"}


class EodhdDividendsQuarantine(_Held, _Dividends):
    name: Final = "eodhd.dividends_quarantine"
    _fields: Final[Mapping[str, str]] = {
        "dividend": "dividend",
        "dividend_currency": "currency",
        "unadjusted_value": "unadjustedValue",
        "record_date": "recordDate",
        "payment_date": "paymentDate",
    }
