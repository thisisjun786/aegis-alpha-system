"""EODHD exchange-day splits and dividends as ``corporate_actions``.

Both mappers read the tables ``aas collect qveris import`` commits for one exchange-day
download of EODHD's bulk ``splits`` or ``dividends`` (``qveris-splits-*`` ``splits``,
``qveris-dividends-*`` ``dividends``): ``provider_symbol`` (``<code>.<exchange>``), the
ex-date ``date``, the provider's values as text, and the collection instant
``retrieved_at``. The collector already held every row of a warned download, every row
without an explicit identity and every value that is not a positive decimal in the
``quarantine`` table, and it refuses a download that names one symbol twice, so a rows
table holds at most one action per symbol and type. These mappers read the rows tables only.

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
  paid: the provider's ``unadjusted_value`` when it states one, else its ``dividend`` (the
  value EODHD adjusts for later splits; on a download of the ex-date itself no later split
  exists yet), as text for ``decimal_text@1``. ``currency`` is ``dividend_currency`` when it
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


class _EodhdActions:
    provider: Final = "eodhd"
    domain: Final = "corporate_actions"
    major: Final = 1
    partition_sql: Final = '"date"'
    date_column: Final = "effective_date"
    time_inputs: Final[Mapping[str, InputKind]] = {"ex_date": "date"}
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    expands: Final = False
    name: str
    source_prefixes: tuple[str, ...]
    _values: tuple[str, ...]

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

    def _select(self, source: str, args: Mapping[str, object], kind: str, columns: str) -> str:
        zone = str(args["timezone"])
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "epoch_us(retrieved_at) AS _aas_ingested_at_us, "
            f"provider_symbol AS _aas_id_token, {zone_start_us(zone, _DAY)} AS _aas_id_at_us, "
            f"{sql_literal(kind + ':')} || strftime(\"date\", '%Y-%m-%d') AS action_id, "
            f'{sql_literal(kind)} AS action_type, "date" AS ex_date, {columns}, '
            f'"date" AS effective_date, "date" AS _aas_t_ex_date FROM {source}'
        )


class EodhdSplits(_EodhdActions):
    name: Final = "eodhd.splits"
    source_prefixes: Final[tuple[str, ...]] = ("qveris-splits-",)
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


class EodhdDividends(_EodhdActions):
    name: Final = "eodhd.dividends"
    source_prefixes: Final[tuple[str, ...]] = ("qveris-dividends-",)
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
        paid = "coalesce(unadjusted_value, dividend)"
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
