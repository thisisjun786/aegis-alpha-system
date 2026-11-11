"""``eodhd.delisted@1``: EODHD US delisted symbol lists as ``instrument_status`` delistings.

Input is the ``symbols`` table of committed ``qveris-eodhd-exchange-symbols-*`` lists asked for
``EXCHANGE_CODE=US`` (the columns ``kr_identity`` keeps: the request's ``exchange_code`` and
``delisted`` flag, the provider's ``Code``, ``Isin`` and ``Type`` as ``code``, ``isin``,
``type``, and the page's ``retrieved_at_utc``). EODHD's delisted list carries no delisting
date, so the date comes from one of two bases, named by ``reason``:

- ``eodhd_delisted_observed`` (``date_basis=observed``, ``status_event_id``
  ``eodhd:delisted:observed``): the ISIN was in the latest listed list of an earlier New York
  date than today's delisted list, not in the latest delisted list of an earlier date, and is
  in today's. The event starts at the New
  York start of the date today's list was retrieved. With no earlier delisted list pinned (the
  first one) nothing is observed: a symbol already on the first list may have been there for
  years.
- ``eodhd_delisted_last_bar`` (``date_basis=last_bar``, ``status_event_id``
  ``eodhd:delisted:last_bar``): the New York start of the day after the instrument's last bar
  in the pinned ``bars`` generation (``prices.us.eodhd``) on or before today's list. It is
  recorded beside an observed event too; an instrument without such a bar has none.

A last-bar event without an observed one in the same promotion carries the row flag
``delisting_inferred``. One whose delisting day (the day after the last bar) falls in
``[UNVERIFIED_FROM, first observation)`` also carries ``survivorship_unverified``: Norgate's
status ends before ``UNVERIFIED_FROM`` and the first pinned delisted list's New York date is
the first day a delisting could be observed, so only that interval has no observed source.

Today's list is the pinned delisted list retrieved last; the others (the latest earlier listed
and delisted lists and the first delisted list, which maintenance pins beside it) are read only
for the comparisons above and map no row. A row names an instrument only through its ISIN,
resolved as the registered FMP ``isin`` at its retrieval instant: a reused ticker with another
ISIN is another instrument, and a row without an ISIN, or with one the snapshot does not hold,
stays unresolved (data-vertical risk 7); nothing is derived from a code or a name. Such a row is
mapped once as a last-bar event dated by the observation day, only so the engine counts it
unresolved; an unresolved row is never stored. Rows of today's list repeating an ISIN map once
(the smallest source row hash). Both events are known at the end of the observation day at the
latest, which the engine lowers to the list's retrieval: a delisting is never known before the
list that shows it.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Final

from aegis_alpha.storage.promotion.formats import sql_literal
from aegis_alpha.storage.promotion.mappers import (
    IDENTITY_ROWS,
    IdentityKey,
    Reference,
    reference_pin,
    reference_table,
)
from aegis_alpha.storage.promotion.mappers.classifications import _observed_us
from aegis_alpha.storage.promotion.mappers.common import exact_args, zone_arg, zone_start_us
from aegis_alpha.storage.promotion.time_rules import InputKind

BARS: Final = "bars"
OBSERVED: Final = "eodhd:delisted:observed"
LAST_BAR: Final = "eodhd:delisted:last_bar"
UNVERIFIED_FROM: Final = date(2026, 7, 29)
"""The first delisting day no observed US source covers: Norgate's status ends the day before."""
_TEXT: Final = frozenset({"VARCHAR"})
_LABEL: Final = "eodhd.delisted@1"


class EodhdDelisted:
    name: Final = "eodhd.delisted"
    major: Final = 1
    provider: Final = "eodhd"
    domain: Final = "instrument_status"
    source_prefixes: Final[tuple[str, ...]] = ("qveris-eodhd-exchange-symbols-",)
    # Each list is read against the others pinned with it, so it has no partition date.
    partition_sql: Final = "CAST(NULL AS DATE)"
    date_column: Final = "effective_from_us"
    time_inputs: Final[Mapping[str, InputKind]] = {"status_date": "date"}
    row_flags: Final[Mapping[str, str]] = {
        "delisting_inferred": "_d_inferred",
        "survivorship_unverified": "_d_unverified",
    }
    manifest_items: Final = None
    expands: Final = True

    def check_args(self, args: Mapping[str, object]) -> None:
        exact_args(_LABEL, args, {"timezone", BARS})
        zone_arg(_LABEL, args)
        pin = reference_pin(args[BARS], f"{_LABEL} {BARS}")
        if not pin.dataset_id.startswith("prices.us."):
            raise ValueError(f"{_LABEL} {BARS} must pin a US prices dataset")

    def references(self, args: Mapping[str, object]) -> Mapping[str, Reference]:
        return {BARS: Reference("prices", reference_pin(args[BARS], f"{_LABEL} {BARS}"))}

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return dict.fromkeys(
            ("exchange_code", "delisted", "code", "isin", "type", "retrieved_at_utc"), _TEXT
        )

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {}

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return IdentityKey("fmp", "isin")

    def outcome(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        zone = str(args["timezone"])
        bars = reference_table(BARS)

        def day(instant: str) -> str:
            return (
                f"CAST(timezone({sql_literal(zone)}, timezone('UTC', make_timestamp({instant}))) "
                "AS DATE)"
            )

        lists = (
            f"s AS (SELECT *, {_observed_us('retrieved_at_utc')} AS _d_at, "  # noqa: S608 -- engine-named relations
            "nullif(isin, '') AS _d_isin "
            f"FROM {source} WHERE exchange_code = 'US' AND delisted IN ('0', '1')), "
            "l AS (SELECT _aas_pin, delisted, max(_d_at) AS list_at FROM s GROUP BY ALL), "
            "t AS (SELECT max(list_at) AS list_at, min(list_at) AS first_at FROM l "
            "WHERE delisted = '1'), "
            # Earlier lists are those of an earlier New York date: a run asks today's listed
            # list just before today's delisted one.
            "pl AS (SELECT arg_max(_aas_pin, l.list_at) AS pin FROM l, t "
            f"WHERE l.delisted = '0' AND {day('l.list_at')} < {day('t.list_at')}), "
            "pd AS (SELECT arg_max(_aas_pin, l.list_at) AS pin FROM l, t "
            f"WHERE l.delisted = '1' AND {day('l.list_at')} < {day('t.list_at')}), "
            "d AS (SELECT s.*, "
            f"{day('s._d_at')} AS _d_day, {day('t.first_at')} AS _d_first, "
            "(SELECT pin FROM pd) IS NOT NULL "
            "AND _d_isin IN (SELECT _d_isin FROM s WHERE _aas_pin = (SELECT pin FROM pl)) "
            "AND _d_isin NOT IN (SELECT _d_isin FROM s WHERE _aas_pin = (SELECT pin FROM pd) "
            "AND _d_isin IS NOT NULL) AS _d_observed "
            "FROM s, t WHERE s.delisted = '1' AND s._aas_pin IN "
            "(SELECT _aas_pin FROM l WHERE delisted = '1' AND list_at = t.list_at) "
            "QUALIFY row_number() OVER (PARTITION BY coalesce(_d_isin, "
            "'#' || CAST(s._aas_pin AS VARCHAR) || ':' || CAST(s._aas_ordinal AS VARCHAR)) "
            "ORDER BY s._aas_row_hash NULLS LAST, s._aas_pin, s._aas_ordinal) = 1), "
            # The instruments the engine will resolve each row to, and their last bar.
            "m AS (SELECT d._aas_pin, d._aas_ordinal, max(b.session_date) AS last_bar "
            f"FROM d JOIN {IDENTITY_ROWS} i ON i.token = d._d_isin "
            "AND i.valid_from_us <= d._d_at AND (i.valid_to_us IS NULL OR d._d_at < i.valid_to_us) "
            f"LEFT JOIN {bars} b ON b.instrument_id = i.instrument_id "
            "AND b.session_date <= d._d_day "
            "GROUP BY ALL), "
            "e AS (SELECT d.*, m._aas_pin IS NOT NULL AS _d_named, m.last_bar AS _d_last "
            "FROM d LEFT JOIN m USING (_aas_pin, _aas_ordinal))"
        )
        delisted_on = "(_d_last + 1)"
        unverified = (
            f"(NOT _d_observed AND {delisted_on} >= DATE '{UNVERIFIED_FROM.isoformat()}' "
            f"AND {delisted_on} < _d_first)"
        )

        def event(  # noqa: PLR0913, PLR0917 -- one event's item, identity, dates, flags and rows
            item: int,
            event_id: str,
            reason: str,
            start: str,
            inferred: str,
            flagged: str,
            where: str,
        ) -> str:
            return (
                f"SELECT _aas_pin, _aas_ordinal, CAST({item} AS BIGINT) AS _aas_item, "  # noqa: S608 -- the CTE named above
                "_aas_row_hash, _d_at AS _aas_ingested_at_us, _d_isin AS _aas_id_token, "
                f"_d_at AS _aas_id_at_us, {sql_literal(event_id)} AS status_event_id, "
                f"{zone_start_us(zone, start)} AS effective_from_us, "
                "CAST(NULL AS BIGINT) AS effective_to_us, 'delisted' AS status, "
                f"{sql_literal(reason)} AS reason, _d_day AS _aas_t_status_date, "
                f"{inferred} AS _d_inferred, {flagged} AS _d_unverified FROM e WHERE {where}"
            )

        return (
            f"WITH {lists} "
            + event(0, OBSERVED, "eodhd_delisted_observed", "_d_day", "false", "false",
                    "_d_named AND coalesce(_d_observed, false)")
            + " UNION ALL "
            + event(1, LAST_BAR, "eodhd_delisted_last_bar", delisted_on,
                    "NOT coalesce(_d_observed, false)", f"coalesce({unverified}, false)",
                    "_d_named AND _d_last IS NOT NULL")
            # A row naming no instrument is counted unresolved by the engine, never stored.
            + " UNION ALL "
            + event(1, LAST_BAR, "eodhd_delisted_last_bar", "_d_day", "false", "false",
                    "NOT _d_named")
        )  # fmt: skip
