"""``kind.delisted@1``: KIND listed-company lists as ``instrument_status`` delistings.

Input is the table ``aas collect kind`` and ``aas identity kr-import`` commit for one KIND
list (``kind-listings-*`` ``listings``): the list ``list_id`` (``kind-kospi`` or
``kind-kosdaq``), the KRX short code ``short_code`` and the collection instant
``retrieved_at_utc`` (an ISO ``...Z`` text). KIND states no delisting and no delisting date, so
a delisting is a short code that leaves both lists:

- ``kind_delisted_observed`` (``date_basis=observed``, ``status_event_id``
  ``kind:delisted:observed``): the code is on the latest list of either market of an earlier
  Seoul date than today's and on neither of today's two lists. The event starts at the Seoul
  start of today's date. A code that moves between the markets stays on one of today's lists
  and is no event. With only one of today's lists pinned nothing is observed: the other may
  still name the code, and the second list's promotion observes it.
- ``kind_delisted_last_bar`` (``date_basis=last_bar``, ``status_event_id``
  ``kind:delisted:last_bar``): the Seoul start of the day after the instrument's last bar in
  the pinned ``bars`` chain (``prices.kr.eodhd``) on or before today, read as of today's latest
  retrieval: each record's latest revision known by then. An instrument without such a bar has
  none.

Both events of a code carry the row flag ``survivorship_unverified`` when the earlier list it
was last on is more than one day before today: the code left at some day of that gap, which no
list observed. A last-bar event whose delisting day falls before the first pinned list (the
earliest KIND list, which maintenance pins beside today's) carries it too: no KIND list
existed to observe it.

Today's lists are the pinned lists of the latest Seoul date (the latest retrieval of each
market); the latest earlier list of each market and the first list are read only for the
comparisons above and map no row. A code names an instrument only through the pinned identity
snapshot's (``kind``, ``krx_short_code``) at today's latest retrieval, the claim the KR
registry makes through the ISIN EODHD binds the code to; nothing is derived from an inferred
ISIN, a code or a name. A code the snapshot does not resolve is mapped once as an observed
event, only so the engine counts it unresolved; an unresolved row is never stored. Rows of the
earlier lists repeating a code map once (the smallest source row hash). Both events are known
at the end of today's Seoul date at the latest, which the engine lowers to the list's
retrieval: a delisting is never known before the lists that show it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.promotion.mappers import (
    IDENTITY_ROWS,
    IdentityKey,
    Reference,
    reference_pin,
    reference_table,
)
from aegis_alpha.storage.promotion.mappers.classifications import (
    KIND_ZONE,
    _kind_day,
    _observed_us,
)
from aegis_alpha.storage.promotion.mappers.common import exact_args, zone_start_us
from aegis_alpha.storage.promotion.time_rules import InputKind

BARS: Final = "bars"
OBSERVED: Final = "kind:delisted:observed"
LAST_BAR: Final = "kind:delisted:last_bar"
LISTS: Final = ("kind-kospi", "kind-kosdaq")
_TEXT: Final = frozenset({"VARCHAR"})
_LABEL: Final = "kind.delisted@1"


class KindDelisted:
    name: Final = "kind.delisted"
    major: Final = 1
    provider: Final = "kind"
    domain: Final = "instrument_status"
    source_prefixes: Final[tuple[str, ...]] = ("kind-listings-",)
    # Each list is read against the others pinned with it, so it has no partition date.
    partition_sql: Final = "CAST(NULL AS DATE)"
    date_column: Final = "effective_from_us"
    time_inputs: Final[Mapping[str, InputKind]] = {"status_date": "date"}
    row_flags: Final[Mapping[str, str]] = {"survivorship_unverified": "_d_unverified"}
    manifest_items: Final = None
    expands: Final = True

    def check_args(self, args: Mapping[str, object]) -> None:
        exact_args(_LABEL, args, {BARS})
        pin = reference_pin(args[BARS], f"{_LABEL} {BARS}")
        if not pin.dataset_id.startswith("prices.kr."):
            raise ValueError(f"{_LABEL} {BARS} must pin a KR prices dataset")

    def references(self, args: Mapping[str, object]) -> Mapping[str, Reference]:
        return {
            BARS: Reference("prices", reference_pin(args[BARS], f"{_LABEL} {BARS}"), revisions=True)
        }

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return dict.fromkeys(("list_id", "short_code", "retrieved_at_utc"), _TEXT)

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {}

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return IdentityKey("kind", "krx_short_code")

    def outcome(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        del args
        bars = reference_table(BARS)
        markets = ", ".join(f"'{name}'" for name in LISTS)
        lists = (
            f"s AS (SELECT *, {_observed_us('retrieved_at_utc')} AS _d_at FROM {source} "  # noqa: S608 -- engine-named relations
            f"WHERE list_id IN ({markets}) AND short_code IS NOT NULL), "
            f"l AS (SELECT _aas_pin, list_id, max(_d_at) AS list_at, {_kind_day('max(_d_at)')} "
            "AS list_day FROM s GROUP BY ALL), "
            "t AS (SELECT max(list_day) AS today, min(list_day) AS first_day FROM l), "
            # Today's list of each market, and the latest earlier one of each.
            "tl AS (SELECT list_id, arg_max(_aas_pin, list_at) AS pin, max(list_at) AS list_at "
            "FROM l, t WHERE list_day = t.today GROUP BY list_id), "
            "pl AS (SELECT list_id, arg_max(_aas_pin, list_at) AS pin, max(list_day) AS day "
            "FROM l, t WHERE list_day < t.today GROUP BY list_id), "
            "w AS (SELECT max(list_at) AS seen_at, count(*) AS lists FROM tl), "
            "d AS (SELECT s.*, pl.day AS _d_seen FROM s JOIN pl ON s._aas_pin = pl.pin "
            f"WHERE (SELECT lists FROM w) = {len(LISTS)} "
            "AND s.short_code NOT IN (SELECT short_code FROM s "
            "WHERE _aas_pin IN (SELECT pin FROM tl)) "
            "QUALIFY row_number() OVER (PARTITION BY s.short_code "
            "ORDER BY s._aas_row_hash NULLS LAST, s._aas_pin, s._aas_ordinal) = 1), "
            # The price chain as a strict reader saw it at today's latest retrieval.
            "bv AS (SELECT instrument_id, session_date FROM (SELECT *, row_number() OVER "
            "(PARTITION BY record_id ORDER BY _aas_sequence DESC, "
            f"revision_known_at_us DESC NULLS LAST) AS _rank FROM {bars} "
            "WHERE revision_known_at_us <= (SELECT seen_at FROM w)) "
            "WHERE _rank = 1 AND op <> 'TOMBSTONE' "
            "AND available_at_us <= (SELECT seen_at FROM w)), "
            # The instruments the engine will resolve each code to, and their last bar.
            "m AS (SELECT d._aas_pin, d._aas_ordinal, max(b.session_date) AS last_bar "
            f"FROM d JOIN {IDENTITY_ROWS} i ON i.token = d.short_code "
            "AND i.valid_from_us <= (SELECT seen_at FROM w) "
            "AND (i.valid_to_us IS NULL OR (SELECT seen_at FROM w) < i.valid_to_us) "
            "LEFT JOIN bv b ON b.instrument_id = i.instrument_id "
            "AND b.session_date <= (SELECT today FROM t) GROUP BY ALL), "
            "e AS (SELECT d.*, m.last_bar AS _d_last, t.today AS _d_today, "
            "t.first_day AS _d_first, (SELECT seen_at FROM w) AS _d_now, "
            "coalesce(t.today - d._d_seen > 1, false) AS _d_gap "
            "FROM d CROSS JOIN t LEFT JOIN m USING (_aas_pin, _aas_ordinal))"
        )

        def event(  # noqa: PLR0913, PLR0917 -- one event's item, identity, date, flag and rows
            item: int, event_id: str, reason: str, start: str, flagged: str, where: str
        ) -> str:
            return (
                f"SELECT _aas_pin, _aas_ordinal, CAST({item} AS BIGINT) AS _aas_item, "  # noqa: S608 -- the CTE named above
                "_aas_row_hash, _d_now AS _aas_ingested_at_us, short_code AS _aas_id_token, "
                f"_d_now AS _aas_id_at_us, '{event_id}' AS status_event_id, "
                f"{zone_start_us(KIND_ZONE, start)} AS effective_from_us, "
                "CAST(NULL AS BIGINT) AS effective_to_us, 'delisted' AS status, "
                f"'{reason}' AS reason, _d_today AS _aas_t_status_date, "
                f"{flagged} AS _d_unverified FROM e WHERE {where}"
            )

        return (
            f"WITH {lists} "
            # A code naming no instrument is counted unresolved by the engine, never stored.
            + event(0, OBSERVED, "kind_delisted_observed", "_d_today", "_d_gap", "true")
            + " UNION ALL "
            + event(1, LAST_BAR, "kind_delisted_last_bar", "(_d_last + 1)",
                    "(_d_gap OR _d_last + 1 < _d_first)", "_d_last IS NOT NULL")
        )  # fmt: skip
