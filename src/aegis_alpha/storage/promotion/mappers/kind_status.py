"""``kind.status@1``: KIND listed-company lists as ``instrument_status`` listing events.

Input is the table ``aas collect kind`` and ``aas identity kr-import`` commit for one KIND
list (``kind-listings-*`` ``listings``): the KRX short code ``short_code``, the listing date
``listed_on`` (``YYYY-MM-DD``) and the collection instant ``retrieved_at_utc`` (an ISO ``...Z``
text), the columns ``kind.listings@1`` reads for the KR identity registry. A table of the
legacy ``korea.public_response@1`` import lacks them and is not this shape.

A list names the companies listed when it was collected. Each row is the event ``listed``
(``status_event_id`` ``kind:listed``, ``reason`` ``kind_listed_on``) starting at the Seoul start
of ``listed_on``, with no end: the list states no delisting. The instrument is the pinned
identity snapshot's resolution of (``kind``, ``krx_short_code``) at the collection instant,
the claim the KR registry makes from the same rows, so a short code reaches an instrument
only through the ISIN EODHD binds it to, never from the code or a name. Ingestion is the
collection instant and the partition date its Seoul date, as for ``kind.industry@1``. The one
time input ``status_date`` is ``listed_on`` (no earlier than 1970-01-01, see ``common``), so a
listing is known by the end of its listing day at the latest.

KIND repeats a company on rows that differ only in a column this mapper does not read (the
지역 region, for one), so source rows equal in (``short_code``, ``listed_on``,
``retrieved_at_utc``) map once: the one with the smallest source row hash, then the lowest
(``_aas_pin``, ``_aas_ordinal``). Two rows of one short code with different listing dates both
map, and the promotion refuses the repeated key rather than choosing a date. A ``listed_on``
that is no ``YYYY-MM-DD`` date leaves the event without a start, which refuses the row.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.promotion.mappers import IdentityKey
from aegis_alpha.storage.promotion.mappers.classifications import (
    KIND_ZONE,
    _kind_day,
    _observed_us,
)
from aegis_alpha.storage.promotion.mappers.common import epoch_floor, iso_day, zone_start_us
from aegis_alpha.storage.promotion.time_rules import InputKind

_TEXT: Final = frozenset({"VARCHAR"})


class KindStatus:
    name: Final = "kind.status"
    major: Final = 1
    provider: Final = "kind"
    domain: Final = "instrument_status"
    source_prefixes: Final[tuple[str, ...]] = ("kind-listings-",)
    partition_sql: Final = _kind_day(_observed_us("retrieved_at_utc"))
    date_column: Final = "effective_from_us"
    time_inputs: Final[Mapping[str, InputKind]] = {"status_date": "date"}
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    expands: Final = False

    def check_args(self, args: Mapping[str, object]) -> None:
        if args:
            raise ValueError("kind.status@1 takes no arguments")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {"short_code": _TEXT, "listed_on": _TEXT, "retrieved_at_utc": _TEXT}

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
        once = (
            "QUALIFY row_number() OVER (PARTITION BY short_code, listed_on, retrieved_at_utc "
            "ORDER BY _aas_row_hash NULLS LAST, _aas_pin, _aas_ordinal) = 1"
        )
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, _k_observed AS _aas_ingested_at_us, "  # noqa: S608 -- engine-named relation
            "short_code AS _aas_id_token, _k_observed AS _aas_id_at_us, "
            "'kind:listed' AS status_event_id, "
            f"{zone_start_us(KIND_ZONE, '_k_listed')} AS effective_from_us, "
            "CAST(NULL AS BIGINT) AS effective_to_us, 'listed' AS status, "
            "'kind_listed_on' AS reason, "
            f"{epoch_floor('_k_listed')} AS _aas_t_status_date FROM ("
            f"SELECT *, {_observed_us('retrieved_at_utc')} AS _k_observed, "
            f"{iso_day('listed_on')} AS _k_listed FROM {source} {once})"
        )
