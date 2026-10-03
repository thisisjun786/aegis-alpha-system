"""Mapper registry: pure SQL from one source shape to one market domain.

A mapper is named ``<provider>.<shape>`` with one major version; any change to its output
for the same input takes a new major. It reads only the source relation it is given and
its spec arguments: never the network, a clock, randomness or the environment. The
promotion engine owns everything common to all mappers (source row hashes, identity
resolution, decimal and time rules, record and revision identity, head diff, flags).
A mapper whose source shape more than one provider's sources share names the source ID
prefixes it reads, so a spec cannot promote one provider's rows into another's dataset.

``select`` returns one SELECT over the source relation with these columns:

- ``_aas_pin``, ``_aas_ordinal``, ``_aas_row_hash`` passed through unchanged;
- ``_aas_item`` (BIGINT) when the mapper ``expands``: one source row (a provider response
  document) holds several domain rows, numbered distinctly within that source row, and a
  source row may yield none;
- ``_aas_ingested_at_us`` (BIGINT, NULL when the source row has no collection time);
- ``_aas_id_token`` (VARCHAR) and ``_aas_id_at_us`` (BIGINT) when the mapper has an
  identity key: the key's token and the instant at which it is resolved;
- every domain column except the one the identity key resolves (``instrument_id``, or a
  classification's ``subject_id``) and ``fields`` when the mapper emits it, with each
  numeric column left as its raw source value for the spec's decimal rule;
- one ``_aas_t_<name>`` column per time input the mapper declares;
- one BOOLEAN column per row flag the mapper declares (``row_flags``).

A domain whose rows always name an instrument needs a mapper with an identity key. A
classification's subject may instead come from a source that carries the subject's
permanent anchor; that mapper mints the ID itself and has no identity key.

A mapper that declares ``manifest_items`` may also read ``MANIFEST_ITEMS``: one row
``(_aas_pin INTEGER, item VARCHAR)`` per element of that list in each pinned source's
commit manifest ``metadata``, the element as canonical JSON text. The engine recomputes
each source's request hash from the manifest's table and metadata and refuses the plan
when it differs from the marker and completed operation, so what the mapper reads from
the manifest is pinned like the rows.

A domain whose ``instrument_id`` is optional (fundamentals) may be mapped without an
identity key; such a mapper emits a NULL ``instrument_id``. Filings name no instrument.
A mapper whose source rows are responses rather than facts declares an ``outcome`` over
a staged source row (completed, no data, failed, ...), which the promotion records as
coverage in place of rows that a response without data cannot give. A partition stages
such a mapper's source rows whose partition date is NULL in every partition, so each is
counted, and refuses them for every other mapper.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Protocol

from aegis_alpha.storage.promotion.time_rules import InputKind

MANIFEST_ITEMS: Final = "_aas_p_items"


@dataclass(frozen=True, slots=True)
class IdentityKey:
    """The assertion key the engine resolves through the pinned identity snapshot."""

    provider: str
    namespace: str


class Mapper(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def major(self) -> int: ...

    @property
    def provider(self) -> str: ...

    @property
    def domain(self) -> str: ...

    @property
    def source_prefixes(self) -> tuple[str, ...]:
        """Source ID prefixes a pinned source must start with; empty accepts any source."""
        ...

    @property
    def partition_sql(self) -> str:
        """SQL over the source columns giving the DATE a spec partition tests.

        A row with no partition date never falls in a partition, and a partitioned plan
        refuses such rows rather than dropping them, except for a mapper that declares an
        ``outcome``: its undated rows are staged in every partition and counted there.
        """
        ...

    @property
    def expands(self) -> bool:
        """Whether one source row maps to any number of rows numbered by ``_aas_item``."""
        ...

    @property
    def date_column(self) -> str:
        """The domain column a tombstone scope's dates test: a DATE, or an instant's UTC day."""
        ...

    @property
    def time_inputs(self) -> Mapping[str, InputKind]: ...

    @property
    def row_flags(self) -> Mapping[str, str]:
        """Each quality flag the source row itself carries and the BOOLEAN column saying so.

        The engine attaches the flag, under the mapper's ``name@major`` as its rule, to the
        revision a flagged row produces.
        """
        ...

    @property
    def manifest_items(self) -> str | None:
        """The list in each pinned source's commit manifest ``metadata`` the mapper reads.

        None when the mapper reads only the source rows.
        """
        ...

    def check_args(self, args: Mapping[str, object]) -> None:
        """Refuse arguments the mapper does not define."""
        ...

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        """Each source column the mapper reads and the DuckDB types it accepts."""
        ...

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        """Each numeric domain column and the DuckDB type of the raw value it emits."""
        ...

    def identity(self, args: Mapping[str, object]) -> IdentityKey | None: ...

    def outcome(self, args: Mapping[str, object]) -> str | None:
        """SQL over a staged source row naming its outcome for coverage, or None."""
        ...

    def select(self, source: str, args: Mapping[str, object]) -> str: ...


# The domain column an identity key resolves into when it is not ``instrument_id``.
_RESOLVED: Final = {"classifications": "subject_id"}


def resolved_column(domain: str) -> str:
    """The column of ``domain`` that a mapper's identity key resolves through the snapshot."""
    return _RESOLVED.get(domain, "instrument_id")


def _registry() -> dict[str, Mapper]:
    from aegis_alpha.storage.promotion.mappers.calendar import CalendarDeclared  # noqa: PLC0415
    from aegis_alpha.storage.promotion.mappers.classifications import (  # noqa: PLC0415
        KindIndustry,
        NorgateClassification,
        SecSic,
    )
    from aegis_alpha.storage.promotion.mappers.dart import (  # noqa: PLC0415 -- registry
        DartFnltt,
        DartFnlttFilings,
    )
    from aegis_alpha.storage.promotion.mappers.eodhd import (  # noqa: PLC0415 -- registry
        EodhdBars,
        EodhdBarsAdjusted,
        EodhdBarsQuarantine,
        EodhdBulkQuarantine,
        EodhdBulkQuarantineAdjusted,
    )
    from aegis_alpha.storage.promotion.mappers.fmp import FmpEodNonSplit  # noqa: PLC0415
    from aegis_alpha.storage.promotion.mappers.fred import FredAlfred  # noqa: PLC0415
    from aegis_alpha.storage.promotion.mappers.fx import (  # noqa: PLC0415
        fred_fx_series,
        norgate_fx_history,
    )
    from aegis_alpha.storage.promotion.mappers.korea import KoreaObservations  # noqa: PLC0415
    from aegis_alpha.storage.promotion.mappers.norgate import NorgateFxCloses  # noqa: PLC0415
    from aegis_alpha.storage.promotion.mappers.norgate_prices import (  # noqa: PLC0415
        NorgatePricesAdjusted,
        NorgatePricesNone,
        NorgateReferenceCloses,
        NorgateReferenceHistory,
    )

    mappers: tuple[Mapper, ...] = (
        CalendarDeclared(),
        DartFnltt(),
        DartFnlttFilings(),
        EodhdBars(),
        EodhdBarsAdjusted(),
        EodhdBarsQuarantine(),
        EodhdBulkQuarantine(),
        EodhdBulkQuarantineAdjusted(),
        FmpEodNonSplit(),
        NorgatePricesAdjusted(),
        NorgatePricesNone(),
        NorgateReferenceCloses(),
        NorgateReferenceHistory(),
        FredAlfred(),
        fred_fx_series(),
        NorgateFxCloses(),
        norgate_fx_history(),
        KoreaObservations("bok"),
        KoreaObservations("oecd"),
        KindIndustry(),
        NorgateClassification(),
        SecSic(),
    )
    return {f"{mapper.name}@{mapper.major}": mapper for mapper in mappers}


REGISTRY: Final = _registry()


def mapper(reference: str) -> Mapper:
    """The registered mapper for ``name@major``."""
    if reference not in REGISTRY:
        raise ValueError(f"unknown mapper {reference}")
    return REGISTRY[reference]
