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
identity key; the mapper then emits ``instrument_id`` itself (NULL for an issuer row).

A mapper may join generations of other datasets that its spec arguments pin
(``references``): SEC company facts read each filing's acceptance time from a pinned
``filings`` generation. The engine verifies each pin, loads the head rows of its chain
(no TOMBSTONE) with the referenced domain's columns into ``reference_table(name)``, and
the mapper's SELECT reads that table as it reads the source relation.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Protocol

from aegis_alpha.storage.market_inputs import GenerationPin
from aegis_alpha.storage.promotion.time_rules import InputKind

MANIFEST_ITEMS: Final = "_aas_p_items"
_PIN_KEYS: Final = frozenset(
    {"dataset_id", "version", "generation_id", "chain_hash", "manifest_hash"}
)


@dataclass(frozen=True, slots=True)
class IdentityKey:
    """The assertion key the engine resolves through the pinned identity snapshot."""

    provider: str
    namespace: str


@dataclass(frozen=True, slots=True)
class Reference:
    """A generation of another dataset that a mapper joins, pinned in its spec arguments."""

    domain: str
    pin: GenerationPin


def reference_pin(value: object, name: str) -> GenerationPin:
    """A mapper argument's generation pin: exactly the five pin fields, each exact text."""
    if not isinstance(value, dict) or set(value) != _PIN_KEYS:
        raise ValueError(
            f"{name} must pin dataset_id, version, generation_id, chain_hash and manifest_hash"
        )
    for key, item in value.items():
        if not isinstance(item, str) or not item or item != item.strip() or "\x00" in item:
            raise ValueError(f"{name} {key} must be exact nonempty text")
    return GenerationPin(**{key: str(item) for key, item in value.items()})


def reference_table(name: str) -> str:
    """The engine's temp table holding the head rows of the reference ``name``."""
    return f"_aas_p_ref_{name}"


def references(found: Mapper, args: Mapping[str, object]) -> Mapping[str, Reference]:
    """The pinned generations ``found`` joins under ``args``; most mappers join none."""
    method = getattr(found, "references", None)
    return {} if method is None else method(args)


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
        refuses such rows rather than dropping them.
        """
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
    from aegis_alpha.storage.promotion.mappers.eodhd import (  # noqa: PLC0415 -- registry
        EodhdBars,
        EodhdBarsAdjusted,
        EodhdBarsQuarantine,
        EodhdBulkQuarantine,
        EodhdBulkQuarantineAdjusted,
    )
    from aegis_alpha.storage.promotion.mappers.fred import FredAlfred  # noqa: PLC0415
    from aegis_alpha.storage.promotion.mappers.fx import (  # noqa: PLC0415
        fred_fx_series,
        norgate_fx_history,
    )
    from aegis_alpha.storage.promotion.mappers.korea import KoreaObservations  # noqa: PLC0415
    from aegis_alpha.storage.promotion.mappers.norgate import NorgateFxCloses  # noqa: PLC0415
    from aegis_alpha.storage.promotion.mappers.sec import (  # noqa: PLC0415 -- registry
        SecCompanyfacts,
        SecSubmissions,
    )

    mappers: tuple[Mapper, ...] = (
        CalendarDeclared(),
        EodhdBars(),
        EodhdBarsAdjusted(),
        EodhdBarsQuarantine(),
        EodhdBulkQuarantine(),
        EodhdBulkQuarantineAdjusted(),
        FredAlfred(),
        fred_fx_series(),
        NorgateFxCloses(),
        norgate_fx_history(),
        KoreaObservations("bok"),
        KoreaObservations("oecd"),
        KindIndustry(),
        NorgateClassification(),
        SecSic(),
        SecCompanyfacts(),
        SecSubmissions(),
    )
    return {f"{mapper.name}@{mapper.major}": mapper for mapper in mappers}


REGISTRY: Final = _registry()


def mapper(reference: str) -> Mapper:
    """The registered mapper for ``name@major``."""
    if reference not in REGISTRY:
        raise ValueError(f"unknown mapper {reference}")
    return REGISTRY[reference]
