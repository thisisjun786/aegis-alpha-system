"""The dataset catalog: every market dataset the data vertical promotes, and what it is.

Each entry names one ``<domain>.<market>.<provider>[.ref]`` dataset, the market domain
it fills, its role, the registered mappers that build it, the time, numeric and quality
rules its promotion specs use, and whether its provider is frozen. The flags a dataset's
revisions can carry follow from those rules and the mappers' row flags; they are derived,
never declared. ``dev-notes/design/data-vertical.md`` holds the same table, and
``tests/tools/test_dataset_catalog.py`` keeps the two, the mapper registry and
``aas data datasets`` in agreement.

The catalog describes datasets; it does not admit them. A generation published under a
name outside it stays readable and is listed as ``uncataloged``. A dataset of a later
time-rule generation (``.r<N>``, N from 2) belongs to the entry of its first generation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal

from aegis_alpha.storage.promotion.mappers import REGISTRY
from aegis_alpha.storage.promotion.time_rules import RULES as TIME_RULES

if TYPE_CHECKING:
    import sqlite3

type Role = Literal["canonical", "reference"]

# Flags each numeric rule may attach to a revision whose value it changed.
DECIMAL_FLAGS: Final = {
    "exact@1": (),
    "krw_tick@1": ("provider_float_reconstructed", "decimal_rounding_tie"),
    "float_shortest@1": ("provider_float_storage", "decimal_rounding_tie"),
    "decimal_text@1": ("volume_precision_limited",),
}
# Flags each quality rule may attach.
QUALITY_FLAGS: Final = {"cross_provider_mismatch@1": ("cross_provider_mismatch",)}
# Every rule but unknown_null@1 may lower a time to the ingestion time it exceeds. That
# flag is common to all datasets, so the catalog does not repeat it per entry.
CLAMP_FLAG: Final = "time_clamped_to_ingestion"
_GENERATION: Final = re.compile(r"(?P<base>.+)\.r(?:[2-9]|[1-9][0-9]+)")


@dataclass(frozen=True, slots=True)
class CatalogEntry:
    dataset_id: str
    domain: str
    role: Role
    mappers: tuple[str, ...]
    time_rules: tuple[str, ...]
    decimal_rules: tuple[str, ...] = ()
    quality_rules: tuple[str, ...] = ()
    frozen: bool = False

    @property
    def flags(self) -> tuple[str, ...]:
        """Every flag a revision of this dataset can carry, in a stable order."""
        found: list[str] = []
        for name in self.mappers:
            found.extend(REGISTRY[name].row_flags)
        for name in self.decimal_rules:
            found.extend(DECIMAL_FLAGS[name])
        for name in self.time_rules:
            flag = TIME_RULES[name].flag
            if flag is not None:
                found.append(flag)
        for name in self.quality_rules:
            found.extend(QUALITY_FLAGS[name])
        return tuple(sorted(set(found)))

    def describe(self) -> dict[str, object]:
        return {
            "dataset_id": self.dataset_id,
            "domain": self.domain,
            "role": self.role,
            "mappers": list(self.mappers),
            "time_rules": list(self.time_rules),
            "decimal_rules": list(self.decimal_rules),
            "quality_rules": list(self.quality_rules),
            "flags": list(self.flags),
            "frozen": self.frozen,
        }


_DAILY: Final = ("session_close_plus_lag@1",)
_DAY_END: Final = ("local_day_end@1",)
_TEXT: Final = ("decimal_text@1",)
_SHORTEST: Final = ("float_shortest@1",)

CATALOG: Final[tuple[CatalogEntry, ...]] = (
    CatalogEntry(
        "prices.kr.eodhd",
        "prices",
        "canonical",
        ("eodhd.bars@1", "eodhd.bars_quarantine@1", "eodhd.bulk_quarantine@1"),
        _DAILY,
        ("krw_tick@1", "float_shortest@1"),
    ),
    CatalogEntry(
        "prices.kr.eodhd.ref",
        "prices",
        "reference",
        ("eodhd.bars_adjusted@1", "eodhd.bulk_quarantine_adjusted@1"),
        _DAILY,
        _SHORTEST,
    ),
    CatalogEntry(
        "prices.us.norgate", "prices", "canonical", ("norgate.prices_none@1",), _DAILY, _TEXT
    ),
    CatalogEntry(
        "prices.us.norgate.ref",
        "prices",
        "reference",
        ("norgate.prices_adjusted@1",),
        _DAILY,
        _SHORTEST,
    ),
    CatalogEntry(
        "prices.us.eodhd",
        "prices",
        "canonical",
        ("eodhd.bars@1",),
        _DAILY,
        _SHORTEST,
        ("cross_provider_mismatch@1",),
    ),
    CatalogEntry(
        "prices.us.fmp.ref",
        "prices",
        "reference",
        ("fmp.eod_non_split@1",),
        _DAILY,
        ("float_shortest@1", "exact@1"),
        frozen=True,
    ),
    CatalogEntry(
        "prices.ref.norgate",
        "prices",
        "reference",
        ("norgate.reference_closes@1", "norgate.reference_history@1"),
        _DAY_END,
        _TEXT,
        frozen=True,
    ),
    CatalogEntry(
        "sessions.xnys",
        "calendar_sessions",
        "canonical",
        ("calendar.declared@1",),
        ("declared_session_end@1",),
    ),
    CatalogEntry(
        "sessions.xkrx",
        "calendar_sessions",
        "canonical",
        ("calendar.declared@1",),
        ("declared_session_end@1",),
    ),
    CatalogEntry(
        "actions.us.norgate",
        "corporate_actions",
        "canonical",
        ("norgate.dividends@1", "norgate.capital_adjustments@1"),
        ("exdate_open@1",),
        _SHORTEST,
        frozen=True,
    ),
    CatalogEntry(
        "actions.us.fmp.ref",
        "corporate_actions",
        "reference",
        ("fmp.dividends@1", "fmp.splits@1"),
        ("exdate_open@1",),
        _SHORTEST,
        frozen=True,
    ),
    CatalogEntry("actions.us.eodhd", "corporate_actions", "canonical", (), ("exdate_open@1",)),
    CatalogEntry("actions.kr.eodhd", "corporate_actions", "canonical", (), ("exdate_open@1",)),
    CatalogEntry(
        "status.us.norgate", "instrument_status", "canonical", ("norgate.status@1",), _DAY_END
    ),
    CatalogEntry("status.kr.kind", "instrument_status", "canonical", (), ()),
    CatalogEntry(
        "filings.us.sec", "filings", "canonical", ("sec.submissions@1",), ("source_column@1",)
    ),
    CatalogEntry("filings.kr.dart", "filings", "canonical", ("dart.fnltt_filings@1",), _DAY_END),
    CatalogEntry(
        "fundamentals.us.sec",
        "fundamentals",
        "canonical",
        ("sec.companyfacts@1",),
        ("source_column@1",),
        _TEXT,
    ),
    CatalogEntry(
        "fundamentals.kr.dart", "fundamentals", "canonical", ("dart.fnltt@1",), _DAY_END, _TEXT
    ),
    CatalogEntry(
        "macro.us.alfred", "macro_observations", "canonical", ("fred.alfred@1",), _DAY_END, _TEXT
    ),
    CatalogEntry(
        "macro.kr.bok",
        "macro_observations",
        "canonical",
        ("bok.observations@1",),
        ("unknown_null@1",),
        _TEXT,
    ),
    CatalogEntry(
        "macro.kr.oecd",
        "macro_observations",
        "canonical",
        ("oecd.observations@1",),
        ("unknown_null@1",),
        _TEXT,
    ),
    CatalogEntry(
        "fx.usdkrw.norgate",
        "fx_rates",
        "canonical",
        ("norgate.fx_closes@1", "norgate.fx_history@1"),
        _DAY_END,
        _TEXT,
        frozen=True,
    ),
    CatalogEntry(
        "fx.usdkrw.fred", "fx_rates", "canonical", ("fred.fx_series@1",), ("unknown_null@1",), _TEXT
    ),
    CatalogEntry(
        "classifications.us.norgate",
        "classifications",
        "canonical",
        ("norgate.classification@1",),
        _DAY_END,
    ),
    CatalogEntry(
        "classifications.us.sec", "classifications", "canonical", ("sec.sic@1",), _DAY_END
    ),
    CatalogEntry(
        "classifications.kr.kind",
        "classifications",
        "canonical",
        ("kind.industry@1",),
        ("source_column@1",),
    ),
)
_BY_ID: Final = {entry.dataset_id: entry for entry in CATALOG}


def catalog_entry(dataset_id: str) -> CatalogEntry | None:
    """The entry ``dataset_id`` belongs to, its ``.r<N>`` time-rule generation included."""
    found = _BY_ID.get(dataset_id)
    if found is not None:
        return found
    match = _GENERATION.fullmatch(dataset_id)
    return None if match is None else _BY_ID.get(match["base"])


def list_datasets(state: sqlite3.Connection) -> dict[str, object]:
    """``aas data datasets``: committed versions, the catalog with its heads, and the rest.

    ``datasets`` lists every committed version in sequence order. Each catalog entry lists
    under ``published`` the datasets of its own (and its ``.r<N>`` generations) that hold a
    committed version, with their version count and head. ``uncataloged`` names committed
    datasets no entry describes.
    """
    rows = [
        dict(row)
        for row in state.execute(
            "SELECT dataset_id, version, generation_id, chain_hash, row_count FROM "
            "dataset_versions WHERE status='committed' ORDER BY dataset_id, sequence"
        ).fetchall()
    ]
    published: dict[str, dict[str, object]] = {}
    for row in rows:
        dataset_id = str(row["dataset_id"])
        held = published.setdefault(dataset_id, {"dataset_id": dataset_id, "versions": 0})
        held["versions"] = int(str(held["versions"])) + 1
        held["head"] = {
            key: row[key] for key in ("version", "generation_id", "chain_hash", "row_count")
        }
    catalog: list[dict[str, object]] = []
    for entry in CATALOG:
        own = [held for dataset_id, held in published.items() if catalog_entry(dataset_id) is entry]
        catalog.append({**entry.describe(), "published": own})
    uncataloged = [dataset_id for dataset_id in published if catalog_entry(dataset_id) is None]
    return {"datasets": rows, "catalog": catalog, "uncataloged": uncataloged}
