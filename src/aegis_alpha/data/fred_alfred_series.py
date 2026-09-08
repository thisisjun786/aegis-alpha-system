"""Frozen AAS-DATA-012 G-A series universe.

The collector may only request identifiers from the hashed catalogs below.
``DEFAULT_SERIES_IDS`` is the owner-frozen legacy four; ``MACRO_SERIES_IDS``
is the explicit broader macro catalog. Both are fixed lists: the collector
never searches, recommends, or derives extra series at execution time.
T10Y2Y is not computed from DGS10 - DGS2; both paths are independent evidence.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from aegis_alpha.data.serialization import content_sha256

PROVIDER: Final = "fred_alfred"
POLICY_ID: Final = "fred-alfred-official-verifier-v1"
PENDING_LICENSE: Final = "PUBLIC_OFFICIAL_PENDING_POLICY_REVIEW"
#: 005 plan dataset. Series identity is the watermark stream, matching FMP.
PLAN_DATASET: Final = "fred_alfred_observations"

#: Owner-frozen default universe from AAS-DATA-012 §2. Order is part of the hash.
DEFAULT_SERIES_IDS: Final[tuple[str, ...]] = ("T10Y2Y", "T10Y3M", "DGS10", "DGS2")
DEFAULT_SERIES_UNIVERSE_SHA256: Final = content_sha256(list(DEFAULT_SERIES_IDS))

#: Explicit macro catalog: rates, inflation, employment, output, consumption,
#: housing, liquidity, credit and risk. The legacy four lead the list so the
#: default selection keeps its historical order. Order is part of the hash.
MACRO_SERIES_IDS: Final[tuple[str, ...]] = (
    # rates and curve
    "T10Y2Y",
    "T10Y3M",
    "DGS10",
    "DGS2",
    "DGS3MO",
    "DGS30",
    "FEDFUNDS",
    "SOFR",
    # inflation
    "CPIAUCSL",
    "CPILFESL",
    "PCEPI",
    "PCEPILFE",
    # employment
    "UNRATE",
    "PAYEMS",
    "ICSA",
    # output, consumption, housing, sentiment
    "GDPC1",
    "GDP",
    "INDPRO",
    "RSAFS",
    "HOUST",
    "UMCSENT",
    # money and liquidity
    "M2SL",
    "WALCL",
    "RRPONTSYD",
    "WTREGEN",
    # credit, dollar, commodities, risk
    "BAMLC0A0CM",
    "BAMLH0A0HYM2",
    "DTWEXBGS",
    "DCOILWTICO",
    "VIXCLS",
    "STLFSI4",
    "NFCI",
    # breakevens, real yields, cycle
    "T10YIE",
    "DFII10",
    "USREC",
)
MACRO_SERIES_UNIVERSE_SHA256: Final = content_sha256(list(MACRO_SERIES_IDS))

#: Every identifier the collector may ever request. Membership checks use this
#: set; ``DEFAULT_SERIES_IDS`` only decides what runs when nothing is selected.
ALLOWED_SERIES_IDS: Final[frozenset[str]] = frozenset(MACRO_SERIES_IDS)

if len(ALLOWED_SERIES_IDS) != len(MACRO_SERIES_IDS):
    raise AssertionError("MACRO_SERIES_IDS must not contain duplicates")
if MACRO_SERIES_IDS[: len(DEFAULT_SERIES_IDS)] != DEFAULT_SERIES_IDS:
    raise AssertionError("MACRO_SERIES_IDS must begin with DEFAULT_SERIES_IDS in order")


def series_universe_sha256(series_ids: Sequence[str]) -> str:
    return content_sha256(list(series_ids))


def validate_series_universe(series_ids: Sequence[str]) -> tuple[str, ...]:
    """Accept only frozen identifiers, returned in the catalog's official order.

    Any selection is normalized to ``MACRO_SERIES_IDS`` order, so the same set of
    identifiers always hashes identically regardless of request order. A
    selection limited to the legacy four therefore hashes to
    ``DEFAULT_SERIES_UNIVERSE_SHA256``.
    """

    if not series_ids:
        raise ValueError("series universe must be nonempty")
    seen: set[str] = set()
    extras: list[str] = []
    unknown_case: list[str] = []
    for series_id in series_ids:
        if not isinstance(series_id, str) or not series_id.strip():
            raise ValueError("series identifiers must be nonempty strings")
        if series_id != series_id.strip() or series_id != series_id.upper():
            unknown_case.append(series_id)
            continue
        if series_id not in ALLOWED_SERIES_IDS:
            extras.append(series_id)
            continue
        if series_id in seen:
            raise ValueError(f"duplicate series_id is forbidden: {series_id}")
        seen.add(series_id)
    if unknown_case:
        raise ValueError(
            "series identifiers must be the exact uppercase FRED ids; "
            f"rejected: {', '.join(unknown_case)}"
        )
    if extras:
        raise ValueError(f"collector must not invent extra series; rejected: {', '.join(extras)}")
    return tuple(series_id for series_id in MACRO_SERIES_IDS if series_id in seen)


def watermark_dataset(series_id: str) -> str:
    """Return the 005 plan dataset. Series identity belongs on ``stream``."""

    if series_id not in ALLOWED_SERIES_IDS:
        raise ValueError(f"watermark dataset is undefined for {series_id}")
    return PLAN_DATASET


def watermark_stream(series_id: str) -> str:
    """Per-series 005 stream. Matches FMP's plan dataset + per-symbol stream."""

    if series_id not in ALLOWED_SERIES_IDS:
        raise ValueError(f"watermark stream is undefined for {series_id}")
    return series_id


def refuse_derived_spread(*, left: str, right: str) -> None:
    """T10Y2Y and DGS10/DGS2 are coexisting evidence, never substitutes."""

    pair = {left, right}
    if pair == {"DGS10", "DGS2"}:
        raise ValueError("refusing to derive T10Y2Y from DGS10-DGS2; both paths are evidence")


__all__ = [
    "ALLOWED_SERIES_IDS",
    "DEFAULT_SERIES_IDS",
    "DEFAULT_SERIES_UNIVERSE_SHA256",
    "MACRO_SERIES_IDS",
    "MACRO_SERIES_UNIVERSE_SHA256",
    "PENDING_LICENSE",
    "PLAN_DATASET",
    "POLICY_ID",
    "PROVIDER",
    "refuse_derived_spread",
    "series_universe_sha256",
    "validate_series_universe",
    "watermark_dataset",
    "watermark_stream",
]
