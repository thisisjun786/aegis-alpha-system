"""KIND listed-company lists as ``status.kr.kind.delisted`` delisting events (``kind.delisted@1``).

Lists are synthetic KIND lists and an EODHD KR symbol list (``tests.storage.kr_identity_support``)
committed the way ``aas collect kind`` and ``aas identity kr-import`` commit them; the bars are a
synthetic ``prices.kr.eodhd`` generation. Every company, code and ISIN is made up.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.storage.identity import (
    decode_registry,
    mint_instrument,
    register_identities,
    snapshot_identities,
)
from aegis_alpha.storage.kr_identity import build_from_workspace, eodhd_unit, import_unit, kind_unit
from aegis_alpha.storage.maintain_promotion import ROUTES, promote_datasets
from aegis_alpha.storage.promotion.engine import dataset_head, promote
from aegis_alpha.storage.promotion.mappers.kind_delisted import LAST_BAR, OBSERVED
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.kr_identity_support import eodhd_job, isin, kind_listing, symbol
from tests.storage.promotion_support import add_source, at, bar, spec, us

# A stays on KOSPI, B leaves both lists on the third day, C moves from KOSDAQ to KOSPI and E
# stays on KOSDAQ.
A, B, C, E = "100010", "100020", "100030", "100040"
ISINS = {A: isin("710000100"), B: isin("710000200"), C: isin("710000300"), E: isin("710000400")}
DAYS = (date(2026, 9, 7), date(2026, 9, 8), date(2026, 9, 9))
LISTS = {
    "kind-kospi": ((A, B), (A, B), (A, C)),
    "kind-kosdaq": ((C, E), (C, E), (E,)),
}
(STATUS,) = [route for route in ROUTES if route.dataset_id == "status.kr.kind"]
(DELISTED,) = [route for route in ROUTES if route.dataset_id == "status.kr.kind.delisted"]


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _seoul(day: date) -> int:
    """UTC microseconds of the Seoul midnight that starts ``day`` (UTC+9, no daylight time)."""
    return us(datetime(day.year, day.month, day.day, tzinfo=UTC) - timedelta(hours=9))


def _lists(
    ws: Workspace,
    days: tuple[date, ...] = DAYS,
    lists: Mapping[str, tuple[tuple[str, ...], ...]] = LISTS,
) -> list[str]:
    """Each day's KOSPI and KOSDAQ list, collected at 15:05 in Seoul; their source IDs."""
    return [_list(ws, list_id, day, codes[index])
            for index, day in enumerate(days) for list_id, codes in lists.items()]  # fmt: skip


def _list(
    ws: Workspace, list_id: str, day: date, codes: tuple[str, ...], at_utc: str = "06:05:48"
) -> str:
    """One KIND list collected on ``day`` at ``at_utc``; its source ID."""
    rows = [(f"합성{code}", code, "2000-01-04") for code in codes]
    retrieved = f"{day.isoformat()}T{at_utc}Z"
    unit = kind_unit(*kind_listing(rows, list_id=list_id, retrieved=retrieved))
    import_unit(ws, unit)
    return unit.content.source_id


def _identity(ws: Workspace, kind: list[str]) -> dict[str, str]:
    """Register every code through its EODHD ISIN and the KIND lists; snapshot them."""
    job = eodhd_unit(*eodhd_job([symbol(code, value) for code, value in ISINS.items()]))
    import_unit(ws, job)
    registry = build_from_workspace(ws, eodhd=[job.content.source_id], kind=kind)
    document = decode_registry(registry.raw(), expected_file_sha256=registry.sha256())
    register_identities(ws.state, document, apply=True)
    snapshot = snapshot_identities(ws.state, "kr", created_at_us=5, apply=True)
    return {"snapshot_id": str(snapshot["snapshot_id"]),
            "content_hash": str(snapshot["content_hash"])}  # fmt: skip


def _bars(ws: Workspace, identity: dict[str, str], last: date) -> None:
    """A trades every day; B's last bar is ``last``."""
    retrieved = at("2026-09-09T10:00:00")
    rows = [bar(f"{A}.KO", day, 10.0, retrieved=retrieved) for day in DAYS]
    rows.append(bar(f"{B}.KO", last, 20.0, retrieved=retrieved))
    pin = add_source(ws, rows, tag="kr-bars", linked=retrieved)
    published = promote(ws, *spec([pin], identity), apply=True)
    assert published["published"] is True, published.get("refusals")


def _delistings(ws: Workspace) -> list[tuple[object, ...]]:
    """Each delisting event with the flags its row carries."""
    return ws.market.execute(
        "SELECT s.instrument_id, s.status_event_id, s.effective_from_us, s.reason, "
        "coalesce(list_sort(list(f.flag) FILTER (f.flag IS NOT NULL)), []) "
        "FROM instrument_status s "
        "LEFT JOIN quality_flags f ON f.record_id = s.record_id AND f.revision_id = s.revision_id "
        "AND f.rule_id = 'kind.delisted' WHERE s.status = 'delisted' "
        "GROUP BY ALL ORDER BY ALL"
    ).fetchall()


def _status_rows(ws: Workspace) -> list[tuple[object, ...]]:
    return ws.market.execute(
        "SELECT * EXCLUDE (generation_id) FROM instrument_status WHERE status <> 'delisted' "
        "ORDER BY ALL"
    ).fetchall()


def test_a_code_that_leaves_both_kind_lists_is_the_one_delisting(ws: Workspace) -> None:
    identity = _identity(ws, _lists(ws))
    # B last traded on Friday 09-04, before the first KIND list.
    _bars(ws, identity, date(2026, 9, 4))
    promote_datasets(ws, apply=True, now_us=10, routes=[STATUS], identity=identity,
                     start=frozenset({"status.kr.kind"}))  # fmt: skip
    head, listed = dataset_head(ws, "status.kr.kind"), _status_rows(ws)
    report = promote_datasets(ws, apply=True, now_us=20, routes=[DELISTED], identity=identity,
                              start=frozenset({"status.kr.kind.delisted"}))  # fmt: skip
    (run,) = cast("list[dict[str, object]]", report["datasets"])
    assert (run["status"], run["started"]) == ("advanced", True)
    # Only B: C moved to KOSPI and stays listed. Consecutive daily lists observe B gone on
    # the third day; its last-bar delisting (09-05) precedes the first list, which no list
    # could observe.
    ib = mint_instrument("krx_isin", ISINS[B])
    assert _delistings(ws) == sorted([
        (ib, OBSERVED, _seoul(DAYS[2]), "kind_delisted_observed", []),
        (ib, LAST_BAR, _seoul(date(2026, 9, 5)), "kind_delisted_last_bar",
         ["survivorship_unverified"]),
    ])  # fmt: skip
    # The listing chain keeps its head and rows.
    assert (dataset_head(ws, "status.kr.kind"), _status_rows(ws)) == (head, listed)


def test_a_delisting_after_a_gap_between_lists_is_survivorship_unverified(ws: Workspace) -> None:
    # No list on 09-08: B left on 09-08 or 09-09.
    days = (DAYS[0], DAYS[2])
    lists = {"kind-kospi": ((A, B), (A,)), "kind-kosdaq": ((E,), (E,))}
    identity = _identity(ws, _lists(ws, days, lists))
    _bars(ws, identity, DAYS[0])
    promote_datasets(ws, apply=True, now_us=20, routes=[DELISTED], identity=identity,
                     start=frozenset({"status.kr.kind.delisted"}))  # fmt: skip
    ib = mint_instrument("krx_isin", ISINS[B])
    assert _delistings(ws) == sorted([
        (ib, OBSERVED, _seoul(DAYS[2]), "kind_delisted_observed", ["survivorship_unverified"]),
        (ib, LAST_BAR, _seoul(DAYS[1]), "kind_delisted_last_bar", ["survivorship_unverified"]),
    ])  # fmt: skip


def test_nothing_is_observed_until_both_of_todays_lists_are_committed(ws: Workspace) -> None:
    first = _lists(ws, DAYS[:1], {"kind-kospi": ((A, B),), "kind-kosdaq": ((C, E),)})
    identity = _identity(ws, [*first, *_lists(ws, DAYS[1:2], {"kind-kospi": ((A,),)})])
    _bars(ws, identity, DAYS[0])
    # Only the second day's KOSPI list: KOSDAQ may still name B, and C is on KOSDAQ.
    alone = promote_datasets(ws, apply=True, now_us=20, routes=[DELISTED], identity=identity,
                             start=frozenset({"status.kr.kind.delisted"}))  # fmt: skip
    assert (cast("list[dict[str, object]]", alone["datasets"])[0]["status"], _delistings(ws)) == (
        "advanced", [])  # fmt: skip
    _lists(ws, DAYS[1:2], {"kind-kosdaq": ((C, E),)})
    promote_datasets(ws, apply=True, now_us=30, routes=[DELISTED], identity=identity,
                     start=frozenset({"status.kr.kind.delisted"}))  # fmt: skip
    ib = mint_instrument("krx_isin", ISINS[B])
    assert [event[:2] for event in _delistings(ws)] == sorted([(ib, OBSERVED), (ib, LAST_BAR)])


def _same_day_lists(ws: Workspace, older: tuple[str, ...], newer: tuple[str, ...]) -> None:
    """The first day's lists, then two KOSDAQ lists of the second day an hour apart."""
    first = _lists(ws, DAYS[:1], {"kind-kospi": ((A,),), "kind-kosdaq": ((C, E),)})
    second = [_list(ws, "kind-kospi", DAYS[1], (A,)),
              _list(ws, "kind-kosdaq", DAYS[1], older),
              _list(ws, "kind-kosdaq", DAYS[1], newer, "07:05:48")]  # fmt: skip
    identity = _identity(ws, [*first, *second])
    _bars(ws, identity, DAYS[0])
    promote_datasets(ws, apply=True, now_us=20, routes=[DELISTED], identity=identity,
                     start=frozenset({"status.kr.kind.delisted"}))  # fmt: skip


def test_an_older_list_of_the_same_market_and_day_observes_nothing(ws: Workspace) -> None:
    # The older KOSDAQ list lacks C; the newer one of the same day still holds it.
    _same_day_lists(ws, (E,), (C, E))
    assert _delistings(ws) == []


def test_a_code_the_newest_list_of_the_day_drops_is_delisted(ws: Workspace) -> None:
    # The older KOSDAQ list still holds C; the newer one of the same day drops it.
    _same_day_lists(ws, (C, E), (E,))
    ic = mint_instrument("krx_isin", ISINS[C])
    assert _delistings(ws) == [(ic, OBSERVED, _seoul(DAYS[1]), "kind_delisted_observed", [])]
