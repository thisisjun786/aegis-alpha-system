"""EODHD US delisted lists as ``status.us.eodhd`` delisting events (``eodhd.delisted@1``).

Lists are synthetic EODHD exchange-symbol jobs (``tests.storage.kr_identity_support``) asked for
``US``; the bars are a synthetic ``prices.us.eodhd`` generation. Every ticker, asset ID and ISIN
is made up, and the ISINs only carry valid check digits.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

import duckdb
import pytest

from aegis_alpha.storage.identity import (
    mint_instrument,
    parse_registry,
    register_identities,
    snapshot_identities,
)
from aegis_alpha.storage.kr_identity import UNBOUNDED, eodhd_unit, import_unit
from aegis_alpha.storage.maintain_promotion import (
    ROUTES,
    promote_datasets,
    standing_unresolved,
)
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.promotion.mappers import IDENTITY_ROWS, mapper, reference_table
from aegis_alpha.storage.promotion.mappers.eodhd_delisted import BARS, LAST_BAR, OBSERVED
from aegis_alpha.storage.us_symbols import ISIN_ANCHOR
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.kr_identity_support import eodhd_job, isin, symbol
from tests.storage.promotion_support import add_source, at, bar, spec, us

A, B, C = "910001", "910002", "910003"
IA, IB, IC = (mint_instrument("norgate_assetid", value) for value in (A, B, C))
ISIN_A, ISIN_B, ISIN_C = isin("000000011", "US"), isin("000000022", "US"), isin("000000033", "US")
UNREGISTERED = isin("000000099", "US")
NORGATE = "norgate_assetid"
# An instrument us_symbols minted from a new listing's ISIN, a Norgate instrument that first got
# its ISIN from a list, and one ISIN two providers tie to two instruments.
ISIN_D, ISIN_E, ISIN_X = isin("000000044", "US"), isin("000000055", "US"), isin("000000066", "US")
E, F = "910005", "910006"
ID, IE = mint_instrument(ISIN_ANCHOR, ISIN_D), mint_instrument(NORGATE, E)
IF, IX = mint_instrument(NORGATE, F), mint_instrument(ISIN_ANCHOR, ISIN_X)
THU_LIST_US = us(at("2026-09-10T21:00:00"))
NEW_YORK = ZoneInfo("America/New_York")
FRI, MON = date(2026, 9, 11), date(2026, 9, 14)
DAY_RULE = {"rule": "local_day_end@1", "basis": "record", "input": "session_date",
            "args": {"timezone": "America/New_York"}}  # fmt: skip
FLOATS = dict.fromkeys(("open", "high", "low", "close", "volume"), "float_shortest@1")
(ROUTE,) = [item for item in ROUTES if item.dataset_id == "status.us.eodhd"]


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _start(day: date) -> int:
    moment = datetime(day.year, day.month, day.day, tzinfo=NEW_YORK)
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


type _Isin = tuple[str, str, int]
"""An ISIN assertion: provider (``fmp`` or ``eodhd``), ISIN and the first instant it holds."""
type _Listing = tuple[tuple[str, str], str, tuple[_Isin, ...]]
"""An instrument: its anchor, its EODHD bar symbol and its ISIN assertions."""

LISTINGS: tuple[_Listing, ...] = (
    ((NORGATE, A), "AAA.US", (("fmp", ISIN_A, UNBOUNDED),)),
    ((NORGATE, B), "OLDB.US", (("fmp", ISIN_B, UNBOUNDED),)),
    ((NORGATE, C), "OLDC.US", (("fmp", ISIN_C, UNBOUNDED),)),
)


def _claim(  # noqa: PLR0913, PLR0917 -- one assertion spells every registry field it varies
    anchor: tuple[str, str],
    provider: str,
    namespace: str,
    token: str,
    link: str,
    valid_from: int = UNBOUNDED,
) -> dict[str, object]:
    return {"instrument": {"anchor_namespace": anchor[0], "anchor_token": anchor[1]},
            "provider": provider, "namespace": namespace, "token": token,
            "valid_from_us": valid_from, "valid_to_us": None, "known_from_us": 1,
            "supersedes_assertion_id": None, "source_snapshot_id": "sl:" + link,
            "source_hash": hashlib.sha256(f"{provider}:{token}".encode()).hexdigest()}  # fmt: skip


def _instrument(anchor: tuple[str, str]) -> dict[str, object]:
    return {"anchor_namespace": anchor[0], "anchor_token": anchor[1], "issuer": None,
            "asset_type": "unclassified", "venue": "XNYS"}  # fmt: skip


def _registry(ws: Workspace, link: str, listings: tuple[_Listing, ...]) -> dict[str, str]:
    """Each listing's anchor, bar symbol and ISIN assertions, snapshotted as ``us-delisted``.

    A Norgate instrument's anchor is its ``norgate``/``norgate_assetid`` assertion; an
    instrument minted from an ISIN (``us_isin``) is anchored by its ``eodhd``/``isin`` one,
    as ``us_symbols`` registers it.
    """
    assertions: list[dict[str, object]] = []
    for anchor, ticker, isins in listings:
        if anchor[0] == NORGATE:
            assertions.append(_claim(anchor, "norgate", NORGATE, anchor[1], link))
        assertions.append(_claim(anchor, "eodhd", "eodhd_symbol", ticker, link))
        assertions.extend(_claim(anchor, provider, "isin", token, link, valid_from)
                          for provider, token, valid_from in isins)  # fmt: skip
    document = {
        "schema": "aas-identity-registry-v1",
        "issuers": [],
        "instruments": [_instrument(anchor) for anchor, _, _ in listings],
        "assertions": assertions,
    }
    register_identities(ws.state, parse_registry(document), apply=True)
    return _snapshot(ws, "us-delisted", 5)


def _snapshot(ws: Workspace, name: str, created_at_us: int) -> dict[str, str]:
    report = snapshot_identities(ws.state, name, created_at_us=created_at_us, apply=True)
    return {"snapshot_id": str(report["snapshot_id"]), "content_hash": str(report["content_hash"])}


def _bars(
    ws: Workspace,
    listings: tuple[_Listing, ...] = LISTINGS,
    extra: tuple[tuple[str, date], ...] = (),
) -> dict[str, str]:
    """AAA trades through Friday; OLDB's last bar is 2026-08-14 and OLDC's 2026-07-20."""
    rows = [("AAA.US", date(2026, 9, 10)), ("AAA.US", FRI), ("OLDB.US", date(2026, 8, 14)),
            ("OLDC.US", date(2026, 7, 20)), *extra]  # fmt: skip
    retrieved = at("2026-09-12T10:00:00")
    pin = add_source(ws, [bar(name, day, 10.0, retrieved=retrieved, currency="USD")
                          for name, day in rows], tag="us-bars", linked=retrieved)  # fmt: skip
    identity = _registry(ws, pin["source_id"], listings)
    published = promote(ws, *spec([pin], identity, dataset="prices.us.eodhd", decimals=FLOATS,
                                  rules={"available_at_us": DAY_RULE,
                                         "revision_known_at_us": DAY_RULE},
                                  mapper={"name": "eodhd.bars@1",
                                          "args": {"timezone": "America/New_York"}}),
                        apply=True)  # fmt: skip
    assert published["published"] is True, published.get("refusals")
    return identity


def _list(ws: Workspace, retrieved: str, delisted: str, rows: list[dict[str, object]]) -> None:
    unit = eodhd_unit(*eodhd_job(rows, exchange="US", delisted=delisted, retrieved=retrieved))
    import_unit(ws, unit)


def _row(code: str, value: str | None) -> dict[str, object]:
    return symbol(code, value, exchange="NYSE", currency="USD")


def _events(ws: Workspace) -> list[tuple[str, str, int, str]]:
    return [
        (str(instrument), str(event), int(start), str(reason))
        for instrument, event, start, reason in ws.market.execute(
            "SELECT instrument_id, status_event_id, effective_from_us, reason "
            "FROM instrument_status ORDER BY ALL"
        ).fetchall()
    ]


def _flags(ws: Workspace) -> list[tuple[str, str, str]]:
    return [
        (str(instrument), str(event), str(flag))
        for instrument, event, flag in ws.market.execute(
            "SELECT s.instrument_id, s.status_event_id, f.flag FROM quality_flags f "
            "JOIN instrument_status s USING (record_id, revision_id) "
            "WHERE f.rule_id = 'eodhd.delisted' ORDER BY ALL"
        ).fetchall()
    ]


def _run(ws: Workspace, identity: dict[str, str], now_us: int) -> dict[str, object]:
    report = promote_datasets(ws, apply=True, now_us=now_us, routes=[ROUTE], identity=identity,
                              start=frozenset({"status.us.eodhd"}))  # fmt: skip
    (run,) = cast("list[dict[str, object]]", report["datasets"])
    return run


def test_delisted_lists_give_inferred_then_observed_events_by_isin(ws: Workspace) -> None:
    identity = _bars(ws)
    # Friday: AAA is listed; the first delisted list already holds OLD twice, a reused
    # ticker under two ISINs, a row without an ISIN and one whose ISIN nobody registered.
    # Thursday's listed list still holds OLD under ISIN_B: on the first delisted list that
    # proves nothing, since no earlier delisted list shows OLD absent.
    _list(ws, "2026-09-10T21:00:00Z", "0", [_row("AAA", ISIN_A), _row("OLD", ISIN_B)])
    _list(ws, "2026-09-11T21:00:00Z", "0", [_row("AAA", ISIN_A)])
    old = [_row("OLD", ISIN_B), _row("OLD", ISIN_C), _row("GONE", None),
           _row("NEW", UNREGISTERED)]  # fmt: skip
    _list(ws, "2026-09-11T22:00:00Z", "1", old)
    first = _run(ws, identity, 10)
    assert (first["status"], first["started"]) == ("advanced", True)
    (source,) = cast("list[dict[str, object]]", first["sources"])
    assert source["status"] == "unresolved"  # GONE and NEW name no instrument
    # Nothing is observed on the first list: both OLD instruments get their last bar's
    # next day, inferred; only the one delisted on or after 2026-07-29 is unverified.
    assert _events(ws) == sorted([
        (IB, LAST_BAR, _start(date(2026, 8, 15)), "eodhd_delisted_last_bar"),
        (IC, LAST_BAR, _start(date(2026, 7, 21)), "eodhd_delisted_last_bar"),
    ])  # fmt: skip
    assert _flags(ws) == sorted([
        (IB, LAST_BAR, "delisting_inferred"), (IB, LAST_BAR, "survivorship_unverified"),
        (IC, LAST_BAR, "delisting_inferred"),
    ])  # fmt: skip
    standing = standing_unresolved(ws)["status.us.eodhd"]
    assert (standing["rows_by_reason"], standing["rows_by_class"]) == (
        {"isin_missing": 1, "isin_unregistered": 1, "resolved": 2},
        {"isin_missing": {"common_stock": 1}, "isin_unregistered": {"common_stock": 1},
         "resolved": {"common_stock": 2}},
    )  # fmt: skip

    # Monday: AAA left the listed list and is on the delisted list for the first time,
    # observed on Monday, its last bar Friday; the old rows change nothing.
    _list(ws, "2026-09-14T21:00:00Z", "0", [])
    _list(ws, "2026-09-14T22:00:00Z", "1", [_row("AAA", ISIN_A), *old])
    second = _run(ws, identity, 20)
    assert second["status"] == "advanced"
    events = _events(ws)
    assert (IA, OBSERVED, _start(MON), "eodhd_delisted_observed") in events
    assert (IA, LAST_BAR, _start(FRI + timedelta(days=1)), "eodhd_delisted_last_bar") in events
    assert len(events) == 4  # noqa: PLR2004 -- two old and two new events
    # An observed delisting is not inferred and leaves no survivorship gap.
    assert [item for item in _flags(ws) if item[0] == IA] == []


def test_a_last_bar_event_ignores_bars_known_only_after_its_list(ws: Workspace) -> None:
    identity = _bars(ws)
    # The Friday bar is known at the end of Friday in New York; a list retrieved on Friday
    # evening (before that) must be dated by Thursday's bar, as a strict reader then saw it,
    # even though the pinned price chain now also holds Friday's.
    _list(ws, "2026-09-11T22:00:00Z", "1", [_row("AAA", ISIN_A)])
    assert _run(ws, identity, 10)["status"] == "advanced"
    assert _events(ws) == [
        (IA, LAST_BAR, _start(date(2026, 9, 11)), "eodhd_delisted_last_bar"),
    ]


def _last_bar_as_of(revisions: list[tuple[str, int, str, int | None, int | None]]) -> list[object]:
    """The last bar the mapper reads for one instrument under hand-made price revisions.

    Each revision is (session day, generation sequence, op, available_at_us, revision_known_at_us).
    """
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE src AS SELECT 0::INTEGER AS _aas_pin, 0::BIGINT AS _aas_ordinal, 'h' AS "
        "_aas_row_hash, 'US' AS exchange_code, '1' AS delisted, 'AAA' AS code, "
        f"'{ISIN_A}' AS isin, 'Common Stock' AS type, '2026-09-11T22:00:00Z' AS retrieved_at_utc"
    )
    connection.execute(
        f"CREATE TABLE {IDENTITY_ROWS} (token VARCHAR, instrument_id VARCHAR, "
        "valid_from_us BIGINT, valid_to_us BIGINT)"
    )
    connection.execute(
        f"INSERT INTO {IDENTITY_ROWS} VALUES ('{ISIN_A}', 'ins-a', 0, NULL)"  # noqa: S608 -- fixed synthetic constants, engine-named relations
    )
    connection.execute(
        f"CREATE TABLE {reference_table(BARS)} (instrument_id VARCHAR, session_date DATE, "
        "available_at_us BIGINT, revision_known_at_us BIGINT, _aas_sequence BIGINT, "
        "op VARCHAR, record_id VARCHAR, revision_id VARCHAR)"
    )
    for day, sequence, op, available, known in revisions:
        connection.execute(
            f"INSERT INTO {reference_table(BARS)} VALUES ('ins-a', ?, ?, ?, ?, ?, ?, ?)",  # noqa: S608 -- fixed synthetic constants, engine-named relations
            [day, available, known, sequence, op, f"r-{day}", f"v-{day}-{sequence}"],
        )
    found = mapper("eodhd.delisted@1")
    sql = found.select("src", {"timezone": "America/New_York"})
    return [
        row[0]
        for row in connection.execute(
            f"SELECT effective_from_us FROM ({sql}) WHERE status_event_id = '{LAST_BAR}' "  # noqa: S608 -- fixed synthetic constants, engine-named relations
            "AND _aas_item = 1"
        ).fetchall()
    ]


def test_a_last_bar_is_read_as_of_the_list_not_from_later_corrections() -> None:
    listed_us = int(datetime(2026, 9, 11, 22, tzinfo=UTC).timestamp() * 1_000_000)
    before, after = listed_us - 3_600_000_000, listed_us + 3_600_000_000
    # Thursday and Friday were known before the list. Friday was corrected after it, which
    # leaves only the later revision as the record's head: the earlier one still dates the event.
    corrected = _last_bar_as_of([
        ("2026-09-10", 1, "ASSERT", before, before),
        ("2026-09-11", 1, "ASSERT", before, before),
        ("2026-09-11", 2, "SUPERSEDE", before, after),
    ])  # fmt: skip
    assert corrected == [_start(date(2026, 9, 12))]
    # A bar first known only after the list does not date it; one deleted only after it still does.
    assert _last_bar_as_of([
        ("2026-09-10", 1, "ASSERT", before, before),
        ("2026-09-11", 1, "ASSERT", after, after),
    ]) == [_start(date(2026, 9, 11))]  # fmt: skip
    assert _last_bar_as_of([
        ("2026-09-11", 1, "ASSERT", before, before),
        ("2026-09-11", 2, "TOMBSTONE", None, after),
    ]) == [_start(date(2026, 9, 12))]  # fmt: skip
    assert _last_bar_as_of([
        ("2026-09-11", 1, "ASSERT", before, before),
        ("2026-09-11", 2, "TOMBSTONE", None, before),
    ]) == []  # fmt: skip


def test_a_list_of_rows_without_isin_is_still_an_unresolved_source(ws: Workspace) -> None:
    identity = _bars(ws)
    _list(ws, "2026-09-11T22:00:00Z", "1", [_row("AAA", ISIN_A)])
    assert _run(ws, identity, 10)["status"] == "advanced"
    # A later list whose only unresolved row has no ISIN at all: no token is fabricated for it,
    # yet the source is recorded as waiting and counted by its reason.
    _list(ws, "2026-09-14T22:00:00Z", "1", [_row("GONE", None)])
    run = _run(ws, identity, 20)
    (source,) = cast("list[dict[str, object]]", run["sources"])
    assert source["status"] == "unresolved"
    standing = standing_unresolved(ws)["status.us.eodhd"]
    assert standing["rows_by_reason"] == {"isin_missing": 1}


def test_a_price_revision_with_an_unknown_knowledge_time_does_not_date_an_event() -> None:
    listed_us = int(datetime(2026, 9, 11, 22, tzinfo=UTC).timestamp() * 1_000_000)
    before = listed_us - 3_600_000_000
    # Only one of the two knowledge times is set: a strict reader excludes the revision, so it
    # must not become the last bar. Thursday's fully known bar still dates the event.
    assert _last_bar_as_of([
        ("2026-09-10", 1, "ASSERT", before, before),
        ("2026-09-11", 1, "ASSERT", before, None),
    ]) == [_start(date(2026, 9, 11))]  # fmt: skip
    assert _last_bar_as_of([
        ("2026-09-10", 1, "ASSERT", before, before),
        ("2026-09-11", 1, "ASSERT", None, before),
    ]) == [_start(date(2026, 9, 11))]  # fmt: skip


def test_an_unknown_isin_in_the_earlier_listed_list_still_flags_the_survivorship_gap(
    ws: Workspace,
) -> None:
    identity = _bars(ws)
    # The earlier listed list holds a row without an ISIN and not OLD's: "ISIN_B IN list" is
    # NULL there. The observed predicate must read as false, so the inferred delisting inside
    # the unverified window keeps both flags.
    _list(ws, "2026-09-11T21:00:00Z", "0", [_row("ZZZ", None)])
    _list(ws, "2026-09-11T22:00:00Z", "1", [_row("OLD", ISIN_C)])
    _run(ws, identity, 10)
    _list(ws, "2026-09-14T21:00:00Z", "0", [])
    _list(ws, "2026-09-14T22:00:00Z", "1", [_row("OLD", ISIN_B), _row("OLD", ISIN_C)])
    _run(ws, identity, 20)
    flags = _flags(ws)
    assert (IB, LAST_BAR, "delisting_inferred") in flags
    assert (IB, LAST_BAR, "survivorship_unverified") in flags
    assert all(event != OBSERVED for _, event, _, _ in _events(ws))


ISIN_LISTINGS: tuple[_Listing, ...] = (
    *LISTINGS,
    # Minted from its ISIN: the eodhd/isin assertion is its anchor, held from the start.
    ((ISIN_ANCHOR, ISIN_D), "NEWD.US", (("eodhd", ISIN_D, UNBOUNDED),)),
    # A Norgate instrument a list joined its ISIN to: eodhd/isin from that list's retrieval.
    ((NORGATE, E), "NORE.US", (("eodhd", ISIN_E, THU_LIST_US),)),
)
ISIN_BARS = (("NEWD.US", FRI), ("NORE.US", FRI))


def _delisted_on_monday(
    ws: Workspace, identity: dict[str, str], rows: list[dict[str, object]]
) -> dict[str, object]:
    """``rows`` are listed Thursday and Friday, absent from Friday's (first) delisted list and
    on Monday's: observed on Monday. Returns Monday's run."""
    _list(ws, "2026-09-10T21:00:00Z", "0", rows)
    _list(ws, "2026-09-11T21:00:00Z", "0", rows)
    _list(ws, "2026-09-11T22:00:00Z", "1", [_row("OLD", ISIN_B)])
    _run(ws, identity, 10)
    _list(ws, "2026-09-14T21:00:00Z", "0", [])
    _list(ws, "2026-09-14T22:00:00Z", "1", rows)
    return _run(ws, identity, 20)


def _both_events(instrument: str) -> list[tuple[str, str, int, str]]:
    return [
        (instrument, LAST_BAR, _start(FRI + timedelta(days=1)), "eodhd_delisted_last_bar"),
        (instrument, OBSERVED, _start(MON), "eodhd_delisted_observed"),
    ]


@pytest.mark.parametrize(
    ("code", "value", "instrument"),
    [("NEWD", ISIN_D, ID), ("NORE", ISIN_E, IE)],
    ids=["minted_from_its_isin", "norgate_joined_by_a_list"],
)
def test_an_instrument_named_by_its_eodhd_isin_gets_both_delisting_events(
    ws: Workspace, code: str, value: str, instrument: str
) -> None:
    identity = _bars(ws, ISIN_LISTINGS, ISIN_BARS)
    run = _delisted_on_monday(ws, identity, [_row(code, value)])
    (source,) = cast("list[dict[str, object]]", run["sources"])
    assert source["status"] != "unresolved"
    assert [item for item in _events(ws) if item[0] == instrument] == _both_events(instrument)
    assert standing_unresolved(ws).get("status.us.eodhd") is None


def test_one_isin_under_both_providers_names_one_instrument_or_stays_ambiguous(
    ws: Workspace,
) -> None:
    listings: tuple[_Listing, ...] = (
        # AAA's ISIN at FMP and, joined later by a list, at EODHD: one instrument.
        ((NORGATE, A), "AAA.US", (("fmp", ISIN_A, UNBOUNDED), ("eodhd", ISIN_A, THU_LIST_US))),
        ((NORGATE, B), "OLDB.US", (("fmp", ISIN_B, UNBOUNDED),)),
        # ISIN_X is a Norgate instrument's at FMP and an ISIN-minted instrument's at EODHD.
        ((NORGATE, F), "XF.US", (("fmp", ISIN_X, UNBOUNDED),)),
        ((ISIN_ANCHOR, ISIN_X), "XM.US", (("eodhd", ISIN_X, UNBOUNDED),)),
    )
    identity = _bars(ws, listings, (("XF.US", FRI), ("XM.US", FRI)))
    run = _delisted_on_monday(ws, identity, [_row("AAA", ISIN_A), _row("XF", ISIN_X)])
    (source,) = cast("list[dict[str, object]]", run["sources"])
    assert source["status"] == "unresolved"
    events = _events(ws)
    assert [item for item in events if item[0] == IA] == _both_events(IA)
    # Neither holder of ISIN_X is picked.
    assert [item for item in events if item[0] in {IF, IX}] == []
    standing = standing_unresolved(ws)["status.us.eodhd"]
    assert standing["rows_by_reason"] == {"isin_ambiguous": 1, "resolved": 1}


def test_a_waiting_list_is_promoted_once_a_list_joins_its_isin_at_eodhd(ws: Workspace) -> None:
    identity = _bars(ws, (*LISTINGS, ((NORGATE, E), "NORE.US", ())), (("NORE.US", FRI),))
    # Friday's list starts the chain; Monday's names NORE by an ISIN nobody registered yet,
    # so its source is recorded as waiting on the identity rows it was judged against.
    _list(ws, "2026-09-11T22:00:00Z", "1", [_row("AAA", ISIN_A)])
    _run(ws, identity, 10)
    _list(ws, "2026-09-14T22:00:00Z", "1", [_row("NORE", ISIN_E)])
    first = _run(ws, identity, 20)
    (source,) = cast("list[dict[str, object]]", first["sources"])
    assert source["status"] == "unresolved"
    # A later symbol-list build joins ISIN_E to the Norgate instrument, as us_symbols does.
    ((link,),) = ws.state.execute(
        "SELECT DISTINCT substr(source_snapshot_id, 4) FROM identity_assertions"
    ).fetchall()
    document = {
        "schema": "aas-identity-registry-v1",
        "issuers": [],
        "instruments": [_instrument((NORGATE, E))],
        "assertions": [_claim((NORGATE, E), "eodhd", "isin", ISIN_E, str(link), THU_LIST_US)],
    }
    register_identities(ws.state, parse_registry(document), apply=True)
    # The waiting source is planned again, since the rows it would resolve with changed.
    second = _run(ws, _snapshot(ws, "us-delisted-joined", 6), 30)
    (source,) = cast("list[dict[str, object]]", second["sources"])
    assert source["status"] == "promoted"
    assert [item for item in _events(ws) if item[0] == IE] == [
        (IE, LAST_BAR, _start(FRI + timedelta(days=1)), "eodhd_delisted_last_bar"),
    ]
