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
from aegis_alpha.storage.promotion.mappers.eodhd_delisted import LAST_BAR, OBSERVED
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.kr_identity_support import eodhd_job, isin, symbol
from tests.storage.promotion_support import add_source, at, bar, spec

A, B, C = "910001", "910002", "910003"
IA, IB, IC = (mint_instrument("norgate_assetid", value) for value in (A, B, C))
ISIN_A, ISIN_B, ISIN_C = isin("000000011", "US"), isin("000000022", "US"), isin("000000033", "US")
UNREGISTERED = isin("000000099", "US")
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


def _registry(ws: Workspace, link: str) -> dict[str, str]:
    """Three instruments: each a Norgate anchor, a bar symbol and a registered FMP ISIN."""

    def claim(anchor: str, provider: str, namespace: str, token: str) -> dict[str, object]:
        return {"instrument": {"anchor_namespace": "norgate_assetid", "anchor_token": anchor},
                "provider": provider, "namespace": namespace, "token": token,
                "valid_from_us": UNBOUNDED, "valid_to_us": None, "known_from_us": 1,
                "supersedes_assertion_id": None, "source_snapshot_id": "sl:" + link,
                "source_hash": hashlib.sha256(token.encode()).hexdigest()}  # fmt: skip

    rows = ((A, "AAA.US", ISIN_A), (B, "OLDB.US", ISIN_B), (C, "OLDC.US", ISIN_C))
    document = {
        "schema": "aas-identity-registry-v1",
        "issuers": [],
        "instruments": [
            {
                "anchor_namespace": "norgate_assetid",
                "anchor_token": anchor,
                "issuer": None,
                "asset_type": "unclassified",
                "venue": "XNYS",
            }
            for anchor, _, _ in rows
        ],
        "assertions": [
            item
            for anchor, ticker, code in rows
            for item in (
                claim(anchor, "norgate", "norgate_assetid", anchor),
                claim(anchor, "eodhd", "eodhd_symbol", ticker),
                claim(anchor, "fmp", "isin", code),
            )
        ],
    }
    register_identities(ws.state, parse_registry(document), apply=True)
    report = snapshot_identities(ws.state, "us-delisted", created_at_us=5, apply=True)
    return {"snapshot_id": str(report["snapshot_id"]), "content_hash": str(report["content_hash"])}


def _bars(ws: Workspace) -> dict[str, str]:
    """AAA trades through Friday; OLDB's last bar is 2026-08-14 and OLDC's 2026-07-20."""
    rows = [("AAA.US", date(2026, 9, 10)), ("AAA.US", FRI), ("OLDB.US", date(2026, 8, 14)),
            ("OLDC.US", date(2026, 7, 20))]  # fmt: skip
    retrieved = at("2026-09-12T10:00:00")
    pin = add_source(ws, [bar(name, day, 10.0, retrieved=retrieved, currency="USD")
                          for name, day in rows], tag="us-bars", linked=retrieved)  # fmt: skip
    identity = _registry(ws, pin["source_id"])
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
