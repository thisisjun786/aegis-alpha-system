"""Dated EODHD US symbol lists continue US ticker claims day by day (``eodhd.us_symbol_list@1``).

Lists are synthetic EODHD exchange-symbol jobs (``tests.storage.kr_identity_support``) asked
for ``US``; every ticker, asset ID and ISIN is made up, and the ISINs only carry valid check
digits. The registered claims a list continues are synthetic registry rows.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.storage import maintain_identity
from aegis_alpha.storage.identity import (
    mint_instrument,
    parse_registry,
    register_identities,
    snapshot_identities,
)
from aegis_alpha.storage.kr_identity import UNBOUNDED, SourceRows, eodhd_unit, import_unit
from aegis_alpha.storage.maintain_promotion import Route, promote_datasets, standing_unresolved
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.us_identity import (
    UsRegistry,
    check_registered,
    listed_claims,
    session_start_us,
)
from aegis_alpha.storage.us_symbols import (
    Held,
    Registry,
    build_symbol_claims,
    covered_day,
)
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.kr_identity_support import bad_check_digit, eodhd_job, isin, symbol
from tests.storage.promotion_support import add_source, at, bar, spec

A, B, C = "900001", "900002", "900003"
IA, IB, IC = (mint_instrument("norgate_assetid", value) for value in (A, B, C))
ISIN_A, ISIN_B, ISIN_C = isin("000000011", "US"), isin("000000022", "US"), isin("000000033", "US")
TUE, WED, THU, FRI = date(2026, 9, 8), date(2026, 9, 9), date(2026, 9, 10), date(2026, 9, 11)
MON = date(2026, 9, 14)
NEW_YORK = "America/New_York"
DAY_RULE = {"rule": "local_day_end@1", "basis": "record", "input": "session_date",
            "args": {"timezone": NEW_YORK}}  # fmt: skip
FLOATS = dict.fromkeys(("open", "high", "low", "close", "volume"), "float_shortest@1")
US = "provider_symbol LIKE '%.US'"
ROUTE = Route("prices.us.eodhd", "eodhd.bars@1", "synthetic-kr-bars-", "bars", US)


def _us(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def _evening(day: date) -> str:
    """22:00 UTC: after the New York close, in summer and in winter."""
    return f"{day.isoformat()}T22:00:00Z"


def _row(code: str, value: str | None, *, currency: str = "USD") -> dict[str, object]:
    return symbol(code, value, exchange="NYSE", currency=currency)


def _list(retrieved: str, rows: list[dict[str, object]]) -> SourceRows:
    return eodhd_unit(*eodhd_job(rows, exchange="US", retrieved=retrieved)).source_rows()


def _base(token: str, instrument: str, through: date) -> Held:
    """A registered master or export claim ending after its last session ``through``."""
    end = session_start_us(through + timedelta(days=1))
    return Held(token, instrument, session_start_us(date(2001, 2, 5)), end, listed=False)


def _registry(*held: Held, owners: dict[str, set[str]] | None = None) -> Registry:
    found = owners if owners is not None else {ISIN_A: {IA}, ISIN_B: {IB}, ISIN_C: {IC}}
    return Registry(held, {key: frozenset(value) for key, value in found.items()},
                    {IA: A, IB: B, IC: C})  # fmt: skip


def _claims(document: dict[str, object]) -> list[tuple[str, str, int, int]]:
    rows = cast("list[dict[str, object]]", document["assertions"])
    return [
        (str(row["token"]), str(cast("dict[str, str]", row["instrument"])["anchor_token"]),
         cast("int", row["valid_from_us"]), cast("int", row["valid_to_us"]))
        for row in rows
    ]  # fmt: skip


def _day(token: str, assetid: str, day: date) -> tuple[str, str, int, int]:
    return (token, assetid, session_start_us(day), session_start_us(day + timedelta(days=1)))


def test_each_observed_day_after_the_export_continues_the_ticker_for_that_day_only() -> None:
    lists = [_list(_evening(WED), [_row("AAA", ISIN_A)]),
             _list(_evening(THU), [_row("AAA", ISIN_A)])]  # fmt: skip
    built = build_symbol_claims(lists, _registry(_base("AAA.US", IA, TUE)))
    document = built.document()
    assert _claims(document) == [_day("AAA.US", A, WED), _day("AAA.US", A, THU)]
    (first, _) = cast("list[dict[str, object]]", document["assertions"])
    # Known from the evening list that observed the day, never earlier.
    assert first["known_from_us"] == _us(datetime(2026, 9, 9, 22, tzinfo=UTC))
    assert str(first["source_snapshot_id"]).startswith("sl:qveris-eodhd-exchange-symbols-")
    report = built.report()
    assert report["registry_through"] == TUE.isoformat()
    assert report["history_gap"] is None
    assert report["observation_gaps"] == []
    assert report["unresolved_days"] == {}


def test_an_unobserved_weekday_is_never_claimed_and_is_reported_as_a_gap() -> None:
    # Nothing observed Wednesday: Thursday is not bracketed, Friday is (by Thursday).
    lists = [_list(_evening(day), [_row("AAA", ISIN_A)]) for day in (THU, FRI)]
    lists.append(_list(_evening(MON + timedelta(days=1)), [_row("AAA", ISIN_A)]))
    built = build_symbol_claims(lists, _registry(_base("AAA.US", IA, TUE)))
    assert _claims(built.document()) == [_day("AAA.US", A, FRI)]
    report = built.report()
    assert report["unresolved_days"] == {"list_not_bracketed": 2}
    assert report["history_gap"] == {"from": WED.isoformat(), "through": WED.isoformat(),
                                     "weekdays": 1}  # fmt: skip
    # Monday was not observed, so Tuesday after it stays unresolved too.
    assert report["observation_gaps"] == [
        {"from": MON.isoformat(), "through": MON.isoformat(), "weekdays": 1}
    ]


def test_saturday_and_sunday_need_no_observation() -> None:
    lists = [_list(_evening(MON), [_row("AAA", ISIN_A)])]
    built = build_symbol_claims(lists, _registry(_base("AAA.US", IA, FRI)))
    assert _claims(built.document()) == [_day("AAA.US", A, MON)]
    assert built.report()["history_gap"] is None


def test_a_reused_or_renamed_ticker_waits_for_a_bracketing_day() -> None:
    # Wednesday AAA names B (reused) and ZZZ names A (renamed): neither is shown the day
    # before, so both wait; Thursday both are bracketed by Wednesday's own observation.
    rows = [_row("AAA", ISIN_B), _row("ZZZ", ISIN_A)]
    lists = [_list(_evening(WED), rows), _list(_evening(THU), rows)]
    built = build_symbol_claims(lists, _registry(_base("AAA.US", IA, TUE)))
    assert _claims(built.document()) == [_day("AAA.US", B, THU), _day("ZZZ.US", A, THU)]
    report = built.report()
    assert report["unresolved"] == {"list_not_bracketed": ["ZZZ.US"],
                                    "list_symbol_moved": ["AAA.US"]}  # fmt: skip


def test_conflicting_or_unverifiable_rows_name_no_instrument() -> None:
    held = [_base(f"{code}.US", IA, TUE) for code in ("AAA", "BBB", "CCC", "DDD", "EEE", "FFF")]
    unknown = isin("000000099", "US")
    shared = isin("000000088", "US")
    euro = isin("000000055", "US")
    first = _list(_evening(WED), [
        _row("AAA", ISIN_A), _row("BBB", None), _row("CCC", bad_check_digit(ISIN_A)),
        _row("DDD", unknown), _row("EEE", shared), _row("FFF", euro, currency="EUR"),
        _row("GGG", ISIN_C), _row("GGG", ISIN_C), _row("HHH", ISIN_B), _row("III", ISIN_B),
    ])  # fmt: skip
    # Another list of the same day names another ISIN for AAA.
    second = _list("2026-09-09T23:00:00Z", [_row("AAA", ISIN_C)])
    owners = {ISIN_A: {IA}, ISIN_B: {IB}, ISIN_C: {IC}, shared: {IA, IB}, euro: {IC}}
    built = build_symbol_claims([first, second], _registry(*held, owners=owners))
    assert built.assertions == []
    assert built.report()["unresolved"] == {
        "list_currency_not_usd": ["FFF.US"],
        "list_isin_ambiguous": ["EEE.US"],
        "list_isin_invalid": ["CCC.US"],
        "list_isin_missing": ["BBB.US"],
        "list_isin_repeated": ["HHH.US", "III.US"],
        "list_isin_unregistered": ["DDD.US"],
        "list_observations_disagree": ["AAA.US"],
        "list_symbol_repeated": ["GGG.US"],
    }


@pytest.mark.parametrize(
    ("retrieved", "covered"),
    [
        (datetime(2026, 7, 1, 13, 29, 59, tzinfo=UTC), date(2026, 6, 30)),  # 09:29:59 EDT
        (datetime(2026, 7, 1, 13, 30, tzinfo=UTC), date(2026, 7, 1)),  # 09:30 EDT
        (datetime(2026, 11, 2, 14, 29, 59, tzinfo=UTC), date(2026, 11, 1)),  # 09:29:59 EST
        (datetime(2026, 11, 2, 14, 30, tzinfo=UTC), date(2026, 11, 2)),  # 09:30 EST
        (datetime(2026, 11, 3, 3, 0, tzinfo=UTC), date(2026, 11, 2)),  # 22:00 EST, UTC's 3rd
    ],
)
def test_an_observation_covers_the_new_york_trading_day_it_follows(
    retrieved: datetime, covered: date
) -> None:
    assert covered_day(_us(retrieved)) == covered


def test_claims_span_one_new_york_day_across_the_dst_change() -> None:
    # 2026-11-01 ends daylight saving time: Friday's day starts at 04:00 UTC, Monday's at 05:00.
    friday, monday = date(2026, 10, 30), date(2026, 11, 2)
    lists = [_list(_evening(day), [_row("AAA", ISIN_A)]) for day in (friday, monday)]
    built = build_symbol_claims(lists, _registry(_base("AAA.US", IA, date(2026, 10, 29))))
    assert _claims(built.document()) == [
        ("AAA.US", A, _us(datetime(2026, 10, 30, 4, tzinfo=UTC)),
         _us(datetime(2026, 10, 31, 4, tzinfo=UTC))),
        ("AAA.US", A, _us(datetime(2026, 11, 2, 5, tzinfo=UTC)),
         _us(datetime(2026, 11, 3, 5, tzinfo=UTC))),
    ]  # fmt: skip


def test_a_day_a_registered_claim_covers_is_never_claimed_again() -> None:
    lists = [_list(_evening(WED), [_row("AAA", ISIN_A), _row("BBB", ISIN_B)])]
    wednesday = session_start_us(WED), session_start_us(THU)
    registry = _registry(
        _base("AAA.US", IA, TUE), _base("BBB.US", IB, TUE),
        Held("AAA.US", IA, *wednesday, listed=True),
        Held("BBB.US", IC, *wednesday, listed=True),
    )  # fmt: skip
    built = build_symbol_claims(lists, registry)
    assert built.assertions == []
    assert built.already_claimed == 1
    assert built.report()["unresolved"] == {"list_registered_conflict": ["BBB.US"]}


# --- maintenance: registration, the snapshot and the waiting bars -----------------------------


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _register_base(ws: Workspace, link: str) -> dict[str, str]:
    """A cutover-like US registry: AAA's claim ends after TUE, its FMP ISIN; its snapshot."""
    anchor = {"anchor_namespace": "norgate_assetid", "anchor_token": A}

    def claim(provider: str, namespace: str, token: str, end: int | None) -> dict[str, object]:
        return {"instrument": anchor, "provider": provider, "namespace": namespace,
                "token": token, "valid_from_us": UNBOUNDED, "valid_to_us": end,
                "known_from_us": 1, "supersedes_assertion_id": None,
                "source_snapshot_id": "sl:" + link,
                "source_hash": hashlib.sha256(token.encode()).hexdigest()}  # fmt: skip

    document = {
        "schema": "aas-identity-registry-v1",
        "issuers": [],
        "instruments": [{**anchor, "issuer": None, "asset_type": "unclassified", "venue": "XNYS"}],
        "assertions": [
            claim("norgate", "norgate_assetid", A, None),
            claim("eodhd", "eodhd_symbol", "AAA.US", session_start_us(WED)),
            claim("fmp", "isin", ISIN_A, None),
        ],
    }
    register_identities(ws.state, parse_registry(document), apply=True)
    report = snapshot_identities(ws.state, "us-cutover", created_at_us=5, apply=True)
    return {"snapshot_id": str(report["snapshot_id"]), "content_hash": str(report["content_hash"])}


def _bars(ws: Workspace, day: date, *symbols: str) -> dict[str, str]:
    morning = at(f"{(day + timedelta(days=1)).isoformat()}T10:00:00")
    return add_source(ws, [bar(name, day, 10.0, retrieved=morning, currency="USD")
                           for name in symbols], tag=f"us-{day}", linked=morning)  # fmt: skip


def _now() -> int:
    """After every link the test made: a maintenance snapshot is pinned at the run's clock."""
    return _us(datetime.now(tz=UTC))


def _import_list(ws: Workspace, day: date, rows: list[dict[str, object]]) -> str:
    unit = eodhd_unit(*eodhd_job(rows, exchange="US", retrieved=_evening(day)))
    import_unit(ws, unit)
    return unit.content.source_id


def _run(report: dict[str, object]) -> dict[str, object]:
    (run,) = cast("list[dict[str, object]]", report["datasets"])
    return run


def test_list_claims_reach_a_new_snapshot_and_promote_the_rows_they_waited_for(
    ws: Workspace,
) -> None:
    first = _bars(ws, TUE, "AAA.US")
    assert standing_unresolved(ws) == {}  # nothing recorded: the summary is empty
    cutover = _register_base(ws, first["source_id"])
    template = spec([first], cutover, dataset="prices.us.eodhd", decimals=FLOATS,
                    rules={"available_at_us": DAY_RULE, "revision_known_at_us": DAY_RULE},
                    partition={"from": TUE.isoformat(), "to": WED.isoformat()},
                    mapper={"name": "eodhd.bars@1", "args": {"timezone": NEW_YORK}})  # fmt: skip
    assert promote(ws, *template, apply=True)["published"] is True

    # Wednesday's bars arrive after the frozen registry's last claim: held as unresolved.
    waiting = _bars(ws, WED, "AAA.US", "NEW.US")
    held = promote_datasets(ws, apply=True, now_us=10, routes=[ROUTE], identity=cutover)
    assert held["unresolved"] == ["prices.us.eodhd"]
    standing = {"prices.us.eodhd": {"sources": 1, "rows": 2, "rows_unavailable": 0,
                                    "unresolved_tokens": 2, "unpublished_sources": 1,
                                    "oldest_date": WED.isoformat()}}  # fmt: skip
    assert held["standing_unresolved"] == standing
    # A pass that brings no new source still lists what waits; its own list is empty.
    quiet = promote_datasets(ws, apply=True, now_us=11, routes=[ROUTE], identity=cutover)
    assert (quiet["unresolved"], quiet["standing_unresolved"]) == ([], standing)

    # Wednesday's US list names AAA by its registered ISIN; NEW is a new listing.
    _import_list(ws, WED, [_row("AAA", ISIN_A), _row("NEW", isin("000000077", "US"))])
    advanced = maintain_identity.advance(ws, apply=True, now_us=_now())
    assert advanced["new_sources"] == {"eodhd": 0, "kind": 0, "dart": 0}
    us = cast("dict[str, object]", advanced["us_symbols"])
    assert (us["new_sources"], us["claims"]) == (1, 1)
    assert us["unresolved"] == {"list_isin_unregistered": ["NEW.US"]}
    snapshot = cast("dict[str, str]", advanced["snapshot"])
    assert advanced["snapshot_registered"] is True
    assert snapshot["snapshot_id"] != cutover["snapshot_id"]
    assert snapshot["snapshot_id"].startswith("maintain-")

    caught_up = _run(promote_datasets(ws, apply=True, now_us=30, routes=[ROUTE],
                                      identity=snapshot))  # fmt: skip
    sources = cast("list[dict[str, object]]", caught_up["sources"])
    assert [(item["source_id"], item["status"]) for item in sources] == [
        (waiting["source_id"], "unresolved")  # promoted, with NEW still waiting
    ]
    (step,) = cast("list[dict[str, object]]", sources[0]["steps"])
    assert (step["published"], step["unresolved_token_count"]) == (True, 1)
    rows = ws.market.execute(
        "SELECT session_date FROM prices WHERE instrument_id=? ORDER BY 1", (IA,)
    ).fetchall()
    assert rows == [(TUE,), (WED,)]
    assert standing_unresolved(ws)["prices.us.eodhd"] == {
        "sources": 1, "rows": 2, "rows_unavailable": 0, "unresolved_tokens": 1,
        "unpublished_sources": 0, "oldest_date": WED.isoformat()}  # fmt: skip

    # Thursday's list adds a later day: Wednesday's waiting source is not planned again.
    _import_list(ws, THU, [_row("AAA", ISIN_A)])
    later = maintain_identity.advance(ws, apply=True, now_us=_now())
    assert cast("dict[str, object]", later["us_symbols"])["claims"] == 1
    newer = cast("dict[str, str]", later["snapshot"])
    assert newer["snapshot_id"] != snapshot["snapshot_id"]
    settled = _run(promote_datasets(ws, apply=True, now_us=50, routes=[ROUTE], identity=newer))
    assert (settled["status"], settled["sources"]) == ("current", [])
    # Nothing new: the stage reports no new list and pins the same snapshot.
    again = maintain_identity.advance(ws, apply=True, now_us=_now())
    assert cast("dict[str, object]", again["us_symbols"])["registry"] == "no_new_sources"
    assert again["snapshot"] == newer

    # The US build reads the list claims for FMP and SEC rows and never requires their
    # lists: a build of the cutover's own source neither refuses nor reports them withdrawn.
    assert [item.symbol for item in listed_claims(ws.state)] == ["AAA.US", "AAA.US"]
    empty: dict[str, object] = {
        "schema": "aas-identity-registry-v1", "issuers": [], "instruments": [], "assertions": []
    }  # fmt: skip
    build = UsRegistry(empty, {}, {}, {}, frozenset({"sl:" + first["source_id"]}))
    checked = check_registered(build, ws.state)
    assert [row["token"] for row in checked.withdrawn] == ["AAA.US", ISIN_A, A]
