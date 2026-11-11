"""US instruments anchored on the ISIN of a dated EODHD US symbol list (#149, #100).

A new listing whose list row carries a valid, unregistered USD ISIN and whose symbol no
registered or accepted claim held before mints ``mint_instrument('us_isin', Isin)``; a row
without an ISIN stays unresolved. A Norgate instrument and the ISIN instrument of the same
listing end up as one instrument whichever is registered first. Lists, masters, asset IDs
and ISINs are synthetic; the ISINs only carry valid check digits.
"""

# ruff: noqa: PLR2004 -- synthetic counts are the expected values
from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.storage import maintain_identity
from aegis_alpha.storage.identity import (
    decode_registry,
    mint_instrument,
    register_identities,
    snapshot_identities,
)
from aegis_alpha.storage.kr_identity import SourceRows, eodhd_unit, import_unit
from aegis_alpha.storage.maintain_promotion import Route, promote_datasets, standing_unresolved
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.us_identity import (
    UsRegistry,
    build_from_workspace,
    session_start_us,
)
from aegis_alpha.storage.us_symbols import Held, Registry, build_symbol_claims
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.kr_identity_support import eodhd_job, isin, symbol
from tests.storage.promotion_support import add_source, at, bar, spec, us
from tests.storage.us_identity_support import (
    FMP_SCHEMA,
    MASTER_SCHEMA,
    commit,
    master,
    profile,
    table,
)

TUE, WED, THU, FRI = date(2026, 9, 8), date(2026, 9, 9), date(2026, 9, 10), date(2026, 9, 11)
X, Y, Z = isin("000000071", "US"), isin("000000072", "US"), isin("000000073", "US")
A = "900001"
IA = mint_instrument("norgate_assetid", A)
NEW_YORK = "America/New_York"
DAY_RULE = {"rule": "local_day_end@1", "basis": "record", "input": "session_date",
            "args": {"timezone": NEW_YORK}}  # fmt: skip
FLOATS = dict.fromkeys(("open", "high", "low", "close", "volume"), "float_shortest@1")
ROUTE = Route("prices.us.eodhd", "eodhd.bars@1", "synthetic-kr-bars-", "bars",
              "provider_symbol LIKE '%.US'")  # fmt: skip


def _isin_instrument(value: str) -> str:
    return mint_instrument("us_isin", value)


def _evening(day: date) -> str:
    return f"{day.isoformat()}T22:00:00Z"


def _row(code: str, value: str | None, kind: str = "Common Stock") -> dict[str, object]:
    return symbol(code, value, exchange="NYSE", kind=kind, currency="USD")


def _list(day: date, rows: list[dict[str, object]]) -> SourceRows:
    return eodhd_unit(*eodhd_job(rows, exchange="US", retrieved=_evening(day))).source_rows()


def _day(token: str, instrument: str, first: date, last: date | None = None) -> tuple[str, ...]:
    end = (last or first) + timedelta(days=1)
    return (token, instrument, str(session_start_us(first)), str(session_start_us(end)))


def _symbol_claims(document: dict[str, object]) -> list[tuple[str, ...]]:
    found = []
    for row in cast("list[dict[str, object]]", document["assertions"]):
        if row["namespace"] != "eodhd_symbol":
            continue
        anchor = cast("dict[str, str]", row["instrument"])
        found.append((str(row["token"]),
                      mint_instrument(anchor["anchor_namespace"], anchor["anchor_token"]),
                      str(row["valid_from_us"]), str(row["valid_to_us"])))  # fmt: skip
    return sorted(found)


def test_a_new_listing_is_registered_on_its_isin_and_a_row_without_one_stays_unresolved() -> None:
    lists = [
        # NEW and its ISIN X appear for the first time; NOI has no ISIN; WRT is a warrant.
        _list(WED, [_row("NEW", X), _row("NOI", None), _row("WRT", Z, "Warrant")]),
        _list(THU, [_row("NEW", X), _row("NOI", None)]),
        # X changes ticker to RNM; NEW is reused by another security (ISIN Y).
        _list(FRI, [_row("RNM", X), _row("NEW", Y)]),
    ]
    built = build_symbol_claims(lists, Registry((), {}, {}))
    document = built.document()
    instruments = cast("list[dict[str, object]]", document["instruments"])
    assert [(row["anchor_namespace"], row["anchor_token"], row["asset_type"], row["venue"])
            for row in instruments] == [("us_isin", X, "unclassified", "XNYS"),
                                        ("us_isin", Y, "unclassified", "XNYS")]  # fmt: skip
    anchors = {(str(row["provider"]), str(row["namespace"]), str(row["token"]))
               for row in cast("list[dict[str, object]]", document["assertions"])
               if row["namespace"] == "isin"}  # fmt: skip
    assert anchors == {("eodhd", "isin", X), ("eodhd", "isin", Y)}
    ix, iy = _isin_instrument(X), _isin_instrument(Y)
    # Same ISIN, other ticker: the same instrument. Same ticker, other ISIN: another one.
    assert _symbol_claims(document) == sorted([
        _day("NEW.US", ix, WED), _day("NEW.US", ix, THU), _day("NEW.US", iy, FRI),
        _day("RNM.US", ix, FRI)])  # fmt: skip
    report = built.report()
    assert report["registered_instruments"] == 2
    assert report["unresolved"] == {"list_isin_missing": ["NOI.US"],
                                    "list_isin_unregistered": ["WRT.US"]}  # fmt: skip
    assert report["unresolved_dispositions"] == {
        "out_of_scope": {"list_isin_unregistered": 1},
        "unresolved": {"list_isin_missing": 2},
    }


def test_a_new_isin_is_not_registered_over_a_symbol_whose_holder_has_no_isin() -> None:
    # A Norgate instrument without a registered ISIN held AAA through Tuesday: Wednesday's
    # AAA with an unregistered ISIN may be the same security, so nothing is minted.
    held = Held("AAA.US", IA, session_start_us(date(2001, 2, 5)), session_start_us(WED),
                listed=False)  # fmt: skip
    built = build_symbol_claims([_list(WED, [_row("AAA", X)])], Registry((held,), {}, {IA: A}))
    assert (built.assertions, built.report()["registered_instruments"]) == ([], 0)
    assert built.report()["unresolved_dispositions"] == {"register": {"list_isin_unregistered": 1}}


def test_an_isin_registered_later_in_the_build_withdraws_a_bridge_it_contradicts() -> None:
    # Norgate's A held AAA through Monday and is registered with ISIN W. Tuesday's AAA
    # carries the unregistered X, Wednesday's AAA carries W, and Thursday's NEW carries X,
    # which mints X's instrument: Tuesday's AAA then names that instrument, not A.
    w = isin("000000074", "US")
    held = Held("AAA.US", IA, session_start_us(date(2001, 2, 5)), session_start_us(TUE),
                listed=False)  # fmt: skip
    lists = [_list(TUE, [_row("AAA", X)]), _list(WED, [_row("AAA", w)]),
             _list(THU, [_row("NEW", X)]), _list(FRI, [_row("AAA", w)])]  # fmt: skip
    built = build_symbol_claims(lists, Registry((held,), {w: frozenset({IA})}, {IA: A}))
    # Only Thursday's new listing is claimed; no claim of A covers Tuesday.
    assert _symbol_claims(built.document()) == [_day("NEW.US", _isin_instrument(X), THU)]
    report = built.report()
    assert (report["bridges"], report["registered_instruments"]) == ([], 1)
    assert report["bridge_unresolved"] == {"list_contradicted": 1}
    assert report["unresolved_days"] == {"list_contradicted": 2, "list_isin_unregistered": 1}


def test_an_isin_joined_to_a_norgate_instrument_later_withdraws_a_bridge_it_contradicts() -> None:
    # As above, but Thursday's X is carried by BBB, which Norgate's B (no ISIN) holds that
    # day: X joins B, so Tuesday's AAA names B, not A.
    w, b = isin("000000074", "US"), "900002"
    ib = mint_instrument("norgate_assetid", b)
    held = (Held("AAA.US", IA, session_start_us(date(2001, 2, 5)), session_start_us(TUE),
                 listed=False),
            Held("BBB.US", ib, session_start_us(date(2001, 2, 5)), None, listed=False))  # fmt: skip
    lists = [_list(TUE, [_row("AAA", X)]), _list(WED, [_row("AAA", w)]),
             _list(THU, [_row("BBB", X)])]  # fmt: skip
    built = build_symbol_claims(lists, Registry(held, {w: frozenset({IA})}, {IA: A, ib: b}))
    assert (built.assertions, built.attached) == ([], 1)
    assert built.report()["bridge_unresolved"] == {"list_contradicted": 1}


# --- maintenance: the registry, both orders, and the standing summary ---------------------


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _import_list(ws: Workspace, day: date, rows: list[dict[str, object]]) -> None:
    import_unit(ws, eodhd_unit(*eodhd_job(rows, exchange="US", retrieved=_evening(day))))


def _advance(ws: Workspace) -> dict[str, object]:
    # A run's clock is after every link the test made, as in production.
    report = maintain_identity.advance(ws, apply=True, now_us=us(datetime.now(tz=UTC)))
    return cast("dict[str, object]", report["us_symbols"])


def _register(ws: Workspace, registry: UsRegistry) -> dict[str, object]:
    document = decode_registry(registry.raw(), expected_file_sha256=registry.sha256())
    return register_identities(ws.state, document, apply=True)


def _master(ws: Workspace) -> str:
    """A Norgate master listing A as AAA from 2001 through Thursday 09-10."""
    rows = [master(int(A), "AAA", first_date="2001-02-05", last_date=THU.isoformat())]
    return commit(ws, "norgate-master", "observations", table(rows, MASTER_SCHEMA))


def _holders(ws: Workspace, namespace: str, token: str) -> set[str]:
    return {str(row[0]) for row in ws.state.execute(
        "SELECT instrument_id FROM identity_assertions WHERE namespace=? AND token=?",
        (namespace, token))}  # fmt: skip


def _one_instrument(ws: Workspace) -> str:
    """The one instrument holding asset ID A, ISIN X and AAA's claims; nothing else."""
    (instrument,) = _holders(ws, "norgate_assetid", A)
    assert _holders(ws, "isin", X) == {instrument}
    assert _holders(ws, "eodhd_symbol", "AAA.US") == {instrument}
    count = ws.state.execute("SELECT count(*) FROM instruments").fetchone()[0]
    assert count == 1
    return instrument


def test_norgate_first_then_the_isin_joins_the_norgate_instrument(ws: Workspace) -> None:
    assert _register(ws, build_from_workspace(ws, master=_master(ws)))["new"] == {
        "issuers": 0, "instruments": 1, "assertions": 3}  # fmt: skip
    # Wednesday is inside the master's claim of AAA: the list's ISIN joins that instrument.
    _import_list(ws, WED, [_row("AAA", X)])
    wednesday = _advance(ws)
    assert (wednesday["claims"], wednesday["attached_isins"]) == (0, 1)
    # Friday is after the master: AAA continues the same instrument by its ISIN.
    _import_list(ws, FRI, [_row("AAA", X)])
    friday = _advance(ws)
    assert (friday["claims"], friday["registered_instruments"]) == (1, 0)
    assert _one_instrument(ws) == IA


def test_a_list_isin_does_not_join_a_norgate_instrument_of_another_asset_type(
    ws: Workspace,
) -> None:
    # Norgate says A is an ETF; Wednesday's AAA row carrying X is common stock.
    rows = [master(int(A), "AAA", first_date="2001-02-05", last_date=THU.isoformat(), etf=True)]
    _register(ws, build_from_workspace(ws, master=commit(
        ws, "norgate-master", "observations", table(rows, MASTER_SCHEMA))))  # fmt: skip
    _import_list(ws, WED, [_row("AAA", X)])
    wednesday = _advance(ws)
    assert (wednesday["attached_isins"], wednesday["registered_instruments"]) == (0, 0)
    assert wednesday["unresolved_days"] == {"list_isin_type_differs": 1}
    assert _holders(ws, "isin", X) == set()


def test_isin_first_then_a_norgate_asset_id_joins_the_isin_instrument(ws: Workspace) -> None:
    _import_list(ws, WED, [_row("AAA", X)])
    assert _advance(ws)["registered_instruments"] == 1
    _import_list(ws, THU, [_row("AAA", X)])
    assert _advance(ws)["claims"] == 1
    # A later Norgate master (a renewed subscription) lists A as AAA over those days.
    master_id = _master(ws)
    built = build_from_workspace(ws, master=master_id)
    assert built.report()["merged_into_isin"] == 1
    assert _register(ws, built)["new"] == {"issuers": 0, "instruments": 0, "assertions": 3}
    instrument = _one_instrument(ws)
    assert instrument == _isin_instrument(X)
    # Norgate's claim of AAA stops where the list claims begin: no overlap, no gap.
    claims = sorted((int(row[0]), int(row[1])) for row in ws.state.execute(
        "SELECT valid_from_us, valid_to_us FROM identity_assertions "
        "WHERE token='AAA.US'"))  # fmt: skip
    assert claims == [(session_start_us(date(2001, 2, 5)), session_start_us(WED)),
                      (session_start_us(WED), session_start_us(THU)),
                      (session_start_us(THU), session_start_us(FRI))]  # fmt: skip
    # The build is cumulative: rebuilding registers nothing new.
    assert _register(ws, build_from_workspace(ws, master=master_id))["new"] == {
        "issuers": 0, "instruments": 0, "assertions": 0}  # fmt: skip


@pytest.mark.parametrize(
    ("fmp_isin", "etf"),
    [(Y, None), (None, True)],
    ids=["fmp_isin_differs", "asset_type_differs"],
)
def test_a_listing_whose_independent_identifiers_differ_is_not_merged_into_an_isin_instrument(
    ws: Workspace, fmp_isin: str | None, *, etf: bool | None
) -> None:
    # The provider's list registers X's instrument over AAA first. A later Norgate master
    # lists A as AAA over those days, but FMP says AAA is Y, or Norgate says A is an ETF
    # where the list's rows were common stock: A is left out, not asserted on X's instrument.
    _import_list(ws, WED, [_row("AAA", X)])
    _advance(ws)
    _import_list(ws, THU, [_row("AAA", X)])
    _advance(ws)
    rows = [master(int(A), "AAA", first_date="2001-02-05", last_date=THU.isoformat(), etf=etf)]
    master_id = commit(ws, "norgate-master", "observations", table(rows, MASTER_SCHEMA))
    profiles = [profile("AAA", None, isin=fmp_isin, etf=bool(etf))]
    fmp_id = commit(ws, "fmp-profiles", "observations", table(profiles, FMP_SCHEMA))
    built = build_from_workspace(ws, master=master_id, fmp=[fmp_id])
    report = built.report()
    assert report["merged_into_isin"] == 0
    unresolved = cast("dict[str, dict[str, list[str]]]", report["unresolved"])
    assert unresolved["listings"] == {"isin_identifier_differs": [A]}
    assert _register(ws, built)["new"] == {"issuers": 0, "instruments": 0, "assertions": 0}
    assert _holders(ws, "norgate_assetid", A) == set()
    assert _holders(ws, "isin", Y) == set()
    assert _holders(ws, "eodhd_symbol", "AAA.US") == {_isin_instrument(X)}


def test_the_standing_summary_moves_a_new_listing_from_claimable_to_resolved(
    ws: Workspace,
) -> None:
    first = add_source(
        ws,
        [bar("OLD.US", TUE, 10.0, retrieved=at("2026-09-09T10:00:00"), currency="USD")],
        tag="tue",
        linked=at("2026-09-09T10:00:00"),
    )
    # OLD's Norgate claim ends after Tuesday; NEW and NOI never had one.
    _register(ws, build_from_workspace(ws, master=commit(
        ws, "norgate-master", "observations",
        table([master(int(A), "OLD", first_date="2001-02-05", last_date=TUE.isoformat())],
              MASTER_SCHEMA))))  # fmt: skip
    pin = snapshot_identities(ws.state, "us-cutover", created_at_us=5, apply=True)
    cutover = {"snapshot_id": str(pin["snapshot_id"]), "content_hash": str(pin["content_hash"])}
    template = spec([first], cutover, dataset="prices.us.eodhd", decimals=FLOATS,
                    rules={"available_at_us": DAY_RULE, "revision_known_at_us": DAY_RULE},
                    partition={"from": TUE.isoformat(), "to": WED.isoformat()},
                    mapper={"name": "eodhd.bars@1", "args": {"timezone": NEW_YORK}})  # fmt: skip
    assert promote(ws, *template, apply=True)["published"] is True
    morning = at("2026-09-10T10:00:00")
    add_source(ws, [bar(name, WED, 10.0, retrieved=morning, currency="USD")
                    for name in ("NEW.US", "NOI.US")], tag="wed", linked=morning)  # fmt: skip
    promote_datasets(ws, apply=True, now_us=10, routes=[ROUTE], identity=cutover)
    _import_list(ws, WED, [_row("NEW", X), _row("NOI", None)])

    def reasons() -> dict[str, int]:
        entry = standing_unresolved(ws)["prices.us.eodhd"]
        return cast("dict[str, int]", entry["rows_by_reason"])

    # Judged now, NEW's day would be registered on its ISIN; NOI's row has none.
    assert reasons() == {"list_claimable": 1, "list_isin_missing": 1}
    _advance(ws)
    assert reasons() == {"list_isin_missing": 1, "resolved": 1}
    assert _holders(ws, "eodhd_symbol", "NEW.US") == {_isin_instrument(X)}
    assert _holders(ws, "eodhd_symbol", "NOI.US") == set()
