"""The US identity registry mints only from Norgate asset IDs and links only on agreement."""

from __future__ import annotations

import hashlib
import io
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow as pa
import pytest

from aegis_alpha.application import us_identity_report
from aegis_alpha.application.cli import main
from aegis_alpha.storage.identity import (
    decode_registry,
    issuer_link_token,
    mint_instrument,
    mint_issuer,
    parse_registry,
    register_identities,
    snapshot_identities,
)
from aegis_alpha.storage.kr_identity import UNBOUNDED
from aegis_alpha.storage.promotion import formats
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.source_identity import SourceFile
from aegis_alpha.storage.us_identity import (
    UsRegistry,
    assetid_set_sha256,
    build_from_workspace,
    build_us_registry,
    link_instant,
    map_sec_tickers,
    session_start_us,
    us_ticker,
)
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.promotion_support import add_source, at, bar, prices, spec
from tests.storage.us_identity_support import (
    BINDINGS_SCHEMA,
    FMP_RETRIEVED,
    FMP_SCHEMA,
    LINKED,
    MASTER_SCHEMA,
    binding,
    commit,
    cusip,
    fmp_rows,
    import_submissions,
    in_memory,
    linked,
    master,
    members_of,
    profile,
    submissions,
    table,
    us_isin,
)
from tests.storage.us_identity_support import link_instant as recorded_link

APPLE_LIKE = cusip("03783310")
FUND_LIKE = cusip("78462F10")
CIK_A = "0000000101"
CIK_B = "0000000202"
FMP_US = (FMP_RETRIEVED - datetime(1970, 1, 1, tzinfo=UTC)) // datetime.resolution
# A last observed session after every synthetic retrieval, so FMP and SEC rows fall
# inside the ticker claims they are matched by.
OBSERVED = "2027-06-30"


def _assertions(registry: UsRegistry) -> list[dict[str, Any]]:
    return cast("list[dict[str, Any]]", registry.document["assertions"])


def _instruments(registry: UsRegistry) -> list[dict[str, Any]]:
    return cast("list[dict[str, Any]]", registry.document["instruments"])


def _keys(registry: UsRegistry) -> set[tuple[str, str, str]]:
    return {(row["provider"], row["namespace"], row["token"]) for row in _assertions(registry)}


def _instrument(assetid: int) -> str:
    return mint_instrument("norgate_assetid", str(assetid))


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def test_us_instruments_are_minted_from_norgate_asset_ids() -> None:
    rows = [
        master(131684, "AAA"),
        master(255128, "FUND", etf=True),
        master(90001, "OLD-201001", delisted=True),
        master(0, "ZERO"),
        master(90002, "EURO", currency="EUR"),
        master(90003, " PAD"),
        # One symbol on two rows (Norgate keeps symbols unique) keeps only the asset IDs.
        master(90004, "TWICE"),
        master(90005, "TWICE"),
        # One asset ID on two rows registers neither.
        master(90006, "SAME"),
        master(90006, "SAME2"),
    ]
    source = linked(rows)
    registry = build_us_registry(source)
    instruments = _instruments(registry)
    assert [row["anchor_token"] for row in instruments] == [
        "90001",
        "90004",
        "90005",
        "131684",
        "255128",
    ]
    assert registry.unresolved["listings"] == {
        "assetid_repeated": ["90006"],
        "symbol_repeated": ["TWICE"],
    }
    assert {row["anchor_namespace"] for row in instruments} == {"norgate_assetid"}
    assert {row["venue"] for row in instruments} == {"XNYS"}
    assert [row["asset_type"] for row in instruments] == [*["unclassified"] * 4, "etf"]
    assert {row["issuer"] for row in instruments} == {None}
    assert registry.mappers["norgate.master@1"].refused == {
        "assetid_invalid": 1,
        "currency_not_usd": 1,
        "symbol_invalid": 1,
    }
    # A delisted listing keeps Norgate's own claims and is no current ticker.
    assert _keys(registry) == {
        ("norgate", "norgate_assetid", "90001"),
        ("norgate", "norgate_assetid", "90004"),
        ("norgate", "norgate_assetid", "90005"),
        ("norgate", "norgate_assetid", "131684"),
        ("norgate", "norgate_assetid", "255128"),
        ("norgate", "norgate_symbol", "OLD-201001"),
        ("norgate", "norgate_symbol", "AAA"),
        ("norgate", "norgate_symbol", "FUND"),
        ("eodhd", "eodhd_symbol", "AAA.US"),
        ("eodhd", "eodhd_symbol", "FUND.US"),
    }
    hashes = {row_hash for _, row_hash in source.rows.records()}
    # A listed row's ticker claims hold from its first date until the day after the
    # master's last observed session; asset IDs and delisted symbols hold for good.
    assert registry.through == date(2001, 2, 3)
    listed = (session_start_us(date(2001, 2, 3)), session_start_us(date(2001, 2, 4)))
    for row in _assertions(registry):
        always = row["namespace"] == "norgate_assetid" or row["token"].endswith("-201001")
        assert (row["valid_from_us"], row["valid_to_us"]) == (
            (UNBOUNDED, None) if always else listed
        )
        # The master records no retrieval instant: claims are known from the sl: link.
        assert row["known_from_us"] == LINKED
        assert row["source_snapshot_id"] == source.rows.snapshot_id
        assert row["source_hash"] in hashes
    assert registry.symbols == {"AAA.US": _instrument(131684), "FUND.US": _instrument(255128)}
    assert build_us_registry(linked(rows)).raw() == registry.raw()
    assert registry.report()["assetids_sha256"] == assetid_set_sha256(
        ["255128", "90001", "90005", "131684", "90004"]
    )
    # A listing state that is not a boolean is refused, not guessed.
    unknown = build_us_registry(linked([master(1, "AAA"), master(2, "BBB", delisted=None)]))
    assert unknown.mappers["norgate.master@1"].refused == {"listing_state_unknown": 1}
    assert unknown.assetids() == ["1"]


def test_the_asset_id_set_hash_is_pinned() -> None:
    # Independent value: SHA-256 of the bytes ["aas-norgate-assetids-v1",[9,131684,255128]].
    pinned = "3424bffc061c3ab653ccbf2f30521980364943ac27f94869341559ab74f1cba6"
    assert assetid_set_sha256(["9", "131684", "255128"]) == pinned
    assert assetid_set_sha256(["255128", "9", "131684", "9"]) == pinned
    assert assetid_set_sha256({"131684", "255128", "9"}) == pinned


def test_ticker_claims_are_bounded_by_the_master() -> None:
    rows = [
        # XYZ was an earlier security's ticker until 2020-01-15; the current holder lists
        # from 2019-06-03 under another symbol history, so its claims start 2020-01-16.
        master(1, "XYZ-202001", delisted=True, last_date="2020-01-15"),
        master(2, "XYZ-201505", delisted=True, last_date="2015-05-20"),
        master(3, "XYZ", first_date="2019-06-03"),
        # A class ticker spelled with Norgate's '.' matches its delisted holder too.
        master(4, "CLS.A-201801", delisted=True, last_date="2018-01-10"),
        master(5, "CLS.A", first_date="2010-01-04"),
        master(6, "PLAIN", first_date="2026-07-28"),
        master(7, "NODATE", first_date=None),
        master(8, "LOST-201001", delisted=True, last_date="2010-01-29"),
        # A delisted holder with no last date leaves the reuse unbounded.
        {**master(9, "LOST-201002", delisted=True, first_date=None), "last_date": None},
        master(10, "LOST"),
        master(11, "LAST-202607", delisted=True, last_date="2026-07-28"),
        master(12, "LAST"),
    ]
    registry = build_us_registry(
        linked(rows),
        fmp=[fmp_rows([profile("XYZ", CIK_A), profile("NODATE", CIK_A)])],
        sec=[in_memory(submissions([(CIK_A, "Synthetic A Inc", ["XYZ"])]))],
    )
    assert registry.through == date(2026, 7, 28)
    end = session_start_us(date(2026, 7, 29))
    assert registry.intervals == {
        "XYZ.US": (session_start_us(date(2020, 1, 16)), end),
        "CLS-A.US": (session_start_us(date(2018, 1, 11)), end),
        "PLAIN.US": (session_start_us(date(2026, 7, 28)), end),
    }
    assert registry.unresolved["tickers"] == {
        "listing_start_unknown": ["NODATE"],
        "ticker_reuse_unbounded": ["LOST"],
        "ticker_reused": ["LAST"],
    }
    assert registry.symbols["NODATE.US"] == "unresolved:listing_start_unknown"
    claims = {
        (row["provider"], row["namespace"], row["token"]): (
            row["valid_from_us"],
            row["valid_to_us"],
        )
        for row in _assertions(registry)
    }
    assert claims[("eodhd", "eodhd_symbol", "XYZ.US")] == registry.intervals["XYZ.US"]
    # FMP and SEC rows retrieved after through may describe a later holder of the ticker.
    assert registry.unresolved["fmp"] == {
        "fmp_after_master_through": ["XYZ"],
        "not_a_listed_norgate_ticker": ["NODATE"],
    }
    assert registry.unresolved["issuers"] == {
        "sec_after_master_through": ["XYZ"],
        "sec_ticker_missing": ["CLS-A", "PLAIN"],
    }
    assert not {key for key in claims if key[0] in {"fmp", "sec"}}
    assert claims[("norgate", "norgate_symbol", "XYZ")] == registry.intervals["XYZ.US"]
    assert claims[("norgate", "norgate_symbol", "XYZ-202001")] == (UNBOUNDED, None)
    assert not {key for key in claims if key[2] in {"NODATE", "LOST", "LAST", "NODATE.US"}}
    resolved = registry.resolve(
        [
            ("XYZ.US", date(2020, 1, 15), 3),
            ("XYZ.US", date(2020, 1, 16), 2),
            ("XYZ.US", date(2026, 7, 28), 1),
            ("XYZ.US", date(2026, 7, 29), 4),
            ("NODATE.US", date(2026, 7, 28), 1),
            ("MUTUAL.US", date(2026, 7, 28), 5),
        ]
    )
    assert resolved == {
        "rows": 16,
        "resolved_rows": 3,
        "resolved_ratio": 0.1875,
        "symbols": 3,
        "resolved_symbols": 1,
        "unresolved_rows": {
            "after_master_through": 4,
            "before_ticker_claim": 3,
            "listing_start_unknown": 1,
            "not_a_listed_norgate_ticker": 5,
        },
        "unresolved_symbols": {
            "after_master_through": 1,
            "before_ticker_claim": 1,
            "listing_start_unknown": 1,
            "not_a_listed_norgate_ticker": 1,
        },
        "unresolved": {
            "after_master_through": ["XYZ.US"],
            "before_ticker_claim": ["XYZ.US"],
            "listing_start_unknown": ["NODATE.US"],
            "not_a_listed_norgate_ticker": ["MUTUAL.US"],
        },
    }
    assert registry.report()["through"] == "2026-07-28"


def test_provider_symbols_reach_only_a_unique_active_ticker() -> None:
    assert us_ticker("BRK.B") == "BRK-B"
    registry = build_us_registry(
        linked(
            [
                master(1, "BRK.B"),
                # Two listings that spell one ticker: neither is chosen.
                master(2, "XYZ.A"),
                master(3, "XYZ-A"),
                master(4, "GONE-200301", delisted=True, last_date="2003-01-31"),
                master(5, "GONE", first_date="1999-04-01"),
                master(6, "LATE-201006", delisted=True, last_date="2010-06-30"),
            ]
        ),
        fmp=[fmp_rows([profile("XYZ-A", CIK_A), profile("NOPE", CIK_B)])],
    )
    assert registry.symbols == {
        "BRK-B.US": _instrument(1),
        "GONE.US": _instrument(5),
        "XYZ-A.US": "unresolved:ticker_ambiguous",
    }
    assert registry.unresolved["tickers"] == {"ticker_ambiguous": ["XYZ-A"]}
    assert registry.unresolved["fmp"] == {"not_a_listed_norgate_ticker": ["NOPE", "XYZ-A"]}
    assert ("norgate", "norgate_symbol", "XYZ.A") in _keys(registry)
    assert not {key for key in _keys(registry) if key[2].startswith("XYZ") and key[0] != "norgate"}
    day = date(2003, 1, 31)
    resolved = registry.resolve(
        [("BRK-B.US", day, 1), ("XYZ-A.US", day, 1), ("MUTUAL.US", day, 1), ("BRK-B.US", day, 1)]
    )
    assert (resolved["resolved_rows"], resolved["resolved_symbols"]) == (2, 1)
    assert resolved["unresolved_symbols"] == {
        "not_a_listed_norgate_ticker": 1,
        "ticker_ambiguous": 1,
    }
    # GONE was the delisted holder's ticker until 2003-01-31.
    assert registry.intervals["GONE.US"][0] == session_start_us(date(2003, 2, 1))


def test_issuer_needs_sec_and_fmp_to_agree() -> None:
    archive = submissions(
        [
            (CIK_A, "Synthetic A Inc", ["AAA", "AAB", "NOFMP"]),
            (CIK_B, "Synthetic B Corp", ["BBB", "SHARED"]),
            ("0000000303", "Synthetic C Ltd", ["SHARED", "CCC"]),
        ]
    )
    later = datetime(2027, 1, 4, tzinfo=UTC)
    registry = build_us_registry(
        linked(
            [
                master(1, "AAA", last_date=OBSERVED),
                master(2, "AAB"),
                master(3, "BBB"),
                master(4, "SHARED"),
                master(5, "CCC"),
                master(6, "NOSEC"),
                master(7, "NOFMP"),
            ]
        ),
        fmp=[
            fmp_rows(
                [
                    profile("AAA", CIK_A, APPLE_LIKE, retrieved=later),
                    profile("AAB", CIK_A),
                    profile("BBB", CIK_A),
                    profile("SHARED", CIK_B),
                    profile("CCC", None),
                    profile("NOSEC", CIK_B),
                ]
            )
        ],
        sec=[in_memory(archive)],
    )
    issuer = mint_issuer("sec_cik", CIK_A)
    assert registry.document["issuers"] == [
        {"anchor_namespace": "sec_cik", "anchor_token": CIK_A, "name": "Synthetic A Inc"}
    ]
    by_token = {row["anchor_token"]: row["issuer"] for row in _instruments(registry)}
    linked_to = {"anchor_namespace": "sec_cik", "anchor_token": CIK_A}
    assert by_token == {
        "1": linked_to,
        "2": linked_to,
        "3": None,
        "4": None,
        "5": None,
        "6": None,
        "7": None,
    }
    assert registry.unresolved["issuers"] == {
        "fmp_cik_differs": ["BBB"],
        "fmp_cik_missing": ["CCC"],
        "fmp_profile_missing": ["NOFMP"],
        "sec_ticker_ambiguous": ["SHARED"],
        "sec_ticker_missing": ["NOSEC"],
    }
    links = {row["token"]: row for row in _assertions(registry) if row["namespace"] == "issuer"}
    assert set(links) == {
        issuer_link_token(issuer, _instrument(1)),
        issuer_link_token(issuer, _instrument(2)),
    }
    first = links[issuer_link_token(issuer, _instrument(1))]
    sec_source = in_memory(archive)[0].rows
    assert first["provider"] == "sec"
    assert first["source_snapshot_id"] == sec_source.snapshot_id
    assert first["source_hash"] in {row_hash for _, row_hash in sec_source.records()}
    # The link rests on SEC, FMP and Norgate, so it is known from the latest of them.
    assert (
        first["known_from_us"] == (later - datetime(1970, 1, 1, tzinfo=UTC)) // datetime.resolution
    )
    assert first["known_from_us"] > LINKED
    # SEC and FMP state today's ticker-to-CIK mapping, so the link holds from the later of
    # their instants, and an earlier CIK can still be registered as its own link.
    assert (first["valid_from_us"], first["valid_to_us"]) == (first["known_from_us"], None)
    second = links[issuer_link_token(issuer, _instrument(2))]
    assert second["valid_from_us"] == max(FMP_US, LINKED)


def test_sec_members_are_read_through_their_index() -> None:
    archive = submissions(
        [(CIK_A, "Synthetic A Inc", ["AAA"]), ("0000000909", "Shell", [])],
        extra={
            "CIK0000000101-submissions-001.json": b"{}",
            "placeholder.txt": b"x",
            "CIK0000000404.json": json.dumps({"cik": "505", "tickers": ["X"]}).encode(),
        },
    )
    members, source_file, opener = in_memory(archive)
    filers, report = map_sec_tickers(members, source_file, opener)
    assert [(filer.cik, filer.name, filer.tickers) for filer in filers] == [
        (CIK_A, "Synthetic A Inc", ("AAA",))
    ]
    assert report.json() == {
        "rows": 5,
        "accepted": 1,
        "skipped": {"no_ticker": 1, "not_a_cik_document": 2},
        "refused": {"cik_differs": 1},
    }
    # Archive bytes other than the ones the source names are refused.
    other = SourceFile(hashlib.sha256(b"other").hexdigest(), len(archive))
    with pytest.raises(ValueError, match="differ from the content source"):
        map_sec_tickers(members, other, opener)
    # A member whose bytes differ from its index row is refused, not reread.
    changed = submissions([(CIK_A, "Synthetic A Inc", ["AAA", "AAC"])])

    @contextmanager
    def swapped() -> Iterator[io.BytesIO]:
        yield io.BytesIO(changed)

    file = SourceFile(hashlib.sha256(changed).hexdigest(), len(changed))
    index = members_of(submissions([(CIK_A, "Synthetic A Inc", ["AAA"])]))
    with pytest.raises(ValueError, match="differs from its index row"):
        map_sec_tickers(type(members)(index, LINKED), file, swapped)


def test_fmp_disagreement_and_shared_identifiers_stay_unresolved() -> None:
    shared = cusip("11111111")
    registry = build_us_registry(
        linked(
            [
                master(1, "AAA", last_date=OBSERVED),
                master(2, "TWO"),
                master(3, "EUR"),
                master(4, "FUND", etf=True),
                master(5, "BADCHK"),
                master(6, "SH1"),
                master(7, "SH2"),
            ]
        ),
        fmp=[
            fmp_rows(
                [
                    profile("AAA", CIK_A, APPLE_LIKE),
                    profile("TWO", CIK_A, cusip("22222222")),
                    profile("TWO", CIK_B, cusip("22222222")),
                    profile("EUR", CIK_A, currency="EUR"),
                    profile("FUND", None, FUND_LIKE, etf=False),
                    profile("BADCHK", CIK_A, APPLE_LIKE[:-1] + "0", isin="US0000000000"),
                    profile("SH1", CIK_A, shared),
                    profile("SH2", CIK_B, shared),
                ]
            )
        ],
    )
    assert registry.unresolved["fmp"] == {
        "fmp_currency_not_usd": ["EUR"],
        "fmp_identifier_invalid": ["BADCHK"],
        "fmp_profile_ambiguous": ["TWO"],
        "fmp_type_differs": ["FUND"],
    }
    assert registry.unresolved["identifiers"] == {
        "cusip_ambiguous": [shared],
        "isin_ambiguous": [us_isin(shared)],
    }
    fmp = {
        (row["namespace"], row["token"]): row
        for row in _assertions(registry)
        if row["provider"] == "fmp"
    }
    assert set(fmp) == {
        ("fmp_symbol", "AAA"),
        ("cusip", APPLE_LIKE),
        ("isin", us_isin(APPLE_LIKE)),
        ("fmp_symbol", "SH1"),
        ("fmp_symbol", "SH2"),
    }
    # A profile states its identifiers as current when retrieved, not since when.
    assert fmp[("cusip", APPLE_LIKE)]["valid_from_us"] == FMP_US
    assert fmp[("fmp_symbol", "AAA")]["valid_from_us"] == session_start_us(date(2001, 2, 3))
    assert {row["known_from_us"] for row in fmp.values()} == {FMP_US}


def test_us_sources_register_as_one_document(ws: Workspace, tmp_path: Path) -> None:
    master_rows = [master(131684, "AAA", last_date=OBSERVED), master(255128, "FUND", etf=True)]
    master_id = commit(ws, "norgate-master", "observations", table(master_rows, MASTER_SCHEMA))
    fmp_table = table([profile("AAA", CIK_A, APPLE_LIKE)], FMP_SCHEMA)
    fmp_id = commit(ws, "fmp-profiles", "observations", fmp_table)
    bindings = [binding(131684), binding(255128)]
    bindings_id = commit(ws, "bindings", "observations", table(bindings, BINDINGS_SCHEMA))
    archive = submissions([(CIK_A, "Synthetic A Inc", ["AAA"])])
    sec_id = import_submissions(ws, tmp_path / "legacy" / "sec", archive)
    assert sec_id.startswith("sec-submissions-zip-")
    registry = build_from_workspace(
        ws, master=master_id, fmp=[fmp_id], sec=[sec_id], bindings=bindings_id
    )
    assert link_instant(ws.state, master_id) == recorded_link(ws, master_id)
    assert registry.bindings is not None
    assert registry.bindings["equal"] is True
    assert registry.bindings["bound_sha256"] == registry.report()["assetids_sha256"]
    norgate = {
        row["known_from_us"] for row in _assertions(registry) if row["provider"] == "norgate"
    }
    assert norgate == {recorded_link(ws, master_id)}
    report = registry.report()
    assert report["instruments_with_issuer"] == 1
    assert report["assertions"] == {
        "eodhd/eodhd_symbol": 2,
        "fmp/cusip": 1,
        "fmp/fmp_symbol": 1,
        "fmp/isin": 1,
        "norgate/norgate_assetid": 2,
        "norgate/norgate_symbol": 2,
        "sec/issuer": 1,
    }
    document = decode_registry(registry.raw(), expected_file_sha256=registry.sha256())
    plan = register_identities(ws.state, document, apply=False)
    assert plan["missing_count"] == {
        "sources": 0,
        "issuers": 0,
        "instruments": 0,
        "predecessors": 0,
    }
    assert plan["conflict_count"] == 0
    applied = register_identities(ws.state, document, apply=True)
    assert applied["new"] == {"issuers": 1, "instruments": 2, "assertions": 10}
    assert register_identities(ws.state, document, apply=True)["new"] == {
        "issuers": 0,
        "instruments": 0,
        "assertions": 0,
    }
    # A different legacy binding set is reported, not trusted.
    other_id = commit(ws, "bindings-two", "observations", table([binding(7)], BINDINGS_SCHEMA))
    other = build_from_workspace(
        ws, master=master_id, fmp=[fmp_id], sec=[sec_id], bindings=other_id
    )
    assert other.bindings is not None
    assert (other.bindings["equal"], other.bindings["missing"], other.bindings["extra"]) == (
        False,
        ["7"],
        ["131684", "255128"],
    )
    with pytest.raises(ValueError, match="no sl: link"):
        link_instant(ws.state, "market-raw-norgate-unlinked")
    # An FMP source without a link is refused before any document is written.
    with pytest.raises(ValueError, match="fmp-unlinked has no sl: link"):
        build_from_workspace(ws, master=master_id, fmp=["fmp-unlinked"])


def _new_york(document: tuple[bytes, str]) -> tuple[bytes, str]:
    """A promotion spec whose ``eodhd.bars@1`` sessions start in New York."""
    body = json.loads(document[0])
    body["mapper"]["args"]["timezone"] = "America/New_York"
    raw = json.dumps(body, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def test_us_registry_resolves_eodhd_bars_in_promotion(ws: Workspace) -> None:
    rows = [
        master(131684, "AAA"),
        master(5, "REUSE-202001", delisted=True, last_date="2020-01-15"),
        master(6, "REUSE"),
        # The master's last observed session.
        master(7, "NEWCO", first_date="2026-09-08"),
    ]
    master_id = commit(ws, "norgate-master", "observations", table(rows, MASTER_SCHEMA))
    registry = build_from_workspace(ws, master=master_id)
    register_identities(
        ws.state,
        decode_registry(registry.raw(), expected_file_sha256=registry.sha256()),
        apply=True,
    )
    snapshot = snapshot_identities(ws.state, "us", created_at_us=5, apply=True)
    identity = {
        "snapshot_id": str(snapshot["snapshot_id"]),
        "content_hash": str(snapshot["content_hash"]),
    }
    late = at("2026-09-10T00:00:00")
    pin = add_source(
        ws,
        [
            bar("AAA.US", date(2026, 9, 8), 12.5, retrieved=late, currency="USD"),
            bar("MUTUAL.US", date(2026, 9, 8), 10.0, retrieved=late, currency="USD"),
            # After the master's last session nothing says who holds the ticker.
            bar("AAA.US", date(2026, 9, 9), 12.75, retrieved=late, currency="USD"),
            # Before the delisted holder's last session REUSE was its ticker.
            bar("REUSE.US", date(2020, 1, 15), 3.0, retrieved=late, currency="USD"),
            bar("REUSE.US", date(2020, 1, 16), 3.25, retrieved=late, currency="USD"),
        ],
        tag="us",
    )
    exact = dict.fromkeys(("open", "high", "low", "close", "volume"), "exact@1")
    document = _new_york(spec([pin], identity, dataset="prices.us.eodhd", decimals=exact))
    applied = promote(ws, *document, apply=True)
    assert applied["rows"] == {"ok": 2, "unresolved": 3}
    assert applied["unresolved_tokens"] == ["AAA.US", "MUTUAL.US", "REUSE.US"]
    published = prices(ws, str(applied["generation_id"]))
    assert sorted((row["instrument_id"], str(row["session_date"])) for row in published) == sorted(
        [(_instrument(131684), "2026-09-08"), (_instrument(6), "2020-01-16")]
    )


def _register(ws: Workspace, registry: UsRegistry) -> dict[str, object]:
    document = decode_registry(registry.raw(), expected_file_sha256=registry.sha256())
    return register_identities(ws.state, document, apply=True)


def test_a_us_build_reads_every_registered_us_source(ws: Workspace) -> None:
    master_id = commit(
        ws,
        "norgate-master",
        "observations",
        table([master(1, "AAA", last_date=OBSERVED)], MASTER_SCHEMA),
    )
    first = commit(ws, "fmp-one", "observations", table([profile("AAA", CIK_A)], FMP_SCHEMA))
    _register(ws, build_from_workspace(ws, master=master_id, fmp=[first]))
    # A build that leaves out a source registered claims cite is refused, naming it.
    with pytest.raises(ValueError, match=first):
        build_from_workspace(ws, master=master_id)
    other_master = commit(
        ws,
        "norgate-master-two",
        "observations",
        table([master(1, "AAA", last_date=OBSERVED)], MASTER_SCHEMA),
    )
    with pytest.raises(ValueError, match=master_id):
        build_from_workspace(ws, master=other_master, fmp=[first])
    # A later profile that disagrees makes the symbol ambiguous; the registered claim is
    # reported withdrawn, never closed by the builder.
    later = commit(ws, "fmp-two", "observations", table([profile("AAA", CIK_B)], FMP_SCHEMA))
    cumulative = build_from_workspace(ws, master=master_id, fmp=[first, later])
    assert cumulative.unresolved["fmp"] == {"fmp_profile_ambiguous": ["AAA"]}
    assert [(row["provider"], row["token"]) for row in cumulative.withdrawn] == [("fmp", "AAA")]
    assert _register(ws, cumulative)["new"] == {"issuers": 0, "instruments": 0, "assertions": 0}


def test_us_cli_builds_and_registers(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        master_id = commit(
            workspace,
            "norgate-master",
            "observations",
            table([master(131684, "AAA"), master(9, "OLD-2001", delisted=True)], MASTER_SCHEMA),
        )

    def run(argv: list[str]) -> dict[str, Any]:
        assert main(argv) == 0
        return json.loads(capsys.readouterr().out)

    output, report_path = tmp_path / "registry.json", tmp_path / "report.json"
    build = ["identity", "us-build", "--home", str(home), "--master", master_id]
    built = run([*build, "--output", str(output), "--report", str(report_path)])
    raw = output.read_bytes()
    assert built["sha256"] == hashlib.sha256(raw).hexdigest()
    assert json.loads(report_path.read_text())["assetids_sha256"] == assetid_set_sha256(
        ["131684", "9"]
    )
    assert main([*build, "--output", str(output)]) == 1
    assert "cannot create new file" in capsys.readouterr().err
    register = ["identity", "register", "--home", str(home), "--file", str(output)]
    registered = run([*register, "--sha256", built["sha256"]])
    assert registered["new"] == {"issuers": 0, "instruments": 2, "assertions": 5}


def test_an_fmp_row_without_a_retrieval_instant_refuses_its_symbol() -> None:
    naive = fmp_rows([profile("AAA", CIK_A)])
    (row,) = naive.rows
    index = naive.columns.index("retrieved_at_utc")
    naive_row = (*row[:index], datetime(2026, 8, 29), *row[index + 1 :])  # noqa: DTZ001
    registry = build_us_registry(
        linked([master(1, "AAA", last_date=OBSERVED), master(2, "BBB"), master(3, "CCC")]),
        fmp=[
            fmp_rows([profile("BBB", CIK_B, retrieved=None), profile("CCC", CIK_B)]),
            type(naive)("sl:fmp-naive", naive.columns, (naive_row,)),
        ],
    )
    assert registry.mappers["fmp.profile@1"].refused == {"fmp_retrieved_invalid": 2}
    assert registry.unresolved["fmp"] == {"fmp_retrieved_invalid": ["AAA", "BBB"]}
    assert ("fmp", "fmp_symbol", "CCC") in _keys(registry)


def _sec_case(case: str) -> tuple[Any, SourceFile, Any, str]:
    archive = submissions([(CIK_A, "Synthetic A Inc", ["AAA"])])
    members = in_memory(archive)[0]
    if case == "oversized":
        (row,) = members.rows.rows
        big = (row[0], row[1], 64 * 1024 * 1024 + 1, *row[3:])
        index = type(members.rows)(members.rows.snapshot_id, members.rows.columns, (big,))
        return type(members)(index, LINKED), *in_memory(archive)[1:], "exceeds its bound"
    if case == "missing":
        fuller = submissions([(CIK_A, "Synthetic A Inc", ["AAA"]), (CIK_B, "B", ["BBB"])])
        index = in_memory(fuller)[0]
        return index, *in_memory(archive)[1:], "lacks indexed member"
    raw = b"not a zip archive"

    @contextmanager
    def opener() -> Iterator[io.BytesIO]:
        yield io.BytesIO(raw)

    file = SourceFile(hashlib.sha256(raw).hexdigest(), len(raw))
    return members, file, opener, "not a zip archive"


@pytest.mark.parametrize("case", ["oversized", "missing", "not_zip"])
def test_sec_archives_that_do_not_match_their_index_are_refused(case: str) -> None:
    members, archive, opener, message = _sec_case(case)
    with pytest.raises(ValueError, match=message):
        map_sec_tickers(members, archive, opener)


def test_only_an_sec_submissions_zip_source_is_read(ws: Workspace) -> None:
    master_id = commit(
        ws, "norgate-master", "observations", table([master(1, "AAA")], MASTER_SCHEMA)
    )
    with pytest.raises(ValueError, match="reads an sec-submissions-zip-"):
        build_from_workspace(ws, master=master_id, sec=[master_id])
    # A submissions source whose retained bytes are no zip archive names none to read.
    empty = commit(
        ws,
        "submissions-zip",
        "members",
        table([master(1, "AAA")], MASTER_SCHEMA),
        provider="sec",
    )
    assert empty.startswith("sec-submissions-zip-")
    with pytest.raises(ValueError, match="exactly one zip archive"):
        build_from_workspace(ws, master=master_id, sec=[empty])


def test_a_corrected_registered_claim_is_not_reported_withdrawn(ws: Workspace) -> None:
    master_id = commit(
        ws,
        "norgate-master",
        "observations",
        table([master(1, "AAA", last_date=OBSERVED)], MASTER_SCHEMA),
    )
    first = commit(ws, "fmp-one", "observations", table([profile("AAA", CIK_A)], FMP_SCHEMA))
    registry = build_from_workspace(ws, master=master_id, fmp=[first])
    _register(ws, registry)
    (original,) = (
        row
        for row in parse_registry(registry.document).assertions
        if row["namespace"] == "fmp_symbol"
    )
    correction = {
        key: original[key]
        for key in _assertions(registry)[0]
        if key not in {"instrument", "supersedes_assertion_id"}
    }
    correction |= {
        "instrument": {"anchor_namespace": "norgate_assetid", "anchor_token": "1"},
        "valid_to_us": session_start_us(date(2026, 7, 1)),
        "known_from_us": max(recorded_link(ws, first), FMP_US) + 1,
        "supersedes_assertion_id": original["assertion_id"],
    }
    document = {
        "schema": registry.document["schema"],
        "issuers": [],
        "instruments": [],
        "assertions": [correction],
    }
    raw = formats.canonical(document)
    corrected = register_identities(
        ws.state,
        decode_registry(raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()),
        apply=True,
    )
    assert corrected["new"] == {"issuers": 0, "instruments": 0, "assertions": 1}
    (correction_id,) = (row["assertion_id"] for row in parse_registry(document).assertions)
    # FMP now disagrees: neither the original (already corrected) nor its correction is
    # built, and only the correction, which nothing corrects, is reported withdrawn.
    later = commit(ws, "fmp-two", "observations", table([profile("AAA", CIK_B)], FMP_SCHEMA))
    cumulative = build_from_workspace(ws, master=master_id, fmp=[first, later])
    assert [row["assertion_id"] for row in cumulative.withdrawn] == [correction_id]


def test_the_report_script_resolves_bulk_and_quarantined_us_rows(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    rows = [master(131684, "AAA"), master(7, "NEWCO", first_date="2026-09-08")]
    late = at("2026-09-10T00:00:00")

    def quarantined(code: str, day: str, exchange: str = "US") -> dict[str, object]:
        row = {"code": code, "date": day, "exchange_short_name": exchange, "close": 1.0}
        return {"ordinal": 0, "reason": "explicit instrument identity is required"} | {
            "source_row_json": json.dumps(row)
        }

    quarantine_schema = pa.schema(
        [("ordinal", pa.int64()), ("reason", pa.string()), ("source_row_json", pa.string())]
    )
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        master_id = commit(workspace, "norgate-master", "observations", table(rows, MASTER_SCHEMA))
        bars = add_source(
            workspace,
            [
                bar("AAA.US", date(2026, 9, 8), 1.0, retrieved=late, currency="USD"),
                bar("AAA.US", date(2026, 9, 9), 1.0, retrieved=late, currency="USD"),
                bar("AAA.KO", date(2026, 9, 8), 1.0, retrieved=late),
            ],
            tag="us",
        )
        held = [
            quarantined("NEWCO", "2026-09-08"),
            quarantined("NEWCO", "2026-09-08"),
            quarantined("MUTUAL", "2026-09-08"),
            quarantined("NEWCO", "2026-09-08", exchange="KO"),
        ]
        quarantine_id = commit(
            workspace, "bulk-quarantine", "quarantine", table(held, quarantine_schema)
        )
        market = workspace.paths.market
    output = tmp_path / "report.json"
    argv = [
        "--market", str(market), "--master", master_id, "--bars", bars["source_id"],
        "--quarantine", quarantine_id, "--output", str(output),
    ]  # fmt: skip
    assert us_identity_report.main(argv) == 0
    report = json.loads(output.read_text())
    assert (report["through"], report["eodhd_symbols"]) == ("2026-09-08", 2)
    assert report["bars"]["rows"]["unresolved_rows"] == {"after_master_through": 1}
    assert report["bars"]["rows"]["resolved_rows"] == 1
    quarantine = report["quarantine"]
    # Two copies of one NEWCO row resolve as two rows and one distinct key.
    assert (quarantine["tables"], quarantine["rows"]["rows"]) == (1, 3)
    assert (quarantine["rows"]["resolved_rows"], quarantine["distinct_keys"]["resolved_rows"]) == (
        2,
        1,
    )
    assert quarantine["rows"]["unresolved"] == {"not_a_listed_norgate_ticker": ["MUTUAL.US"]}


def test_the_report_reads_the_master_as_us_build_does(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    rows = [
        master(131684, "AAA", last_date=OBSERVED),
        master(2, "XYZ-202001", delisted=True, last_date="2020-01-15"),
        master(3, "XYZ", first_date="2019-06-03"),
        master(4, "NODATE", first_date=None),
    ]
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        master_id = commit(workspace, "norgate-master", "observations", table(rows, MASTER_SCHEMA))
        built = build_from_workspace(workspace, master=master_id)
        market = workspace.paths.market
    with duckdb.connect(str(market), read_only=True) as connection:
        reported = build_us_registry(us_identity_report.read_master(connection, master_id))
    assert built.through == date(2027, 6, 30)
    assert (reported.symbols, reported.intervals, reported.through) == (
        built.symbols,
        built.intervals,
        built.through,
    )
    assert reported.unresolved == built.unresolved
