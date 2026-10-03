"""The KR identity registry mints instruments only from KR ISINs and leaves ambiguity open."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from aegis_alpha.application import kr_identity_report
from aegis_alpha.application.cli import main
from aegis_alpha.storage.identity import (
    decode_registry,
    issuer_link_token,
    mint_instrument,
    mint_issuer,
    register_identities,
    snapshot_identities,
)
from aegis_alpha.storage.kr_identity import (
    UNBOUNDED,
    KrRegistry,
    SourceRows,
    build_from_workspace,
    build_kr_registry,
    eodhd_unit,
    import_unit,
    instant_us,
    kind_unit,
    read_eodhd_job,
    read_kind_receipt,
)
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.kr_identity_support import (
    DART_RETRIEVED,
    KIND_RETRIEVED,
    RETRIEVED,
    bad_check_digit,
    commit_dart,
    corp_code_xml,
    dart_rows,
    dart_source,
    eodhd_job,
    isin,
    kind_listing,
    symbol,
    write_eodhd_job,
    write_kind_listing,
)
from tests.storage.promotion_support import add_source, at, bar, prices, spec

SAMSUNG_LIKE = isin("710000100")
PREFERRED = isin("710000150")
FUND = isin("740000100")
KOSDAQ = isin("720000100")


def _rows(*jobs: tuple[bytes, dict[str, bytes]]) -> list[SourceRows]:
    return [eodhd_unit(*job).source_rows() for job in jobs]


def _assertions(document: dict[str, object]) -> list[dict[str, Any]]:
    return cast("list[dict[str, Any]]", document["assertions"])


def _keys(document: dict[str, object]) -> set[tuple[str, str, str]]:
    return {(row["provider"], row["namespace"], row["token"]) for row in _assertions(document)}


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def test_kr_instruments_are_minted_from_isin_never_from_a_code() -> None:
    job = eodhd_job(
        [
            symbol("100010", SAMSUNG_LIKE),
            symbol("100015", PREFERRED),
            symbol("400010", FUND, kind="ETF"),
        ]
    )
    registry = build_kr_registry(_rows(job))
    instruments = cast("list[dict[str, Any]]", registry.document["instruments"])
    assert [row["anchor_token"] for row in instruments] == sorted([SAMSUNG_LIKE, PREFERRED, FUND])
    assert {row["anchor_namespace"] for row in instruments} == {"krx_isin"}
    # EODHD types a KRX preferred share "Common Stock", so no share class is asserted.
    assert {row["asset_type"] for row in instruments} == {"unclassified", "etf"}
    assert {row["venue"] for row in instruments} == {"XKRX"}
    assert registry.symbols["100010.KO"] == mint_instrument("krx_isin", SAMSUNG_LIKE)
    assert _keys(registry.document) == {
        ("eodhd", "eodhd_symbol", "100010.KO"),
        ("eodhd", "eodhd_symbol", "100015.KO"),
        ("eodhd", "eodhd_symbol", "400010.KO"),
        ("eodhd", "krx_short_code", "100010"),
        ("eodhd", "krx_short_code", "100015"),
        ("eodhd", "krx_short_code", "400010"),
    }
    source = _rows(job)[0]
    for row in _assertions(registry.document):
        # The symbol list carries no dates: the claim covers the provider's whole series and
        # is known from the instant the list was retrieved.
        assert (row["valid_from_us"], row["valid_to_us"]) == (UNBOUNDED, None)
        assert row["known_from_us"] == instant_us(RETRIEVED)
        assert row["source_snapshot_id"] == source.snapshot_id
    hashes = {hash_ for _, hash_ in source.records()}
    assert {row["source_hash"] for row in _assertions(registry.document)} <= hashes
    # Building again from the same rows gives the same bytes.
    assert build_kr_registry(_rows(job)).raw() == registry.raw()


def test_missing_invalid_and_ambiguous_isins_stay_unresolved() -> None:
    shared = isin("730000100")
    twice = isin("730000200")
    job = eodhd_job(
        [
            symbol("100010", SAMSUNG_LIKE),
            symbol("200010", None),
            symbol("200020", bad_check_digit(isin("730000300"))),
            symbol("200030", isin("730000600"), currency="USD"),
            symbol("200040", isin("730000400"), kind="Warrant"),
            # One ISIN claimed by two short codes: neither symbol is trusted.
            symbol("300010", shared),
            symbol("300020", shared),
            # One symbol listed with two ISINs (active list vs delisted list).
            symbol("300030", twice),
        ]
    )
    delisted = eodhd_job(
        [symbol("300030", isin("730000500")), symbol("100010", None)],
        delisted="1",
    )
    # The same short code on KOSDAQ names another ISIN, so the code itself is ambiguous.
    kosdaq = eodhd_job([symbol("100010", KOSDAQ, exchange="KQ")], exchange="KQ")
    registry = build_kr_registry(_rows(job, delisted, kosdaq))
    assert registry.unresolved["symbols"] == {
        "isin_ambiguous": ["300010.KO", "300020.KO"],
        "isin_invalid": ["200020.KO"],
        "isin_missing": ["200010.KO"],
        "currency_not_krw": ["200030.KO"],
        "symbol_ambiguous": ["300030.KO"],
        "type_unknown": ["200040.KO"],
    }
    assert registry.unresolved["isins"] == {"isin_ambiguous": [shared]}
    # 300030 names two ISINs across its lists, so the code is ambiguous as well.
    assert registry.unresolved["short_codes"] == {"short_code_ambiguous": ["100010", "300030"]}
    # A null ISIN on another list does not undo the one list that names it.
    assert registry.symbols["100010.KO"] == mint_instrument("krx_isin", SAMSUNG_LIKE)
    assert registry.symbols["100010.KQ"] == mint_instrument("krx_isin", KOSDAQ)
    assert _keys(registry.document) == {
        ("eodhd", "eodhd_symbol", "100010.KO"),
        ("eodhd", "eodhd_symbol", "100010.KQ"),
    }
    resolution = registry.resolve(["100010.KO", "200010.KO", "999999.KO"])
    assert resolution["resolved"] == 1
    assert resolution["unresolved"] == {
        "isin_missing": ["200010.KO"],
        "not_in_symbol_list": ["999999.KO"],
    }


def test_kind_and_dart_reach_an_instrument_only_through_one_isin() -> None:
    job = eodhd_job(
        [
            symbol("100010", SAMSUNG_LIKE),
            symbol("100015", PREFERRED),
            symbol("400010", FUND, kind="ETF"),
            symbol("500010", None),
        ]
    )
    kind = kind_unit(
        *kind_listing(
            [
                ("합성전자", "100010", "1975-06-11"),
                ("합성우선", "100015", "1989-09-25"),
                ("새상장", "500010", "2026-08-25"),
            ]
        )
    )
    dart = dart_source(
        dart_rows(
            corp_code_xml(
                [
                    ("00100010", "합성전자", "100010"),
                    ("00900000", "비상장", ""),
                    ("00500010", "새상장", "500010"),
                    ("00400010", "합성운용", "400010"),
                    ("00600010", "같은코드가", "600010"),
                    ("00600011", "같은코드나", "600010"),
                ]
            )
        )
    )
    registry = build_kr_registry(_rows(job), [kind.source_rows()], [dart])
    issuer = mint_issuer("dart_corp_code", "00100010")
    instrument = mint_instrument("krx_isin", SAMSUNG_LIKE)
    assert registry.document["issuers"] == [
        {"anchor_namespace": "dart_corp_code", "anchor_token": "00100010", "name": "합성전자"}
    ]
    by_isin = {
        row["anchor_token"]: row
        for row in cast("list[dict[str, Any]]", registry.document["instruments"])
    }
    assert by_isin[SAMSUNG_LIKE]["issuer"] == {
        "anchor_namespace": "dart_corp_code",
        "anchor_token": "00100010",
    }
    # An ETF and a preferred share the corp's stock code does not name keep no issuer.
    assert by_isin[FUND]["issuer"] is None
    assert by_isin[PREFERRED]["issuer"] is None
    links = [row for row in _assertions(registry.document) if row["namespace"] == "issuer"]
    assert [row["token"] for row in links] == [issuer_link_token(issuer, instrument)]
    assert links[0]["provider"] == "dart"
    assert links[0]["known_from_us"] == instant_us(DART_RETRIEVED)
    listed = {
        row["token"]: row for row in _assertions(registry.document) if row["provider"] == "kind"
    }
    assert set(listed) == {"100010", "100015"}
    seoul_open = datetime(1975, 6, 10, 15, tzinfo=UTC)  # 1975-06-11 00:00 in Asia/Seoul
    assert listed["100010"]["valid_from_us"] == int(seoul_open.timestamp()) * 1_000_000
    assert listed["100010"]["known_from_us"] == instant_us(KIND_RETRIEVED)
    assert listed["100010"]["instrument"] == {
        "anchor_namespace": "krx_isin",
        "anchor_token": SAMSUNG_LIKE,
    }
    assert registry.unresolved["kind"] == {"short_code_unresolved": ["500010"]}
    assert registry.unresolved["dart"] == {
        "short_code_unresolved": ["500010"],
        "stock_code_ambiguous": ["600010"],
        "stock_code_is_etf": ["400010"],
    }
    report = registry.report()
    mappers = cast("dict[str, dict[str, Any]]", report["mappers"])
    assert mappers["dart.corp_codes@1"]["skipped"] == {"not_listed": 1}
    assert mappers["kind.listings@1"] == {
        "rows": 3,
        "accepted": 3,
        "skipped": {},
        "refused": {},
    }


def test_kr_receipts_are_checked_against_their_recorded_bytes(tmp_path: Path) -> None:
    receipt, raw = kind_listing([("합성전자", "100010", "1975-06-11")])
    ((row, _),) = kind_unit(receipt, raw).source_rows().records()
    assert row == {
        "list_id": "kind-kospi",
        "company_name": "합성전자",
        "market": "유가",
        "short_code": "100010",
        "industry": "제조업",
        "products": "합성 제품",
        "listed_on": "1975-06-11",
        "fiscal_month": "12월",
        "representative": "대표",
        "homepage": "http://example.invalid",
        "region": "서울특별시",
        "retrieved_at_utc": KIND_RETRIEVED,
    }
    with pytest.raises(ValueError, match="bytes differ"):
        kind_unit(receipt, raw + b" ")
    with pytest.raises(ValueError, match="listed-company header"):
        kind_unit(*kind_listing([], header=("회사명",)))
    complete, files = eodhd_job([symbol("100010", SAMSUNG_LIKE)])
    ((symbol_row, _),) = eodhd_unit(complete, files).source_rows().records()
    assert symbol_row == {
        "exchange_code": "KO",
        "delisted": "0",
        "job_status": "RAW_ACQUIRED",
        "code": "100010",
        "name": "Synthetic 100010",
        "country": "Korea",
        "exchange": "KO",
        "currency": "KRW",
        "type": "Common Stock",
        "isin": SAMSUNG_LIKE,
        "retrieved_at_utc": RETRIEVED,
    }
    tampered = {path: (raw + b" " if path.endswith(".raw") else raw) for path, raw in files.items()}
    with pytest.raises(ValueError, match="bytes differ"):
        eodhd_unit(complete, tampered)
    extra = {**files, "jobs/x/0001.raw": b"{}"}
    with pytest.raises(ValueError, match="differ from the files"):
        eodhd_unit(complete, extra)
    # The readers find the same units from the collected files on disk.
    kind_path = write_kind_listing(tmp_path / "kind", receipt, raw)
    job_dir = write_eodhd_job(tmp_path / "job", complete, files)
    assert read_kind_receipt(kind_path) == kind_unit(receipt, raw)
    assert read_eodhd_job(job_dir) == eodhd_unit(complete, files)


def test_dart_receipt_must_be_one_completed_corp_code_archive() -> None:
    rows = dart_rows(corp_code_xml([("00100010", "합성전자", "100010")]))
    job = _rows(eodhd_job([symbol("100010", SAMSUNG_LIKE)]))
    with pytest.raises(ValueError, match="exactly one corp_codes receipt"):
        build_kr_registry(job, dart=[dart_source(rows[:1])])
    corrupt = (rows[0], (*rows[1][:6], "0" * 64, rows[1][7]))
    with pytest.raises(ValueError, match="recorded SHA-256"):
        build_kr_registry(job, dart=[dart_source(corrupt)])
    padded = dart_rows(corp_code_xml([("00100010", " 합성전자", "100010")]))
    registry = build_kr_registry(job, dart=[dart_source(padded)])
    # A name that is not trimmed text is refused, never renamed.
    assert registry.document["issuers"] == []
    mappers = cast("dict[str, dict[str, Any]]", registry.report()["mappers"])
    assert mappers["dart.corp_codes@1"]["refused"] == {"corp_name_invalid": 1}


def test_kr_sources_import_as_content_and_register_as_one_document(ws: Workspace) -> None:
    job = eodhd_unit(
        *eodhd_job([symbol("100010", SAMSUNG_LIKE), symbol("400010", FUND, kind="ETF")])
    )
    kind = kind_unit(*kind_listing([("합성전자", "100010", "1975-06-11")]))
    dart = commit_dart(ws, dart_rows(corp_code_xml([("00100010", "합성전자", "100010")])))
    first = [import_unit(ws, unit) for unit in (job, kind)]
    again = [import_unit(ws, unit) for unit in (job, kind)]
    assert [row["source_id"] for row in first] == [job.content.source_id, kind.content.source_id]
    assert [row["reused"] for row in again] == [True, True]
    assert job.content.source_id.startswith("qveris-eodhd-exchange-symbols-")
    assert kind.content.source_id.startswith("kind-listings-")
    registry = build_from_workspace(
        ws, eodhd=[job.content.source_id], kind=[kind.content.source_id], dart=[dart]
    )
    assert (
        registry.raw()
        == build_kr_registry(
            [job.source_rows()],
            [kind.source_rows()],
            [dart_source(dart_rows(corp_code_xml([("00100010", "합성전자", "100010")])), dart)],
        ).raw()
    )
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
    assert applied["new"] == {"issuers": 1, "instruments": 2, "assertions": 6}
    repeated = register_identities(ws.state, document, apply=True)
    assert repeated["new"] == {"issuers": 0, "instruments": 0, "assertions": 0}


def test_kr_registry_resolves_eodhd_bars_in_promotion(ws: Workspace) -> None:
    job = eodhd_unit(*eodhd_job([symbol("100010", SAMSUNG_LIKE), symbol("200010", None)]))
    import_unit(ws, job)
    registry = build_from_workspace(ws, eodhd=[job.content.source_id])
    register_identities(
        ws.state,
        decode_registry(registry.raw(), expected_file_sha256=registry.sha256()),
        apply=True,
    )
    snapshot = snapshot_identities(ws.state, "kr", created_at_us=5, apply=True)
    identity = {
        "snapshot_id": str(snapshot["snapshot_id"]),
        "content_hash": str(snapshot["content_hash"]),
    }
    late = at("2025-01-10T00:00:00")
    pin = add_source(
        ws,
        [
            bar("100010.KO", date(2025, 1, 2), 51900.0, retrieved=late),
            bar("200010.KO", date(2025, 1, 2), 1000.0, retrieved=late),
        ],
        tag="kr",
    )
    applied = promote(ws, *spec([pin], identity), apply=True)
    assert applied["rows"] == {"ok": 1, "unresolved": 1}
    assert applied["unresolved_tokens"] == ["200010.KO"]
    rows = prices(ws, str(applied["generation_id"]))
    assert [row["instrument_id"] for row in rows] == [mint_instrument("krx_isin", SAMSUNG_LIKE)]


def test_kr_cli_imports_builds_and_registers(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    home = tmp_path / "aas"
    initialize(home)
    complete, files = eodhd_job([symbol("100010", SAMSUNG_LIKE)])
    job_dir = write_eodhd_job(tmp_path / "job", complete, files)
    kind_path = write_kind_listing(
        tmp_path / "kind", *kind_listing([("합성전자", "100010", "1975-06-11")])
    )
    args = ["identity", "kr-import", "--home", str(home)]
    sources = ["--eodhd-job", str(job_dir), "--kind-receipt", str(kind_path)]

    def run(argv: list[str]) -> dict[str, Any]:
        assert main(argv) == 0
        return json.loads(capsys.readouterr().out)

    planned = run([*args, *sources, "--plan"])
    assert [row["committed"] for row in planned["sources"]] == [False, False]
    imported = run([*args, *sources])
    eodhd_id, kind_id = (row["source_id"] for row in imported["sources"][::-1])
    assert run([*args, *sources, "--plan"])["sources"][0]["committed"] is True
    output = tmp_path / "registry.json"
    report_path = tmp_path / "report.json"
    built = run(
        [
            "identity", "kr-build", "--home", str(home), "--eodhd", eodhd_id,
            "--kind", kind_id, "--output", str(output), "--report", str(report_path),
        ]
    )  # fmt: skip
    raw = output.read_bytes()
    assert built["sha256"] == hashlib.sha256(raw).hexdigest()
    assert json.loads(report_path.read_text())["instruments"] == 1
    # The output is a new file; an existing one is never overwritten.
    rebuild = ["identity", "kr-build", "--home", str(home), "--eodhd", eodhd_id]
    assert main([*rebuild, "--output", str(output)]) == 1
    assert "cannot create new file" in capsys.readouterr().err
    # Both files are checked before either is written, so a refusal leaves neither.
    taken, fresh = tmp_path / "taken.json", tmp_path / "fresh.json"
    taken.write_bytes(b"{}")
    assert main([*rebuild, "--output", str(fresh), "--report", str(taken)]) == 1
    assert "cannot create new file taken.json" in capsys.readouterr().err
    assert not fresh.exists()
    # A failure after the first file is linked removes it again.
    assert main([*rebuild, "--output", str(fresh), "--report", str(fresh)]) == 1
    assert "cannot create new files" in capsys.readouterr().err
    assert not fresh.exists()
    assert sorted(path.name for path in tmp_path.iterdir() if path.name.startswith(".")) == []
    register = ["identity", "register", "--home", str(home), "--file", str(output)]
    registered = run([*register, "--sha256", built["sha256"]])
    assert registered["new"] == {"issuers": 0, "instruments": 1, "assertions": 3}


@pytest.mark.parametrize(
    "other",
    [
        symbol("100010", "US0378331005"),
        symbol("100010", bad_check_digit(SAMSUNG_LIKE)),
        symbol("100010", SAMSUNG_LIKE, kind="Warrant"),
        symbol("100010", SAMSUNG_LIKE, currency="USD"),
        symbol("100010", SAMSUNG_LIKE, kind="ETF"),
    ],
    ids=["other_isin", "bad_check_digit", "unknown_type", "other_currency", "other_type"],
)
def test_lists_that_disagree_on_a_symbol_leave_it_unresolved(other: dict[str, object]) -> None:
    active = eodhd_job([symbol("100010", SAMSUNG_LIKE)])
    delisted = eodhd_job([other], delisted="1")
    registry = build_kr_registry(_rows(active, delisted))
    # Neither list is chosen: the symbol stays unresolved and nothing is minted for it.
    assert registry.symbols == {"100010.KO": "unresolved:symbol_ambiguous"}
    assert registry.unresolved["symbols"] == {"symbol_ambiguous": ["100010.KO"]}
    assert registry.document["instruments"] == []
    assert registry.document["assertions"] == []


def test_only_a_blank_isin_defers_to_the_list_that_names_one() -> None:
    active = eodhd_job([symbol("100010", SAMSUNG_LIKE), symbol("100015", PREFERRED)])
    # A blank ISIN, and a type the registry maps to the same asset type, agree with it.
    delisted = eodhd_job(
        [symbol("100010", ""), symbol("100015", PREFERRED, kind="Preferred Stock")],
        delisted="1",
    )
    registry = build_kr_registry(_rows(active, delisted))
    assert registry.symbols == {
        "100010.KO": mint_instrument("krx_isin", SAMSUNG_LIKE),
        "100015.KO": mint_instrument("krx_isin", PREFERRED),
    }
    instruments = cast("list[dict[str, Any]]", registry.document["instruments"])
    assert {row["asset_type"] for row in instruments} == {"unclassified"}


def test_a_symbol_whose_isin_is_ambiguous_elsewhere_still_makes_the_isin_ambiguous() -> None:
    shared = isin("730000700")
    # 300040.KO's lists disagree, yet the row naming ``shared`` still ties it to 300040.
    job = eodhd_job([symbol("300030", shared), symbol("300040", shared)])
    delisted = eodhd_job([symbol("300040", isin("730000800"))], delisted="1")
    registry = build_kr_registry(_rows(job, delisted))
    assert registry.unresolved["symbols"] == {
        "isin_ambiguous": ["300030.KO"],
        "symbol_ambiguous": ["300040.KO"],
    }
    assert registry.unresolved["isins"] == {"isin_ambiguous": [shared]}
    assert registry.document["instruments"] == []


def test_a_foreign_issuer_listed_on_krx_resolves_by_its_own_isin() -> None:
    foreign = isin("G00000010", country="KY")
    registry = build_kr_registry(
        _rows(eodhd_job([symbol("900010", foreign, exchange="KQ")], exchange="KQ"))
    )
    assert registry.symbols == {"900010.KQ": mint_instrument("krx_isin", foreign)}
    (instrument,) = cast("list[dict[str, Any]]", registry.document["instruments"])
    assert instrument["asset_type"] == "unclassified"
    assert _keys(registry.document) == {
        ("eodhd", "eodhd_symbol", "900010.KQ"),
        ("eodhd", "krx_short_code", "900010"),
    }


def test_a_code_that_is_not_a_short_code_resolves_without_a_short_code_claim() -> None:
    renamed = isin("720000300")
    alone = isin("720000400")
    job = eodhd_job(
        [
            symbol("100030", renamed, exchange="KQ"),
            symbol("100030_old", renamed, exchange="KQ"),
            symbol("1000401E", alone, exchange="KQ"),
        ],
        exchange="KQ",
    )
    registry = build_kr_registry(_rows(job))
    assert registry.symbols == {
        "100030.KQ": mint_instrument("krx_isin", renamed),
        "100030_old.KQ": mint_instrument("krx_isin", renamed),
        "1000401E.KQ": mint_instrument("krx_isin", alone),
    }
    assert _keys(registry.document) == {
        ("eodhd", "eodhd_symbol", "100030.KQ"),
        ("eodhd", "eodhd_symbol", "100030_old.KQ"),
        ("eodhd", "eodhd_symbol", "1000401E.KQ"),
        ("eodhd", "krx_short_code", "100030"),
    }


def test_dart_receipts_and_kind_lists_read_together_leave_conflicts_unresolved() -> None:
    other = isin("710000200")
    job = eodhd_job(
        [
            symbol("100010", SAMSUNG_LIKE),
            symbol("100020", other),
            symbol("100030", isin("710000300")),
            symbol("100040", isin("710000400")),
        ]
    )
    earlier = dart_source(
        dart_rows(
            corp_code_xml(
                [
                    ("00100010", "합성전자", "100010"),
                    ("00100030", "두코드", "100030"),
                    ("00100030", "두코드", "100040"),
                ]
            )
        ),
        "dart-earlier",
    )
    later = dart_source(
        dart_rows(
            corp_code_xml([("00100010", "합성전자새이름", "100010"), ("00100020", "둘", "100020")]),
            retrieved="2026-10-03T00:00:00Z",
        ),
        "dart-later",
    )
    kind = kind_unit(
        *kind_listing([("합성전자", "100010", "1975-06-11"), ("둘", "100020", "1990-01-02")])
    )
    relisted = kind_unit(
        *kind_listing([("둘", "100020", "2001-05-03")], retrieved="2026-10-03T00:00:00Z")
    )
    registry = build_kr_registry(
        _rows(job), [kind.source_rows(), relisted.source_rows()], [later, earlier]
    )
    # The earliest receipt names the issuer; the later one adds a corp it did not list.
    assert registry.document["issuers"] == [
        {"anchor_namespace": "dart_corp_code", "anchor_token": "00100010", "name": "합성전자"},
        {"anchor_namespace": "dart_corp_code", "anchor_token": "00100020", "name": "둘"},
    ]
    links = {
        row["instrument"]["anchor_token"]: row
        for row in _assertions(registry.document)
        if row["namespace"] == "issuer"
    }
    assert links[SAMSUNG_LIKE]["source_snapshot_id"] == earlier.snapshot_id
    assert links[other]["source_snapshot_id"] == later.snapshot_id
    assert registry.unresolved["dart"] == {"corp_code_ambiguous": ["100030", "100040"]}
    assert registry.unresolved["kind"] == {"listing_ambiguous": ["100020"]}
    mappers = cast("dict[str, dict[str, Any]]", registry.report()["mappers"])
    assert mappers["dart.corp_codes@1"]["refused"] == {}


def test_receipts_that_are_not_successful_kr_lists_are_refused() -> None:
    with pytest.raises(ValueError, match="kind-kospi or kind-kosdaq"):
        kind_unit(*kind_listing([], list_id="kind-konex"))
    with pytest.raises(ValueError, match="HTTP 200"):
        kind_unit(*kind_listing([], status=503))
    with pytest.raises(ValueError, match="not an EODHD exchange symbol list"):
        eodhd_unit(*eodhd_job([], tool_id="eodhd.eod.history.v1"))
    with pytest.raises(ValueError, match="not a successful symbol list"):
        eodhd_unit(*eodhd_job([], page_status=429))


def _register(ws: Workspace, registry: KrRegistry) -> dict[str, object]:
    document = decode_registry(registry.raw(), expected_file_sha256=registry.sha256())
    return register_identities(ws.state, document, apply=True)


def test_a_build_reads_every_registered_kr_source_and_reports_withdrawn_claims(
    ws: Workspace,
) -> None:
    second = isin("710000500")
    first = eodhd_unit(*eodhd_job([symbol("100010", SAMSUNG_LIKE)]))
    later = eodhd_unit(
        *eodhd_job(
            [symbol("100010", SAMSUNG_LIKE), symbol("100050", second)],
            retrieved="2026-10-03T05:00:00Z",
        )
    )
    for unit in (first, later):
        import_unit(ws, unit)
    registered = _register(ws, build_from_workspace(ws, eodhd=[first.content.source_id]))
    assert registered["new"] == {"issuers": 0, "instruments": 1, "assertions": 2}
    # A build from the later collection alone would cite later evidence for the registered
    # claims and conflict with them, so it names the registered source to add instead.
    with pytest.raises(ValueError, match=first.content.source_id):
        build_from_workspace(ws, eodhd=[later.content.source_id])
    cumulative = build_from_workspace(ws, eodhd=[first.content.source_id, later.content.source_id])
    assert cumulative.withdrawn == []
    assert _register(ws, cumulative)["new"] == {"issuers": 0, "instruments": 1, "assertions": 2}
    # A newer list naming another ISIN makes 100010.KO ambiguous. The registered claims are
    # not rebuilt and are reported, never closed by the builder.
    moved = eodhd_unit(
        *eodhd_job([symbol("100010", isin("710000600"))], retrieved="2026-10-10T05:00:00Z")
    )
    import_unit(ws, moved)
    sources = [first.content.source_id, later.content.source_id, moved.content.source_id]
    contradicted = build_from_workspace(ws, eodhd=sources)
    assert contradicted.symbols["100010.KO"] == "unresolved:symbol_ambiguous"
    assert {(row["namespace"], row["token"]) for row in contradicted.withdrawn} == {
        ("eodhd_symbol", "100010.KO"),
        ("krx_short_code", "100010"),
    }
    assert contradicted.report()["withdrawn_count"] == len(contradicted.withdrawn)
    assert _register(ws, contradicted)["new"] == {"issuers": 0, "instruments": 0, "assertions": 0}


def test_the_report_script_builds_the_document_kr_build_builds(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    complete, files = eodhd_job([symbol("100010", SAMSUNG_LIKE), symbol("200010", None)])
    job_dir = write_eodhd_job(tmp_path / "job", complete, files)
    receipt, raw = kind_listing([("합성전자", "100010", "1975-06-11")])
    kind_path = write_kind_listing(tmp_path / "kind", receipt, raw)
    xml = corp_code_xml([("00100010", "합성전자", "100010")])
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        job = import_unit(workspace, read_eodhd_job(job_dir))
        kind = import_unit(workspace, read_kind_receipt(kind_path))
        dart = commit_dart(workspace, dart_rows(xml))
        bars = add_source(
            workspace,
            [
                bar(name, date(2025, 1, 2), 1000.0, retrieved=at("2025-01-10T00:00:00"))
                for name in ("100010.KO", "200010.KO", "300010.KO")
            ],
            tag="kr",
        )
        built = build_from_workspace(
            workspace, eodhd=[str(job["source_id"])], kind=[str(kind["source_id"])], dart=[dart]
        )
        market = workspace.paths.market
    output = tmp_path / "report.json"
    argv = [
        "--market", str(market), "--dart-source", dart, "--kind-receipt", str(kind_path),
        "--eodhd-job", str(job_dir), "--symbols-prefix", bars["source_id"],
        "--output", str(output),
    ]  # fmt: skip
    assert kr_identity_report.main(argv) == 0
    report = json.loads(output.read_text())
    assert report["registry"]["sha256"] == built.sha256()
    assert report["resolution"]["unresolved_count"] == {
        "isin_missing": 1,
        "not_in_symbol_list": 1,
    }
