# ruff: noqa: PLR2004 -- synthetic row counts are the expected values
"""Snapshot classifications from synthetic Norgate, SEC and KIND sources.

Every asset ID, CIK, code and company here is made up. Expected subject IDs come from the
Python identity mint and expected instants from Python's zone data, independently of the
SQL the mappers run.
"""

from __future__ import annotations

import hashlib
import io
import json
import time
import zipfile
from datetime import UTC, date, datetime, timedelta
from fractions import Fraction
from typing import TYPE_CHECKING, Any, Final, cast
from unittest.mock import patch
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pytest

from aegis_alpha.application.cli import main
from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.storage import market, source_library
from aegis_alpha.storage.identity import (
    decode_registry,
    mint_instrument,
    mint_issuer,
    register_identities,
    snapshot_identities,
)
from aegis_alpha.storage.kr_identity import build_from_workspace, eodhd_unit, import_unit, kind_unit
from aegis_alpha.storage.market_inputs import GenerationPin, load_pinned_heads
from aegis_alpha.storage.promotion.engine import promote, verify_promotion
from aegis_alpha.storage.promotion.mappers import mapper
from aegis_alpha.storage.promotion.spec import parse_spec
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.read_heads import HeadBinding, HeadPin, HeadQuery
from aegis_alpha.storage.sec_companies import import_companies, plan_companies
from aegis_alpha.storage.source_identity import SourceContent, SourceFile
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.kr_identity_support import KIND_RETRIEVED, eodhd_job, isin, kind_listing, symbol
from tests.storage.us_identity_support import import_submissions

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence
    from pathlib import Path

BUDGET: Final = ComputeBudget(Fraction(1), 256 * 1024 * 1024)
NEW_YORK: Final = ZoneInfo("America/New_York")
SEOUL: Final = ZoneInfo("Asia/Seoul")
EXPORTED: Final = "2026-07-29"
DAY_RULE: Final = {
    "rule": "local_day_end@1",
    "basis": "revision",
    "input": "as_of",
    "args": {"timezone": "America/New_York"},
}
OBSERVED_RULE: Final = {
    "rule": "source_column@1",
    "basis": "revision",
    "input": "observed_at",
    "args": {},
}
MASTER: Final = pa.schema(
    [
        ("assetid", pa.int64()),
        ("symbol", pa.string()),
        ("exchange", pa.string()),
        ("exchange_full", pa.string()),
        ("subtype1", pa.string()),
        ("subtype2", pa.string()),
        ("subtype3", pa.string()),
        ("first_date", pa.string()),
        ("last_date", pa.string()),
    ]
)
OPERATING: Final = ("Equity", "Operating/Holding Company", None)
ETF: Final = ("Exchange Traded Product", "Exchange Traded Fund (ETF)", None)
SPAC: Final = ("Equity", "Special Purpose Company", "Blank Check Company")


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _us(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def _day_end(day: date, zone: ZoneInfo) -> int:
    return _us(datetime.combine(day + timedelta(days=1), datetime.min.time(), zone)) - 1


def _row(  # noqa: PLR0913 -- one synthetic master row spells its columns
    assetid: int,
    types: tuple[str | None, str | None, str | None] = OPERATING,
    *,
    exchange: str | None = "Nasdaq",
    exchange_full: str | None = "Nasdaq",
    first: str | None = "2001-02-03",
    last: str | None = None,
) -> dict[str, object]:
    return {
        "assetid": assetid,
        "symbol": f"S{assetid}",
        "exchange": exchange,
        "exchange_full": exchange_full,
        "subtype1": types[0],
        "subtype2": types[1],
        "subtype3": types[2],
        "first_date": first,
        "last_date": last,
    }


def _commit(
    workspace: Workspace, rows: list[dict[str, object]], *, tag: str, linked: datetime
) -> dict[str, str]:
    """Commit a master table as one linked content source at ``linked``; return its pin."""
    stamp = _us(linked) * 1000
    with patch.object(time, "time_ns", lambda: stamp):
        _, digest, size = put_raw(workspace.paths.raw, f"synthetic-master-{tag}".encode())
        content = SourceContent("synthetic", "security-master", 1, (SourceFile(digest, size),))
        arrow = pa.table({name: [row[name] for row in rows] for name in MASTER.names}, MASTER)
        result = source_library.import_content_arrow(
            workspace, content, "observations", arrow.to_reader()
        )
    tables = cast("list[dict[str, object]]", result["tables"])
    return {
        "source_id": content.source_id,
        "source_sha256": content.sha256,
        "table": "observations",
        "digest": str(tables[0]["digest"]),
    }


def _spec(  # noqa: PLR0913 -- every spec field a test varies
    sources: list[dict[str, str]],
    name: str,
    args: Mapping[str, object],
    *,
    dataset: str = "classifications.us.norgate",
    rule: Mapping[str, object] = DAY_RULE,
    identity: Mapping[str, str] | None = None,
    parent: str | None = None,
    partition: Mapping[str, str] | None = None,
) -> tuple[bytes, str]:
    document = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": "classifications", "dataset_id": dataset, "parent": parent},
        "sources": sources,
        "mapper": {"name": name, "args": dict(args)},
        "partition": None if partition is None else dict(partition),
        "time_rules": {"available_at_us": rule, "revision_known_at_us": rule},
        "decimal_rule": {},
        "quality_rules": [],
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": identity,
    }
    raw = json.dumps(document, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def _norgate(scheme: str = "norgate.security_type", as_of: str = EXPORTED) -> dict[str, str]:
    return {"scheme": scheme, "as_of": as_of}


def _pin(workspace: Workspace, generation_id: str) -> GenerationPin:
    marker = market.marker_for(workspace.market, generation_id)
    return GenerationPin(
        str(marker["dataset_id"]),
        str(marker["version"]),
        generation_id,
        str(marker["chain_hash"]),
        str(marker["request_hash"]),
    )


def _read(
    workspace: Workspace,
    generation_id: str,
    *,
    cutoff: int | None = None,
    grants: tuple[str, ...] = (),
) -> list[tuple[object, ...]]:
    """The heads a reader sees: (subject, scheme, code, label, effective_from), sorted."""
    read = load_pinned_heads(
        workspace,
        HeadBinding("classifications", (HeadPin(_pin(workspace, generation_id)),), grants),
        HeadQuery(cutoff_us=cutoff),
        budget=BUDGET,
    )
    names = ("subject_id", "scheme", "code", "label", "effective_from")
    return sorted(tuple(row.values[name] for name in names) for row in read.rows)


def _source(columns: str, rows: Sequence[tuple[object, ...]]) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    connection.execute(
        "CREATE TABLE src (_aas_pin INTEGER, _aas_ordinal BIGINT, _aas_row_hash VARCHAR, "
        f"{columns})"
    )
    marks = ", ".join("?" for _ in range(len(rows[0]) + 2))
    connection.executemany(
        f"INSERT INTO src VALUES (0, {marks})",  # noqa: S608 -- test-owned table
        [(index, "h", *row) for index, row in enumerate(rows)],
    )
    return connection


def test_norgate_classification_maps_synthetic_fixture() -> None:
    found = mapper("norgate.classification@1")
    for bad in ({"scheme": "norgate.sector", "as_of": EXPORTED}, {"scheme": "x"}, {}):
        with pytest.raises(ValueError, match=r"norgate\.classification@1"):
            found.check_args(bad)
    with pytest.raises(ValueError, match="as_of"):
        found.check_args(_norgate(as_of="2026-02-30"))
    columns = (
        "assetid BIGINT, subtype1 VARCHAR, subtype2 VARCHAR, subtype3 VARCHAR, "
        "exchange VARCHAR, exchange_full VARCHAR, first_date VARCHAR, last_date VARCHAR"
    )
    rows = [
        (101, *OPERATING, "NYSE", "New York Stock Exchange", "1999-11-18", None),
        (102, *SPAC, "Nasdaq", "Nasdaq", "2021-03-01", "2023-05-05"),
        (0, *OPERATING, "NYSE", "New York Stock Exchange", "1999-11-18", None),
        (103, *OPERATING, "NYSE", "New York Stock Exchange", "1999-11-18", "2026-07-30"),
        (104, None, None, None, "OTC", "", "2010-01-04", "not a date"),
        (105, "Equity", None, "Blank Check Company", "OTC", "OTC Grey Market", None, None),
        (106, "Equity", "", None, "OTC", "OTC Grey Market", None, None),
    ]
    connection = _source(columns, rows)
    names = (
        "subject_id, subject_kind, scheme, code, label, effective_from, effective_to, "
        "_aas_ingested_at_us, _aas_t_as_of"
    )
    exported = date.fromisoformat(EXPORTED)
    minted = {assetid: mint_instrument("norgate_assetid", str(assetid)) for assetid in (101, 102)}
    by_scheme = {}
    for scheme in ("norgate.security_type", "norgate.exchange"):
        args = _norgate(scheme)
        found.check_args(args)
        assert found.identity(args) is None
        by_scheme[scheme] = connection.execute(
            f"SELECT {names} FROM ({found.select('src', args)}) ORDER BY _aas_ordinal"  # noqa: S608
        ).fetchall()
    types = by_scheme["norgate.security_type"]
    common = ("instrument", "norgate.security_type")
    assert types[0] == (
        minted[101],
        *common,
        "Equity > Operating/Holding Company",
        "Operating/Holding Company",
        exported,
        None,
        None,
        exported,
    )
    assert types[1][:5] == (
        minted[102],
        *common,
        "Equity > Special Purpose Company > Blank Check Company",
        "Blank Check Company",
    )
    # Asset ID 0 is not canonical and a row dated after the export contradicts as_of: no
    # subject. Absent or skipped type levels give no code; an unparseable date is no evidence.
    assert [row[0] for row in types[2:4]] == [None, None]
    assert [row[3] for row in types[4:]] == [None, None, None]
    assert types[4][0] == mint_instrument("norgate_assetid", "104")
    exchanges = by_scheme["norgate.exchange"]
    assert exchanges[0][2:5] == ("norgate.exchange", "NYSE", "New York Stock Exchange")
    assert exchanges[4][3:5] == (None, None)
    assert exchanges[5][3:5] == ("OTC", "OTC Grey Market")


def test_sec_sic_maps_synthetic_fixture() -> None:
    found = mapper("sec.sic@1")
    args = {"as_of": "2026-09-06"}
    found.check_args(args)
    with pytest.raises(ValueError, match=r"sec\.sic@1"):
        found.check_args({"as_of": "2026-09-06", "scheme": "sic"})
    columns = (
        "member VARCHAR, cik VARCHAR, sic VARCHAR, sic_description VARCHAR, "
        "latest_filing_date VARCHAR"
    )
    rows = [
        ("CIK0000000001.json", "1", "3571", "Electronic Computers", "2026-09-05"),
        ("CIK0000000002.json", "0000000002", "6770", "Blank Checks", None),
        ("CIK0000000003.json", "33", "1000", "Metal Mining", "2026-01-01"),
        ("CIK0000000004.json", "4", "", "", "2026-01-01"),
        ("CIK0000000009.json", "9", "0000", "", "2026-01-01"),
        ("CIK0000000005.json", "5", None, None, "2026-01-01"),
        ("CIK0000000006.json", "6", "357", "Short", "2026-01-01"),
        ("CIK0000000007.json", "7", "2834", "Pharmaceutical Preparations", "2026-09-07"),
        ("CIK0000000008-submissions-001.json", "8", "2834", "Paged", None),
    ]
    connection = _source(columns, rows)
    mapped = connection.execute(
        "SELECT _aas_ordinal, subject_id, subject_kind, scheme, code, label, effective_from, "  # noqa: S608
        f"_aas_t_as_of FROM ({found.select('src', args)}) ORDER BY _aas_ordinal"
    ).fetchall()
    day = date(2026, 9, 6)
    assert mapped[0] == (
        0,
        mint_issuer("sec_cik", "0000000001"),
        "issuer",
        "sec.sic",
        "3571",
        "Electronic Computers",
        day,
        day,
    )
    assert mapped[1][1] == mint_issuer("sec_cik", "0000000002")
    # No SIC or no description maps to no row; a CIK the document does not state, a filing
    # after as_of and a paginated member keep no issuer; a code that is not four digits keeps
    # no code.
    assert [row[0] for row in mapped] == [0, 1, 2, 6, 7, 8]
    assert [row[1] for row in mapped[2:]] == [
        None,
        mint_issuer("sec_cik", "0000000006"),
        None,
        None,
    ]
    assert mapped[3][4] is None


def test_kind_industry_maps_synthetic_fixture() -> None:
    found = mapper("kind.industry@1")
    found.check_args({})
    with pytest.raises(ValueError, match="no arguments"):
        found.check_args({"timezone": "Asia/Seoul"})
    columns = "short_code VARCHAR, industry VARCHAR, retrieved_at_utc VARCHAR"
    rows = [
        ("100010", "반도체 제조업", "2026-09-05T15:30:00.5Z"),
        ("100020", "", "2026-09-05T15:30:00Z"),
        ("100030", "은행 및 저축기관", "2026-09-05 15:30:00"),
    ]
    mapped = (
        _source(columns, rows)
        .execute(
            "SELECT _aas_id_token, _aas_id_at_us, _aas_ingested_at_us, subject_kind, scheme, "  # noqa: S608
            "code, label, effective_from, _aas_t_observed_at, _aas_t_as_of "
            f"FROM ({found.select('src', {})}) ORDER BY _aas_ordinal"
        )
        .fetchall()
    )
    observed = _us(datetime(2026, 9, 5, 15, 30, 0, 500000, tzinfo=UTC))
    seoul_day = datetime(2026, 9, 5, 15, 30, tzinfo=UTC).astimezone(SEOUL).date()
    assert seoul_day == date(2026, 9, 6)
    assert mapped[0] == (
        "100010",
        observed,
        observed,
        "instrument",
        "kind.industry",
        "반도체 제조업",
        "반도체 제조업",
        seoul_day,
        observed,
        seoul_day,
    )
    # An empty industry is no classification; an instant not spelled as UTC has no time.
    assert len(mapped) == 2
    assert mapped[1][1:3] == (None, None)
    assert mapped[1][7] is None


def test_classification_subjects_resolve_or_mint(ws: Workspace) -> None:
    del ws
    pin = {"source_id": "s", "source_sha256": "0" * 64, "table": "t", "digest": "0" * 64}
    identity = {"snapshot_id": "kr", "content_hash": "0" * 64}
    parse_spec(*_spec([pin], "norgate.classification@1", _norgate()))
    parse_spec(*_spec([pin], "kind.industry@1", {}, rule=OBSERVED_RULE, identity=identity))
    with pytest.raises(ValueError, match="identity snapshot"):
        parse_spec(*_spec([pin], "norgate.classification@1", _norgate(), identity=identity))
    with pytest.raises(ValueError, match="identity snapshot"):
        parse_spec(*_spec([pin], "kind.industry@1", {}, rule=OBSERVED_RULE))


def test_classifications_are_not_returned_before_the_snapshot(ws: Workspace) -> None:
    linked = datetime(2026, 9, 10, tzinfo=UTC)
    pin = _commit(ws, [_row(101), _row(102, ETF)], tag="first", linked=linked)
    applied = promote(ws, *_spec([pin], "norgate.classification@1", _norgate()), apply=True)
    assert applied["published"] is True
    assert applied["rows"] == {"ok": 2}
    generation = str(applied["generation_id"])
    known = _day_end(date.fromisoformat(EXPORTED), NEW_YORK)
    stored = ws.market.execute(
        "SELECT DISTINCT op, available_at_us, revision_known_at_us, ingested_at_us "
        "FROM classifications WHERE generation_id=?",
        [generation],
    ).fetchall()
    assert stored == [("ASSERT", known, known, _us(linked))]
    exported = date.fromisoformat(EXPORTED)
    expected = sorted(
        [
            (
                mint_instrument("norgate_assetid", "101"),
                "norgate.security_type",
                "Equity > Operating/Holding Company",
                "Operating/Holding Company",
                exported,
            ),
            (
                mint_instrument("norgate_assetid", "102"),
                "norgate.security_type",
                "Exchange Traded Product > Exchange Traded Fund (ETF)",
                "Exchange Traded Fund (ETF)",
                exported,
            ),
        ]
    )
    grant = ("local_day_end@1",)
    # Before the snapshot is known a strict read returns nothing, granted or not; from
    # then on only a reader that grants the date-only rule sees it. Research reads all.
    assert _read(ws, generation, cutoff=known - 1, grants=grant) == []
    assert _read(ws, generation, cutoff=known, grants=grant) == expected
    assert _read(ws, generation, cutoff=_us(linked) * 2) == []
    assert _read(ws, generation) == expected
    flags = ws.market.execute(
        "SELECT DISTINCT flag FROM quality_flags WHERE generation_id=?", [generation]
    ).fetchall()
    assert flags == [("time_precision_day",)]
    assert verify_promotion(ws, generation, budget=BUDGET)["verified"] is True


def test_a_later_snapshot_adds_rows_of_its_own_date(ws: Workspace) -> None:
    first = _commit(
        ws, [_row(101), _row(102, ETF)], tag="a", linked=datetime(2026, 8, 1, tzinfo=UTC)
    )
    scheme = _norgate("norgate.exchange")
    one = promote(ws, *_spec([first], "norgate.classification@1", scheme), apply=True)
    head = str(one["generation_id"])
    again = promote(
        ws, *_spec([first], "norgate.classification@1", scheme, parent=head), apply=True
    )
    assert again["empty_delta"] is True
    moved = [_row(101, exchange="NYSE", exchange_full="New York Stock Exchange"), _row(102, ETF)]
    second = _commit(ws, moved, tag="b", linked=datetime(2026, 9, 2, tzinfo=UTC))
    later = _norgate("norgate.exchange", "2026-08-31")
    two = promote(ws, *_spec([second], "norgate.classification@1", later, parent=head), apply=True)
    assert two["operations"] == {"ASSERT": 2}
    newest = str(two["generation_id"])
    grant = ("local_day_end@1",)
    between = _day_end(date(2026, 8, 31), NEW_YORK) - 1
    subject = mint_instrument("norgate_assetid", "101")
    seen = _read(ws, newest, cutoff=between, grants=grant)
    assert [row[2:] for row in seen if row[0] == subject] == [
        ("Nasdaq", "Nasdaq", date(2026, 7, 29))
    ]
    seen = sorted(_read(ws, newest, grants=grant), key=lambda row: cast("date", row[4]))
    assert [row[2:] for row in seen if row[0] == subject] == [
        ("Nasdaq", "Nasdaq", date(2026, 7, 29)),
        ("NYSE", "New York Stock Exchange", date(2026, 8, 31)),
    ]


def test_snapshot_rows_dated_after_as_of_refuse_the_promotion(ws: Workspace) -> None:
    rows = [_row(101), _row(102, last="2026-07-30")]
    pin = _commit(ws, rows, tag="late", linked=datetime(2026, 9, 1, tzinfo=UTC))
    planned = promote(ws, *_spec([pin], "norgate.classification@1", _norgate()), apply=False)
    assert planned["rows"] == {"ok": 1, "refused_required": 1}
    assert planned["refusals"]
    with pytest.raises(ValueError, match="promotion refused"):
        promote(ws, *_spec([pin], "norgate.classification@1", _norgate()), apply=True)


def test_a_partitioned_plan_refuses_undated_norgate_rows(ws: Workspace) -> None:
    rows = [_row(101), _row(102, first=None), _row(103, first="2001-2-3")]
    pin = _commit(ws, rows, tag="part", linked=datetime(2026, 9, 1, tzinfo=UTC))
    window = {"from": "2001-01-01", "to": "2002-01-01"}
    raw, sha = _spec([pin], "norgate.classification@1", _norgate(), partition=window)
    planned = promote(ws, raw, sha, apply=False)
    # The text first_date is the partition date; a row without a readable one is refused.
    assert planned["refusals"] == [
        "2 source rows have no partition date, so no partition holds them"
    ]
    assert planned["rows"] == {"ok": 1}
    with pytest.raises(ValueError, match="promotion refused"):
        promote(ws, raw, sha, apply=True)
    unpartitioned = promote(ws, *_spec([pin], "norgate.classification@1", _norgate()), apply=False)
    assert unpartitioned["rows"] == {"ok": 3}


def test_kind_industries_resolve_short_codes_through_the_snapshot(ws: Workspace) -> None:
    listed = isin("710000100")
    job = eodhd_unit(*eodhd_job([symbol("100010", listed)]))
    import_unit(ws, job)
    rows = [("합성전자", "100010", "1975-06-11"), ("합성은행", "200020", "2001-01-02")]
    industries = {"100010": "반도체 제조업", "200020": "은행 및 저축기관"}
    kind = kind_unit(*kind_listing(rows, industries=industries))
    committed = import_unit(ws, kind)
    registry = build_from_workspace(
        ws, eodhd=[job.content.source_id], kind=[kind.content.source_id]
    )
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
    pin = {
        "source_id": kind.content.source_id,
        "source_sha256": kind.content.sha256,
        "table": "listings",
        "digest": str(committed["digest"]),
    }
    raw, sha = _spec(
        [pin],
        "kind.industry@1",
        {},
        dataset="classifications.kr.kind",
        rule=OBSERVED_RULE,
        identity=identity,
    )
    applied = promote(ws, raw, sha, apply=True)
    assert applied["rows"] == {"ok": 1, "unresolved": 1}
    assert applied["unresolved_tokens"] == ["200020"]
    generation = str(applied["generation_id"])
    observed = _us(datetime.fromisoformat(KIND_RETRIEVED))
    expected = [
        (
            mint_instrument("krx_isin", listed),
            "kind.industry",
            "반도체 제조업",
            "반도체 제조업",
            datetime.fromisoformat(KIND_RETRIEVED).astimezone(SEOUL).date(),
        )
    ]
    # The collection instant is the source's own time: no grant, and nothing before it.
    assert _read(ws, generation, cutoff=observed - 1) == []
    assert _read(ws, generation, cutoff=observed) == expected


def test_kind_industry_v2_maps_repeated_rows_once() -> None:
    at = "2026-09-05T15:30:00Z"
    merged = ("100010", "반도체 제조업", at, "전남광주통합특별시", "b")
    before = ("100010", "반도체 제조업", at, "전라남도", "a")
    other = ("100020", "은행 및 저축기관", at, "서울특별시", "c")
    split = [
        ("100030", "제조업", at, "서울특별시", "d"),
        ("100030", "도매업", at, "부산광역시", "e"),
    ]
    later = ("100010", "반도체 제조업", "2026-09-05T15:31:00Z", "전라남도", "f")

    def mapped(name: str, rows: Sequence[tuple[str, ...]]) -> list[tuple[object, ...]]:
        connection = duckdb.connect()
        connection.execute("SET TimeZone='UTC'")
        connection.execute(
            "CREATE TABLE src (_aas_pin INTEGER, _aas_ordinal BIGINT, _aas_row_hash VARCHAR, "
            "short_code VARCHAR, industry VARCHAR, retrieved_at_utc VARCHAR, region VARCHAR)"
        )
        connection.executemany(
            "INSERT INTO src VALUES (0, ?, ?, ?, ?, ?, ?)",
            [(index, row[4], *row[:4]) for index, row in enumerate(rows)],
        )
        return connection.execute(
            "SELECT _aas_row_hash, _aas_id_token, _aas_id_at_us, code, label, effective_from "  # noqa: S608
            f"FROM ({mapper(name).select('src', {})}) ORDER BY _aas_row_hash"
        ).fetchall()

    rows = [merged, other, before, *split, later]
    once = mapped("kind.industry@2", rows)
    # One row per (short code, industry, instant): the smallest row hash, in any order.
    assert [row[0] for row in once] == ["a", "c", "d", "e", "f"]
    assert mapped("kind.industry@2", rows[::-1]) == once
    assert mapped("kind.industry@2", [before, merged]) == mapped(
        "kind.industry@2", [merged, before]
    )
    # Two industries of one short code at one instant both map, for the engine to refuse.
    assert [row[3] for row in once if row[1] == "100030"] == ["제조업", "도매업"]
    every = mapped("kind.industry@1", rows)
    assert [row[0] for row in every] == ["a", "b", "c", "d", "e", "f"]
    assert [row[1:] for row in every if row[0] != "b"] == [row[1:] for row in once]
    with pytest.raises(ValueError, match=r"kind\.industry@2 takes no arguments"):
        mapper("kind.industry@2").check_args({"x": 1})


def _kind_world(
    workspace: Workspace, cells: Sequence[tuple[str | None, str]], codes: Sequence[str]
) -> tuple[dict[str, str], dict[str, str]]:
    """Commit a KIND list of ``codes`` and snapshot it; the listings pin and the identity."""
    listed = isin("710000100")
    job = eodhd_unit(*eodhd_job([symbol("100010", listed)]))
    import_unit(workspace, job)
    rows = [("합성전자", code, "1975-06-11") for code in codes]
    kind = kind_unit(*kind_listing(rows, cells=cells))
    committed = import_unit(workspace, kind)
    registry = build_from_workspace(
        workspace, eodhd=[job.content.source_id], kind=[kind.content.source_id]
    )
    register_identities(
        workspace.state,
        decode_registry(registry.raw(), expected_file_sha256=registry.sha256()),
        apply=True,
    )
    snapshot = snapshot_identities(workspace.state, "kr", created_at_us=5, apply=True)
    identity = {
        "snapshot_id": str(snapshot["snapshot_id"]),
        "content_hash": str(snapshot["content_hash"]),
    }
    pin = {
        "source_id": kind.content.source_id,
        "source_sha256": kind.content.sha256,
        "table": "listings",
        "digest": str(committed["digest"]),
    }
    return pin, identity


def _kind_spec(name: str, pin: dict[str, str], identity: dict[str, str]) -> tuple[bytes, str]:
    return _spec(
        [pin],
        name,
        {},
        dataset="classifications.kr.kind",
        rule=OBSERVED_RULE,
        identity=identity,
    )


def test_kind_industry_v2_publishes_one_row_per_instrument_in_any_row_order(
    tmp_path: Path,
) -> None:
    regions = [(None, "전남광주통합특별시"), (None, "전라남도")]
    stored = []
    for order, cells in (("a", regions), ("b", regions[::-1])):
        initialize(tmp_path / order)
        with open_workspace(tmp_path / order, writable=True, strategy_write=True) as workspace:
            pin, identity = _kind_world(workspace, cells, ["100010", "100010"])
            refused = promote(workspace, *_kind_spec("kind.industry@1", pin, identity), apply=False)
            assert refused["published"] is False
            assert "1 natural keys repeat across 2 source rows" in cast(
                "list[str]", refused["refusals"]
            )
            applied = promote(workspace, *_kind_spec("kind.industry@2", pin, identity), apply=True)
            assert applied["published"] is True, applied["refusals"]
            assert applied["rows"] == {"ok": 1}
            assert applied["unselected_rows"] == 1
            stored.append(
                workspace.market.execute(
                    "SELECT subject_id, code, record_id, revision_id, source_row_hash "
                    "FROM classifications WHERE generation_id=?",
                    [str(applied["generation_id"])],
                ).fetchall()
            )
    assert len(stored[0]) == 1
    assert stored[0][0][:2] == (mint_instrument("krx_isin", isin("710000100")), "제조업")
    assert stored[0] == stored[1]


def test_kind_industry_v2_refuses_two_industries_at_one_instant(ws: Workspace) -> None:
    cells = [("반도체 제조업", "전라남도"), ("은행 및 저축기관", "전라남도")]
    pin, identity = _kind_world(ws, cells, ["100010", "100010"])
    planned = promote(ws, *_kind_spec("kind.industry@2", pin, identity), apply=False)
    assert planned["published"] is False
    assert "1 natural keys repeat across 2 source rows" in cast("list[str]", planned["refusals"])


def _submissions(documents: Mapping[str, Mapping[str, object] | bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in documents.items():
            payload = body if isinstance(body, bytes) else json.dumps(body).encode()
            archive.writestr(zipfile.ZipInfo(name, (2026, 9, 5, 4, 25, 4)), payload)
    return buffer.getvalue()


def _company(cik: str, sic: str, description: str, dates: list[str]) -> dict[str, object]:
    return {
        "cik": cik,
        "entityType": "operating",
        "sic": sic,
        "sicDescription": description,
        "name": f"Synthetic {cik}",
        "tickers": [],
        "filings": {"recent": {"filingDate": dates}},
    }


ARCHIVE: Final = {
    "CIK0000000001.json": _company(
        "1", "3571", "Electronic Computers", ["2026-08-01", "2026-09-01"]
    ),
    "CIK0000000002.json": _company("2", "", "", []),
    "CIK0000000003.json": {"cik": "3", "name": "Synthetic 3", "sic": "6770", "filings": []},
    "CIK0000000001-submissions-001.json": {"filingDate": ["2001-01-01"]},
    "placeholder.txt": b"synthetic",
}


def test_sec_companies_import_and_promote_sic(ws: Workspace, tmp_path: Path) -> None:
    members = import_submissions(ws, tmp_path / "sec", _submissions(ARCHIVE))
    planned = plan_companies(ws, members)
    assert planned["committed"] is False
    assert (planned["members"], planned["companies"], planned["with_sic"]) == (5, 3, 2)
    applied = import_companies(ws, members)
    assert applied["source_id"] == planned["source_id"]
    assert str(applied["source_id"]).startswith("sec-submissions-companies-")
    assert (applied["rows"], applied["reused"]) == (3, False)
    assert (applied["members"], applied["companies"], applied["with_sic"]) == (5, 3, 2)
    rerun = import_companies(ws, members)
    # A reused source is not read again, so the rerun reports no member counts.
    assert rerun["reused"] is True
    assert {"members", "companies", "with_sic"}.isdisjoint(rerun)
    assert (rerun["source_id"], rerun["rows"]) == (applied["source_id"], 3)
    stored = ws.market.execute(
        f"SELECT cik, name, entity_type, sic, sic_description, latest_filing_date FROM "  # noqa: S608
        f'"{_target(ws, str(applied["source_id"]))}" ORDER BY _aas_ordinal'
    ).fetchall()
    assert stored == [
        ("1", "Synthetic 1", "operating", "3571", "Electronic Computers", "2026-09-01"),
        ("2", "Synthetic 2", "operating", "", "", None),
        ("3", "Synthetic 3", None, "6770", None, None),
    ]
    pin = {
        "source_id": str(applied["source_id"]),
        "source_sha256": SourceContent(
            "sec", "submissions-companies", 1, _files(ws, members)
        ).sha256,
        "table": "companies",
        "digest": str(applied["digest"]),
    }
    raw, sha = _spec([pin], "sec.sic@1", {"as_of": "2026-09-06"}, dataset="classifications.us.sec")
    planned_sic = promote(ws, raw, sha, apply=False)
    # The second company states no SIC and the third no SIC description: neither classifies.
    assert planned_sic["rows"] == {"ok": 1}
    assert planned_sic["unselected_rows"] == 2
    assert promote(ws, raw, sha, apply=True)["operations"] == {"ASSERT": 1}


@pytest.mark.parametrize(
    ("body", "reason"),
    [(b"not json", "is not UTF-8 JSON"), (b"[1, 2]", "is not a JSON object")],
)
def test_sec_companies_refuse_a_member_that_is_not_a_json_object(
    ws: Workspace, tmp_path: Path, body: bytes, reason: str
) -> None:
    members = import_submissions(
        ws, tmp_path / "sec", _submissions({**ARCHIVE, "CIK0000000004.json": body})
    )
    before = {str(row["source_id"]) for row in source_library.list_sources(ws)}
    with pytest.raises(ValueError, match=f"CIK0000000004.json {reason}"):
        plan_companies(ws, members)
    with pytest.raises(ValueError, match=f"CIK0000000004.json {reason}"):
        import_companies(ws, members)
    assert {str(row["source_id"]) for row in source_library.list_sources(ws)} == before


def test_sec_companies_cli_plans_and_imports(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    home = tmp_path / "aas"
    initialize(home)
    # Without the third company, whose SIC has no description, the CLI archive holds one
    # classified company and one without a SIC, so the counts below differ from the API test's.
    clean = {key: value for key, value in ARCHIVE.items() if key != "CIK0000000003.json"}
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        members = import_submissions(workspace, tmp_path / "sec", _submissions(clean))
    args = ["import", "sec-companies", "--home", str(home), "--source", members]

    def run(argv: list[str]) -> dict[str, Any]:
        assert main(argv) == 0
        return json.loads(capsys.readouterr().out)

    planned = run([*args, "--plan"])
    assert (planned["mode"], planned["committed"], planned["companies"]) == ("plan", False, 2)
    imported = run(args)
    assert (imported["mode"], imported["rows"], imported["source_id"]) == (
        "apply",
        2,
        planned["source_id"],
    )
    assert run([*args, "--plan"])["committed"] is True
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        pin = {
            "source_id": str(imported["source_id"]),
            "source_sha256": SourceContent(
                "sec", "submissions-companies", 1, _files(workspace, members)
            ).sha256,
            "table": "companies",
            "digest": str(imported["digest"]),
        }
        raw, sha = _spec(
            [pin], "sec.sic@1", {"as_of": "2026-09-06"}, dataset="classifications.us.sec"
        )
        applied = promote(workspace, raw, sha, apply=True)
        assert applied["rows"] == {"ok": 1}
        assert applied["unselected_rows"] == 1
        heads = _read(workspace, str(applied["generation_id"]), grants=("local_day_end@1",))
    assert heads == [
        (
            mint_issuer("sec_cik", "0000000001"),
            "sec.sic",
            "3571",
            "Electronic Computers",
            date(2026, 9, 6),
        )
    ]
    with pytest.raises(SystemExit):
        main(["import", "sec-companies", "--home", str(home)])


def _target(workspace: Workspace, source_id: str) -> str:
    (table,) = source_library.list_tables(workspace, source_id)
    return str(table["target"])


def _files(workspace: Workspace, members: str) -> tuple[SourceFile, ...]:
    from aegis_alpha.storage.us_identity import sec_archive  # noqa: PLC0415

    archive, _ = sec_archive(workspace, members)
    return (archive,)
