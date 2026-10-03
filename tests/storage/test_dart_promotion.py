# ruff: noqa: PLR2004, S608 -- synthetic counts are the expected values; test-owned SQL
"""DART statement receipts promoted to issuer fundamentals and filings, end to end.

Expected issuers, times and dimensions are spelled independently of the engine: issuers
come from ``mint_issuer``, times are written out as UTC instants of the filing day's end
in Korea, and dimensions come from ``formats.dimensions_hash``.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.storage.identity import mint_issuer
from aegis_alpha.storage.promotion import formats
from aegis_alpha.storage.promotion.engine import promote, verify_promotion
from aegis_alpha.storage.state import get_operation
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage import dart_receipt_support as dart
from tests.storage.promotion_support import at, us

CORP = "00000101"
ISSUER = mint_issuer("dart_corp_code", CORP)
ORIGINAL, AMENDED = "20250814000123", "20251002000456"
# local_day_end@1 in Asia/Seoul (UTC+9): the last microsecond of the filing day in UTC.
END_ORIGINAL = us(at("2025-08-14T14:59:59.999999"))
END_AMENDED = us(at("2025-10-02T14:59:59.999999"))
LINES = [
    dart.Line("ifrs-full_Revenue", "매출액", "1200", cumulative="2300", ord="1"),
    dart.Line("-표준계정코드 미사용-", "기타", "-5", ord="2"),
    dart.Line("-표준계정코드 미사용-", "기타", "7", ord="3"),
]


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _apply(workspace: Workspace, document: tuple[bytes, str]) -> dict[str, object]:
    return promote(workspace, document[0], document[1], apply=True)


def _plan(workspace: Workspace, document: tuple[bytes, str]) -> dict[str, object]:
    return promote(workspace, document[0], document[1], apply=False)


def _facts(workspace: Workspace, generation_id: str | None = None) -> list[dict[str, object]]:
    where = "" if generation_id is None else "WHERE generation_id = ?"
    cursor = workspace.market.execute(
        "SELECT issuer_id, instrument_id, concept, period_start, period_end, fiscal_period, "
        "unit, dimensions_hash, form, accession, value, value_state, op, available_at_us, "
        f"revision_known_at_us, record_id, revision_id FROM fundamentals {where} "
        "ORDER BY accession, fiscal_period, dimensions_hash",
        [] if generation_id is None else [generation_id],
    )
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _dims(name: str, order: str, number: str = ORIGINAL) -> str:
    return formats.dimensions_hash(
        {
            "account_detail": "-",
            "account_nm": name,
            "fs_div": "CFS",
            "ord": order,
            "rcept_no": number,
            "sj_div": "IS",
        }
    )


def test_statements_promote_as_issuer_fundamentals_with_coverage(ws: Workspace) -> None:
    pin = dart.add_receipts(
        ws,
        [
            dart.completed(CORP, "2025", "11012", ORIGINAL, LINES),
            dart.no_data(CORP, "2025", "11013"),
            dart.no_data(CORP, "2025", "11014", outcome="FAILED"),
            dart.corp_codes(),
        ],
        tag="first",
    )
    document = dart.spec([pin])
    plan = _plan(ws, document)
    outcomes = {"completed": 1, "failed": 1, "no_data": 1, "other_endpoint": 1}
    assert plan["source_outcomes"] == outcomes
    assert (plan["source_rows"], plan["mapped_rows"], plan["rows"]) == (4, 4, {"ok": 4})
    assert (plan["refusals"], plan["blocking"]) == ([], [])
    result = _apply(ws, document)
    assert result["published"] is True
    facts = _facts(ws)
    assert [
        (row["concept"], row["fiscal_period"], row["dimensions_hash"], row["value"])
        for row in facts
    ] == sorted(
        [
            ("ifrs-full_Revenue", "Q2", _dims("매출액", "1"), Decimal(1200)),
            ("ifrs-full_Revenue", "H1", _dims("매출액", "1"), Decimal(2300)),
            ("-표준계정코드 미사용-", "Q2", _dims("기타", "2"), Decimal(-5)),
            ("-표준계정코드 미사용-", "Q2", _dims("기타", "3"), Decimal(7)),
        ],
        key=lambda item: (item[1], item[2]),
    )
    for row in facts:
        # The second quarter, or the half year for the cumulative amount.
        start = date(2025, 4, 1) if row["fiscal_period"] == "Q2" else date(2025, 1, 1)
        assert (row["issuer_id"], row["instrument_id"], row["period_start"]) == (
            ISSUER,
            None,
            start,
        )
        assert (row["period_end"], row["unit"], row["form"], row["accession"]) == (
            date(2025, 6, 30),
            "KRW",
            "11012",
            ORIGINAL,
        )
        assert (row["op"], row["value_state"]) == ("ASSERT", "present")
        assert (row["available_at_us"], row["revision_known_at_us"]) == (END_ORIGINAL,) * 2
    flags = ws.market.execute(
        "SELECT flag, count(*) FROM quality_flags GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert flags == [("time_precision_day", 4)]
    # The responses without a statement are coverage: in the manifest and the quality check.
    operation = get_operation(ws.state, str(result["operation_id"]))
    assert operation is not None
    manifest = json.loads(
        (
            ws.paths.raw / str(operation["payload_hash"])[:2] / str(operation["payload_hash"])
        ).read_bytes()
    )
    assert manifest["source_outcomes"] == outcomes
    (reason,) = ws.state.execute(
        "SELECT reason FROM quality_checks WHERE dataset_id='fundamentals.kr.dart' "
        "AND rule_id='promotion_report'"
    ).fetchone()
    assert json.loads(reason)["source_outcomes"] == outcomes
    assert verify_promotion(ws, str(result["generation_id"]))["verified"] is True
    assert verify_workspace(ws)["verified"] is True
    # The same receipts promoted again under the head change nothing.
    child = dart.spec([pin], parent=str(result["generation_id"]))
    again = _apply(ws, child)
    assert (again["published"], again["empty_delta"], again["unchanged"]) == (False, True, 4)
    assert verify_workspace(ws)["verified"] is True


def test_coverage_without_new_rows_is_recorded_on_the_head(ws: Workspace) -> None:
    first = dart.add_receipts(ws, [dart.completed(CORP, "2025", "11012", ORIGINAL, LINES)], tag="a")
    base = _apply(ws, dart.spec([first]))
    # Later responses without a statement change no row; their coverage stays recorded.
    later = dart.add_receipts(
        ws,
        [
            dart.no_data(CORP, "2025", "11013"),
            dart.no_data(CORP, "2025", "11014", outcome="FAILED"),
        ],
        tag="b",
    )
    document = dart.spec([first, later], parent=str(base["generation_id"]))
    result = _apply(ws, document)
    assert (result["published"], result["empty_delta"]) == (False, True)
    checks = ws.state.execute(
        "SELECT check_id, version, result, reason FROM quality_checks "
        "WHERE dataset_id='fundamentals.kr.dart' AND rule_id='promotion_coverage'"
    ).fetchall()
    assert [(row[0], row[1], row[2]) for row in checks] == [
        (result["coverage_check"], "1", "recorded")
    ]
    reason = json.loads(checks[0][3])
    assert reason["request_hash"] == result["request_hash"]
    assert reason["spec_sha256"] == document[1]
    assert [item["source_id"] for item in reason["sources"]] == [
        first["source_id"],
        later["source_id"],
    ]
    assert reason["source_outcomes"] == {"completed": 1, "failed": 1, "no_data": 1}
    assert reason["unchanged"] == 4
    # The spec and request stay as raw evidence; repeating the request adds no check.
    assert (ws.paths.raw / document[1][:2] / document[1]).read_bytes() == document[0]
    repeated = _apply(ws, document)
    assert repeated["coverage_check"] == result["coverage_check"]
    (count,) = ws.state.execute(
        "SELECT count(*) FROM quality_checks WHERE rule_id='promotion_coverage'"
    ).fetchone()
    assert count == 1
    assert verify_workspace(ws)["verified"] is True
    # A first promotion with no rows has no version to hold its coverage.
    empty = dart.add_receipts(ws, [dart.no_data("00000303", "2025", "11013")], tag="c")
    alone = _apply(ws, dart.spec([empty], dataset="fundamentals.kr.dart.solo"))
    assert (alone["empty_delta"], alone["coverage_check"]) == (True, None)


def test_an_unreadable_response_refuses_the_promotion(ws: Workspace) -> None:
    damaged = dart.completed(CORP, "2025", "11012", ORIGINAL, LINES, raw=b"other bytes")
    pin = dart.add_receipts(
        ws, [dart.completed(CORP, "2024", "11011", "20250320000001", LINES), damaged], tag="bad"
    )
    plan = _plan(ws, dart.spec([pin]))
    assert plan["source_outcomes"] == {"completed": 1, "unreadable": 1}
    assert plan["rows"] == {"ok": 3, "refused_required": 1}
    assert plan["refusals"] == ["1 rows required refused"]
    with pytest.raises(ValueError, match="required refused"):
        _apply(ws, dart.spec([pin]))
    assert ws.market.execute("SELECT count(*) FROM fundamentals").fetchone() == (0,)


def test_an_amendment_adds_records_under_its_own_filing(ws: Workspace) -> None:
    first = dart.add_receipts(ws, [dart.completed(CORP, "2025", "11012", ORIGINAL, LINES)], tag="a")
    base = _apply(ws, dart.spec([first]))
    amended = [LINES[0], dart.Line("-표준계정코드 미사용-", "기타", "-6", ord="2")]
    later = "2026-10-01T00:00:00Z"
    second = dart.add_receipts(
        ws,
        [
            # The same filing collected again later, and its amendment.
            dart.completed(CORP, "2025", "11012", ORIGINAL, LINES, retrieved=later),
            dart.completed(CORP, "2025", "11012", AMENDED, amended, fs_div="OFS", retrieved=later),
        ],
        tag="b",
    )
    child = _apply(ws, dart.spec([second], parent=str(base["generation_id"])))
    assert (child["operations"], child["unchanged"]) == ({"ASSERT": 3}, 4)
    added = _facts(ws, str(child["generation_id"]))
    assert {row["accession"] for row in added} == {AMENDED}
    assert {row["available_at_us"] for row in added} == {END_AMENDED}
    assert {row["op"] for row in _facts(ws)} == {"ASSERT"}
    # The earlier filing's facts stay; the amendment's facts are told apart by its number.
    assert len(_facts(ws)) == 7
    revenue = {
        cast("str", row["dimensions_hash"])
        for row in _facts(ws)
        if row["concept"] == "ifrs-full_Revenue" and row["fiscal_period"] == "Q2"
    }
    assert len(revenue) == 2


def test_a_partition_selects_requests_by_business_year(ws: Workspace) -> None:
    pin = dart.add_receipts(
        ws,
        [
            dart.completed(CORP, "2024", "11011", "20250320000001", LINES[:1]),
            dart.completed(CORP, "2025", "11012", ORIGINAL, LINES),
            dart.no_data(CORP, "2025", "11013"),
            dart.corp_codes(),
        ],
        tag="years",
    )
    document = dart.spec([pin], partition={"from": "2024-01-01", "to": "2025-01-01"})
    plan = _plan(ws, document)
    # A row without a business year (the corp code list) is counted in every partition.
    assert plan["source_outcomes"] == {"completed": 1, "other_endpoint": 1}
    result = _apply(ws, document)
    facts = _facts(ws, str(result["generation_id"]))
    assert {(row["accession"], row["period_end"], row["fiscal_period"]) for row in facts} == {
        ("20250320000001", date(2024, 12, 31), "FY")
    }


def test_a_request_without_a_business_year_refuses_every_partition(ws: Workspace) -> None:
    good = dart.completed(CORP, "2024", "11011", "20250320000001", LINES[:1])
    yearless = (*good[:3], dart.request(CORP, "24", "11011"), *good[4:])
    pin = dart.add_receipts(ws, [good, yearless], tag="yearless")
    for start, end in (("2024-01-01", "2025-01-01"), ("2030-01-01", "2031-01-01")):
        plan = _plan(ws, dart.spec([pin], partition={"from": start, "to": end}))
        assert cast("dict[str, int]", plan["source_outcomes"])["unreadable"] == 1
        assert plan["refusals"] == ["1 rows required refused"]
    # A spec that grants leaving unreadable responses out promotes the rest and keeps
    # counting them.
    granted = dart.spec(
        [pin], partition={"from": "2024-01-01", "to": "2025-01-01"}, args={"accept": ["unreadable"]}
    )
    result = _apply(ws, granted)
    assert result["source_outcomes"] == {"completed": 1, "unreadable": 1}
    assert len(_facts(ws, str(result["generation_id"]))) == 1


def test_filings_promote_one_row_per_filing(ws: Workspace) -> None:
    consolidated = dart.completed(CORP, "2025", "11012", ORIGINAL, LINES)
    separate = dart.completed(CORP, "2025", "11012", ORIGINAL, LINES, fs_div="OFS")
    pin = dart.add_receipts(ws, [consolidated, dart.no_data(CORP, "2025", "11013")], tag="f1")
    document = dart.spec([pin], mapper="dart.fnltt_filings@1", dataset="filings.kr.dart")
    result = _apply(ws, document)
    rows = ws.market.execute(
        "SELECT issuer_id, filing_id, form, filed_date, accepted_at_us, period_end, op, "
        "available_at_us FROM filings"
    ).fetchall()
    assert rows == [(ISSUER, ORIGINAL, "11012", date(2025, 8, 14), None, None, "ASSERT",
                     END_ORIGINAL)]  # fmt: skip
    # Both statements of one filing in one generation are read once.
    both = dart.add_receipts(ws, [consolidated, separate], tag="f2")
    repeated = _plan(
        ws,
        dart.spec(
            [both],
            mapper="dart.fnltt_filings@1",
            dataset="filings.kr.dart",
            parent=str(result["generation_id"]),
        ),
    )
    assert (repeated["refusals"], repeated["mapped_rows"], repeated["unchanged"]) == ([], 1, 1)
    # Promoted on its own, the other statement names the same filing and changes nothing.
    alone = dart.add_receipts(ws, [separate], tag="f3")
    again = _apply(
        ws,
        dart.spec(
            [alone],
            mapper="dart.fnltt_filings@1",
            dataset="filings.kr.dart",
            parent=str(result["generation_id"]),
        ),
    )
    assert (again["empty_delta"], again["unchanged"]) == (True, 1)
