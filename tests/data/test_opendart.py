"""OpenDART requests, outcomes and the client: synthetic answers, no network."""

# ruff: noqa: PLR2004 -- synthetic counts are the expected values
from __future__ import annotations

import hashlib
import io
import json
import zipfile
from collections.abc import Mapping
from datetime import date

import pytest

from aegis_alpha.data.opendart import (
    COMPLETED,
    FAILED,
    NO_DATA,
    DartRequest,
    DartResponse,
    HttpAnswer,
    OpenDartClient,
    TransportError,
    classify,
    listed_corp_codes,
    parse_list,
    report_of,
    stops_run,
)
from tests.data.opendart_support import (
    KEY,
    FakeClock,
    FakeProvider,
    corp_archive,
    filing,
    list_page,
    statements,
    status,
)

LEGACY = {
    "endpoint": "financials",
    "observation_date": "2026-09-06",
    "parameters_json": (
        '{"bsns_year":"2026","corp_code":"00000101","fs_div":"CFS","reprt_code":"11014"}'
    ),
}


def test_request_fingerprint_names_the_question_without_its_observation_date() -> None:
    request = DartRequest.financials("00000101", 2026, "11014", "CFS")
    # The same question asked on another day, or recorded by the legacy cohort with its
    # observation date, is the same request.
    assert DartRequest.from_document(LEGACY) == request
    expected = hashlib.sha256(
        json.dumps(
            ["aas-opendart-request-v1", "financials", LEGACY["parameters_json"]],
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    assert request.fingerprint == expected
    assert request.fingerprint == (
        "d5d860290657b03536eac9fa2825b6c079ba0623c1e3110fb96ce6c9333c4f9c"
    )
    assert request.document == {
        "endpoint": "financials",
        "parameters_json": LEGACY["parameters_json"],
    }
    page = DartRequest.list_page(date(2026, 11, 14), 2)
    assert page.parameters == {
        "bgn_de": "20261114",
        "end_de": "20261114",
        "last_reprt_at": "N",
        "page_count": "100",
        "page_no": "2",
        "pblntf_ty": "A",
    }


@pytest.mark.parametrize(
    ("endpoint", "parameters"),
    [
        ("company", {}),
        ("corp_codes", {"page_no": "1"}),
        ("financials", {"bsns_year": "2014", "corp_code": "00000101", "fs_div": "CFS",
                        "reprt_code": "11011"}),
        ("financials", {"bsns_year": "2026", "corp_code": "0000010", "fs_div": "CFS",
                        "reprt_code": "11011"}),
        ("financials", {"bsns_year": "2026", "corp_code": "00000101", "fs_div": "XFS",
                        "reprt_code": "11011"}),
        ("financials", {"bsns_year": "2026", "corp_code": "00000101", "fs_div": "CFS",
                        "reprt_code": "11015"}),
        ("list", {"bgn_de": "20260101", "end_de": "20260501", "last_reprt_at": "N",
                  "page_count": "100", "page_no": "1", "pblntf_ty": "A"}),
        ("list", {"bgn_de": "20260102", "end_de": "20260101", "last_reprt_at": "N",
                  "page_count": "100", "page_no": "1", "pblntf_ty": "A"}),
        ("list", {"bgn_de": "20260101", "end_de": "20260101", "last_reprt_at": "N",
                  "page_count": "100", "page_no": "0", "pblntf_ty": "A"}),
        ("list", {"bgn_de": "20260101", "end_de": "20260101", "last_reprt_at": "Y",
                  "page_count": "100", "page_no": "1", "pblntf_ty": "A"}),
    ],
)  # fmt: skip
def test_requests_outside_the_three_endpoints_are_refused(
    endpoint: str, parameters: dict[str, str]
) -> None:
    with pytest.raises(ValueError, match=r"."):
        DartRequest.of(endpoint, parameters)


def test_parameters_must_be_canonical_json() -> None:
    with pytest.raises(ValueError, match="canonical"):
        DartRequest("corp_codes", "{ }")


def _response(body: bytes, http: int = 200) -> DartResponse:
    clock = FakeClock()
    return DartResponse(http, (), body, clock(), clock())


def test_outcomes_route_the_collector_and_keep_the_provider_status() -> None:
    corp = DartRequest.of("corp_codes")
    statement = DartRequest.financials("00000101", 2026, "11014", "CFS")
    page = DartRequest.list_page(date(2026, 11, 14), 1)
    archive = corp_archive([("00000101", "000101")])
    assert classify(corp, _response(archive)) == (COMPLETED, None)
    assert classify(corp, _response(status("020"))) == (FAILED, "020")
    assert classify(corp, _response(b"not a zip")) == (FAILED, None)
    # An archive that does not parse into listed companies is a failed answer, not a crash.
    assert classify(corp, _response(corp_archive([("00000303", "")]))) == (FAILED, None)
    broken = io.BytesIO()
    with zipfile.ZipFile(broken, "w") as archive_file:
        archive_file.writestr("CORPCODE.xml", b"<result><list>")
    assert classify(corp, _response(broken.getvalue())) == (FAILED, None)
    answer = statements("00000101", "2026", "11014", "20261114000001")
    assert classify(statement, _response(answer)) == (COMPLETED, "000")
    assert classify(statement, _response(status("013"))) == (NO_DATA, "013")
    assert classify(statement, _response(status("800"))) == (FAILED, "800")
    assert classify(statement, _response(answer, http=500)) == (FAILED, "000")
    empty = json.dumps({"status": "000", "list": []}).encode()
    assert classify(statement, _response(empty)) == (FAILED, "000")
    filings = [filing("00000101", "분기보고서 (2026.09)", "20261114000001", "20261114")]
    assert classify(page, _response(list_page(filings))) == (COMPLETED, "000")
    assert classify(page, _response(status("013"))) == (NO_DATA, "013")
    malformed = json.dumps({"status": "000", "list": [{"corp_code": 1}]}).encode()
    assert classify(page, _response(malformed)) == (FAILED, "000")
    # A list answer is the requested page of the requested day, counting a bounded number
    # of pages, and a document repeating a key is no answer at all.
    assert classify(page, _response(list_page(filings, page=2, total=2))) == (FAILED, "000")
    late = [filing("00000101", "분기보고서 (2026.09)", "20261115000001", "20261115")]
    assert classify(page, _response(list_page(late))) == (FAILED, "000")
    assert classify(page, _response(list_page(filings, total=1_001))) == (FAILED, "000")
    repeated = b'{"status":"013","status":"000","list":[]}'
    assert classify(statement, _response(repeated)) == (FAILED, None)
    assert stops_run(_response(status("020")))
    assert stops_run(_response(status("011")))
    assert stops_run(_response(b"", http=429))
    assert not stops_run(_response(status("013")))


def test_periodic_report_names_map_to_their_request() -> None:
    assert report_of("분기보고서 (2026.09)") == ("11014", 2026)
    assert report_of("분기보고서 (2026.03)") == ("11013", 2026)
    assert report_of("반기보고서 (2026.06)") == ("11012", 2026)
    assert report_of("[기재정정]사업보고서 (2025.12)") == ("11011", 2025)
    assert report_of("[첨부추가] [기재정정] 반기보고서  (2026.06)") == ("11012", 2026)
    # A fiscal year that does not end in December names no December-year request.
    assert report_of("사업보고서 (2026.03)") is None
    assert report_of("분기보고서 (2026.12)") is None
    assert report_of("주요사항보고서(자기주식취득결정)") is None


def test_a_list_page_names_its_filings_and_counts_what_it_cannot_map() -> None:
    page = parse_list(
        list_page(
            [
                filing("00000101", "분기보고서 (2026.09)", "20261114000001", "20261114"),
                filing("00000202", "[기재정정]반기보고서 (2026.06)", "20261114000002",
                       "20261114", "K"),
                filing("00000303", "사업보고서 (2026.06)", "20261114000003", "20261114"),
                filing("00000404", "분기보고서 (2014.09)", "20141114000004", "20261114"),
            ],
            total=3,
        )
    )  # fmt: skip
    assert page.total_pages == 3
    assert page.unmapped == 2
    assert [(f.corp_code, f.bsns_year, f.reprt_code, f.filed_on, f.market)
            for f in page.filings] == [
        ("00000101", 2026, "11014", date(2026, 11, 14), "Y"),
        ("00000202", 2026, "11012", date(2026, 11, 14), "K"),
    ]  # fmt: skip
    with pytest.raises(ValueError, match="completed"):
        parse_list(status("013"))


def test_listed_corp_codes_are_those_with_a_stock_code() -> None:
    archive = corp_archive([("00000101", "000101"), ("00000202", ""), ("00000303", "00303A")])
    assert listed_corp_codes(archive) == {"00000101": "000101", "00000303": "00303A"}


def test_the_client_sends_the_key_and_never_returns_it() -> None:
    provider = FakeProvider(answers={("00000101", "2026", "11014", "CFS"): (200, status("013"))})
    client = OpenDartClient(KEY, provider, FakeClock())
    response = client.request(DartRequest.financials("00000101", 2026, "11014", "CFS"))
    assert response.requested_at < response.retrieved_at
    assert provider.asked("fnlttSinglAcntAll.json") == [
        {"bsns_year": "2026", "corp_code": "00000101", "fs_div": "CFS", "reprt_code": "11014"}
    ]

    def echo(method: str, url: str, body: bytes | None, headers: Mapping[str, str]) -> HttpAnswer:
        del method, url, body, headers
        return HttpAnswer(200, (), KEY.encode())

    with pytest.raises(TransportError, match="echoed"):
        OpenDartClient(KEY, echo, FakeClock()).request(DartRequest.of("corp_codes"))
    with pytest.raises(ValueError, match="40"):
        OpenDartClient("short", provider, FakeClock())
