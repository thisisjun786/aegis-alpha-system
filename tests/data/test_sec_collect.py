"""SEC EDGAR requests, answers and the document plan, offline against ``FakeSec``."""

# ruff: noqa: PLR2004 -- synthetic counts are the expected values
from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

import pytest

from aegis_alpha.data import sec_collect as sec
from aegis_alpha.data.opendart import HttpAnswer, TransportError
from aegis_alpha.data.provider_request import COMPLETED, FAILED, NO_DATA, Response
from tests.data.opendart_support import FakeClock
from tests.data.us_collect_support import (
    USER_AGENT,
    SecFact,
    SecFiling,
    companyfacts_bytes,
    index_bytes,
    submissions_bytes,
)

CIK = "0000000101"
OTHER = "0000000202"
A1 = "0000000101-26-000001"
A2 = "0000000101-26-000002"
B1 = "0000000202-26-000001"
MONDAY = date(2026, 9, 14)


def _response(status: int, body: bytes) -> Response:
    moment = datetime(2026, 9, 16, tzinfo=UTC)
    return Response(status, (), body, moment, moment)


def test_requests_name_the_document_and_never_the_contact() -> None:
    assert sec.url(sec.daily_index(date(2026, 10, 2))) == (
        "https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/master.20261002.idx"
    )
    assert sec.url(sec.submissions("101")) == (
        "https://data.sec.gov/submissions/CIK0000000101.json"
    )
    assert sec.url(sec.companyfacts(CIK)) == (
        "https://data.sec.gov/api/xbrl/companyfacts/CIK0000000101.json"
    )
    assert sec.submissions("101").parameters == {"cik": CIK}
    with pytest.raises(ValueError, match="ten decimal digits"):
        sec.submissions("12345678901")
    seen: list[dict[str, str]] = []

    def transport(method: str, url: str, body: bytes | None, headers: dict[str, str]) -> HttpAnswer:
        del method, url, body
        seen.append(dict(headers))
        return HttpAnswer(200, (), USER_AGENT.encode())

    client = sec.SecClient(USER_AGENT, transport, FakeClock())  # ty: ignore[invalid-argument-type]
    # An answer echoing the contact is not retained.
    with pytest.raises(TransportError, match="echoed"):
        client.request(sec.submissions(CIK))
    # The contact address alone, without the rest of the User-Agent, is refused too.
    address = USER_AGENT.rsplit(" ", 1)[-1]

    def echo(method: str, url: str, body: bytes | None, headers: dict[str, str]) -> HttpAnswer:
        del method, url, body, headers
        return HttpAnswer(200, (("x-contact", address),), b"{}")

    with pytest.raises(TransportError, match="echoed"):
        sec.SecClient(USER_AGENT, echo, FakeClock()).request(sec.submissions(CIK))  # ty: ignore[invalid-argument-type]
    assert seen == [{"Accept": "*/*", "User-Agent": USER_AGENT}]
    assert USER_AGENT not in json.dumps(sec.submissions(CIK).document)
    with pytest.raises(ValueError, match="contact"):
        sec.SecClient("no contact here", transport)  # ty: ignore[invalid-argument-type]


def test_a_daily_index_keeps_every_line_and_reads_the_filings() -> None:
    filings = [
        SecFiling(CIK, A1, "10-Q", MONDAY, "2026-09-14T20:01:02.000Z"),
        SecFiling(OTHER, B1, "8-K", MONDAY, "2026-09-14T21:01:02.000Z"),
    ]
    # A line whose file lies under another filer's CIK names no filing.
    crossed = f"101|SYNTHETIC CO 101|8-K|20260914|edgar/data/202/{B1}.txt"
    body = index_bytes(MONDAY, filings, extra=("not|an|index|line", crossed))
    lines = sec.parse_index(body)
    assert [(line.cik, line.form, line.filed, line.accession) for line in lines] == [
        (CIK, "10-Q", MONDAY, A1),
        (OTHER, "8-K", MONDAY, B1),
        (None, None, None, None),
        (None, None, None, None),
    ]
    assert [line.line for line in lines[-2:]] == ["not|an|index|line", crossed]
    # The header is matched by its column names: whitespace, case and the full-index
    # spelling "Filename" read the same lines; an index without the header is refused.
    header = b"CIK|Company Name|Form Type|Date Filed|File Name"
    for variant in (b"CIK | Company Name | Form Type | Date Filed | Filename  ",
                    b"cik|company name|form type|date filed|file name"):  # fmt: skip
        assert sec.parse_index(body.replace(header, variant)) == lines
    with pytest.raises(ValueError, match="no CIK"):
        sec.parse_index(body.replace(header, b"CIK|Company Name|Form Type|Date Filed"))
    with pytest.raises(ValueError, match="dashed rule"):
        sec.parse_index(body.replace(b"-" * 80, b"=" * 80))
    request = sec.daily_index(MONDAY)
    assert sec.classify(request, _response(200, body)) == (COMPLETED, None)
    assert sec.classify(request, _response(404, b"Not Found")) == (NO_DATA, None)
    assert sec.classify(request, _response(200, b"<html>maintenance</html>"))[0] == FAILED
    assert sec.stops_run(_response(403, b"Request Rate Threshold Exceeded"))
    assert not sec.stops_run(_response(404, b""))


def test_company_facts_keep_each_number_as_written() -> None:
    facts = [
        SecFact(A1, "Assets", "123456789012345678901234567890", date(2026, 6, 30), MONDAY),
        SecFact(A1, "EarningsPerShareBasic", "1.10", date(2026, 6, 30), MONDAY,
                start=date(2026, 4, 1)),
        SecFact(A2, "Revenue", "2.5E+9", date(2026, 6, 30), MONDAY),
    ]  # fmt: skip
    read = list(sec.companyfacts_facts(companyfacts_bytes(CIK, facts), CIK))
    assert [(fact.tag, fact.value, fact.start, fact.fy) for fact in read] == [
        ("Assets", "123456789012345678901234567890", None, "2026"),
        ("EarningsPerShareBasic", "1.10", date(2026, 4, 1), "2026"),
        ("Revenue", "2.5E+9", None, "2026"),
    ]
    with pytest.raises(ValueError, match="another CIK"):
        list(sec.companyfacts_facts(companyfacts_bytes(CIK, facts), OTHER))
    text = companyfacts_bytes(CIK, facts[:1]).replace(b'"accn"', b'"accession"')
    with pytest.raises(ValueError, match="unknown or missing"):
        list(sec.companyfacts_facts(text, CIK))
    # A repeated key is ambiguous: the answer is unreadable, not its last value.
    repeated = companyfacts_bytes(CIK, facts[:1]).replace(b'{"cik"', b'{"cik": 202, "cik"', 1)
    assert repeated != companyfacts_bytes(CIK, facts[:1])
    with pytest.raises(ValueError, match="repeats a key"):
        list(sec.companyfacts_facts(repeated, CIK))
    twice = submissions_bytes(CIK, []).replace(b'{"cik"', b'{"cik": "202", "cik"', 1)
    assert sec.classify(sec.submissions(CIK), _response(200, twice))[0] == FAILED
    quoted = companyfacts_bytes(CIK, facts[:1]).replace(b"123456789012345678901234567890", b'"123"')
    with pytest.raises(ValueError, match="no decimal value"):
        list(sec.companyfacts_facts(quoted, CIK))


def _knowledge() -> sec.SecKnowledge:
    knowledge = sec.SecKnowledge()
    read = datetime(2026, 9, 15, 2, tzinfo=UTC)
    knowledge.index(MONDAY, COMPLETED, read)
    for cik, accession, form in ((CIK, A1, "10-Q"), (CIK, A2, "8-K"), (OTHER, B1, "10-K")):
        line = sec.IndexLine("", cik, "SYNTHETIC", form, MONDAY, accession)
        knowledge.file(line, read)
    return knowledge


def test_index_days_are_covered_by_an_answer_or_a_404_after_the_next_day() -> None:
    policy = sec.SecPolicy(lookback_days=7)
    knowledge = sec.SecKnowledge()
    today = date(2026, 9, 17)  # a Thursday
    first = sec.plan_indexes(knowledge, today, policy)
    # Weekdays only, from the lookback day to yesterday.
    assert [item.request.parameters["day"] for item in first] == [
        "2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16",
    ]  # fmt: skip
    knowledge.index(date(2026, 9, 10), COMPLETED, datetime(2026, 9, 11, 3, tzinfo=UTC))
    # A 404 read the next morning may be an index not yet published: asked again.
    knowledge.index(date(2026, 9, 11), NO_DATA, datetime(2026, 9, 12, 3, tzinfo=UTC))
    # A 404 read after the next day ended is a day without an index.
    knowledge.index(date(2026, 9, 14), NO_DATA, datetime(2026, 9, 16, 14, tzinfo=UTC))
    later = sec.plan_indexes(knowledge, today, policy)
    assert [item.request.parameters["day"] for item in later] == [
        "2026-09-11", "2026-09-15", "2026-09-16",
    ]  # fmt: skip
    # Gaps are filled from the earliest known day; --since moves the start.
    since = sec.plan_indexes(knowledge, today, policy, since=date(2026, 9, 9))
    assert since[0].request.parameters["day"] == "2026-09-09"


def test_documents_are_asked_per_filer_for_the_wanted_filings_and_retried_in_window() -> None:
    policy = sec.SecPolicy()
    knowledge = _knowledge()
    now = datetime(2026, 9, 15, 3, tzinfo=UTC)
    filings, facts, counts = sec.plan_documents(knowledge, now, policy, lambda _: True)
    assert [(item.request.endpoint, item.request.parameters["cik"], item.wanted, item.reason)
            for item in (*filings, *facts)] == [
        ("submissions", CIK, (A1, A2), "new_filing"),
        ("submissions", OTHER, (B1,), "new_filing"),
        ("companyfacts", CIK, (A1,), "new_filing"),  # an 8-K has no statements
        ("companyfacts", OTHER, (B1,), "new_filing"),
    ]  # fmt: skip
    assert counts["submissions"]["wanted"] == 3
    # Only registered issuers, when the universe says so.
    only = sec.plan_documents(knowledge, now, policy, lambda cik: cik == OTHER)
    assert [item.request.parameters["cik"] for item in only[0]] == [OTHER]
    assert only[2]["submissions"]["outside_universe"] == 2
    # Asked and listed: done. Asked and not listed: a day later, within the window.
    knowledge.ask(sec.SUBMISSIONS, CIK, now)
    knowledge.ask(sec.SUBMISSIONS, OTHER, now)
    knowledge.listed.update({A1, B1})
    assert sec.plan_documents(knowledge, now + timedelta(hours=1), policy, lambda _: True)[0] == []
    retry, _, _ = sec.plan_documents(knowledge, now + timedelta(days=1), policy, lambda _: True)
    assert [(item.request.parameters["cik"], item.wanted, item.reason) for item in retry] == [
        (CIK, (A2,), "listing_retry")
    ]
    late = now + timedelta(days=policy.listing_days + 1)
    stale, _, counts = sec.plan_documents(knowledge, late, policy, lambda _: True)
    assert stale == []
    assert counts["submissions"]["abandoned"] == 1
    with pytest.raises(ValueError, match="registered or all"):
        sec.SecPolicy(issuers="some")
