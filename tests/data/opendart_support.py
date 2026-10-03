"""A synthetic OpenDART and KIND for collector tests: answers, a ledger of calls and a clock.

Every corp code, receipt number and company here is made up.
"""

from __future__ import annotations

import io
import json
import urllib.parse
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Final

from aegis_alpha.data.opendart import HttpAnswer, TransportError

KEY: Final = "synthetic" * 4 + "0000"  # 40 letters and digits, not a credential
JSON: Final = (("content-type", "application/json;charset=UTF-8"),)


def corp_archive(corps: list[tuple[str, str]]) -> bytes:
    """A zipped ``CORPCODE.xml`` of (corp code, stock code) rows; a blank stock is unlisted."""
    items = "".join(
        f"<list><corp_code>{corp}</corp_code><corp_name>합성 {corp}</corp_name>"
        f"<corp_eng_name>Synthetic</corp_eng_name><stock_code>{stock or ' '}</stock_code>"
        "<modify_date>20260101</modify_date></list>"
        for corp, stock in corps
    )
    xml = f'<?xml version="1.0" encoding="UTF-8"?><result>{items}</result>'.encode()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("CORPCODE.xml", xml)
    return buffer.getvalue()


def statements(corp: str, year: str, report: str, number: str) -> bytes:
    """A completed ``fnlttSinglAcntAll`` answer with one income statement line."""
    line = {
        "rcept_no": number,
        "reprt_code": report,
        "bsns_year": year,
        "corp_code": corp,
        "sj_div": "IS",
        "sj_nm": "손익계산서",
        "account_id": "ifrs-full_Revenue",
        "account_nm": "매출액",
        "account_detail": "-",
        "thstrm_nm": "당기",
        "thstrm_amount": "1000",
        "ord": "1",
        "currency": "KRW",
    }
    return json.dumps({"status": "000", "message": "정상", "list": [line]}).encode()


def status(code: str) -> bytes:
    return json.dumps({"status": code, "message": "synthetic"}).encode()


def filing(corp: str, name: str, number: str, filed: str, market: str = "Y") -> dict[str, str]:
    return {
        "corp_cls": market,
        "corp_name": f"합성 {corp}",
        "corp_code": corp,
        "stock_code": "000000",
        "report_nm": name,
        "rcept_no": number,
        "flr_nm": f"합성 {corp}",
        "rcept_dt": filed,
        "rm": "",
    }


def list_page(filings: list[dict[str, str]], *, page: int = 1, total: int = 1) -> bytes:
    return json.dumps(
        {
            "status": "000",
            "message": "정상",
            "page_no": page,
            "page_count": 100,
            "total_count": len(filings),
            "total_page": total,
            "list": filings,
        },
        ensure_ascii=False,
    ).encode()


@dataclass(slots=True)
class FakeClock:
    now: datetime = field(default_factory=lambda: datetime(2026, 10, 3, 1, 0, tzinfo=UTC))

    def __call__(self) -> datetime:
        self.now += timedelta(milliseconds=1)
        return self.now

    def advance(self, **delta: float) -> None:
        self.now += timedelta(**delta)


@dataclass(slots=True)
class FakeProvider:
    """Routes OpenDART and KIND calls to answers; unrouted statements are ``013`` no data."""

    corp_codes: bytes = b""
    lists: dict[str, bytes] = field(default_factory=dict)
    answers: dict[tuple[str, str, str, str], tuple[int, bytes]] = field(default_factory=dict)
    kind: dict[str, tuple[int, bytes]] = field(default_factory=dict)
    fail: set[str] = field(default_factory=set)
    calls: list[tuple[str, dict[str, str]]] = field(default_factory=list)

    def __call__(
        self, method: str, url: str, body: bytes | None, headers: Mapping[str, str]
    ) -> HttpAnswer:
        del headers
        if url.startswith("https://kind.krx.co.kr/"):
            assert method == "POST"
            assert body is not None
            market = dict(urllib.parse.parse_qsl(body.decode()))["marketType"]
            self.calls.append(("kind", {"marketType": market}))
            code, payload = self.kind[market]
            return HttpAnswer(code, (("content-type", "application/vnd.ms-excel"),), payload)
        assert method == "GET"
        path, _, query = url.partition("?")
        parameters = dict(urllib.parse.parse_qsl(query))
        assert parameters.pop("crtfc_key") == KEY
        endpoint = path.rsplit("/", 1)[-1]
        self.calls.append((endpoint, parameters))
        if endpoint in self.fail:
            raise TransportError("synthetic transport failure")
        if endpoint == "corpCode.xml":
            return HttpAnswer(200, (("content-type", "application/zip"),), self.corp_codes)
        if endpoint == "list.json":
            day = parameters["bgn_de"]
            page = parameters["page_no"]
            payload = self.lists.get(f"{day}/{page}", status("013"))
            return HttpAnswer(200, JSON, payload)
        key = tuple(parameters[name] for name in ("corp_code", "bsns_year", "reprt_code", "fs_div"))
        code, payload = self.answers.get(key, (200, status("013")))
        return HttpAnswer(code, JSON, payload)

    def asked(self, endpoint: str) -> list[dict[str, str]]:
        return [parameters for name, parameters in self.calls if name == endpoint]
