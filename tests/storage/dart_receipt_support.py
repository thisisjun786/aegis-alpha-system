"""Synthetic OpenDART financial statement receipts for the ``dart.fnltt`` mappers.

Rows have the source library's DART receipts shape (``kr_identity_support.DART_COLUMNS``).
Every corp code, receipt number, account and amount here is made up.
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

import pyarrow as pa

from aegis_alpha.storage import source_library
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import SourceContent, SourceFile
from tests.storage.kr_identity_support import DART_COLUMNS

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

RETRIEVED: Final = "2026-09-12T08:33:43.268707Z"
TABLE: Final = "receipts"


@dataclass(frozen=True, slots=True)
class Line:
    """One statement line of a ``fnlttSinglAcntAll`` response."""

    account_id: str
    account_nm: str
    amount: str
    cumulative: str | None = None
    sj_div: str = "IS"
    account_detail: str = "-"
    ord: str = "1"
    currency: str = "KRW"
    extra: dict[str, str] = field(default_factory=dict)


def request(corp: str, year: str, report: str, fs_div: str = "CFS") -> str:
    parameters = {"bsns_year": year, "corp_code": corp, "fs_div": fs_div, "reprt_code": report}
    return json.dumps(
        {
            "endpoint": "financials",
            "observation_date": "2026-09-06",
            "parameters_json": json.dumps(parameters, separators=(",", ":"), sort_keys=True),
        }
    )


def body(corp: str, year: str, report: str, number: str, lines: list[Line]) -> bytes:
    """A provider response with status 000 and one list item per line."""
    items = []
    for line in lines:
        item = {
            "rcept_no": number,
            "reprt_code": report,
            "bsns_year": year,
            "corp_code": corp,
            "sj_div": line.sj_div,
            "sj_nm": "synthetic statement",
            "account_id": line.account_id,
            "account_nm": line.account_nm,
            "account_detail": line.account_detail,
            "thstrm_nm": "synthetic term",
            "thstrm_amount": line.amount,
            "ord": line.ord,
            "currency": line.currency,
            **line.extra,
        }
        if line.cumulative is not None:
            item["thstrm_add_amount"] = line.cumulative
        items.append(item)
    document = {"status": "000", "message": "정상", "list": items}
    return json.dumps(document, ensure_ascii=False).encode()


def completed(  # noqa: PLR0913 -- one receipt spells its request and response
    corp: str,
    year: str,
    report: str,
    number: str,
    lines: list[Line],
    *,
    fs_div: str = "CFS",
    retrieved: str = RETRIEVED,
    raw: bytes | None = None,
) -> tuple[object, ...]:
    """A completed financials receipt; ``raw`` replaces the response bytes, keeping the hash."""
    response = body(corp, year, report, number, lines)
    payload = response if raw is None else raw
    return (
        hashlib.sha256(f"{corp}/{year}/{report}/{fs_div}/{number}".encode()).hexdigest(),
        "COMPLETED",
        "financials",
        request(corp, year, report, fs_div),
        "{}",
        base64.b64encode(payload).decode(),
        hashlib.sha256(response).hexdigest(),
        retrieved,
    )


def no_data(
    corp: str, year: str, report: str, *, outcome: str = "NO_DATA", retrieved: str = RETRIEVED
) -> tuple[object, ...]:
    """A financials request the provider answered without a statement (or that failed)."""
    response = json.dumps({"status": "013", "message": "no data"}).encode()
    return (
        hashlib.sha256(f"{corp}/{year}/{report}/{outcome}".encode()).hexdigest(),
        outcome,
        "financials",
        request(corp, year, report),
        "{}",
        base64.b64encode(response).decode(),
        hashlib.sha256(response).hexdigest(),
        retrieved,
    )


def corp_codes() -> tuple[object, ...]:
    """The corp code list row that shares the receipts table."""
    raw = b"synthetic-corp-code-archive"
    return (
        "c" * 64,
        "COMPLETED",
        "corp_codes",
        json.dumps({"endpoint": "corp_codes", "parameters_json": "{}"}),
        "{}",
        base64.b64encode(raw).decode(),
        hashlib.sha256(raw).hexdigest(),
        RETRIEVED,
    )


def table(rows: list[tuple[object, ...]]) -> pa.Table:
    return pa.table(
        {name: [row[index] for row in rows] for index, name in enumerate(DART_COLUMNS)},
        schema=pa.schema([(name, pa.string()) for name in DART_COLUMNS]),
    )


def add_receipts(
    workspace: Workspace, rows: list[tuple[object, ...]], *, tag: str
) -> dict[str, str]:
    """Commit ``rows`` as one linked content source; return its spec pin."""
    _, digest, size = put_raw(workspace.paths.raw, f"synthetic-dart-{tag}".encode())
    content = SourceContent("synthetic", "dart-fnltt", 1, (SourceFile(digest, size),))
    result = source_library.import_content_arrow(workspace, content, TABLE, table(rows).to_reader())
    tables = result["tables"]
    assert isinstance(tables, list)
    return {
        "source_id": content.source_id,
        "source_sha256": content.sha256,
        "table": TABLE,
        "digest": str(tables[0]["digest"]),
    }


DAY_RULE: Final = {
    "rule": "local_day_end@1",
    "basis": "revision",
    "input": "filed_date",
    "args": {"timezone": "Asia/Seoul"},
}


def spec(
    sources: list[dict[str, str]],
    *,
    mapper: str = "dart.fnltt@1",
    dataset: str = "fundamentals.kr.dart",
    parent: str | None = None,
    partition: dict[str, str] | None = None,
) -> tuple[bytes, str]:
    """Exact spec bytes and their SHA-256 for a DART receipts promotion."""
    domain = "fundamentals" if mapper == "dart.fnltt@1" else "filings"
    document = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": domain, "dataset_id": dataset, "parent": parent},
        "sources": sources,
        "mapper": {"name": mapper, "args": {}},
        "partition": partition,
        "time_rules": {"available_at_us": DAY_RULE, "revision_known_at_us": DAY_RULE},
        "decimal_rule": {"value": "decimal_text@1"} if domain == "fundamentals" else {},
        "quality_rules": [],
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": None,
    }
    raw = json.dumps(document, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()
