"""Synthetic KIND, EODHD symbol-list and DART corp-code receipts for the KR registry.

Every company, code and ISIN here is made up; the ISINs only carry valid check digits.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import zipfile
from typing import TYPE_CHECKING, Final

import pyarrow as pa

from aegis_alpha.identity.records import IdentifierType, IdentifierValueError, normalize_identifier
from aegis_alpha.storage import source_library
from aegis_alpha.storage.kr_identity import SourceRows
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import LINK_PREFIX, SourceContent, SourceFile

if TYPE_CHECKING:
    from pathlib import Path

    from aegis_alpha.storage.workspace import Workspace

RETRIEVED: Final = "2026-09-06T05:36:00.646796Z"
KIND_RETRIEVED: Final = "2026-09-06T06:05:48.725955Z"
DART_RETRIEVED: Final = "2026-09-06T06:28:44.526738Z"
DART_COLUMNS: Final = (
    "fingerprint",
    "outcome",
    "endpoint",
    "request_json",
    "receipt_json",
    "raw_base64",
    "raw_sha256",
    "retrieved_at_utc",
)


def isin(body: str, country: str = "KR") -> str:
    """The ISIN ``<country><body><check digit>`` for a nine-character body."""
    for digit in "0123456789":
        candidate = f"{country}{body}{digit}"
        try:
            return normalize_identifier(IdentifierType.ISIN, candidate)
        except IdentifierValueError:
            continue
    raise AssertionError(body)


def bad_check_digit(value: str) -> str:
    return value[:-1] + str((int(value[-1]) + 1) % 10)


def symbol(
    code: str,
    value: str | None,
    *,
    exchange: str = "KO",
    kind: str = "Common Stock",
    currency: str = "KRW",
) -> dict[str, object]:
    return {
        "Code": code,
        "Name": f"Synthetic {code}",
        "Country": "Korea",
        "Exchange": exchange,
        "Currency": currency,
        "Type": kind,
        "Isin": value,
    }


def eodhd_job(  # noqa: PLR0913 -- one synthetic job spells every receipt field
    rows: list[dict[str, object]],
    *,
    exchange: str = "KO",
    delisted: str = "0",
    retrieved: str = RETRIEVED,
    status: str = "RAW_ACQUIRED",
    tool_id: str = "eodhd.exchange_symbols.list.v1.synthetic",
    page_status: int = 200,
) -> tuple[bytes, dict[str, bytes]]:
    """``complete.json`` and the files it lists for one exchange-symbol-list job."""
    parameters = {"EXCHANGE_CODE": exchange, "delisted": delisted, "fmt": "json"}
    job = {
        "dataset": "universe",
        "job_id": f"kr-{exchange}-universe-{delisted}",
        "parameters_json": json.dumps(parameters, separators=(",", ":")),
        "tool_id": tool_id,
        "upstream": "eodhd",
    }
    fingerprint = hashlib.sha256(json.dumps(job, sort_keys=True).encode()).hexdigest()
    page = {"execution_id": "synthetic", "result": {"status_code": page_status, "data": rows}}
    files = {
        f"jobs/{fingerprint}/0000.intent.json": json.dumps({"job": job}).encode(),
        f"jobs/{fingerprint}/0000.raw": json.dumps(page).encode(),
        f"jobs/{fingerprint}/0000.response.json": json.dumps(
            {"retrieved_at_utc": retrieved, "status": 200}
        ).encode(),
    }
    complete = {
        "dataset": "universe",
        "files": [
            {"path": path, "sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw)}
            for path, raw in files.items()
        ],
        "job": job,
        "status": status,
    }
    return json.dumps(complete).encode(), files


def write_eodhd_job(directory: Path, complete: bytes, files: dict[str, bytes]) -> Path:
    directory.mkdir(parents=True)
    (directory / "complete.json").write_bytes(complete)
    for path, raw in files.items():
        (directory / path.rsplit("/", 1)[-1]).write_bytes(raw)
    return directory


_HEADER: Final = (
    "회사명",
    "시장구분",
    "종목코드",
    "업종",
    "주요제품",
    "상장일",
    "결산월",
    "대표자명",
    "홈페이지",
    "지역",
)


def kind_listing(
    rows: list[tuple[str, str, str]],
    *,
    list_id: str = "kind-kospi",
    retrieved: str = KIND_RETRIEVED,
    header: tuple[str, ...] = _HEADER,
    status: int = 200,
) -> tuple[bytes, bytes]:
    """A KIND receipt and its EUC-KR HTML table; each row is (name, short code, listed on)."""
    cells = "".join(f"<th>{name}</th>" for name in header)
    body = "".join(
        "<tr>"
        f"<td>{name}</td><td>\n\t\t유가\n\t</td>"
        f"<td style=\"mso-number-format:'@';\">{code}</td>"
        f"<td>제조업</td><td>합성 제품</td><td>{listed}</td><td>12월</td>"
        "<td>대표</td><td> http://example.invalid </td><td>서울특별시</td></tr>"
        for name, code, listed in rows
    )
    raw = (
        '<html><head><meta charset="euc-kr"/></head><body><table>'
        f"<tr>{cells}</tr>{body}</table></body></html>"
    ).encode("euc-kr")
    fingerprint = hashlib.sha256(list_id.encode()).hexdigest()
    receipt = {
        "request": {"source_id": list_id, "observation_date": "2026-09-06"},
        "raw": {
            "content_sha256": hashlib.sha256(raw).hexdigest(),
            "relative_path": f"{fingerprint}/response.raw",
            "size_bytes": len(raw),
        },
        "retrieved_at_utc": retrieved,
        "status": status,
    }
    return json.dumps(receipt).encode(), raw


def write_kind_listing(directory: Path, receipt: bytes, raw: bytes) -> Path:
    directory.mkdir(parents=True)
    (directory / "response.json").write_bytes(receipt)
    (directory / "response.raw").write_bytes(raw)
    return directory / "response.json"


def corp_code_xml(corps: list[tuple[str, str, str]]) -> bytes:
    """``CORPCODE.xml`` for (corp code, name, stock code) rows; a blank stock is unlisted."""
    items = "".join(
        f"<list><corp_code>{code}</corp_code><corp_name>{name}</corp_name>"
        f"<corp_eng_name>Synthetic</corp_eng_name><stock_code>{stock or ' '}</stock_code>"
        "<modify_date>20260101</modify_date></list>"
        for code, name, stock in corps
    )
    return f'<?xml version="1.0" encoding="UTF-8"?><result>{items}</result>'.encode()


def dart_rows(xml: bytes, *, retrieved: str = DART_RETRIEVED) -> tuple[tuple[object, ...], ...]:
    """A DART receipts table: one completed ``corp_codes`` receipt and one financials row."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        # A fixed member time keeps the archive bytes, and so the source hash, identical.
        archive.writestr(zipfile.ZipInfo("CORPCODE.xml", (2026, 9, 1, 0, 0, 0)), xml)
    raw = buffer.getvalue()
    request = json.dumps({"endpoint": "corp_codes", "parameters_json": "{}"})
    return (
        ("f" * 64, "COMPLETED", "financials", "{}", "{}", "", "0" * 64, retrieved),
        (
            "c" * 64,
            "COMPLETED",
            "corp_codes",
            request,
            "{}",
            base64.b64encode(raw).decode(),
            hashlib.sha256(raw).hexdigest(),
            retrieved,
        ),
    )


def dart_source(
    rows: tuple[tuple[object, ...], ...], source_id: str = "dart-synthetic"
) -> SourceRows:
    return SourceRows(LINK_PREFIX + source_id, DART_COLUMNS, rows)


def commit_dart(workspace: Workspace, rows: tuple[tuple[object, ...], ...]) -> str:
    """Commit a DART receipts table as a linked content source; return its source ID."""
    _, digest, size = put_raw(workspace.paths.raw, b"synthetic-dart-receipts")
    content = SourceContent("synthetic", "dart-receipts", 1, (SourceFile(digest, size),))
    table = pa.table(
        {name: [row[index] for row in rows] for index, name in enumerate(DART_COLUMNS)},
        schema=pa.schema([(name, pa.string()) for name in DART_COLUMNS]),
    )
    source_library.import_content_arrow(workspace, content, "receipts", table.to_reader())
    return content.source_id
