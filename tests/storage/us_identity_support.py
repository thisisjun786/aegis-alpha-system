"""Synthetic Norgate master, FMP profile, SEC submissions and binding sources for the US registry.

Every asset ID, ticker, CIK, CUSIP and company here is made up; CUSIPs and ISINs only
carry valid check digits.
"""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final, cast

import pyarrow as pa

from aegis_alpha.identity.records import IdentifierType, IdentifierValueError, normalize_identifier
from aegis_alpha.storage import source_library
from aegis_alpha.storage.kr_identity import SourceRows
from aegis_alpha.storage.legacy_import.engine import apply_import
from aegis_alpha.storage.legacy_import.manifest import parse_manifest
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import LINK_PREFIX, SourceContent, SourceFile
from aegis_alpha.storage.us_identity import ArchiveOpener, LinkedRows

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from pathlib import Path

    from aegis_alpha.storage.workspace import Workspace

LINKED: Final = 1_790_000_000_000_000
FMP_RETRIEVED: Final = datetime(2026, 8, 29, 8, 37, 46, 271608, tzinfo=UTC)
MASTER_SCHEMA: Final = pa.schema(
    [
        ("assetid", pa.int64()),
        ("symbol", pa.string()),
        ("security_name", pa.string()),
        ("is_delisted", pa.bool_()),
        ("exchange", pa.string()),
        ("currency", pa.string()),
        ("subtype1", pa.string()),
        ("first_date", pa.string()),
        ("last_date", pa.string()),
        ("is_etf", pa.bool_()),
    ]
)
FMP_SCHEMA: Final = pa.schema(
    [
        ("symbol", pa.string()),
        ("companyName", pa.string()),
        ("currency", pa.string()),
        ("cik", pa.string()),
        ("cusip", pa.string()),
        ("isin", pa.string()),
        ("isEtf", pa.bool_()),
        ("retrieved_at_utc", pa.timestamp("us", tz="UTC")),
    ]
)
BINDINGS_SCHEMA: Final = pa.schema(
    [
        ("provider", pa.string()),
        ("namespace", pa.string()),
        ("provider_identifier", pa.string()),
        ("instrument_id", pa.string()),
        ("state", pa.string()),
        ("as_of", pa.timestamp("us", tz="UTC")),
    ]
)


def _checked(kind: IdentifierType, body: str) -> str:
    for digit in "0123456789":
        try:
            return normalize_identifier(kind, f"{body}{digit}")
        except IdentifierValueError:
            continue
    raise AssertionError(body)


def cusip(body: str) -> str:
    """The CUSIP ``<body><check digit>`` for an eight-character body."""
    return _checked(IdentifierType.CUSIP, body)


def us_isin(cusip_value: str) -> str:
    return _checked(IdentifierType.ISIN, f"US{cusip_value}")


def master(  # noqa: PLR0913 -- one synthetic master row spells its columns
    assetid: int,
    symbol: str,
    *,
    delisted: bool | None = False,
    etf: bool | None = None,
    currency: str = "USD",
    name: str | None = None,
    first_date: str | None = "2001-02-03",
    last_date: str | None = None,
) -> dict[str, object]:
    """One master row; a delisted row's ``last_date`` defaults to before ``first_date``."""
    if delisted and last_date is None:
        last_date = "2001-01-31"
    return {
        "assetid": assetid,
        "symbol": symbol,
        "security_name": name or f"Synthetic {symbol}",
        "is_delisted": delisted,
        "exchange": "NYSE Arca" if etf else "Nasdaq",
        "currency": currency,
        "subtype1": "Exchange Traded Product" if etf else "Equity",
        "first_date": first_date,
        "last_date": last_date,
        "is_etf": etf,
    }


def profile(  # noqa: PLR0913 -- one synthetic profile row spells its columns
    symbol: str,
    cik: str | None,
    cusip_value: str | None = None,
    *,
    isin: str | None = None,
    etf: bool = False,
    currency: str = "USD",
    retrieved: datetime | None = FMP_RETRIEVED,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "companyName": f"Synthetic {symbol}",
        "currency": currency,
        "cik": cik,
        "cusip": cusip_value,
        "isin": us_isin(cusip_value) if isin is None and cusip_value else isin,
        "isEtf": etf,
        "retrieved_at_utc": retrieved,
    }


def binding(assetid: int) -> dict[str, object]:
    return {
        "provider": "norgate",
        "namespace": "norgate_assetid",
        "provider_identifier": str(assetid),
        "instrument_id": f"norgate-instrument-{assetid}",
        "state": "resolved",
        "as_of": datetime(2026, 7, 29, tzinfo=UTC),
    }


def table(rows: Sequence[dict[str, object]], schema: pa.Schema) -> pa.Table:
    return pa.table({name: [row[name] for row in rows] for name in schema.names}, schema=schema)


def rows_of(arrow: pa.Table, source_id: str) -> SourceRows:
    """The rows a committed table of ``arrow`` reads back as, cited by ``source_id``."""
    columns = tuple(arrow.schema.names)
    values = tuple(tuple(item[name] for name in columns) for item in arrow.to_pylist())
    return SourceRows(LINK_PREFIX + source_id, columns, values)


def linked(rows: Sequence[dict[str, object]], source_id: str = "norgate-master") -> LinkedRows:
    return LinkedRows(rows_of(table(rows, MASTER_SCHEMA), source_id), LINKED)


def fmp_rows(rows: Sequence[dict[str, object]], source_id: str = "fmp-profiles") -> SourceRows:
    return rows_of(table(rows, FMP_SCHEMA), source_id)


def commit(
    workspace: Workspace, shape: str, name: str, arrow: pa.Table, *, provider: str = "synthetic"
) -> str:
    """Commit ``arrow`` as one linked content source of its own bytes; return its ID."""
    _, digest, size = put_raw(workspace.paths.raw, f"{provider}-{shape}-{arrow.num_rows}".encode())
    content = SourceContent(provider, shape, 1, (SourceFile(digest, size),))
    source_library.import_content_arrow(workspace, content, name, arrow.to_reader())
    return content.source_id


def link_instant(workspace: Workspace, source_id: str) -> int:
    row = workspace.state.execute(
        "SELECT retrieved_at_us FROM source_snapshots WHERE snapshot_id=?",
        (LINK_PREFIX + source_id,),
    ).fetchone()
    return int(row[0])


def submissions(
    filers: Sequence[tuple[str, str, Sequence[str]]],
    *,
    extra: dict[str, bytes] | None = None,
) -> bytes:
    """A submissions archive of (CIK, name, tickers) documents plus ``extra`` members."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for cik, name, tickers in filers:
            body = {
                "cik": cik,
                "name": name,
                "tickers": list(tickers),
                "exchanges": ["Nasdaq"] * len(tickers),
                "filings": {"recent": {}},
            }
            member = zipfile.ZipInfo(f"CIK{cik}.json", (2026, 9, 5, 4, 25, 4))
            archive.writestr(member, json.dumps(body).encode())
        for name, raw in (extra or {}).items():
            archive.writestr(zipfile.ZipInfo(name, (2026, 9, 5, 4, 25, 4)), raw)
    return buffer.getvalue()


def members_of(raw: bytes) -> SourceRows:
    """The member index ``sec.submissions_zip@1`` would commit for ``raw``."""
    rows = []
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        for info in archive.infolist():
            payload = archive.read(info)
            rows.append(
                (
                    info.filename,
                    info.compress_size,
                    info.file_size,
                    info.CRC,
                    "2026-09-05T04:25:04",
                    info.compress_type,
                    hashlib.sha256(payload).hexdigest(),
                )
            )
    columns = ("member", "compressed_size", "size", "crc32", "modified", "compress_type", "sha256")
    return SourceRows(LINK_PREFIX + "sec-submissions-zip-synthetic", columns, tuple(rows))


def in_memory(raw: bytes) -> tuple[LinkedRows, SourceFile, ArchiveOpener]:
    """An SEC input read from ``raw`` itself rather than from a workspace."""
    from contextlib import contextmanager  # noqa: PLC0415

    @contextmanager
    def opener() -> Iterator[io.BytesIO]:
        yield io.BytesIO(raw)

    archive = SourceFile(hashlib.sha256(raw).hexdigest(), len(raw))
    return LinkedRows(members_of(raw), LINKED), archive, opener


def _private(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(payload)
    path.chmod(0o600)
    return path


def import_submissions(workspace: Workspace, directory: Path, raw: bytes) -> str:
    """Import ``raw`` and a receipt with ``aas import legacy``; return the members source ID."""
    archive = _private(directory / "submissions.zip", raw)
    receipt = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    evidence = _private(directory / "receipt.json", json.dumps(receipt).encode())
    entry = {
        "name": "sec",
        "loader": "sec.submissions_zip@1",
        "path": str(archive),
        "args": {"evidence": [str(evidence)]},
        "expect": {},
    }
    document = json.dumps({"schema_version": "aas-legacy-import-v1", "entries": [entry]}).encode()
    manifest = parse_manifest(document, hashlib.sha256(document).hexdigest())
    report = apply_import(workspace, manifest)
    (only,) = cast("list[dict[str, list[dict[str, object]]]]", report["entries"])
    (source,) = only["sources"]
    return str(source["source_id"])
