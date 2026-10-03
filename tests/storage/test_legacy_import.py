"""``aas import legacy``: synthetic legacy originals become content-addressed sources."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import subprocess
import sys
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from aegis_alpha.storage.legacy_import.engine import (
    LOADERS,
    apply_import,
    plan_import,
    verify_import,
)
from aegis_alpha.storage.legacy_import.files import RetainedBytes
from aegis_alpha.storage.legacy_import.manifest import Manifest, parse_manifest
from aegis_alpha.storage.raw import put_raw_file
from aegis_alpha.storage.source_identity import SourceFile
from aegis_alpha.storage.source_library import read_table
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace

if TYPE_CHECKING:
    from collections.abc import Sequence

_ROOT = Path(__file__).resolve().parents[2]
_HISTORY_COLUMNS = "Open,High,Low,Close,Volume,Turnover,Unadjusted Close,Dividend"

type Report = dict[str, object]


def _private(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(payload)
    path.chmod(0o600)
    return path


def _json(value: object) -> bytes:
    return json.dumps(value, indent=2).encode()


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _record(
    symbol: str, assetid: int, csv: bytes, columns: Sequence[str], rows: int
) -> dict[str, object]:
    lines = csv.decode().splitlines()[1:]
    return {
        "assetid": assetid,
        "columns": columns,
        "first_index": lines[0].split(",")[0] + " 00:00:00",
        "identity": {
            "assetid": assetid,
            "database": "US Equities",
            "securityname": f"{symbol} Common",
            "symbol": symbol,
        },
        "index_name": "Date",
        "last_index": lines[-1].split(",")[0] + " 00:00:00",
        "path": _sha(csv) + ".csv",
        "rows": rows,
        "sha256": _sha(csv),
        "status": "exported",
        "symbol": symbol,
    }


def _history_export(
    root: Path,
    batches: list[list[tuple[str, int, bytes]]],
    *,
    reused: dict[str, dict[str, object]] | None = None,
    unexported: Sequence[str] = (),
) -> Path:
    """Write ``batch-NNN-result.json`` files, the CSV files each lists, the plan and acquisition."""
    columns = _HISTORY_COLUMNS.split(",")
    for number, batch in enumerate(batches):
        records = {}
        for symbol, assetid, csv in batch:
            _private(root / f"batch-{number:03d}" / "history" / (_sha(csv) + ".csv"), csv)
            rows = len(csv.decode().splitlines()) - 1
            records[symbol] = _record(symbol, assetid, csv, columns, rows)
        result = {
            "batch": number,
            "checked_at": "2026-09-09T00:00:00+00:00",
            "checkpoints": [],
            "expected": len(records),
            "recorded": len(records),
            "records": records,
            "unattempted": [],
            "validation_layer": "synthetic",
        }
        _private(root / f"batch-{number:03d}-result.json", _json(result))
    reused = reused or {}
    plan = {
        "pending_symbols": [
            *(symbol for batch in batches for symbol, _, _ in batch),
            *unexported,
        ],
        "reused": reused,
    }
    plan_raw = _json(plan)
    _private(root / f"plan-{_sha(plan_raw)}.json", plan_raw)
    acquisition = _json(
        {
            "batches": len(batches),
            "plan_sha256": _sha(plan_raw),
            "reused_series": len(reused),
            "unattempted": 0,
        }
    )
    _private(root / f"acquisition-{_sha(acquisition)}.json", acquisition)
    return root


def _checkpoint(
    directory: Path, series: list[tuple[str, int, bytes]]
) -> dict[str, dict[str, object]]:
    """A sibling capture: CSV files and the checkpoint listing them; the plan's reuse entries."""
    columns = _HISTORY_COLUMNS.split(",")
    records = []
    for symbol, assetid, csv in series:
        _private(directory / "history" / (_sha(csv) + ".csv"), csv)
        records.append(_record(symbol, assetid, csv, columns, len(csv.splitlines()) - 1))
    raw = _json({"records": records, "schema_version": "aas-norgate-reference-export-v1"})
    _private(directory / "history" / "checkpoints" / f"{_sha(raw)}.json", raw)
    return {
        str(record["symbol"]): {
            "assetid": record["assetid"],
            "manifest_sha256": _sha(raw),
            "path": f"/elsewhere/{directory.name}/history/{record['path']}",
            "rows": record["rows"],
            "sha256": record["sha256"],
        }
        for record in records
    }


def _csv(*rows: str) -> bytes:
    return ("Date," + _HISTORY_COLUMNS + "\n" + "".join(row + "\n" for row in rows)).encode()


_AAA = _csv(
    "2020-08-27,504.05,507.0,500.0,500.04,1.6357e+06,8.2e+08,500.04,0.0",
    "2020-08-28,505.0,510.0,499.0,499.23,4.47336e+07,1.9685556e+09,499.23,0.82",
)
_BBB = _csv("1990-01-02,1.5,1.75,1.25,1.625,100,162.5,1.625,0.0")


def _manifest(entries: list[dict[str, object]]) -> Manifest:
    raw = _json({"schema_version": "aas-legacy-import-v1", "entries": entries})
    return parse_manifest(raw, _sha(raw))


def _entry(name: str, loader: str, path: Path, **expect: int) -> dict[str, object]:
    return {"name": name, "loader": loader, "path": str(path), "args": {}, "expect": expect}


def _tree(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): _sha(path.read_bytes())
        for path in sorted(root.rglob("*"))
        if path.is_file()
        and not path.name.endswith("-shm")
        and not (path.name.endswith("-wal") and path.stat().st_size == 0)
    }


def _sources(report: Report) -> list[dict[str, object]]:
    return [
        source
        for entry in cast("list[dict[str, object]]", report["entries"])
        for source in cast("list[dict[str, object]]", entry["sources"])
    ]


def _only(report: Report) -> dict[str, object]:
    (entry,) = cast("list[dict[str, object]]", report["entries"])
    return entry


def _rows(workspace: Workspace, source: dict[str, object]) -> list[dict[str, object]]:
    read = read_table(workspace, str(source["source_id"]), str(source["table"]), 10)
    return cast("list[dict[str, object]]", read["rows"])


@pytest.fixture
def home(tmp_path: Path) -> Path:
    root = tmp_path / "aas"
    initialize(root)
    return root


def _export(tmp_path: Path) -> Path:
    return _history_export(
        tmp_path / "legacy" / "equity-none",
        [[("AAA", 131684, _AAA)], [("BBB", 255128, _BBB), ("CCC", 255129, _BBB)]],
    )


def test_plan_reads_originals_and_writes_nothing(tmp_path: Path, home: Path) -> None:
    root = _export(tmp_path)
    manifest = _manifest(
        [_entry("equity", "norgate.history_export@1", root, units=2, records=3, rows=4)],
    )
    before_home, before_root = _tree(home), _tree(root)
    report = plan_import(manifest)
    assert _tree(home) == before_home
    assert _tree(root) == before_root
    entry = _only(report)
    assert entry["metrics"] == {
        "missing_series": 0,
        "planned_series": 3,
        "records": 3,
        "repeated_series": 0,
        "rows": 4,
        "unplanned_series": 0,
        "units": 2,
    }
    assert all(cast("dict", check)["matched"] for check in cast("dict", entry["expect"]).values())
    assert report["reconciled"] is True
    assert report["provider_calls"] == 0
    # Two batches share one CSV byte string; the file is counted once.
    assert entry["files"] == len([*root.glob("batch-*-result.json"), *root.rglob("*.csv")])
    # The plan and acquisition record the loader read are retained; nothing is left uncovered.
    assert cast("dict", entry["retained"]) == {
        "files": 2,
        "bytes": sum(path.stat().st_size for path in root.glob("[pa][lc]*-*.json")),
    }
    assert cast("dict", entry["uncovered"]) == {"files": 0, "bytes": 0, "paths": []}
    sources = _sources(report)
    assert [source["rows"] for source in sources] == [2, 2]
    assert all(str(source["source_id"]).startswith("norgate-history-csv-") for source in sources)


def test_apply_commits_content_sources_and_reruns_reuse(tmp_path: Path, home: Path) -> None:
    root = _export(tmp_path)
    manifest = _manifest([_entry("equity", "norgate.history_export@1", root)])
    planned = _sources(plan_import(manifest))
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        applied = apply_import(workspace, manifest)
        again = apply_import(workspace, manifest)
        commits = workspace.market.execute("SELECT count(*) FROM source_library_commits").fetchone()
        links = workspace.state.execute(
            "SELECT count(*) FROM source_snapshots WHERE provider='source-library'"
        ).fetchone()
    assert [(s["source_id"], s["rows"], s["digest"]) for s in _sources(applied)] == [
        (s["source_id"], s["rows"], s["digest"]) for s in planned
    ]
    assert [s["reused"] for s in _sources(applied)] == [False, False]
    assert [s["reused"] for s in _sources(again)] == [True, True]
    assert commits == (2,)
    assert links is not None
    assert tuple(links) == (2,)
    # Every original is retained in raw/ under its own hash, and the manifest beside them.
    for path in [*root.rglob("*.csv"), *root.glob("*.json")]:
        digest = _sha(path.read_bytes())
        assert (home / "raw" / digest[:2] / digest).read_bytes() == path.read_bytes()


def test_history_rows_keep_original_text(tmp_path: Path, home: Path) -> None:
    root = _export(tmp_path)
    manifest = _manifest([_entry("equity", "norgate.history_export@1", root)])
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        first = _sources(apply_import(workspace, manifest))[0]
        rows = _rows(workspace, first)
    assert rows == [
        {
            "assetid": 131684,
            "symbol": "AAA",
            "database": "US Equities",
            "security_name": "AAA Common",
            "csv_sha256": _sha(_AAA),
            "date": "2020-08-27",
            "open": "504.05",
            "high": "507.0",
            "low": "500.0",
            "close": "500.04",
            "volume": "1.6357e+06",
            "turnover": "8.2e+08",
            "unadjusted_close": "500.04",
            "dividend": "0.0",
            "delivery_month": None,
            "open_interest": None,
        },
        {
            "assetid": 131684,
            "symbol": "AAA",
            "database": "US Equities",
            "security_name": "AAA Common",
            "csv_sha256": _sha(_AAA),
            "date": "2020-08-28",
            "open": "505.0",
            "high": "510.0",
            "low": "499.0",
            "close": "499.23",
            "volume": "4.47336e+07",
            "turnover": "1.9685556e+09",
            "unadjusted_close": "499.23",
            "dividend": "0.82",
            "delivery_month": None,
            "open_interest": None,
        },
    ]


def test_source_ids_follow_bytes_not_paths(tmp_path: Path) -> None:
    first = _export(tmp_path / "one")
    second = _export(tmp_path / "two")
    one = _sources(plan_import(_manifest([_entry("a", "norgate.history_export@1", first)])))
    two = _sources(
        plan_import(_manifest([_entry("other-name", "norgate.history_export@1", second)]))
    )
    assert [s["source_id"] for s in one] == [s["source_id"] for s in two]
    changed = _history_export(
        tmp_path / "three", [[("AAA", 131684, _AAA.replace(b"499.23", b"499.24"))]]
    )
    three = _sources(plan_import(_manifest([_entry("a", "norgate.history_export@1", changed)])))
    assert three[0]["source_id"] != one[0]["source_id"]


def _refusal(report: Report) -> str:
    refusals = cast("list[dict[str, str]]", _only(report)["refusals"])
    assert len(refusals) == 1
    return refusals[0]["reason"]


def test_unit_contradicting_its_index_is_refused(tmp_path: Path, home: Path) -> None:
    root = _export(tmp_path)
    manifest = _manifest([_entry("equity", "norgate.history_export@1", root)])
    result_path = root / "batch-001-result.json"
    result = json.loads(result_path.read_bytes())
    result["records"]["BBB"]["rows"] = 2
    _private(result_path, _json(result))
    assert "recorded rows" in _refusal(plan_import(manifest))
    # The other batch still plans, and an apply stops at the refused unit.
    assert len(_sources(plan_import(manifest))) == 1
    with (
        open_workspace(home, writable=True, strategy_write=True) as workspace,
        pytest.raises(ValueError, match="recorded rows"),
    ):
        apply_import(workspace, manifest)
    result["records"]["BBB"]["rows"] = 1
    _private(result_path, _json(result))
    csv_path = root / "batch-001" / "history" / (_sha(_BBB) + ".csv")
    _private(csv_path, _BBB.replace(b"1.625", b"1.626"))
    assert "recorded hash" in _refusal(plan_import(manifest))
    _private(csv_path, _BBB)
    result["records"]["BBB"]["columns"] = [*_HISTORY_COLUMNS.split(","), "Adjusted"]
    _private(result_path, _json(result))
    assert "unknown or repeated column" in _refusal(plan_import(manifest))
    # A malformed index is a refused unit, not an aborted plan.
    del result["records"]["BBB"]["columns"]
    _private(result_path, _json(result))
    assert "no column list" in _refusal(plan_import(manifest))
    # A group-readable original is refused, exactly as the apply's raw admission would.
    result["records"]["BBB"]["columns"] = _HISTORY_COLUMNS.split(",")
    _private(result_path, _json(result))
    csv_path.chmod(0o640)
    assert "private" in _refusal(plan_import(manifest))
    # A batch directory that no result file describes leaves the export incomplete.
    csv_path.chmod(0o600)
    (root / "batch-007").mkdir(mode=0o700)
    assert "has no result file" in _refusal(plan_import(manifest))


def test_verify_requires_committed_identical_linked_sources(tmp_path: Path, home: Path) -> None:
    root = _export(tmp_path)
    manifest = _manifest([_entry("equity", "norgate.history_export@1", root)])
    with open_workspace(home) as workspace:
        before = verify_import(workspace, manifest)
    assert before["complete"] is False
    # Every planned source and both retained index files (plan, acquisition) are unmatched.
    assert before["unmatched"] == len(_sources(before)) + 2
    assert {s["status"] for s in _sources(before)} == {"missing"}
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        apply_import(workspace, manifest)
    with open_workspace(home) as workspace:
        after = verify_import(workspace, manifest)
    assert after["complete"] is True
    assert after["unmatched"] == 0
    assert {s["status"] for s in _sources(after)} == {"committed"}
    # A stored row that no longer matches its commit marker fails the source.
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        (manifest_json,) = cast(
            "tuple[str]",
            workspace.market.execute(
                "SELECT manifest_json FROM source_library_commits WHERE source_id=?",
                [_sources(after)[0]["source_id"]],
            ).fetchone(),
        )
        target = json.loads(manifest_json)["tables"][0]["target"]
        update = f"UPDATE \"{target}\" SET close = ? WHERE date = '2020-08-28'"  # noqa: S608 -- synthetic table name from the marker
        workspace.market.execute(update, ["1"])
    with open_workspace(home) as workspace:
        altered = verify_import(workspace, manifest)
    assert altered["complete"] is False
    assert [s["status"] for s in _sources(altered)] == ["table_mismatch", "committed"]
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        workspace.market.execute(update, ["499.23"])
    # A retained original that no longer matches its address fails its link.
    digest = _sha(_BBB)
    retained = home / "raw" / digest[:2] / digest
    retained.chmod(0o600)
    retained.write_bytes(_BBB.replace(b"1.625", b"1.626"))
    with open_workspace(home) as workspace:
        corrupt = verify_import(workspace, manifest)
    assert corrupt["complete"] is False
    assert [s["status"] for s in _sources(corrupt)] == ["committed", "link_corrupt"]


def _capture(directory: Path, jobs: list[tuple[str, int, str, str]], *, other: bool) -> str:
    """One capture directory: request, manifest, journal and its index-constituent files."""
    request = _json({"family": "membership", "jobs": [job[0] for job in jobs]})
    _private(directory / "request.json", request)
    lines = []
    for job_id, assetid, indexname, symbol in jobs:
        body = "Date,Index Constituent\n1990-01-02,0\n1990-01-03,1\n"
        raw = gzip.compress(body.encode(), mtime=0)
        _private(directory / (_sha(raw) + ".csv.gz"), raw)
        lines.append(
            {
                "job": {
                    "assetid": assetid,
                    "id": job_id,
                    "kwargs": {"indexname": indexname},
                    "method": "index_constituent_timeseries",
                    "symbol": symbol,
                },
                "payload": {
                    "columns": ["Index Constituent"],
                    "csv_sha256": _sha(body.encode()),
                    "file": _sha(raw) + ".csv.gz",
                    "first_index": "1990-01-02 00:00:00",
                    "index_name": "Date",
                    "last_index": "1990-01-03 00:00:00",
                    "rows": 2,
                    "sha256": _sha(raw),
                    "status": "captured",
                },
            }
        )
    if other:
        lines.append({"job": {"assetid": 1, "id": "x", "kwargs": {}, "method": "currency"}})
    journal = "".join(json.dumps(line) + "\n" for line in lines).encode()
    _private(directory / "receipts.jsonl", journal)
    manifest = _json({"journal_sha256": _sha(journal), "request_sha256": _sha(request)})
    _private(directory / ("manifest-" + _sha(manifest) + ".json"), manifest)
    return _sha(request)


def _membership(tmp_path: Path) -> Path:
    root = tmp_path / "legacy" / "preservation"
    pairs = [(10, "S&P 500"), (11, "S&P 500"), (10, "Nasdaq 100"), (12, "Russell 3000")]
    plan = {
        "pairs": [{"assetid": a, "indexname": i, "symbol": f"S{a}"} for a, i in pairs],
        "pairs_count": len(pairs),
    }
    _private(root / "membership-plan-0001.json", _json(plan))
    request = _capture(
        root / "batch-attempts" / "req-1",
        [("j1", 10, "S&P 500", "S10"), ("j2", 11, "S&P 500", "S11")],
        other=False,
    )
    result = {
        "family": "membership",
        "output": "/elsewhere/batch-attempts/req-1",
        "request_sha256": request,
        "verification": {
            "journal_sha256": json.loads(
                next((root / "batch-attempts" / "req-1").glob("manifest-*")).read_bytes()
            )["journal_sha256"]
        },
    }
    _private(root / "batch-results" / (request + ".json"), _json(result))
    _private(root / "batch-results" / "other.json", _json({"family": "fundamentals"}))
    _capture(root / "pilot-capture", [("j3", 10, "Nasdaq 100", "S10")], other=True)
    return root


def test_membership_reconciles_planned_pairs(tmp_path: Path, home: Path) -> None:
    root = _membership(tmp_path)
    entry = _entry("membership", "norgate.index_membership@1", root, pairs=3, missing_pairs=1)
    entry["args"] = {"include": ["pilot-capture"]}
    manifest = _manifest([entry])
    report = plan_import(manifest)
    assert _only(report)["metrics"] == {
        "missing_pairs": 1,
        "pairs": 3,
        "planned_pairs": 4,
        "repeated_pairs": 0,
        "rows": 6,
        "unplanned_pairs": 0,
        "units": 2,
    }
    assert report["reconciled"] is True
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        sources = _sources(apply_import(workspace, manifest))
        rows = _rows(workspace, sources[1])
    assert [
        (row["assetid"], row["indexname"], row["date"], row["index_constituent"]) for row in rows
    ] == [
        (10, "Nasdaq 100", "1990-01-02", "0"),
        (10, "Nasdaq 100", "1990-01-03", "1"),
    ]
    # A journal that no longer matches its manifest is refused.
    journal = root / "pilot-capture" / "receipts.jsonl"
    _private(journal, journal.read_bytes() + b'{"job": {"method": "currency"}}\n')
    assert "journal does not match" in _refusal(plan_import(manifest))


def _archive(tmp_path: Path) -> tuple[Path, Path, bytes]:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(zipfile.ZipInfo("CIK0000000001.json", (2026, 9, 5, 4, 25, 4)), b"{}")
        archive.writestr(zipfile.ZipInfo("placeholder.txt", (2026, 9, 5, 4, 25, 4)), b"x")
    payload = buffer.getvalue()
    archive_path = _private(tmp_path / "legacy" / "sec" / "submissions.zip", payload)
    receipt = {"bytes": len(payload), "json_members": 1, "members": 2, "sha256": _sha(payload)}
    receipt_path = _private(tmp_path / "legacy" / "sec" / "receipt.json", _json(receipt))
    return archive_path, receipt_path, payload


def test_sec_archive_indexes_members_and_checks_receipts(tmp_path: Path, home: Path) -> None:
    archive_path, receipt_path, _ = _archive(tmp_path)
    entry = _entry("sec", "sec.submissions_zip@1", archive_path, json_members=1, members=2)
    entry["args"] = {"evidence": [str(receipt_path)]}
    manifest = _manifest([entry])
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        report = apply_import(workspace, manifest)
        (source,) = _sources(report)
        rows = _rows(workspace, source)
    assert str(source["source_id"]).startswith("sec-submissions-zip-")
    assert report["reconciled"] is True
    assert [(row["member"], row["size"], row["sha256"], row["modified"]) for row in rows] == [
        ("CIK0000000001.json", 2, _sha(b"{}"), "2026-09-05T04:25:04"),
        ("placeholder.txt", 1, _sha(b"x"), "2026-09-05T04:25:04"),
    ]
    _private(receipt_path, _json({"sha256": "0" * 64}))
    assert "disagrees on sha256" in _refusal(plan_import(manifest))


def _small_originals(
    tmp_path: Path,
) -> tuple[dict[str, dict[str, object]], pa.Table, dict[str, object]]:
    """FRED, KR public, FMP and identity originals; their entries keyed by loader."""
    legacy = tmp_path / "legacy"
    fred = _private(
        legacy / "DEXKOUS.csv", b"observation_date,DEXKOUS\n2011-10-03,1180.00\n2011-10-04,\n"
    )
    body = b"<html>listing</html>"
    request = {
        "end": None,
        "observation_date": "2026-09-06",
        "source_id": "kind-kospi",
        "start": None,
    }
    raw_ref = {
        "content_sha256": _sha(body),
        "relative_path": "x/response.raw",
        "size_bytes": len(body),
    }
    listing = {
        "company_name": "Alpha",
        "fiscal_month": 12,
        "listed_on": "2026-08-25",
        "market": "KOSPI",
        "raw_fields": [["회사명", "Alpha"]],
        "source_rows": [[["회사명", "Alpha"]]],
        "stock_code": "000001",
    }
    korea = legacy / "korea"
    _private(korea / "r1" / "request.json", _json(request))
    _private(korea / "r1" / "response.json", _json({"raw": raw_ref}))
    _private(korea / "r1" / "response.raw", body)
    _private(
        korea / "r1" / "normalized.v1.json",
        _json(
            {
                "raw": raw_ref,
                "request": request,
                "row_count": 1,
                "rows": [listing],
                "schema_version": 1,
            }
        ),
    )
    dataset = legacy / "fmp" / "run_id=fmp-run-1" / "fmp_price_eod_non_split_adjusted"
    _private(
        dataset / "history-index.json",
        _json({"dataset": "fmp_price_eod_non_split_adjusted", "schema_version": 1, "symbols": {}}),
    )
    table = pa.table(
        {
            "symbol": ["AAA"],
            "date": pa.array([date(2026, 8, 28)], pa.date32()),
            "adjOpen": [1.5],
            "adjHigh": [1.75],
            "adjLow": [1.25],
            "adjClose": [1.625],
            "volume": pa.array([10], pa.int64()),
            "provider": ["fmp"],
            "source_receipt_id": ["sha256:" + "a" * 64],
            "raw_content_sha256": ["b" * 64],
            "retrieved_at_utc": pa.array(
                [datetime(2026, 8, 30, tzinfo=UTC)], pa.timestamp("us", tz="UTC")
            ),
        }
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    _private(dataset / "part-00000.parquet", sink.getvalue().to_pybytes())
    mapping = {
        "effective_end": None,
        "effective_start": "1990-01-02T00:00:00+00:00",
        "instrument_id": "norgate-instrument-1",
        "mapping_id": "norgate-assetid-1-mapping",
        "mapping_sha256": "c" * 64,
        "namespace": "norgate_assetid",
        "provider": "norgate",
        "provider_identifier": "1",
        "source_snapshot_id": "norgate-snapshot",
    }
    binding = {
        **mapping,
        "instrument_commitment_sha256": "d" * 64,
        "issuer_commitment_sha256": "e" * 64,
        "issuer_id": "norgate-provisional-issuer-1",
    }
    authority = _private(
        legacy / "identity-authority.json",
        _json(
            {
                "counts": {"instruments": 1, "provider_mappings": 1},
                "issuer_bindings": [binding],
                "mappings": [mapping],
            }
        ),
    )
    entries = {
        "fred.series_csv@1": _entry("fred", "fred.series_csv@1", fred, rows=2),
        "korea.public_response@1": _entry(
            "korea", "korea.public_response@1", korea, units=1, listings=1
        ),
        "fmp.price_eod_non_split@1": _entry(
            "fmp", "fmp.price_eod_non_split@1", legacy / "fmp", rows=1
        ),
        "norgate.identity_authority@1": _entry(
            "identity", "norgate.identity_authority@1", authority, mappings=1, issuer_bindings=1
        ),
    }
    return entries, table, mapping


def test_small_loaders_map_their_originals(tmp_path: Path, home: Path) -> None:
    entries, table, mapping = _small_originals(tmp_path)
    manifest = _manifest(list(entries.values()))
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        report = apply_import(workspace, manifest)
        sources = _sources(report)
        tables = {str(s["source_id"]).rsplit("-", 1)[0]: _rows(workspace, s) for s in sources}
    assert report["reconciled"] is True
    assert tables["fred-series-csv"] == [
        {"series_id": "DEXKOUS", "observation_date": "2011-10-03", "value": "1180.00"},
        {"series_id": "DEXKOUS", "observation_date": "2011-10-04", "value": ""},
    ]
    assert tables["kind-listings"] == [
        {
            "stock_code": "000001",
            "company_name": "Alpha",
            "market": "KOSPI",
            "listed_on": "2026-08-25",
            "fiscal_month": 12,
            "raw_fields": '[["\\ud68c\\uc0ac\\uba85","Alpha"]]',
            "source_rows": '[[["\\ud68c\\uc0ac\\uba85","Alpha"]]]',
        }
    ]
    assert tables["fmp-price-eod-non-split"] == [
        {**row, "date": "2026-08-28", "retrieved_at_utc": "2026-08-30 00:00:00.000000Z"}
        for row in table.to_pylist()
    ]
    assert tables["norgate-identity-mappings"] == [mapping]
    assert (
        tables["norgate-identity-issuer-bindings"][0]["issuer_id"] == "norgate-provisional-issuer-1"
    )
    # Both identity tables come from one file, so their IDs share its hex.
    ids = sorted(str(s["source_id"]) for s in sources if "identity" in str(s["source_id"]))
    assert ids[0].rsplit("-", 1)[1] == ids[1].rsplit("-", 1)[1]


def test_manifest_is_exact_strict_json() -> None:
    raw = _json({"schema_version": "aas-legacy-import-v1", "entries": []})
    with pytest.raises(ValueError, match="SHA-256"):
        parse_manifest(raw, "0" * 64)
    with pytest.raises(ValueError, match="at least one entry"):
        parse_manifest(raw, _sha(raw))
    entry = {"name": "a", "loader": "x@1", "path": "relative", "args": {}, "expect": {}}
    raw = _json({"schema_version": "aas-legacy-import-v1", "entries": [entry]})
    with pytest.raises(ValueError, match="absolute"):
        parse_manifest(raw, _sha(raw))
    raw = b'{"schema_version":"aas-legacy-import-v1","schema_version":"x","entries":[]}'
    with pytest.raises(ValueError, match="strict JSON"):
        parse_manifest(raw, _sha(raw))
    entry = {"name": "a", "loader": "x@1", "path": "/a", "args": {}, "expect": {}, "more": 1}
    raw = _json({"schema_version": "aas-legacy-import-v1", "entries": [entry]})
    with pytest.raises(ValueError, match="exactly"):
        parse_manifest(raw, _sha(raw))
    entry = {"name": "a", "loader": "unknown@1", "path": "/a", "args": {}, "expect": {}}
    raw = _json({"schema_version": "aas-legacy-import-v1", "entries": [entry]})
    with pytest.raises(ValueError, match="unknown legacy loader"):
        plan_import(parse_manifest(raw, _sha(raw)))
    # Arguments and expected metrics are checked against the loader before anything is read.
    for field, value, message in (
        ("args", {"include": []}, "no argument"),
        ("expect", {"pairs": 1}, "no metric"),
    ):
        entry = {"name": "a", "loader": "fred.series_csv@1", "path": "/a", "args": {}, "expect": {}}
        entry[field] = value
        raw = _json({"schema_version": "aas-legacy-import-v1", "entries": [entry]})
        with pytest.raises(ValueError, match=message):
            plan_import(parse_manifest(raw, _sha(raw)))


def _cli(*args: str, home: Path, code: int = 0) -> dict[str, object]:
    result = subprocess.run(  # noqa: S603 -- fixed interpreter, temporary synthetic home
        [sys.executable, "-m", "aegis_alpha", "import", "legacy", *args],
        env={**os.environ, "AAS_HOME": str(home), "PYTHONPATH": str(_ROOT / "src")},
        cwd=home.parent,
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == code, result.stderr
    return json.loads(result.stdout)


def test_cli_plans_applies_and_verifies(tmp_path: Path, home: Path) -> None:
    root = _export(tmp_path)
    raw = _json(
        {
            "schema_version": "aas-legacy-import-v1",
            "entries": [_entry("equity", "norgate.history_export@1", root, rows=4)],
        }
    )
    path = _private(tmp_path / "manifest.json", raw)
    before = _tree(home)
    # A plan opens no installation: it runs with a home that does not exist.
    planned = _cli("--manifest", str(path), "--sha256", _sha(raw), "--plan", home=tmp_path / "none")
    assert not (tmp_path / "none").exists()
    assert _tree(home) == before
    assert planned["reconciled"] is True
    verified = _cli("--manifest", str(path), "--sha256", _sha(raw), "--verify", home=home, code=1)
    assert verified["complete"] is False
    assert _tree(home) == before
    applied = _cli("--manifest", str(path), "--sha256", _sha(raw), home=home)
    assert applied["mode"] == "apply"
    verified = _cli("--manifest", str(path), "--sha256", _sha(raw), "--verify", home=home)
    assert verified["complete"] is True
    assert _sha(raw) in {path.name for path in (home / "raw").rglob("*")}
    # An expected count that does not match fails the plan's exit status, after the report.
    raw = _json(
        {
            "schema_version": "aas-legacy-import-v1",
            "entries": [_entry("equity", "norgate.history_export@1", root, rows=5)],
        }
    )
    path = _private(tmp_path / "manifest.json", raw)
    planned = _cli("--manifest", str(path), "--sha256", _sha(raw), "--plan", home=home, code=1)
    assert planned["reconciled"] is False


_SPX = _csv("1990-01-02,353.4,355.67,351.35,355.67,0,0,355.67,0.0")


def test_reused_series_are_units_until_imported(tmp_path: Path, home: Path) -> None:
    legacy = tmp_path / "legacy"
    reused = _checkpoint(legacy / "history-selected-1", [("$SPX", 392, _SPX)])
    root = _history_export(legacy / "index", [[("AAA", 131684, _AAA)]], reused=reused)
    manifest = _manifest(
        [_entry("index", "norgate.history_export@1", root, planned_series=2, records=2, rows=3)]
    )
    report = plan_import(manifest)
    entry = _only(report)
    assert report["reconciled"] is True
    assert cast("dict", entry["metrics"])["missing_series"] == 0
    assert [s["unit"] for s in _sources(report)] == [
        "batch-000-result.json",
        "reused/history-selected-1/" + str(reused["$SPX"]["manifest_sha256"]),
    ]
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        sources = _sources(apply_import(workspace, manifest))
        rows = _rows(workspace, sources[1])
    assert [(row["symbol"], row["assetid"], row["close"]) for row in rows] == [
        ("$SPX", 392, "355.67")
    ]
    with open_workspace(home) as workspace:
        assert verify_import(workspace, manifest)["complete"] is True
    # Without its checkpoint the reused series is a refused unit and a missing series.
    checkpoint = next((legacy / "history-selected-1" / "history" / "checkpoints").iterdir())
    payload = checkpoint.read_bytes()
    checkpoint.unlink()
    report = plan_import(manifest)
    assert cast("dict", _only(report)["metrics"])["missing_series"] == 1
    assert report["reconciled"] is False
    # A checkpoint that disagrees with the plan is refused, never read in its place.
    record = json.loads(payload)
    record["records"][0]["rows"] = 2
    changed = _json(record)
    _private(checkpoint.parent / f"{_sha(changed)}.json", changed)
    reused["$SPX"]["manifest_sha256"] = _sha(changed)
    other = _history_export(legacy / "other", [[("AAA", 131684, _AAA)]], reused=reused)
    report = plan_import(_manifest([_entry("other", "norgate.history_export@1", other)]))
    assert "does not hold the planned $SPX" in _refusal(report)
    # An export that only reuses series is one checkpoint unit.
    only = _history_export(legacy / "only", [], reused=reused)
    report = plan_import(_manifest([_entry("only", "norgate.history_export@1", only)]))
    assert "does not hold the planned $SPX" in _refusal(report)
    reused = _checkpoint(legacy / "history-selected-2", [("$SPX", 392, _SPX)])
    only = _history_export(legacy / "only-2", [], reused=reused)
    report = plan_import(_manifest([_entry("only", "norgate.history_export@1", only)]))
    assert report["reconciled"] is True
    assert cast("dict", _only(report)["metrics"])["records"] == 1
    # A planned series that no batch exported fails reconciliation unless the manifest pins it.
    gap = _history_export(legacy / "gap", [[("AAA", 131684, _AAA)]], unexported=["ZZZ"])
    report = plan_import(_manifest([_entry("gap", "norgate.history_export@1", gap)]))
    assert cast("dict", _only(report)["expect"])["missing_series"] == {
        "expected": 0,
        "observed": 1,
        "matched": False,
    }
    assert report["reconciled"] is False
    pinned = _manifest([_entry("gap", "norgate.history_export@1", gap, missing_series=1)])
    assert plan_import(pinned)["reconciled"] is True
    # An acquisition record that disagrees with its plan refuses the whole entry.
    acquisition = next(root.glob("acquisition-*.json"))
    document = json.loads(acquisition.read_bytes())
    document["reused_series"] = 0
    acquisition.unlink()
    raw = _json(document)
    _private(root / f"acquisition-{_sha(raw)}.json", raw)
    assert "acquisition record disagrees" in _refusal(plan_import(manifest))


def test_uncovered_files_keep_verify_incomplete(tmp_path: Path, home: Path) -> None:
    root = _export(tmp_path)
    _private(root / "collect.py", b"print('collect')\n")
    _private(root / "batch-000" / "history" / (_sha(_BBB) + ".csv"), _BBB)
    _private(root / "batch-000" / "stderr.txt", b"")
    manifest = _manifest([_entry("equity", "norgate.history_export@1", root)])
    planned = _only(plan_import(manifest))
    planned_paths = cast("dict", planned["uncovered"])["paths"]
    assert planned["uncovered"] == {
        "files": 3,
        "bytes": len(b"print('collect')\n") + len(_BBB),
        "paths": [
            f"batch-000/history/{_sha(_BBB)}.csv",
            "batch-000/stderr.txt",
            "collect.py",
        ],
    }
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        apply_import(workspace, manifest)
    with open_workspace(home) as workspace:
        report = verify_import(workspace, manifest)
    assert report["unmatched"] == 0
    assert cast("dict", report["totals"])["uncovered_files"] == len(planned_paths)
    assert report["complete"] is False
    # The operator retains or excludes each leftover explicitly; retained files go to raw/.
    entry = _entry("equity", "norgate.history_export@1", root)
    entry["retain"] = ["*.py", "batch-*/history/*.csv"]
    entry["exclude"] = ["batch-*/stderr.txt"]
    manifest = _manifest([entry])
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        applied = _only(apply_import(workspace, manifest))
    assert applied["retained"] == {
        "files": 4,
        "bytes": sum(
            path.stat().st_size
            for path in [root / "collect.py", *root.glob("plan-*"), *root.glob("acquisition-*")]
        )
        + len(_BBB),
    }
    assert applied["excluded"] == {"files": 1, "bytes": 0}
    digest = _sha(b"print('collect')\n")
    assert (home / "raw" / digest[:2] / digest).read_bytes() == b"print('collect')\n"
    with open_workspace(home) as workspace:
        assert verify_import(workspace, manifest)["complete"] is True
    # A retained file whose raw copy is gone is unmatched.
    (home / "raw" / digest[:2] / digest).unlink()
    with open_workspace(home) as workspace:
        report = verify_import(workspace, manifest)
    assert report["complete"] is False
    assert report["unmatched_sources"] == [
        {"entry": "equity", "file": "collect.py", "status": "not_retained"}
    ]


def _all_loaders(tmp_path: Path) -> dict[str, dict[str, object]]:
    entries, _, _ = _small_originals(tmp_path / "small")
    membership = _entry(
        "membership", "norgate.index_membership@1", _membership(tmp_path), missing_pairs=1
    )
    membership["args"] = {"include": ["pilot-capture"]}
    archive_path, receipt_path, _ = _archive(tmp_path)
    for loader in ("sec.submissions_zip@1", "sec.companyfacts_zip@1"):
        name = loader.split(".")[1].split("_")[0]
        entries[loader] = {
            **_entry(name, loader, archive_path, members=2),
            "args": {"evidence": [str(receipt_path)]},
        }
    entries["norgate.history_export@1"] = _entry(
        "equity", "norgate.history_export@1", _export(tmp_path), rows=4
    )
    entries["norgate.index_membership@1"] = membership
    return entries


@pytest.mark.parametrize("loader", sorted(LOADERS))
def test_every_loader_plans_applies_and_verifies_alike(
    tmp_path: Path, home: Path, loader: str
) -> None:
    entries = _all_loaders(tmp_path)
    assert set(entries) == set(LOADERS)
    manifest = _manifest([entries[loader]])
    planned = plan_import(manifest)
    assert planned["reconciled"] is True
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        applied = apply_import(workspace, manifest)
        again = apply_import(workspace, manifest)
    with open_workspace(home) as workspace:
        verified = verify_import(workspace, manifest)

    def identities(report: Report) -> list[tuple[object, ...]]:
        return [(s["unit"], s["source_id"], s["rows"], s["digest"]) for s in _sources(report)]

    assert identities(planned)
    assert identities(applied) == identities(planned) == identities(again)
    assert identities(verified) == identities(planned)
    assert {s["reused"] for s in _sources(again)} == {True}
    assert again["reconciled"] is True
    assert _only(again)["metrics"] == _only(planned)["metrics"]
    assert verified["complete"] is True


def test_retained_bytes_refuse_a_changed_raw_object(tmp_path: Path, home: Path) -> None:
    original = _private(tmp_path / "legacy" / "a.csv", _AAA)
    raw = home / "raw"
    relative, digest, size = put_raw_file(raw, original)
    retained = {original: SourceFile(digest, size)}
    assert RetainedBytes(raw, retained).read(original, max_bytes=1024) == _AAA
    copy = raw / relative
    copy.chmod(0o600)
    copy.write_bytes(_AAA.replace(b"499.23", b"499.24"))
    with pytest.raises(ValueError, match="does not match its address"):
        RetainedBytes(raw, retained).read(original, max_bytes=1024)
    copy.write_bytes(_AAA[:-1])
    with (
        pytest.raises(ValueError, match="does not match its address"),
        RetainedBytes(raw, retained).stream(original),
    ):
        pass
    with pytest.raises(ValueError, match="outside its unit"):
        RetainedBytes(raw, retained).read(tmp_path / "other.csv", max_bytes=1024)
