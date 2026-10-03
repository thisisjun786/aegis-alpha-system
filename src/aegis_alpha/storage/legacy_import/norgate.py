"""Norgate legacy exports: history CSV batches, index membership captures, identity authority.

``norgate.history_export@1`` reads an export directory of ``batch-NNN-result.json`` files.
Each result lists, per security, its asset ID, identity, column list, row count, date range
and the SHA-256 that names its CSV under ``batch-NNN/history/``. One result file and the CSV
files it lists are one unit. Rows keep the CSV text; the asset ID, symbol, database and
security name come from the result record that names the file.

``norgate.index_membership@1`` reads a capture root whose ``batch-results/*.json`` name a
capture directory under ``batch-attempts/``. A capture directory holds ``request.json``, one
``manifest-*.json`` whose journal hash is the SHA-256 of ``receipts.jsonl``, and the
gzip CSV files that journal names. The unit is that directory's request, manifest, journal
and every ``index_constituent_timeseries`` file it names (plus the batch result when there
is one); other methods' files in the same capture are not part of this source. ``include``
names further capture directories below the root that no batch result names. The root's one
``membership-plan-*.json`` lists the planned (asset, index) pairs; the report states how many
were captured, missing and repeated.

``norgate.identity_authority@1`` reads one identity-authority JSON document and emits its
``mappings`` and ``issuer_bindings`` lists as two tables of the same source content.
"""

from __future__ import annotations

# ruff: noqa: TRY004 -- untrusted legacy bytes raise one ingress error type, ValueError.
import gzip
import hashlib
import re
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.storage.legacy_import.loaders import (
    MAX_FILE_BYTES,
    MAX_INDEX_BYTES,
    Run,
    Table,
    Unit,
    csv_table,
    integer,
    json_document,
    json_object,
    json_records,
    record_batch,
    text,
)
from aegis_alpha.storage.legacy_import.manifest import relative_name

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    import pyarrow as pa

    from aegis_alpha.storage.legacy_import.files import Bytes, OriginalBytes
    from aegis_alpha.storage.legacy_import.manifest import Entry

_SHA256: Final = re.compile(r"[0-9a-f]{64}")
_BATCH_RESULT: Final = re.compile(r"batch-([0-9]{3,})-result\.json")

# The value columns an export may carry, in the order they are stored. A record's own
# column list must be a subset; a column outside this set is refused, not dropped.
_HISTORY_VALUES: Final = (
    ("Open", "open"),
    ("High", "high"),
    ("Low", "low"),
    ("Close", "close"),
    ("Volume", "volume"),
    ("Turnover", "turnover"),
    ("Unadjusted Close", "unadjusted_close"),
    ("Dividend", "dividend"),
    ("Delivery Month", "delivery_month"),
    ("Open Interest", "open_interest"),
)
HISTORY: Final = Table(
    "norgate",
    "history-csv",
    "bars",
    (
        ("assetid", "int64"),
        ("symbol", "string"),
        ("database", "string"),
        ("security_name", "string"),
        ("csv_sha256", "string"),
        ("date", "string"),
        *((column, "string") for _, column in _HISTORY_VALUES),
    ),
)
MEMBERSHIP: Final = Table(
    "norgate",
    "index-membership",
    "constituents",
    (
        ("job_id", "string"),
        ("assetid", "int64"),
        ("symbol", "string"),
        ("indexname", "string"),
        ("file_sha256", "string"),
        ("csv_sha256", "string"),
        ("date", "string"),
        ("index_constituent", "string"),
    ),
)
MAPPINGS: Final = Table(
    "norgate",
    "identity-mappings",
    "mappings",
    (
        ("mapping_id", "string"),
        ("provider", "string"),
        ("namespace", "string"),
        ("provider_identifier", "string"),
        ("instrument_id", "string"),
        ("effective_start", "string"),
        ("effective_end", "string"),
        ("mapping_sha256", "string"),
        ("source_snapshot_id", "string"),
    ),
)
ISSUER_BINDINGS: Final = Table(
    "norgate",
    "identity-issuer-bindings",
    "issuer_bindings",
    (
        ("mapping_id", "string"),
        ("provider", "string"),
        ("namespace", "string"),
        ("provider_identifier", "string"),
        ("instrument_id", "string"),
        ("issuer_id", "string"),
        ("effective_start", "string"),
        ("effective_end", "string"),
        ("mapping_sha256", "string"),
        ("instrument_commitment_sha256", "string"),
        ("issuer_commitment_sha256", "string"),
        ("source_snapshot_id", "string"),
    ),
)
_MEMBERSHIP_METHOD: Final = "index_constituent_timeseries"
_COUNTS: Final = {"mappings": "provider_mappings", "issuer_bindings": "instruments"}


def _sha(value: object, name: str) -> str:
    digest = text(value, name)
    if _SHA256.fullmatch(digest) is None:
        raise ValueError(f"legacy {name} must be a SHA-256")
    return digest


def _day(value: object, name: str) -> str:
    stamp = text(value, name)
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2} 00:00:00", stamp) is None:
        raise ValueError(f"legacy {name} must be a midnight date")
    return stamp[:10]


def _children(path: Path) -> list[str]:
    try:
        return sorted(item.name for item in path.iterdir())
    except OSError as error:
        raise ValueError(f"cannot list legacy directory {path.name}: {error}") from None


class HistoryExport:
    """``norgate.history_export@1``."""

    name = "norgate.history_export@1"
    arg_names: frozenset[str] = frozenset()
    metric_names = frozenset({"units", "records", "rows"})

    def units(self, entry: Entry, source: OriginalBytes, run: Run) -> list[Unit]:
        del run
        children = _children(entry.path)
        results = [name for name in children if _BATCH_RESULT.fullmatch(name)]
        if not results:
            raise ValueError("Norgate export has no batch result files")
        batches = {name for name in children if re.fullmatch(r"batch-[0-9]{3,}", name)}
        orphans = sorted(batches - {name.removesuffix("-result.json") for name in results})
        if orphans:
            raise ValueError(f"Norgate export batch {orphans[0]} has no result file")
        units = []
        for name in results:
            path = entry.path / name
            batch = name.removesuffix("-result.json")
            records = self._records(source.read(path, max_bytes=MAX_INDEX_BYTES), name)
            history = entry.path / batch / "history"
            files = [path, *(history / str(record["path"]) for record in records.values())]
            units.append(Unit(name, tuple(dict.fromkeys(files)), (HISTORY,)))
        return units

    @staticmethod
    def _records(payload: bytes, name: str) -> dict[str, dict[str, object]]:
        result = json_object(json_document(payload, name), name)
        records = json_object(result.get("records"), f"{name} records")
        unattempted = result.get("unattempted")
        if unattempted != [] or result.get("expected") != result.get("recorded"):
            raise ValueError(f"Norgate batch result is incomplete: {name}")
        if integer(result.get("recorded"), f"{name} recorded") != len(records):
            raise ValueError(f"Norgate batch result counts disagree: {name}")
        for symbol, record in records.items():
            item = json_object(record, f"{name} record")
            if item.get("status") != "exported" or item.get("symbol") != symbol:
                raise ValueError(f"Norgate batch record is not an export of {symbol}: {name}")
            digest = _sha(item.get("sha256"), f"{name} record sha256")
            if item.get("path") != digest + ".csv":
                raise ValueError(f"Norgate batch record path is not its hash: {name}")
        return cast("dict[str, dict[str, object]]", records)

    def batches(
        self, unit: Unit, table: Table, source: Bytes, run: Run
    ) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa  # noqa: PLC0415 -- the Arrow loaders need the legacy extra

        del table
        records = self._records(source.read(unit.files[0], max_bytes=MAX_INDEX_BYTES), unit.name)
        history = unit.files[0].parent / unit.name.removesuffix("-result.json") / "history"
        known = dict(_HISTORY_VALUES)
        run.metrics["units"] += 1
        for symbol, record in records.items():
            columns = [
                text(column, f"{symbol} column") for column in cast("list", record["columns"])
            ]
            if len(set(columns)) != len(columns) or not set(columns) <= set(known):
                raise ValueError(f"Norgate record {symbol} has an unknown or repeated column")
            if record.get("index_name") != "Date":
                raise ValueError(f"Norgate record {symbol} is not indexed by Date")
            identity = json_object(record.get("identity"), f"{symbol} identity")
            assetid = integer(record.get("assetid"), f"{symbol} assetid")
            if identity.get("assetid") != assetid or identity.get("symbol") != symbol:
                raise ValueError(f"Norgate record {symbol} identity disagrees with the record")
            digest = str(record["sha256"])
            payload = source.read(history / str(record["path"]), max_bytes=MAX_FILE_BYTES)
            if hashlib.sha256(payload).hexdigest() != digest:
                raise ValueError(f"Norgate CSV for {symbol} does not match its recorded hash")
            parsed = csv_table(payload, ("Date", *columns), str(record["path"]))
            rows = len(parsed[0])
            if rows != integer(record.get("rows"), f"{symbol} rows"):
                raise ValueError(f"Norgate CSV for {symbol} does not have its recorded rows")
            if rows and (
                parsed[0][0].as_py() != _day(record.get("first_index"), "first_index")
                or parsed[0][rows - 1].as_py() != _day(record.get("last_index"), "last_index")
            ):
                raise ValueError(f"Norgate CSV for {symbol} does not span its recorded dates")
            by_name = dict(zip(("Date", *columns), parsed, strict=True))
            constant = (
                (assetid, pa.int64()),
                (symbol, pa.string()),
                (text(identity.get("database"), "database"), pa.string()),
                (text(identity.get("securityname"), "securityname"), pa.string()),
                (digest, pa.string()),
            )
            arrays = [pa.repeat(pa.scalar(value, kind), rows) for value, kind in constant]
            arrays.append(by_name["Date"])
            arrays.extend(
                by_name[original] if original in by_name else pa.nulls(rows, pa.string())
                for original, _ in _HISTORY_VALUES
            )
            run.metrics["records"] += 1
            run.metrics["rows"] += rows
            yield pa.record_batch(arrays, schema=HISTORY.schema())

    def finish(self, entry: Entry, source: OriginalBytes, run: Run) -> None:
        del entry, source, run


class IndexMembership:
    """``norgate.index_membership@1``."""

    name = "norgate.index_membership@1"
    arg_names = frozenset({"include"})
    metric_names = frozenset(
        {
            "units",
            "pairs",
            "rows",
            "planned_pairs",
            "missing_pairs",
            "unplanned_pairs",
            "repeated_pairs",
        }
    )

    def units(self, entry: Entry, source: OriginalBytes, run: Run) -> list[Unit]:  # noqa: C901 -- discovery, unit files and the plan in one pass
        values = entry.args
        include = values.get("include", [])
        if not isinstance(include, list):
            raise ValueError("norgate.index_membership include must be a list")
        directories: list[tuple[str, Path | None]] = []
        results = entry.path / "batch-results"
        for name in _children(results):
            if not name.endswith(".json"):
                continue
            path = results / name
            result = json_object(
                json_document(source.read(path, max_bytes=MAX_INDEX_BYTES), name), name
            )
            if result.get("family") != "membership":
                continue
            output = text(result.get("output"), f"{name} output").rsplit("/", 1)[-1]
            relative_name(output, f"{name} output")
            directories.append(("batch-attempts/" + output, path))
        directories.extend((str(relative_name(value, "include")), None) for value in include)
        units = []
        for relative, result_path in directories:
            directory = entry.path / relative
            journal = source.read(directory / "receipts.jsonl", max_bytes=MAX_INDEX_BYTES)
            manifests = [name for name in _children(directory) if name.startswith("manifest-")]
            if len(manifests) != 1:
                raise ValueError(f"Norgate capture {relative} needs exactly one manifest")
            files = [directory / "request.json", directory / manifests[0]]
            files.append(directory / "receipts.jsonl")
            files.extend(
                _sibling(directory / "receipts.jsonl", str(item["file"]))
                for _, item in _receipts(journal)
            )
            if result_path is not None:
                files.insert(0, result_path)
            units.append(
                Unit(
                    relative,
                    tuple(dict.fromkeys(files)),
                    (MEMBERSHIP,),
                    {"result": result_path is not None},
                )
            )
        plans = [name for name in _children(entry.path) if name.startswith("membership-plan-")]
        if len(plans) != 1:
            raise ValueError("Norgate capture root needs exactly one membership plan")
        plan = json_object(
            json_document(source.read(entry.path / plans[0], max_bytes=MAX_INDEX_BYTES), plans[0]),
            plans[0],
        )
        pairs = plan.get("pairs")
        if not isinstance(pairs, list):
            raise ValueError("Norgate membership plan has no pair list")
        planned = {
            (
                integer(json_object(pair, "pair").get("assetid"), "pair assetid"),
                text(pair.get("indexname"), "pair indexname"),
            )
            for pair in pairs
        }
        if len(planned) != len(pairs) or plan.get("pairs_count") != len(pairs):
            raise ValueError("Norgate membership plan repeats a pair or miscounts them")
        run.state["planned"] = planned
        run.state["captured"] = {}
        run.metrics["planned_pairs"] = len(planned)
        return units

    def batches(  # noqa: C901 -- every receipt check sits beside the bytes it checks
        self, unit: Unit, table: Table, source: Bytes, run: Run
    ) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa  # noqa: PLC0415 -- the Arrow loaders need the legacy extra

        del table
        files = list(unit.files)
        request_path, manifest_path, journal_path = (
            files[1:4] if unit.context["result"] else files[:3]
        )
        request = source.read(request_path, max_bytes=MAX_INDEX_BYTES)
        request_sha = hashlib.sha256(request).hexdigest()
        manifest_raw = source.read(manifest_path, max_bytes=MAX_INDEX_BYTES)
        manifest = json_object(json_document(manifest_raw, manifest_path.name), "manifest")
        journal = source.read(journal_path, max_bytes=MAX_INDEX_BYTES)
        if manifest.get("journal_sha256") != hashlib.sha256(journal).hexdigest():
            raise ValueError(f"Norgate capture {unit.name} journal does not match its manifest")
        if manifest.get("request_sha256") != request_sha:
            raise ValueError(f"Norgate capture {unit.name} request does not match its manifest")
        if unit.context["result"]:
            result = json_object(
                json_document(source.read(files[0], max_bytes=MAX_INDEX_BYTES), files[0].name),
                "batch result",
            )
            verification = json_object(result.get("verification"), "batch verification")
            if (
                result.get("request_sha256") != request_sha
                or verification.get("journal_sha256") != manifest["journal_sha256"]
            ):
                raise ValueError(f"Norgate capture {unit.name} disagrees with its batch result")
        captured = cast("dict[tuple[int, str], int]", run.state["captured"])
        run.metrics["units"] += 1
        for job, payload in _receipts(journal):
            pair = (
                integer(job.get("assetid"), "job assetid"),
                text(json_object(job.get("kwargs"), "job kwargs").get("indexname"), "indexname"),
            )
            name = str(payload["file"])
            raw = source.read(_sibling(journal_path, name), max_bytes=MAX_FILE_BYTES)
            file_sha = hashlib.sha256(raw).hexdigest()
            if file_sha != payload.get("sha256") or name != file_sha + ".csv.gz":
                raise ValueError(f"Norgate capture file {name} does not match its receipt")
            try:
                body = gzip.decompress(raw)
            except (OSError, EOFError) as error:
                raise ValueError(f"Norgate capture file {name} is not gzip") from error
            csv_sha = hashlib.sha256(body).hexdigest()
            if csv_sha != payload.get("csv_sha256"):
                raise ValueError(f"Norgate capture file {name} CSV hash does not match")
            if (
                payload.get("columns") != ["Index Constituent"]
                or payload.get("index_name") != "Date"
            ):
                raise ValueError(f"Norgate capture file {name} is not an index constituent series")
            parsed = csv_table(body, ("Date", "Index Constituent"), name)
            rows = len(parsed[0])
            if rows != integer(payload.get("rows"), "receipt rows"):
                raise ValueError(f"Norgate capture file {name} does not have its recorded rows")
            if rows and (
                parsed[0][0].as_py() != _day(payload.get("first_index"), "first_index")
                or parsed[0][rows - 1].as_py() != _day(payload.get("last_index"), "last_index")
            ):
                raise ValueError(f"Norgate capture file {name} does not span its recorded dates")
            captured[pair] = captured.get(pair, 0) + 1
            run.metrics["pairs"] += 1
            run.metrics["rows"] += rows
            constant = (
                (text(job.get("id"), "job id"), pa.string()),
                (pair[0], pa.int64()),
                (text(job.get("symbol"), "job symbol"), pa.string()),
                (pair[1], pa.string()),
                (file_sha, pa.string()),
                (csv_sha, pa.string()),
            )
            arrays = [pa.repeat(pa.scalar(value, kind), rows) for value, kind in constant]
            arrays.extend(parsed)
            yield pa.record_batch(arrays, schema=MEMBERSHIP.schema())

    def finish(self, entry: Entry, source: OriginalBytes, run: Run) -> None:
        del entry, source
        planned = cast("set[tuple[int, str]]", run.state["planned"])
        captured = cast("dict[tuple[int, str], int]", run.state["captured"])
        run.metrics["missing_pairs"] = len(planned - set(captured))
        run.metrics["unplanned_pairs"] = len(set(captured) - planned)
        run.metrics["repeated_pairs"] = sum(count - 1 for count in captured.values())


def _sibling(path: Path, name: str) -> Path:
    relative_name(name, "capture file")
    if "/" in name:
        raise ValueError("Norgate capture file must be in its capture directory")
    return path.parent / name


def _receipts(journal: bytes) -> Iterator[tuple[dict[str, object], dict[str, object]]]:
    """The captured index-constituent receipts of a journal, in journal order."""
    try:
        lines = journal.decode("utf-8", errors="strict").splitlines()
    except UnicodeDecodeError:
        raise ValueError("Norgate capture journal is not UTF-8") from None
    for line in lines:
        receipt = json_object(json_document(line.encode(), "receipts.jsonl"), "receipt")
        job = json_object(receipt.get("job"), "receipt job")
        if job.get("method") != _MEMBERSHIP_METHOD:
            continue
        payload = json_object(receipt.get("payload"), "receipt payload")
        if payload.get("status") != "captured":
            raise ValueError("Norgate index constituent receipt was not captured")
        yield job, payload


class IdentityAuthority:
    """``norgate.identity_authority@1``."""

    name = "norgate.identity_authority@1"
    arg_names: frozenset[str] = frozenset()
    metric_names = frozenset({"mappings", "issuer_bindings"})

    def units(self, entry: Entry, source: OriginalBytes, run: Run) -> list[Unit]:
        del source, run
        return [Unit(entry.path.name, (entry.path,), (MAPPINGS, ISSUER_BINDINGS))]

    def batches(
        self, unit: Unit, table: Table, source: Bytes, run: Run
    ) -> Iterator[pa.RecordBatch]:
        document = json_object(
            json_document(source.read(unit.files[0], max_bytes=MAX_INDEX_BYTES), unit.name),
            "identity authority",
        )
        counts = json_object(document.get("counts"), "identity authority counts")
        records = document.get(table.name)
        rows = json_records(table, records, f"identity authority {table.name}")
        expected = counts.get(_COUNTS[table.name])
        if expected != len(rows):
            raise ValueError(f"identity authority {table.name} does not match its counts")
        run.metrics[table.name] += len(rows)
        for start in range(0, len(rows), 65536):
            yield record_batch(table, rows[start : start + 65536])

    def finish(self, entry: Entry, source: OriginalBytes, run: Run) -> None:
        del entry, source, run
