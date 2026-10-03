"""Legacy public and frozen-provider originals: SEC archives, FRED CSV, KR public, FMP runs.

``sec.submissions_zip@1`` and ``sec.companyfacts_zip@1`` read one SEC bulk archive and the
receipts that describe it (``args.evidence``, absolute paths). The archive and its receipts
are one unit; the table is the archive's member index in central-directory order: name,
sizes, CRC-32, modification time, compression method and the SHA-256 of each member's bytes.
The member bytes themselves stay in the retained archive in ``raw/``, which the mappers read
through this index. Every receipt that states the archive's hash, size, member counts or
expanded size must agree with the archive. Receipts are optional; the ``receipts`` metric
counts them so a manifest can pin how many the archive must carry.

``sec.submissions_filings@1`` reads the same submissions archive and receipts as one unit and
writes the filings its documents list. A filer document ``CIK##########.json`` holds the
recent filings as parallel arrays under ``filings.recent`` and names its older pages under
``filings.files``; a page ``CIK##########-submissions-###.json`` holds the same arrays at its
top level. Each array position is one row, members in central-directory order and positions
in array order: the member name, the member's ten-digit CIK and each array's value as SEC
wrote it (text stays text; ``size`` and the XBRL markers stay integers). An array a document
lacks is null in its rows. A key outside the known arrays, arrays of unequal length, a value
of another JSON type, a filer document whose ``cik`` is not its member's or a listed page
whose name is not a ``CIK##########-submissions-###.json`` of that CIK refuses the unit.
Pages a filer lists but the archive lacks (``missing_pages``), pages no filer lists
(``unlisted_pages``), pages whose filing count differs from the listing (``miscounted_pages``)
and JSON members of any other name (``unknown_members``) are discrepancy metrics.

``fred.series_csv@1`` reads one FRED download (``observation_date,<SERIES>``) as text rows.

``korea.public_response@1`` reads a directory of request directories, each holding
``request.json``, ``response.json``, ``response.raw`` and the legacy parser's
``normalized.v1.json``. Each request directory is one unit; its rows are the normalized
records, checked against the response bytes they name. KIND listings and BOK/OECD
observations are separate shapes.

``fmp.price_eod_non_split@1`` reads an FMP run root whose ``run_id=*`` directories each hold
one ``fmp_price_eod_non_split_adjusted`` dataset (``history-index.json`` and
``part-*.parquet``). Each run's dataset is one unit; rows are the Parquet rows in part order.
FMP is a frozen source: every run's dataset is preserved as it was collected, and choosing
among overlapping runs belongs to promotion, not to the import. The run's
``history-index.json`` is the collector's cumulative recent-date cache across runs, not an
inventory of that run's parts, so it is retained as evidence and checked only for its dataset.
"""

from __future__ import annotations

# ruff: noqa: TRY004 -- untrusted legacy bytes raise one ingress error type, ValueError.
import hashlib
import re
import zipfile
import zlib
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Final, cast

from aegis_alpha.storage.legacy_import.loaders import (
    MAX_FILE_BYTES,
    MAX_INDEX_BYTES,
    Run,
    Table,
    Unit,
    csv_table,
    json_document,
    json_object,
    json_records,
    record_batch,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pyarrow as pa

    from aegis_alpha.storage.legacy_import.files import Bytes, OriginalBytes
    from aegis_alpha.storage.legacy_import.manifest import Entry

_BATCH: Final = 65536
_MEMBER_COLUMNS: Final = (
    ("member", "string"),
    ("compressed_size", "int64"),
    ("size", "int64"),
    ("crc32", "int64"),
    ("modified", "string"),
    ("compress_type", "int64"),
    ("sha256", "string"),
)
SUBMISSIONS: Final = Table("sec", "submissions-zip", "members", _MEMBER_COLUMNS)
# The parallel arrays of an SEC submissions document, in SEC's order, and their types.
FILING_ARRAYS: Final = (
    ("accessionNumber", "string"),
    ("filingDate", "string"),
    ("reportDate", "string"),
    ("acceptanceDateTime", "string"),
    ("act", "string"),
    ("form", "string"),
    ("fileNumber", "string"),
    ("filmNumber", "string"),
    ("items", "string"),
    ("core_type", "string"),
    ("size", "int64"),
    ("isXBRL", "int64"),
    ("isInlineXBRL", "int64"),
    ("isXBRLNumeric", "int64"),
    ("primaryDocument", "string"),
    ("primaryDocDescription", "string"),
)
SUBMISSION_FILINGS: Final = Table(
    "sec",
    "submissions-filings",
    "filings",
    (("member", "string"), ("cik", "string"), *FILING_ARRAYS),
)
_FILER: Final = re.compile(r"CIK([0-9]{10})\.json")
_PAGE: Final = re.compile(r"CIK([0-9]{10})-submissions-[0-9]{3}\.json")
COMPANYFACTS: Final = Table("sec", "companyfacts-zip", "members", _MEMBER_COLUMNS)
FRED: Final = Table(
    "fred",
    "series-csv",
    "observations",
    (("series_id", "string"), ("observation_date", "string"), ("value", "string")),
)
LISTINGS_COLUMNS: Final = (
    ("stock_code", "string"),
    ("company_name", "string"),
    ("market", "string"),
    ("listed_on", "string"),
    ("fiscal_month", "int64"),
    ("raw_fields", "string"),
    ("source_rows", "string"),
)
OBSERVATION_COLUMNS: Final = (
    ("series_id", "string"),
    ("period", "string"),
    ("value", "string"),
    ("value_raw", "string"),
    ("units", "string"),
    ("unit_multiplier", "string"),
    ("base_period", "string"),
    ("regime", "string"),
    ("status", "string"),
)
KIND_LISTINGS: Final = Table("kind", "listings", "listings", LISTINGS_COLUMNS)
BOK_OBSERVATIONS: Final = Table("bok", "observations", "observations", OBSERVATION_COLUMNS)
OECD_OBSERVATIONS: Final = Table("oecd", "observations", "observations", OBSERVATION_COLUMNS)
# The legacy KR public request IDs this loader knows, and the table each one fills.
_KOREA_REQUESTS: Final = {
    "kind-kospi": KIND_LISTINGS,
    "kind-kosdaq": KIND_LISTINGS,
    "bok-policy": BOK_OBSERVATIONS,
    "oecd-cpi": OECD_OBSERVATIONS,
}
_KOREA_FILES: Final = ("request.json", "response.json", "response.raw", "normalized.v1.json")
_NESTED: Final = frozenset({"raw_fields", "source_rows"})
FMP_DATASET: Final = "fmp_price_eod_non_split_adjusted"
FMP: Final = Table(
    "fmp",
    "price-eod-non-split",
    "bars",
    (
        ("symbol", "string"),
        ("date", "date32"),
        ("adjOpen", "float64"),
        ("adjHigh", "float64"),
        ("adjLow", "float64"),
        ("adjClose", "float64"),
        ("volume", "int64"),
        ("provider", "string"),
        ("source_receipt_id", "string"),
        ("raw_content_sha256", "string"),
        ("retrieved_at_utc", "timestamp_us_utc"),
    ),
)
_PART: Final = re.compile(r"part-[0-9]+\.parquet")
_SERIES: Final = re.compile(r"[A-Z0-9]+")


class SecArchive:
    """``sec.submissions_zip@1`` and ``sec.companyfacts_zip@1``."""

    arg_names = frozenset({"evidence"})
    metric_names = frozenset({"members", "json_members", "expanded_bytes", "receipts"})
    zero_metrics: frozenset[str] = frozenset()

    def __init__(self, name: str, table: Table) -> None:
        self.name = name
        self.table = table

    def units(self, entry: Entry, source: OriginalBytes, run: Run) -> list[Unit]:
        del source, run
        values = entry.args
        evidence = values.get("evidence", [])
        if not isinstance(evidence, list) or not all(
            isinstance(item, str) and item.startswith("/") for item in evidence
        ):
            raise ValueError(f"{self.name} evidence must be a list of absolute paths")
        files = (entry.path, *(Path(item) for item in evidence))
        return [Unit(entry.path.name, tuple(dict.fromkeys(files)), (self.table,))]

    def batches(
        self, unit: Unit, table: Table, source: Bytes, run: Run
    ) -> Iterator[pa.RecordBatch]:
        rows: list[tuple[object, ...]] = []
        members = json_members = expanded = 0
        with source.stream(unit.files[0]) as handle:
            archive_file = source.seen[unit.files[0]]
            try:
                archive = zipfile.ZipFile(handle)
            except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError, EOFError) as error:
                raise ValueError(f"SEC archive is not a zip file: {unit.name}") from error
            with archive:
                for info in archive.infolist():
                    if info.flag_bits & 1:
                        raise ValueError(f"SEC archive member is encrypted: {info.filename}")
                    member = hashlib.sha256()
                    try:
                        with archive.open(info) as stream:
                            for chunk in iter(lambda s=stream: s.read(1024 * 1024), b""):
                                member.update(chunk)
                    except (
                        zipfile.BadZipFile,
                        zipfile.LargeZipFile,
                        zlib.error,
                        NotImplementedError,
                        RuntimeError,
                        OSError,
                        EOFError,
                    ) as error:
                        raise ValueError(
                            f"SEC archive member is unreadable or fails its CRC: {info.filename}"
                        ) from (error)
                    year, month, day, hour, minute, second = info.date_time
                    rows.append(
                        (
                            info.filename,
                            info.compress_size,
                            info.file_size,
                            info.CRC,
                            f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}",
                            info.compress_type,
                            member.hexdigest(),
                        )
                    )
                    members += 1
                    json_members += info.filename.endswith(".json")
                    expanded += info.file_size
                    if len(rows) == _BATCH:
                        yield record_batch(table, rows)
                        rows = []
        if rows:
            yield record_batch(table, rows)
        _check_receipts(
            unit,
            source,
            {
                "sha256": archive_file.sha256,
                "bytes": archive_file.size_bytes,
                "members": members,
                "json_members": json_members,
                "expanded_bytes": expanded,
            },
        )
        run.metrics["members"] += members
        run.metrics["receipts"] += len(unit.files) - 1
        run.metrics["json_members"] += json_members
        run.metrics["expanded_bytes"] += expanded

    def finish(self, entry: Entry, source: OriginalBytes, run: Run) -> None:
        del entry, source, run


def _check_receipts(unit: Unit, source: Bytes, observed: dict[str, object]) -> None:
    """Refuse a receipt that does not name the archive or disagrees with what was read."""
    for path in unit.files[1:]:
        receipt = json_object(
            json_document(source.read(path, max_bytes=MAX_INDEX_BYTES), path.name), path.name
        )
        if "sha256" not in receipt:
            raise ValueError(f"SEC archive receipt does not name the archive: {path.name}")
        for key, value in observed.items():
            if key in receipt and receipt[key] != value:
                raise ValueError(f"SEC archive receipt {path.name} disagrees on {key}")


def _open_archive(handle: BinaryIO, name: str) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(handle)
    except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError, EOFError) as error:
        raise ValueError(f"SEC archive is not a zip file: {name}") from error


def _member_bytes(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> bytes:
    if info.flag_bits & 1:
        raise ValueError(f"SEC archive member is encrypted: {info.filename}")
    if info.file_size > MAX_FILE_BYTES:
        raise ValueError(f"SEC archive member exceeds its bound: {info.filename}")
    try:
        return archive.read(info)
    except (
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
        zlib.error,
        NotImplementedError,
        RuntimeError,
        OSError,
        EOFError,
    ) as error:
        raise ValueError(
            f"SEC archive member is unreadable or fails its CRC: {info.filename}"
        ) from error


def _filing_rows(arrays: dict[str, object], member: str, cik: str) -> list[tuple[object, ...]]:
    """One row per position of a submissions document's parallel filing arrays."""
    kinds = dict(FILING_ARRAYS)
    unknown = sorted(set(arrays) - set(kinds))
    if unknown:
        raise ValueError(f"SEC submissions document {member} has unknown array {unknown[0]!r}")
    lengths = set()
    for name, values in arrays.items():
        if not isinstance(values, list):
            raise ValueError(f"SEC submissions {member} {name} must be a JSON list")
        lengths.add(len(values))
        for value in values:
            if value is None:
                continue
            if kinds[name] == "string" and not isinstance(value, str):
                raise ValueError(f"SEC submissions {member} {name} values must be text")
            if kinds[name] == "int64" and (isinstance(value, bool) or not isinstance(value, int)):
                raise ValueError(f"SEC submissions {member} {name} values must be integers")
    if len(lengths) > 1:
        raise ValueError(f"SEC submissions {member} arrays have unequal lengths")
    count = next(iter(lengths), 0)
    columns = [
        cast("list[object]", arrays[name]) if name in arrays else [None] * count
        for name, _ in FILING_ARRAYS
    ]
    return [(member, cik, *values) for values in zip(*columns, strict=True)] if count else []


class SecSubmissionsFilings:
    """``sec.submissions_filings@1``."""

    name = "sec.submissions_filings@1"
    arg_names = frozenset({"evidence"})
    metric_names = frozenset(
        {
            "members",
            "json_members",
            "expanded_bytes",
            "receipts",
            "filers",
            "pages",
            "filings",
            "other_members",
            "missing_pages",
            "unlisted_pages",
            "miscounted_pages",
            "unknown_members",
        }
    )
    zero_metrics = frozenset(
        {"missing_pages", "unlisted_pages", "miscounted_pages", "unknown_members"}
    )
    table = SUBMISSION_FILINGS

    def units(self, entry: Entry, source: OriginalBytes, run: Run) -> list[Unit]:
        return SecArchive(self.name, self.table).units(entry, source, run)

    def batches(
        self, unit: Unit, table: Table, source: Bytes, run: Run
    ) -> Iterator[pa.RecordBatch]:
        rows: list[tuple[object, ...]] = []
        metrics: Counter[str] = Counter()
        listed: dict[str, int] = {}
        found: dict[str, int] = {}
        with source.stream(unit.files[0]) as handle:
            archive_file = source.seen[unit.files[0]]
            with _open_archive(handle, unit.name) as archive:
                for info in archive.infolist():
                    payload = _member_bytes(archive, info)
                    metrics["members"] += 1
                    metrics["expanded_bytes"] += info.file_size
                    name = info.filename
                    if not name.endswith(".json"):
                        metrics["other_members"] += 1
                        continue
                    metrics["json_members"] += 1
                    filer, page = _FILER.fullmatch(name), _PAGE.fullmatch(name)
                    if filer is None and page is None:
                        metrics["unknown_members"] += 1
                        continue
                    document = json_object(json_document(payload, name), name)
                    if filer is not None:
                        cik = filer.group(1)
                        arrays = _filer_arrays(document, name, cik, listed)
                        metrics["filers"] += 1
                    else:
                        cik = cast("re.Match[str]", page).group(1)
                        arrays = document
                        metrics["pages"] += 1
                    produced = _filing_rows(arrays, name, cik)
                    if page is not None:
                        found[name] = len(produced)
                    metrics["filings"] += len(produced)
                    rows.extend(produced)
                    while len(rows) >= _BATCH:
                        yield record_batch(table, rows[:_BATCH])
                        rows = rows[_BATCH:]
        if rows:
            yield record_batch(table, rows)
        _check_receipts(
            unit,
            source,
            {
                "sha256": archive_file.sha256,
                "bytes": archive_file.size_bytes,
                "members": metrics["members"],
                "json_members": metrics["json_members"],
                "expanded_bytes": metrics["expanded_bytes"],
            },
        )
        metrics["receipts"] = len(unit.files) - 1
        metrics["missing_pages"] = len(set(listed) - set(found))
        metrics["unlisted_pages"] = len(set(found) - set(listed))
        metrics["miscounted_pages"] = sum(
            1 for page, count in found.items() if page in listed and listed[page] != count
        )
        run.metrics.update(metrics)

    def finish(self, entry: Entry, source: OriginalBytes, run: Run) -> None:
        del entry, source, run


def _filer_arrays(
    document: dict[str, object], name: str, cik: str, listed: dict[str, int]
) -> dict[str, object]:
    """A filer document's recent filing arrays; its listed pages go into ``listed``."""
    stated = document.get("cik")
    if (
        isinstance(stated, bool)
        or not isinstance(stated, str | int)
        or str(stated).zfill(10) != cik
    ):
        raise ValueError(f"SEC submissions document {name} names another CIK")
    filings = json_object(document.get("filings"), f"{name} filings")
    if set(filings) != {"recent", "files"}:
        raise ValueError(f"SEC submissions {name} filings keys are unknown")
    pages = _page_list(filings["files"], name)
    for page, _ in pages:
        named = _PAGE.fullmatch(page)
        if named is None or named.group(1) != cik:
            raise ValueError(f"SEC submissions {name} lists a page of another filer: {page}")
    listed.update(pages)
    return json_object(filings["recent"], f"{name} filings.recent")


def _page_list(value: object, name: str) -> list[tuple[str, int]]:
    """The (page member, filing count) pairs a filer document lists under ``filings.files``."""
    if not isinstance(value, list):
        raise ValueError(f"SEC submissions {name} filings.files must be a JSON list")
    pages = []
    for item in value:
        entry = json_object(item, f"{name} filings.files entry")
        page, count = entry.get("name"), entry.get("filingCount")
        if not isinstance(page, str) or isinstance(count, bool) or not isinstance(count, int):
            raise ValueError(f"SEC submissions {name} lists a page without name and filingCount")
        pages.append((page, count))
    return pages


class FredSeriesCsv:
    """``fred.series_csv@1``."""

    name = "fred.series_csv@1"
    arg_names: frozenset[str] = frozenset()
    metric_names = frozenset({"rows"})
    zero_metrics: frozenset[str] = frozenset()

    def units(self, entry: Entry, source: OriginalBytes, run: Run) -> list[Unit]:
        del source, run
        return [Unit(entry.path.name, (entry.path,), (FRED,))]

    def batches(
        self, unit: Unit, table: Table, source: Bytes, run: Run
    ) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa  # noqa: PLC0415 -- the Arrow loaders need the legacy extra

        payload = source.read(unit.files[0], max_bytes=MAX_FILE_BYTES)
        header = payload.split(b"\n", 1)[0].removesuffix(b"\r").decode("utf-8", "replace")
        names = header.split(",")
        if len(names) != 2 or names[0] != "observation_date" or not _SERIES.fullmatch(names[1]):  # noqa: PLR2004 -- date and value
            raise ValueError(f"FRED CSV header is not observation_date,<SERIES>: {unit.name}")
        parsed = csv_table(payload, tuple(names), unit.name)
        rows = len(parsed[0])
        run.metrics["rows"] += rows
        series = pa.repeat(pa.scalar(names[1], pa.string()), rows)
        yield pa.record_batch([series, *parsed], schema=table.schema())

    def finish(self, entry: Entry, source: OriginalBytes, run: Run) -> None:
        del entry, source, run


class KoreaPublicResponse:
    """``korea.public_response@1``."""

    name = "korea.public_response@1"
    arg_names: frozenset[str] = frozenset()
    metric_names = frozenset({"units", "listings", "observations"})
    zero_metrics: frozenset[str] = frozenset()

    def units(self, entry: Entry, source: OriginalBytes, run: Run) -> list[Unit]:
        del run
        units = []
        for directory in sorted(entry.path.iterdir()):
            names = sorted(item.name for item in directory.iterdir())
            if names != sorted(_KOREA_FILES):
                raise ValueError(f"KR public request {directory.name} needs exactly {_KOREA_FILES}")
            request = json_object(
                json_document(
                    source.read(directory / "request.json", max_bytes=MAX_INDEX_BYTES),
                    "request.json",
                ),
                "request",
            )
            table = _KOREA_REQUESTS.get(str(request.get("source_id")))
            if table is None:
                raise ValueError(f"KR public request {directory.name} has an unknown source")
            files = tuple(directory / name for name in _KOREA_FILES)
            units.append(Unit(directory.name, files, (table,)))
        return units

    def batches(
        self, unit: Unit, table: Table, source: Bytes, run: Run
    ) -> Iterator[pa.RecordBatch]:
        request_path, response_path, raw_path, normalized_path = unit.files
        request = json_document(source.read(request_path, max_bytes=MAX_INDEX_BYTES), "request")
        response = json_object(
            json_document(source.read(response_path, max_bytes=MAX_INDEX_BYTES), "response"),
            "response",
        )
        body = source.read(raw_path, max_bytes=MAX_FILE_BYTES)
        normalized = json_object(
            json_document(
                source.read(normalized_path, max_bytes=MAX_FILE_BYTES), normalized_path.name
            ),
            "normalized",
        )
        digest = hashlib.sha256(body).hexdigest()
        for document in (response, normalized):
            raw = json_object(document.get("raw"), "raw reference")
            if raw.get("content_sha256") != digest:
                raise ValueError(f"KR public request {unit.name} names other response bytes")
        if normalized.get("request") != request or normalized.get("schema_version") != 1:
            raise ValueError(f"KR public request {unit.name} normalized document disagrees")
        rows = json_records(
            table,
            normalized.get("rows"),
            "KR public rows",
            nested=_NESTED if table is KIND_LISTINGS else frozenset(),
        )
        if normalized.get("row_count") != len(rows):
            raise ValueError(f"KR public request {unit.name} miscounts its rows")
        run.metrics["units"] += 1
        run.metrics[table.name] += len(rows)
        yield record_batch(table, rows)

    def finish(self, entry: Entry, source: OriginalBytes, run: Run) -> None:
        del entry, source, run


class FmpNonSplit:
    """``fmp.price_eod_non_split@1``."""

    name = "fmp.price_eod_non_split@1"
    arg_names: frozenset[str] = frozenset()
    metric_names = frozenset({"units", "rows"})
    zero_metrics: frozenset[str] = frozenset()

    def units(self, entry: Entry, source: OriginalBytes, run: Run) -> list[Unit]:
        del source, run
        units = []
        for run_directory in sorted(entry.path.iterdir()):
            dataset = run_directory / FMP_DATASET
            if not run_directory.name.startswith("run_id=") or not dataset.is_dir():
                continue
            names = sorted(item.name for item in dataset.iterdir())
            parts = [name for name in names if _PART.fullmatch(name)]
            if not parts or sorted(["history-index.json", *parts]) != names:
                raise ValueError(f"FMP run {run_directory.name} has unexpected dataset files")
            files = (dataset / "history-index.json", *(dataset / name for name in parts))
            units.append(Unit(run_directory.name, files, (FMP,)))
        if not units:
            raise ValueError("FMP run root has no non-split dataset")
        return units

    def batches(
        self, unit: Unit, table: Table, source: Bytes, run: Run
    ) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa  # noqa: PLC0415 -- the Arrow loaders need the legacy extra
        import pyarrow.parquet as pq  # noqa: PLC0415

        index = json_object(
            json_document(source.read(unit.files[0], max_bytes=MAX_INDEX_BYTES), "history index"),
            "history index",
        )
        if index.get("dataset") != FMP_DATASET:
            raise ValueError(f"FMP run {unit.name} index names another dataset")
        target = table.schema()
        run.metrics["units"] += 1
        for path in unit.files[1:]:
            payload = source.read(path, max_bytes=MAX_FILE_BYTES)
            try:
                parsed = pq.read_table(pa.BufferReader(payload))
            except (pa.ArrowInvalid, OSError) as error:
                raise ValueError(f"FMP part is not Parquet: {path.name}") from error
            if not parsed.schema.remove_metadata().equals(target):
                raise ValueError(f"FMP part {path.name} does not have the non-split schema")
            run.metrics["rows"] += parsed.num_rows
            yield from parsed.replace_schema_metadata(None).to_batches(max_chunksize=_BATCH)

    def finish(self, entry: Entry, source: OriginalBytes, run: Run) -> None:
        del entry, source, run
