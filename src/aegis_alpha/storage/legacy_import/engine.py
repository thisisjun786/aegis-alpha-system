"""Plan, apply and verify one ``aas-legacy-import-v1`` manifest.

- ``plan_import`` reads the originals where they lie and writes nothing anywhere. It needs no
  installation: for every unit it parses and validates the bytes, computes each output
  table's row count and source-library digest, and the content source ID those bytes
  receive. A unit that fails is reported with its reason and the plan goes on.
- ``apply_import`` retains every original of a unit in ``raw/`` (content-addressed, never
  overwriting different bytes), then commits each output table through
  ``source_library.import_content_arrow`` from the retained copies, which also records the
  ``sl:`` link. Units commit one at a time; a unit already committed is reused after its rows
  are re-derived and match, so an interrupted import is finished by running it again.
  The first failing unit stops the apply.
- ``verify_import`` re-derives the plan from the originals and checks that every planned
  source is committed and complete in the installation with the same rows and digest, that
  its stored table still rehashes to them, that its ``sl:`` link matches and every linked
  original is intact in ``raw/``, that every retained file is intact in ``raw/``, and that the
  entry's retained-file inventory is committed. A source or retained file that fails is
  ``unmatched``.

Every regular file below an entry's root is accounted for. A unit file is covered by its
source. An index file the loader read to discover its units (a membership plan, a batch
result of another family, an export plan) and a file matching the entry's ``retain``
patterns are retained in ``raw/`` as they are. The entry's retained files are named by one
inventory source, ``legacy-retained-files-<hex>`` with table ``retained_files`` (relative
path, SHA-256, size, reason ``index`` or ``retain``), whose originals are the inventory
document ``aas-legacy-retained-v1`` and every retained file; so after the entry's root is
deleted the store still maps each retained path to its bytes. A file matching ``exclude``
is reported as excluded. Every other file is ``uncovered``: the report counts it, its bytes
and the first paths. ``complete`` holds only with zero unmatched, ``reconciled`` and zero
uncovered files, and it is the precondition for deleting an entry's root outside the store.

Every report states, per entry, the loader's reconciliation metrics beside the manifest's
``expect`` counts. No provider is called and no clock value reaches a stored row.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import stat
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data.descriptor_tree import DescriptorTree, DescriptorTreeError
from aegis_alpha.storage import source_library_schema as schema
from aegis_alpha.storage.legacy_import.files import OriginalBytes, RetainedBytes
from aegis_alpha.storage.legacy_import.loaders import (
    SCHEMA_MAJOR,
    Loader,
    Run,
    Table,
    Unit,
    admit_entry,
)
from aegis_alpha.storage.legacy_import.manifest import (
    MAX_MANIFEST_BYTES,
    Entry,
    Manifest,
    parse_manifest,
)
from aegis_alpha.storage.legacy_import.norgate import (
    HistoryExport,
    IdentityAuthority,
    IndexMembership,
)
from aegis_alpha.storage.legacy_import.public import (
    COMPANYFACTS,
    SUBMISSIONS,
    FmpNonSplit,
    FredSeriesCsv,
    KoreaPublicResponse,
    SecArchive,
    SecSubmissionsFilings,
)
from aegis_alpha.storage.source_identity import SourceContent, SourceFile

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import pyarrow as pa

    from aegis_alpha.storage.legacy_import.files import Bytes
    from aegis_alpha.storage.workspace import Workspace

LOADERS: Final[dict[str, Loader]] = {
    loader.name: loader
    for loader in (
        HistoryExport(),
        IndexMembership(),
        IdentityAuthority(),
        SecArchive("sec.submissions_zip@1", SUBMISSIONS),
        SecArchive("sec.companyfacts_zip@1", COMPANYFACTS),
        SecSubmissionsFilings(),
        FredSeriesCsv(),
        KoreaPublicResponse(),
        FmpNonSplit(),
    )
}

type _Commit = Callable[[Entry, Loader, Unit, Run], list[dict[str, object]]]
# Keeps an entry's retained files and records their inventory; returns the files that are
# not intact in raw/ and the inventory source's report row (None when nothing is retained).
type _Retain = Callable[
    [Entry, dict[Path, SourceFile], dict[Path, str]],
    tuple[list[dict[str, str]], dict[str, object] | None],
]

RETAINED_FORMAT: Final = "aas-legacy-retained-v1"
RETAINED: Final = Table(
    "legacy",
    "retained-files",
    "retained_files",
    (("path", "string"), ("sha256", "string"), ("size_bytes", "int64"), ("reason", "string")),
)

# The uncovered paths a report names per entry; the counts and bytes cover all of them.
_UNCOVERED_PATHS: Final = 20


def read_manifest_file(path: Path, sha256: str) -> Manifest:
    """Read and parse an exact manifest file of at most 1 MiB."""
    absolute = path.absolute()
    try:
        with DescriptorTree.open_path(absolute.parent) as tree:
            raw = tree.read_bytes(absolute.name, max_bytes=MAX_MANIFEST_BYTES)
    except (OSError, DescriptorTreeError) as error:
        raise ValueError("cannot read a bounded regular legacy manifest") from error
    return parse_manifest(raw, sha256)


def _loader(entry: Entry) -> Loader:
    try:
        loader = LOADERS[entry.loader]
    except KeyError:
        raise ValueError(f"unknown legacy loader {entry.loader!r}") from None
    admit_entry(loader, entry)
    return loader


def _reader(
    loader: Loader, unit: Unit, table: Table, source: Bytes, run: Run
) -> pa.RecordBatchReader:
    import pyarrow as pa  # noqa: PLC0415 -- the Arrow loaders need the legacy extra

    return pa.RecordBatchReader.from_batches(
        table.schema(), loader.batches(unit, table, source, run)
    )


def _derive(entry: Entry, loader: Loader, unit: Unit, run: Run) -> list[dict[str, object]]:
    """Each output table of one unit read from the originals: rows, digest and source ID."""
    from aegis_alpha.storage.source_library_digest import arrow_digest  # noqa: PLC0415

    del entry
    derived: list[dict[str, object]] = []
    for table in unit.tables:
        source = OriginalBytes()
        rows, digest = arrow_digest(_reader(loader, unit, table, source, run))
        files = source.files(unit.files)
        content = SourceContent(table.provider, table.shape, SCHEMA_MAJOR, files)
        derived.append(
            {
                "unit": unit.name,
                "table": table.name,
                "source_id": content.source_id,
                "rows": rows,
                "digest": digest,
                "_files": [(item.sha256, item.size_bytes) for item in files],
            }
        )
    return derived


def _inventory(root: Path) -> list[tuple[Path, int]]:
    """Every non-directory below ``root`` (``root`` itself when it is a file), unfollowed."""
    try:
        if not stat.S_ISDIR(os.lstat(root).st_mode):
            return [(root, os.lstat(root).st_size)]
        found: list[tuple[Path, int]] = []
        pending = [root]
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for item in entries:
                    path = directory / item.name
                    if item.is_dir(follow_symlinks=False):
                        pending.append(path)
                    else:
                        found.append((path, item.stat(follow_symlinks=False).st_size))
    except OSError as error:
        raise ValueError(f"cannot list legacy entry: {error}") from None
    return sorted(found)


def _match(path: Path, root: Path, patterns: tuple[str, ...]) -> bool:
    relative = path.relative_to(root).as_posix()
    return any(fnmatch.fnmatchcase(relative, pattern) for pattern in patterns)


def _coverage(
    entry: Entry, units: list[Unit], discovery: OriginalBytes
) -> tuple[dict[Path, SourceFile], dict[Path, str], dict[str, object], list[dict[str, str]]]:
    """Classify every file below the entry root; hash the ones that are retained."""
    covered = {path for unit in units for path in unit.files}
    retained = {path: item for path, item in discovery.seen.items() if path not in covered}
    reasons = dict.fromkeys(retained, "index")
    refusals: list[dict[str, str]] = []
    excluded = [0, 0]
    uncovered: list[tuple[Path, int]] = []
    for path, size in _inventory(entry.path):
        if path in covered or path in retained:
            continue
        if _match(path, entry.path, entry.retain):
            try:
                with discovery.stream(path):
                    pass
            except (OSError, ValueError) as error:
                refusals.append({"unit": "retain", "reason": str(error)})
                continue
            retained[path] = discovery.seen[path]
            reasons[path] = "retain"
        elif _match(path, entry.path, entry.exclude):
            excluded[0] += 1
            excluded[1] += size
        else:
            uncovered.append((path, size))
    report = {
        "retained": {
            "files": len(retained),
            "bytes": sum(item.size_bytes for item in retained.values()),
        },
        "excluded": {"files": excluded[0], "bytes": excluded[1]},
        "uncovered": {
            "files": len(uncovered),
            "bytes": sum(size for _, size in uncovered),
            "paths": [
                path.relative_to(entry.path).as_posix() for path, _ in uncovered[:_UNCOVERED_PATHS]
            ],
        },
    }
    return retained, reasons, report, refusals


def _relative(entry: Entry, path: Path) -> str:
    if path == entry.path or not path.is_relative_to(entry.path):
        return path.name
    return path.relative_to(entry.path).as_posix()


def _inventory_document(
    entry: Entry, retained: dict[Path, SourceFile], reasons: dict[Path, str]
) -> bytes:
    """The canonical retained-file inventory of one entry: path, hash, size and reason."""
    files = sorted(
        [_relative(entry, path), item.sha256, item.size_bytes, reasons[path]]
        for path, item in retained.items()
    )
    return schema.encoded(
        {"format": RETAINED_FORMAT, "entry": entry.name, "loader": entry.loader, "files": files}
    ).encode()


def _inventory_reader(document: bytes) -> pa.RecordBatchReader:
    import pyarrow as pa  # noqa: PLC0415 -- the Arrow loaders need the legacy extra

    files = cast("list[list[object]]", json.loads(document)["files"])
    columns = list(zip(*files, strict=True)) if files else [[], [], [], []]
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(list(column), type=f.type)
            for column, f in zip(columns, RETAINED.schema(), strict=True)
        ],
        schema=RETAINED.schema(),
    )
    return pa.RecordBatchReader.from_batches(RETAINED.schema(), [batch])


def _inventory_content(document: bytes, files: tuple[SourceFile, ...]) -> SourceContent:
    index = SourceFile(hashlib.sha256(document).hexdigest(), len(document))
    return SourceContent(RETAINED.provider, RETAINED.shape, SCHEMA_MAJOR, (index, *files))


def _derive_inventory(document: bytes, files: tuple[SourceFile, ...]) -> dict[str, object]:
    from aegis_alpha.storage.source_library_digest import arrow_digest  # noqa: PLC0415

    rows, digest = arrow_digest(_inventory_reader(document))
    return {
        "unit": "retained",
        "table": RETAINED.name,
        "source_id": _inventory_content(document, files).source_id,
        "rows": rows,
        "digest": digest,
    }


def _report(
    manifest: Manifest,
    mode: str,
    commit: _Commit,
    retain: _Retain,
    *,
    stop_on_refusal: bool,
) -> dict[str, object]:
    entries = []
    totals = {"units": 0, "sources": 0, "rows": 0, "refused_units": 0, "uncovered_files": 0}
    for entry in manifest.entries:
        loader = _loader(entry)
        run = Run()
        sources: list[dict[str, object]] = []
        refusals: list[dict[str, str]] = []
        discovery = OriginalBytes()
        try:
            units = loader.units(entry, discovery, run)
            retained, reasons, coverage, coverage_refusals = _coverage(entry, units, discovery)
        except (OSError, ValueError) as error:
            if stop_on_refusal:
                error.add_note(f"legacy entry {entry.name}")
                raise
            entries.append(_entry(entry, [], [{"unit": "", "reason": str(error)}], run, {}))
            totals["refused_units"] += 1
            continue
        if coverage_refusals and stop_on_refusal:
            raise ValueError(coverage_refusals[0]["reason"])
        refusals.extend(coverage_refusals)
        missing, inventory = retain(entry, retained, reasons)
        cast("dict[str, object]", coverage["retained"])["inventory"] = inventory
        files: dict[str, int] = {}
        for unit in units:
            try:
                produced = commit(entry, loader, unit, run)
            except (OSError, ValueError) as error:
                if stop_on_refusal:
                    error.add_note(f"legacy entry {entry.name} unit {unit.name}")
                    raise
                refusals.append({"unit": unit.name, "reason": str(error)})
                continue
            for item in produced:
                files.update(cast("list[tuple[str, int]]", item.pop("_files")))
            sources.extend(produced)
        loader.finish(entry, discovery, run)
        files_report: dict[str, object] = {"files": len(files), "bytes": sum(files.values())}
        files_report.update(coverage)
        if missing:
            files_report["retained_unmatched"] = missing
        entries.append(_entry(entry, sources, refusals, run, files_report, units=len(units)))
        totals["units"] += len(units)
        totals["sources"] += len(sources)
        totals["rows"] += sum(cast("int", item["rows"]) for item in sources)
        totals["refused_units"] += len(refusals)
        totals["uncovered_files"] += cast("dict[str, int]", coverage["uncovered"])["files"]
    reconciled = all(
        all(
            check["matched"] for check in cast("dict[str, dict[str, object]]", e["expect"]).values()
        )
        and not e["refusals"]
        for e in entries
    )
    return {
        "mode": mode,
        "manifest_sha256": manifest.sha256,
        "entries": entries,
        "totals": totals,
        "reconciled": reconciled,
        "provider_calls": 0,
    }


def _entry(  # noqa: PLR0913 -- one report row
    entry: Entry,
    sources: list[dict[str, object]],
    refusals: list[dict[str, str]],
    run: Run,
    files: dict[str, object],
    *,
    units: int = 0,
) -> dict[str, object]:
    loader = LOADERS[entry.loader]
    metrics = {name: run.metrics.get(name, 0) for name in sorted(loader.metric_names)}
    expect = {}
    expected_counts = dict.fromkeys(loader.zero_metrics, 0) | entry.expect
    for metric, expected in sorted(expected_counts.items()):
        expect[metric] = {
            "expected": expected,
            "observed": metrics[metric],
            "matched": metrics[metric] == expected,
        }
    return {
        "name": entry.name,
        "loader": entry.loader,
        "units": units,
        **files,
        "sources": sources,
        "metrics": metrics,
        "expect": expect,
        "refusals": refusals,
    }


def _with_files(derived: list[dict[str, object]], files: tuple[SourceFile, ...]) -> None:
    for item in derived:
        item["_files"] = [(item_file.sha256, item_file.size_bytes) for item_file in files]


def plan_import(manifest: Manifest) -> dict[str, object]:
    """Read and reconcile every unit from the originals; write nothing."""
    for entry in manifest.entries:
        _loader(entry)

    return _report(manifest, "plan", _derive, _plan_retain, stop_on_refusal=False)


def _plan_retain(
    entry: Entry, retained: dict[Path, SourceFile], reasons: dict[Path, str]
) -> tuple[list[dict[str, str]], dict[str, object] | None]:
    if not retained:
        return [], None
    document = _inventory_document(entry, retained, reasons)
    return [], _derive_inventory(document, tuple(retained.values()))


def apply_import(workspace: Workspace, manifest: Manifest) -> dict[str, object]:
    """Retain each unit's originals in ``raw/`` and commit its tables as content sources."""
    from aegis_alpha.storage.raw import put_raw, put_raw_file  # noqa: PLC0415
    from aegis_alpha.storage.source_library import import_content_arrow  # noqa: PLC0415

    for entry in manifest.entries:
        _loader(entry)
    put_raw(workspace.paths.raw, manifest.raw)

    def commit(entry: Entry, loader: Loader, unit: Unit, run: Run) -> list[dict[str, object]]:
        retained: dict[Path, SourceFile] = {}
        for path in unit.files:
            _, digest, size = put_raw_file(workspace.paths.raw, path)
            retained[path] = SourceFile(digest, size)
        files = tuple(retained[path] for path in unit.files)
        produced = []
        for table in unit.tables:
            source = RetainedBytes(workspace.paths.raw, retained)
            content = SourceContent(table.provider, table.shape, SCHEMA_MAJOR, files)
            result = import_content_arrow(
                workspace,
                content,
                table.name,
                _reader(loader, unit, table, source, run),
                lineage={
                    "loader": loader.name,
                    "entry": entry.name,
                    "unit": unit.name,
                    "manifest_sha256": manifest.sha256,
                },
            )
            source.files(unit.files)
            committed = cast("list[dict[str, object]]", result["tables"])[0]
            produced.append(
                {
                    "unit": unit.name,
                    "table": table.name,
                    "source_id": content.source_id,
                    "rows": committed["rows"],
                    "digest": committed["digest"],
                    "reused": result["reused"],
                    "link": result.get("link"),
                }
            )
        _with_files(produced, files)
        return produced

    def retain(
        entry: Entry, retained: dict[Path, SourceFile], reasons: dict[Path, str]
    ) -> tuple[list[dict[str, str]], dict[str, object] | None]:
        for path, item in retained.items():
            _, digest, size = put_raw_file(workspace.paths.raw, path)
            if SourceFile(digest, size) != item:
                message = f"legacy index file changed after it was read: {path.name}"
                raise ValueError(message + f" (entry {entry.name})")
        if not retained:
            return [], None
        document = _inventory_document(entry, retained, reasons)
        put_raw(workspace.paths.raw, document)
        content = _inventory_content(document, tuple(retained.values()))
        result = import_content_arrow(
            workspace,
            content,
            RETAINED.name,
            _inventory_reader(document),
            lineage={
                "loader": entry.loader,
                "entry": entry.name,
                "unit": "retained",
                "manifest_sha256": manifest.sha256,
            },
        )
        committed = cast("list[dict[str, object]]", result["tables"])[0]
        return [], {
            "unit": "retained",
            "table": RETAINED.name,
            "source_id": content.source_id,
            "rows": committed["rows"],
            "digest": committed["digest"],
            "reused": result["reused"],
            "link": result.get("link"),
        }

    return _report(manifest, "apply", commit, retain, stop_on_refusal=True)


def _status(workspace: Workspace, item: dict[str, object]) -> str:
    from aegis_alpha.storage.source_identity import link_source  # noqa: PLC0415
    from aegis_alpha.storage.source_library import _marker, _verify_manifest  # noqa: PLC0415
    from aegis_alpha.storage.state import get_operation  # noqa: PLC0415

    source_id = str(item["source_id"])
    marker = _marker(workspace, source_id)
    if marker is None:
        return "missing"
    operation = get_operation(workspace.state, str(marker[0]))
    if operation is None or operation["phase"] != "COMPLETED":
        return "incomplete"
    stored = json.loads(str(marker[4]))
    tables = stored["tables"]
    if [(t["name"], t["rows"], t["digest"]) for t in tables] != [
        (item["table"], item["rows"], item["digest"])
    ]:
        return "mismatch"
    try:
        # The marker records what was committed; the stored rows are rehashed against it.
        _verify_manifest(workspace, stored)
    except ValueError:
        return "table_mismatch"
    link = link_source(workspace, source_id, apply=False)
    return "committed" if link == "unchanged" else f"link_{link}"


def verify_import(workspace: Workspace, manifest: Manifest) -> dict[str, object]:
    """Check that every planned source is committed, identical and linked to intact raw."""
    for entry in manifest.entries:
        _loader(entry)

    def commit(entry: Entry, loader: Loader, unit: Unit, run: Run) -> list[dict[str, object]]:
        derived = _derive(entry, loader, unit, run)
        for item in derived:
            item["status"] = _status(workspace, item)
        return derived

    def retain(
        entry: Entry, retained: dict[Path, SourceFile], reasons: dict[Path, str]
    ) -> tuple[list[dict[str, str]], dict[str, object] | None]:
        from aegis_alpha.storage.raw import verify_raw  # noqa: PLC0415

        missing = []
        for path, item in retained.items():
            try:
                verify_raw(workspace.paths.raw, item.relative_path, item.sha256, item.size_bytes)
            except (OSError, ValueError, DescriptorTreeError):
                relative = path.relative_to(entry.path) if path.is_relative_to(entry.path) else path
                missing.append({"file": str(relative), "status": "not_retained"})
        if not retained:
            return missing, None
        document = _inventory_document(entry, retained, reasons)
        derived = _derive_inventory(document, tuple(retained.values()))
        derived["status"] = _status(workspace, derived)
        return missing, derived

    report = _report(manifest, "verify", commit, retain, stop_on_refusal=False)
    entries = cast("list[dict[str, object]]", report["entries"])
    unmatched: list[dict[str, object]] = [
        {"entry": entry["name"], "unit": source["unit"], "status": source["status"]}
        for entry in entries
        for source in cast("list[dict[str, object]]", entry["sources"])
        if source["status"] != "committed"
    ]
    unmatched.extend(
        {"entry": entry["name"], "unit": "retained", "status": item["status"]}
        for entry in entries
        if (item := _inventory_of(entry)) is not None and item["status"] != "committed"
    )
    unmatched.extend(
        {"entry": entry["name"], **item}
        for entry in entries
        for item in cast("list[dict[str, str]]", entry.get("retained_unmatched", []))
    )
    totals = cast("dict[str, int]", report["totals"])
    report["unmatched"] = len(unmatched) + totals["refused_units"]
    report["unmatched_sources"] = unmatched
    report["complete"] = (
        report["unmatched"] == 0 and bool(report["reconciled"]) and totals["uncovered_files"] == 0
    )
    return report


def _inventory_of(entry: dict[str, object]) -> dict[str, object] | None:
    retained = cast("dict[str, object] | None", entry.get("retained"))
    if retained is None:
        return None
    return cast("dict[str, object] | None", retained.get("inventory"))
