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
  source is committed and complete in the installation with the same rows and digest, and
  that its ``sl:`` link matches and every linked original is intact in ``raw/``. A unit with
  no such source is ``unmatched``; zero unmatched sources is the precondition for deleting
  the originals outside the store.

Every report states, per entry, the loader's reconciliation metrics beside the manifest's
``expect`` counts. No provider is called and no clock value reaches a stored row.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data.descriptor_tree import DescriptorTree, DescriptorTreeError
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
        FredSeriesCsv(),
        KoreaPublicResponse(),
        FmpNonSplit(),
    )
}

type _Commit = Callable[[Entry, Loader, Unit, Run], list[dict[str, object]]]


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


def _report(
    manifest: Manifest, mode: str, commit: _Commit, *, stop_on_refusal: bool
) -> dict[str, object]:
    entries = []
    totals = {"units": 0, "sources": 0, "rows": 0, "refused_units": 0}
    for entry in manifest.entries:
        loader = _loader(entry)
        run = Run()
        sources: list[dict[str, object]] = []
        refusals: list[dict[str, str]] = []
        discovery = OriginalBytes()
        try:
            units = loader.units(entry, discovery, run)
        except (OSError, ValueError) as error:
            if stop_on_refusal:
                error.add_note(f"legacy entry {entry.name}")
                raise
            entries.append(_entry(entry, [], [{"unit": "", "reason": str(error)}], run, {}))
            totals["refused_units"] += 1
            continue
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
        files_report = {"files": len(files), "bytes": sum(files.values())}
        entries.append(_entry(entry, sources, refusals, run, files_report, units=len(units)))
        totals["units"] += len(units)
        totals["sources"] += len(sources)
        totals["rows"] += sum(cast("int", item["rows"]) for item in sources)
        totals["refused_units"] += len(refusals)
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
    files: dict[str, int],
    *,
    units: int = 0,
) -> dict[str, object]:
    loader = LOADERS[entry.loader]
    metrics = {name: run.metrics.get(name, 0) for name in sorted(loader.metric_names)}
    expect = {}
    for metric, expected in sorted(entry.expect.items()):
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

    return _report(manifest, "plan", _derive, stop_on_refusal=False)


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

    return _report(manifest, "apply", commit, stop_on_refusal=True)


def _status(workspace: Workspace, item: dict[str, object]) -> str:
    from aegis_alpha.storage.source_identity import link_source  # noqa: PLC0415
    from aegis_alpha.storage.source_library import _marker  # noqa: PLC0415
    from aegis_alpha.storage.state import get_operation  # noqa: PLC0415

    source_id = str(item["source_id"])
    marker = _marker(workspace, source_id)
    if marker is None:
        return "missing"
    operation = get_operation(workspace.state, str(marker[0]))
    if operation is None or operation["phase"] != "COMPLETED":
        return "incomplete"
    tables = json.loads(str(marker[4]))["tables"]
    if [(t["name"], t["rows"], t["digest"]) for t in tables] != [
        (item["table"], item["rows"], item["digest"])
    ]:
        return "mismatch"
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

    report = _report(manifest, "verify", commit, stop_on_refusal=False)
    unmatched = [
        {"entry": entry["name"], "unit": source["unit"], "status": source["status"]}
        for entry in cast("list[dict[str, object]]", report["entries"])
        for source in cast("list[dict[str, object]]", entry["sources"])
        if source["status"] != "committed"
    ]
    report["unmatched"] = len(unmatched) + cast("dict[str, int]", report["totals"])["refused_units"]
    report["unmatched_sources"] = unmatched
    report["complete"] = report["unmatched"] == 0 and bool(report["reconciled"])
    return report
