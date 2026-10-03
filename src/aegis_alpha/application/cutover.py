"""``aas maintain cutover-check``: the operations cutover checklist and its retirement record.

The operations cutover moves a running installation from the legacy units to ``aas
maintain``. Its last step proves the result. This module checks the installation, a
backup and the user's systemd units, read-only:

- ``schema``: the core schema is current;
- ``operations``: no storage operation is left ``PREPARED``;
- ``backup``: the named backup verifies file by file, was taken with ``--deep`` (so the
  installation's every source table and promoted delta was rehashed before it was
  copied), belongs to this installation, holds every finished storage operation and
  lies on another device than the installation root, state, market and ``raw/`` (the
  devices ``aas db source-retire`` requires a backup to avoid);
- ``units``: the only ``aas-*`` user units are ``aas-maintain.service`` and an enabled,
  active ``aas-maintain.timer``;
- ``collectors``: ``jobs.enabled`` and every expected provider section are on;
- ``install_receipt``: the installed tool recorded its receipt;
- ``maintain_run``: the latest ``aas maintain run`` report of this installation succeeded;
- ``legacy``: every legacy-import manifest named is retained in ``raw/`` and has a
  ``--verify`` report for its exact bytes that is ``complete``, every source that report
  matched is still committed (or retired) in this installation, the manifest's entry paths
  are gone, and every other path named as removed is gone.

The report also states what the cutover retired: ``source_retirements`` by backup,
operation and reason, and the size of ``raw/`` (retirement never deletes raw bytes, so the
archives it holds stay). ``--record`` keeps a passing report as an
``aas-cutover-record-v1`` document: its exact bytes in ``raw/`` and in
``<runtime>/cutover-record.json``, beside the exact bytes of every legacy ``--verify``
report it names (the record holds their SHA-256). A failing report is printed and never
recorded.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import subprocess
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data.serialization import canonical_json_bytes

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from aegis_alpha.storage.paths import StoragePaths
    from aegis_alpha.storage.workspace import Workspace

    type Systemctl = Callable[[Sequence[str]], str]

RECORD_SCHEMA: Final = "aas-cutover-record-v1"
RECORD_NAME: Final = "cutover-record.json"
MAINTAIN_SERVICE: Final = "aas-maintain.service"
MAINTAIN_TIMER: Final = "aas-maintain.timer"
UNIT_PATTERN: Final = "aas-*"
# The installation paths a backup must not share a device with: the same set
# ``aas db source-retire`` refuses a backup for, so a passing check names a backup that
# retirement also accepts.
PROTECTED_STORES: Final = ("root", "state", "market", "raw")
_SAMPLE: Final = 20
_MAX_REPORT_BYTES: Final = 64 * 1024 * 1024
_MAX_MANIFEST_BYTES: Final = 1024 * 1024
_MATCHED: Final = frozenset({"committed", "retired"})
_SYSTEMCTL_SECONDS: Final = 30


@dataclass(frozen=True, slots=True)
class CutoverRequest:
    """What the check is asked to prove beyond the installation and its backup."""

    expect_providers: tuple[str, ...] = ()
    legacy_manifests: tuple[Path, ...] = ()
    legacy_verify: tuple[Path, ...] = ()
    removed: tuple[Path, ...] = ()


def systemctl(arguments: Sequence[str]) -> str:
    """Run one read-only ``systemctl --user`` query and return its standard output."""
    completed = subprocess.run(  # noqa: S603 -- fixed executable, arguments built here
        ["systemctl", "--user", *arguments],  # noqa: S607 -- the user's systemctl on PATH
        capture_output=True,
        text=True,
        timeout=_SYSTEMCTL_SECONDS,
        check=True,
    )
    return completed.stdout


def _columns(output: str, width: int) -> dict[str, list[str]]:
    rows: dict[str, list[str]] = {}
    for line in output.splitlines():
        fields = line.split(maxsplit=width)
        if len(fields) >= 2:  # noqa: PLR2004 -- a unit name and at least one state
            rows[fields[0]] = fields[1:width]
    return rows


def unit_check(run: Systemctl) -> dict[str, object]:
    """The user's ``aas-*`` units: only the maintenance pair, its timer enabled and active."""
    try:
        files = _columns(
            run(["list-unit-files", "--no-legend", "--plain", "--no-pager", UNIT_PATTERN]), 3
        )
        loaded = _columns(
            run(["list-units", "--all", "--no-legend", "--plain", "--no-pager", UNIT_PATTERN]), 4
        )
    except (OSError, subprocess.SubprocessError) as error:
        return {"passed": False, "error": "systemctl_unavailable", "detail": type(error).__name__}
    names = sorted(set(files) | set(loaded))
    other = [name for name in names if name not in {MAINTAIN_SERVICE, MAINTAIN_TIMER}]
    timer_file = files.get(MAINTAIN_TIMER, [None])[0]
    timer_active = loaded.get(MAINTAIN_TIMER, [None, None])[1]
    units = {
        name: {
            "file_state": files[name][0] if name in files else None,
            "active": loaded[name][1] if name in loaded and len(loaded[name]) > 1 else None,
        }
        for name in names
    }
    return {
        "passed": not other
        and MAINTAIN_SERVICE in files
        and timer_file == "enabled"
        and timer_active == "active",
        "other_units": other,
        "maintain_service_installed": MAINTAIN_SERVICE in files,
        "maintain_timer": {"file_state": timer_file, "active": timer_active},
        "units": units,
    }


def _operation_ids(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT operation_id FROM storage_operations WHERE phase != 'PREPARED'"
        )
    }


def _backup_operations(root: Path) -> set[str]:
    # immutable=1 reads the copy without creating a -wal or -shm file inside the backup.
    uri = (root / "state.sqlite3").absolute().as_uri() + "?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    try:
        return _operation_ids(connection)
    finally:
        connection.close()


def _device(path: Path) -> int:
    return path.stat().st_dev


@dataclass(frozen=True, slots=True)
class BackupEvidence:
    """A backup read before the installation is opened: its manifest or why it is not one."""

    root: Path
    manifest: dict[str, object] | None
    backup_id: str | None
    operations: frozenset[str]
    error: str | None = None


def read_backup(root: Path) -> BackupEvidence:
    """Rehash every file ``root`` lists and read the operations its state copy holds.

    This reads the whole backup, so it runs before the installation's locks are taken.
    """
    from aegis_alpha.storage.backup import manifest_sha256, validated_backup  # noqa: PLC0415

    root = root.absolute()
    try:
        manifest = validated_backup(root)
        backup_id = manifest_sha256(root)
        held = _backup_operations(root)
    except (ValueError, TypeError, OSError, sqlite3.Error) as error:
        return BackupEvidence(root, None, None, frozenset(), str(error))
    return BackupEvidence(root, manifest, backup_id, frozenset(held))


def backup_check(evidence: BackupEvidence, workspace: Workspace) -> dict[str, object]:
    """Whether the backup is a deep, complete, current backup of this installation elsewhere."""
    root, manifest = evidence.root, evidence.manifest
    if manifest is None:
        return {"passed": False, "backup_root": str(root), "error": evidence.error}
    paths = workspace.paths
    stores = {"root": paths.root, "market": paths.market, "raw": paths.raw, "state": paths.state}
    if paths.strategies.exists():
        stores["strategies"] = paths.strategies
    device = _device(root)
    shared = sorted(name for name, path in stores.items() if _device(path) == device)
    missing = sorted(_operation_ids(workspace.state) - evidence.operations)
    logical = manifest.get("logical")
    files = manifest.get("files")
    checks = {
        "deep": manifest.get("deep") is True,
        "same_installation": manifest.get("installation_id") == workspace.installation_id,
        "holds_every_operation": not missing,
        "other_device": not set(PROTECTED_STORES) & set(shared),
    }
    return {
        "passed": all(checks.values()),
        **checks,
        "backup_root": str(root),
        "backup_id": evidence.backup_id,
        "files": len(files) if isinstance(files, dict) else None,
        "logical": logical,
        "shared_device_with": shared,
        "operations_after_backup": len(missing),
        "operations_after_backup_sample": missing[:_SAMPLE],
    }


def _schema_check(workspace: Workspace) -> dict[str, object]:
    from aegis_alpha.storage.migration import inspect_core_schema  # noqa: PLC0415

    status = inspect_core_schema(workspace)
    return {
        "passed": status.state == "current",
        "state": status.state,
        "versions": {"state": status.state_version, "market": status.market_version},
        "current": status.target_version,
    }


def _operations_check(workspace: Workspace) -> dict[str, object]:
    prepared = [
        {"operation_id": str(row[0]), "kind": str(row[1])}
        for row in workspace.state.execute(
            "SELECT operation_id, kind FROM storage_operations WHERE phase='PREPARED' "
            "ORDER BY created_at_us, operation_id"
        )
    ]
    return {"passed": not prepared, "prepared": len(prepared), "sample": prepared[:_SAMPLE]}


def _collectors_check(paths: StoragePaths, expected: Sequence[str]) -> dict[str, object]:
    from aegis_alpha.application.maintain_config import PROVIDERS, load_config  # noqa: PLC0415

    unknown = sorted(set(expected) - set(PROVIDERS))
    if unknown:
        raise ValueError("unknown expected providers: " + ", ".join(unknown))
    try:
        config = load_config(paths)
    except (ValueError, TypeError, OSError) as error:
        return {"passed": False, "error": str(error)}
    enabled = config.enabled()
    missing = [name for name in PROVIDERS if name in expected and name not in enabled]
    return {
        "passed": config.jobs_enabled and not missing,
        "jobs_enabled": config.jobs_enabled,
        "enabled": enabled,
        "expected": [name for name in PROVIDERS if name in expected],
        "missing": missing,
    }


def _receipt_check(paths: StoragePaths) -> dict[str, object]:
    from aegis_alpha.application.install_receipt import runtime_report  # noqa: PLC0415

    try:
        report = runtime_report(paths)
    except (ValueError, TypeError, OSError) as error:
        return {"passed": False, "error": str(error)}
    return {
        "passed": report["receipt_sha256"] is not None,
        "receipt_sha256": report["receipt_sha256"],
        "differences": report["differences"],
    }


def _maintain_check(paths: StoragePaths, installation_id: str) -> dict[str, object]:
    import json  # noqa: PLC0415

    from aegis_alpha.application.maintain import REPORT_NAME  # noqa: PLC0415
    from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415

    if not (paths.runtime / REPORT_NAME).exists():
        return {"passed": False, "report_sha256": None}
    with DescriptorTree.open_path(paths.runtime) as tree:
        raw = tree.read_bytes(REPORT_NAME, max_bytes=_MAX_REPORT_BYTES)
    report = json.loads(raw)
    if not isinstance(report, dict):
        return {"passed": False, "error": "the maintain report is not an object"}
    report = cast("dict[str, object]", report)
    same = report.get("installation_id") == installation_id
    return {
        "passed": same and report.get("mode") == "run" and report.get("status") == "succeeded",
        "report_sha256": hashlib.sha256(raw).hexdigest(),
        "same_installation": same,
        "status": report.get("status"),
        "started_at_utc": report.get("started_at_utc"),
        "failed_stages": report.get("failed_stages"),
        "stopped_providers": report.get("stopped_providers"),
    }


def _read_small(path: Path, max_bytes: int = _MAX_MANIFEST_BYTES) -> bytes:
    from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415

    absolute = path.absolute()
    with DescriptorTree.open_path(absolute.parent) as tree:
        return tree.read_bytes(absolute.name, max_bytes=max_bytes)


def _in_raw(raw_root: Path, payload: bytes) -> bool:
    from aegis_alpha.data.descriptor_tree import DescriptorTreeError  # noqa: PLC0415
    from aegis_alpha.storage.raw import verify_raw  # noqa: PLC0415

    digest = hashlib.sha256(payload).hexdigest()
    try:
        verify_raw(raw_root, digest[:2] + "/" + digest, digest, len(payload))
    except (ValueError, OSError, DescriptorTreeError):
        return False
    return True


@dataclass(frozen=True, slots=True)
class VerifyReport:
    """One ``aas import legacy --verify`` report: its path, exact bytes and parsed document."""

    path: Path
    raw: bytes
    document: dict[str, object]

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


def read_verify_reports(paths: Sequence[Path]) -> tuple[list[VerifyReport], list[dict[str, str]]]:
    """The ``--verify`` reports named, and the ones that could not be read as one."""
    import json  # noqa: PLC0415

    from aegis_alpha.data.descriptor_tree import DescriptorTreeError  # noqa: PLC0415

    reports: list[VerifyReport] = []
    errors: list[dict[str, str]] = []
    for path in paths:
        try:
            raw = _read_small(path, _MAX_REPORT_BYTES)
            document = json.loads(raw)
        except (ValueError, TypeError, OSError, DescriptorTreeError) as error:
            errors.append({"report": str(path), "error": str(error)})
            continue
        if not isinstance(document, dict):
            errors.append({"report": str(path), "error": "the verify report is not an object"})
            continue
        reports.append(VerifyReport(path, raw, cast("dict[str, object]", document)))
    return reports, errors


def _verified_sources(document: dict[str, object]) -> list[dict[str, object]]:
    found: list[dict[str, object]] = []
    for entry in cast("list[dict[str, object]]", document.get("entries") or []):
        found.extend(cast("list[dict[str, object]]", entry.get("sources") or []))
        retained = cast("dict[str, object] | None", entry.get("retained"))
        inventory = retained.get("inventory") if retained else None
        if isinstance(inventory, dict):
            found.append(cast("dict[str, object]", inventory))
    return found


def _source_state(workspace: Workspace, source_id: str) -> str:
    from aegis_alpha.storage.source_library import _marker  # noqa: PLC0415
    from aegis_alpha.storage.state import get_operation  # noqa: PLC0415

    marker = _marker(workspace, source_id)
    if marker is None:
        return "missing"
    operation = get_operation(workspace.state, str(marker[0]))
    if operation is None or operation["phase"] != "COMPLETED":
        return "incomplete"
    return "committed"


def _verification(
    workspace: Workspace, digest: str, reports: Sequence[VerifyReport]
) -> dict[str, object]:
    """Whether a ``complete`` verify report of this manifest still holds in the installation."""
    matching = [r for r in reports if r.document.get("manifest_sha256") == digest]
    if not matching:
        return {"passed": False, "report": None}
    report = matching[-1]
    document = report.document
    sources = _verified_sources(document)
    unmatched = [
        {"source_id": str(item.get("source_id")), "status": str(item.get("status"))}
        for item in sources
        if item.get("status") not in _MATCHED
    ]
    absent = [
        {"source_id": source_id, "state": state}
        for item in sources
        if (state := _source_state(workspace, source_id := str(item.get("source_id"))))
        != "committed"
    ]
    complete = document.get("mode") == "verify" and document.get("complete") is True
    return {
        "passed": complete and bool(sources) and not unmatched and not absent,
        "report": str(report.path),
        "report_sha256": report.sha256,
        "mode": document.get("mode"),
        "complete": document.get("complete"),
        "sources": len(sources),
        "unmatched": unmatched[:_SAMPLE],
        "not_committed": absent[:_SAMPLE],
    }


def legacy_check(
    workspace: Workspace,
    manifests: Sequence[Path],
    verify_reports: Sequence[Path],
    removed: Sequence[Path],
) -> dict[str, object]:
    """Verified legacy manifests whose entry paths are gone, and other removed paths.

    A manifest passes when its exact bytes are retained in ``raw/``, a ``--verify`` report
    of those bytes is ``complete``, every source that report lists is matched there and is
    still committed here (a retired source keeps its commit marker), and none of its entry
    paths exist. ``apply`` retains the manifest before its first unit, so retention alone
    does not prove the import finished; the verify report does.
    """
    from aegis_alpha.data.descriptor_tree import DescriptorTreeError  # noqa: PLC0415
    from aegis_alpha.storage.legacy_import.manifest import parse_manifest  # noqa: PLC0415

    reports, unreadable = read_verify_reports(verify_reports)
    items: list[dict[str, object]] = []
    for path in manifests:
        try:
            payload = _read_small(path)
            digest = hashlib.sha256(payload).hexdigest()
            entries = parse_manifest(payload, digest).entries
        except (ValueError, TypeError, OSError, DescriptorTreeError) as error:
            items.append({"manifest": str(path), "passed": False, "error": str(error)})
            continue
        retained = _in_raw(workspace.paths.raw, payload)
        verified = _verification(workspace, digest, reports)
        present = [entry.name for entry in entries if os.path.lexists(entry.path)]
        items.append(
            {
                "manifest": str(path),
                "manifest_sha256": digest,
                "retained": retained,
                "verified": verified,
                "entries": [{"name": e.name, "path": str(e.path)} for e in entries],
                "present": present,
                "passed": retained and verified["passed"] is True and not present,
            }
        )
    paths = [{"path": str(p), "removed": not os.path.lexists(p)} for p in removed]
    return {
        "passed": not unreadable
        and all(item["passed"] for item in items)
        and all(p["removed"] for p in paths),
        "manifests": items,
        "unreadable_reports": unreadable,
        "removed": paths,
    }


def retirement_summary(workspace: Workspace) -> dict[str, object]:
    """What ``source_retirements`` records, grouped by backup, operation and reason."""
    from aegis_alpha.storage.source_library import retired_sources  # noqa: PLC0415

    rows = list(retired_sources(workspace).values())

    def grouped(key: str) -> dict[str, dict[str, int]]:
        sources: Counter[str] = Counter()
        counted: Counter[str] = Counter()
        for row in rows:
            sources[str(row[key])] += 1
            counted[str(row[key])] += cast("int", row["rows"])
        return {name: {"sources": sources[name], "rows": counted[name]} for name in sorted(sources)}

    return {
        "sources": len(rows),
        "rows": sum(cast("int", row["rows"]) for row in rows),
        "by_backup": grouped("backup_id"),
        "by_operation": grouped("operation_id"),
        "by_reason": grouped("reason"),
    }


def raw_summary(raw_root: Path) -> dict[str, int]:
    """The files and bytes in ``raw/``; retirement and compaction never remove any."""
    files = size = 0
    for directory, _, names in os.walk(raw_root):
        for name in names:
            files += 1
            size += os.lstat(Path(directory) / name).st_size
    return {"files": files, "bytes": size}


def check_cutover(
    workspace: Workspace,
    backup: BackupEvidence,
    request: CutoverRequest,
    *,
    run: Systemctl | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Check the cutover read-only; ``passed`` only when every check passed."""
    paths = workspace.paths
    run = run or systemctl
    checks: dict[str, dict[str, object]] = {
        "schema": _schema_check(workspace),
        "operations": _operations_check(workspace),
        "backup": backup_check(backup, workspace),
        "units": unit_check(run),
        "collectors": _collectors_check(paths, request.expect_providers),
        "install_receipt": _receipt_check(paths),
        "maintain_run": _maintain_check(paths, workspace.installation_id),
        "legacy": legacy_check(
            workspace, request.legacy_manifests, request.legacy_verify, request.removed
        ),
    }
    failed = [name for name, check in checks.items() if check["passed"] is not True]
    return {
        "schema": RECORD_SCHEMA,
        "checked_at_utc": (now or datetime.now(UTC)).astimezone(UTC).isoformat(),
        "installation_id": workspace.installation_id,
        "passed": not failed,
        "failed": failed,
        "checks": checks,
        "retirements": retirement_summary(workspace),
        "raw": raw_summary(paths.raw),
    }


def write_record(
    paths: StoragePaths, report: dict[str, object], verify_reports: Sequence[Path] = ()
) -> str:
    """Keep a passing report's exact bytes in ``raw/`` and ``<runtime>/cutover-record.json``.

    The legacy ``--verify`` reports the record names are kept in ``raw/`` first, by the
    exact bytes whose SHA-256 the record holds.
    """
    from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415
    from aegis_alpha.storage.locks import private_directory  # noqa: PLC0415
    from aegis_alpha.storage.raw import put_raw  # noqa: PLC0415

    if report.get("passed") is not True:
        raise ValueError("only a passing cutover check is recorded")
    named = {
        str(verified.get("report_sha256"))
        for item in cast(
            "list[dict[str, object]]",
            cast("dict[str, dict[str, object]]", report["checks"])["legacy"]["manifests"],
        )
        if isinstance(verified := item.get("verified"), dict)
    }
    reports, _ = read_verify_reports(verify_reports)
    kept = {r.sha256 for r in reports if r.sha256 in named}
    if kept != named:
        raise ValueError("a verify report the check named changed before it was recorded")
    for verify in reports:
        if verify.sha256 in named:
            put_raw(paths.raw, verify.raw)
    raw = canonical_json_bytes(report)
    _, digest, _ = put_raw(paths.raw, raw)
    private_directory(paths.runtime, create=True)
    with DescriptorTree.open_path(paths.runtime) as tree:
        tree.atomic_write_bytes(RECORD_NAME, raw)
    return digest
