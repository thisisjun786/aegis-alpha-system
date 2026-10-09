"""``aas maintain cutover-check``: the cutover checklist and its record, offline.

A synthetic installation is cut over the way the runbook does it: a legacy import verified
``complete`` whose original is then deleted, collectors and the install receipt configured,
a succeeded maintenance report and a deep backup on "another device". ``systemctl`` and device
numbers are fakes; nothing here reads the account's units or disks.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.application import cutover
from aegis_alpha.application.cli import main
from aegis_alpha.application.cutover import (
    CutoverRequest,
    check_cutover,
    read_backup,
    unit_check,
    write_record,
)
from aegis_alpha.application.install_receipt import write_receipt
from aegis_alpha.application.maintain import REPORT_NAME
from aegis_alpha.storage import workspace as workspace_module
from aegis_alpha.storage.backup import backup
from aegis_alpha.storage.legacy_import.engine import apply_import, verify_import
from aegis_alpha.storage.legacy_import.manifest import parse_manifest
from aegis_alpha.storage.paths import load_paths
from aegis_alpha.storage.workspace import initialize, open_workspace, write_json

PROVIDERS = ("kind", "dart", "sec", "fred", "qveris")
# The shape of `systemctl --user list-unit-files|list-units --plain --no-legend` output.
CUT_OVER_FILES = "aas-maintain.service static -\naas-maintain.timer enabled enabled\n"
CUT_OVER_UNITS = (
    "aas-maintain.service loaded inactive dead AAS daily maintenance\n"
    "aas-maintain.timer loaded active waiting Run AAS daily maintenance at 03:00 UTC\n"
)
LEGACY_FILES = (
    "aas-native-data-maintenance.service   linked  enabled\n"
    "aas-qveris-korea-backfill.service     static  -\n"
    "aas-native-data-maintenance.timer     enabled enabled\n"
    "aas-qveris-korea-backfill.timer       enabled enabled\n"
)
LEGACY_UNITS = (
    "aas-native-data-maintenance.service   loaded failed   failed  AAS legacy maintenance\n"
    "aas-qveris-korea-backfill.service     loaded inactive dead    AAS legacy backfill\n"
    "aas-native-data-maintenance.timer     loaded active   waiting Maintain twice daily\n"
    "aas-qveris-korea-backfill.timer       loaded active   waiting Resume backfill\n"
)


def _systemctl(files: str, units: str) -> cutover.Systemctl:
    def run(arguments: Sequence[str]) -> str:
        assert arguments[-1] == "aas-*"
        return files if arguments[0] == "list-unit-files" else units

    return run


CUT_OVER = _systemctl(CUT_OVER_FILES, CUT_OVER_UNITS)


def _private(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(payload)
    path.chmod(0o600)
    return path


def _runtime(home: Path, *, jobs: bool = True, off: Sequence[str] = ()) -> None:
    sections: dict[str, dict[str, object]] = {
        "kind": {"enabled": True},
        "dart": {"enabled": True, "key_file": "opendart-api-key"},
        "sec": {"enabled": True, "user_agent_file": "sec-user-agent"},
        "fred": {"enabled": True, "key_file": "fred-api-key"},
        "qveris": {
            "enabled": True,
            "key_file": "qveris-api-key",
            "raw_root": str(home.parent / "qveris-raw"),
            "since": {"US": "2026-09-01", "KO": "2026-09-05", "KQ": "2026-09-05"},
            "max_calls": 30,
            "max_credits": "100",
        },
    }
    for name in off:
        sections[name] = {**sections[name], "enabled": False}
    runtime = json.loads((home / "runtime.json").read_text())
    write_json(home / "runtime.json", {**runtime, "jobs": {"enabled": jobs},
                                       "providers": sections})  # fmt: skip


def _maintain_report(home: Path, *, status: str = "succeeded") -> None:
    with open_workspace(home) as workspace:
        installation = workspace.installation_id
    document = {"schema": "aas-maintain-report-v1", "mode": "run", "status": status,
                "installation_id": installation, "started_at_utc": "2026-10-04T03:00:00+00:00",
                "failed_stages": [], "stopped_providers": []}  # fmt: skip
    _private(load_paths(home).runtime / REPORT_NAME, json.dumps(document).encode())


def _legacy(tmp_path: Path, home: Path) -> Path:
    """Import one legacy FRED download, then delete it as the runbook's cleanup does."""
    original = _private(
        tmp_path / "legacy" / "DEXKOUS.csv",
        b"observation_date,DEXKOUS\n2011-10-03,1180.00\n2011-10-04,\n",
    )
    entry = {"name": "fred", "loader": "fred.series_csv@1", "path": str(original),
             "args": {}, "expect": {"rows": 2}}  # fmt: skip
    raw = json.dumps({"schema_version": "aas-legacy-import-v1", "entries": [entry]}).encode()
    manifest = _private(tmp_path / "specs" / "legacy-import.json", raw)
    parsed = parse_manifest(raw, hashlib.sha256(raw).hexdigest())
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        assert apply_import(workspace, parsed)["reconciled"] is True
        verified = verify_import(workspace, parsed)
    assert verified["complete"] is True
    _private(_verify_path(manifest), json.dumps(verified).encode())
    original.unlink()
    return manifest


def _verify_path(manifest: Path) -> Path:
    return manifest.with_name(manifest.stem + ".verify.json")


@pytest.fixture
def cut_over(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, Path]:
    """A cut-over installation, its legacy manifest and a deep backup on another device."""
    home = tmp_path / "aas"
    initialize(home)
    manifest = _legacy(tmp_path, home)
    _runtime(home)
    write_receipt(load_paths(home), lock=None)
    _maintain_report(home)
    backups = tmp_path / "aas-backups"
    backups.mkdir(mode=0o700)
    backup(home, backups / "20261004", deep=True)
    other = backups.stat().st_dev + 1
    real = cutover._device  # noqa: SLF001 -- the device numbers are the fake
    monkeypatch.setattr(
        cutover,
        "_device",
        lambda path: other if path.is_relative_to(backups) else real(path),
    )
    return home, manifest, backups / "20261004"


def _check(  # noqa: PLR0913 -- the request's parts, defaulted to a passing cutover
    home: Path,
    backup_root: Path,
    *,
    run: cutover.Systemctl = CUT_OVER,
    providers: Sequence[str] = PROVIDERS,
    manifests: Sequence[Path] = (),
    verify: Sequence[Path] | None = None,
    removed: Sequence[Path] = (),
) -> dict[str, object]:
    evidence = read_backup(backup_root)
    reports = [_verify_path(m) for m in manifests] if verify is None else verify
    request = CutoverRequest(tuple(providers), tuple(manifests), tuple(reports), tuple(removed))
    with open_workspace(home) as workspace:
        return check_cutover(workspace, evidence, request, run=run)


def _checks(report: dict[str, object]) -> dict[str, dict[str, object]]:
    return cast("dict[str, dict[str, object]]", report["checks"])


def test_a_cut_over_installation_passes_and_records_it(
    cut_over: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    home, manifest, backup_root = cut_over
    gone = tmp_path / "legacy-worktree"
    report = _check(home, backup_root, manifests=[manifest], removed=[gone])
    assert (report["passed"], report["failed"]) == (True, [])
    checks = _checks(report)
    assert checks["backup"]["deep"] is True
    assert checks["backup"]["shared_device_with"] == []
    assert checks["units"]["other_units"] == []
    assert checks["collectors"]["enabled"] == list(PROVIDERS)
    legacy = checks["legacy"]
    item = cast("list[dict[str, object]]", legacy["manifests"])[0]
    verified = cast("dict[str, object]", item["verified"])
    assert (item["retained"], verified["passed"], verified["complete"]) == (True, True, True)
    assert (
        verified["report_sha256"] == hashlib.sha256(_verify_path(manifest).read_bytes()).hexdigest()
    )
    assert legacy["removed"] == [{"path": str(gone), "removed": True}]
    assert report["retirements"] == {"sources": 0, "rows": 0, "by_backup": {},
                                     "by_operation": {}, "by_reason": {}}  # fmt: skip
    paths = load_paths(home)
    digest = write_record(paths, report, [_verify_path(manifest)])
    kept = cast("str", verified["report_sha256"])
    assert (paths.raw / kept[:2] / kept).read_bytes() == _verify_path(manifest).read_bytes()
    recorded = (paths.runtime / "cutover-record.json").read_bytes()
    assert hashlib.sha256(recorded).hexdigest() == digest
    assert (paths.raw / digest[:2] / digest).read_bytes() == recorded
    assert json.loads(recorded)["schema"] == "aas-cutover-record-v1"


def test_legacy_units_left_behind_fail_the_unit_check() -> None:
    report = unit_check(_systemctl(LEGACY_FILES + CUT_OVER_FILES, LEGACY_UNITS + CUT_OVER_UNITS))
    assert report["passed"] is False
    assert report["other_units"] == [
        "aas-native-data-maintenance.service",
        "aas-native-data-maintenance.timer",
        "aas-qveris-korea-backfill.service",
        "aas-qveris-korea-backfill.timer",
    ]
    # A failed legacy service whose file was removed is still loaded until reset-failed.
    failed_only = "aas-native-data-maintenance.service loaded failed failed AAS legacy\n"
    leftover = unit_check(_systemctl(CUT_OVER_FILES, CUT_OVER_UNITS + failed_only))
    assert leftover["other_units"] == ["aas-native-data-maintenance.service"]


@pytest.mark.parametrize(
    ("files", "units"),
    [
        ("aas-maintain.service static -\naas-maintain.timer disabled enabled\n", CUT_OVER_UNITS),
        (CUT_OVER_FILES, "aas-maintain.timer loaded inactive dead Run AAS daily maintenance\n"),
        ("aas-maintain.timer enabled enabled\n", CUT_OVER_UNITS),
    ],
    ids=["timer-disabled", "timer-inactive", "service-missing"],
)
def test_the_maintenance_timer_must_be_installed_enabled_and_active(files: str, units: str) -> None:
    assert unit_check(_systemctl(files, units))["passed"] is False


def test_an_unavailable_systemctl_fails_closed() -> None:
    def missing(arguments: Sequence[str]) -> str:
        raise FileNotFoundError(arguments[0])

    def refused(arguments: Sequence[str]) -> str:
        raise subprocess.CalledProcessError(1, list(arguments))

    for run in (missing, refused):
        report = unit_check(run)
        assert (report["passed"], report["error"]) == (False, "systemctl_unavailable")


def test_a_backup_must_be_deep_current_and_on_another_device(
    cut_over: tuple[Path, Path, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home, _, deep_backup = cut_over
    shallow = tmp_path / "aas-backups" / "shallow"
    backup(home, shallow)
    assert _checks(_check(home, shallow))["backup"]["deep"] is False
    # An operation finished after the backup is not in it.
    with open_workspace(home, writable=True) as workspace:
        workspace.state.execute(
            "INSERT INTO storage_operations VALUES ('later', 'test', ?, 'x', NULL, ?, "
            "'COMPLETED', NULL, 1, 2)",
            ("a" * 64, "b" * 64),
        )
        workspace.state.commit()
    stale = _checks(_check(home, deep_backup))["backup"]
    assert (stale["passed"], stale["holds_every_operation"]) == (False, False)
    assert stale["operations_after_backup_sample"] == ["later"]
    # The same device as the stores: the fake device numbers are undone.
    monkeypatch.setattr(cutover, "_device", lambda path: path.stat().st_dev)
    same = tmp_path / "aas-backups" / "same-device"
    backup(home, same, deep=True)
    near = _checks(_check(home, same))["backup"]
    assert (near["passed"], near["other_device"]) == (False, False)
    assert near["shared_device_with"] == ["market", "raw", "root", "state", "strategies"]


def test_a_broken_or_foreign_backup_fails(
    cut_over: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    home, _, backup_root = cut_over
    other = tmp_path / "other"
    initialize(other)
    foreign = tmp_path / "aas-backups" / "foreign"
    backup(other, foreign, deep=True)
    assert _checks(_check(home, foreign))["backup"]["same_installation"] is False
    runtime = backup_root / "runtime.json"
    runtime.write_bytes(runtime.read_bytes() + b" ")
    broken = _checks(_check(home, backup_root))["backup"]
    assert broken["passed"] is False
    assert "mismatch" in str(broken["error"])
    assert _checks(_check(home, tmp_path / "absent"))["backup"]["passed"] is False


def test_collectors_receipt_and_maintenance_run_are_required(
    cut_over: tuple[Path, Path, Path],
) -> None:
    home, _, backup_root = cut_over
    _runtime(home, off=["qveris"])
    collectors = _checks(_check(home, backup_root))["collectors"]
    assert (collectors["passed"], collectors["missing"]) == (False, ["qveris"])
    # Not expecting a provider is the operator's choice; jobs.enabled is still required.
    assert _checks(_check(home, backup_root, providers=["kind"]))["collectors"]["passed"]
    _runtime(home, jobs=False)
    assert _checks(_check(home, backup_root, providers=[]))["collectors"]["passed"] is False
    _runtime(home)
    _maintain_report(home, status="partial")
    report = _check(home, backup_root)
    assert report["failed"] == ["maintain_run"]
    (load_paths(home).runtime / "install-receipt.json").unlink()
    assert _check(home, backup_root)["failed"] == ["install_receipt", "maintain_run"]


def test_legacy_paths_must_be_imported_and_gone(
    cut_over: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    home, manifest, backup_root = cut_over
    kept = _private(tmp_path / "kept" / "file", b"x")
    never = _private(
        tmp_path / "specs" / "never.json",
        json.dumps(
            {
                "schema_version": "aas-legacy-import-v1",
                "entries": [
                    {
                        "name": "kept",
                        "loader": "fred.series_csv@1",
                        "path": str(kept),
                        "args": {},
                        "expect": {},
                    }
                ],
            }
        ).encode(),
    )
    report = _check(home, backup_root, manifests=[manifest, never], removed=[kept.parent])
    legacy = _checks(report)["legacy"]
    assert report["failed"] == ["legacy"]
    first, second = cast("list[dict[str, object]]", legacy["manifests"])
    assert first["passed"] is True
    assert (second["retained"], second["present"]) == (False, ["kept"])
    assert cast("dict[str, object]", second["verified"]) == {"passed": False, "report": None}
    assert legacy["removed"] == [{"path": str(kept.parent), "removed": False}]


def test_a_retained_manifest_without_a_complete_verify_fails(
    cut_over: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    # apply keeps the manifest in raw/ before its first unit, so retention proves nothing.
    home, manifest, backup_root = cut_over
    report = json.loads(_verify_path(manifest).read_text())

    def legacy(verify: Sequence[Path]) -> dict[str, object]:
        result = _check(home, backup_root, manifests=[manifest], verify=verify)
        assert result["failed"] == ["legacy"]
        item = cast("list[dict[str, object]]", _checks(result)["legacy"]["manifests"])[0]
        assert item["retained"] is True
        return cast("dict[str, object]", item["verified"])

    assert legacy([]) == {"passed": False, "report": None}
    partial = _private(
        tmp_path / "partial.json", json.dumps({**report, "complete": False}).encode()
    )
    assert legacy([partial])["complete"] is False
    entry = report["entries"][0]
    source = {**entry["sources"][0], "source_id": "never-committed"}
    lost = {**report, "entries": [{**entry, "sources": [source]}]}
    forged = _private(tmp_path / "forged.json", json.dumps(lost).encode())
    assert legacy([forged])["not_committed"] == [
        {"source_id": "never-committed", "state": "missing"}
    ]
    broken = _private(tmp_path / "broken.json", b"[]")
    result = _check(
        home, backup_root, manifests=[manifest], verify=[_verify_path(manifest), broken]
    )
    assert _checks(result)["legacy"]["unreadable_reports"] == [
        {"report": str(broken), "error": "the verify report is not an object"}
    ]


def test_a_prepared_operation_fails_the_operations_check(
    cut_over: tuple[Path, Path, Path],
) -> None:
    home, manifest, backup_root = cut_over
    with open_workspace(home, writable=True) as workspace:
        workspace.state.execute(
            "INSERT INTO storage_operations VALUES ('source:p', 'source-import', ?, 'x', "
            "NULL, ?, 'PREPARED', NULL, 1, NULL)",
            ("a" * 64, "b" * 64),
        )
        workspace.state.commit()
    report = _check(home, backup_root, manifests=[manifest])
    assert report["failed"] == ["operations"]
    operations = _checks(report)["operations"]
    assert (operations["prepared"], operations["sample"]) == (
        1,
        [{"operation_id": "source:p", "kind": "source-import"}],
    )


def test_an_outdated_core_schema_fails_the_schema_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "v1"
    with monkeypatch.context() as patch:
        patch.setattr(workspace_module, "_INSTALL_VERSION", 1)
        initialize(home)
    with open_workspace(home) as admitted:
        report = check_cutover(
            admitted, read_backup(tmp_path / "none"), CutoverRequest(), run=CUT_OVER
        )
    schema = _checks(report)["schema"]
    assert (schema["passed"], schema["state"]) == (False, "outdated")
    assert "schema" in cast("list[str]", report["failed"])


def test_retirements_are_reported_by_backup_operation_and_reason(
    cut_over: tuple[Path, Path, Path],
) -> None:
    home, _, backup_root = cut_over
    with open_workspace(home, writable=True) as workspace:
        workspace.state.execute(
            "INSERT INTO storage_operations VALUES ('source-retire:r', 'source-retire', ?, 'x', "
            "NULL, ?, 'COMPLETED', NULL, 1, 2)",
            ("a" * 64, "b" * 64),
        )
        for source, rows in (("old-a", 3), ("old-b", 4)):
            workspace.state.execute(
                "INSERT INTO source_retirements VALUES (?, ?, ?, 'superseded', 'new', '{}', ?, "
                "'backup-1', 'source-retire:r', 5)",
                (source, "c" * 64, rows, "d" * 64),
            )
        workspace.state.commit()
    with open_workspace(home) as workspace:
        summary = cutover.retirement_summary(workspace)
    assert summary == {
        "sources": 2,
        "rows": 7,
        "by_backup": {"backup-1": {"sources": 2, "rows": 7}},
        "by_operation": {"source-retire:r": {"sources": 2, "rows": 7}},
        "by_reason": {"superseded": {"sources": 2, "rows": 7}},
    }
    assert _checks(_check(home, backup_root))["backup"]["holds_every_operation"] is False


def test_the_command_records_only_a_passing_check(
    cut_over: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    home, manifest, backup_root = cut_over
    monkeypatch.setattr(cutover, "systemctl", CUT_OVER)
    arguments = ["maintain", "cutover-check", "--home", str(home), "--backup", str(backup_root),
                 "--legacy-manifest", str(manifest),
                 "--legacy-verify", str(_verify_path(manifest)), "--record",
                 *(item for name in PROVIDERS for item in ("--expect-provider", name))]  # fmt: skip
    record = load_paths(home).runtime / "cutover-record.json"
    _maintain_report(home, status="partial")
    assert main(arguments) == 1
    failing = json.loads(capsys.readouterr().out)
    assert (failing["failed"], failing["exit_code"]) == (["maintain_run"], 1)
    assert "record_sha256" not in failing
    assert not record.exists()
    _maintain_report(home)
    assert main(arguments) == 0
    passing = json.loads(capsys.readouterr().out)
    assert passing["passed"] is True
    assert hashlib.sha256(record.read_bytes()).hexdigest() == passing["record_sha256"]
    with pytest.raises(ValueError, match="only a passing"):
        write_record(load_paths(home), failing)


_OPERATIONS = Path(__file__).resolve().parents[2] / "dev-notes" / "operations.md"
_INVOCATION = re.compile(r"(?:^|[\s;&|(\"`])aas((?: --home \S+)?((?: [a-z][\w-]*){1,3}))(.*)")
_OPTION = re.compile(r"(?<![\w-])--[a-z][a-z0-9-]*")
# Where one shell command of a bash fence ends.
_COMMAND_END = re.compile(r"&&|\|\||[|;>]")
# The rehearsal block's homes, backups, reports and compact target, as the test finds them.
_REHEARSAL_OUTPUTS = 14


def _runbook() -> str:
    _, found, rest = _OPERATIONS.read_text(encoding="utf-8").partition("\n## 운영 전환\n")
    assert found, "the operations runbook section is missing"
    return rest.split("\n## ", 1)[0]


def _help(arguments: list[str], capsys: pytest.CaptureFixture[str]) -> str | None:
    with pytest.raises(SystemExit) as stopped:
        main([*arguments, "--help"])
    out = capsys.readouterr().out
    return out if stopped.value.code == 0 else None


def _invocations(text: str) -> list[tuple[tuple[str, ...], str]]:
    """Every ``aas`` command the runbook names, in fences and inline spans, with its options."""
    lines = [
        line
        for block in re.findall(r"```bash\n(.*?)```", text, re.DOTALL)
        for line in block.replace("\\\n", " ").splitlines()
    ]
    found = [
        (tuple(match[2].split()), _COMMAND_END.split(match[3], 1)[0])
        for line in lines
        for match in _INVOCATION.finditer(line)
    ]
    spans = (
        _INVOCATION.match(" ".join(span.split())) for span in re.findall(r"`(aas [^`]+)`", text)
    )
    found.extend((tuple(match[2].split()), match[3]) for match in spans if match is not None)
    return found


def test_the_runbook_names_only_commands_and_options_the_cli_has(
    capsys: pytest.CaptureFixture[str],
) -> None:
    text = _runbook()
    invocations = _invocations(text)
    commands = {words for words, _ in invocations}
    assert ("maintain", "cutover-check") in commands
    assert ("identity", "kr-build") in commands  # an inline step-4 command
    usages: dict[tuple[str, ...], tuple[tuple[str, ...], str]] = {}
    for words in sorted(commands):
        # The longest word prefix that is a command; the rest are its arguments.
        for size in range(len(words), 0, -1):
            if (usage := _help(list(words[:size]), capsys)) is not None:
                usages[words] = (words[:size], usage)
                break
        else:
            pytest.fail(f"aas {' '.join(words)} is not a command")
    for words, rest in invocations:
        command, usage = usages[words]
        options = set(_OPTION.findall(" ".join(words[len(command) :]) + rest))
        for option in sorted(options):
            assert option in usage, f"aas {' '.join(command)} {option}"
    check = text[text.index("aas maintain cutover-check") :].split("\n```", 1)[0]
    usage = usages[("maintain", "cutover-check")][1]
    for option in set(_OPTION.findall(check)):
        assert option in usage, option
    # The runbook retires every legacy unit and enables every approved collector.
    for unit in ("aas-native-data-maintenance", "aas-qveris-korea-backfill",
                 "aas-korea-historical-backfill", "aas-etf-discovery"):  # fmt: skip
        assert unit in text
    for provider in PROVIDERS:
        assert f"--expect-provider {provider}" in check
    assert "BACKUPS=~/aas-backups" in text


def test_the_rehearsal_writes_only_paths_its_cleanup_removes() -> None:
    text = _runbook()
    step = text[text.index("**2. 리허설.**") : text.index("**3. ")]
    (block,) = re.findall(r"```bash\n(.*?)```", step, re.DOTALL)
    # Nothing outside the rehearsal's own installation and outputs is touched.
    for forbidden in ("systemctl", "sed -i", "receipt", "import legacy", "$LEGACY", "$HOME_NEW",
                      "$HOME_FINAL", "$OLD_HOME", "jq ", "tar "):  # fmt: skip
        assert forbidden not in block, forbidden
    *body, cleanup = block.strip().splitlines()
    assert cleanup.startswith("rm -rf -- ")
    assert "rm " not in "\n".join(body)
    removed = [word.strip('"') for word in cleanup.removeprefix("rm -rf -- ").split()]
    assert 'RB="$BACKUPS/rehearsal-$TS"' in block
    written = [
        *re.findall(r"(?:--output|--backup-output|--to|--home|>) (\"[^\"]+\"|\S+)", block),
        *re.findall(r"mkdir -m 0700 (.+?)(?:\n|$)", block)[0].split(),
        *re.findall(r"(?:AAS_HOME|REPORTS)=(\"[^\"]+\"|\S+)", block),
    ]
    paths = [word.strip('"') for word in written if word[:1] in {'"', "$"}]
    assert len(paths) == _REHEARSAL_OUTPUTS, paths
    for path in paths:
        assert any(path == root or path.startswith(root + "/") for root in removed), path
