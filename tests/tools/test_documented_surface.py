"""The shipped documents must describe the surface the code actually has."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_README = (_ROOT / "README.md").read_text(encoding="utf-8")
_OPERATIONS = (_ROOT / "dev-notes/operations.md").read_text(encoding="utf-8")


def _cli(*arguments: str) -> str:
    return subprocess.run(  # noqa: S603 -- fixed interpreter, read-only command
        [sys.executable, "-m", "aegis_alpha", *arguments],
        cwd=_ROOT,
        text=True,
        capture_output=True,
        check=True,
    ).stdout


def test_documents_name_the_add_on_and_both_recovery_outcomes() -> None:
    for document in (_README, _OPERATIONS):
        assert "aas db run-install" in document
        assert "aas db recover" in document
    assert "INTERRUPTED" in _README
    assert "INTERRUPTED" in _OPERATIONS
    # The marker-present outcome is the half a reader is most likely to miss.
    assert "재개" in _OPERATIONS


def test_documents_describe_backup_restore_of_recorded_runs() -> None:
    assert "aas db backup" in _OPERATIONS
    assert "aas db restore" in _OPERATIONS
    assert "설치한 wheel 검증" in _OPERATIONS
    assert "verify_installed_scenario.py" in _OPERATIONS
    # The lane is named so a reader can find the evidence, and its boundary is stated.
    assert "verify-lane-build" in _README
    assert "legacy" in _OPERATIONS


def test_no_document_still_claims_the_native_flow_is_unverified_on_a_base_install() -> None:
    """This sentence was true before the installed scenario existed. It is not now."""
    stale = "기본 설치만으로\n같은 흐름을 검증하지는 않았다"
    assert stale not in _README
    assert "native 경로는 다르다" in _README


def test_status_and_help_match_the_documented_commands() -> None:
    status = json.loads(_cli("status"))
    assert status["run_storage"]["install_command"] in _OPERATIONS
    assert status["run_storage"]["recovery_command"] in _OPERATIONS
    assert status["run_storage"]["execution_eligibility"] is False
    # argparse wraps descriptions, so compare against collapsed whitespace.
    help_text = " ".join(_cli("db", "--help").split())
    for command in ("run-install", "recover", "backup", "restore", "verify"):
        assert command in help_text
    # Every db subcommand carries a description, not a bare name.
    assert "Install the formal run add-on schema" in help_text
    assert "without recalculating" in help_text
