from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.data.qveris_acquisition_cli import main
from aegis_alpha.data.serialization import canonical_json_bytes
from tests.data.test_qveris_acquisition import eod_job

if TYPE_CHECKING:
    from pathlib import Path

CLI_ERROR = 2


@pytest.mark.parametrize("relative", ["jobs.json", "inputs/jobs.json"])
def test_relative_jobs_plan_remains_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    relative: str,
) -> None:
    jobs = tmp_path / relative
    jobs.parent.mkdir(exist_ok=True)
    jobs.write_bytes(canonical_json_bytes({"schema_version": 1, "jobs": [eod_job().document()]}))
    monkeypatch.chdir(tmp_path)
    output = tmp_path / "not-created"
    assert (
        main(["--jobs", relative, "--output-root", str(output), "--key-file", "/absent-key"]) == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["provider_calls"] == result["writes"] == 0
    assert result["jobs"][0]["fingerprint"] == eod_job().fingerprint
    assert not output.exists()


@pytest.mark.parametrize("parent_alias", [False, True])
def test_relative_jobs_alias_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    parent_alias: bool,
) -> None:
    real = tmp_path / "real"
    real.mkdir()
    jobs = real / "jobs.json"
    jobs.write_bytes(canonical_json_bytes({"schema_version": 1, "jobs": [eod_job().document()]}))
    alias = tmp_path / "alias"
    alias.symlink_to(real if parent_alias else jobs, target_is_directory=parent_alias)
    monkeypatch.chdir(tmp_path)
    relative = "alias/jobs.json" if parent_alias else "alias"
    output = tmp_path / "not-created"
    assert main(["--jobs", relative, "--output-root", str(output)]) == CLI_ERROR
    captured = capsys.readouterr()
    assert not captured.out
    assert json.loads(captured.err)["status"] == "STOPPED"
    assert not output.exists()
