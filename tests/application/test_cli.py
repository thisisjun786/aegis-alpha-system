from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_USAGE_ERROR = 2
_ROOT = Path(__file__).resolve().parents[2]


def _run(*arguments: str) -> subprocess.CompletedProcess[str]:
    # -S deliberately removes site packages: no VT or database driver can be imported.
    return subprocess.run(  # noqa: S603 -- fixed interpreter and controlled CLI inputs
        [sys.executable, "-S", "-m", "aegis_alpha", *arguments],
        env={**os.environ, "PYTHONPATH": str(_ROOT / "src"), "PYTHONDONTWRITEBYTECODE": "1"},
        cwd=_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_cli_status_without_site_packages() -> None:
    result = _run("status")
    assert result.returncode == 0, result.stderr
    status = json.loads(result.stdout)
    assert status["capabilities"] == {
        "allocation_preview": True,
        "strategy_execution": False,
        "database_adapter": True,
        "provider_collection": True,
        "daily_collection": True,
        "compute_budget": True,
        "live_orders": False,
    }
    assert status["runtime_dependencies"]["vibe_trading"] is False


def test_cli_example_without_site_packages() -> None:
    result = _run("preview", "--input", "examples/portfolio-preview.json")
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["cash_weight"] == pytest.approx(0.17)
    assert report["execution_enabled"] is False


def test_cli_input_error_is_json_on_stderr(tmp_path: Path) -> None:
    path = tmp_path / "invalid.json"
    path.write_text('{"schema_version": true}', encoding="utf-8")
    result = _run("preview", "--input", str(path))
    assert result.returncode == 1
    assert not result.stdout
    assert "error" in json.loads(result.stderr)


def test_cli_missing_file_and_usage_errors(tmp_path: Path) -> None:
    assert _run("preview", "--input", str(tmp_path / "absent.json")).returncode == 1
    assert _run("preview").returncode == _USAGE_ERROR
    assert _run("trade").returncode == _USAGE_ERROR
