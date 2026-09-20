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
        "aegis_etf_target_replay": True,
            "aegis_registered_strategy_run": True,
            "aegis_declared_research_run": True,
        "research_proxy_returns": True,
        "research_candidate_generation": True,
        "database_adapter": True,
        "provider_collection": True,
        "daily_collection": True,
        "compute_budget": True,
        "live_orders": False,
        "etf_candidate_comparison": True,
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


def test_status_separates_the_run_add_on_from_execution_eligibility() -> None:
    """aegis_registered_strategy_run says the code is here, not that this install can run.

    The run tables are a separate explicit install, and a stored result is research
    evidence. Status has to say both, or a true capability flag reads as readiness.
    """
    result = _run("status")
    assert result.returncode == 0, result.stderr
    status = json.loads(result.stdout)
    assert status["capabilities"]["aegis_registered_strategy_run"] is True
    block = status["run_storage"]
    assert block["module"] == "aegis"
    assert block["add_on_required"] is True
    assert block["install_command"] == "aas db run-install"
    assert block["recovery_command"] == "aas db recover"
    assert block["backup_restore"] is True
    assert block["research_only"] is True
    assert block["execution_eligibility"] is False
    assert block["live_trading_approval"] is False
    assert block["point_in_time_certified"] is False
