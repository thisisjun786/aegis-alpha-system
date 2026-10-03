"""``aas maintain`` configuration, install receipt, units and command line, offline."""

from __future__ import annotations

import configparser
import json
import sys
from datetime import date
from pathlib import Path

import pytest

from aegis_alpha.application import install_receipt
from aegis_alpha.application.cli import main
from aegis_alpha.application.maintain_config import parse_config
from aegis_alpha.storage.paths import load_paths
from aegis_alpha.storage.workspace import initialize, write_json

ROOT = Path(__file__).resolve().parents[2]
PROVIDERS: dict[str, dict[str, object]] = {
    "kind": {"enabled": True},
    "dart": {"enabled": True, "key_file": "opendart-api-key", "max_calls": 100},
    "sec": {"enabled": False, "user_agent_file": "sec-user-agent", "since": "2026-08-31"},
    "fred": {"enabled": True, "key_file": "/abs/fred-api-key"},
    "qveris": {
        "enabled": True,
        "key_file": "qveris-api-key",
        "raw_root": "/abs/raw/qveris",
        "since": {"US": "2026-07-28", "KO": "2026-08-31", "KQ": "2026-08-31"},
        "symbol_lists": ["KO", "KQ"],
        "forex": ["USDKRW"],
        "max_calls": 30,
        "max_credits": "100",
    },
}


def _home(tmp_path: Path, *, providers: object, jobs: bool) -> Path:
    home = tmp_path / "aas"
    initialize(home)
    runtime = json.loads((home / "runtime.json").read_text())
    write_json(home / "runtime.json", {**runtime, "providers": providers,
                                       "jobs": {"enabled": jobs}})  # fmt: skip
    return home


def test_the_runtime_configuration_names_providers_caps_and_secrets(tmp_path: Path) -> None:
    home = _home(tmp_path, providers=PROVIDERS, jobs=True)
    paths = load_paths(home)
    config = parse_config(paths, json.loads((home / "runtime.json").read_text()))
    assert config.jobs_enabled is True
    assert config.enabled() == ["kind", "dart", "fred", "qveris"]
    assert config.sec is None  # disabled sections are validated and left out
    assert config.dart is not None
    assert config.dart.key_file == paths.secrets / "opendart-api-key"
    assert (config.dart.max_calls, config.dart.daily_quota) == (100, 19_000)
    assert config.fred is not None
    assert config.fred.key_file == Path("/abs/fred-api-key")
    qveris = config.qveris
    assert qveris is not None
    assert qveris.policy.since["US"] == date(2026, 7, 28)
    assert qveris.policy.datasets == ("prices", "splits", "dividends")
    assert (qveris.caps.max_calls, qveris.caps.max_credits) == (30, "100")


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"qveris": {**PROVIDERS["qveris"], "max_credit": "1"}}, "unknown"),
        ({"dart": {"enabled": True}}, "missing"),
        ({"fred": {"enabled": "yes", "key_file": "k"}}, "boolean enabled"),
        ({"fmp": {"enabled": True}}, "unknown sections"),
        ({"qveris": {**PROVIDERS["qveris"], "raw_root": "relative"}}, "absolute"),
        ({"sec": {"enabled": True, "user_agent_file": "u", "issuers": "some"}}, "registered"),
    ],
)
def test_a_typo_or_missing_cap_refuses_the_whole_configuration(
    tmp_path: Path, change: dict[str, object], message: str
) -> None:
    home = _home(tmp_path, providers={}, jobs=True)
    runtime = {"jobs": {"enabled": True}, "providers": {**PROVIDERS, **change}}
    with pytest.raises((ValueError, TypeError), match=message):
        parse_config(load_paths(home), runtime)


def test_the_install_receipt_records_the_runtime_and_reports_its_differences(
    tmp_path: Path,
) -> None:
    home = _home(tmp_path, providers={}, jobs=False)
    paths = load_paths(home)
    assert install_receipt.runtime_report(paths)["receipt_sha256"] is None
    lock = tmp_path / "uv.lock"
    lock.write_text("synthetic lock\n")
    lock.chmod(0o600)
    written = install_receipt.write_receipt(paths, lock=lock)
    assert written["schema"] == "aas-install-receipt-v1"
    assert written["lock_sha256"] is not None
    interpreter = written["interpreter"]
    assert isinstance(interpreter, dict)
    assert interpreter["version_info"][:2] == list(sys.version_info[:2])
    report = install_receipt.runtime_report(paths)
    assert (report["receipt_sha256"], report["differences"]) == (written["receipt_sha256"], [])
    recorded = json.loads((paths.runtime / "install-receipt.json").read_text())
    recorded["interpreter"]["version"] = "3.13.13 (another build)"
    (paths.runtime / "install-receipt.json").write_text(json.dumps(recorded))
    # A different interpreter is reported, never refused.
    assert install_receipt.runtime_report(paths)["differences"] == ["interpreter.version"]


def _unit(name: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser(interpolation=None)
    config.read(ROOT / "config/systemd" / name)
    return config


def test_the_maintenance_units_run_the_installed_command_once_a_day() -> None:
    service = _unit("aas-maintain.service")["Service"]
    assert service["Type"] == "exec"  # RuntimeMaxSec applies only to a running service
    assert service["ExecStart"] == "%h/.local/bin/aas maintain run"
    assert (service["KillSignal"], service["Restart"], service["UMask"]) == ("SIGINT", "no",
                                                                            "0077")  # fmt: skip
    timer = _unit("aas-maintain.timer")
    assert timer["Timer"]["OnCalendar"] == "*-*-* 03:00:00 UTC"
    assert timer["Timer"]["Persistent"] == "true"
    assert timer["Timer"]["Unit"] == "aas-maintain.service"
    assert timer["Install"]["WantedBy"] == "timers.target"


def test_plan_reads_no_key_and_run_without_the_jobs_grant_calls_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    providers = {**PROVIDERS, "qveris": {**PROVIDERS["qveris"],
                                          "raw_root": str(tmp_path / "absent")}}  # fmt: skip
    home = _home(tmp_path, providers=providers, jobs=False)
    # No key file exists: a plan never reads one.
    assert main(["maintain", "plan", "--home", str(home), "--at", "2026-09-16T10:00:00Z"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert (plan["mode"], plan["provider_calls"], plan["failed_stages"]) == ("plan", 0, [])
    collect = plan["stages"]["collect"]
    assert collect["jobs_enabled"] is False
    assert collect["qveris"]["windows"]["US"] == {"from": "2026-07-28", "through": "2026-09-15"}
    assert collect["dart"]["provider_calls"] == 0
    assert main(["maintain", "run", "--home", str(home)]) == 0
    run = json.loads(capsys.readouterr().out)
    assert run["stages"]["collect"] == {"status": "jobs_disabled", "provider_calls": 0}
    assert run["exit_code"] == 0
    stored = json.loads((home / "runtime" / "maintain-report.json").read_text())
    assert stored["stages"] == run["stages"]
    assert main(["maintain", "receipt", "--home", str(home)]) == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["schema"] == "aas-install-receipt-v1"
