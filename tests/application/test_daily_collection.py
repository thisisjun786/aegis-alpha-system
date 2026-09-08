from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.application.daily_collection import daily_status, run_daily
from aegis_alpha.application.provider_config import CollectionConfig, ProviderProfile

_NOW = datetime(2026, 9, 5, 6, 30, tzinfo=UTC)
_REPO = Path(__file__).resolve().parents[2]


def _config(
    root: Path, *, provider: str = "fmp", mode: str = "daily", enabled: bool = True
) -> CollectionConfig:
    return CollectionConfig(
        root / "db.json",
        (
            ProviderProfile(
                provider=provider,
                enabled=enabled,
                credential_file=None,
                max_calls=10,
                mode=mode,
                options={},
            ),
        ),
    )


def _success(*_args: object) -> dict[str, object]:
    return {
        "status": "succeeded",
        "execution_started": True,
        "result": {"invocation_calls_attempted": 3},
    }


def test_repeated_day_never_dispatches_twice_and_missed_days_use_current_incremental(
    tmp_path: Path,
) -> None:
    state = tmp_path / "journal"
    config = _config(tmp_path)
    calls = []

    def runner(
        _config: CollectionConfig, profile: ProviderProfile, run_id: str | None
    ) -> dict[str, object]:
        calls.append((profile.mode, run_id))
        return _success()

    first = run_daily(config, state, runner=runner, clock=lambda: _NOW.replace(day=1))
    again = run_daily(config, state, runner=runner, clock=lambda: _NOW.replace(day=1))
    later = run_daily(config, state, runner=runner, clock=lambda: _NOW)
    assert first["status"] == later["status"] == "succeeded"
    again_providers = cast("list[dict[str, object]]", again["providers"])
    assert again_providers[0]["already_admitted"] is True
    assert calls == [("daily", None), ("daily", None)]
    assert daily_status(state)["last_success"] == {"fmp": "2026-09-05"}
    assert later["catch_up"] == "current_incremental_watermarks"


_CHILD = """
import os, sys
from pathlib import Path
from typing import cast
from datetime import UTC, datetime
from aegis_alpha.application.daily_collection import run_daily
from aegis_alpha.application.provider_config import CollectionConfig, ProviderProfile
root=Path(sys.argv[1])
p=ProviderProfile(provider="fmp",enabled=True,credential_file=None,max_calls=10,mode="daily",options={})
c=CollectionConfig(root.parent/"db.json",(p,))
def runner(*args):
    print("admitted",flush=True)
    if sys.argv[2]=="crash": os._exit(19)
    sys.stdin.read(1)
    return {"status":"succeeded","execution_started":True,"result":{"invocation_calls_attempted":3}}
run_daily(c,root,runner=runner,clock=lambda:datetime(2026,9,5,6,30,tzinfo=UTC))
"""


def _child(root: Path, mode: str) -> subprocess.Popen[str]:
    return subprocess.Popen(  # noqa: S603 -- fixed interpreter and synthetic child-only journal
        [sys.executable, "-c", _CHILD, str(root), mode],
        cwd=_REPO,
        env={**os.environ, "PYTHONPATH": str(_REPO / "src")},
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_hard_crash_keeps_unknown_admission_and_prevents_paid_replay(tmp_path: Path) -> None:
    state = tmp_path / "journal"
    child = _child(state, "crash")
    output, error = child.communicate(timeout=10)
    assert child.returncode == 19, error  # noqa: PLR2004 -- intentional child-only hard exit
    assert output.strip() == "admitted"
    report = daily_status(state)
    assert report["active"] is False
    assert report["last_success"] == {}
    providers = cast("dict[str, dict[str, object]]", report["providers"])
    assert providers["fmp"]["calls_attempted"] is None

    def forbidden(*_args: object) -> dict[str, object]:
        pytest.fail("an uncertain admitted run cannot be automatically replayed")

    assert (
        run_daily(_config(tmp_path), state, runner=forbidden, clock=lambda: _NOW)["status"]
        == "partial"
    )


def test_cross_process_duplicate_is_rejected_while_first_is_active(tmp_path: Path) -> None:
    state = tmp_path / "journal"
    child = _child(state, "wait")
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "admitted"
        assert daily_status(state)["active"] is True
        result = run_daily(_config(tmp_path), state, runner=_success, clock=lambda: _NOW)
        assert result["status"] == "already_running"
        child.communicate("finish", timeout=10)
        assert child.returncode == 0
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
    assert daily_status(state)["last_success"] == {"fmp": "2026-09-05"}


@pytest.mark.parametrize("tamper", ["start", "orphan_result"])
def test_journal_corruption_blocks_before_another_provider_call(
    tmp_path: Path, tamper: str
) -> None:
    state = tmp_path / "journal"
    run_daily(_config(tmp_path), state, runner=_success, clock=lambda: _NOW)
    start = state / "2026-09-05.fmp.start.json"
    if tamper == "start":
        body = json.loads(start.read_text())
        body["reserved_calls"] = 100
        start.write_text(json.dumps(body))
    else:
        start.unlink()
    with pytest.raises(ValueError, match="admission"):
        run_daily(_config(tmp_path), state, runner=_success, clock=lambda: _NOW)


def test_partial_or_unknown_spend_never_advances_success(tmp_path: Path) -> None:
    def unknown(*_args: object) -> dict[str, object]:
        return {"status": "succeeded", "execution_started": True, "result": {}}

    state = tmp_path / "journal"
    assert (
        run_daily(_config(tmp_path), state, runner=unknown, clock=lambda: _NOW)["status"]
        == "partial"
    )
    assert daily_status(state)["last_success"] == {}


def test_probe_mode_is_not_silently_scheduled_as_daily_prices(tmp_path: Path) -> None:
    def forbidden(*_args: object) -> dict[str, object]:
        pytest.fail("a universe probe must not enter daily price automation")

    result = run_daily(
        _config(tmp_path, mode="universe"),
        tmp_path / "journal",
        runner=forbidden,
        clock=lambda: _NOW,
    )
    assert result["status"] == "partial"
    providers = cast("list[dict[str, object]]", result["providers"])
    assert providers[0]["status"] == "blocked"
