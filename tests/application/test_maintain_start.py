"""``jobs.start``: the datasets ``aas maintain`` may start from their declared first template.

Sources are synthetic Qveris jobs replayed through the scripted gateway and committed the way
``aas collect qveris import`` commits them (``tests.storage.test_qveris_actions``). No provider
is asked: ``jobs.enabled`` is false in every run here. Every symbol, rate and date is made up.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.application.maintain import Ports, plan_maintenance, run_maintenance
from aegis_alpha.application.maintain_config import MaintainConfig, parse_config
from aegis_alpha.storage.paths import load_paths
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.data.qveris_support import bulk_job, fx_job
from tests.storage.test_qveris_actions import EX, _dividend, _fx_rows, _split, replay

# Serial: replaying a Qveris job takes the host-wide account lease (an abstract Unix socket
# named by the account), so every file that takes it runs in one xdist worker.
pytestmark = pytest.mark.xdist_group("qveris-account-lease")

FX, US_ACTIONS, KR_ACTIONS = "fx.usdkrw.eodhd", "actions.us.eodhd", "actions.kr.eodhd"
# Before the packaged calendar declarations: the calendar stage refreshes nothing, so a start
# whose time rule pins a session calendar is blocked (``no_calendar``) while FX is not.
NOW = datetime(2026, 9, 16, 10, tzinfo=UTC)


def _home(tmp_path: Path) -> Path:
    """An installation holding collected FX, US splits and KR dividends, and no chain."""
    home = tmp_path / "aas"
    initialize(home)
    raw = tmp_path / "qveris"
    with open_workspace(home, writable=True, strategy_write=True) as ws:
        replay(ws, raw, fx_job(), _fx_rows(("2026-08-31", 1385.25), ("2026-09-01", 1390.5)))
        replay(ws, raw, bulk_job("US", "splits", EX.isoformat()), [_split("AAA", "US", "2/1")])
        replay(ws, raw, bulk_job("KO", "dividends", EX.isoformat()),
               [_dividend("123456", "KO", "100", unadjustedValue="100")])  # fmt: skip
    return home


def _versions(home: Path) -> list[tuple[str, int]]:
    with open_workspace(home) as ws:
        rows = ws.state.execute(
            "SELECT dataset_id, sequence FROM dataset_versions ORDER BY dataset_id, sequence"
        ).fetchall()
    return [(str(dataset), int(sequence)) for dataset, sequence in rows]


def _promote(report: dict[str, object]) -> dict[str, object]:
    return cast("dict[str, object]", cast("dict[str, object]", report["stages"])["promote"])


def _routes(report: dict[str, object]) -> dict[tuple[str, str], dict[str, object]]:
    return {
        (str(item["dataset_id"]), str(item["mapper"])): item
        for item in cast("list[dict[str, object]]", _promote(report)["datasets"])
    }


def _outcomes(report: dict[str, object]) -> dict[tuple[str, str], str]:
    start = cast("dict[str, object]", _promote(report)["start"])
    return {
        (str(item["dataset_id"]), str(item["mapper"])): str(item["outcome"])
        for item in cast("list[dict[str, object]]", start["routes"])
    }


def _run(home: Path, config: MaintainConfig, now: datetime) -> dict[str, object]:
    with open_workspace(home, writable=True, strategy_write=True) as ws:
        return run_maintenance(ws, config, Ports(), clock=lambda: now)


def test_a_run_starts_only_the_configured_datasets_and_a_second_run_starts_nothing(
    tmp_path: Path,
) -> None:
    home = _home(tmp_path)
    config = MaintainConfig(start=(FX, US_ACTIONS))
    first = _run(home, config, NOW)
    # A start blocked by a missing precondition is no failure: the exit code stays 0.
    assert (first["exit_code"], first["failed_stages"]) == (0, []), first["stages"]
    calendars = cast("dict[str, object]", cast("dict[str, object]", first["stages"])["calendars"])
    assert {item["status"] for item in cast("list[dict[str, object]]", calendars["calendars"])} == {
        "declared_after_now"
    }
    routes = _routes(first)
    fx = routes[(FX, "eodhd.fx_history@1")]
    assert (fx["status"], fx["started"]) == ("advanced", True)
    # Named, but its time rule pins a calendar the installation does not hold yet.
    splits = routes[(US_ACTIONS, "eodhd.splits@1")]
    assert (splits["status"], splits["started"]) == ("no_calendar", False)
    assert cast("dict[str, int]", splits["skipped"])["no_calendar"] == 1
    # Not named: it waits as before, whatever it holds.
    kr = routes[(KR_ACTIONS, "eodhd.dividends@1")]
    assert (kr["status"], kr["started"]) == ("no_head", False)
    assert cast("dict[str, int]", kr["skipped"])["no_head"] == 1
    assert cast("dict[str, object]", _promote(first)["start"])["configured"] == [FX, US_ACTIONS]
    assert _outcomes(first) == {
        (FX, "eodhd.fx_history@1"): "started",
        (US_ACTIONS, "eodhd.splits@1"): "no_calendar",
        (US_ACTIONS, "eodhd.dividends@1"): "no_calendar",
    }
    assert _versions(home) == [(FX, 1)]
    heads = cast("dict[str, object]", cast("dict[str, object]", first["stages"])["heads"])
    freshness = {
        str(item["dataset_id"]): item["freshness"]
        for item in cast("list[dict[str, object]]", heads["datasets"])
    }
    assert freshness[KR_ACTIONS] == freshness[US_ACTIONS] == "no_head"
    assert freshness[FX] != "no_head"

    # The same configuration again: the started chain holds a generation, so nothing starts.
    second = _run(home, config, NOW + timedelta(hours=1))
    assert (second["exit_code"], second["failed_stages"]) == (0, [])
    again = _routes(second)
    assert (again[(FX, "eodhd.fx_history@1")]["status"],
            again[(FX, "eodhd.fx_history@1")]["started"]) == ("current", False)  # fmt: skip
    assert _outcomes(second)[(FX, "eodhd.fx_history@1")] == "has_generation"
    assert _versions(home) == [(FX, 1)]


def _files(home: Path) -> dict[str, str]:
    """Every stored byte. A SQLite reader opens WAL sidecars; a write-ahead log stays empty."""
    files = [path for path in sorted(home.rglob("*")) if path.is_file()]
    assert all(path.stat().st_size == 0 for path in files if path.name.endswith("-wal"))
    return {
        str(path.relative_to(home)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in files
        if not path.name.endswith((".lock", "-wal", "-shm"))
    }


def test_a_plan_reports_what_the_configured_start_would_do_and_writes_nothing(
    tmp_path: Path,
) -> None:
    home = _home(tmp_path)
    config = MaintainConfig(start=(FX, US_ACTIONS))
    before = _files(home)
    with open_workspace(home) as ws:
        planned = plan_maintenance(ws, config, now=NOW, promotions=True)
    assert (planned["exit_code"], planned["failed_stages"]) == (0, []), planned["stages"]
    routes = _routes(planned)
    fx = routes[(FX, "eodhd.fx_history@1")]
    assert (fx["status"], fx["started"]) == ("planned", True)
    (source,) = cast("list[dict[str, object]]", fx["sources"])
    assert source["status"] == "planned"
    assert routes[(US_ACTIONS, "eodhd.splits@1")]["status"] == "no_calendar"
    assert routes[(KR_ACTIONS, "eodhd.dividends@1")]["status"] == "no_head"
    assert _outcomes(planned) == {
        (FX, "eodhd.fx_history@1"): "started",
        (US_ACTIONS, "eodhd.splits@1"): "no_calendar",
        (US_ACTIONS, "eodhd.dividends@1"): "no_calendar",
    }
    assert _files(home) == before
    assert _versions(home) == []
    # Without a list nothing would start: the same plan reports every chain waiting.
    with open_workspace(home) as ws:
        waiting = plan_maintenance(ws, MaintainConfig(), now=NOW, promotions=True)
    assert _routes(waiting)[(FX, "eodhd.fx_history@1")]["status"] == "no_head"
    assert _outcomes(waiting) == {}


def test_the_start_list_is_read_from_the_jobs_section(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    paths = load_paths(home)
    assert parse_config(paths, {"jobs": {"enabled": False}}).start == ()
    names = ["status.kr.kind", "fx.usdkrw.eodhd", "actions.us.eodhd", "actions.kr.eodhd",
             "status.us.eodhd"]  # fmt: skip
    config = parse_config(paths, {"jobs": {"enabled": True, "start": names}})
    assert (config.jobs_enabled, config.start) == (True, tuple(names))


@pytest.mark.parametrize(
    ("start", "message"),
    [
        (["prices.us.nowhere"], "unknown datasets"),
        (["fx.usdkrw.eodhd.r2"], "unknown datasets"),
        (["prices.us.eodhd"], "no declared start"),
        (["prices.us.norgate"], "no declared start"),
        (["fx.usdkrw.eodhd", "fx.usdkrw.eodhd"], "repeats"),
        ("fx.usdkrw.eodhd", "list of dataset names"),
        ([7], "list of dataset names"),
    ],
)
def test_an_unknown_or_unstartable_start_refuses_the_whole_configuration(
    tmp_path: Path, start: object, message: str
) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with pytest.raises((ValueError, TypeError), match=message):
        parse_config(load_paths(home), {"jobs": {"enabled": True, "start": start}})
